"""
核心同步逻辑 — 基于 Binlog 的实时同步（企业级）
特性：批量写入 | 断点续传 | 连接池 | 监控指标 | 错误重试
"""
import logging
import time
import sqlite3
import threading
from collections import defaultdict
from queue import Queue, Empty
from datetime import datetime
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent

from .config import AppConfig, SyncRule

logger = logging.getLogger("mysql_sync")


# ========== 连接池 ==========

class ConnectionPool:
    """简易 MySQL 连接池"""

    def __init__(self, config_dict, pool_size=5):
        self.config = config_dict
        self.pool_size = pool_size
        self._pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()

        for _ in range(pool_size):
            conn = self._create_conn()
            self._pool.put(conn)

    def _create_conn(self):
        return pymysql.connect(
            host=self.config.get("host", "127.0.0.1"),
            port=self.config.get("port", 3306),
            user=self.config.get("user", "root"),
            password=self.config.get("passwd", ""),
            database=self.config.get("database"),
            autocommit=True,
            charset="utf8mb4",
        )

    def get_conn(self):
        try:
            conn = self._pool.get(timeout=5)
            conn.ping(reconnect=True)
            return conn
        except Empty:
            return self._create_conn()

    def put_conn(self, conn):
        try:
            conn.ping()
            self._pool.put_nowait(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def close_all(self):
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Exception:
                pass


# ========== 同步状态 ==========

class SyncState:
    """同步状态管理（用 SQLite 存储，支持断点续传）"""

    def __init__(self, db_path: str = "sync_state.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_db()

    def _init_db(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS sync_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db TEXT NOT NULL,
                source_table TEXT NOT NULL,
                target_db TEXT NOT NULL,
                target_table TEXT NOT NULL,
                status TEXT DEFAULT 'running',
                last_event TEXT,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_count INTEGER DEFAULT 0,
                total_rows INTEGER DEFAULT 0,
                batch_count INTEGER DEFAULT 0,
                UNIQUE(source_db, source_table, target_db, target_table)
            );
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                source TEXT,
                target TEXT,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS binlog_position (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                filename TEXT NOT NULL,
                position INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_log_created ON sync_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name, recorded_at);
        """)
        self.conn.commit()

    def update_status(self, rule: SyncRule, event_type: str, rows: int = 0):
        key = (rule.source_db, rule.source_table, rule.target_db, rule.target_table)
        self.conn.execute("""
            INSERT INTO sync_status (source_db, source_table, target_db, target_table, status, last_event, total_rows, batch_count)
            VALUES (?, ?, ?, ?, 'running', ?, ?, 1)
            ON CONFLICT(source_db, source_table, target_db, target_table)
            DO UPDATE SET last_event=?, total_rows=total_rows+?, batch_count=batch_count+1, last_update=CURRENT_TIMESTAMP
        """, (*key, event_type, rows, event_type, rows))
        self.conn.commit()

    def add_log(self, event_type: str, source: str, target: str, message: str):
        self.conn.execute(
            "INSERT INTO sync_log (event_type, source, target, message) VALUES (?, ?, ?, ?)",
            (event_type, source, target, message)
        )
        self.conn.commit()

    # ===== 断点续传 =====

    def save_binlog_position(self, filename: str, position: int):
        """保存 Binlog 位点"""
        self.conn.execute("""
            INSERT INTO binlog_position (id, filename, position, updated_at)
            VALUES (1, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET filename=?, position=?, updated_at=CURRENT_TIMESTAMP
        """, (filename, position, filename, position))
        self.conn.commit()

    def get_binlog_position(self):
        """获取上次的 Binlog 位点"""
        cursor = self.conn.execute("SELECT filename, position FROM binlog_position WHERE id=1")
        row = cursor.fetchone()
        if row:
            return {"filename": row[0], "position": row[1]}
        return None

    # ===== 监控指标 =====

    def record_metric(self, name: str, value: float):
        self.conn.execute(
            "INSERT INTO metrics (metric_name, metric_value) VALUES (?, ?)",
            (name, value)
        )

    def commit_metrics(self):
        self.conn.commit()

    def get_metrics(self, name: str, limit: int = 60):
        cursor = self.conn.execute(
            "SELECT metric_value, recorded_at FROM metrics WHERE metric_name=? ORDER BY id DESC LIMIT ?",
            (name, limit)
        )
        return cursor.fetchall()

    def get_all_status(self):
        cursor = self.conn.execute(
            "SELECT source_db, source_table, target_db, target_table, status, last_event, last_update, error_count, total_rows, batch_count "
            "FROM sync_status ORDER BY id"
        )
        return cursor.fetchall()

    def get_recent_logs(self, limit=100):
        cursor = self.conn.execute(
            "SELECT event_type, source, target, message, created_at FROM sync_log ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return cursor.fetchall()

    def get_stats(self):
        """获取总体统计"""
        cursor = self.conn.execute("SELECT SUM(total_rows), SUM(batch_count), SUM(error_count) FROM sync_status")
        row = cursor.fetchone()
        return {
            "total_rows": row[0] or 0,
            "total_batches": row[1] or 0,
            "total_errors": row[2] or 0,
        }

    def close(self):
        self.conn.close()


# ========== 批量写入器 ==========

class BatchWriter:
    """批量写入器 — 攒一批数据再写入，减少 IO"""

    def __init__(self, pool: ConnectionPool, batch_size: int = 500, flush_interval: float = 1.0):
        self.pool = pool
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        # 缓冲区：(db, table) -> list of (sql, params)
        self._insert_buffer = defaultdict(list)
        self._update_buffer = defaultdict(list)
        self._delete_buffer = defaultdict(list)
        self._lock = threading.Lock()
        self._last_flush = time.time()

    def add_insert(self, db: str, table: str, columns: list, rows: list):
        with self._lock:
            key = (db, table)
            for row in rows:
                self._insert_buffer[key].append((columns, row))
            if len(self._insert_buffer[key]) >= self.batch_size:
                self._flush_inserts(key)

    def add_update(self, db: str, table: str, columns: list, pk_cols: list, rows: list):
        with self._lock:
            key = (db, table)
            for row in rows:
                self._update_buffer[key].append((columns, pk_cols, row))
            if len(self._update_buffer[key]) >= self.batch_size:
                self._flush_updates(key)

    def add_delete(self, db: str, table: str, pk_cols: list, rows: list):
        with self._lock:
            key = (db, table)
            for row in rows:
                self._delete_buffer[key].append((pk_cols, row))
            if len(self._delete_buffer[key]) >= self.batch_size:
                self._flush_deletes(key)

    def flush_all(self):
        """强制刷新所有缓冲区"""
        with self._lock:
            for key in list(self._insert_buffer.keys()):
                if self._insert_buffer[key]:
                    self._flush_inserts(key)
            for key in list(self._update_buffer.keys()):
                if self._update_buffer[key]:
                    self._flush_updates(key)
            for key in list(self._delete_buffer.keys()):
                if self._delete_buffer[key]:
                    self._flush_deletes(key)

    def _flush_inserts(self, key):
        db, table = key
        items = self._insert_buffer[key]
        if not items:
            return

        columns = items[0][0]
        col_str = ",".join([f"`{c}`" for c in columns])
        placeholders = ",".join(["%s"] * len(columns))
        update_str = ",".join([f"`{c}`=VALUES(`{c}`)" for c in columns])

        sql = f"INSERT INTO `{db}`.`{table}` ({col_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_str}"
        params = [[row.get(c) for c in columns] for _, row in items]

        conn = self.pool.get_conn()
        try:
            cursor = conn.cursor()
            cursor.executemany(sql, params)
            cursor.close()
            logger.debug(f"批量写入 {db}.{table}: {len(items)} 条")
        except Exception as e:
            logger.error(f"批量写入失败 {db}.{table}: {e}")
            # 降级：逐条写入
            for p in params:
                try:
                    conn.execute(sql, p)
                except Exception:
                    pass
        finally:
            self.pool.put_conn(conn)

        self._insert_buffer[key] = []

    def _flush_updates(self, key):
        db, table = key
        items = self._update_buffer[key]
        if not items:
            return

        conn = self.pool.get_conn()
        try:
            for columns, pk_cols, row_data in items:
                new_vals = [row_data.get(c) for c in columns]
                where_vals = [row_data.get(c) for c in pk_cols]
                set_str = ",".join([f"`{c}`=%s" for c in columns])
                where_str = " AND ".join([f"`{c}`=%s" for c in pk_cols])
                conn.execute(
                    f"UPDATE `{db}`.`{table}` SET {set_str} WHERE {where_str}",
                    new_vals + where_vals
                )
        except Exception as e:
            logger.error(f"批量更新失败 {db}.{table}: {e}")
        finally:
            self.pool.put_conn(conn)

        self._update_buffer[key] = []

    def _flush_deletes(self, key):
        db, table = key
        items = self._delete_buffer[key]
        if not items:
            return

        conn = self.pool.get_conn()
        try:
            for pk_cols, row_data in items:
                where_vals = [row_data.get(c) for c in pk_cols]
                where_str = " AND ".join([f"`{c}`=%s" for c in pk_cols])
                conn.execute(
                    f"DELETE FROM `{db}`.`{table}` WHERE {where_str}",
                    where_vals
                )
        except Exception as e:
            logger.error(f"批量删除失败 {db}.{table}: {e}")
        finally:
            self.pool.put_conn(conn)

        self._delete_buffer[key] = []

    def auto_flush(self):
        """检查是否需要自动刷新"""
        if time.time() - self.last_flush >= self.flush_interval:
            self.flush_all()
            self.last_flush = time.time()


# ========== 同步引擎 ==========

class MySQLSyncer:
    """MySQL Binlog 同步引擎"""

    def __init__(self, config: AppConfig, state: SyncState = None):
        self.config = config
        self.state = state or SyncState(config.state_db)
        self.running = False
        self.stream = None
        self._rules_map = {}

        # 连接池
        self.src_pool = ConnectionPool(config.source.to_dict(), pool_size=3)
        self.dst_pool = ConnectionPool(config.target.to_dict(), pool_size=5)

        # 批量写入器
        self.writer = BatchWriter(
            self.dst_pool,
            batch_size=config.batch_size,
            flush_interval=config.flush_interval,
        )

        # 指标
        self._start_time = None
        self._event_count = 0

    def _build_rules_map(self):
        """构建规则映射表，展开通配符"""
        self._rules_map = {}
        for rule in self.config.rules:
            if rule.source_table == "*":
                conn = self.src_pool.get_conn()
                try:
                    rows = conn.execute(f"SHOW TABLES FROM `{rule.source_db}`").fetchall()
                    for (table_name,) in rows:
                        key = (rule.source_db, table_name)
                        self._rules_map[key] = SyncRule(
                            source_db=rule.source_db,
                            source_table=table_name,
                            target_db=rule.target_db,
                            target_table=table_name,
                        )
                finally:
                    self.src_pool.put_conn(conn)
            else:
                key = (rule.source_db, rule.source_table)
                self._rules_map[key] = rule

    def _should_sync(self, schema: str, table: str) -> bool:
        if schema in ("mysql", "sys", "information_schema", "performance_schema"):
            return False
        if schema == self.config.target.database:
            return False
        return (schema, table) in self._rules_map

    def _should_sync_for_ddl(self, schema: str) -> bool:
        if schema in ("mysql", "sys", "information_schema", "performance_schema"):
            return False
        if schema == self.config.target.database:
            return False
        return schema in {r.source_db for r in self.config.rules}

    def ensure_table_exists(self, rule: SyncRule):
        """目标表不存在就自动创建"""
        conn = self.dst_pool.get_conn()
        try:
            exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (rule.target_db, rule.target_table)
            )
            if exists == 0:
                src_conn = self.src_pool.get_conn()
                try:
                    row = src_conn.execute(
                        f"SHOW CREATE TABLE `{rule.source_db}`.`{rule.source_table}`"
                    ).fetchone()
                    create_sql = row[1].replace(f"`{rule.source_db}`", f"`{rule.target_db}`")
                    conn.execute(create_sql)
                    logger.info(f"自动创建表: {rule.target_db}.{rule.target_table}")
                    self.state.add_log("DDL", f"{rule.source_db}.{rule.source_table}",
                                       f"{rule.target_db}.{rule.target_table}", "自动创建表")
                finally:
                    self.src_pool.put_conn(src_conn)
        finally:
            self.dst_pool.put_conn(conn)

    def handle_ddl(self, schema: str, sql: str):
        """处理 DDL 事件"""
        if not self._should_sync_for_ddl(schema):
            return

        sql_upper = sql.upper()
        if not any(kw in sql_upper for kw in (
            "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "RENAME TABLE", "TRUNCATE"
        )):
            return

        translated = sql.replace(f"`{schema}`.", f"`{self.config.target.database}`.")
        if "CREATE TABLE" in sql_upper:
            translated = translated.replace(
                "CREATE TABLE ", f"CREATE TABLE IF NOT EXISTS `{self.config.target.database}`."
            )

        conn = self.dst_pool.get_conn()
        try:
            conn.execute(translated)
            logger.info(f"DDL 同步: {sql[:80]}")
            self.state.add_log("DDL", schema, self.config.target.database, sql[:200])
        except Exception as e:
            logger.error(f"DDL 同步失败: {e}")
            self.state.add_log("DDL_ERROR", schema, self.config.target.database, str(e))
        finally:
            self.dst_pool.put_conn(conn)

    def handle_dml(self, event):
        """处理 DML 事件 — 攒批处理"""
        schema = event.schema
        table = event.table

        if not self._should_sync(schema, table):
            return

        rule = self._rules_map[(schema, table)]
        columns = [col["name"] for col in event.columns]

        if isinstance(event, WriteRowsEvent):
            rows = [row["values"] for row in event.rows]
            self.writer.add_insert(rule.target_db, rule.target_table, columns, rows)
            self.state.update_status(rule, "INSERT", len(rows))

        elif isinstance(event, UpdateRowsEvent):
            pk_cols = [col["name"] for col in event.columns if col.get("is_primary")]
            if not pk_cols:
                pk_cols = [columns[0]]
            rows = [row["after_values"] for row in event.rows]
            self.writer.add_update(rule.target_db, rule.target_table, columns, pk_cols, rows)
            self.state.update_status(rule, "UPDATE", len(rows))

        elif isinstance(event, DeleteRowsEvent):
            pk_cols = [col["name"] for col in event.columns if col.get("is_primary")]
            if not pk_cols:
                pk_cols = [columns[0]]
            rows = [row["values"] for row in event.rows]
            self.writer.add_delete(rule.target_db, rule.target_table, pk_cols, rows)
            self.state.update_status(rule, "DELETE", len(rows))

        self._event_count += 1

        # 定期刷新
        if self._event_count % 100 == 0:
            self.writer.flush_all()

    def _record_position(self, event):
        """记录 Binlog 位点"""
        if hasattr(event, 'packet') and hasattr(event.packet, 'log_pos'):
            filename = self.stream.binlog_file() if hasattr(self.stream, 'binlog_file') else None
            position = event.packet.log_pos
            if filename and position:
                self.state.save_binlog_position(filename, position)

    def start(self):
        """启动同步（支持断点续传）"""
        self._build_rules_map()
        self.running = True
        self._start_time = time.time()

        # 确保目标表存在
        for rule in self.config.rules:
            self.ensure_table_exists(rule)

        # 获取上次位点
        resume_pos = self.state.get_binlog_position()
        stream_kwargs = {
            "connection_settings": self.config.source.to_dict(),
            "server_id": self.config.server_id,
            "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
            "freeze_schema": True,
        }

        if resume_pos:
            stream_kwargs["log_file"] = resume_pos["filename"]
            stream_kwargs["log_pos"] = resume_pos["position"]
            logger.info(f"从断点恢复: {resume_pos['filename']}:{resume_pos['position']}")
        else:
            # 只从当前位置开始，不处理历史数据
            stream_kwargs["resume_stream"] = True
            logger.info("首次启动，从当前位置开始")

        self.stream = BinLogStreamReader(**stream_kwargs)

        logger.info("同步引擎已启动")

        try:
            for event in self.stream:
                if not self.running:
                    break

                if isinstance(event, QueryEvent):
                    self.handle_ddl(event.schema.decode(), event.query.decode().strip())
                else:
                    self.handle_dml(event)

                # 记录位点
                self._record_position(event)

                # 定期记录指标
                if self._event_count % 1000 == 0:
                    elapsed = time.time() - self._start_time
                    qps = self._event_count / max(elapsed, 1)
                    self.state.record_metric("qps", qps)
                    self.state.record_metric("total_events", self._event_count)
                    self.state.commit_metrics()

        except Exception as e:
            logger.error(f"同步异常: {e}")
            self.state.add_log("ERROR", "engine", "", str(e))
            raise
        finally:
            self.stop()

    def stop(self):
        """停止同步"""
        self.running = False
        # 最后刷新一次
        self.writer.flush_all()
        if self.stream:
            self.stream.close()
        self.src_pool.close_all()
        self.dst_pool.close_all()
        logger.info("同步引擎已停止")

    def get_status(self):
        return self.state.get_all_status()

    def get_logs(self, limit=100):
        return self.state.get_recent_logs(limit)

    def get_stats(self):
        stats = self.state.get_stats()
        if self._start_time:
            elapsed = time.time() - self._start_time
            stats["uptime_seconds"] = int(elapsed)
            stats["qps"] = round(self._event_count / max(elapsed, 1), 2)
        stats["running"] = self.running
        return stats
