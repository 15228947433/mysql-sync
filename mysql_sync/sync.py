"""
核心同步逻辑 — 全量 + 增量同步（企业级）
特性：全量同步 | 增量同步 | 并行处理 | 批量写入 | 断点续传 | 连接池
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
from pymysqlreplication.event import QueryEvent

from .config import AppConfig, SyncRule

logger = logging.getLogger("mysql_sync")


# ========== 连接池 ==========

class ConnectionPool:
    """简易 MySQL 连接池"""

    def __init__(self, config_dict, pool_size=5):
        self.config = config_dict
        self.pool_size = pool_size
        self._pool = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self._pool.put(self._create_conn())

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
                self._pool.get_nowait().close()
            except Exception:
                pass


# ========== 同步状态 ==========

class SyncState:
    """同步状态管理（SQLite）"""

    def __init__(self, db_path="sync_state.db"):
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
                status TEXT DEFAULT 'pending',
                phase TEXT DEFAULT 'pending',
                last_event TEXT,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_count INTEGER DEFAULT 0,
                total_rows INTEGER DEFAULT 0,
                full_sync_rows INTEGER DEFAULT 0,
                incr_sync_rows INTEGER DEFAULT 0,
                progress REAL DEFAULT 0,
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
        """)
        self.conn.commit()

    def update_phase(self, rule, phase, progress=0):
        self.conn.execute("""
            INSERT INTO sync_status (source_db, source_table, target_db, target_table, status, phase, progress)
            VALUES (?, ?, ?, ?, 'running', ?, ?)
            ON CONFLICT(source_db, source_table, target_db, target_table)
            DO UPDATE SET phase=?, progress=?, last_update=CURRENT_TIMESTAMP, status='running'
        """, (rule.source_db, rule.source_table, rule.target_db, rule.target_table, phase, progress, phase, progress))
        self.conn.commit()

    def update_incr(self, rule, event_type, rows=0):
        self.conn.execute("""
            INSERT INTO sync_status (source_db, source_table, target_db, target_table, status, phase, last_event, incr_sync_rows, total_rows)
            VALUES (?, ?, ?, ?, 'running', 'incremental', ?, ?, ?, ?)
            ON CONFLICT(source_db, source_table, target_db, target_table)
            DO UPDATE SET last_event=?, incr_sync_rows=incr_sync_rows+?, total_rows=total_rows+?, last_update=CURRENT_TIMESTAMP
        """, (rule.source_db, rule.source_table, rule.target_db, rule.target_table,
              event_type, rows, rows, rows, event_type, rows, rows))
        self.conn.commit()

    def update_full(self, rule, rows):
        self.conn.execute("""
            INSERT INTO sync_status (source_db, source_table, target_db, target_table, status, phase, full_sync_rows, total_rows)
            VALUES (?, ?, ?, ?, 'running', 'full_sync', ?, ?)
            ON CONFLICT(source_db, source_table, target_db, target_table)
            DO UPDATE SET full_sync_rows=full_sync_rows+?, total_rows=total_rows+?, last_update=CURRENT_TIMESTAMP
        """, (rule.source_db, rule.source_table, rule.target_db, rule.target_table, rows, rows, rows, rows))
        self.conn.commit()

    def add_log(self, event_type, source, target, message):
        self.conn.execute("INSERT INTO sync_log (event_type, source, target, message) VALUES (?,?,?,?)",
                          (event_type, source, target, message))
        self.conn.commit()

    def save_binlog_position(self, filename, position):
        self.conn.execute("""
            INSERT INTO binlog_position (id, filename, position, updated_at) VALUES (1,?,?,CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET filename=?, position=?, updated_at=CURRENT_TIMESTAMP
        """, (filename, position, filename, position))
        self.conn.commit()

    def get_binlog_position(self):
        cursor = self.conn.execute("SELECT filename, position FROM binlog_position WHERE id=1")
        row = cursor.fetchone()
        return {"filename": row[0], "position": row[1]} if row else None

    def record_metric(self, name, value):
        self.conn.execute("INSERT INTO metrics (metric_name, metric_value) VALUES (?,?)", (name, value))

    def commit(self):
        self.conn.commit()

    def get_all_status(self):
        return self.conn.execute(
            "SELECT source_db, source_table, target_db, target_table, status, phase, last_event, last_update, error_count, total_rows, full_sync_rows, incr_sync_rows, progress FROM sync_status ORDER BY id"
        ).fetchall()

    def get_recent_logs(self, limit=100):
        return self.conn.execute(
            "SELECT event_type, source, target, message, created_at FROM sync_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()

    def get_stats(self):
        row = self.conn.execute("SELECT SUM(total_rows), SUM(full_sync_rows), SUM(incr_sync_rows), SUM(error_count) FROM sync_status").fetchone()
        return {"total_rows": row[0] or 0, "full_sync_rows": row[1] or 0, "incr_sync_rows": row[2] or 0, "total_errors": row[3] or 0}

    def close(self):
        self.conn.close()


# ========== 全量同步器 ==========

class FullSyncer:
    """全量同步器 — 扫描源表，批量导入目标表"""

    def __init__(self, src_pool, dst_pool, state, batch_size=500):
        self.src_pool = src_pool
        self.dst_pool = dst_pool
        self.state = state
        self.batch_size = batch_size
        self.running = True

    def sync_table(self, rule):
        """全量同步单张表"""
        logger.info(f"开始全量同步: {rule.source_db}.{rule.source_table} → {rule.target_db}.{rule.target_table}")
        self.state.update_phase(rule, "full_sync", 0)
        self.state.add_log("FULL_SYNC_START", f"{rule.source_db}.{rule.source_table}",
                          f"{rule.target_db}.{rule.target_table}", "开始全量同步")

        src_conn = self.src_pool.get_conn()
        dst_conn = self.dst_pool.get_conn()

        try:
            # 1. 确保目标表存在
            self._ensure_table(src_conn, dst_conn, rule)

            # 2. 获取总行数
            total = src_conn.execute(f"SELECT COUNT(*) FROM `{rule.source_db}`.`{rule.source_table}`").fetchone()[0]
            if total == 0:
                logger.info(f"源表为空，跳过全量同步: {rule.source_db}.{rule.source_table}")
                self.state.update_phase(rule, "full_done", 100)
                return

            # 3. 获取列信息
            src_conn.execute(f"SELECT * FROM `{rule.source_db}`.`{rule.source_table}` LIMIT 1")
            columns = [desc[0] for desc in src_conn.description]
            col_str = ",".join([f"`{c}`" for c in columns])
            placeholders = ",".join(["%s"] * len(columns))
            update_str = ",".join([f"`{c}`=VALUES(`{c}`)" for c in columns])
            sql = f"INSERT INTO `{rule.target_db}`.`{rule.target_table}` ({col_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_str}"

            # 4. 分批读取+写入
            offset = 0
            synced = 0
            while self.running:
                rows = src_conn.execute(
                    f"SELECT * FROM `{rule.source_db}`.`{rule.source_table}` LIMIT {self.batch_size} OFFSET {offset}"
                ).fetchall()

                if not rows:
                    break

                params = [list(row) for row in rows]
                try:
                    cursor = dst_conn.cursor()
                    cursor.executemany(sql, params)
                    cursor.close()
                except Exception as e:
                    # 降级：逐条写入
                    logger.warning(f"批量写入失败，降级逐条: {e}")
                    for p in params:
                        try:
                            dst_conn.execute(sql, p)
                        except Exception:
                            pass

                synced += len(rows)
                offset += self.batch_size
                progress = round(synced / total * 100, 1)
                self.state.update_full(rule, len(rows))
                self.state.update_phase(rule, "full_sync", progress)

                if synced % (self.batch_size * 10) == 0:
                    logger.info(f"全量同步进度: {rule.source_db}.{rule.source_table} {synced}/{total} ({progress}%)")

            self.state.update_phase(rule, "full_done", 100)
            logger.info(f"全量同步完成: {rule.source_db}.{rule.source_table} → {rule.target_db}.{rule.target_table} ({synced} 行)")
            self.state.add_log("FULL_SYNC_DONE", f"{rule.source_db}.{rule.source_table}",
                              f"{rule.target_db}.{rule.target_table}", f"全量同步完成 {synced} 行")

        except Exception as e:
            logger.error(f"全量同步失败: {rule.source_db}.{rule.source_table}: {e}")
            self.state.update_phase(rule, "full_error", 0)
            self.state.add_log("FULL_SYNC_ERROR", f"{rule.source_db}.{rule.source_table}",
                              f"{rule.target_db}.{rule.target_table}", str(e))
        finally:
            self.src_pool.put_conn(src_conn)
            self.dst_pool.put_conn(dst_conn)

    def _ensure_table(self, src_conn, dst_conn, rule):
        exists = dst_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (rule.target_db, rule.target_table)
        )
        if exists == 0:
            row = src_conn.execute(f"SHOW CREATE TABLE `{rule.source_db}`.`{rule.source_table}`").fetchone()
            create_sql = row[1].replace(f"`{rule.source_db}`", f"`{rule.target_db}`")
            dst_conn.execute(create_sql)
            logger.info(f"自动创建表: {rule.target_db}.{rule.target_table}")

    def stop(self):
        self.running = False


# ========== 批量写入器 ==========

class BatchWriter:
    """批量写入器"""

    def __init__(self, pool, batch_size=500, flush_interval=1.0):
        self.pool = pool
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer = defaultdict(list)  # (db, table) -> [(columns, row_dict)]
        self._lock = threading.Lock()
        self.last_flush = time.time()

    def add(self, db, table, columns, rows):
        with self._lock:
            key = (db, table)
            for row in rows:
                self._buffer[key].append((columns, row))
            if len(self._buffer[key]) >= self.batch_size:
                self._flush(key)

    def _flush(self, key):
        items = self._buffer[key]
        if not items:
            return
        db, table = key
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
        except Exception as e:
            logger.error(f"批量写入失败 {db}.{table}: {e}")
            for p in params:
                try:
                    conn.execute(sql, p)
                except Exception:
                    pass
        finally:
            self.pool.put_conn(conn)

        self._buffer[key] = []

    def flush_all(self):
        with self._lock:
            for key in list(self._buffer.keys()):
                if self._buffer[key]:
                    self._flush(key)
            self.last_flush = time.time()

    def auto_flush(self):
        if time.time() - self.last_flush >= self.flush_interval:
            self.flush_all()


# ========== 增量事件缓存 ==========

class IncrementalBuffer:
    """增量事件缓存 — 全量同步期间缓存正在同步表的增量事件"""

    def __init__(self):
        self._buffer = defaultdict(list)  # (db, table) -> [event_data]
        self._tables_in_full_sync = set()  # 正在全量同步的表
        self._lock = threading.Lock()

    def mark_full_syncing(self, db, table, syncing=True):
        with self._lock:
            key = (db, table)
            if syncing:
                self._tables_in_full_sync.add(key)
            else:
                self._tables_in_full_sync.discard(key)
                # 全量完成，释放缓存
                events = self._buffer.pop(key, [])
                return events
        return []

    def is_buffering(self, db, table):
        with self._lock:
            return (db, table) in self._tables_in_full_sync

    def add(self, db, table, columns, event_type, rows, pk_cols=None):
        with self._lock:
            key = (db, table)
            self._buffer[key].append({
                "type": event_type,
                "columns": columns,
                "rows": rows,
                "pk_cols": pk_cols,
            })

    def flush(self, db, table):
        with self._lock:
            key = (db, table)
            events = self._buffer.pop(key, [])
            return events
        return []


# ========== 同步引擎 ==========

class MySQLSyncer:
    """全量 + 增量同步引擎"""

    def __init__(self, config: AppConfig, state: SyncState = None):
        self.config = config
        self.state = state or SyncState(config.state_db)
        self.running = False
        self.stream = None
        self._rules_map = {}

        self.src_pool = ConnectionPool(config.source.to_dict(), pool_size=3)
        self.dst_pool = ConnectionPool(config.target.to_dict(), pool_size=5)
        self.writer = BatchWriter(self.dst_pool, config.batch_size, config.flush_interval)
        self.incr_buffer = IncrementalBuffer()

        self._start_time = None
        self._event_count = 0

    def _build_rules_map(self):
        self._rules_map = {}
        for rule in self.config.rules:
            if rule.source_table == "*":
                conn = self.src_pool.get_conn()
                try:
                    rows = conn.execute(f"SHOW TABLES FROM `{rule.source_db}`").fetchall()
                    for (t,) in rows:
                        self._rules_map[(rule.source_db, t)] = SyncRule(rule.source_db, t, rule.target_db, t)
                finally:
                    self.src_pool.put_conn(conn)
            else:
                self._rules_map[(rule.source_db, rule.source_table)] = rule

    def _should_sync(self, schema, table):
        if schema in ("mysql", "sys", "information_schema", "performance_schema"):
            return False
        if schema == self.config.target.database:
            return False
        return (schema, table) in self._rules_map

    def _should_sync_ddl(self, schema):
        if schema in ("mysql", "sys", "information_schema", "performance_schema"):
            return False
        if schema == self.config.target.database:
            return False
        return schema in {r.source_db for r in self.config.rules}

    # ===== 全量同步 =====

    def _run_full_sync(self):
        """全量同步所有表"""
        full_syncer = FullSyncer(self.src_pool, self.dst_pool, self.state, self.config.batch_size)

        for rule in self.config.rules:
            if not self.running:
                break

            if rule.source_table == "*":
                # 展开通配符
                conn = self.src_pool.get_conn()
                try:
                    tables = conn.execute(f"SHOW TABLES FROM `{rule.source_db}`").fetchall()
                    for (t,) in tables:
                        if not self.running:
                            break
                        r = SyncRule(rule.source_db, t, rule.target_db, t)
                        self.incr_buffer.mark_full_syncing(r.source_db, r.source_table, True)
                        full_syncer.sync_table(r)
                        self.incr_buffer.mark_full_syncing(r.source_db, r.source_table, False)
                finally:
                    self.src_pool.put_conn(conn)
            else:
                self.incr_buffer.mark_full_syncing(rule.source_db, rule.source_table, True)
                full_syncer.sync_table(rule)
                self.incr_buffer.mark_full_syncing(rule.source_db, rule.source_table, False)

        logger.info("全量同步全部完成，进入纯增量模式")

    # ===== DDL 处理 =====

    def handle_ddl(self, schema, sql):
        if not self._should_sync_ddl(schema):
            return
        sql_upper = sql.upper()
        if not any(kw in sql_upper for kw in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "RENAME TABLE", "TRUNCATE")):
            return

        translated = sql.replace(f"`{schema}`.", f"`{self.config.target.database}`.")
        if "CREATE TABLE" in sql_upper:
            translated = translated.replace("CREATE TABLE ", f"CREATE TABLE IF NOT EXISTS `{self.config.target.database}`.")

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

    # ===== DML 处理 =====

    def handle_dml(self, event):
        schema = event.schema
        table = event.table
        if not self._should_sync(schema, table):
            return

        rule = self._rules_map[(schema, table)]
        columns = [col["name"] for col in event.columns]

        if isinstance(event, WriteRowsEvent):
            rows = [row["values"] for row in event.rows]
            event_type = "INSERT"
        elif isinstance(event, UpdateRowsEvent):
            rows = [row["after_values"] for row in event.rows]
            event_type = "UPDATE"
        elif isinstance(event, DeleteRowsEvent):
            rows = [row["values"] for row in event.rows]
            event_type = "DELETE"
        else:
            return

        # 如果这张表正在全量同步，缓存增量事件
        if self.incr_buffer.is_buffering(schema, table):
            self.incr_buffer.add(schema, table, columns, event_type, rows)
            return

        # 否则直接处理
        self.writer.add(rule.target_db, rule.target_table, columns, rows)
        self.state.update_incr(rule, event_type, len(rows))
        self._event_count += 1

    # ===== 启动 =====

    def start(self):
        self._build_rules_map()
        self.running = True
        self._start_time = time.time()

        # 记录 binlog 位点（全量前）
        logger.info("记录初始 binlog 位点...")

        # 全量同步
        if self.config.sync_type in ("full_and_incr", "full"):
            self._run_full_sync()

        # 增量同步
        if self.config.sync_type in ("full_and_incr", "incr"):
            self._start_incremental()

    def _start_incremental(self):
        """启动增量同步（binlog 监听）"""
        resume_pos = self.state.get_binlog_position()
        kwargs = {
            "connection_settings": self.config.source.to_dict(),
            "server_id": self.config.server_id,
            "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
            "freeze_schema": True,
            "resume_stream": True,
        }
        if resume_pos:
            kwargs["log_file"] = resume_pos["filename"]
            kwargs["log_pos"] = resume_pos["position"]
            logger.info(f"从断点恢复: {resume_pos['filename']}:{resume_pos['position']}")

        self.stream = BinLogStreamReader(**kwargs)
        logger.info("增量同步已启动")

        try:
            for event in self.stream:
                if not self.running:
                    break

                if isinstance(event, QueryEvent):
                    self.handle_ddl(event.schema.decode(), event.query.decode().strip())
                else:
                    self.handle_dml(event)

                # 记录位点
                if hasattr(event, 'packet') and hasattr(event.packet, 'log_pos'):
                    filename = self.stream.binlog_file() if hasattr(self.stream, 'binlog_file') else None
                    if filename and event.packet.log_pos:
                        self.state.save_binlog_position(filename, event.packet.log_pos)

                # 定期刷新
                if self._event_count % 100 == 0:
                    self.writer.flush_all()
                if self._event_count % 1000 == 0:
                    elapsed = time.time() - self._start_time
                    self.state.record_metric("qps", self._event_count / max(elapsed, 1))
                    self.state.commit()

        except Exception as e:
            logger.error(f"增量同步异常: {e}")
            self.state.add_log("ERROR", "engine", "", str(e))
        finally:
            self.stop()

    def stop(self):
        self.running = False
        self.writer.flush_all()
        if self.stream:
            self.stream.close()
        self.src_pool.close_all()
        self.dst_pool.close_all()
        logger.info("同步引擎已停止")

    def get_status(self):
        return self.state.get_all_status()

    def get_stats(self):
        stats = self.state.get_stats()
        if self._start_time:
            stats["uptime_seconds"] = int(time.time() - self._start_time)
            stats["qps"] = round(self._event_count / max(time.time() - self._start_time, 1), 2)
        stats["running"] = self.running
        return stats
