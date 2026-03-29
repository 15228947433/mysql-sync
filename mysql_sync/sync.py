"""
核心同步逻辑 — 基于 Binlog 的实时同步
"""
import logging
import time
import sqlite3
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent

from .config import AppConfig, SyncRule

logger = logging.getLogger("mysql_sync")


class SyncState:
    """同步状态管理（用 SQLite 存储）"""

    def __init__(self, db_path: str = "sync_state.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
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
        """)
        self.conn.commit()

    def update_status(self, rule: SyncRule, event_type: str, rows: int = 0):
        key = (rule.source_db, rule.source_table, rule.target_db, rule.target_table)
        self.conn.execute("""
            INSERT INTO sync_status (source_db, source_table, target_db, target_table, status, last_event, total_rows)
            VALUES (?, ?, ?, ?, 'running', ?, ?)
            ON CONFLICT(source_db, source_table, target_db, target_table)
            DO UPDATE SET last_event=?, total_rows=total_rows+?, last_update=CURRENT_TIMESTAMP
        """, (*key, event_type, rows, event_type, rows))
        self.conn.commit()

    def add_log(self, event_type: str, source: str, target: str, message: str):
        self.conn.execute(
            "INSERT INTO sync_log (event_type, source, target, message) VALUES (?, ?, ?, ?)",
            (event_type, source, target, message)
        )
        self.conn.commit()

    def get_all_status(self):
        cursor = self.conn.execute(
            "SELECT source_db, source_table, target_db, target_table, status, last_event, last_update, error_count, total_rows "
            "FROM sync_status ORDER BY id"
        )
        return cursor.fetchall()

    def get_recent_logs(self, limit=100):
        cursor = self.conn.execute(
            "SELECT event_type, source, target, message, created_at FROM sync_log ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return cursor.fetchall()

    def close(self):
        self.conn.close()


class MySQLSyncer:
    """MySQL Binlog 同步器"""

    def __init__(self, config: AppConfig, state: SyncState = None):
        self.config = config
        self.state = state or SyncState(config.state_db)
        self.running = False
        self.stream = None
        self._rules_map = {}  # (db, table) -> SyncRule

    def _build_rules_map(self):
        """构建规则映射表，展开通配符"""
        self._rules_map = {}
        for rule in self.config.rules:
            if rule.source_table == "*":
                # 通配符：获取源库所有表
                src_conn = self._get_src_conn()
                try:
                    rows = src_conn.execute(f"SHOW TABLES FROM `{rule.source_db}`").fetchall()
                    for (table_name,) in rows:
                        key = (rule.source_db, table_name)
                        self._rules_map[key] = SyncRule(
                            source_db=rule.source_db,
                            source_table=table_name,
                            target_db=rule.target_db,
                            target_table=table_name,
                        )
                finally:
                    src_conn.close()
            else:
                key = (rule.source_db, rule.source_table)
                self._rules_map[key] = rule

    def _should_sync(self, schema: str, table: str) -> bool:
        """判断是否需要同步"""
        if schema in ("mysql", "sys", "information_schema", "performance_schema"):
            return False
        if schema == self.config.target.database:
            return False
        return (schema, table) in self._rules_map

    def _get_src_conn(self):
        return pymysql.connect(**self.config.source.to_dict())

    def _get_dst_conn(self):
        return pymysql.connect(**self.config.target.to_dict())

    def ensure_table_exists(self, rule: SyncRule):
        """目标表不存在就自动创建"""
        dst_conn = self._get_dst_conn()
        try:
            exists = dst_conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (rule.target_db, rule.target_table)
            )
            if exists == 0:
                src_conn = self._get_src_conn()
                row = src_conn.execute(
                    f"SHOW CREATE TABLE `{rule.source_db}`.`{rule.source_table}`"
                ).fetchone()
                create_sql = row[1]
                create_sql = create_sql.replace(
                    f"`{rule.source_db}`", f"`{rule.target_db}`"
                )
                dst_conn.execute(create_sql)
                src_conn.close()
                logger.info(f"自动创建表: {rule.target_db}.{rule.target_table}")
                self.state.add_log("DDL", f"{rule.source_db}.{rule.source_table}",
                                   f"{rule.target_db}.{rule.target_table}", "自动创建表")
        finally:
            dst_conn.close()

    def handle_ddl(self, schema: str, sql: str):
        """处理 DDL 事件"""
        if not self._should_sync_for_ddl(schema):
            return

        sql_upper = sql.upper()
        if not any(kw in sql_upper for kw in (
            "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "RENAME TABLE", "TRUNCATE"
        )):
            return

        # 替换库名
        translated = sql.replace(f"`{schema}`.", f"`{self.config.target.database}`.")
        if "CREATE TABLE" in sql_upper:
            translated = translated.replace(
                "CREATE TABLE ", f"CREATE TABLE IF NOT EXISTS `{self.config.target.database}`."
            )

        dst_conn = self._get_dst_conn()
        try:
            dst_conn.ping(reconnect=True)
            dst_conn.execute(translated)
            logger.info(f"DDL 同步: {sql[:80]}")
            self.state.add_log("DDL", schema, self.config.target.database, sql[:200])
        except Exception as e:
            logger.error(f"DDL 同步失败: {e}")
            self.state.add_log("DDL_ERROR", schema, self.config.target.database, str(e))
        finally:
            dst_conn.close()

    def _should_sync_for_ddl(self, schema: str) -> bool:
        if schema in ("mysql", "sys", "information_schema", "performance_schema"):
            return False
        if schema == self.config.target.database:
            return False
        return schema in {r.source_db for r in self.config.rules}

    def handle_dml(self, event):
        """处理 DML 事件"""
        schema = event.schema
        table = event.table

        if not self._should_sync(schema, table):
            return

        rule = self._rules_map[(schema, table)]
        self.ensure_table_exists(rule)

        columns = [col["name"] for col in event.columns]
        dst_conn = self._get_dst_conn()

        try:
            dst_conn.ping(reconnect=True)

            if isinstance(event, WriteRowsEvent):
                self._handle_insert(dst_conn, rule, columns, event.rows)
            elif isinstance(event, UpdateRowsEvent):
                self._handle_update(dst_conn, rule, columns, event.rows, event)
            elif isinstance(event, DeleteRowsEvent):
                self._handle_delete(dst_conn, rule, columns, event.rows, event)

        except Exception as e:
            logger.error(f"DML 同步失败 {schema}.{table}: {e}")
            self.state.add_log("DML_ERROR", f"{schema}.{table}",
                               f"{rule.target_db}.{rule.target_table}", str(e))
        finally:
            dst_conn.close()

    def _handle_insert(self, conn, rule, columns, rows):
        col_str = ",".join([f"`{c}`" for c in columns])
        placeholders = ",".join(["%s"] * len(columns))
        update_str = ",".join([f"`{c}`=VALUES(`{c}`)" for c in columns])
        sql = f"INSERT INTO `{rule.target_db}`.`{rule.target_table}` ({col_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_str}"

        for row in rows:
            vals = [row["values"].get(c) for c in columns]
            conn.execute(sql, vals)

        self.state.update_status(rule, "INSERT", len(rows))

    def _handle_update(self, conn, rule, columns, rows, event):
        pk_cols = [col["name"] for col in event.columns if col.get("is_primary")]
        if not pk_cols:
            pk_cols = [columns[0]]

        for row in rows:
            new_vals = [row["after_values"].get(c) for c in columns]
            where_vals = [row["after_values"].get(c) for c in pk_cols]
            set_str = ",".join([f"`{c}`=%s" for c in columns])
            where_str = " AND ".join([f"`{c}`=%s" for c in pk_cols])
            conn.execute(
                f"UPDATE `{rule.target_db}`.`{rule.target_table}` SET {set_str} WHERE {where_str}",
                new_vals + where_vals
            )

        self.state.update_status(rule, "UPDATE", len(rows))

    def _handle_delete(self, conn, rule, columns, rows, event):
        pk_cols = [col["name"] for col in event.columns if col.get("is_primary")]
        if not pk_cols:
            pk_cols = [columns[0]]

        for row in rows:
            where_vals = [row["values"].get(c) for c in pk_cols]
            where_str = " AND ".join([f"`{c}`=%s" for c in pk_cols])
            conn.execute(
                f"DELETE FROM `{rule.target_db}`.`{rule.target_table}` WHERE {where_str}",
                where_vals
            )

        self.state.update_status(rule, "DELETE", len(rows))

    def start(self):
        """启动同步"""
        self._build_rules_map()
        self.running = True

        # 预先确保所有目标表存在
        for rule in self.config.rules:
            self.ensure_table_exists(rule)

        logger.info("启动 Binlog 监听...")

        self.stream = BinLogStreamReader(
            connection_settings=self.config.source.to_dict(),
            server_id=self.config.server_id,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
            freeze_schema=True,
        )

        try:
            for event in self.stream:
                if not self.running:
                    break

                if isinstance(event, QueryEvent):
                    self.handle_ddl(event.schema.decode(), event.query.decode().strip())
                else:
                    self.handle_dml(event)
        except Exception as e:
            logger.error(f"同步异常: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """停止同步"""
        self.running = False
        if self.stream:
            self.stream.close()
        logger.info("同步已停止")

    def get_status(self):
        """获取同步状态"""
        return self.state.get_all_status()

    def get_logs(self, limit=100):
        """获取同步日志"""
        return self.state.get_recent_logs(limit)
