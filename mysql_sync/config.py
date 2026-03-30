"""
配置管理 — 全部存 SQLite，不需要配置文件
"""
import os
import sqlite3
from dataclasses import dataclass, field
from typing import List


@dataclass
class MySQLConfig:
    host: str = ""
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""

    def to_dict(self):
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "passwd": self.password,
            "database": self.database or None,
        }


@dataclass
class SyncRule:
    source_db: str
    source_table: str
    target_db: str
    target_table: str = ""


@dataclass
class AppConfig:
    source: MySQLConfig = field(default_factory=MySQLConfig)
    target: MySQLConfig = field(default_factory=MySQLConfig)
    rules: List[SyncRule] = field(default_factory=list)
    server_id: int = 100
    sync_interval: int = 1
    log_level: str = "INFO"
    web_port: int = 8520
    web_host: str = "0.0.0.0"
    state_db: str = "sync_state.db"
    batch_size: int = 500
    flush_interval: float = 1.0
    pool_size: int = 5
    sync_type: str = "full_and_incr"

    @property
    def configured(self) -> bool:
        """是否有有效配置"""
        return bool(self.source.host and self.target.host and self.rules)


def _init_config_table(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sync_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            source_host TEXT DEFAULT '',
            source_port INTEGER DEFAULT 3306,
            source_user TEXT DEFAULT 'root',
            source_password TEXT DEFAULT '',
            source_database TEXT DEFAULT '',
            target_host TEXT DEFAULT '',
            target_port INTEGER DEFAULT 3306,
            target_user TEXT DEFAULT 'root',
            target_password TEXT DEFAULT '',
            target_database TEXT DEFAULT '',
            server_id INTEGER DEFAULT 100,
            batch_size INTEGER DEFAULT 500,
            flush_interval REAL DEFAULT 1.0,
            pool_size INTEGER DEFAULT 5,
            sync_type TEXT DEFAULT 'full_and_incr',
            log_level TEXT DEFAULT 'INFO',
            web_port INTEGER DEFAULT 8520,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS sync_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db TEXT NOT NULL,
            source_table TEXT NOT NULL,
            target_db TEXT NOT NULL,
            target_table TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # 确保有默认行
    conn.execute("INSERT OR IGNORE INTO sync_config (id) VALUES (1)")
    conn.commit()
    conn.close()


def load_config(db_path: str = "sync_state.db") -> AppConfig:
    """从 SQLite 加载配置"""
    _init_config_table(db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    row = conn.execute("SELECT * FROM sync_config WHERE id=1").fetchone()
    config = AppConfig()

    if row:
        config.source = MySQLConfig(
            host=row["source_host"] or "",
            port=row["source_port"] or 3306,
            user=row["source_user"] or "root",
            password=row["source_password"] or "",
            database=row["source_database"] or "",
        )
        config.target = MySQLConfig(
            host=row["target_host"] or "",
            port=row["target_port"] or 3306,
            user=row["target_user"] or "root",
            password=row["target_password"] or "",
            database=row["target_database"] or "",
        )
        config.server_id = row["server_id"] or 100
        config.batch_size = row["batch_size"] or 500
        config.flush_interval = row["flush_interval"] or 1.0
        config.pool_size = row["pool_size"] or 5
        config.sync_type = row["sync_type"] or "full_and_incr"
        config.log_level = row["log_level"] or "INFO"
        config.web_port = row["web_port"] or 8520

    rules = conn.execute("SELECT source_db, source_table, target_db, target_table FROM sync_rules").fetchall()
    config.rules = [SyncRule(r["source_db"], r["source_table"], r["target_db"], r["target_table"]) for r in rules]

    conn.close()
    return config


def save_config(config: AppConfig, db_path: str = "sync_state.db"):
    """保存配置到 SQLite"""
    _init_config_table(db_path)
    conn = sqlite3.connect(db_path)

    conn.execute("""
        UPDATE sync_config SET
            source_host=?, source_port=?, source_user=?, source_password=?, source_database=?,
            target_host=?, target_port=?, target_user=?, target_password=?, target_database=?,
            server_id=?, batch_size=?, flush_interval=?, pool_size=?,
            sync_type=?, log_level=?, web_port=?, updated_at=CURRENT_TIMESTAMP
        WHERE id=1
    """, (
        config.source.host, config.source.port, config.source.user, config.source.password, config.source.database,
        config.target.host, config.target.port, config.target.user, config.target.password, config.target.database,
        config.server_id, config.batch_size, config.flush_interval, config.pool_size,
        config.sync_type, config.log_level, config.web_port,
    ))

    conn.execute("DELETE FROM sync_rules")
    for r in config.rules:
        conn.execute(
            "INSERT INTO sync_rules (source_db, source_table, target_db, target_table) VALUES (?,?,?,?)",
            (r.source_db, r.source_table, r.target_db, r.target_table),
        )

    conn.commit()
    conn.close()
