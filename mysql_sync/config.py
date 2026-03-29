"""
配置管理 — 从 YAML 文件或环境变量读取配置
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class MySQLConfig:
    host: str = "127.0.0.1"
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
    target_table: str


@dataclass
class AppConfig:
    # 源库
    source: MySQLConfig = field(default_factory=MySQLConfig)
    # 目标库（可以跟源库同一个实例）
    target: MySQLConfig = field(default_factory=lambda: MySQLConfig(database="Z"))
    # 同步规则列表
    rules: List[SyncRule] = field(default_factory=list)
    # 通用配置
    server_id: int = 100
    sync_interval: int = 1  # DDL 检查间隔（秒）
    log_level: str = "INFO"
    web_port: int = 8520
    web_host: str = "0.0.0.0"
    # 数据库文件（存储同步状态）
    state_db: str = "sync_state.db"


def load_config(config_path: str = "config.yaml") -> AppConfig:
    """从 YAML 文件加载配置"""
    if not os.path.exists(config_path):
        return AppConfig()

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    config = AppConfig()

    # 解析源库
    if "source" in data:
        src = data["source"]
        config.source = MySQLConfig(
            host=src.get("host", "127.0.0.1"),
            port=src.get("port", 3306),
            user=src.get("user", "root"),
            password=src.get("password", ""),
            database=src.get("database", ""),
        )

    # 解析目标库
    if "target" in data:
        tgt = data["target"]
        config.target = MySQLConfig(
            host=tgt.get("host", "127.0.0.1"),
            port=tgt.get("port", 3306),
            user=tgt.get("user", "root"),
            password=tgt.get("password", ""),
            database=tgt.get("database", "Z"),
        )

    # 解析同步规则
    if "rules" in data:
        for rule in data["rules"]:
            config.rules.append(SyncRule(
                source_db=rule["source_db"],
                source_table=rule["source_table"],
                target_db=rule.get("target_db", config.target.database),
                target_table=rule.get("target_table", rule["source_table"]),
            ))

    # 通用配置
    config.server_id = data.get("server_id", 100)
    config.sync_interval = data.get("sync_interval", 1)
    config.log_level = data.get("log_level", "INFO")
    config.web_port = data.get("web_port", 8520)
    config.web_host = data.get("web_host", "0.0.0.0")
    config.state_db = data.get("state_db", "sync_state.db")

    # 环境变量覆盖
    config.source.password = os.getenv("SOURCE_PASSWORD", config.source.password)
    config.target.password = os.getenv("TARGET_PASSWORD", config.target.password)
    config.web_port = int(os.getenv("WEB_PORT", config.web_port))

    return config


def save_config(config: AppConfig, config_path: str = "config.yaml"):
    """保存配置到 YAML 文件"""
    data = {
        "source": {
            "host": config.source.host,
            "port": config.source.port,
            "user": config.source.user,
            "password": config.source.password,
            "database": config.source.database,
        },
        "target": {
            "host": config.target.host,
            "port": config.target.port,
            "user": config.target.user,
            "password": config.target.password,
            "database": config.target.database,
        },
        "rules": [
            {
                "source_db": r.source_db,
                "source_table": r.source_table,
                "target_db": r.target_db,
                "target_table": r.target_table,
            }
            for r in config.rules
        ],
        "server_id": config.server_id,
        "sync_interval": config.sync_interval,
        "log_level": config.log_level,
        "web_port": config.web_port,
        "web_host": config.web_host,
        "state_db": config.state_db,
    }

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
