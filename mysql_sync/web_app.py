"""
Web 管理界面 — 基于 Flask
支持：任务管理 | 监控大屏 | 告警配置 | API 接口
配置全部存 SQLite，不需要配置文件
"""
import json
import threading
import logging
from flask import Flask, render_template, jsonify, request

from .config import AppConfig, MySQLConfig, SyncRule, save_config
from .sync import MySQLSyncer, SyncState

logger = logging.getLogger("mysql_sync")

syncer = None
sync_thread = None


def create_app(config: AppConfig, db_path: str = "sync_state.db") -> Flask:
    app = Flask(
        __name__,
        template_folder="../templates",
        static_folder="../static"
    )
    state = SyncState(config.state_db)

    @app.route("/")
    def index():
        resp = app.make_response(render_template("index.html"))
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return resp

    # ===== 配置管理 =====

    @app.route("/api/config", methods=["GET"])
    def get_config():
        return jsonify({
            "configured": config.configured,
            "source": {
                "host": config.source.host,
                "port": config.source.port,
                "user": config.source.user,
                "database": config.source.database,
            },
            "target": {
                "host": config.target.host,
                "port": config.target.port,
                "user": config.target.user,
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
            "batch_size": config.batch_size,
            "flush_interval": config.flush_interval,
            "pool_size": config.pool_size,
            "sync_type": config.sync_type,
        })

    @app.route("/api/config", methods=["POST"])
    def update_config():
        global syncer, sync_thread
        data = request.json

        if "source" in data:
            s = data["source"]
            config.source = MySQLConfig(
                host=s.get("host", ""),
                port=s.get("port", 3306),
                user=s.get("user", "root"),
                password=s.get("password", ""),
                database=s.get("database", ""),
            )

        if "target" in data:
            t = data["target"]
            config.target = MySQLConfig(
                host=t.get("host", ""),
                port=t.get("port", 3306),
                user=t.get("user", "root"),
                password=t.get("password", ""),
                database=t.get("database", ""),
            )

        if "rules" in data:
            config.rules = [
                SyncRule(
                    source_db=r["source_db"],
                    source_table=r["source_table"],
                    target_db=r.get("target_db", config.target.database),
                    target_table=r.get("target_table", r["source_table"]),
                )
                for r in data["rules"]
            ]

        config.batch_size = data.get("batch_size", config.batch_size)
        config.flush_interval = data.get("flush_interval", config.flush_interval)
        config.pool_size = data.get("pool_size", config.pool_size)
        config.sync_type = data.get("sync_type", config.sync_type)
        config.server_id = data.get("server_id", config.server_id)

        # 保存到 SQLite
        save_config(config, db_path)

        # 停止旧的同步
        if syncer and sync_thread and sync_thread.is_alive():
            syncer.stop()
            syncer = None
            sync_thread = None

        # 重新加载 SyncState
        state_db = config.state_db
        state.__init__(state_db)

        return jsonify({"ok": True})

    # ===== 规则管理 =====

    @app.route("/api/rules", methods=["GET"])
    def get_rules():
        return jsonify([
            {
                "source_db": r.source_db,
                "source_table": r.source_table,
                "target_db": r.target_db,
                "target_table": r.target_table,
            }
            for r in config.rules
        ])

    @app.route("/api/rules", methods=["POST"])
    def add_rule():
        data = request.json
        rule = SyncRule(
            source_db=data["source_db"],
            source_table=data["source_table"],
            target_db=data.get("target_db", config.target.database),
            target_table=data.get("target_table", data["source_table"]),
        )
        config.rules.append(rule)
        save_config(config, db_path)
        return jsonify({"ok": True})

    @app.route("/api/rules/<int:index>", methods=["DELETE"])
    def delete_rule(index):
        if 0 <= index < len(config.rules):
            config.rules.pop(index)
            save_config(config, db_path)
            return jsonify({"ok": True})
        return jsonify({"error": "index out of range"}), 400

    # ===== 同步控制 =====

    @app.route("/api/sync/start", methods=["POST"])
    def start_sync():
        global syncer, sync_thread
        if syncer and sync_thread and sync_thread.is_alive():
            return jsonify({"error": "同步已在运行"}), 400

        if not config.configured:
            return jsonify({"error": "请先配置数据库连接和同步规则"}), 400

        syncer = MySQLSyncer(config, state)
        sync_thread = threading.Thread(target=syncer.start, daemon=True)
        sync_thread.start()
        return jsonify({"ok": True, "message": "同步已启动"})

    @app.route("/api/sync/stop", methods=["POST"])
    def stop_sync():
        global syncer, sync_thread
        if syncer:
            syncer.stop()
            syncer = None
            sync_thread = None
            return jsonify({"ok": True, "message": "同步已停止"})
        return jsonify({"error": "同步未运行"}), 400

    @app.route("/api/sync/reset", methods=["POST"])
    def reset_sync():
        """重置同步状态（清空配置和状态，重新开始）"""
        global syncer, sync_thread
        if syncer:
            syncer.stop()
            syncer = None
            sync_thread = None

        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM sync_rules")
        conn.execute("""UPDATE sync_config SET
            source_host='', source_port=3306, source_user='root', source_password='', source_database='',
            target_host='', target_port=3306, target_user='root', target_password='', target_database='',
            updated_at=CURRENT_TIMESTAMP WHERE id=1""")
        conn.execute("DELETE FROM sync_status")
        conn.execute("DELETE FROM sync_log")
        conn.execute("DELETE FROM full_sync_progress")
        conn.execute("DELETE FROM binlog_position")
        conn.commit()
        conn.close()

        config.source = MySQLConfig()
        config.target = MySQLConfig()
        config.rules = []

        return jsonify({"ok": True, "message": "已重置"})

    @app.route("/api/sync/status", methods=["GET"])
    def sync_status():
        is_running = syncer is not None and sync_thread is not None and sync_thread.is_alive()
        statuses = state.get_all_status()
        stats = syncer.get_stats() if syncer else state.get_stats()
        return jsonify({
            "running": is_running,
            "configured": config.configured,
            "stats": stats,
            "rules": [
                {
                    "source": f"{s[0]}.{s[1]}",
                    "target": f"{s[2]}.{s[3]}",
                    "status": s[4],
                    "phase": s[5],
                    "last_event": s[6],
                    "last_update": s[7],
                    "error_count": s[8],
                    "total_rows": s[9],
                    "full_sync_rows": s[10] if len(s) > 10 else 0,
                    "incr_sync_rows": s[11] if len(s) > 11 else 0,
                    "progress": s[12] if len(s) > 12 else 0,
                }
                for s in statuses
            ]
        })

    @app.route("/api/sync/logs", methods=["GET"])
    def sync_logs():
        limit = request.args.get("limit", 100, type=int)
        logs_list = state.get_recent_logs(limit)
        return jsonify([
            {
                "event_type": l[0],
                "source": l[1],
                "target": l[2],
                "message": l[3],
                "created_at": l[4],
            }
            for l in logs_list
        ])

    # ===== 监控指标 =====

    @app.route("/api/metrics/<name>", methods=["GET"])
    def get_metrics(name):
        limit = request.args.get("limit", 60, type=int)
        data = state.get_metrics(name, limit)
        return jsonify([
            {"value": d[0], "time": d[1]} for d in data
        ])

    @app.route("/api/stats", methods=["GET"])
    def get_stats():
        if syncer and sync_thread and sync_thread.is_alive():
            return jsonify(syncer.get_stats())
        return jsonify(state.get_stats())

    # ===== 数据库探测 =====

    @app.route("/api/probe/tables", methods=["POST"])
    def probe_tables():
        import pymysql
        data = request.json
        try:
            conn = pymysql.connect(
                host=data.get("host", ""),
                port=data.get("port", 3306),
                user=data.get("user", "root"),
                password=data.get("password", ""),
            )
            cur = conn.cursor()
            databases = []
            cur.execute("SHOW DATABASES")
            for (db_name,) in cur.fetchall():
                if db_name in ("mysql", "sys", "information_schema", "performance_schema"):
                    continue
                cur.execute(f"SHOW TABLES FROM `{db_name}`")
                tables = [t[0] for t in cur.fetchall()]
                databases.append({"name": db_name, "tables": tables})
            cur.close()
            conn.close()
            return jsonify({"databases": databases})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ===== 健康检查 =====

    @app.route("/api/health", methods=["GET"])
    def health():
        is_running = syncer is not None and sync_thread is not None and sync_thread.is_alive()
        return jsonify({
            "status": "healthy" if is_running else "stopped",
            "running": is_running,
            "configured": config.configured,
        })

    return app
