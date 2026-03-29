"""
Web 管理界面 — 基于 Flask
"""
import json
import threading
import logging
from flask import Flask, render_template, jsonify, request

from .config import AppConfig, SyncRule, save_config
from .sync import MySQLSyncer, SyncState

logger = logging.getLogger("mysql_sync")

syncer = None
sync_thread = None


def create_app(config: AppConfig) -> Flask:
    app = Flask(
        __name__,
        template_folder="../../templates",
        static_folder="../../static"
    )
    state = SyncState(config.state_db)

    @app.route("/")
    def index():
        return render_template("index.html")

    # ===== 配置管理 =====

    @app.route("/api/config", methods=["GET"])
    def get_config():
        return jsonify({
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
            "web_port": config.web_port,
        })

    @app.route("/api/config", methods=["POST"])
    def update_config():
        data = request.json
        if "source" in data:
            s = data["source"]
            config.source.host = s.get("host", config.source.host)
            config.source.port = s.get("port", config.source.port)
            config.source.user = s.get("user", config.source.user)
            config.source.password = s.get("password", config.source.password)
            config.source.database = s.get("database", config.source.database)

        if "target" in data:
            t = data["target"]
            config.target.host = t.get("host", config.target.host)
            config.target.port = t.get("port", config.target.port)
            config.target.user = t.get("user", config.target.user)
            config.target.password = t.get("password", config.target.password)
            config.target.database = t.get("database", config.target.database)

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

        save_config(config)
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
        save_config(config)
        return jsonify({"ok": True})

    @app.route("/api/rules/<int:index>", methods=["DELETE"])
    def delete_rule(index):
        if 0 <= index < len(config.rules):
            config.rules.pop(index)
            save_config(config)
            return jsonify({"ok": True})
        return jsonify({"error": "index out of range"}), 400

    # ===== 同步控制 =====

    @app.route("/api/sync/start", methods=["POST"])
    def start_sync():
        global syncer, sync_thread
        if syncer and sync_thread and sync_thread.is_alive():
            return jsonify({"error": "同步已在运行"}), 400

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

    @app.route("/api/sync/status", methods=["GET"])
    def sync_status():
        is_running = syncer is not None and sync_thread is not None and sync_thread.is_alive()
        statuses = state.get_all_status()
        return jsonify({
            "running": is_running,
            "rules": [
                {
                    "source": f"{s[0]}.{s[1]}",
                    "target": f"{s[2]}.{s[3]}",
                    "status": s[4],
                    "last_event": s[5],
                    "last_update": s[6],
                    "error_count": s[7],
                    "total_rows": s[8],
                }
                for s in statuses
            ]
        })

    @app.route("/api/sync/logs", methods=["GET"])
    def sync_logs():
        limit = request.args.get("limit", 100, type=int)
        logs = state.get_recent_logs(limit)
        return jsonify([
            {
                "event_type": l[0],
                "source": l[1],
                "target": l[2],
                "message": l[3],
                "created_at": l[4],
            }
            for l in logs
        ])

    # ===== 数据库探测 =====

    @app.route("/api/probe/tables", methods=["POST"])
    def probe_tables():
        """探测源库有哪些表"""
        import pymysql
        data = request.json
        try:
            conn = pymysql.connect(
                host=data.get("host", config.source.host),
                port=data.get("port", config.source.port),
                user=data.get("user", config.source.user),
                password=data.get("password", config.source.password),
            )
            databases = []
            rows = conn.execute("SHOW DATABASES").fetchall()
            for (db_name,) in rows:
                if db_name in ("mysql", "sys", "information_schema", "performance_schema"):
                    continue
                conn.execute(f"USE `{db_name}`")
                tables = conn.execute("SHOW TABLES").fetchall()
                databases.append({
                    "name": db_name,
                    "tables": [t[0] for t in tables],
                })
            conn.close()
            return jsonify({"databases": databases})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return app
