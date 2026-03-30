"""
MySQL Sync 启动入口
支持 CLI 模式和 Web 模式
"""
import sys
import argparse
import logging
from mysql_sync.config import load_config, save_config, AppConfig
from mysql_sync.sync import MySQLSyncer, SyncState


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_web(args):
    """Web 管理模式（默认）"""
    config = load_config(args.db)
    setup_logging(config.log_level)

    from mysql_sync.web_app import create_app
    app = create_app(config, args.db)

    host = args.host or config.web_host
    port = args.port or config.web_port

    print(f"Web 管理界面: http://{host}:{port}")
    app.run(host=host, port=port, debug=False)


def cmd_sync(args):
    """纯 CLI 同步模式"""
    config = load_config(args.db)
    setup_logging(config.log_level)

    if not config.rules:
        print("错误：没有同步规则，请先通过 Web 界面配置")
        sys.exit(1)

    syncer = MySQLSyncer(config)
    print(f"启动同步，共 {len(config.rules)} 条规则...")
    syncer.start()


def cmd_version(args):
    from mysql_sync import __version__, __author__
    print(f"MySync v{__version__}")


def main():
    parser = argparse.ArgumentParser(description="MySQL Sync — MySQL 跨库同步工具")
    parser.add_argument("--db", default="sync_state.db", help="状态数据库路径 (默认: sync_state.db)")
    parser.add_argument("--host", help="Web 监听地址")
    parser.add_argument("--port", type=int, help="Web 监听端口")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("web", help="启动 Web 管理界面")
    subparsers.add_parser("sync", help="启动同步 (CLI 模式)")
    subparsers.add_parser("version", help="显示版本")

    args = parser.parse_args()

    if args.command == "sync":
        cmd_sync(args)
    elif args.command == "version":
        cmd_version(args)
    else:
        cmd_web(args)


if __name__ == "__main__":
    main()
