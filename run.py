"""
MySQL Sync 启动入口
支持 CLI 模式和 Web 模式
"""
import sys
import argparse
import logging
from mysql_sync.config import load_config
from mysql_sync.sync import MySQLSyncer, SyncState


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_sync(args):
    """纯 CLI 同步模式"""
    config = load_config(args.config)
    setup_logging(config.log_level)

    if not config.rules:
        print("错误：配置文件中没有同步规则，请先在 config.yaml 中添加 rules")
        sys.exit(1)

    syncer = MySQLSyncer(config)
    print(f"启动同步，共 {len(config.rules)} 条规则...")
    syncer.start()


def cmd_web(args):
    """Web 管理模式"""
    config = load_config(args.config)
    setup_logging(config.log_level)

    from mysql_sync.web_app import create_app
    app = create_app(config)

    host = args.host or config.web_host
    port = args.port or config.web_port

    print(f"Web 管理界面: http://{host}:{port}")
    app.run(host=host, port=port, debug=False)


def cmd_status(args):
    """查看同步状态"""
    config = load_config(args.config)
    state = SyncState(config.state_db)

    statuses = state.get_all_status()
    if not statuses:
        print("暂无同步记录")
        return

    print(f"{'源表':<25} {'目标表':<25} {'状态':<10} {'最近事件':<10} {'同步行数':<10}")
    print("-" * 85)
    for s in statuses:
        source = f"{s[0]}.{s[1]}"
        target = f"{s[2]}.{s[3]}"
        print(f"{source:<25} {target:<25} {s[4]:<10} {s[5] or '-':<10} {s[8]:<10}")


def cmd_version(args):
    """显示版本"""
    from mysql_sync import __version__, __author__
    print(f"MySQL Sync v{__version__}")
    print(f"Author: {__author__}")
    print(f"License: MIT")


def main():
    parser = argparse.ArgumentParser(
        description="MySQL Sync — 轻量级 MySQL 跨库/跨实例表同步工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-c", "--config", default="config.yaml", help="配置文件路径 (默认: config.yaml)")

    subparsers = parser.add_subparsers(dest="command")

    # sync 命令
    sync_parser = subparsers.add_parser("sync", help="启动同步（CLI模式）")

    # web 命令
    web_parser = subparsers.add_parser("web", help="启动 Web 管理界面")
    web_parser.add_argument("--host", help="监听地址 (默认: 0.0.0.0)")
    web_parser.add_argument("--port", type=int, help="监听端口 (默认: 8520)")

    # status 命令
    status_parser = subparsers.add_parser("status", help="查看同步状态")

    # version 命令
    version_parser = subparsers.add_parser("version", help="显示版本")

    args = parser.parse_args()

    if args.command == "sync":
        cmd_sync(args)
    elif args.command == "web":
        cmd_web(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "version":
        cmd_version(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
