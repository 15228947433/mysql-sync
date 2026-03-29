#!/bin/bash
# MySQL Sync 一键安装脚本
# 用法: curl -sSL https://raw.githubusercontent.com/15228947433/mysql-sync/main/install.sh | bash

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "╔══════════════════════════════════════════╗"
echo "║      MySQL Sync — 跨库同步工具安装        ║"
echo "╚══════════════════════════════════════════╝"
echo -e "${NC}"

# 检查 Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}错误: 未找到 python3，请先安装 Python 3.9+${NC}"
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo -e "Python 版本: ${GREEN}${PYTHON_VERSION}${NC}"

# 检查 pip
if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}错误: 未找到 pip3${NC}"
    exit 1
fi

# 检查 MySQL 客户端
if ! command -v mysql &> /dev/null; then
    echo -e "${YELLOW}提示: 未找到 mysql 客户端，建议安装${NC}"
fi

# 创建安装目录
INSTALL_DIR="${MYSQL_SYNC_DIR:-/opt/mysql-sync}"
echo -e "安装目录: ${GREEN}${INSTALL_DIR}${NC}"

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# 克隆代码
if [ -d ".git" ]; then
    echo -e "${YELLOW}检测到已有安装，更新中...${NC}"
    git pull
else
    echo -e "下载代码..."
    git clone https://github.com/15228947433/mysql-sync.git .
fi

# 安装依赖
echo -e "安装依赖..."
pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --quiet

# 创建配置文件
if [ ! -f "config.yaml" ]; then
    echo -e "创建配置文件..."
    cp config.example.yaml config.yaml
    echo -e "${YELLOW}请编辑 ${INSTALL_DIR}/config.yaml 填入数据库信息${NC}"
fi

# 创建 systemd 服务
if command -v systemctl &> /dev/null; then
    echo -e "创建 systemd 服务..."
    cat > /etc/systemd/system/mysql-sync.service << EOF
[Unit]
Description=MySQL Sync - 数据同步工具
After=network.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
ExecStart=/usr/bin/python3 run.py web --host 0.0.0.0 --port 8520
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable mysql-sync
    echo -e "${GREEN}服务已创建: mysql-sync.service${NC}"
    echo -e "启动: ${GREEN}systemctl start mysql-sync${NC}"
    echo -e "查看状态: ${GREEN}systemctl status mysql-sync${NC}"
    echo -e "查看日志: ${GREEN}journalctl -u mysql-sync -f${NC}"
fi

# 获取 IP
IP=$(curl -s ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           安装完成！                      ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
echo ""
echo -e "配置文件: ${YELLOW}${INSTALL_DIR}/config.yaml${NC}"
echo -e "Web 界面: ${GREEN}http://${IP}:8520${NC}"
echo ""
echo -e "启动命令:"
echo -e "  ${GREEN}systemctl start mysql-sync${NC}    # 后台启动"
echo -e "  ${GREEN}python3 run.py web${NC}            # 前台启动"
echo -e "  ${GREEN}python3 run.py sync${NC}           # CLI 同步"
echo ""
