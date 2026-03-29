# MySQL Sync 🔄

> 轻量级 MySQL 跨库/跨实例表同步工具，基于 Binlog 实时同步。

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-ready-blue.svg)](https://www.docker.com/)

---

## ✨ 特性

- ✅ **实时同步** — 基于 Binlog，毫秒级延迟
- ✅ **自动建表** — 目标表不存在自动创建
- ✅ **DDL 同步** — 源表 ALTER/DROP，目标表自动跟着变
- ✅ **跨库映射** — A.a → Z.a, B.b → Z.b，随意配置
- ✅ **跨实例支持** — 不同 MySQL 实例之间也能同步
- ✅ **Web 管理界面** — 页面上配置规则、查看状态、查看日志
- ✅ **Docker 一键启动** — 不用装依赖
- ✅ **轻量** — 纯 Python，一个脚本搞定

---

## 🚀 快速开始

### 方式一：直接运行

```bash
# 1. 克隆
git clone https://github.com/15228947433/mysql-sync.git
cd mysql-sync

# 2. 安装依赖
pip install -r requirements.txt

# 3. 复制配置文件
cp config.example.yaml config.yaml
# 编辑 config.yaml，填入数据库信息和同步规则

# 4. 启动 Web 管理界面
python run.py web

# 或纯 CLI 同步
python run.py sync
```

### 方式二：Docker

```bash
# 1. 复制配置文件
cp config.example.yaml config.yaml
# 编辑 config.yaml

# 2. 启动
docker compose up -d

# 3. 访问
# http://localhost:8520
```

---

## 📖 使用说明

### 1. 确认 MySQL 开启 Binlog

```bash
mysql -e "SHOW VARIABLES LIKE 'binlog_format';"
```

结果必须是 `ROW`。如果不是，修改 `my.cnf`：

```ini
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server-id=1
```

然后重启 MySQL。

### 2. 配置同步规则

编辑 `config.yaml`：

```yaml
source:
  host: "127.0.0.1"
  port: 3306
  user: "root"
  password: "xxx"

target:
  host: "127.0.0.1"
  port: 3306
  user: "root"
  password: "xxx"
  database: "Z"       # 目标库名

rules:
  - source_db: "A"
    source_table: "a"
    target_db: "Z"
    target_table: "a"
  - source_db: "B"
    source_table: "b"
    target_db: "Z"
    target_table: "b"
```

### 3. 启动

```bash
# Web 模式（推荐）
python run.py web

# CLI 模式
python run.py sync

# 查看状态
python run.py status
```

### 4. Web 界面

访问 `http://localhost:8520`：

- 📋 **同步规则** — 添加/删除同步规则
- ⚙️ **数据库配置** — 修改源库和目标库连接信息
- 📝 **同步日志** — 查看实时同步日志
- ▶️ **启停控制** — 一键启动/停止同步

---

## 📁 项目结构

```
mysql-sync/
├── mysql_sync/
│   ├── __init__.py
│   ├── config.py       # 配置管理
│   ├── sync.py         # 核心同步逻辑
│   └── web_app.py      # Web 管理界面
├── templates/
│   └── index.html      # Web 前端
├── run.py              # 启动入口
├── config.example.yaml # 配置示例
├── requirements.txt    # Python 依赖
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## 🔧 命令参考

```bash
# 启动 Web 管理界面
python run.py web [--host 0.0.0.0] [--port 8520]

# 启动 CLI 同步
python run.py sync

# 查看同步状态
python run.py status

# 指定配置文件
python run.py -c /path/to/config.yaml sync
```

---

## ⚠️ 注意事项

- 源库必须开启 Binlog（ROW 格式）
- server_id 不能跟其他从库冲突
- 目标表会自动创建，但建议先用小表测试
- 生产环境建议先在测试环境验证

---

## 📄 License

MIT
