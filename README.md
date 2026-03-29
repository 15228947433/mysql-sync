# MySQL Sync 🔄

> 轻量级 MySQL 跨库/跨实例表同步工具，基于 Binlog 全量+增量实时同步。

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-ready-blue.svg)](https://www.docker.com/)

---

## ✨ 特性

| 功能 | 说明 |
|------|------|
| 全量同步 | 首次启动自动全量同步历史数据 |
| 增量同步 | 基于 Binlog 毫秒级实时同步 |
| 并行处理 | 全量期间其他表增量正常处理，不阻塞 |
| 自动建表 | 目标表不存在自动创建，结构跟源表一致 |
| DDL 同步 | 源表 ALTER/DROP，目标表自动跟着变 |
| 跨库映射 | A.a → Z.a, B.b → Z.b，随意配置 |
| 跨实例支持 | 不同 MySQL 实例之间也能同步 |
| 数据校验 | 定期检查源表和目标表数据一致性 |
| 自动修复 | 数据不一致时自动修复 |
| Web 管理 | DTS 风格向导流程，6步创建同步任务 |
| 监控大屏 | QPS 趋势图、统计卡片、任务状态 |
| 告警通知 | Webhook 通知（企微/钉钉/飞书） |
| 断点续传 | Binlog 位点持久化，重启从上次位置继续 |
| 批量写入 | 攒一批再写，减少 IO |
| 连接池 | 源库/目标库独立连接池 |
| Docker | 一键部署 |
| 一键安装 | 自动配置 systemd 服务 |

---

## 🚀 快速开始

### 方式一：一键安装（推荐）

```bash
curl -sSL https://raw.githubusercontent.com/15228947433/mysql-sync/main/install.sh | bash
```

自动完成：
- 下载代码
- 安装依赖
- 创建配置文件
- 创建 systemd 服务

### 方式二：手动安装

```bash
git clone https://github.com/15228947433/mysql-sync.git
cd mysql-sync
pip install -r requirements.txt
cp config.example.yaml config.yaml
# 编辑 config.yaml
python run.py web
```

### 方式三：Docker

```bash
cp config.example.yaml config.yaml
# 编辑 config.yaml
docker compose up -d
```

---

## 📖 使用流程

### 1. 确认 MySQL 开启 Binlog

```bash
mysql -e "SHOW VARIABLES LIKE 'binlog_format';"
# 结果必须是 ROW
```

如果不是，修改 `my.cnf`：
```ini
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server-id=1
```

### 2. 打开 Web 界面

访问 `http://你的IP:8520`

### 3. 按向导创建同步任务

1. 配置源库 → 测试连接
2. 配置目标库 → 测试连接
3. 选择同步对象（勾选库和表）
4. 配置同步选项（全量/增量、DDL同步等）
5. 预检查（自动检查 Binlog 格式、权限等）
6. 确认启动

### 4. 监控

- 监控大屏：实时 QPS、同步行数、错误率
- 同步日志：查看每个操作的详细记录
- 数据校验：点击"数据校验"检查一致性

---

## ⚙️ 配置说明

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
  database: "Z"

rules:
  # 同步整个库
  - source_db: "A"
    source_table: "*"
    target_db: "Z"
  # 同步指定表
  - source_db: "B"
    source_table: ["b1", "b2"]
    target_db: "Z"

sync_type: "full_and_incr"  # full_and_incr | incr | full
batch_size: 500
flush_interval: 1.0
pool_size: 5
server_id: 100
web_port: 8520
```

---

## 🔧 命令参考

```bash
# Web 管理界面
python run.py web [--host 0.0.0.0] [--port 8520]

# CLI 同步
python run.py sync

# 查看状态
python run.py status

# 指定配置文件
python run.py -c /path/to/config.yaml sync
```

---

## 📁 项目结构

```
mysql-sync/
├── mysql_sync/
│   ├── __init__.py
│   ├── config.py       # 配置管理
│   ├── sync.py         # 核心同步引擎（全量+增量）
│   ├── checker.py      # 数据一致性校验
│   └── web_app.py      # Web 管理界面 API
├── templates/
│   └── index.html      # Web 前端（Vue3 + Element Plus + ECharts）
├── run.py              # 启动入口
├── install.sh          # 一键安装脚本
├── config.example.yaml # 配置示例
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## ⚠️ 注意事项

- 源库必须开启 Binlog（ROW 格式）
- server_id 不能跟其他从库冲突
- 建议先在测试环境验证
- 生产环境建议开启数据校验

---

## 📄 License

MIT
