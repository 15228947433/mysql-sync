"""
告警通知模块 — 支持企业微信/钉钉/飞书/自定义 Webhook
"""
import json
import logging
import time
import threading
import requests
from datetime import datetime

logger = logging.getLogger("mysql_sync")


class AlertManager:
    """告警管理器"""

    def __init__(self, webhook_url="", delay_threshold=60, error_rate_threshold=10,
                 silent_start=None, silent_end=None):
        self.webhook_url = webhook_url
        self.delay_threshold = delay_threshold
        self.error_rate_threshold = error_rate_threshold
        self.silent_start = silent_start  # "23:00"
        self.silent_end = silent_end      # "08:00"
        self._last_alert_time = {}
        self._alert_cooldown = 300  # 5分钟内不重复告警

    def _is_silent(self):
        """检查是否在静默时段"""
        if not self.silent_start or not self.silent_end:
            return False
        now = datetime.now().strftime("%H:%M")
        if self.silent_start <= self.silent_end:
            return self.silent_start <= now <= self.silent_end
        else:
            return now >= self.silent_start or now <= self.silent_end

    def _should_alert(self, alert_key):
        """检查是否应该发送告警（防重复）"""
        last_time = self._last_alert_time.get(alert_key, 0)
        if time.time() - last_time < self._alert_cooldown:
            return False
        self._last_alert_time[alert_key] = time.time()
        return True

    def send(self, level, title, content, source="", target=""):
        """发送告警"""
        if not self.webhook_url:
            return
        if self._is_silent():
            logger.debug(f"静默时段，跳过告警: {title}")
            return

        alert_key = f"{level}:{title}:{source}:{target}"
        if not self._should_alert(alert_key):
            return

        # 构建消息
        message = self._build_message(level, title, content, source, target)

        try:
            resp = requests.post(
                self.webhook_url,
                json=message,
                timeout=10,
                headers={"Content-Type": "application/json"}
            )
            if resp.status_code == 200:
                logger.info(f"告警发送成功: {title}")
            else:
                logger.warning(f"告警发送失败: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            logger.error(f"告警发送异常: {e}")

    def _build_message(self, level, title, content, source, target):
        """构建消息（自动适配不同平台）"""
        level_emoji = {"error": "🔴", "warning": "🟡", "info": "🔵"}.get(level, "⚪")
        time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        text = f"""{level_emoji} MySQL Sync 告警

**{title}**

{content}

源: {source}
目标: {target}
时间: {time_str}
"""

        # 尝试适配不同平台
        url = self.webhook_url.lower()

        if "qyapi.weixin" in url:
            # 企业微信
            return {
                "msgtype": "markdown",
                "markdown": {"content": text}
            }
        elif "oapi.dingtalk" in url:
            # 钉钉
            return {
                "msgtype": "markdown",
                "markdown": {"title": title, "text": text}
            }
        elif "open.feishu" in url:
            # 飞书
            return {
                "msg_type": "text",
                "content": {"text": text}
            }
        else:
            # 通用
            return {
                "title": title,
                "content": text,
                "level": level,
                "source": source,
                "target": target,
                "time": time_str,
            }

    def alert_sync_error(self, source, target, error_msg):
        """同步错误告警"""
        self.send("error", "同步错误", f"错误信息: {error_msg}", source, target)

    def alert_ddl_error(self, source, target, error_msg):
        """DDL 同步错误告警"""
        self.send("warning", "DDL 同步失败", f"错误信息: {error_msg}", source, target)

    def alert_full_sync_done(self, source, target, rows):
        """全量同步完成通知"""
        self.send("info", "全量同步完成", f"共同步 {rows} 行", source, target)

    def alert_data_inconsistent(self, source, target, diff):
        """数据不一致告警"""
        self.send("warning", "数据不一致", f"行数差异: {diff}", source, target)

    def test(self):
        """测试告警"""
        self.send("info", "测试告警", "这是一条测试消息，如果收到说明配置正确", "test.source", "test.target")


def load_alert_config():
    """从本地存储加载告警配置"""
    try:
        import os
        config_path = os.path.join(os.path.dirname(__file__), "..", "alert_config.json")
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_alert_config(config):
    """保存告警配置到本地"""
    import os
    config_path = os.path.join(os.path.dirname(__file__), "..", "alert_config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
