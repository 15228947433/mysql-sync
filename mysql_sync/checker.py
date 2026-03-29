"""
数据校验模块 — 定期检查源表和目标表数据一致性
"""
import logging
import time
import threading
import pymysql
from .config import AppConfig
from .sync import ConnectionPool, SyncState

logger = logging.getLogger("mysql_sync")


class DataChecker:
    """数据一致性校验器"""

    def __init__(self, config: AppConfig, state: SyncState):
        self.config = config
        self.state = state
        self.src_pool = ConnectionPool(config.source.to_dict(), pool_size=2)
        self.dst_pool = ConnectionPool(config.target.to_dict(), pool_size=2)
        self.running = False

    def check_all(self):
        """校验所有同步表的数据一致性"""
        results = []
        for rule in self.config.rules:
            if rule.source_table == "*":
                # 展开通配符
                conn = self.src_pool.get_conn()
                try:
                    tables = conn.execute(f"SHOW TABLES FROM `{rule.source_db}`").fetchall()
                    for (t,) in tables:
                        result = self._check_table(rule.source_db, t, rule.target_db, t)
                        results.append(result)
                finally:
                    self.src_pool.put_conn(conn)
            else:
                result = self._check_table(
                    rule.source_db, rule.source_table,
                    rule.target_db, rule.target_table or rule.source_table
                )
                results.append(result)

        return results

    def _check_table(self, src_db, src_table, tgt_db, tgt_table):
        """校验单张表"""
        result = {
            "source": f"{src_db}.{src_table}",
            "target": f"{tgt_db}.{tgt_table}",
            "status": "unknown",
            "src_count": 0,
            "tgt_count": 0,
            "diff": 0,
            "sample_match": None,
            "detail": "",
        }

        src_conn = self.src_pool.get_conn()
        dst_conn = self.dst_pool.get_conn()

        try:
            # 1. 行数对比
            src_count = src_conn.execute(f"SELECT COUNT(*) FROM `{src_db}`.`{src_table}`").fetchone()[0]
            tgt_count = dst_conn.execute(f"SELECT COUNT(*) FROM `{tgt_db}`.`{tgt_table}`").fetchone()[0]
            result["src_count"] = src_count
            result["tgt_count"] = tgt_count
            result["diff"] = src_count - tgt_count

            # 2. 抽样校验（取前10行对比）
            try:
                src_conn.execute(f"SELECT * FROM `{src_db}`.`{src_table}` LIMIT 10")
                columns = [desc[0] for desc in src_conn.description]
                src_rows = src_conn.fetchall()

                col_str = ",".join([f"`{c}`" for c in columns])
                dst_rows = dst_conn.execute(f"SELECT {col_str} FROM `{tgt_db}`.`{tgt_table}` LIMIT 10").fetchall()

                match_count = 0
                for src_row in src_rows:
                    if src_row in dst_rows:
                        match_count += 1

                if len(src_rows) > 0:
                    result["sample_match"] = round(match_count / len(src_rows) * 100, 1)
                else:
                    result["sample_match"] = 100
            except Exception as e:
                result["sample_match"] = None
                result["detail"] = f"抽样校验异常: {e}"

            # 3. 判定状态
            if result["diff"] == 0 and (result["sample_match"] is None or result["sample_match"] == 100):
                result["status"] = "consistent"
            elif abs(result["diff"]) <= 10 and (result["sample_match"] is None or result["sample_match"] >= 90):
                result["status"] = "slight_diff"
            else:
                result["status"] = "inconsistent"

            # 记录到日志
            self.state.add_log(
                "CHECK",
                result["source"],
                result["target"],
                f"行数: {src_count}→{tgt_count} (差异{result['diff']}), 抽样匹配: {result['sample_match']}%"
            )

        except Exception as e:
            result["status"] = "error"
            result["detail"] = str(e)
            logger.error(f"校验失败 {src_db}.{src_table}: {e}")
            self.state.add_log("CHECK_ERROR", result["source"], result["target"], str(e))
        finally:
            self.src_pool.put_conn(src_conn)
            self.dst_pool.put_conn(dst_conn)

        return result

    def check_single(self, src_db, src_table, tgt_db, tgt_table):
        """校验单张表（外部调用）"""
        return self._check_table(src_db, src_table, tgt_db, tgt_table)

    def auto_repair(self, src_db, src_table, tgt_db, tgt_table):
        """自动修复不一致的数据"""
        logger.info(f"开始修复: {src_db}.{src_table} → {tgt_db}.{tgt_table}")

        src_conn = self.src_pool.get_conn()
        dst_conn = self.dst_pool.get_conn()

        try:
            # 获取主键
            pk_rows = src_conn.execute(f"SHOW KEYS FROM `{src_db}`.`{src_table}` WHERE Key_name='PRIMARY'").fetchall()
            pk_cols = [row[4] for row in pk_rows] if pk_rows else []

            if not pk_cols:
                logger.warning(f"无主键，无法自动修复: {src_db}.{src_table}")
                return {"status": "no_pk", "message": "表无主键，无法自动修复"}

            # 获取所有列
            src_conn.execute(f"SELECT * FROM `{src_db}`.`{src_table}` LIMIT 1")
            columns = [desc[0] for desc in src_conn.description]
            col_str = ",".join([f"`{c}`" for c in columns])
            placeholders = ",".join(["%s"] * len(columns))
            update_str = ",".join([f"`{c}`=VALUES(`{c}`)" for c in columns if c not in pk_cols])

            # 分批同步
            batch_size = 500
            offset = 0
            repaired = 0

            while True:
                rows = src_conn.execute(
                    f"SELECT * FROM `{src_db}`.`{src_table}` LIMIT {batch_size} OFFSET {offset}"
                ).fetchall()

                if not rows:
                    break

                for row in rows:
                    vals = list(row)
                    try:
                        dst_conn.execute(
                            f"INSERT INTO `{tgt_db}`.`{tgt_table}` ({col_str}) VALUES ({placeholders}) "
                            f"ON DUPLICATE KEY UPDATE {update_str}",
                            vals
                        )
                        repaired += 1
                    except Exception:
                        pass

                offset += batch_size

            logger.info(f"修复完成: {repaired} 行")
            return {"status": "ok", "repaired": repaired}

        except Exception as e:
            logger.error(f"修复失败: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            self.src_pool.put_conn(src_conn)
            self.dst_pool.put_conn(dst_conn)

    def close(self):
        self.src_pool.close_all()
        self.dst_pool.close_all()
