# coding=utf-8
"""
Tushare 工具模块

为 AI 分析器提供 Function Calling 工具，通过 Tushare 接口查询实时金融数据。
支持概念板块行情、成分股、大盘指数、个股日线行情、个股每日指标和涨跌停统计查询。
"""

import json
import os
from typing import Any, Dict, List, Optional

# Tushare 工具的 OpenAI tools JSON Schema 定义
TUSHARE_TOOLS_SCHEMA: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_concept_sector_daily",
            "description": (
                "获取同花顺概念板块日线行情数据，包括涨跌幅、成交量、换手率等。"
                "可按板块代码和日期查询。板块代码来自概念板块列表（如 885311.TI）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {
                        "type": "string",
                        "description": "概念板块代码，如 885311.TI（智能电网）。来自概念板块列表。",
                    },
                    "trade_date": {
                        "type": "string",
                        "description": "交易日期，YYYYMMDD 格式，如 20260214。不填则返回最近交易日数据。",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_concept_sector_members",
            "description": (
                "获取同花顺概念板块的成分股列表，返回板块包含的所有个股代码和名称。"
                "可用于了解板块内有哪些龙头股。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {
                        "type": "string",
                        "description": "概念板块代码，如 885311.TI（智能电网）。来自概念板块列表。",
                    },
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_index_daily",
            "description": (
                "获取大盘指数日线行情数据，包括开盘、收盘、最高、最低、涨跌幅、成交量等。"
                "常用指数代码：000001.SH（上证指数）、399001.SZ（深证成指）、399006.SZ（创业板指）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {
                        "type": "string",
                        "description": (
                            "指数代码。常用：000001.SH（上证指数）、399001.SZ（深证成指）、"
                            "399006.SZ（创业板指）、399300.SZ（沪深300）。"
                        ),
                    },
                    "trade_date": {
                        "type": "string",
                        "description": "交易日期，YYYYMMDD 格式。不填则返回最近交易日数据。",
                    },
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_daily_basic",
            "description": (
                "获取个股每日重要指标，包括换手率、量比、市盈率（PE/PE_TTM）、市净率（PB）、"
                "股息率、总市值、流通市值等。可按股票代码或交易日期查询。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {
                        "type": "string",
                        "description": "股票代码，如 000001.SZ（平安银行）、600519.SH（贵州茅台）。",
                    },
                    "trade_date": {
                        "type": "string",
                        "description": "交易日期，YYYYMMDD 格式。不填则返回最近交易日数据。",
                    },
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_daily",
            "description": (
                "获取个股日线行情数据，包括开盘、收盘、最高、最低、昨收、涨跌幅、成交量、成交额。"
                "用于判断个股当日涨跌幅度、是否接近涨停、量能变化等。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {
                        "type": "string",
                        "description": "股票代码，如 000001.SZ（平安银行）、600519.SH（贵州茅台）。",
                    },
                    "trade_date": {
                        "type": "string",
                        "description": "交易日期，YYYYMMDD 格式。不填则返回最近交易日数据。",
                    },
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_limit_list",
            "description": (
                "获取每日涨跌停股票列表，包括封单比、封单额、首次封板时间、开板次数、"
                "涨停强度等。用于查看当日哪些股票涨停/跌停及其封板质量。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "trade_date": {
                        "type": "string",
                        "description": "交易日期，YYYYMMDD 格式。不填则返回最近交易日数据。",
                    },
                    "limit_type": {
                        "type": "string",
                        "description": "涨跌停类型：U=涨停，D=跌停，Z=炸板。不填则返回全部。",
                        "enum": ["U", "D", "Z"],
                    },
                    "ts_code": {
                        "type": "string",
                        "description": "股票代码，可选。填写则只返回该股票的涨跌停信息。",
                    },
                },
                "required": [],
            },
        },
    },
]


class TushareToolExecutor:
    """Tushare 工具执行器，负责实际调用 Tushare API 并返回格式化文本。"""

    def __init__(self, token: str):
        """
        初始化 Tushare 工具执行器。

        Args:
            token: Tushare Pro API Token
        """
        self.token = token
        self._api = None

    @property
    def api(self):
        """延迟初始化 tushare pro_api（避免未安装 tushare 时报错）。"""
        if self._api is None:
            import tushare as ts
            self._api = ts.pro_api(self.token)
        return self._api

    def execute(self, function_name: str, arguments: Dict[str, Any]) -> str:
        """
        统一分发工具调用，捕获异常并返回友好错误信息。

        Args:
            function_name: 工具函数名
            arguments: 函数参数字典

        Returns:
            str: 格式化的文本结果
        """
        dispatch = {
            "get_concept_sector_daily": self.get_concept_sector_daily,
            "get_concept_sector_members": self.get_concept_sector_members,
            "get_index_daily": self.get_index_daily,
            "get_stock_daily_basic": self.get_stock_daily_basic,
            "get_stock_daily": self.get_stock_daily,
            "get_limit_list": self.get_limit_list,
        }

        func = dispatch.get(function_name)
        if not func:
            return f"错误：未知的工具函数 '{function_name}'"

        try:
            return func(**arguments)
        except ImportError:
            return "错误：tushare 未安装，请先安装：pip install tushare"
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            if len(error_msg) > 200:
                error_msg = error_msg[:200] + "..."
            return f"Tushare 查询失败 ({error_type}): {error_msg}"

    def get_concept_sector_daily(
        self,
        ts_code: str = "",
        trade_date: str = "",
    ) -> str:
        """
        获取同花顺概念板块日线行情。

        Args:
            ts_code: 板块代码（如 885311.TI）
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            格式化的行情文本
        """
        params = {}
        fields = "ts_code,trade_date,open,close,high,low,pct_change,vol,turnover_rate"

        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date

        # 如果既没有 ts_code 也没有 trade_date，无法查询
        if not params:
            return "错误：请至少提供板块代码（ts_code）或交易日期（trade_date）之一。"

        df = self.api.ths_daily(**params, fields=fields)

        if df is None or df.empty:
            return f"未查询到数据（ts_code={ts_code}, trade_date={trade_date}）。可能是非交易日或代码有误。"

        # 限制返回行数，避免返回过多数据
        df = df.head(20)

        lines = [f"同花顺概念板块日线行情（共 {len(df)} 条）："]
        lines.append("板块代码 | 交易日 | 开盘 | 收盘 | 最高 | 最低 | 涨跌幅(%) | 成交量 | 换手率(%)")
        lines.append("-" * 80)
        for _, row in df.iterrows():
            pct = f"{row.get('pct_change', 0):.2f}" if row.get("pct_change") is not None else "-"
            vol = f"{row.get('vol', 0):.0f}" if row.get("vol") is not None else "-"
            tr = f"{row.get('turnover_rate', 0):.2f}" if row.get("turnover_rate") is not None else "-"
            lines.append(
                f"{row.get('ts_code', '-')} | {row.get('trade_date', '-')} | "
                f"{row.get('open', '-')} | {row.get('close', '-')} | "
                f"{row.get('high', '-')} | {row.get('low', '-')} | "
                f"{pct} | {vol} | {tr}"
            )

        return "\n".join(lines)

    def get_concept_sector_members(self, ts_code: str) -> str:
        """
        获取同花顺概念板块成分股列表。

        Args:
            ts_code: 板块代码（如 885311.TI）

        Returns:
            格式化的成分股列表文本
        """
        df = self.api.ths_member(ts_code=ts_code)

        if df is None or df.empty:
            return f"未查询到板块 {ts_code} 的成分股数据。请检查板块代码是否正确。"

        lines = [f"板块 {ts_code} 成分股列表（共 {len(df)} 只）："]
        lines.append("股票代码 | 股票名称")
        lines.append("-" * 40)
        for _, row in df.iterrows():
            lines.append(f"{row.get('con_code', '-')} | {row.get('con_name', '-')}")

        return "\n".join(lines)

    def get_index_daily(
        self,
        ts_code: str,
        trade_date: str = "",
    ) -> str:
        """
        获取大盘指数日线行情。

        Args:
            ts_code: 指数代码（如 000001.SH）
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            格式化的指数行情文本
        """
        params = {"ts_code": ts_code}
        if trade_date:
            params["trade_date"] = trade_date

        df = self.api.index_daily(**params)

        if df is None or df.empty:
            return f"未查询到指数 {ts_code} 的行情数据。可能是非交易日或代码有误。"

        # 限制返回行数
        df = df.head(10)

        lines = [f"指数 {ts_code} 日线行情（共 {len(df)} 条）："]
        lines.append("交易日 | 开盘 | 收盘 | 最高 | 最低 | 涨跌点 | 涨跌幅(%) | 成交量(手) | 成交额(千元)")
        lines.append("-" * 100)
        for _, row in df.iterrows():
            pct = f"{row.get('pct_chg', 0):.2f}" if row.get("pct_chg") is not None else "-"
            change = f"{row.get('change', 0):.2f}" if row.get("change") is not None else "-"
            vol = f"{row.get('vol', 0):.0f}" if row.get("vol") is not None else "-"
            amount = f"{row.get('amount', 0):.0f}" if row.get("amount") is not None else "-"
            lines.append(
                f"{row.get('trade_date', '-')} | "
                f"{row.get('open', '-')} | {row.get('close', '-')} | "
                f"{row.get('high', '-')} | {row.get('low', '-')} | "
                f"{change} | {pct} | {vol} | {amount}"
            )

        return "\n".join(lines)

    def get_stock_daily_basic(
        self,
        ts_code: str,
        trade_date: str = "",
    ) -> str:
        """
        获取个股每日指标。

        Args:
            ts_code: 股票代码（如 000001.SZ）
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            格式化的个股指标文本
        """
        params = {"ts_code": ts_code}
        fields = (
            "ts_code,trade_date,close,turnover_rate,turnover_rate_f,"
            "volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,"
            "dv_ratio,dv_ttm,total_share,float_share,free_share,"
            "total_mv,circ_mv"
        )
        if trade_date:
            params["trade_date"] = trade_date

        df = self.api.daily_basic(**params, fields=fields)

        if df is None or df.empty:
            return f"未查询到股票 {ts_code} 的每日指标数据。可能是非交易日或代码有误。"

        # 限制返回行数
        df = df.head(5)

        lines = [f"股票 {ts_code} 每日指标（共 {len(df)} 条）："]
        for _, row in df.iterrows():
            td = row.get("trade_date", "-")
            lines.append(f"\n--- {td} ---")
            lines.append(f"收盘价: {row.get('close', '-')}")
            lines.append(f"换手率: {self._fmt(row.get('turnover_rate'))}%")
            lines.append(f"换手率(自由流通): {self._fmt(row.get('turnover_rate_f'))}%")
            lines.append(f"量比: {self._fmt(row.get('volume_ratio'))}")
            lines.append(f"市盈率(PE): {self._fmt(row.get('pe'))}")
            lines.append(f"市盈率(PE_TTM): {self._fmt(row.get('pe_ttm'))}")
            lines.append(f"市净率(PB): {self._fmt(row.get('pb'))}")
            lines.append(f"市销率(PS): {self._fmt(row.get('ps'))}")
            lines.append(f"股息率: {self._fmt(row.get('dv_ratio'))}%")
            total_mv = row.get("total_mv")
            circ_mv = row.get("circ_mv")
            lines.append(f"总市值: {self._fmt_mv(total_mv)}")
            lines.append(f"流通市值: {self._fmt_mv(circ_mv)}")

        return "\n".join(lines)

    def get_stock_daily(
        self,
        ts_code: str,
        trade_date: str = "",
    ) -> str:
        """
        获取个股日线行情（OHLCV）。

        Args:
            ts_code: 股票代码（如 000001.SZ）
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            格式化的日线行情文本
        """
        params = {"ts_code": ts_code}
        if trade_date:
            params["trade_date"] = trade_date

        df = self.api.daily(**params)

        if df is None or df.empty:
            return f"未查询到股票 {ts_code} 的日线行情数据。可能是非交易日或代码有误。"

        df = df.head(10)

        lines = [f"股票 {ts_code} 日线行情（共 {len(df)} 条）："]
        lines.append("交易日 | 开盘 | 收盘 | 最高 | 最低 | 昨收 | 涨跌幅(%) | 成交量(手) | 成交额(千元)")
        lines.append("-" * 100)
        for _, row in df.iterrows():
            pct = f"{row.get('pct_chg', 0):.2f}" if row.get("pct_chg") is not None else "-"
            vol = f"{row.get('vol', 0):.0f}" if row.get("vol") is not None else "-"
            amount = f"{row.get('amount', 0):.0f}" if row.get("amount") is not None else "-"
            lines.append(
                f"{row.get('trade_date', '-')} | "
                f"{row.get('open', '-')} | {row.get('close', '-')} | "
                f"{row.get('high', '-')} | {row.get('low', '-')} | "
                f"{row.get('pre_close', '-')} | "
                f"{pct} | {vol} | {amount}"
            )

        return "\n".join(lines)

    def get_limit_list(
        self,
        trade_date: str = "",
        limit_type: str = "",
        ts_code: str = "",
    ) -> str:
        """
        获取每日涨跌停统计。

        Args:
            trade_date: 交易日期（YYYYMMDD）
            limit_type: U=涨停, D=跌停, Z=炸板
            ts_code: 股票代码（可选，筛选单只股票）

        Returns:
            格式化的涨跌停列表文本
        """
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if limit_type:
            params["limit_type"] = limit_type
        if ts_code:
            params["ts_code"] = ts_code

        if not params:
            return "错误：请至少提供交易日期（trade_date）或股票代码（ts_code）之一。"

        fields = (
            "ts_code,trade_date,name,close,pct_chg,amp,"
            "fc_ratio,fl_ratio,fd_amount,first_time,last_time,"
            "open_times,strth,limit"
        )
        df = self.api.limit_list_d(**params, fields=fields)

        if df is None or df.empty:
            return f"未查询到涨跌停数据（trade_date={trade_date}, limit_type={limit_type}）。可能是非交易日。"

        df = df.head(50)

        limit_label = {"U": "涨停", "D": "跌停", "Z": "炸板"}
        lines = [f"涨跌停统计（共 {len(df)} 条）："]
        lines.append("代码 | 名称 | 收盘 | 涨跌幅(%) | 封单比 | 封单额(万) | 首封时间 | 开板次数 | 强度 | 类型")
        lines.append("-" * 110)
        for _, row in df.iterrows():
            pct = f"{row.get('pct_chg', 0):.2f}" if row.get("pct_chg") is not None else "-"
            fc = f"{row.get('fc_ratio', 0):.2f}" if row.get("fc_ratio") is not None else "-"
            fd = f"{row.get('fd_amount', 0):.0f}" if row.get("fd_amount") is not None else "-"
            ft = row.get("first_time", "-") or "-"
            ot = row.get("open_times", "-")
            strth = f"{row.get('strth', 0):.1f}" if row.get("strth") is not None else "-"
            lt = limit_label.get(row.get("limit", ""), row.get("limit", "-"))
            lines.append(
                f"{row.get('ts_code', '-')} | {row.get('name', '-')} | "
                f"{row.get('close', '-')} | {pct} | {fc} | {fd} | "
                f"{ft} | {ot} | {strth} | {lt}"
            )

        return "\n".join(lines)

    @staticmethod
    def _fmt(value) -> str:
        """格式化数值，None 显示为 '-'。"""
        if value is None:
            return "-"
        try:
            return f"{float(value):.2f}"
        except (ValueError, TypeError):
            return str(value)

    @staticmethod
    def _fmt_mv(value) -> str:
        """格式化市值（万元 → 亿元），None 显示为 '-'。"""
        if value is None:
            return "-"
        try:
            v = float(value)
            if v >= 10000:
                return f"{v / 10000:.2f} 亿元"
            return f"{v:.2f} 万元"
        except (ValueError, TypeError):
            return str(value)

    def validate(self) -> tuple:
        """
        验证 Tushare 配置是否有效。

        Returns:
            tuple: (是否有效, 错误信息)
        """
        if not self.token:
            return False, "未配置 Tushare Token，请在 config.yaml 或环境变量 TUSHARE_TOKEN 中设置"
        try:
            import tushare
        except ImportError:
            return False, "tushare 未安装，请先安装：pip install tushare"
        return True, ""
