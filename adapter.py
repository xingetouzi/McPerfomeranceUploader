from datetime import timedelta
from functools import partial
import pandas as pd
import pytz

ORDER_ACT_MAP = {
    "进场Long": "Buy",
    "进场Short": "Sell",
    "出场Long": "Sell",
    "出场Short": "Buy",
}

OFFSET_FLAG_MAP = {
    "进场Long": "Open",
    "进场Short": "Open",
    "出场Long": "Close",
    "出场Short": "Close",
}

SIGN_MAP = {
    "进场Long": 1,
    "进场Short": -1,
    "出场Long": -1,
    "出场Short": 1,
}

TIME_DELTA_MAP = {
    "Minute": 60,
    "Minutes": 60,
    "Hour": 60 * 60,
    "Hours": 60 * 60,
    "Day": 24 * 60 * 60,
    "Days": 24 * 60 * 60,
}

TZ = pytz.timezone("Asia/Shanghai")


class GuoJinAdapter(object):
    def __init__(self, extractor):
        """

        Args:
            extractor(extractor.PerformanceExtractor):

        Returns:

        """
        self._e = extractor
        self._name = None
        self._orders = None
        self._positions = None
        self._symbol = None

    @staticmethod
    def get_localized_time(dt, format="%Y%m%dT%H%M%S"):
        return TZ.fromutc(dt.to_pydatetime().replace(tzinfo=None)).strftime(format)

    @staticmethod
    def get_time_frame(string):
        num, unit = string.split(" ")
        return timedelta(seconds=int(num) * TIME_DELTA_MAP[unit])

    @property
    def name(self):
        if self._name is None:
            self._name = self._e.info["策略名称"]
        return self._name

    @property
    def symbol(self):
        if self._symbol is None:
            self._symbol = self._e.info["商品名称"]
        return self._symbol

    @property
    def bitpower_symbol(self):
        return "".join(self.symbol.split(".")[-2:]).replace("HOT", "0000").replace("000000", "0000").upper()

    @property
    def orders(self):
        if self._orders is None:
            df = pd.DataFrame(self._e.trade_details)
            temp = pd.DataFrame(index=df.index)
            temp["Market"] = [self.symbol.split(".")[1].lower()] * len(temp)
            temp["Code"] = [self.bitpower_symbol] * len(temp)
            temp["Time"] = (df["时间"] - self.get_time_frame(self._e.info["数据周期"])).map(self.get_localized_time)
            temp["OrderAct"] = df["类型"].map(lambda x: ORDER_ACT_MAP[x])
            temp["OffsetFlag"] = df["类型"].map(lambda x: OFFSET_FLAG_MAP[x])
            temp["Price"] = df["价格"].map(lambda x: round(x, 6))  # 处理输出时的精度问题
            temp["Volume"] = df["合约"].astype(int)
            self._orders = temp
        return self._orders

    @property
    def positions(self):
        if self._positions is None:
            df = pd.DataFrame(self._e.trade_details)
            temp = pd.DataFrame(index=df.index)
            temp["Market"] = [self.symbol.split(".")[1]] * len(temp)
            temp["Code"] = [self.bitpower_symbol] * len(temp)
            if self._e.info["数据周期"].endswith("Day") or self._e.info["数据周期"].endswith("Days"):
                func = partial(self.get_localized_time, format="%Y%m%dT")
                temp["Time"] = (df["时间"] - self.get_time_frame(self._e.info["数据周期"])).map(func) + "150000"
                temp["ExpireTime"] = df["时间"].map(func) + "150000"
            else:
                temp["Time"] = (df["时间"] - self.get_time_frame(self._e.info["数据周期"])).map(self.get_localized_time)
                temp["ExpireTime"] = df["时间"].map(self.get_localized_time)
            temp["Position"] = (df["类型"].map(lambda x: SIGN_MAP[x]) * df["合约"].astype(int)).cumsum()
            temp["Price"] = df["价格"].map(lambda x: round(x, 6))  # 处理输出时的精度问题

            self._positions = temp
        return self._positions


if __name__ == "__main__":
    import os
    import json
    import sys

    from extractor import PerformanceExtractor

    e = PerformanceExtractor()
    file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data",
                        "ICE.CFFEX.IF.HOT  _ATS_v1.1.1.JZ.1c.xm.IF888.120101.0.0 策略回测绩效报告.xlsx")
    # file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "JXBG-0309",
    #                     "ICE.SHFE.rb.HOT  _SFET&CSR_v1.2; JiangNan_v1.4.1.RB.HOT.1d.2c.160901.ob.2 策略回测绩效报告.xlsx")
    e.open_with_name(file)
    # print(e.trade_details[0])
    # print(json.dumps(e.trade_analysis))
    # print(json.dumps(e.period_analysis))
    a = GuoJinAdapter(e)
    print(a.name)
    print(a.orders)
    print(a.positions)
