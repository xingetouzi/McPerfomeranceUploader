import xlrd
import xlrd.xldate
import pytz
from datetime import datetime
from functools import reduce, wraps
import tablib
import pandas as pd

TZ = pytz.timezone("Asia/Shanghai")
PRECISION_THRESHOLD = 1e-8
DELIMITERS = '/'


def parse(timestr):
    """

    Args:
        timestr(str):

    Returns:

    """
    dates = timestr.split(DELIMITERS)
    timetuple = [0, 1, 1]
    if len(dates[0]) == 4:
        for i in range(0, len(dates)):
            timetuple[i] = int(dates[i])
    else:
        for i in range(len(dates), 0, -1):
            timetuple[len(dates) - i] = int(dates[i - 1])
    # print(datetime(*timetuple))
    return datetime(*timetuple)


def get_timestamp(date):
    if date is None:
        return None
    tuple = xlrd.xldate.xldate_as_tuple(date, 0)
    if tuple[0] is 0:
        d = datetime(1970, 1, 1, *tuple[3:])
        return int(pytz.utc.localize(d).timestamp())
    else:
        d = datetime(*tuple)
        return int(TZ.localize(d).timestamp())


def get_precision(value):
    if value == '':
        return None
    elif isinstance(value, float) and abs(value - round(value, 0)) <= PRECISION_THRESHOLD:
        return int(value)
    else:
        return value


def get_period(value):
    if isinstance(value, str):
        if ' - ' in value:
            start, end = value.split(' - ')
            if start == '今天':
                start = end
            d = parse(start)
            return int(TZ.localize(d).timestamp())
        else:
            return value
    else:
        return get_timestamp(value)


def get_clean(value):
    if isinstance(value, str):
        return value.replace(".", "")
    else:
        return value


def _cache(cache_name):
    def decorator(func):
        @wraps(func)
        def wrapper(self):
            if self._dict.get(cache_name, None) is None:
                self._dict[cache_name] = func(self)
            return self._dict[cache_name]

        return wrapper

    return decorator


class PerformanceExtractor:
    def __init__(self):
        self._work_book = None
        self._performance = None
        self._dict = {}

    def open_with_name(self, file):
        self._work_book = xlrd.open_workbook(file)

    def open_with_content(self, content):
        self._work_book = xlrd.open_workbook(file_contents=content)

    @property
    def strategy(self):
        for prop in ["trade_details", "trade_analysis", "period_analysis", "info", "strategy_analysis"]:
            getattr(self, prop)
        return self._dict

    @staticmethod
    def _get_headers(sheet, index):
        headers = sheet.row_values(index)
        if headers[0] == "":
            headers[0] = "index"
        while len(headers) > 1 and headers[len(headers) - 1] == '':
            headers.pop()
        return headers

    @staticmethod
    def _get_rows(dataset, sheet, index, bound):
        ncol = len(dataset.headers)
        while index < sheet.nrows:
            values = sheet.row_values(index)[:ncol]
            if bound(values):
                break
            values = list(map(get_clean, map(get_precision, values)))
            # in trade_analysis
            if isinstance(values[0], str) and "日期" in values[0]:
                values = values[:1] + list(map(get_timestamp, values[1:]))
            # in period_analysis
            if "期间" in dataset.headers[0]:
                values[0] = get_period(values[0])
            dataset.append(values)
            index += 1
        return index

    @staticmethod
    def _get_index_dict(dataset):
        dataframe = pd.DataFrame(dataset.dict).set_index(dataset.headers[0]).fillna(value="n/a")
        return dataframe.to_dict("index")

    @property
    @_cache("trade_details")
    def trade_details(self):

        def get_value(key, value):
            if value == '':
                return None
            elif key in ["日期", "时间"]:
                return get_timestamp(value)
            elif key in ["委托单编号", "交易编号"]:
                return int(value)
            else:
                return value

        sheet = self._work_book.sheet_by_name("交易列表")
        names = sheet.row_values(2)
        rows = map(lambda r: list(map(lambda x: x.value, r)), list(sheet.get_rows())[3:])
        data = list(
            map(lambda x: {t[0]: get_value(*t) for t in zip(names, x)}, rows))
        df = pd.DataFrame(data).ffill()
        df["时间"] = (df["日期"] + df["时间"]).map(lambda x: datetime.fromtimestamp(x, tz=pytz.utc))
        del df["日期"]
        return df.to_dict("records")

    @property
    @_cache("trade_details")

    @property
    @_cache("trade_analysis")
    def trade_analysis(self):
        sheet = self._work_book.sheet_by_name("交易分析")
        data = {}
        index = 0
        empty = lambda l: reduce(lambda x, y: x and y == '', l, True)
        while index < sheet.nrows:
            names = sheet.row_values(index)[0]
            dataset = tablib.Dataset()
            if names == "连续交易系列分析":
                index += 1
                dataset.headers = ["index", "value"]
            else:
                index += 2
                dataset.headers = self._get_headers(sheet, index)
            index += 1
            if names != "连续交易系列统计":
                index = self._get_rows(dataset, sheet, index, empty) + 2
                data[names] = self._get_index_dict(dataset)
                # print(dataset.csv)
            else:
                data[names] = {}
                index = self._get_rows(dataset, sheet, index, lambda x: isinstance(x[0], str))
                data[names][dataset.headers[0]] = dataset.dict
                # print(dataset.csv)
                dataset = tablib.Dataset()
                dataset.headers = self._get_headers(sheet, index)
                index = self._get_rows(dataset, sheet, index + 1, empty)
                data[names][dataset.headers[0]] = dataset.dict
                # print(dataset.csv)
                break
        return data

    @property
    @_cache("info")
    def info(self):
        def get_value(key, value):
            if value == '':
                return None
            elif "日期" in key:
                return get_timestamp(value)
            else:
                return value

        sheet = self._work_book.sheet_by_name("设置")
        index = 2
        ncol = 2
        data = {}
        empty = lambda l: reduce(lambda x, y: x and y == '', l, True)
        while index < sheet.nrows:
            values = sheet.row_values(index)[:ncol]
            if empty(values):
                break
            data[values[0]] = get_value(values[0], values[1])
            index += 1
        return data

    @property
    @_cache("period_analysis")
    def period_analysis(self):
        sheet = self._work_book.sheet_by_name("周期分析")
        data = {}
        index = 0
        empty = lambda l: reduce(lambda x, y: x and y == '', l, True)
        while index < sheet.nrows:
            names = sheet.row_values(index)[0]
            if names == "月化收益和潜在亏损":
                break
            else:
                dataset = tablib.Dataset()
                index += 2
                dataset.headers = self._get_headers(sheet, index)
                index += 1
                index = self._get_rows(dataset, sheet, index, empty) + 2
                if names != "月份分析":
                    data[names] = dataset.dict
                    # print(dataset.csv)
        return data

    @property
    @_cache("strategy_analysis")
    def strategy_analysis(self):
        sheet = self._work_book.sheet_by_name("策略分析")
        data = {}
        index = 0
        empty = lambda l: reduce(lambda x, y: x and y == '', l, True)
        while index < sheet.nrows:
            names = sheet.row_values(index)[0]
            if names == "详细权益曲线":
                break
            else:
                dataset = tablib.Dataset()
                if names == "策略绩效概要":
                    index += 2
                    dataset.headers = self._get_headers(sheet, index)
                else:
                    index += 1
                    dataset.headers = ["index", "value"]
                index += 1
                index = self._get_rows(dataset, sheet, index, empty) + 2
                data[names] = self._get_index_dict(dataset)
                # print(dataset.csv)
        return data


if __name__ == "__main__":
    import os
    import json
    import sys

    e = PerformanceExtractor()
    file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data",
                        "CFFEX.IF HOT  MACD LE_ MACD SE 策略回测绩效报告.xls")
    e.open_with_name(file)
    # print(e.trade_details[0])
    # print(json.dumps(e.trade_analysis))
    # print(json.dumps(e.period_analysis))
    print(e.strategy)
    # print(e.info)
    # print(e.trade_details)
    # print(sys.getsizeof(e.trade_details))
    # print(e.strategy_analysis)
