from connection import MongoConnection
import pymongo

INDEX_TUPLE = {
    "年化收益率": [("strategy_analysis.策略绩效概要.年化收益利率.所有交易", pymongo.DESCENDING)],
    "风报比": [("strategy_analysis.策略绩效概要.策略最大潜在亏损收益比.所有交易", pymongo.DESCENDING)],
    "最大回撤率": [("strategy_analysis.策略绩效概要.策略最大潜在亏损（%）.所有交易", pymongo.DESCENDING)],
    "总盈利": [("strategy_analysis.策略绩效概要.账户资金收益比.所有交易", pymongo.DESCENDING)],
    "夏普比率": [("strategy_analysis.绩效比率.夏普比率.value", pymongo.DESCENDING)],
    "索提诺比率": [("strategy_analysis.绩效比率.Sortino 比率.value", pymongo.DESCENDING)],
    "Calmar比率": [("strategy_analysis.绩效比率.Calmar 比率.value", pymongo.DESCENDING)],
    "胜率": [("trade_analysis.总体交易分析.% 胜率.所有交易", pymongo.DESCENDING)],
    "平均盈利": [("trade_analysis.总体交易分析.平均盈利额.所有交易", pymongo.DESCENDING)],
    "当月收益率": [("period_analysis.Monthly Rolling Period Analysis.0.盈利（%）", pymongo.DESCENDING)],
    "策略名称": [("info.策略名称", pymongo.ASCENDING)],
}

if __name__ == "__main__":
    c = MongoConnection()
    indexes = [pymongo.IndexModel(v, name=k) for k, v in INDEX_TUPLE.items()]
    c.collection.create_indexes(indexes)
    print(c.collection.index_information())
