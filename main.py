import os
from extractor import PerformanceExtractor
from connection import MongoConnection

if __name__ == "__main__":
    e = PerformanceExtractor()
    file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data",
                        "CFFEX.IF HOT  MACD LE_ MACD SE 策略回测绩效报告.xls")
    e.open_with_name(file)
    strategy = e.strategy
    collection = MongoConnection().collection
    result = collection.update_one({"info.策略名称": strategy["info"]["策略名称"]},
                                   {"$set": strategy},
                                   upsert=True)
    print(result.matched_count, result.matched_count, result.upserted_id)
