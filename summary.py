import os
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.abspath(__file__))


class Summary(object):
    def __init__(self, *relative):
        self._relative = relative
        self._absolute = os.path.join(ROOT, *relative)

    def generate(self):
        files = [os.path.basename(p) for p in os.listdir(self._absolute) if p.endswith(".csv")]
        df = pd.DataFrame(pd.Series(files, name="Location"))
        df["VendorID"] = ["1"] * len(df)
        df["StrategyID"] = np.arange(1, len(df) + 1)
        df = df.reindex(columns=["VendorID", "StrategyID", "Location"])
        df["Location"] = df["Location"].map(lambda x: os.path.join(".", *(self._relative + (x, ))))
        return df

if __name__ == "__main__":
    Summary("record", "order").generate().to_csv("summary_order.csv", index=False)
    Summary("record", "position").generate().to_csv("summary_position.csv", index=False)
