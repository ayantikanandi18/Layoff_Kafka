from pytrends.request import TrendReq
import pandas as pd

pytrends = TrendReq()
keywords = ["layoffs", "unemployment", "recession"]
pytrends.build_payload(kw_list=keywords, timeframe="now 1-H")
df = pytrends.interest_over_time()
print(df.tail(10))  # Check the last 10 rows
