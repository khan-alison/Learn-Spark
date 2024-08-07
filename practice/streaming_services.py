from pytrends.request import TrendReq
import pandas as pd

# Khởi tạo pytrends
pytrends = TrendReq(hl='en-US', tz=360)

# Định nghĩa từ khóa cần tìm kiếm
kw_list = ["machine learning"]  # Chỉ một từ khóa mỗi lần

# Xây dựng payload
pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo='')

# Lấy dữ liệu xu hướng theo thời gian
data = pytrends.interest_over_time()
print("Xu hướng theo thời gian:")
print(data.head())

# Lấy dữ liệu xu hướng theo khu vực
region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
print("\nXu hướng theo khu vực:")
print(region_data.head())

# Lấy các truy vấn liên quan
related_queries = pytrends.related_queries()
print("\nCác truy vấn liên quan:")
if related_queries[kw_list[0]]['top'] is not None:
    print(related_queries[kw_list[0]]['top'].head())
else:
    print("Không có dữ liệu")

# Lấy các chủ đề liên quan
related_topics = pytrends.related_topics()
print("\nCác chủ đề liên quan:")
if related_topics[kw_list[0]]['top'] is not None:
    print(related_topics[kw_list[0]]['top'].head())
else:
    print("Không có dữ liệu")

# Lưu dữ liệu vào file CSV
data.to_csv('trend_data.csv')