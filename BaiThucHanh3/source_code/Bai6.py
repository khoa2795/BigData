from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

OUTPUT_FILE = "/home/khoa/BigData/BaiThucHanh3/output/output_bai6.txt"


def parse_ratings(line):
     parts = line.strip().split(",")
     user_id = parts[0]
     movie_id = parts[1]
     rating = float(parts[2])
     timestamp = int(parts[3])
     return user_id, movie_id, rating, timestamp


def timestamp_to_year(timestamp):
     return datetime.utcfromtimestamp(timestamp).year


ratings_rdd = (
     sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_1 (2).txt")
     .union(sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_2 (2).txt"))
    .map(parse_ratings)
)

year_pairs = ratings_rdd.map(lambda x: (timestamp_to_year(x[3]), (x[2], 1)))

year_stats = (
    year_pairs
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: (v[0] / v[1], v[1]))
    .sortByKey()
)

rows = year_stats.collect()

print("Thong ke rating theo nam:")
for row in rows:
    print(row)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("Thong ke rating theo nam:\n")
    for row in rows:
        f.write(f"{row}\n")

print(f"Da xuat file: {OUTPUT_FILE}")
