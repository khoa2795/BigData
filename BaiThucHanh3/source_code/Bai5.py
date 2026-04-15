from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

OUTPUT_FILE = "/home/khoa/BigData/BaiThucHanh3/output/output_bai5.txt"


def parse_ratings(line):
     parts = line.strip().split(",")
     user_id = parts[0]
     movie_id = parts[1]
     rating = float(parts[2])
     timestamp = int(parts[3])
     return user_id, movie_id, rating, timestamp


def parse_users(line):
     parts = line.strip().split(",")
     user_id = parts[0]
     gender = parts[1]
     age = int(parts[2])
     occupation = parts[3]
     zipcode = parts[4]
     return user_id, gender, age, occupation, zipcode


def parse_occupation(line):
     parts = line.strip().split(",", 1)
     occ_id = parts[0]
     occ_name = parts[1]
     return occ_id, occ_name


ratings_rdd = (
     sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_1 (2).txt")
     .union(sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_2 (2).txt"))
    .map(parse_ratings)
)
users_rdd = sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/users (2).txt").map(parse_users)
occupation_rdd = sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/occupation.txt").map(parse_occupation)

ratings_by_user_rdd = ratings_rdd.map(lambda x: (x[0], x[2]))
user_occid_rdd = users_rdd.map(lambda x: (x[0], x[3]))

occ_rating_pairs = (
    ratings_by_user_rdd
    .join(user_occid_rdd)
    .map(lambda x: (x[1][1], (x[1][0], 1)))
)

occ_stats = (
    occ_rating_pairs
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: (v[0] / v[1], v[1]))
)

result = occ_stats.join(occupation_rdd).map(
    lambda x: (x[0], x[1][1], x[1][0][0], x[1][0][1])
)

top_10 = result.takeOrdered(10, key=lambda r: -r[2])

print("Top 10 occupations theo avg rating:")
for row in top_10:
    print(row)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("Top 10 occupations theo avg rating:\n")
    for row in top_10:
        f.write(f"{row}\n")

print(f"Da xuat file: {OUTPUT_FILE}")
