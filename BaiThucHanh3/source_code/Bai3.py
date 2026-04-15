from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

OUTPUT_FILE = "/home/khoa/BigData/BaiThucHanh3/output/output_bai3.txt"


def parse_movies(line):
     parts = line.strip().split(",", 2)
     movie_id = parts[0]
     title = parts[1]
     genres = parts[2]
     return movie_id, title, genres


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


movies_rdd = sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/movies (2).txt").map(parse_movies)
ratings_rdd = (
     sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_1 (2).txt")
     .union(sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_2 (2).txt"))
    .map(parse_ratings)
)
users_rdd = sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/users (2).txt").map(parse_users)

movie_title_rdd = movies_rdd.map(lambda x: (x[0], x[1]))
user_gender_rdd = users_rdd.map(lambda x: (x[0], x[1]))
ratings_by_user_rdd = ratings_rdd.map(lambda x: (x[0], (x[1], x[2])))

movie_gender_pairs = (
    ratings_by_user_rdd
    .join(user_gender_rdd)
    .map(lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1)))
)

movie_gender_stats = (
    movie_gender_pairs
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: (v[0] / v[1], v[1]))
)

result = movie_gender_stats.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
).join(movie_title_rdd).map(
    lambda x: (x[0], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2])
)

top_20 = result.takeOrdered(20, key=lambda r: -r[3])

print("Top 20 movie-gender theo avg rating:")
for row in top_20:
    print(row)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("Top 20 movie-gender theo avg rating:\n")
    for row in top_20:
        f.write(f"{row}\n")

print(f"Da xuat file: {OUTPUT_FILE}")
