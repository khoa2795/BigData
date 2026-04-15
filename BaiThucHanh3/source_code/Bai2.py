from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

OUTPUT_FILE = "/home/khoa/BigData/BaiThucHanh3/output/output_bai2.txt"


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


movies_rdd = sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/movies (2).txt").map(parse_movies)
ratings_rdd = (
     sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_1 (2).txt")
     .union(sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_2 (2).txt"))
    .map(parse_ratings)
)

movie_genres_rdd = movies_rdd.map(lambda x: (x[0], x[2].split("|")))
movie_rating_rdd = ratings_rdd.map(lambda x: (x[1], x[2]))

genre_rating_pairs = (
    movie_rating_rdd
    .join(movie_genres_rdd)
    .flatMap(lambda x: [(genre, (x[1][0], 1)) for genre in x[1][1]])
)

genre_stats = (
    genre_rating_pairs
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: (v[0] / v[1], v[1]))
)

top_10 = genre_stats.takeOrdered(10, key=lambda r: -r[1][0])

print("Top 10 genres theo avg rating:")
for row in top_10:
    print(row)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("Top 10 genres theo avg rating:\n")
    for row in top_10:
        f.write(f"{row}\n")

print(f"Da xuat file: {OUTPUT_FILE}")
