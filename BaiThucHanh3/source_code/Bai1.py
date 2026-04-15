from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

OUTPUT_FILE = "/home/khoa/BigData/BaiThucHanh3/output/output_bai1.txt"

def parse_movies(movies):
     parts = movies.strip().split(",", 2)
     movie_id = parts[0]
     title = parts[1]
     genres = parts[2]
     return movie_id, title, genres

def parse_ratings(ratings):
     parts = ratings.strip().split(",")
     user_id = parts[0]
     movie_id = parts[1]
     rating = float(parts[2])
     timestamp = int(parts[3])
     return user_id, movie_id, rating, timestamp

# Read
movies_rdd = sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/movies (2).txt").map(parse_movies)
ratings_rdd = (
     sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_1 (2).txt")
     .union(sc.textFile("file:///home/khoa/BigData/BaiThucHanh3/Bai_thuc_hanh/ratings_2 (2).txt"))
    .map(parse_ratings)
)

# Mapping movie

"""
def movie_to_title(row):
    return(row[0], row[1])

movie_title_rdd = movies_rdd.map(movie_to_title)
"""

movie_title_rdd = movies_rdd.map(lambda x: (x[0], x[1]))

# Reduce

movie_stats = (
    ratings_rdd
    .map(lambda x: (x[1], (x[2], 1)))
    .reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: (v[0] / v[1], v[1]))
)

# Join movie_id, title, avg, count

result = movie_stats.join(movie_title_rdd).map(
    lambda x: (x[0], x[1][1], x[1][0][0], x[1][0][1])
)

# Filter phim có điều kiện count
min_ratings = 5
qualified = result.filter(lambda r: r[3] >= min_ratings)

best_avg = qualified.takeOrdered(1, key=lambda r: -r[2])
top_10 = qualified.take(10)

print("Top các phim có lượt ratings >= 5")
for movie in top_10:
    print(movie)
print('\nPhim có avg cao nhất')
print(best_avg[0])

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("Top các phim có lượt ratings >= 5\n")
    for movie in top_10:
        f.write(f"{movie}\n")
    f.write("\nPhim có avg cao nhất\n")
    if best_avg:
        f.write(f"{best_avg[0]}\n")
    else:
        f.write("Khong co phim nao thoa dieu kien\n")

print(f"Da xuat file: {OUTPUT_FILE}")