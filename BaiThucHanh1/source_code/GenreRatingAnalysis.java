import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Bai2 {

    // =========================================================
    // MAPPER
    // Input : Ratings (UserID, MovieID, Rating, Timestamp)
    // Distributed Cache : Movies (MovieID, Title, Genres)
    // Emits : (Genre -> Rating)
    // =========================================================
    public static class GenreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        // HashMap để tra cứu thể loại của MovieID hiện tại
        private Map<String, String> movieGenresMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    // Lấy tên file local khi Hadoop đưa file cache về worker node
                    String localFileName = Paths.get(cacheFile.getPath()).getFileName().toString();
                    loadMovieGenres(localFileName);
                }
            }
        }

        private void loadMovieGenres(String filePath) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Cấu trúc file movies.txt: MovieID, Title, Genres
                String[] parts = line.split(",\\s*", 3);
                if (parts.length >= 3) {
                    String movieId = parts[0].trim();
                    String genres  = parts[2].trim();
                    movieGenresMap.put(movieId, genres);
                }
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*");
            if (parts.length < 3) return;

            try {
                String movieId = parts[1].trim();
                double rating  = Double.parseDouble(parts[2].trim());

                // So sánh MovieID để lấy ra các thể loại phim (Genres)
                String genresStr = movieGenresMap.get(movieId);
                if (genresStr != null && !genresStr.isEmpty()) {
                    
                    // Phân tách các thể loại vì một phim có thể có nhiều thể loại nối bằng "|"
                    String[] genres = genresStr.split("\\|");
                    
                    for (String genre : genres) {
                        // Phát kết nối: Key là Thể loại, Value là Điểm đánh giá (Rating)
                        context.write(new Text(genre.trim()), new DoubleWritable(rating));
                    }
                }

            } catch (NumberFormatException e) {
                // Bỏ qua nếu dữ liệu không phải là số
            }
        }
    }

    // =========================================================
    // REDUCER
    // Input : (Genre, [rating1, rating2, rating3, ...])
    // Output: Genre   Avg: xx, Count: xx
    // =========================================================
    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalRating = 0.0;
            int    count       = 0;

            for (DoubleWritable val : values) {
                totalRating += val.get();
                count++;
            }

            if (count == 0) return;

            // Tính điểm trung bình
            double avgRating = totalRating / count;

            // Định dạng chuỗi kết quả: "Avg: 3.85, Count: 30"
            String outputValue = String.format("Avg: %.2f, Count: %d", avgRating, count);
            
            // Định dạng lề cho Key (Tên thể loại) luôn chiếm 11 ký tự để thẳng hàng giống y hệt hình mẫu
            String formattedKey = String.format("%-11s", key.toString());
            
            context.write(new Text(formattedKey), new Text(outputValue));
        }
    }

    // =========================================================
    // DRIVER
    // =========================================================
    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: Bai2 <ratings_1> <ratings_2> <movies_file> <output_dir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        
        // Cấu hình dấu cách (space) làm khoảng trắng mặc định thay cho dấu TAB (\t) để thẳng hàng tuyệt đối
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        
        Job job = Job.getInstance(conf, "Genre Rating Analysis");

        job.setJarByClass(Bai2.class);
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        // Khai báo kiểu dữ liệu đầu ra riêng của Mapper (Vì đầu ra Mapper khác đầu ra Job)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Khai báo kiểu dữ liệu đầu ra chính thức của Job/Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Khai báo hai file ratings làm nguồn dữ liệu đầu vào
        FileInputFormat.addInputPath(job, new Path(args[0])); // ratings_1.txt
        FileInputFormat.addInputPath(job, new Path(args[1])); // ratings_2.txt

        // Cấp phát file movies.txt lên thanh RAM (Distributed Cache) cho các node đọc
        job.addCacheFile(new Path(args[2]).toUri());           // movies.txt

        // Khai báo đường dẫn kết xuất
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Lệnh chạy
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
