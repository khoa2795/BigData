import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Bai3 {

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            for (URI cacheFile : cacheFiles) {
                String localFileName = Paths.get(cacheFile.getPath()).getFileName().toString();
                if ("users.txt".equals(localFileName)) {
                    loadUserGender(localFileName);
                }
            }
        }

        private void loadUserGender(String filePath) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    // users.txt: UserID, Gender, Age, Occupation, Zip-code
                    String[] parts = line.split(",\\s*");
                    if (parts.length >= 2) {
                        userGenderMap.put(parts[0].trim(), parts[1].trim().toUpperCase());
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = line.split(",\\s*");
            if (parts.length < 3) {
                return;
            }

            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String ratingStr = parts[2].trim();

            String gender = userGenderMap.get(userId);
            if (gender == null) {
                return;
            }

            try {
                double rating = Double.parseDouble(ratingStr);
                context.write(new Text(movieId), new Text(gender + ":" + rating));
            } catch (NumberFormatException ignored) {
                // Skip malformed rating values.
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {

        private final Map<String, String> movieTitleMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            for (URI cacheFile : cacheFiles) {
                String localFileName = Paths.get(cacheFile.getPath()).getFileName().toString();
                if ("movies.txt".equals(localFileName)) {
                    loadMovieTitles(localFileName);
                }
            }
        }

        private void loadMovieTitles(String filePath) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    // movies.txt: MovieID, Title, Genres
                    String[] parts = line.split(",\\s*", 3);
                    if (parts.length >= 2) {
                        movieTitleMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double maleSum = 0.0;
            int maleCount = 0;
            double femaleSum = 0.0;
            int femaleCount = 0;

            for (Text value : values) {
                String[] token = value.toString().split(":", 2);
                if (token.length != 2) {
                    continue;
                }

                String gender = token[0].trim();
                try {
                    double rating = Double.parseDouble(token[1].trim());
                    if ("M".equals(gender)) {
                        maleSum += rating;
                        maleCount++;
                    } else if ("F".equals(gender)) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                } catch (NumberFormatException ignored) {
                    // Skip malformed values.
                }
            }

            String movieId = key.toString();
            String movieTitle = movieTitleMap.getOrDefault(movieId, "MovieID_" + movieId);

            String maleAvg = maleCount > 0 ? String.format("%.2f", maleSum / maleCount) : "N/A";
            String femaleAvg = femaleCount > 0 ? String.format("%.2f", femaleSum / femaleCount) : "N/A";

            context.write(new Text(movieTitle), new Text("Male_Avg: " + maleAvg + ", Female_Avg: " + femaleAvg));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: Bai3 <ratings_1> <ratings_2> <movies_file> <users_file> <output_dir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analysis");

        job.setJarByClass(Bai3.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(args[2]).toUri()); // movies.txt
        job.addCacheFile(new Path(args[3]).toUri()); // users.txt

        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
