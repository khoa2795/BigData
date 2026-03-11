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

public class Bai4 {

    public static class AgeGroupMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, Integer> userAgeMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            for (URI cacheFile : cacheFiles) {
                String localFileName = Paths.get(cacheFile.getPath()).getFileName().toString();
                if ("users.txt".equals(localFileName)) {
                    loadUserAges(localFileName);
                }
            }
        }

        private void loadUserAges(String filePath) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    // users.txt: UserID, Gender, Age, Occupation, Zip-code
                    String[] parts = line.split(",\\s*");
                    if (parts.length >= 3) {
                        try {
                            String userId = parts[0].trim();
                            int age = Integer.parseInt(parts[2].trim());
                            userAgeMap.put(userId, age);
                        } catch (NumberFormatException ignored) {
                            // Skip malformed ages.
                        }
                    }
                }
            }
        }

        private String toAgeGroup(int age) {
            if (age <= 18) {
                return "0-18";
            }
            if (age <= 35) {
                return "18-35";
            }
            if (age <= 50) {
                return "35-50";
            }
            return "50+";
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

            Integer age = userAgeMap.get(userId);
            if (age == null) {
                return;
            }

            try {
                double rating = Double.parseDouble(ratingStr);
                String ageGroup = toAgeGroup(age);
                context.write(new Text(movieId), new Text(ageGroup + ":" + rating));
            } catch (NumberFormatException ignored) {
                // Skip malformed rating values.
            }
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {

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

            double sum0_18 = 0.0;
            int count0_18 = 0;
            double sum18_35 = 0.0;
            int count18_35 = 0;
            double sum35_50 = 0.0;
            int count35_50 = 0;
            double sum50Plus = 0.0;
            int count50Plus = 0;

            for (Text value : values) {
                String[] token = value.toString().split(":", 2);
                if (token.length != 2) {
                    continue;
                }

                String ageGroup = token[0].trim();
                try {
                    double rating = Double.parseDouble(token[1].trim());
                    switch (ageGroup) {
                        case "0-18":
                            sum0_18 += rating;
                            count0_18++;
                            break;
                        case "18-35":
                            sum18_35 += rating;
                            count18_35++;
                            break;
                        case "35-50":
                            sum35_50 += rating;
                            count35_50++;
                            break;
                        case "50+":
                            sum50Plus += rating;
                            count50Plus++;
                            break;
                        default:
                            break;
                    }
                } catch (NumberFormatException ignored) {
                    // Skip malformed values.
                }
            }

            String movieId = key.toString();
            String movieTitle = movieTitleMap.getOrDefault(movieId, "MovieID_" + movieId);

            String avg0_18 = count0_18 > 0 ? String.format("%.2f", sum0_18 / count0_18) : "N/A";
            String avg18_35 = count18_35 > 0 ? String.format("%.2f", sum18_35 / count18_35) : "N/A";
            String avg35_50 = count35_50 > 0 ? String.format("%.2f", sum35_50 / count35_50) : "N/A";
            String avg50Plus = count50Plus > 0 ? String.format("%.2f", sum50Plus / count50Plus) : "N/A";

            String output = String.format("[0-18: %s, 18-35: %s, 35-50: %s, 50+: %s]",
                    avg0_18, avg18_35, avg35_50, avg50Plus);
            context.write(new Text(movieTitle), new Text(output));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: Bai4 <ratings_1> <ratings_2> <movies_file> <users_file> <output_dir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analysis");

        job.setJarByClass(Bai4.class);
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

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
