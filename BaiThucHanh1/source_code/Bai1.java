import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.nio.file.Paths;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Bai1 {

    // =========================================================
    // MAPPER: Ratings files (ratings_1.txt & ratings_2.txt)
    // Input format: UserID, MovieID, Rating, Timestamp
    // Emits: (MovieID -> Rating)
    // =========================================================
    public static class RatingsMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Split by comma with optional surrounding spaces
            String[] parts = line.split(",\\s*");
            if (parts.length < 3) return;

            try {
                String movieId = parts[1].trim();
                String rating  = parts[2].trim();

                // Validate rating is a valid number
                Double.parseDouble(rating);

                // Emit: key=MovieID, value=rating value
                context.write(new Text(movieId), new Text(rating));
            } catch (NumberFormatException e) {
                // Skip malformed lines
            }
        }
    }

    // =========================================================
    // REDUCER
    // Input:  (MovieID, [rating1, rating2, ...])
    // Output: "MovieTitle  AverageRating: xx (TotalRatings: xx)"
    //
    // Uses Distributed Cache to look up movie titles from movies.txt
    // Tracks maxMovie and maxRating among films with >= 5 ratings
    // =========================================================
    public static class RatingsReducer extends Reducer<Text, Text, Text, Text> {

        // Class-level variables for tracking the top-rated movie
        private String  maxMovie  = "";
        private double  maxRating = -1.0;

        // Movie ID -> Title lookup map (loaded from Distributed Cache)
        private Map<String, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load movies.txt from the Distributed Cache.
            // Hadoop localizes the file to the task's working directory
            // using just the filename — so we extract only the filename from the URI.
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    // Get just the filename (e.g. "movies.txt")
                    String localFileName = Paths.get(cacheFile.getPath()).getFileName().toString();
                    loadMovieMap(localFileName);
                }
            }
        }

        private void loadMovieMap(String filePath) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Format: MovieID, Title, Genres
                String[] parts = line.split(",\\s*", 3);
                if (parts.length >= 2) {
                    String movieId = parts[0].trim();
                    String title   = parts[1].trim();
                    movieMap.put(movieId, title);
                }
            }
            br.close();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalRating = 0.0;
            int    count       = 0;

            for (Text val : values) {
                try {
                    totalRating += Double.parseDouble(val.toString().trim());
                    count++;
                } catch (NumberFormatException e) {
                    // Skip bad values
                }
            }

            if (count == 0) return;

            double avgRating = totalRating / count;

            // Resolve movie title from the lookup map
            String movieId    = key.toString();
            String movieTitle = movieMap.getOrDefault(movieId, "MovieID_" + movieId);

            // Format output: "MovieTitle  AverageRating: xx (TotalRatings: xx)"
            String outputValue = String.format("AverageRating: %.2f (TotalRatings: %d)", avgRating, count);
            context.write(new Text(movieTitle), new Text(outputValue));

            // Track highest-rated movie among those with >= 5 ratings
            if (count >= 5 && avgRating > maxRating) {
                maxRating = avgRating;
                maxMovie  = movieTitle;
            }
        }

        // Called once after all reduce() calls — output the top movie
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String result = String.format(
                    "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.",
                    maxMovie, maxRating
                );
                context.write(new Text("\n[TOP RATED]"), new Text(result));
            }
        }
    }

    // =========================================================
    // DRIVER
    // =========================================================
    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: Bai1 <ratings_1> <ratings_2> <movies_file> <output_dir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(Bai1.class);
        job.setMapperClass(RatingsMapper.class);
        job.setReducerClass(RatingsReducer.class);

        // Output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add both ratings files as input
        FileInputFormat.addInputPath(job, new Path(args[0])); // ratings_1.txt
        FileInputFormat.addInputPath(job, new Path(args[1])); // ratings_2.txt

        // Add movies.txt to Distributed Cache for title lookup in Reducer
        job.addCacheFile(new Path(args[2]).toUri());           // movies.txt

        // Output directory
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Run and wait for completion
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
