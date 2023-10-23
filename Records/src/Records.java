import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Records extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Records(), args);
        System.exit(res);
    }
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(getConf(), "Records");
        job.setJarByClass(this.getClass());

        job.setMapperClass(RecordMapper.class);
        // job.setCombinerClass(RecordCombiner.class);
        job.setReducerClass(RecordReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RecordMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Logger logger = Logger.getLogger(RecordMapper.class);
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            PropertyConfigurator.configure("log4j.properties");
            super.setup(context);
        }
        private Text compositeKey = new Text();
        private Text genreKey = new Text();

        public void map(LongWritable offset, Text value, Context context) {
            try {
                String[] columns = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                if (columns.length == 13) {
                    String label_id = columns[6];
                    String artist_id = columns[4];
                    String artist_name = columns[5];
                    int release_date = Integer.parseInt(columns[11]);
                    int decade = release_date / 10 * 10;
                    String genre = columns[8];
                    compositeKey.set(label_id + "," + artist_id + "," + artist_name + "," + decade);
                    genreKey.set(genre);
                    context.write(compositeKey, genreKey);
                }
            } catch (Exception e) {
                logger.error("Error in Mapper:", e);
                // e.printStackTrace();
            }
        }
    }

    public static class RecordReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int record_count = 0;
            Set<String> genres = new HashSet<>();

            for (Text value : values) {
                record_count++;
                genres.add(value.toString());
            }

            String[] key_split = key.toString().split(",");
            if (key_split.length == 4) {
                String label_id = key_split[0], artist_id = key_split[1], artist_name = key_split[2], decade = key_split[3];
                String genresList = "[" + String.join(",", genres) + "]";

                String resultKey = label_id + "," + artist_id + "," + artist_name + "," + decade;
                String resultValue = "," + record_count + "," + genresList;

                context.write(new Text(resultKey), new Text(resultValue));
            }
        }
    }

//    TODO: create combiner
//    public static class RecordCombiner
//            extends Reducer<Text,IntWritable,Text,IntWritable> {
//        // ...
//    }

}