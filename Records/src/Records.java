import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

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
        job.setCombinerClass(RecordReducer.class);
        job.setReducerClass(RecordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RecordMapper extends Mapper<Object, Text, Text, Text> {
        private Text compositeKey = new Text();
        private Text genreAndArtist = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] columns = value.toString().split(",");
                if (columns.length == 12) {
                    String labelId = columns[6];
                    String artistId = columns[4];
                    String artistName = columns[5];
                    int releaseDate = Integer.parseInt(columns[11]);
                    int decade = releaseDate / 10 * 10;
                    compositeKey.set(labelId + "," + artistId + "," + decade);
                    String genre = columns[8];
                    genreAndArtist.set(genre + "," + artistName);
                    context.write(compositeKey, genreAndArtist);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class RecordReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        // ...
    }
}