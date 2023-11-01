import java.io.IOException;
import java.nio.charset.StandardCharsets;
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


public class RecordsDriver extends Configured implements Tool
{
    static String GENRE_SEPARATOR = "\u0001";

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int res = ToolRunner.run(new RecordsDriver(), args);
        long endTime = System.currentTimeMillis();
        long executionTime = (endTime - startTime) / 1000;
        System.out.println("Execution time: ~" + executionTime + " seconds");
        System.exit(res);
    }
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(getConf(), "Records");
        job.setJarByClass(this.getClass());

        job.setMapperClass(RecordMapper.class);
        job.setCombinerClass(RecordReducer.class);
        job.setReducerClass(RecordReducer.class);

        job.setMapOutputKeyClass(RecordsKey.class);
        job.setMapOutputValueClass(Record.class);

        job.setOutputKeyClass(RecordsKey.class);
        job.setOutputValueClass(Record.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RecordMapper extends Mapper<LongWritable, Text, RecordsKey, Record> {
        @Override
        public void map(LongWritable offset, Text line, Context context) {
            try {
                String textLine = new String(line.getBytes(), 0, line.getLength(), StandardCharsets.UTF_8);
                // "\u0001"
                String[] columns = textLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if (columns.length == 13) {
                    int label_id = Integer.parseInt(columns[6]);
                    int artist_id = Integer.parseInt(columns[4]);
                    String artist_name = columns[5];
                    int release_date = Integer.parseInt(columns[11]);
                    int decade = release_date / 10 * 10;
                    String genre = columns[8];// + GENRE_SEPARATOR;

                    RecordsKey key = new RecordsKey(label_id, artist_id, artist_name, decade);
                    Record record = new Record(1, genre.trim());

                    context.write(key, record);
                }
            } catch (Exception e) {
                 e.printStackTrace();
            }
        }
    }

    public static class RecordReducer extends Reducer<RecordsKey, Record, RecordsKey, Record> {
        @Override
        public void reduce(RecordsKey key, Iterable<Record> values, Context context) throws IOException, InterruptedException {
            Record resultRecord = new Record();
            for (Record value : values) {
                resultRecord.merge(value);
            }
            context.write(key, resultRecord);
        }
    }
}
