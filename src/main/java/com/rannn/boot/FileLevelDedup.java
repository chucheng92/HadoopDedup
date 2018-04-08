package com.rannn.boot;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import com.rannn.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class FileLevelDedup {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: FileLevelDedup <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Job_FileLevelDedup");
        job.setJarByClass(FileLevelDedup.class);
        job.setMapperClass(FileLevelDedupMapper.class);
        job.setReducerClass(FileLevelDedupReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);

        System.exit(job.isSuccessful() ? 0 : 1);
    }

    enum Counter {
        LINESKIP,
    }

    /**
     * input-key is file path
     * input-value is binary content
     */
    private static class FileLevelDedupMapper extends
            Mapper<Text, BytesWritable, Text, Text> {

        @Override
        protected void map(Text key, BytesWritable value,
                           Context context) throws IOException, InterruptedException {
            String md5 = null;
            try {
                md5 = StringUtils.getMd5(value.getBytes());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Text md5Text = new Text(md5);

            context.write(md5Text, key);
        }
    }

    private static class FileLevelDedupReducer extends
            Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            Text imageFilePath = null;
            for (Text value : values) {
                imageFilePath = value;
                break;
            }
            context.write(imageFilePath, key);
        }
    }
}
