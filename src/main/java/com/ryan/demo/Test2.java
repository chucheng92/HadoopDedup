package com.nothankyou.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 电话清单倒排索引Test Program
 */
public class Test2 extends Configured implements Tool {

    enum Counter {
        LINESKIP,
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Test2(), args);

        System.exit(res);
    }

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            try {
                String[] lineArr = line.split(" ");
                String number1 = lineArr[0];
                String number2 = lineArr[1];

                context.write(new Text(number2), new Text(number1));
            } catch (ArrayIndexOutOfBoundsException e) {
                context.getCounter(Counter.LINESKIP).increment(1);
                return;
            }
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String out = "";
            String tempValue;

            for (Text value: values) {
                tempValue = value.toString();
                out += tempValue + "|";
            }

            context.write(key, new Text(out));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, "Job_Test2");
        job.setJarByClass(Test2.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }
}
