package com.nothankyou.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Mac地址测试程序
 *
 * @author 哓哓
 */
public class Test1 extends Configured implements Tool {

    private static Logger log = LoggerFactory.getLogger(Test1.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Test1(), args);

        System.exit(res);
    }

    enum Counter {
        LINESKIP,
    }

    public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            try {
                String[] lineArr = line.split(" ");
                String time = lineArr[0] + " " + lineArr[1] + " " + lineArr[2];
                String macAddress = lineArr[6];

                Text out = new Text(time  + " " + macAddress);

                context.write(NullWritable.get(), out); // 输出key\tvalue
            } catch (ArrayIndexOutOfBoundsException e) {
                context.getCounter(Counter.LINESKIP).increment(1); // 出错令计数器加1
                return;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, "Job_Test1");
        job.setJarByClass(Test1.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);          // 指定输出的key的格式
        job.setOutputValueClass(Text.class);                // 指定输出的value的格式

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }
}
