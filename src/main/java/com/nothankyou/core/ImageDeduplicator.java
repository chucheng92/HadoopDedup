package com.nothankyou.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nothankyou.util.Md5Util;

public class ImageDeduplicator extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new ImageDeduplicator(), args);
		
		System.exit(res);
	}

	enum Counter {
		LINESKIP,
	}

	// input-key is image file-path
	// input-value is image binary-content
	private static class ImageDedupMapper extends
			Mapper<Text, BytesWritable, Text, Text> {

		@Override
		protected void map(Text key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String md5 = Md5Util.getMd5(value.getBytes());
			Text md5Text = new Text(md5);
			
			// output-key is md5 hash
			// output-value is image file-path
			context.write(md5Text, key);
		}
	}

	private static class ImageDedupReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Text imageFilePath = null;
			for (Text value:values) {
				imageFilePath = value;
				break;
			}	
			context.write(imageFilePath, key);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf, "Job_ImageDeduplicator");
		job.setJarByClass(ImageDeduplicator.class);
		job.setMapperClass(ImageDedupMapper.class);
		job.setReducerClass(ImageDedupReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);

		return job.isSuccessful() ? 0 : 1;
	}

}
