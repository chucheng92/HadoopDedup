package com.ryan.boot;

import java.io.IOException;

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

import com.ryan.util.Md5Util;

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

	// input-key is image file path
	// input-value is image binary content
	private static class FileLevelDedupMapper extends
			Mapper<Text, BytesWritable, Text, Text> {

		@Override
		protected void map(Text key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String md5 = Md5Util.getMd5(value.getBytes());
			Text md5Text = new Text(md5);
			
			// output-key is md5 hash
			// output-value is image file path
			context.write(md5Text, key);
		}
	}

	private static class FileLevelDedupReducer extends
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
}
