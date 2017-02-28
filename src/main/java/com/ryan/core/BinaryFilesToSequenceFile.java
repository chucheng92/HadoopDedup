package com.nothankyou.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this class make all of small files(imgs) -> one sequence file
 * 
 * input:one single img kv file(included more img paths) output:one single
 * sequence file
 * 
 * @author taoxiaoran
 * @date 2017-2-6
 */
public class BinaryFilesToSequenceFile extends Configured implements Tool {

	private static Logger logger = LoggerFactory
			.getLogger(BinaryFilesToSequenceFile.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: BinaryFilesToSequenceFile <in path for kv file> <out path for sequence file>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://Master.Hadoop:9000");
		// make sure client side replication equals 1
		// if not, even if the hdfs-site.xml replication equals 1,
		// the replication of this file in hdfs equals 3 either
		conf.set("dfs.replication", "1");
		int res = ToolRunner.run(conf, new BinaryFilesToSequenceFile(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf, "Job_CreateSequenceFileMapper");
		job.setJarByClass(BinaryFilesToSequenceFile.class);
		job.setMapperClass(BinaryFilesToSequenceFileMapper.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.waitForCompletion(true);

		return job.isSuccessful() ? 0 : 1;
	}

	enum Counter {
		LINESKIP,
	}

	private static class BinaryFilesToSequenceFileMapper extends
			Mapper<Object, Text, Text, BytesWritable> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			logger.info("BinaryFilesToSequenceFileMapper - map method called:");

			String uri = value.toString();
			Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FSDataInputStream fsin = null;
			try {
				fsin = fs.open(new Path(uri));
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024 * 1024];

				while (fsin.read(buffer, 0, buffer.length) >= 0) {
					bout.write(buffer);
				}
				context.write(value, new BytesWritable(bout.toByteArray()));
				bout.close();
			} catch (Exception e) {
				context.getCounter(Counter.LINESKIP).increment(1);
				// e.printStackTrace();
			} finally {
				IOUtils.closeStream(fsin);
			}
		}
	}
}
