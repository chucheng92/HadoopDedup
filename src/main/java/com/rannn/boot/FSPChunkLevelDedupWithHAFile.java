package com.rannn.boot;

import com.rannn.core.FSPCore;
import com.rannn.pojo.ChunkInfo;
import com.rannn.util.Constant;
import com.rannn.util.HDFSFileUtil;

import com.rannn.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class FSPChunkLevelDedupWithHAFile {
    private static final Logger log = LoggerFactory.getLogger(FSPChunkLevelDedup.class);
    private static final String HDFS_PATH = "hdfs://Master.Hadoop:9000/usr/local/hadoop";

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();

        //TODO config 60MB fraction
        // calculate input split
        // Math.max(minSize, Math.min(maxSize, blockSize));
        conf.set("mapred.min.split.size", "62914560");//minSize=60MB
        conf.set("mapred.max.split.size", "62914560");//maxSize=60MB
        conf.set("dfs.permissions", "false");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: FSPChunkLevelDedupWithHAFile <in> <out>");
            System.exit(2);
        }

        log.debug("=========job start=========");

        Job job = new Job(conf, "Job_FSPChunkLevelDedupWithHAFile");
        job.setJarByClass(FSPChunkLevelDedupWithHAFile.class);
        job.setMapperClass(HAFileMapper.class);
        job.setReducerClass(HAFileReducer.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //in:hafile
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //out

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ChunkInfo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        log.debug("=========job end=========");
        job.waitForCompletion(true);


        long end = System.currentTimeMillis();

        log.debug("consume time:{} ", end - start);
        System.exit(job.isSuccessful() ? 0 : 1);
    }

    enum Counter {
        LINESKIP,
    }

    private static class HAFileMapper extends Mapper<Text, BytesWritable, Text, ChunkInfo> {
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            value.setCapacity(value.getLength());
            byte[] bytes = value.getBytes();

            List<ChunkInfo> chunkList = new FSPCore(key.toString(), bytes, Constant.DEFAULT_CHUNK_SIZE).fsp();

            for (ChunkInfo val : chunkList) {
                String hash = StringUtils.getKeccak(val.getBuffer());
                val.setHash(hash);
                Text reduceKey = new Text(hash);
                context.write(reduceKey, val);
            }
        }
    }

    private static class HAFileReducer extends Reducer<Text, ChunkInfo, Text, NullWritable> {
        private int id = 1;

        @Override
        protected void reduce(Text key, Iterable<ChunkInfo> values, Context context) throws IOException, InterruptedException {
            int fileNumCounter = 0;
            int chunkNumCounter = 0;
            int offset = -1;
            byte[] buffer = new byte[Constant.DEFAULT_CHUNK_SIZE];
            boolean flag = true;
            String fileName = Constant.DEFAULT_FILE_NAME;
            String blockAddress = Constant.DEFAULT_BLOCK_ADDRESS;
            // duplicate chunks
            for (ChunkInfo chunk : values) {
                chunkNumCounter++;
                if (flag) {
                    fileName = chunk.getFileName();
                    offset = chunk.getOffset();
                    buffer = chunk.getBuffer();
                    try {
                        blockAddress = HDFS_PATH + "/chunk/" + key.toString() + ".blob";
                        Path chunkPath = new Path(blockAddress);
                        HDFSFileUtil.createHDFSFile(chunkPath, buffer);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    fileNumCounter++;
                }
                if (!chunk.getFileName().equals(fileName)) {
                    fileNumCounter++;
                }
                flag = false;
            }

            ChunkInfo chunkInfo = new ChunkInfo();
            chunkInfo.setId(id);
            chunkInfo.setSize(Constant.DEFAULT_CHUNK_SIZE);
            chunkInfo.setFileNum(fileNumCounter);
            chunkInfo.setChunkNum(chunkNumCounter);
            chunkInfo.setHash(key.toString());
            chunkInfo.setFileName(fileName);
            chunkInfo.setOffset(offset);
            chunkInfo.setBlockAddress(blockAddress);
            id++;
            context.write(new Text(chunkInfo.toString()), NullWritable.get());

            log.debug("ChunkInfo:{}", chunkInfo.toString());
        }
    }
}

