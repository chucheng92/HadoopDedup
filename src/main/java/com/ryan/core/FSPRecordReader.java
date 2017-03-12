package com.ryan.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ryan.pojo.ChunkInfo;
import com.ryan.util.Constant;
import com.ryan.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FSP 固定分块算法
 *
 * @author Ryan Tao
 * @website http://taoxiaoran.top
 */
public class FSPRecordReader extends RecordReader<IntWritable, ChunkInfo> {

    private static final Logger log = LoggerFactory.getLogger(FSPRecordReader.class);

    private Configuration conf;
    private FileSystem fs;
    private FSDataInputStream fsin;
    private Path filePath;
    private FileSplit fileSplit;
    private String fileName;
    private long start;
    private long pos;
    private long end;
    private byte[] buffer; //file content
    private int chunkSize = Constant.DEFAULT_CHUNK_SIZE;
    private int chunkId;
    private byte[] tempBytes = new byte[2];
    private IntWritable key = new IntWritable(0);
    private ChunkInfo value = new ChunkInfo(0, chunkSize, 0, 0, tempBytes
            , Constant.DEFAULT_HASH_VALUE, Constant.DEFAULT_FILE_NAME, -1);
    private List<Long> list = new ArrayList<>();

    public FSPRecordReader() {
        log.debug("========called:FSPRecordReader Default Constructor============");
        // TODO Auto-generated constructor stub
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        log.debug("==============called:initialize=========");

        // TODO Auto-generated method stub
        conf = context.getConfiguration();
        conf.set("fs.default.name", "hdfs://Master.Hadoop:9000");
        this.fileSplit = (FileSplit) split;
        this.filePath = this.fileSplit.getPath();
        this.chunkId = 0;
        this.start = fileSplit.getStart();
        this.end = start + split.getLength();
        this.pos = this.start;

        try {
            fs = filePath.getFileSystem(conf);
            fileName = this.filePath.toString();

            log.debug("===========current split=>fileName={}", fileName);

            // HBase
            Result result = HBaseUtil.getResultByRowKey(Constant.DEFAULT_HBASE_TABLE_NAME, fileName);
            if (null != result.list()) {
                buffer = null;

                log.debug("this file-split has been processed");
            } else {
                fsin = fs.open(filePath);
                fsin.seek(start);
                // read file from fsin to output stream
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                buffer = new byte[Constant.DEFAULT_CHUNK_SIZE];
                int flag = 0;
                while ((flag = fsin.read(buffer)) != -1) {
                    out.write(buffer);
                }
                if (null != fsin) {
                    fsin.close();
                }
                buffer = out.toByteArray();
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.markChunkPostition(buffer, chunkSize);
    }

    // generate chunk position and storage it in list
    private void markChunkPostition(byte[] bytes, int size) {
        // TODO Auto-generated method stub
        if (bytes != null) {
            log.debug("==============called:markChunkPostition=========");

            int chunkNum = (int) Math.ceil(bytes.length / (double) size);
            for (int i = 0; i < bytes.length; i += size) {
                // generate 4KB array
                list.add((long) i);
            }

            System.out.println("chunk position:" + list);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        log.debug("==========called:nextKeyValue=============");

        int currentPos = this.chunkId;
        this.chunkId++;
        if (currentPos >= list.size()) {
            return false;
        }
        key.set(currentPos);

        // if specific chunk < 4KB then padding it to 4KB
        byte[] bytes = new byte[chunkSize];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = buffer[(int) (list.get(currentPos) + i)];
        }

        value.setId(chunkId);
        value.setSize(chunkSize);
        value.setFileNum(1);
        value.setChunkNum(1);
        value.setBuffer(bytes);
        value.setHash(Constant.DEFAULT_HASH_VALUE);
        value.setFileName(fileName);
        value.setOffset(currentPos);

        log.debug("==========fileName={}", fileName);

        pos += chunkSize;

        return true;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        log.debug("========called:getCurrentKey=========");

        return key;
    }

    @Override
    public ChunkInfo getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        log.debug("===========called:getCurrentValue======");
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        log.debug("==========called:getProgress==========");
        if (start == end) {
            log.debug("===========getProcess={}", 0.0f);
            return 0.0f;
        } else {
            log.debug("===========getProcess={}", Math.min(1.0f, (pos - start) / (float) (end - start)));
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        log.debug("=============called:close=====================");
        if (fsin != null) {
            fsin.close();
        }
    }

}
