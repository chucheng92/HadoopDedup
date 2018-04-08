package com.rannn.core;

import com.rannn.pojo.ChunkInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * custom FileInputFormat
 *
 * @author Rannn Tao
 * @github lemonjing
 */
public class FSPFileInputFormat extends FileInputFormat<IntWritable, ChunkInfo>{
    @Override
    public RecordReader<IntWritable, ChunkInfo> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FSPRecordReader();
    }
}
