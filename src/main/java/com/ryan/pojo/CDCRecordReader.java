package com.ryan.pojo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ryan.util.Md5Util;

public class CDCRecordReader extends RecordReader<IntWritable, ChunkInfo> {
	
	private static final Logger log = LoggerFactory.getLogger(CDCRecordReader.class);
	
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
	private int chunkSize = 4 * 1024;
	private int chunkId;
	private byte[] tempBytes = new byte[2];
	private IntWritable key = new IntWritable(0);
	private ChunkInfo value = new ChunkInfo(0, chunkSize, 0, 0, tempBytes, " ", " ");
	private List<Long> list = new ArrayList<>();
	
	public CDCRecordReader() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		conf = context.getConfiguration();
		this.fileSplit = (FileSplit)split;
		this.filePath = this.fileSplit.getPath();
		this.chunkId = 0;
		this.start = fileSplit.getStart();
		this.pos = this.start;
		
		try {
			this.fs = filePath.getFileSystem(conf);
			this.fileName = this.filePath.toString();
			log.info("fileName={}", fileName);
			this.fsin = fs.open(filePath);
			fsin.seek(start);
			// read file from fsin to output stream
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			buffer = new byte[4*1024];
			int flag = 0;
			while ((flag = fsin.read(buffer)) != -1) {
				out.write(buffer);
			}
			if (-1 == flag) {
				fsin.close();
			}
			buffer = out.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.markChunkPostition(buffer, chunkSize);
	}
	
	// generate chunk position and storage in list
	private void markChunkPostition(byte[] bytes, int size) {
		// TODO Auto-generated method stub
		int chunkNum = bytes.length / size + 1;
		for (int i = 0; i < bytes.length; i += size) {
			// generate 4KB array
			list.add((long)i);
		}
		//fill
		if (list.size() != chunkNum) {
			list.add(list.get(list.size() - 1) + size);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int currentPos = this.chunkId;
		this.chunkId++;
		if ((currentPos + 1) >= list.size()) {
			return false;
		}
		key.set(currentPos);
		value.buffer = new byte[chunkSize];
		for (int i=0; i< value.buffer.length; i++) {
			value.buffer[i] = buffer[(int)(list.get(currentPos) + i)];
		}
		value.id = chunkId;
		value.size = chunkSize;
		value.fileNum = 1;
		value.chunkNum = 1;
		value.name = "chunk_" + chunkId;
		
		return true;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public ChunkInfo getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos-start)/(float)(end-start));
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (fsin != null) {
			fsin.close();
		}
	}
	
}
