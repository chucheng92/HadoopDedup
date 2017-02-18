package com.nothankyou.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * pre-process work for using sequence file
 * 
 * generate file_path(value) kv file
 * 
 * @author taoxiaoran
 * @date 2017-2-9
 */
public class HDFSFileList {
	
	private static final String BINARY_FILE_PATH = "/usr/local/hadoop/imgset";
	
	private static final String KV_FILE_PATH = "/usr/local/hadoop/kv_file.txt";
	
	private  static long fileCounter = 0L;
	
	public static void main(String[] args) {
		FileSystem fs = null;

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://Master.Hadoop:9000");
		// conf.set("dfs.replication", "1");

		try {
			fs = FileSystem.get(URI.create("hdfs://Master.Hadoop:9000"), conf);
			Path path = new Path(BINARY_FILE_PATH);
			showFiles(fs, path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (null != fs) {
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private static void showFiles(FileSystem fs, Path path) {
		// TODO Auto-generated method stub
		if (null == fs || null == path) {
			return;
		}
		// get file lists
		try {
			FileStatus[] files = fs.listStatus(path);
			FSDataOutputStream fsout = null;
			fsout = fs.create(new Path(KV_FILE_PATH));
			// show files(include file & directory)
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDir()) {
					showFiles(fs, files[i].getPath());
				} else {
					String filePath = files[i].getPath().toString();
					fsout.write((filePath+"\n").getBytes());
					fileCounter++;
				}
			}
			fsout.close();
			System.out.println("generate kv file, Done!");
			System.out.println("the number of file:" + fileCounter);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
