package com.nothankyou.core;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * list hdfs img files & generate file-path-name file
 * 
 * @author taoxiaoran
 * @date 2017-2-9
 */
public class HDFSFileList {
	public static void main(String[] args) {
		FileSystem fs = null;
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://Master.Hadoop:9000");
		
		try {
			fs = FileSystem.get(URI.create("hdfs://Master.Hadoop:9000"), conf);
			Path path = new Path("/");
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
			// show files(include file & directory)
			for (int i=0; i<files.length;i++) {
				if (files[i].isDir()) {
					System.out.println(">>>" + files[i].getPath() + ", dir owner:" + files[i].getOwner());
					showFiles(fs, files[i].getPath());
				} else {
					System.out.println("  " + files[i].getPath() + ", length:" + files[i].getLen()
							+", owner:" + files[i].getOwner());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
