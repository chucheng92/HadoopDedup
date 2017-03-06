package com.ryan.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * pre-process work for using sequence file
 * 
 * generate file_path(value) kv file
 * 
 * @author Ryan Tao
 * @date 2017-2-9
 */
public class HDFSFileUtil {

	private static Logger logger = LoggerFactory.getLogger(HDFSFileUtil.class);

	private static final String BINARY_FILE_PATH = "/usr/local/hadoop/imgset";

	private static final String KV_FILE_PATH = "/usr/local/hadoop/kv_file.txt";

	private static long fileCount = 0L;

	private static FileSystem fs = null;

	private static Configuration conf = null;

	static {
		conf = new Configuration();
		conf.set("fs.default.name", "hdfs://Master.Hadoop:9000");
		// make sure client side replication equals 1
		// if not, even if the hdfs-site.xml replication equals 1,
		// the replication of this file in hdfs equals 3 either
		conf.set("dfs.replication", "1");
	}

	public static void main(String[] args) {
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

	/**
	 * list hdfs files
	 * @param fs
	 * @param path
     */
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
					fsout.write((filePath + "\n").getBytes());
					fileCount++;
				}
			}
			fsout.close();
			logger.info("generate kv file, Done!");
			logger.info("the number of file:" + fileCount);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    /**
     * create hdfs file
     * @param path
     * @param bytes
     * @return
     */
	public static void createHDFSFile(Path path, byte[] bytes) {
		try {
			fs = FileSystem.get(URI.create("hdfs://Master.Hadoop:9000"), conf);
			FSDataOutputStream fsout = fs.create(path);
            ByteArrayInputStream bin = new ByteArrayInputStream(bytes);

            IOUtils.copyBytes(bin, fsout, conf, true);

            logger.info("create file success");
        } catch (IOException e) {
			e.printStackTrace();
		}
	}
}
