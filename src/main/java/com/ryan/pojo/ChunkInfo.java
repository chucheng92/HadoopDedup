package com.ryan.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import com.ryan.util.Constant;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * custom data-type
 * @author Ryan Tao
 *
 */
public class ChunkInfo implements Writable {

	private static final Logger log = LoggerFactory.getLogger(ChunkInfo.class);

	private int id; //chunk id
	private int size; //chunk size
	private int fileNum; //one chunk belong to how many files
	private int chunkNum; //one chunk exist how many times
	private byte[] buffer = null;//chunk bytes
	private String hash; //chunk hash value
	private String fileName; // belong to first old file(full hdfs path)


	public ChunkInfo() {
		log.info("called:ChunkInfo Default Constructor");
		this.id = 0;
		this.size = Constant.DEFAULT_CHUNK_SIZE;
		this.fileNum = 1;
		this.chunkNum = 1;
		this.buffer = new byte[size];
		this.hash = Constant.DEFAULT_HASH_VALUE;
		this.fileName = Constant.DEFAULT_FILE_NAME;
	}
	
	public ChunkInfo(int id, int size, int fileNum, int chunkNum,
			byte[] buffer, String hash, String fileName) {
		this.id = id;
		this.size = size;
		this.fileNum = fileNum;
		this.chunkNum = chunkNum;
		this.buffer = buffer;
		this.hash = hash;
		this.fileName = fileName;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		//  deserialization
		try {
			this.id = arg0.readInt();
			this.size = arg0.readInt();
			this.fileNum = arg0.readInt();
			this.chunkNum = arg0.readInt();
			this.hash = arg0.readUTF();
			this.fileName = arg0.readUTF();
		} catch(EOFException e) {
			return;
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(this.id);  
        arg0.writeInt(this.size);  
        arg0.writeInt(this.fileNum);  
        arg0.writeInt(this.chunkNum);  
        arg0.writeUTF(this.hash);  
        arg0.writeUTF(this.fileName);
        
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.id + " " + this.size + " " + this.fileNum 
				+ " " + this.chunkNum + " " + this.hash + " " + this.fileName;
	}

	// =========== getters/setters =============
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getFileNum() {
		return fileNum;
	}

	public void setFileNum(int fileNum) {
		this.fileNum = fileNum;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public void setChunkNum(int chunkNum) {
		this.chunkNum = chunkNum;
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}