package com.ryan.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * custom data-type
 * @author Ryan Tao
 *
 */
public class ChunkInfo implements Writable {
	public int id; //chunk id
	public int size; //chunk size
	public int fileNum; //one chunk belong to how many files
	public int chunkNum; //one chunk exist how many times
	public byte[] buffer = null;//chunk bytes
	public String hash; //chunk hash value
	public String name; // chunk name
	
	public ChunkInfo() {
		this.id = 0;
		this.size = 4 * 1024;
		this.fileNum = 1;
		this.chunkNum = 1;
		this.buffer = new byte[size];
		this.hash = " ";
		this.name = "chunk_" + hash;
	}
	
	public ChunkInfo(int id, int size, int fileNum, int chunkNum,
			byte[] buffer, String fileName, String hash) {
		this.id = id;
		this.size = size;
		this.fileNum = fileNum;
		this.chunkNum = chunkNum;
		this.buffer = buffer;
		this.hash = hash;
		this.name = fileName;
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
			this.name = arg0.readUTF();
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
        arg0.writeUTF(this.name);
        
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.id + " " + this.size + " " + this.fileNum 
				+ " " + this.chunkNum + " " + this.hash + " " + this.name;
	}
	
}
