package com.ryan.core;

import com.ryan.pojo.ChunkInfo;
import com.ryan.util.Constant;
import com.ryan.util.Parameters;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ryan Tao
 * @github lemonjing
 */
public class FSPCore {
    private static final Logger log = LoggerFactory.getLogger(FSPCore.class);

    private String fileName;
    private long start;
    private long pos;
    private long end;
    private byte[] buffer; //file content
    private int chunkSize = Constant.DEFAULT_CHUNK_SIZE;
    private int chunkId;
    private byte[] tempBytes = new byte[2];
    private IntWritable key = new IntWritable(0);
    private List<Long> list = new ArrayList<>();

    public FSPCore(String fileName, byte[] bytes, int chunkSize) {
        this.fileName = fileName;
        this.buffer = bytes;
        this.chunkSize = chunkSize;
    }

    public List<ChunkInfo> fsp() {
        List<ChunkInfo> chunkList = new ArrayList<>();
        markChunkPostition(buffer, chunkSize);

        while (true) {
            int currentPos = this.chunkId;
            this.chunkId++;
            if (currentPos >= list.size()) {
                return chunkList;
            } else {
                key.set(currentPos);
                chunkList.add(nextKeyValue(currentPos));
            }
        }
    }

    /**
     * generate chunk position and storage it in list
     *
     * @param bytes
     * @param size
     */
    private void markChunkPostition(byte[] bytes, int size) {
        Parameters.checkNotNull(bytes);
        Parameters.checkCondition(bytes.length > 0);

        log.debug("==============HAFileWay@called:markChunkPostition=========");

        int chunkNum = (int) Math.ceil(bytes.length / (double) size);

        for (int i = 0; i < bytes.length; i += size) {
            // generate 4KB array
            list.add((long) i);
        }
        System.out.println("chunk position:" + list);
    }

    public ChunkInfo nextKeyValue(int currentPos) {
        log.debug("==========HAFileWay@called:nextKeyValue=============");

        // if specific chunk < 4KB then padding it to 4KB
        byte[] bytes = new byte[chunkSize];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = buffer[(int) (list.get(currentPos) + i)];
        }

        ChunkInfo value = new ChunkInfo();
        value.setId(chunkId);
        value.setSize(chunkSize);
        value.setFileNum(1);
        value.setChunkNum(1);
        value.setBuffer(bytes);
        value.setHash(Constant.DEFAULT_HASH_VALUE);
        value.setFileName(fileName);
        value.setOffset(currentPos);
        value.setBlockAddress(Constant.DEFAULT_BLOCK_ADDRESS);

        log.debug("==========fileName={}", fileName);

        return value;
    }
}
