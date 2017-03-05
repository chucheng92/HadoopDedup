package com.ryan.pojo;

import java.util.List;

/**
 * Created by Ryan Tao on 2017/2/5.
 * @author ryan
 *
 * to storage specific file info
 */
public class FileInfo {
    private String fileName;
    private String Path;
    // chunk hash list
    private List<String> chunkList;

    public FileInfo() {
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getPath() {
        return Path;
    }

    public void setPath(String path) {
        Path = path;
    }

    public List<String> getChunkList() {
        return chunkList;
    }

    public void setChunkList(List<String> chunkList) {
        this.chunkList = chunkList;
    }
}
