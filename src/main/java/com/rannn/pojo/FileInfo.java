package com.rannn.pojo;

import java.util.List;

/**
 * to storage specific file info
 *
 * @author Rannn Tao
 * @github lemonjing
 */
@Deprecated
public class FileInfo {
    private String fileName;
    private String Path;
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
