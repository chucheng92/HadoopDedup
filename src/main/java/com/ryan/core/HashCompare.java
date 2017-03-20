package com.ryan.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author Ryan Tao
 * @github lemonjing
 */
public class HashCompare {
    private static final Logger log = LoggerFactory.getLogger(HashCompare.class);

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        File file = new File("src/main/resources/hamlet1.txt");
        byte[] bytes = transformToBytes(file);
        System.out.println("======len=" + bytes.length);
    }

    public static String md5Performance(byte[] bytes) {
        return "";
    }

    private static byte[] transformToBytes(File file) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BufferedInputStream bin = null;
        try {
            bin = new BufferedInputStream(new FileInputStream(file));
            byte[] buffer = new byte[1024];
            int len = 0;
            while (-1 != (len = bin.read(buffer, 0, buffer.length))) {
                System.out.println("======len=" + len);
                bos.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bos.toByteArray();
    }
}
