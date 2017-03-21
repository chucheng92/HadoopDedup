package com.ryan.core;

import com.ryan.security.Digest;
import com.ryan.security.Digests;
import com.ryan.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * <p>this class used to be compare MD5 & SHA-1 with keccak</p>
 * <p>indicator: time, cpu, cascade rate</p>
 * <p>dataset: hamlet.txt</p>
 *
 * @author Ryan Tao
 * @github lemonjing
 */
public class HashCompare {
    private static final Logger log = LoggerFactory.getLogger(HashCompare.class);

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        File file1 = new File("src/main/resources/hamlet1.txt");
        byte[] bytes1 = transformToBytes(file1);
        File file2 = new File("src/main/resources/hamlet2.txt");
        byte[] bytes2 = transformToBytes(file2);
        Digest d1 = Digests.md5();
        Digest d2 = Digests.sha1();
        Digest d3 = Digests.sha256();
        Digest d4 = Digests.keccak224();
        Digest d5 = Digests.keccak256();
        Digest d6 = Digests.keccak384();
        Digest d7 = Digests.keccak512();

        // run time
        long startTime = System.currentTimeMillis();
        shaPerformance(bytes1, d1);
        long endTime = System.currentTimeMillis();
        double res = (endTime - startTime);
        System.out.println("run time@md5:" + res);

        // cascade rate md5
        byte[] md5Bytes1 = shaPerformance(bytes1, d1);
        byte[] md5Bytes2 = shaPerformance(bytes2, d1);
        int countMD5 = 0;
        for (int i = 0; i < md5Bytes1.length; i++) {
            if (md5Bytes1[i] == md5Bytes2[i]) {
                countMD5++;
            }
        }
        System.out.println("cascade rate@md5:" + countMD5);

        // cascade rate sha1
        byte[] sha1Bytes1 = shaPerformance(bytes1, d2);
        byte[] sha1Bytes2 = shaPerformance(bytes2, d2);
        int countSHA1 = 0;
        for (int i = 0; i < sha1Bytes1.length; i++) {
            if (sha1Bytes1[i] == sha1Bytes2[i]) {
                countSHA1++;
            }
        }
        System.out.println("cascade rate@sha1:" + countSHA1);

        // cascade rate sha256
        byte[] sha256Bytes1 = shaPerformance(bytes1, d3);
        byte[] sha256Bytes2 = shaPerformance(bytes2, d3);
        int countSHA256 = 0;
        for (int i = 0; i < sha256Bytes1.length; i++) {
            if (sha256Bytes1[i] == sha256Bytes2[i]) {
                countSHA256++;
            }
        }
        System.out.println("cascade rate@sha256:" + countSHA256);

        // cascade rate keccak224
        byte[] keccak224Bytes1 = shaPerformance(bytes1, d4);
        byte[] keccak224Bytes2 = shaPerformance(bytes2, d4);
        int countKeccak224 = 0;
        for (int i = 0; i < keccak224Bytes1.length; i++) {
            if (keccak224Bytes1[i] == keccak224Bytes2[i]) {
                countKeccak224++;
            }
        }
        System.out.println("cascade rate@keccak224:" + countKeccak224);

        // cascade rate keccak256
        byte[] keccak256Bytes1 = shaPerformance(bytes1, d5);
        byte[] keccak256Bytes2 = shaPerformance(bytes2, d5);
        int countKeccak256 = 0;
        for (int i = 0; i < keccak256Bytes1.length; i++) {
            if (keccak256Bytes1[i] == keccak256Bytes2[i]) {
                countKeccak256++;
            }
        }
        System.out.println("cascade rate@keccak256:" + countKeccak256);

        // cascade rate keccak384
        byte[] keccak384Bytes1 = shaPerformance(bytes1, d6);
        byte[] keccak384Bytes2 = shaPerformance(bytes2, d6);
        int countKeccak384 = 0;
        for (int i = 0; i < keccak384Bytes1.length; i++) {
            if (keccak384Bytes1[i] == keccak384Bytes2[i]) {
                countKeccak384++;
            }
        }
        System.out.println("cascade rate@keccak384:" + countKeccak384);

        // cascade rate keccak512
        byte[] keccak512Bytes1 = shaPerformance(bytes1, d7);
        byte[] keccak512Bytes2 = shaPerformance(bytes2, d7);
        int countKeccak512 = 0;
        for (int i = 0; i < keccak512Bytes1.length; i++) {
            if (keccak512Bytes1[i] == keccak512Bytes2[i]) {
                countKeccak512++;
            }
        }
        System.out.println("cascade rate@keccak512:" + countKeccak512);
    }

    public static byte[] shaPerformance(byte[] bytes, Digest d) {
        d.update(bytes);
        byte[] shaBytes = d.digest();

        return shaBytes;
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
