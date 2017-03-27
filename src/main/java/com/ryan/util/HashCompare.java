package com.ryan.util;

import com.ryan.security.Digest;
import com.ryan.security.Digests;
import org.bouncycastle.pqc.jcajce.provider.BouncyCastlePQCProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;

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

        // run time sha224
        long startTime = System.currentTimeMillis();
        // bouncy castle SHA224
        for (int i = 0; i < 100; i++) {
            Security.addProvider(new BouncyCastlePQCProvider());
            try {
                MessageDigest md = MessageDigest.getInstance("SHA224");
                md.update(bytes1);
                byte[] sha224Bytes = md.digest();
            } catch (NoSuchAlgorithmException e) {
                //do nothing
            }
        }

        long endTime = System.currentTimeMillis();
        double res = (endTime - startTime) * 1.0/ 100;
        System.out.println("run time@sha224:" + res);

        // run time keccak224
        long startTime2 = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            d4.update(bytes1);
            byte[] keccak224Bytes = d4.digest();
        }
        long endTime2 = System.currentTimeMillis();
        double res2 = (endTime2 - startTime2) * 1.0/ 100;
        System.out.println("run time@keccak224:" + res2);
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
