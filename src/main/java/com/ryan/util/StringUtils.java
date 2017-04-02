package com.ryan.util;

import com.ryan.security.Digest;
import com.ryan.security.Digests;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;

/**
 * Utility class to generate MD5
 *
 * @author Ryan Tao
 * @github lemonjing
 */
public class StringUtils {
    /**
     * get md5 hash from byte array
     *
     * @param bytes
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String getMd5(byte[] bytes) throws NoSuchAlgorithmException {
        Parameters.checkNotNull(bytes);
        Parameters.checkCondition(bytes.length >= 0);
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(bytes);
        byte[] md5Bytes = md.digest();

        return bytesToHexString(md5Bytes);

    }

    /**
     * get keccak224 hash
     *
     * @param bytes
     * @return
     */
    public static  String getKeccak(byte[] bytes) {
        Parameters.checkNotNull(bytes);
        Parameters.checkCondition(bytes.length >= 0);
        Digest d = Digests.keccak224();
        byte[] keccakBytes = d.update(bytes).digest();

        return bytesToHexString(keccakBytes);
    }

    /**
     * get keccak224 hash
     *
     * @param bytes
     * @return
     */
    public static  String getSHA224(byte[] bytes) throws NoSuchAlgorithmException {
        Parameters.checkNotNull(bytes);
        Parameters.checkCondition(bytes.length >= 0);
        Security.addProvider(new BouncyCastleProvider());
        MessageDigest md = MessageDigest.getInstance("SHA224");
        md.update(bytes);
        byte[] sha224Bytes = md.digest();

        return bytesToHexString(sha224Bytes);
    }

    /**
     * byte array to hex string
     *
     * @param bytes
     * @return
     */
    public static String bytesToHexString(byte[] bytes) {
        String hex = "";
        for (int i = 0; i < bytes.length; i++) {
            hex += byteToHexString(bytes[i]);
        }
        return hex;
    }

    /**
     * byte to hex string
     *
     * @param b
     * @return
     */
    public static String byteToHexString(byte b) {
        char[] digit = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
                'B', 'C', 'D', 'E', 'F'};
        char[] tempArr = new char[2];
        tempArr[0] = digit[(b >>> 4) & 0x0F];
        tempArr[1] = digit[b & 0x0F];

        String res = new String(tempArr);

        return res;
    }
}
