package com.nothankyou.util;

/**
 * Created by saber on 2017/2/16.
 */
public class Md5Util {

    public static String MD5Digest() {
        return "";
    }

    private static String bytesToHexString(byte[] byteArr) {
        String strDigest = "";
        for (int i = 0; i < byteArr.length; i++) {
            strDigest += byteToHexString(byteArr[i]);
        }

        return strDigest;
    }

    private static String byteToHexString(byte b) {
        char[] digit = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
                'B', 'C', 'D', 'E', 'F'};
        char[] tempArr = new char[2];
        tempArr[0] = digit[(b >>> 4) & 0x0F];
        tempArr[1] = digit[b & 0x0F];

        String res = new String(tempArr);

        return res;
    }
}
