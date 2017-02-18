package com.nothankyou.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by saber on 2017/2/6.
 * md5:128bit
 */
public class Md5Util {

   public static String getMd5(byte[] bytes) {
	   try {
		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(bytes);
		byte[] md5Bytes = md.digest();
		
		return bytesToHexString(md5Bytes);
	} catch (NoSuchAlgorithmException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();	
		return "get md5 error";
	} 
   }

    private static String bytesToHexString(byte[] bytes) {
        String hex = "";
        for (int i = 0; i < bytes.length; i++) {
            hex += byteToHexString(bytes[i]);
        }

        return hex;
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
