package com.vision.util;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Component;

@Component
public class AES_Encrypt_DecryptUtil {
	
	/* Should maintain the String length to 16*/
	private static final String initVector = "vision09SecretIV"; // encryptionIntVec

	/* Should maintain the String length to 16*/
	private static final String key = "visionEncryptKey"; // aesEncryptionKey
	
	private static final String initVectorIndex3 = "Hgq2wHg98dur6dDz"; // Length 16

	private static final String keyIndex3 = "OW1KDREOXy1bmQ5x"; // Length 16

	private static final String initVectorIndex5 = "rmCuRn7shAqG"; // Length 12

	private static final String keyIndex5 = "qtFk3DtjWE2x"; // Length 12
    
    
    public static void main(String[] args) {
    	try {
    		String plainTxt = "Good";
    		String eStr = encrypt(plainTxt);
    		// System.out.println(String.format("eStr: %s", eStr));
    		String eStrURL_Encode = URLEncoder.encode(eStr, "UTF-8");
    		// System.out.println(String.format("eStr URL Encoded: %s", eStrURL_Encode));
    		
    		String eStrURL_Encode_1 = URLEncoder.encode(eStrURL_Encode, "UTF-8");
    		// System.out.println(String.format("eStr URL Encoded 1: %s", eStrURL_Encode_1));
    		
    		
    		String dStrURL_Decode = URLDecoder.decode(eStrURL_Encode_1, "UTF-8");
    		// System.out.println(String.format("dStr URL Decoded: %s", dStrURL_Decode));
    		
    		String dStrURL_Decode_1 = URLDecoder.decode(dStrURL_Decode, "UTF-8");
    		// System.out.println(String.format("dStr URL Decoded 1: %s", dStrURL_Decode_1));
    		
    		String dStr = decrypt(dStrURL_Decode_1);
    		// System.out.println(String.format("dStr: %s", dStr));
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}
	
    public static String decrypt(String encryptedData) throws Exception {
		IvParameterSpec iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
		SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
		cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
		return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedData)));
	}
    
    public static String decrypt_SecurityLvl_3(String encryptedData) throws Exception {
		IvParameterSpec iv = new IvParameterSpec(initVectorIndex3.getBytes("UTF-8"));
		SecretKeySpec skeySpec = new SecretKeySpec(keyIndex3.getBytes("UTF-8"), "AES");

		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
		cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
		return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedData)));
	}
    
    
    public static String decrypt_SecurityLvl_5(String encryptedData, String key) throws Exception {
		IvParameterSpec iv = new IvParameterSpec((initVectorIndex5+key).getBytes("UTF-8"));
		SecretKeySpec skeySpec = new SecretKeySpec((keyIndex5+key).getBytes("UTF-8"), "AES");
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
		cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
		return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedData)));
	}
	
	public static String encrypt(String plainTxt) throws Exception {
		IvParameterSpec iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
		SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
		cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
		return Base64.getEncoder().encodeToString(cipher.doFinal(plainTxt.getBytes("UTF-8")));
	}
}
