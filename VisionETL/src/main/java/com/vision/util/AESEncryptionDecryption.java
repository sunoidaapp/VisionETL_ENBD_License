package com.vision.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AESEncryptionDecryption {

	private static SecretKeySpec secretKey;
	private static byte[] key;

	public static void main(String[] args) {
		AESEncryptionDecryption encryption = new AESEncryptionDecryption();
		String originalString = "Sunoida123";
		String encryptedString = encryption.encryptPKCS7(originalString);
		// System.out.println(encryptedString);
		String decryptedString = encryption.decryptPKCS7(encryptedString);
		// System.out.println(decryptedString);
	}

	/*public static void setKey(String myKey) {
		MessageDigest sha = null;
		try {
			key = myKey.getBytes("UTF-8");
			sha = MessageDigest.getInstance("SHA-1");
			key = sha.digest(key);
			key = Arrays.copyOf(key, 16);
			secretKey = new SecretKeySpec(key, "AES");
		} catch (NoSuchAlgorithmException e) {
			// e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// e.printStackTrace();
		}
	}*/
	
	public static void setKey(String myKey) {
		try {
			secretKey = new SecretKeySpec(myKey.getBytes("UTF-8"),"AES" );
		} catch (UnsupportedEncodingException e) {
			// e.printStackTrace();
		}
	}

	public static String encrypt(String strToEncrypt, String secret) {
		try {
//			Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
			setKey(secret);
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
//			Cipher cipher = Cipher.getInstance("RSA/NONE/NoPadding");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
		} catch (Exception e) {
			// System.out.println("Error while encrypting: " + e.toString());
		}
		return null;
	}

	public static String decrypt(String strToDecrypt, String secret) {
		try {
//			Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
			setKey(secret);
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
//			Cipher cipher = Cipher.getInstance("RSA/NONE/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, secretKey);
			return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
		} catch (Exception e) {
			// System.out.println("Error while decrypting: " + e.toString());
		}
		return null;
	}
	

    final static String PKCS7key = "MIGfMA0GCSqGSIb3";
    final static String iv = "MIGfMA0GCSqGSIb3";
    final static String algorithm = "AES/CBC/PKCS5Padding";
    private static Cipher cipher = null;
    private static SecretKeySpec skeySpec = null;
    private static IvParameterSpec  ivSpec = null;

    private static void setUp(){
        try{
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider()); 
            skeySpec = new SecretKeySpec(PKCS7key.getBytes("ASCII"), "AES");
            ivSpec = new IvParameterSpec(iv.getBytes("ASCII"));
            cipher = Cipher.getInstance(algorithm);
        }catch(NoSuchAlgorithmException | NoSuchPaddingException ex){
            // ex.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        }
    }

	public static String encryptPKCS7(String str) {
		try {
			setUp();
			try {
				cipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivSpec);
			} catch (InvalidAlgorithmParameterException ex) {
				// ex.printStackTrace();
				return "";
			}
			byte[] enc = cipher.doFinal(str.getBytes("ASCII"));
			String s = new String(Base64.getEncoder().encode(enc));
			String encodedUrl2 = URLEncoder.encode(s, "UTF-8");// changed
			// s = s.replace("+", "__plus__");
			// s = s.replace("/", "__slash__");
			return encodedUrl2;
		} catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException ex) {
			// ex.printStackTrace();
			return "";
		} catch (UnsupportedEncodingException e) {
			// e.printStackTrace();
			return "";
		} catch (Exception e) {
			// e.printStackTrace();
			return "";
		}
	}

	public static String decryptPKCS7(String str) {
		String s = "";
		try {
			setUp();
			try {
				cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);
			} catch (InvalidAlgorithmParameterException ex) {
				// ex.printStackTrace();
				return "";
			}
			byte[] decryptedToken;
			try {
				decryptedToken = cipher.doFinal(Base64.getDecoder().decode(str));
				s = new String(decryptedToken);
			} catch (IllegalBlockSizeException | BadPaddingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return s;
		} catch (InvalidKeyException ex) {
			// ex.printStackTrace();
			return "";
		} catch (Exception e) {
			// e.printStackTrace();
			return "";
		}
	}

}