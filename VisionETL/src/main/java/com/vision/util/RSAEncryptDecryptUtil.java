package com.vision.util;

import java.net.URLDecoder;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;

public class RSAEncryptDecryptUtil {
	
	/*public static void main(String[] args) throws Exception {
        
		//generatePublicAndPrivateKeyString();
    	
    	//String message = "Sunoida Vignesh !@#$^&%*";
    	//String publicKeyString = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAI5NLT9yAkziRg+O3I0jaa1fWq/85KN+nWymCZHm17uE3a+Tlxx9YQlg61DxLF8rv7RcM5nAT8o4XJtlT/veCh8CAwEAAQ==";
    	
    	//encryptUsingPublicKeyString(message, publicKeyString);
    	
    	//String encryptedMessage = "Ojvp9W9cJwgQqOQJeFnsnRPs/maSfkqfOI7x8oxHTD8aCckwg73rm77EcqsfW7lG1P2+7xonjeli7NBjSvXdsQ==";
    	//String privateKeyString = "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAyUMD4R14pwce3bxQ6MAv4ULFrtXQCllaAbLzx8qYNhHA2QdiI8+APUXwq/bjPPzV1x1ios9MzhI3dZI7bjYtiQIDAQABAkBjjGI+1vT2qO77fkoG1gjYws5EzD064TdD39/00qC3HO7k0CyI8u3RgWku1LQF6PJh3e0ZG7lfPu+SNuuVTVSBAiEA/l3zFcSvebZ35/jA7OLRgZJm97qJVHXgH5JnhfE7gC8CIQDKjcmcSKNB4SgK2KEXP2fJ8rRHF7bJocvhKmGQv3/HxwIgDtGWWcxNlL+mmKMLSkGkhvHqgcAiRANh/TTgRxNYrg0CIFKhGsDNhQQIBNy/2J9yNzXT86UY0HFatqApONnitQ7bAiEAs2MPAFQSv6EpRd5CJxPhl6iku5Xcma8oIkMRWkpCfjk=";
    	
    	//decryptUsingPrivateKeyString(encryptedMessage, privateKeyString);
    }*/
    
    
    public static Map<String, String> generatePublicAndPrivateKeyString() throws Exception {
    	
    	Map<String, String> rsaKeyMap = new HashMap<String, String>();
    	
    	KeyPair keyPair = generateKeyPair();

        PublicKey publicKey = keyPair.getPublic();
        PrivateKey privateKey = keyPair.getPrivate();
        
        String publicKeyString = convertKeyToString(publicKey);
        String privateKeyString = convertKeyToString(privateKey);

        rsaKeyMap.put("Public", publicKeyString);
        rsaKeyMap.put("Private", privateKeyString);
        return rsaKeyMap;
    }
    
    public static String encryptUsingPublicKeyString(String message, String publicKeyString) throws Exception {
        String encryptedMessage = encrypt(message, publicKeyString);
        return encryptedMessage;
    }
    
    public static String decryptUsingPrivateKeyString(String encryptedMessage, String privateKeyString) throws Exception {
    	String urlDecodedToken = URLDecoder.decode(encryptedMessage, "UTF-8");
        String decryptedMessage = decrypt(urlDecodedToken, privateKeyString);
        return decryptedMessage;
    }

    public static KeyPair generateKeyPair() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }

    public static String convertKeyToString(Key key) {
        byte[] keyBytes = key.getEncoded();
        return Base64.getEncoder().encodeToString(keyBytes);
    }

    public static String encrypt(String message, String publicKeyString) throws Exception {
        PublicKey publicKey = convertStringToPublicKey(publicKeyString);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] encryptedBytes = cipher.doFinal(message.getBytes());
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }

    public static String decrypt(String encryptedMessage, String privateKeyString) throws Exception {
        PrivateKey privateKey = convertStringToPrivateKey(privateKeyString);
        byte[] encryptedBytes = Base64.getDecoder().decode(encryptedMessage);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
        return new String(decryptedBytes);
    }

    public static PublicKey convertStringToPublicKey(String publicKeyString) throws Exception {
        byte[] publicKeyBytes = Base64.getDecoder().decode(publicKeyString);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        return keyFactory.generatePublic(publicKeySpec);
    }

    public static PrivateKey convertStringToPrivateKey(String privateKeyString) throws Exception {
        byte[] privateKeyBytes = Base64.getDecoder().decode(privateKeyString);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        return keyFactory.generatePrivate(privateKeySpec);
    }
}
