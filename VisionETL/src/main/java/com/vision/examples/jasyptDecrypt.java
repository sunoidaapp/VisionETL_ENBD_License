package com.vision.examples;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class jasyptDecrypt {

	public static void main(String[] args) {

		//String password = "W4W1RvonQuXbwNIQMQchCyOhndyAusfH";
		// String password = "D8wvoxJPI9VxBrSAFhaohHc6ay8D2V/1";
		String password = "t6o6dJEwfHPQcOtIjPZcd7zuBYzGSoWG";

		String jasyptSecreatKey = "v!$!0n";
		StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword(jasyptSecreatKey);
		encryptor.setAlgorithm("PBEWithSHA1AndDESede");
		password = encryptor.decrypt(password);
		System.out.println(password);
	}
}
