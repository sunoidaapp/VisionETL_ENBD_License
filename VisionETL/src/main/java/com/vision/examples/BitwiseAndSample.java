package com.vision.examples;

public class BitwiseAndSample {
	public static void main(String[] args) {

	}
	public boolean startHi(String str) {
		if (str.startsWith("hi")) {
			return true;
		} else {
			return false;
		}
	}

	public boolean icyHot(int temp1, int temp2) {
		if ((temp1 > 100 && temp2 < 0) || (temp1 < 0 && temp2 > 100)) {
			return true;
		} else {
			return false;
		}
	}

}
