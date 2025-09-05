package com.vision.examples;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexExample {
	
	private static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

	public static void main(String[] args) {

		
		//Seamus.Schmeler@gmail.com
		
		//Se***********er@gmail.com
		Pattern pattern = Pattern.compile(EMAIL_PATTERN);
		

		String value = "Seamus.Schmeler@gmail.com";
		Matcher matcher = pattern.matcher(value);
		if (matcher.matches()) {
			// System.out.println("Email " + value + " is valid");
		} else {
			// System.out.println("Email " + value + " is invalid");
		}

		
		String maskedEmail = value.replaceAll("(?<=.{2}).(?=[^@]*?.@)", "*");
		
		
		// System.out.println(maskedEmail);
	}
}
