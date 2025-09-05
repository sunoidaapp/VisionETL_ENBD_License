package com.vision.examples;

public class ReplaceSample {
	public static void main(String[] args) {
		String value = "`node142235703`.`BUSINESS_DATE_DD_c351274212` THEN `node142235703`.`WASH_RATE_52`";
		String replaceStr = "`node142235703`.";
		String replaceWith = "`group168076405_1.1`.";
		
		value = value.replaceAll(replaceStr, replaceWith);
		// System.out.println(String.format("value = %s | replaceStr = %s | replaceWith = %s", value, replaceStr, null));
	}
}