package com.vision.examples;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vision.vb.EtlFeedTranColumnVb;

public class ReflectionSample {

	public static void main(String[] args) {
		try {
			MethodClass classObj = new MethodClass();
//			Method mth = classObj.getClass().getDeclaredMethod("sampleMth", Map.class, String.class);
			
			Method mth = classObj.getClass().getDeclaredMethod("sampleMth", new Class[] {Map.class, Map.class, String.class}); 
			
			Map<String, List<EtlFeedTranColumnVb>> ioMap1 = new HashMap<String, List<EtlFeedTranColumnVb>>();
			Map<String, List<EtlFeedTranColumnVb>> ioMap2 = new HashMap<String, List<EtlFeedTranColumnVb>>();
			// System.out.println(mth.invoke(classObj, ioMap1, ioMap2, "S"));

		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

}

class MethodClass {
	public String sampleMth(Map<String, List<EtlFeedTranColumnVb>> ioMap1, Map<String, List<EtlFeedTranColumnVb>> ioMap2, String s) {
		return s;
	}
}
