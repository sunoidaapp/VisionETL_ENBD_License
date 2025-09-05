package com.vision.authentication;

import org.springframework.stereotype.Component;

import com.vision.vb.VisionUsersVb;

@Component
public class CustomContextHolder {
	private static InheritableThreadLocalSecurityContextHolder strategy = new InheritableThreadLocalSecurityContextHolder();

	public static void setContext(VisionUsersVb context) {
		strategy.setContext(context);
	}
	public static VisionUsersVb getContext() {
		return strategy.getContext();
	}
}

class InheritableThreadLocalSecurityContextHolder {
	// ~ Static fields/initializers
	private static ThreadLocal<VisionUsersVb> contextHolder = new InheritableThreadLocal<VisionUsersVb>();
	// ~ Methods

	public void clearContext() {
		contextHolder.set(null);
	}
	public VisionUsersVb getContext() {
		if (contextHolder.get() == null) {
			return null;
		}
		return (VisionUsersVb) contextHolder.get();
	}
	public void setContext(VisionUsersVb context) {
		contextHolder.set(context);
	}
}