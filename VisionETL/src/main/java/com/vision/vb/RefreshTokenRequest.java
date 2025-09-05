package com.vision.vb;

public class RefreshTokenRequest {
	private String refreshToken;
	private String r; //R_SESSION_ID in the PRD_REFRESH_TOKEN table
	private String a; //Angular-side public key that JAVA must use to encrypt JWT token 
	
	public String getRefreshToken() {
		return refreshToken;
	}

	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}

	public String getR() {
		return r;
	}

	public void setR(String r) {
		this.r = r;
	}

	public String getA() {
		return a;
	}

	public void setA(String a) {
		this.a = a;
	}

}
