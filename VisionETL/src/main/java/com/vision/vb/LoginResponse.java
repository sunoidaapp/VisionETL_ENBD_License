package com.vision.vb;

public class LoginResponse {
    private String token;
    private String refreshToken;
    private String jpk; //Dynamic RSA - Java Public Key
    private String prf; //password reset flag
    
	//Response for a new JWT Bearer token based on Refresh token
    public LoginResponse(String token) {
        this.token = token;
    }

    //Response for Login attempt
    public LoginResponse(String token, String refreshToken) {
        this.token = token;
        this.refreshToken = refreshToken;
    }
    
	//Response for Login attempt
    public LoginResponse(String token, String refreshToken, String jpk, String prf) {
        this.token = token;
        this.refreshToken = refreshToken;
        this.jpk = jpk;
        this.prf = prf;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

	public String getRefreshToken() {
		return refreshToken;
	}

	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}
	
	public String getJpk() {
		return jpk;
	}

	public void setJpk(String jpk) {
		this.jpk = jpk;
	}

	public String getPrf() {
		return prf;
	}

	public void setPrf(String prf) {
		this.prf = prf;
	}
	
}
