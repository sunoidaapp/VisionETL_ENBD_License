package com.vision.vb;

public class LoginRequest {
    private String username;
    private String password;
    private String a; //Angular-side public key that JAVA must use to encrypt JWT token 

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

	public String getA() {
		return a;
	}

	public void setA(String a) {
		this.a = a;
	}
}
