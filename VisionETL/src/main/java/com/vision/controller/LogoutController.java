package com.vision.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.vision.authentication.CustomContextHolder;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LogoutUserServices;

@RestController
public class LogoutController {
	
	@Autowired
	private LogoutUserServices logoutUsersServices;
	
	@GetMapping("/visionLogout")
	public ResponseEntity logout() {
		VisionUsersVb userVb = CustomContextHolder.getContext();
		try {
			logoutUsersServices.invalidateAllAccessTokens(userVb);
			return ResponseEntity.ok().build();
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
	}

}
