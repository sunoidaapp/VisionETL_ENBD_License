package com.vision.controller;

import javax.security.sasl.AuthenticationException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.Constants;
import com.vision.vb.RefreshTokenRequest;
import com.vision.vb.RefreshTokenVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LoginUserServices;

@RestController
public class RefreshJWTTokenController {

	@Autowired
	private LoginUserServices loginUsersServices;

	@PostMapping("/refreshToken")
	public ResponseEntity<?> refreshToken(@RequestBody RefreshTokenRequest refreshTokenRequest) {
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		try {
			String encryptedRefreshToken = refreshTokenRequest.getRefreshToken();
			String rSessionID = refreshTokenRequest.getR();

			ExceptionCode exceptionCode = loginUsersServices.performValidationOnRefreshToken(encryptedRefreshToken, rSessionID);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				RefreshTokenVb existingRtokenVb = (RefreshTokenVb) exceptionCode.getResponse();
				String decryptedRT = (String) exceptionCode.getOtherInfo();
				
				exceptionCode = loginUsersServices.getNewBearerTokenByRefreshToken(existingRtokenVb, decryptedRT);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					return ResponseEntity.ok(exceptionCode.getResponse());
				} else if(exceptionCode.getErrorCode() == Constants.INVALID_STATUS_FLAG_IN_DATABASE) {
					return ResponseEntity.status(HttpStatus.FORBIDDEN).body(exceptionCode.getErrorMsg());
				} else {
					return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(exceptionCode.getErrorMsg());
				}
			} else {
				loginUsersServices.writePRD_Suspecious_Token_Audit(refreshTokenRequest.getR(), null, refreshTokenRequest.getRefreshToken(), null, "R", exceptionCode.getErrorMsg(), contextVb);
				return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
			}
			
		} catch (Exception e) {
			// e.printStackTrace();
			loginUsersServices.writePRD_Suspecious_Token_Audit(null, null, null, null, "R",
					"Exception in getting New Bearer token.", contextVb);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

	@PostMapping("/getNewRefreshToken")
	public ResponseEntity<?> getNewRefreshToken(@RequestBody RefreshTokenRequest refreshTokenRequest) {
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		try {
			String encryptedRefreshToken = refreshTokenRequest.getRefreshToken();
			String rSessionID = refreshTokenRequest.getR();
			
			
			ExceptionCode exceptionCode = loginUsersServices.performValidationOnRefreshToken(encryptedRefreshToken, rSessionID);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				RefreshTokenVb existingRtokenVb = (RefreshTokenVb) exceptionCode.getResponse();
				String decryptedRT = (String) exceptionCode.getOtherInfo();
				
				exceptionCode = loginUsersServices.getNewRefreshTokenByRefreshToken(existingRtokenVb, decryptedRT);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					return ResponseEntity.ok(exceptionCode.getResponse());
				}  else if(exceptionCode.getErrorCode() == Constants.INVALID_STATUS_FLAG_IN_DATABASE) {
					return ResponseEntity.status(HttpStatus.FORBIDDEN).body(exceptionCode.getErrorMsg());
				}  else if(exceptionCode.getErrorCode() == Constants.WE_HAVE_ERROR_DESCRIPTION) {
					return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(exceptionCode.getErrorMsg());
				}else {
					return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(exceptionCode.getErrorMsg());
				}
			} else {
				loginUsersServices.writePRD_Suspecious_Token_Audit(refreshTokenRequest.getR(), null, refreshTokenRequest.getRefreshToken(), null, "R", exceptionCode.getErrorMsg(), contextVb);
				throw new AuthenticationException("Invalid token");
			}
			
		} catch (Exception e) {
			// e.printStackTrace();
			loginUsersServices.writePRD_Suspecious_Token_Audit(null, null, null, null, "R",
					"Exception in getting New Refresh token.", contextVb);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

}
