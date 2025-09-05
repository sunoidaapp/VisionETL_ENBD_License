package com.vision.controller;

import java.net.URLDecoder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.Constants;
import com.vision.vb.RefreshTokenRequest;
import com.vision.vb.RefreshTokenVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LoginUserServices;

import io.swagger.annotations.Api;

@RestController
@RequestMapping("supportController")
@Api(value = "supportController", description = "Support Controller")
@Profile("SAML2")
public class SSOSupportController {
	
	@Autowired
	private LoginUserServices loginUsersServices;
	
	@PostMapping("/putAPK")
	public ResponseEntity storeAngularPublicKeyWithR_SessionID(@RequestBody RefreshTokenRequest refreshTokenRequest) {
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		try {
			String encryptedRefreshToken = refreshTokenRequest.getRefreshToken();
			String rSessionID = refreshTokenRequest.getR();

			ExceptionCode exceptionCode = loginUsersServices.performValidationOnRefreshToken(encryptedRefreshToken, rSessionID);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				RefreshTokenVb existingRtokenVb = (RefreshTokenVb) exceptionCode.getResponse();
				String decryptedRT = (String) exceptionCode.getOtherInfo();
				
				if(existingRtokenVb.getUtilizationCount()==1) {
					loginUsersServices.updateAngularRSAPublicToken(existingRtokenVb.getrSessionId(), URLDecoder.decode(refreshTokenRequest.getA(), "UTF-8"));
					return ResponseEntity.ok().build();
				} else {
					loginUsersServices.writePRD_Suspecious_Token_Audit(refreshTokenRequest.getR(), null, refreshTokenRequest.getRefreshToken(), null, "R", "Support post apk is used when Utilization_Count is greater then 1", contextVb);
					return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
				}
				
			} else {
				loginUsersServices.writePRD_Suspecious_Token_Audit(refreshTokenRequest.getR(), null, refreshTokenRequest.getRefreshToken(), null, "R", exceptionCode.getErrorMsg(), contextVb);
				return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			loginUsersServices.writePRD_Suspecious_Token_Audit(null, null, null, null, "R",
					"Exception in getting New Bearer token.", contextVb);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}
}
