package com.vision.controller;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.security.sasl.AuthenticationException;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.JwtTokenUtil;
import com.vision.util.RSAEncryptDecryptUtil;
import com.vision.vb.LoginRequest;
import com.vision.vb.LoginResponse;
import com.vision.vb.RefreshTokenVb;
import com.vision.vb.UpdatePasswordRequest;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LoginUserServices;
import com.vision.wb.LogoutUserServices;

@RestController
@Profile("DEFAULT")
public class LoginController {

	@Autowired
	private LoginUserServices loginUsersServices;
	
	@Autowired
	private LogoutUserServices logoutUserServices;
	
	private final AuthenticationManager authenticationManager;
	
	@Value("${vision.concurrent.access.flag}")
	private String concurrentAccessFlag;
	
	@Value("${vision.default.auth.adServers}")
	private String visionAdServers = "skip";
	
	@Value("${vision.inbound.private.key}")
	private String visionInboundAuthPrivateKey;
	
	public LoginController(AuthenticationManager authenticationManager) {
		this.authenticationManager = authenticationManager;
	}
	
	public static Logger logger = LoggerFactory.getLogger(LoginController.class);
	@Value("${app.clientName}")
	private String clientName;
	
	@PostMapping("/login")
	public ResponseEntity<?> login(@RequestBody LoginRequest loginRequest) {
		ExceptionCode exceptionCode = new ExceptionCode();
		VisionUsersVb lUser = new VisionUsersVb();
		boolean passwordResetFlag = false;

		try {

			// Create an authentication token with the provided username and password
			Authentication authenticationToken = new UsernamePasswordAuthenticationToken(loginRequest.getUsername(),
					loginRequest.getPassword());

			// Authenticate the authentication token using the AuthenticationManager
			Authentication authentication = authenticationManager.authenticate(authenticationToken);

			// Generate a token for successful authentication (e.g., JWT token)
			UserDetails userDetails = (UserDetails) authentication.getPrincipal();

			// Get Vision user details with the User Login ID from the 'UserDetails'

			exceptionCode = loginUsersServices.getActiveUserByUserLoginIdOrUserEmailId(userDetails.getUsername());
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				lUser = (VisionUsersVb) exceptionCode.getResponse();
				
				// license - start
				logger.error("Start time " + new Date());
				exceptionCode = loginUsersServices.validateLinceKey(lUser);
				if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(exceptionCode.getErrorMsg());
				}
				// license - end
				logger.info("end time " + new Date());
				
				//Check to force user to reset the password
				if("native".equalsIgnoreCase(visionAdServers)) {
					passwordResetFlag = loginUsersServices.passwordResetChk(lUser);
				}
				lUser.setLastSuccessfulLoginDate(lUser.getLastActivityDate());
				loginUsersServices.updateActivityDateByUserLoginId(lUser);
				lUser.setUserGrpProfile(lUser.getUserGroup() + "-" + lUser.getUserProfile());
				
				String userGrpProf = "";
				int cnt = 1;
				String[] userGrpProfileArr = lUser.getUserGrpProfile().split(",");
				for(int i=0; i<userGrpProfileArr.length; i++) {
					userGrpProf = userGrpProf+"'"+userGrpProfileArr[i]+"'";
					if(cnt != userGrpProfileArr.length)
						userGrpProf = userGrpProf+",";
					cnt++;
				}
				lUser.setUserGrpProfile(userGrpProf);
				if ("Y".equalsIgnoreCase(lUser.getUpdateRestriction())) {
					/* Update restriction - Start */
					List<UserRestrictionVb> restrictionList = loginUsersServices.getRestrictionTree();
					Iterator<UserRestrictionVb> restrictionItr = restrictionList.iterator();
					while (restrictionItr.hasNext()) {
						UserRestrictionVb restrictionVb = restrictionItr.next();
						restrictionVb.setRestrictionSql(
								loginUsersServices.getVisionDynamicHashVariable(restrictionVb.getMacrovarName()));
					}
					restrictionList = loginUsersServices.doUpdateRestrictionToUserObject(lUser, restrictionList);
					lUser.setRestrictionList(restrictionList);
					/* Update restriction - End */
				}

				// Custom logic to generate JWT token
//				String token = JwtTokenUtil.generateJwtToken(lUser);

				lUser.setrSessionId(lUser.getVisionId()+"_"+loginUsersServices.getRandomNumber()+"_"+System.currentTimeMillis());
				
				// Generate Refresh token
				exceptionCode = JwtTokenUtil.generateRefreshJwtToken(lUser);
				
				String refreshToken = (String) exceptionCode.getResponse();
				
				  
                Map<String, String> returnMap = CommonUtils.extractCreationExpireDate(refreshToken);
                lUser.setIpAddress(CustomContextHolder.getContext().getIpAddress());
                lUser.setMacAddress(CustomContextHolder.getContext().getMacAddress());
                lUser.setRemoteHostName(CustomContextHolder.getContext().getRemoteHostName());
                lUser.setComments("Success");
                
                
                Map<String, String> rsaKeyMap = RSAEncryptDecryptUtil.generatePublicAndPrivateKeyString();
                
                if("N".equalsIgnoreCase(concurrentAccessFlag))
                	logoutUserServices.invalidateAllAccessTokensByVisionID(lUser);
                
                int result = loginUsersServices.insertPrdRefreshToken(lUser, refreshToken, returnMap.get("dateCreation"), returnMap.get("validTill"), URLDecoder.decode(loginRequest.getA(), "UTF-8"), rsaKeyMap.get("Private"));

				if (result != Constants.SUCCESSFUL_OPERATION) {
					loginUsersServices.writeUserLoginAudit(String.valueOf(lUser.getVisionId()), "FAILED", "Unable to insert refresh token into PRD_REFRESH_TOKEN table", 0, CustomContextHolder.getContext().getIpAddress(),
							CustomContextHolder.getContext().getMacAddress(),
							CustomContextHolder.getContext().getRemoteHostName());
					return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Authentication success. Login failed - System error");
				}
                
				// Return the token as response
                String responseEncryptedToken = JwtTokenUtil.encryptAndEncodeToken(refreshToken, URLDecoder.decode(loginRequest.getA(), "UTF-8")); //Encrypted using Angular RSA Public key
				return ResponseEntity.ok(new LoginResponse(null, responseEncryptedToken, URLEncoder.encode(rsaKeyMap.get("Public"), "UTF-8"), ((passwordResetFlag)?"Y":"N")));
			} else {
				loginUsersServices.writeUserLoginAudit(userDetails.getUsername(), "FAILED", exceptionCode.getErrorMsg(),
						0, CustomContextHolder.getContext().getIpAddress(),
						CustomContextHolder.getContext().getMacAddress(),
						CustomContextHolder.getContext().getRemoteHostName());
				return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(exceptionCode.getErrorMsg());
			}

		} catch (Exception e) {
			loginUsersServices.writeUserLoginAudit(loginRequest.getUsername(), "FAILED", e.getMessage(), 0,
					CustomContextHolder.getContext().getIpAddress(), CustomContextHolder.getContext().getMacAddress(),
					CustomContextHolder.getContext().getRemoteHostName());
			
			loginUsersServices.updateUnsuccessfulLoginAttempts(loginRequest.getUsername());
			
			// Invalid credentials
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(e.getMessage());
		}
	}
	@PostMapping("/updatePassword")
	public ResponseEntity<?> updatePassword(@RequestBody UpdatePasswordRequest updatePasswordRequest) {
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		try {
			String encryptedRefreshToken = updatePasswordRequest.getRefreshToken();
			String rSessionID = updatePasswordRequest.getR();

			ExceptionCode exceptionCode = loginUsersServices.performValidationOnRefreshToken(encryptedRefreshToken,
					rSessionID);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				RefreshTokenVb existingRtokenVb = (RefreshTokenVb) exceptionCode.getResponse();
				String currentPwdReq = RSAEncryptDecryptUtil.decryptUsingPrivateKeyString(updatePasswordRequest.getCurrentPassword(), visionInboundAuthPrivateKey);
				exceptionCode = loginUsersServices.visionPwdCheck(existingRtokenVb.getVisionId(), currentPwdReq);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = loginUsersServices.updatePassword(updatePasswordRequest, existingRtokenVb.getVisionId());
					if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
						return ResponseEntity.ok(exceptionCode.getResponse());
					} else {
						loginUsersServices.writePRD_Suspecious_Token_Audit(updatePasswordRequest.getR(), null, updatePasswordRequest.getRefreshToken(), null, "R", exceptionCode.getErrorMsg(), contextVb);
						return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid credential");
//						throw new AuthenticationException("Invalid credential");
					}
				} else {
					loginUsersServices.writePRD_Suspecious_Token_Audit(updatePasswordRequest.getR(), null, updatePasswordRequest.getRefreshToken(), null, "R", "Current password invalid", contextVb);
//					throw new AuthenticationException("Invalid credential");
					return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Current password invalid");
				}
			} else {
				loginUsersServices.writePRD_Suspecious_Token_Audit(updatePasswordRequest.getR(), null, updatePasswordRequest.getRefreshToken(), null, "R", exceptionCode.getErrorMsg(), contextVb);
				throw new AuthenticationException("Invalid token");
			}

		} catch (Exception e) {
			e.printStackTrace();
			loginUsersServices.writePRD_Suspecious_Token_Audit(updatePasswordRequest.getR(), null, updatePasswordRequest.getRefreshToken(), null, "R", "Exception in updating user password. Cause - "+e.getMessage(), contextVb);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}
}
