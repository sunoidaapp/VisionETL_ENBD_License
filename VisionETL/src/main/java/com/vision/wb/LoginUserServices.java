package com.vision.wb;

import java.net.InetAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.servlet.http.HttpServletRequest;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.sunoida.LicenseInfoProvider;

import com.vision.authentication.CustomContextHolder;
import com.vision.dao.CommonDao;
import com.vision.dao.LoginUserDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.JwtTokenUtil;
import com.vision.util.RSAEncryptDecryptUtil;
import com.vision.util.ValidationUtil;
import com.vision.vb.BearerTokenVb;
import com.vision.vb.LoginResponse;
import com.vision.vb.RefreshTokenVb;
import com.vision.vb.UpdatePasswordRequest;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VisionUsersVb;

import io.jsonwebtoken.Claims;

@Service
public class LoginUserServices {

	public static Logger logger = LoggerFactory.getLogger(LoginUserServices.class);

	@Autowired
	LoginUserDao loginUserDao;

	@Value("${app.productName}")
	private String productName;
	
	@Value("${vision.token.limit}")
	private String bearerTokenLimit;
	
	@Value("${vision.refresh.token.limit}")
	private String refreshTokenLimit;
	
	@Value("${vision.native.pwd.reset.days}")
	private int visionPswResetDays;
	
	@Value("${vision.login.failed.limit}")
	private int visionMaxFailedLoginLimit;
	
	private static String jasyptSecreatKey;
	private static String defaultJasyptSecreatKey;	
	
	@Value("${encryptor.password}")
	public void setDefaultJasyptSecreatKey(String defaultJasyptSecreatKey) {
		LoginUserServices.defaultJasyptSecreatKey = defaultJasyptSecreatKey;
	}
	
	@Value("${vision.inbound.private.key}")
	private String visionInboundAuthPrivateKey;
	
	@Value("${spring.profiles.active}")
	private String activeProfile;
	
	@Value("${app.enableDebug}")
	protected String enableDebug;
	
	@Value("${sunoida.license.file.hash.key}")
	private String sunoidaLicenseFileHashKey;

	public boolean passwordResetChk(VisionUsersVb userVb) {
		boolean returnFlag = false;
		if(!activeProfile.equalsIgnoreCase("DEFAULT")) {
			returnFlag = false;
		} else if("Y".equalsIgnoreCase(userVb.getPasswordResetFlag())) {
			returnFlag = true;
		} else if(visionPswResetDays>0 && visionPswResetDays<=userVb.getLastPwdResetCount()) {
			returnFlag = true;
		}
		return returnFlag;
	}
	
	public ExceptionCode getActiveUserByUserLoginIdOrUserEmailId(String userIdentityAttValue) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<VisionUsersVb> lUsers = loginUserDao.getUserByUserLoginIdOrUserEmailId(userIdentityAttValue);

			if (lUsers == null || lUsers.isEmpty() || lUsers.size() > 1) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("The username and password entered do not match ["
						+ userIdentityAttValue + "]");
			} else {
				VisionUsersVb vUserVb = lUsers.get(0);
				boolean isAppAllowed = false;
				if (StringUtils.hasText(vUserVb.getApplicationAccess())) {
					String[] allowedApps = vUserVb.getApplicationAccess().split(Constants.COMMA);
					for (String appName : allowedApps) {
						if (appName.equalsIgnoreCase(productName)) {
							isAppAllowed = true;
							break;
						}
					}
				}
				if (0 != vUserVb.getUserStatus()) {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode
							.setErrorMsg("User account is not active for login detail [" + vUserVb.getUserName() + "]");
				} else if (chkUnsuccessfulLoginCountExceeded(vUserVb.getLastUnsuccessfulLoginAttempts())) {
					exceptionCode.setErrorCode(Constants.WE_HAVE_ERROR_DESCRIPTION );
					exceptionCode.setErrorMsg(
							"Too many failed login attempts for Vision ID [" + vUserVb.getUserName() + "]. Account locked.");
				} else if (!isAppAllowed) {
					exceptionCode.setErrorCode(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
					exceptionCode.setErrorMsg("Access denied for login detail [" + vUserVb.getUserName()
							+ "]. User access is restricted for the " + productName + " application. ");
				} else {
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					exceptionCode.setResponse(lUsers.get(0));
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(String.format("Problem fetching User info for login detail [%s]. Cause [%s]",
					userIdentityAttValue, e.getMessage()));
		}
		return exceptionCode;
	}

	public ExceptionCode getActiveUserByVisionId(String visionId) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<VisionUsersVb> lUsers = loginUserDao.getUserByVisionId(visionId);
			if (lUsers == null || lUsers.isEmpty() || lUsers.size() > 1) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("The username and password entered do not match [" + visionId + "]");
			} else {
				VisionUsersVb vUserVb = lUsers.get(0);
				boolean isAppAllowed = false;
				if (StringUtils.hasText(vUserVb.getApplicationAccess())) {
					String[] allowedApps = vUserVb.getApplicationAccess().split(Constants.COMMA);
					for (String appName : allowedApps) {
						if (appName.equalsIgnoreCase(productName)) {
							isAppAllowed = true;
							break;
						}
					}
				}
				if (0 != vUserVb.getUserStatus()) {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("User account is not active for Vision ID [" + vUserVb.getUserName() + "]");
				} else if (chkUnsuccessfulLoginCountExceeded(vUserVb.getLastUnsuccessfulLoginAttempts())) {
					exceptionCode.setErrorCode(Constants.WE_HAVE_ERROR_DESCRIPTION);
					exceptionCode.setErrorMsg(
							"Too many failed login attempts for Vision ID [" + vUserVb.getUserName() + "]. Account locked.");
				} else if (!isAppAllowed) {
					exceptionCode.setErrorCode(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
					exceptionCode.setErrorMsg("Access denied for login detail [" + vUserVb.getUserName()
					+ "]. User access is restricted for the " + productName + " application. ");
				} else if ("Y".equalsIgnoreCase(vUserVb.getPasswordResetFlag())) {
					exceptionCode.setErrorCode(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
					exceptionCode.setErrorMsg("Access denied for login detail [" + vUserVb.getUserName()
					+ "]. User is forced to reset password.");
				} else {
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					exceptionCode.setResponse(lUsers.get(0));
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(String.format("Problem fetching User info for Vision ID [%s]. Cause [%s]",
					visionId, e.getMessage()));
		}
		return exceptionCode;
	}

	public boolean updateActivityDateByUserLoginId(VisionUsersVb dObj) {
		return loginUserDao.updateActivityDateByUserLoginId(dObj);
	}
	
	public int updateUnsuccessfulLoginAttempts(String userId) {
		return loginUserDao.updateUnsuccessfulLoginAttempts(userId);
	}

	private boolean chkUnsuccessfulLoginCountExceeded(String failedLoginCount) {
		if(visionMaxFailedLoginLimit == 0)
			return false;
		if (ValidationUtil.isValid(failedLoginCount) && !"NULL".equalsIgnoreCase(failedLoginCount.trim())) {
			try {
				return (Integer.parseInt(failedLoginCount) > visionMaxFailedLoginLimit) ? true : false;
			} catch (Exception e) {
				return true;
			}
		} else {
			return false;
		}
	}

	public List<UserRestrictionVb> getRestrictionTree() throws Exception {
		try {
			return loginUserDao.getRestrictionTree();
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
				logger.info(
						"Problem in fetching user restriction tree - [MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION'] ");
			}
			throw new Exception(
					"Problem in fetching user restriction tree - [MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION'] ");
		}
	}

	public String getVisionDynamicHashVariable(String macrovarName) throws Exception {
		try {
			return loginUserDao.getVisionDynamicHashVariable(macrovarName);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
				logger.info("Problem in fetching data - [vision_dynamic_hash_var where VARIABLE_NAME = 'VU_RESTRICTION_"
						+ macrovarName + "']");
			}
			throw new Exception(
					"Problem in fetching data - [vision_dynamic_hash_var where VARIABLE_NAME = 'VU_RESTRICTION_"
							+ macrovarName + "']");
		}
	}

	public List<UserRestrictionVb> doUpdateRestrictionToUserObject(VisionUsersVb lUser,
			List<UserRestrictionVb> restrictionList) throws Exception {
		try {
			return loginUserDao.doUpdateRestrictionToUserObject(lUser, restrictionList);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
				logger.info("Problem updating restriction to user object " + lUser.getVisionId());
			}
			throw new Exception("Problem updating restriction to user object " + lUser.getVisionId());
		}
	}

	public void writeUserLoginAudit(String userLoginId, String status, String comments, int visionId, String ipAddress,
			String macAddress, String remoteHostName) {
		try {
			VisionUsersVb vObject = new VisionUsersVb();
			vObject.setUserLoginId(userLoginId);
			vObject.setLoginStatus(status);
			vObject.setComments(comments);
			vObject.setVisionId(visionId);
			vObject.setIpAddress(ipAddress);
			vObject.setMacAddress(macAddress);
			vObject.setRemoteHostName(remoteHostName);
			loginUserDao.insertUserLoginAudit(vObject);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
	}

	/*public void findSystemInfo(HttpServletRequest request, VisionUsersVb vObject) {
		try {
			String clientIP = request.getHeader("X-Forwarded-For");
	        if (clientIP != null && !clientIP.isEmpty()) {
	            // The X-Forwarded-For header may contain a comma-separated list of IP addresses;
	            // the first address in the list is typically the client's IP.
	            clientIP = clientIP.split(",")[0].trim();
	        } else {
	            // If the X-Forwarded-For header is not present, use the remote address as a fallback.
	            clientIP = request.getRemoteAddr();
	        }
	        
		//	String ipAddress = request.getRemoteAddr();
			if ("0:0:0:0:0:0:0:1".equalsIgnoreCase(clientIP)) {
				clientIP = InetAddress.getLocalHost().getHostAddress();
			}
			InetAddress inetAddress = InetAddress.getByName(clientIP);
			vObject.setIpAddress(clientIP);
			vObject.setRemoteHostName(inetAddress.getHostName());
			vObject.setMacAddress(CommonDao.getMacAddress(clientIP));
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}*/
	
	public void findSystemInfo(HttpServletRequest request, VisionUsersVb vObject) {
		try {
			String clientIP = request.getHeader("X-Real-IP");
	        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
	            clientIP = request.getHeader("X-Forwarded-For");
	            if (clientIP != null && !clientIP.isEmpty() && !"unknown".equalsIgnoreCase(clientIP)) {
		            String[] ipList = clientIP.split(",");
		            clientIP = ipList[0].trim();
	            }
	        }

	        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
	            clientIP = request.getRemoteAddr();
	        }

	        if ("0:0:0:0:0:0:0:1".equalsIgnoreCase(clientIP)) {
	            clientIP = InetAddress.getLocalHost().getHostAddress();
	        }
	        InetAddress inetAddress = InetAddress.getByName(clientIP);
	        vObject.setIpAddress(clientIP);
	        vObject.setRemoteHostName(inetAddress.getHostName());
	        vObject.setMacAddress(CommonDao.getMacAddress(clientIP));
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
	}

	public String getRandomNumber() {
		Integer otp = new Random().nextInt(999999);
		int noOfOtpDigit = 6;
		while (Integer.toString(otp).length() != noOfOtpDigit) {
			otp = new Random().nextInt(999999);
		}
		String otpString = String.valueOf(otp);
		return otpString;
	}

	public int insertPrdRefreshToken(VisionUsersVb lUser, String refreshToken, String strTokenCreatedDate, String strTokenExpirationDate, String a_PublicToken, String j_PrivateToken) {
		return loginUserDao.insertPrdRefreshToken(lUser, refreshToken, strTokenCreatedDate, strTokenExpirationDate, a_PublicToken, j_PrivateToken);
	}
	
	public int updateAngularRSAPublicToken(String rSessionID, String angularRSAPublicToken) {
		return loginUserDao.updateAngularRSAPublicToken(rSessionID, angularRSAPublicToken);
	}
	
	public ExceptionCode performValidationOnRefreshToken(String refreshTokenReq, String rSessionIdReq) {
		ExceptionCode exceptionCode = new ExceptionCode();
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		exceptionCode.setErrorMsg("Invalid Token");
		try {
			if(StringUtils.hasText(refreshTokenReq)) {
				if(StringUtils.hasText(rSessionIdReq)) {
					RefreshTokenVb refreshTokenVb = loginUserDao.getRefreshTokenInfoWith_rSessionID(rSessionIdReq);
					if(refreshTokenVb!=null) {
						if(refreshTokenVb.getUtilizationCount()<Integer.parseInt(refreshTokenLimit)) {
							/* Increase utilization count for refresh token */
							loginUserDao.increaseUtilizationCount_RefreshToken(rSessionIdReq);
							refreshTokenVb.setUtilizationCount((refreshTokenVb.getUtilizationCount()+1));
							
							String decryptedRefreshTokenReq = JwtTokenUtil.decodeAndDecryptToken(refreshTokenReq, refreshTokenVb.getjPrivateKey());
//							String decryptedRefreshTokenFromTable = JwtTokenUtil.decodeAndDecryptToken(refreshTokenVb.getRefreshToken(), refreshTokenVb.getjPrivateKey());
							
							if(decryptedRefreshTokenReq.equals(refreshTokenVb.getRefreshToken())) {
								if(refreshTokenVb.getTokenStatus() == 0) {
									if(JwtTokenUtil.validateToken(refreshTokenVb.getRefreshToken())) {
										Claims claims = JwtTokenUtil.parseToken(refreshTokenVb.getRefreshToken());
										String visionId = String.valueOf(claims.get("v"));
										String rSessionId = String.valueOf(claims.get("r"));
										if(visionId.equalsIgnoreCase(refreshTokenVb.getVisionId())) {
											exceptionCode = JwtTokenUtil.validateTokenSecondaryChk(contextVb, refreshTokenVb);
											if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
												exceptionCode.setResponse(refreshTokenVb);
												exceptionCode.setOtherInfo(refreshTokenVb.getRefreshToken());
												return exceptionCode;
											} else {
												exceptionCode.setErrorMsg(String.format("Invalid refresh token - Cause [%s]", exceptionCode.getErrorMsg()));
												return exceptionCode;
											}
										} else {
											exceptionCode.setErrorMsg("Invalid refresh token - Vision ID is not matching");
											return exceptionCode;
										}
									} else {
										exceptionCode.setErrorMsg("Invalid refresh token- Failed token validation for expiry or signing");
										return exceptionCode;
									}
								} else {
									exceptionCode.setErrorMsg("Invalid refresh token- token not active");
									return exceptionCode;
								}
							} else {
								exceptionCode.setErrorMsg("Invalid refresh token- Refresh tokens are not matching");
								return exceptionCode;
							}
						} else {
							exceptionCode.setErrorMsg("Invalid refresh token- token utilization count exceeded");
							return exceptionCode;
						}
					} else {
						exceptionCode.setErrorMsg("Invalid refresh token- rSessionID cannot find data from storage");
						return exceptionCode;
					}
				} else {
					exceptionCode.setErrorMsg("Invalid refresh token- rSessionID not available in request");
					return exceptionCode;
				}
			} else {
				exceptionCode.setErrorMsg("Invalid refresh token- token not available in request");
				return exceptionCode;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(String.format("Invalid refresh token - Cause [%s]", e.getMessage()));
			return exceptionCode;
		}
	}
	
	public void writePRD_Suspecious_Token_Audit(String rSessionId, String bSessionId, String refreshToken, String bearerToken, String tokenType,
			String comments, VisionUsersVb contextVb){
		loginUserDao.writePRD_Suspecious_Token_Audit(rSessionId, bSessionId, refreshToken, bearerToken, tokenType, comments, contextVb);
	}
	
	public ExceptionCode getNewBearerTokenByRefreshToken(RefreshTokenVb existingRtokenVb, String refreshToken) {
		ExceptionCode exceptionCode = new ExceptionCode();
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		try {
			Claims claims = JwtTokenUtil.parseToken(refreshToken);
			String visionId = String.valueOf(claims.get("v"));
			String rSessionId = String.valueOf(claims.get("r"));
			BearerTokenVb existingBtokenVb = loginUserDao.getBearerTokenWithPrdID_rSessID_visionID(rSessionId, visionId);
			
			exceptionCode = getActiveUserByVisionId(visionId);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				VisionUsersVb lUser = (VisionUsersVb) exceptionCode.getResponse();
				lUser.setUserGrpProfile(lUser.getUserGroup() + "-" + lUser.getUserProfile());
				lUser.setrSessionId(rSessionId);
				lUser.setIpAddress(contextVb.getIpAddress());
				lUser.setMacAddress(contextVb.getMacAddress());
				lUser.setRemoteHostName(contextVb.getRemoteHostName());
				lUser.setComments("Success");
				/*if (ValidationUtil.isValid(existingBtokenVb)) {
					lUser.setbSessionId(existingBtokenVb.getbSessionId());
				} else {
					lUser.setbSessionId(lUser.getVisionId()+"_"+getRandomNumber()+"_"+System.currentTimeMillis());
				}*/
				if (ValidationUtil.isValid(existingBtokenVb)) {
					if(existingBtokenVb.getTokenStatus() == 0) {
						lUser.setbSessionId(existingBtokenVb.getbSessionId());
					} else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Session Invalid");
						return exceptionCode;
					}
				} else {
					lUser.setbSessionId(lUser.getVisionId()+"_"+getRandomNumber()+"_"+System.currentTimeMillis());
				}
				// Custom logic to generate JWT token( Bearer Token )
				exceptionCode = JwtTokenUtil.generateJwtToken(lUser);
				String token = (String) exceptionCode.getResponse();
				Map<String, String> returnMap = CommonUtils.extractCreationExpireDate(token);
				
				if (ValidationUtil.isValid(existingBtokenVb)) {
					loginUserDao.selectAndInsertPrdBearerTokenIntoAudit(lUser);
					loginUserDao.updatePrdBearerToken(lUser, token, returnMap.get("validTill"));
				} else {
					loginUserDao.writePrdBearerToken(lUser, token, returnMap.get("dateCreation"), returnMap.get("validTill"));
					writeUserLoginAudit(lUser.getUserName(), "SUCCESS", null, Integer.parseInt(visionId), contextVb.getIpAddress(), contextVb.getMacAddress(),
							contextVb.getRemoteHostName());
				}
				
				// Return the token as response
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				
				String responseEncryptedToken = JwtTokenUtil.encryptAndEncodeToken(token, existingRtokenVb.getaPublicKey()); //Encrypted using Angular RSA Public key
				
				exceptionCode.setResponse(new LoginResponse(responseEncryptedToken));
				return exceptionCode;
			} else {
				writeUserLoginAudit(visionId, "FAILED", exceptionCode.getErrorMsg(),
						Integer.parseInt(visionId), contextVb.getIpAddress(), contextVb.getMacAddress(),
						contextVb.getRemoteHostName());
				return exceptionCode;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Unable to generate bearer token");
			return exceptionCode;
		}
	}
	
	public ExceptionCode getNewRefreshTokenByRefreshToken(RefreshTokenVb existingRtokenVb, String refreshToken) {
		ExceptionCode exceptionCode = new ExceptionCode();
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		try {
			Claims claims = JwtTokenUtil.parseToken(refreshToken);
			String visionId = String.valueOf(claims.get("v"));
			String rSessionId = String.valueOf(claims.get("r"));
			
			exceptionCode = getActiveUserByVisionId(visionId);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				VisionUsersVb lUser = (VisionUsersVb) exceptionCode.getResponse();
				lUser.setUserGrpProfile(lUser.getUserGroup() + "-" + lUser.getUserProfile());
				lUser.setrSessionId(rSessionId);
				lUser.setIpAddress(contextVb.getIpAddress());
				lUser.setMacAddress(contextVb.getMacAddress());
				lUser.setRemoteHostName(contextVb.getRemoteHostName());
				lUser.setComments("Success");
				// Custom logic to generate JWT token( Refresh Token )
				exceptionCode = JwtTokenUtil.generateRefreshJwtToken(lUser);
				String newRefreshToken = (String) exceptionCode.getResponse();
				Map<String, String> returnMap = CommonUtils.extractCreationExpireDate(newRefreshToken);

				loginUserDao.selectAndInsertPrdRefreshTokenIntoAudit(lUser);
				loginUserDao.updatePrdRefreshToken(lUser, newRefreshToken, returnMap.get("validTill"));
				
				// Return the token as response
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);

				String responseEncryptedToken = JwtTokenUtil.encryptAndEncodeToken(newRefreshToken,
						existingRtokenVb.getaPublicKey()); // Encrypted using Angular RSA Public key
				exceptionCode.setResponse(new LoginResponse(null, responseEncryptedToken));
				return exceptionCode;
			} else {
				writeUserLoginAudit(visionId, "FAILED", exceptionCode.getErrorMsg(), Integer.parseInt(visionId),
						contextVb.getIpAddress(), contextVb.getMacAddress(), contextVb.getRemoteHostName());
				return exceptionCode;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg("Unable to generate refresh token");
			return exceptionCode;
		}
	}
	
	public ExceptionCode performValidationOnBearerToken(String encryptedBearerTokenReq, String rSessionIdReq) {
		ExceptionCode exceptionCode = new ExceptionCode();
		VisionUsersVb contextVb = CustomContextHolder.getContext();
		exceptionCode.setErrorMsg("Invalid Token");
		try {
			if(StringUtils.hasText(encryptedBearerTokenReq)) {
				if(StringUtils.hasText(rSessionIdReq)) {
					RefreshTokenVb refreshTokenVb = loginUserDao.getRefreshTokenInfoWith_rSessionID(rSessionIdReq);
					if(refreshTokenVb!=null) {
						if(refreshTokenVb.getUtilizationCount()<Integer.parseInt(bearerTokenLimit)) {
							if(refreshTokenVb.getTokenStatus() == 0) {
								String bearerTokenReq = JwtTokenUtil.decodeAndDecryptToken(encryptedBearerTokenReq, refreshTokenVb.getjPrivateKey());
								
								Claims claims = JwtTokenUtil.parseToken(bearerTokenReq);
								String visionId = String.valueOf(claims.get("v"));
								String rSessionId = String.valueOf(claims.get("r"));
								String bSessionIdReq = String.valueOf(claims.get("b"));
								
								
								BearerTokenVb bearerTokenVb = loginUserDao.getBearerTokenWithPrdID_rSessID_visionID(rSessionId, visionId);
								
								if(bearerTokenVb!=null) {
									if(bearerTokenVb.getUtilizationCount()<Integer.parseInt(bearerTokenLimit)) {
										/* Increase utilization count for bearer token */
										loginUserDao.increaseUtilizationCount_BearerToken(rSessionIdReq, bSessionIdReq);
										bearerTokenVb.setUtilizationCount((bearerTokenVb.getUtilizationCount()+1));
										
//										String decryptedBearerTokenFromTable = JwtTokenUtil.decodeAndDecryptToken(bearerTokenVb.getBearerToken(), refreshTokenVb.getjPrivateKey());
										if(bearerTokenReq.equals(bearerTokenVb.getBearerToken())) {
											if(bearerTokenVb.getTokenStatus() == 0) {
												if(JwtTokenUtil.validateToken(bearerTokenVb.getBearerToken())) {
													if(visionId.equalsIgnoreCase(bearerTokenVb.getVisionId())) {
														exceptionCode = JwtTokenUtil.validateBearerTokenSecondaryChk(contextVb, bearerTokenVb);
														if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
															exceptionCode.setResponse(bearerTokenVb);
															exceptionCode.setOtherInfo(bearerTokenVb.getBearerToken());
															return exceptionCode;
														} else {
															exceptionCode.setErrorMsg(String.format("Invalid bearer token - Cause [%s]", exceptionCode.getErrorMsg()));
															return exceptionCode;
														}
													} else {
														exceptionCode.setErrorMsg("Invalid bearer token - Vision ID is not matching");
														return exceptionCode;
													}
												} else {
													exceptionCode.setErrorMsg("Invalid bearer token- Failed token validation for expiry or signing");
													return exceptionCode;
												}
											} else {
												exceptionCode.setErrorMsg("Invalid bearer token- bearer token not active");
												return exceptionCode;
											}
										} else {
											exceptionCode.setErrorMsg("Invalid bearer token- Bearer tokens are not matching");
											return exceptionCode;
										}
									} else {
										exceptionCode.setErrorMsg("Invalid bearer token- bearer token utilization count exceeded");
										return exceptionCode;
									}
								} else {
									exceptionCode.setErrorMsg("Invalid bearer token- bSessionID cannot find data from storage");
									return exceptionCode;
								}
							} else {
								exceptionCode.setErrorMsg("Invalid bearer token- refresh token not active");
								return exceptionCode;
							}
						} else {
							exceptionCode.setErrorMsg("Invalid bearer token- refresh token utilization count exceeded");
							return exceptionCode;
						}
					} else {
						exceptionCode.setErrorMsg("Invalid bearer token- rSessionID cannot find data from storage");
						return exceptionCode;
					}
				} else {
					exceptionCode.setErrorMsg("Invalid bearer token- rSessionID not available in request");
					return exceptionCode;
				}
			} else {
				exceptionCode.setErrorMsg("Invalid bearer token- token not available in request");
				return exceptionCode;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(String.format("Invalid bearer token - Cause [%s]", e.getMessage()));
			return exceptionCode;
		}
	}
	public ExceptionCode visionPwdCheck(String visionId,String password) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String userPwdTbl = loginUserDao.getExistingVisionPwd(visionId);
			if(ValidationUtil.isValid(userPwdTbl)) {
				userPwdTbl = jaspytPasswordDecrypt(userPwdTbl);
				if(password.equals(userPwdTbl)) {
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				}
			}
		} catch(Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	public static String jaspytPasswordDecrypt(String encryptedPwd) {
		String decryptedPwd = "";
		try {
			StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
	    	encryptor.setPassword(jasyptSecreatKey);
	        encryptor.setAlgorithm("PBEWithSHA1AndDESede");
			//encryptedPwd = encryptedPwd.substring(4, encryptedPwd.length()-1);
			decryptedPwd = encryptor.decrypt(encryptedPwd);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return decryptedPwd;
	}

	public ExceptionCode updatePassword(UpdatePasswordRequest updatePasswordRequest, String visionID) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String newPwdReq = RSAEncryptDecryptUtil.decryptUsingPrivateKeyString(updatePasswordRequest.getNewPassword(), visionInboundAuthPrivateKey);
			int result = loginUserDao.updateUserPassword(ValidationUtil.jasyptEncryption(newPwdReq), visionID);
			exceptionCode.setErrorCode(result);
		} catch (Exception e) {
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	public ExceptionCode validateLinceKey(VisionUsersVb lUser) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			loginUserDao.getLicenseDetails(lUser);
			if (!ValidationUtil.isValid(lUser.getLicenseHashValue())
					|| !ValidationUtil.isValid(lUser.getLicenseKey())
					|| !ValidationUtil.isValid(lUser.getLicenseStr())) {
///				return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body("Client Id not Maintained. - Contact System Admin");
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Client Id not Maintained. - Contact System Admin");
				return exceptionCode;
			} 
			if (!ValidationUtil.isValid(lUser.getLicenseHashValue())) {
//				return ResponseEntity.status(HttpStatus.UNAVAILABLE_FOR_LEGAL_REASONS).body("License not Generated. - Contact System Admin");
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("License Validation Failed. - Contact System Admin");
				return exceptionCode;
			}

			String decryptlicenseInfo = LicenseInfoProvider.getInfo("all", lUser.getLicenseHashValue(), lUser.getLicenseStr(), lUser.getLicenseKey(), sunoidaLicenseFileHashKey.trim());
			// o=A0001||bc=A0001||d=3||cu=50||nuc=50||ad=24-May-2024||ed=23-Nov-2029||g=10||
			
			if(!ValidationUtil.isValid(decryptlicenseInfo)) {
//				return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body("License Validation Failed. - Contact System Admin");
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("License Validation Failed. - Contact System Admin");
				return exceptionCode;				
			} else {
				String activationDateStr = CommonUtils.getattributeValues(decryptlicenseInfo, "ad");
				String expiryDateStr = CommonUtils.getattributeValues(decryptlicenseInfo, "ed");
				int graceDays = Integer.parseInt(CommonUtils.getattributeValues(decryptlicenseInfo, "g"));
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
				LocalDate activationDate = LocalDate.parse(activationDateStr, formatter);
				LocalDate expiryDate = LocalDate.parse(expiryDateStr, formatter);
				LocalDate currentDate = LocalDate.now();
				boolean isCurrentDateInRange = !currentDate.isBefore(activationDate) && !currentDate.isAfter(expiryDate.plusDays(graceDays));
				if (!isCurrentDateInRange) {
//					return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body("License Expired. - Contact System Admin");
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("License Expired. - Contact System Admin");
					return exceptionCode;
				}
			}
		} catch(Exception ex) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Invalid License. - Contact System Admin");
			return exceptionCode;	
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION); 
		return exceptionCode;
	}
	

	@Bean
	public LicenseInfoProvider LicenseInfoProviderBean() {
		return new LicenseInfoProvider();
	}

}
