package com.vision.controller;

import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.saml2.provider.service.authentication.Saml2AuthenticatedPrincipal;
import org.springframework.ui.Model;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.JwtTokenUtil;
import com.vision.util.RSAEncryptDecryptUtil;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LoginUserServices;
import com.vision.wb.LogoutUserServices;

@RestController
@Profile("SAML2")
public class SAML2_ResponseHandlerController {

	public static Logger logger = LoggerFactory.getLogger(SAML2_ResponseHandlerController.class);

	@Value("${sso.saml2.returnViewURL}")
	private String returnViewURL;

	@Value("${sso.saml2.attributes.list}")
	private String userAttributesListStr;

	@Autowired
	private LoginUserServices loginUsersServices;
	
	@Autowired
	private LogoutUserServices logoutUsersServices;
	
	@Autowired
	private Environment environment;
	
	@Value("${vision.outbound.public.key}")
	private String visionOutboundPublicKey; //Angular static public key
	
	@Value("${vision.concurrent.access.flag}")
	private String concurrentAccessFlag;

	@GetMapping("/")
	public RedirectView home(Model model, @AuthenticationPrincipal Saml2AuthenticatedPrincipal principal) {
		String userId = "";
		
		try {
			ExceptionCode exceptionCode = new ExceptionCode();

			String[] userAttributesList = userAttributesListStr.split(",");
			// username,emailAddress
			for (String userAttr : userAttributesList) {
				String userAttrName = environment.getProperty("sso.saml2.user." + userAttr);
				String userAttrValue = principal.getFirstAttribute(userAttrName);
				model.addAttribute(userAttrName, userAttrValue);
			}
			model.addAttribute("userAttributes", principal.getAttributes());

			/*
			 * String emailAddress = principal.getFirstAttribute("email");
			 * model.addAttribute("emailAddress", emailAddress);
			 */

			if (model.getAttribute(environment.getProperty("sso.saml2.user.email")) != null)
				userId = String.valueOf(model.getAttribute(environment.getProperty("sso.saml2.user.email")));
			else if (!StringUtils.hasText(userId)
					&& model.getAttribute(environment.getProperty("sso.saml2.user.name")) != null)
				userId = String.valueOf(model.getAttribute(environment.getProperty("sso.saml2.user.name")));
		
			
			if (StringUtils.hasText(userId)) {
				exceptionCode = loginUsersServices.getActiveUserByUserLoginIdOrUserEmailId(userId);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					VisionUsersVb lUser = (VisionUsersVb) exceptionCode.getResponse();
					// license - start
					exceptionCode = loginUsersServices.validateLinceKey(lUser);
					if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						// return
						// ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(exceptionCode.getErrorMsg());
						String fullUrl = String.format("%s?r=%s&jpk=%s&status=%s&message=%s", returnViewURL, "", "",
								String.valueOf(Constants.ERRONEOUS_OPERATION), exceptionCode.getErrorMsg());
						loginUsersServices.writeUserLoginAudit(userId, "FAILED", exceptionCode.getErrorMsg(), 0,
								CustomContextHolder.getContext().getIpAddress(),
								CustomContextHolder.getContext().getMacAddress(),
								CustomContextHolder.getContext().getRemoteHostName());
						return new RedirectView(fullUrl);
 
					}
					// license - end
					
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
					lUser.setrSessionId(lUser.getVisionId()+"_"+loginUsersServices.getRandomNumber()+"_"+System.currentTimeMillis());

					// Custom logic to generate JWT token
//					String token = JwtTokenUtil.generateJwtToken(lUser);

					// Generate Refresh token
					exceptionCode  = JwtTokenUtil.generateRefreshJwtToken(lUser);
					String refreshToken = (String) exceptionCode.getResponse();
					  
					Map<String, String> returnMap = CommonUtils.extractCreationExpireDate(refreshToken);
					lUser.setIpAddress(CustomContextHolder.getContext().getIpAddress());
					lUser.setMacAddress(CustomContextHolder.getContext().getMacAddress());
					lUser.setRemoteHostName(CustomContextHolder.getContext().getRemoteHostName());
					lUser.setComments("Success");
//					loginUsersServices.insertPrdRefreshToken(lUser, refreshToken, expiration);
					
					Map<String, String> rsaKeyMap = RSAEncryptDecryptUtil.generatePublicAndPrivateKeyString();
					
					if("N".equalsIgnoreCase(concurrentAccessFlag))
						logoutUsersServices.invalidateAllAccessTokensByVisionID(lUser);
					
					int result = loginUsersServices.insertPrdRefreshToken(lUser, refreshToken, returnMap.get("dateCreation"), returnMap.get("validTill"), null, rsaKeyMap.get("Private"));
					if(result!=Constants.SUCCESSFUL_OPERATION) {
						String fullUrl = String.format("%s?r=%s&jpk=%s&status=%s&message=%s", returnViewURL, "", "",
								String.valueOf(Constants.ERRONEOUS_OPERATION), "Authentication success. Login failed - System error");
						loginUsersServices.writeUserLoginAudit(userId, "FAILED", "Unable to insert refresh token into PRD_REFRESH_TOKEN table", 0, CustomContextHolder.getContext().getIpAddress(),
								CustomContextHolder.getContext().getMacAddress(),
								CustomContextHolder.getContext().getRemoteHostName());
						return new RedirectView(fullUrl);
					}


					// Construct the full URL with the token as a query parameter
					String responseEncryptedToken = JwtTokenUtil.encryptAndEncodeToken(refreshToken, visionOutboundPublicKey); //Encrypted using Angular RSA Public key
					
					String fullUrl = String.format("%s?r=%s&jpk=%s&status=%s&message=%s", returnViewURL,
							responseEncryptedToken, URLEncoder.encode(rsaKeyMap.get("Public"), "UTF-8"), String.valueOf(HttpStatus.OK), "Success");
					
					return new RedirectView(fullUrl);
				} else {
					// Construct the full URL with the token as a query parameter
					String fullUrl = String.format("%s?r=%s&jpk=%s&status=%s&message=%s", returnViewURL, "", "",
							String.valueOf(exceptionCode.getErrorCode()), exceptionCode.getErrorMsg());
					loginUsersServices.writeUserLoginAudit(userId, "FAILED", exceptionCode.getErrorMsg(), 0, CustomContextHolder.getContext().getIpAddress(),
							CustomContextHolder.getContext().getMacAddress(),
							CustomContextHolder.getContext().getRemoteHostName());
					return new RedirectView(fullUrl);
				}
			} else {
				String fullUrl = String.format("%s?r=%s&jpk=%s&status=%s&message=%s", returnViewURL, "", "",
						String.valueOf(HttpStatus.EXPECTATION_FAILED),
						"Unable to fetch username or emailId from the auth token to authorize against Vision user list");
				loginUsersServices.writeUserLoginAudit(userId, "FAILED",
						"Unable to fetch username or emailId from the auth token to authorize against Vision user list",
						0, CustomContextHolder.getContext().getIpAddress(),
						CustomContextHolder.getContext().getMacAddress(),
						CustomContextHolder.getContext().getRemoteHostName());
				return new RedirectView(fullUrl);
			}
		} catch (Exception e) {
			// e.printStackTrace();
			String fullUrl = String.format("%s?r=%s&jpk=%s&status=%s&message=Problem unknown. Cause [%s]",
					returnViewURL, "", "", String.valueOf(HttpStatus.PRECONDITION_FAILED), e.getMessage());
			loginUsersServices.writeUserLoginAudit(userId, "FAILED", e.getMessage(), 0, CustomContextHolder.getContext().getIpAddress(),
					CustomContextHolder.getContext().getMacAddress(),
					CustomContextHolder.getContext().getRemoteHostName());
			return new RedirectView(fullUrl);
		}
	}

}
