package com.vision.util;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;

import javax.crypto.SecretKey;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.vision.exception.ExceptionCode;
import com.vision.vb.BearerTokenVb;
import com.vision.vb.RefreshTokenVb;
import com.vision.vb.VisionUsersVb;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

@Component
@SuppressWarnings({ "deprecation"})
public class JwtTokenUtil {
	
	//To generate random secret key
	private static final SecretKey RANDOM_SECRET_KEY = generateRandomSecretKey();

    private static String SECRET_KEY;
	
//	private static final SecretKey SECRET_KEY = JwtSecretKey.SECRET_KEY;
    private static long TOKEN_EXPIRATION_TIME = 900_000; // 15 minutes
    private static long REFRESH_TOKEN_EXPIRATION_TIME = 864_000_000; // 10 days
	
	private static String tokenEncryptionFlag = "N";
	
	private static String tokenSecondaryValidationFlag = "Y";
	private static String tokenIpValidationFlag = "Y";
	private static String tokenHostValidationFlag = "Y";
	private static String tokenMacValidationFlag = "Y";
	
	public static ExceptionCode generateJwtToken(VisionUsersVb lUser) throws Exception {
		
//		String rsaPublicKeyDecoded = URLDecoder.decode(rsaPublicKey, "UTF-8");
		
		ExceptionCode exceptionCode = new ExceptionCode();
		Date now = new Date();
        Date expiration = new Date(now.getTime() + TOKEN_EXPIRATION_TIME);
		
        /* Claim Descriptions
         * 1. v - visionID
         * 2. ud - userDetails (visionUsersVb Object)
         * 3. b - bearer token session ID (PRD_BEARER_TOKEN - B_SESSION_ID)
         * 4. r - refresh token session ID (PRD_REFRESH_TOKEN - R_SESSION_ID) 
         *  */
        
		String token = Jwts.builder()
			//	.setSubject(lUser.getUserName())
				.setIssuedAt(new Date())
				.setExpiration(expiration)
				.claim("v", lUser.getVisionId())
				.claim("b", lUser.getbSessionId())
				.claim("r", lUser.getrSessionId())
//				.signWith(RANDOM_SECRET_KEY).compact()
				.signWith(SignatureAlgorithm.HS256, SECRET_KEY).compact();
		
		/*if("Y".equalsIgnoreCase(tokenEncryptionFlag)) {
			token = JwtTokenUtil.encryptAndEncodeToken(token, rsaPublicKeyDecoded);
		}*/
		exceptionCode.setResponse(token);
		exceptionCode.setOtherInfo(expiration);
		return exceptionCode;
	}
	
	
	public static ExceptionCode generateRefreshJwtToken(VisionUsersVb lUser) throws Exception {
		
//		String rsaPublicKeyDecoded = URLDecoder.decode(rsaPublicKey, "UTF-8");
		
		ExceptionCode exceptionCode = new ExceptionCode();
		Date now = new Date();
        Date expiration = new Date(now.getTime() + REFRESH_TOKEN_EXPIRATION_TIME);
		
        /* Claim Descriptions
         * 1. v - visionID
         * 2. r - refresh token session ID (PRD_REFRESH_TOKEN - R_SESSION_ID) 
         * 3. ug - userGroup
         * 4. up - userProfile
         *  */
        
        String token = Jwts.builder()
        	//	.setSubject(lUser.getUserName())
        		.setIssuedAt(new Date())
				.setExpiration(expiration)
				.claim("v", lUser.getVisionId())
				.claim("r", lUser.getrSessionId())
				.claim("ug", lUser.getUserGroup())
				.claim("up", lUser.getUserProfile())
				//.claim("ud", lUser)
//				.signWith(RANDOM_SECRET_KEY).compact()
				.signWith(SignatureAlgorithm.HS256, SECRET_KEY).compact();
    	/*if("Y".equalsIgnoreCase(tokenEncryptionFlag)) {
			token = JwtTokenUtil.encryptAndEncodeToken(token, rsaPublicKeyDecoded);
		}*/
        exceptionCode.setResponse(token);
        exceptionCode.setOtherInfo(expiration);
		return exceptionCode;
	}
	
	private static SecretKey generateRandomSecretKey() {
        return Keys.secretKeyFor(SignatureAlgorithm.HS512);
    }
	
	public static boolean validateToken(String token) {
        try {
            Claims claims = Jwts.parser()
                    .setSigningKey(SECRET_KEY)
                    .parseClaimsJws(token)
                    .getBody();

            // Check expiration
            Date expirationDate = claims.getExpiration();
            if (expirationDate.before(new Date())) {
                // Token has expired
                return false;
            }
            // Token is valid
            return true;
        } catch (Exception e) {
            // e.printStackTrace();
            return false;
        }
    }
	
	public static ExceptionCode validateTokenSecondaryChk(VisionUsersVb contextVb, RefreshTokenVb rtokenVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String errorMsg = "Issue in token validation Secondary Chk. Cause - ";
        try {
        	
        	if("N".equalsIgnoreCase(tokenSecondaryValidationFlag)) {
        		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
        	} else if("Y".equalsIgnoreCase(tokenIpValidationFlag) && !contextVb.getIpAddress().equalsIgnoreCase(rtokenVb.getIpAddress())) {
        		exceptionCode.setErrorMsg(errorMsg+"IP not matching");
            } else if("Y".equalsIgnoreCase(tokenMacValidationFlag) && !contextVb.getMacAddress().equalsIgnoreCase(rtokenVb.getMacAddress())) {
            	exceptionCode.setErrorMsg(errorMsg+"MAC Adddress not matching");
            } else if ("Y".equalsIgnoreCase(tokenHostValidationFlag) && !contextVb.getRemoteHostName().equalsIgnoreCase(rtokenVb.getHostname())) {
            	exceptionCode.setErrorMsg(errorMsg+"Hostname not matching");
            } else {
            	exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
            }
        } catch (Exception e) {
            // e.printStackTrace();
            exceptionCode.setErrorMsg(errorMsg+e.getMessage());
        }
        return exceptionCode;
    }
	
	public static ExceptionCode validateBearerTokenSecondaryChk(VisionUsersVb contextVb, BearerTokenVb btokenVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String errorMsg = "Issue in token validation Secondary Chk. Cause - ";
        try {
        	if("N".equalsIgnoreCase(tokenSecondaryValidationFlag)) {
        		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
    		} else if("Y".equalsIgnoreCase(tokenIpValidationFlag) && !contextVb.getIpAddress().equalsIgnoreCase(btokenVb.getIpAddress())) {
        		exceptionCode.setErrorMsg(errorMsg+"IP not matching");
            } else if("Y".equalsIgnoreCase(tokenMacValidationFlag) && !contextVb.getMacAddress().equalsIgnoreCase(btokenVb.getMacAddress())) {
            	exceptionCode.setErrorMsg(errorMsg+"MAC Adddress not matching");
            } else if ("Y".equalsIgnoreCase(tokenHostValidationFlag) && !contextVb.getRemoteHostName().equalsIgnoreCase(btokenVb.getHostname())) {
            	exceptionCode.setErrorMsg(errorMsg+"Hostname not matching");
            } else {
            	exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
            }
        } catch (Exception e) {
            // e.printStackTrace();
            exceptionCode.setErrorMsg(errorMsg+e.getMessage());
        }
        return exceptionCode;
    }
	
	public static Claims parseToken(String token) {
        return Jwts.parser()
                .setSigningKey(SECRET_KEY)
                .parseClaimsJws(token)
                .getBody();
    }
	
	public static String encryptAndEncodeToken(String jwtToken, String rsaPublicKey) throws Exception {
		if("N".equalsIgnoreCase(tokenEncryptionFlag))
			return jwtToken;
		String encryptedToken = RSAEncryptDecryptUtil.encrypt(jwtToken, rsaPublicKey);
		String urlEncodedToken = URLEncoder.encode(encryptedToken, "UTF-8");
		return urlEncodedToken;
	}
	
	public static String decodeAndDecryptToken(String jwtEncryptedURLEncodedStr, String rsaPrivateKey) throws Exception {
		if("N".equalsIgnoreCase(tokenEncryptionFlag))
			return jwtEncryptedURLEncodedStr;
		String urlDecodedToken = URLDecoder.decode(jwtEncryptedURLEncodedStr, "UTF-8");
		String decryptedToken = RSAEncryptDecryptUtil.decrypt(urlDecodedToken, rsaPrivateKey);
		return decryptedToken;
	}
	
	@Value("${vision.token.expiry}")
	public void setTOKEN_EXPIRATION_TIME(String tOKEN_EXPIRATION_TIME) {
		this.TOKEN_EXPIRATION_TIME = Long.parseLong(tOKEN_EXPIRATION_TIME);
	}

	@Value("${vision.refresh.token.expiry}")
	public void setREFRESH_TOKEN_EXPIRATION_TIME(String rEFRESH_TOKEN_EXPIRATION_TIME) {
		this.REFRESH_TOKEN_EXPIRATION_TIME = Long.parseLong(rEFRESH_TOKEN_EXPIRATION_TIME);
	}

	
	@Value("${vision.token.secondary.validation}")
	public void setTokenSecondaryValidationFlag(String tokenSecondaryValidationFlag) {
		JwtTokenUtil.tokenSecondaryValidationFlag = tokenSecondaryValidationFlag;
	}
	
	@Value("${vision.token.ip.validation}")
	public void setTokenIpValidationFlag(String tokenIpValidationFlag) {
		JwtTokenUtil.tokenIpValidationFlag = tokenIpValidationFlag;
	}
	@Value("${vision.token.host.validation}")
	public void setTokenHostValidationFlag(String tokenHostValidationFlag) {
		JwtTokenUtil.tokenHostValidationFlag = tokenHostValidationFlag;
	}
	@Value("${vision.token.mac.validation}")
	public void setTokenMacValidationFlag(String tokenMacValidationFlag) {
		JwtTokenUtil.tokenMacValidationFlag = tokenMacValidationFlag;
	}

	@Value("${vision.jwt.secret.key}")
	public void setSECRET_KEY(String sECRET_KEY) {
		JwtTokenUtil.SECRET_KEY = sECRET_KEY;
	}
	@Value("${token.encryption.flag}")
	public void setTokenEncryptionFlag(String tokenEncryptionFlag) {
		JwtTokenUtil.tokenEncryptionFlag = tokenEncryptionFlag;
	}
}
