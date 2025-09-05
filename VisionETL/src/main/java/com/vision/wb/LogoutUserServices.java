package com.vision.wb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.vision.dao.LoginUserDao;
import com.vision.vb.VisionUsersVb;

@Service
public class LogoutUserServices {

	public static Logger logger = LoggerFactory.getLogger(LogoutUserServices.class);

	@Autowired
	LoginUserDao loginUserDao;


	public void invalidateAllAccessTokens(VisionUsersVb userVb) throws Exception {
		loginUserDao.invalidateRefreshToken(userVb.getrSessionId(), 2, String.valueOf(userVb.getVisionId()));
		loginUserDao.invalidateBearerTokenByR_SessionID(2, String.valueOf(userVb.getVisionId()), userVb.getrSessionId());
	}
	
	public void invalidateAllAccessTokensByVisionID(VisionUsersVb userVb) throws Exception {
		loginUserDao.invalidateRefreshTokenByVisionID(2, String.valueOf(userVb.getVisionId()));
		loginUserDao.invalidateRefreshTokenByVisionID(2, String.valueOf(userVb.getVisionId()));
	}
	
}
