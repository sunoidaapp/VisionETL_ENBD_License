package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.BearerTokenVb;
import com.vision.vb.RefreshTokenVb;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VisionUsersVb;

@Component
@SuppressWarnings("deprecation")
public class LoginUserDao extends AbstractCommonDao {

	@Value("${app.productName}")
	public String productName;

	@Value("${app.clientName}")
	private String clientName;

	
	public List<VisionUsersVb> getUserByUserLoginIdOrUserEmailId(String userIdentityAttValue) {

		StringBuffer strQueryAppr = new StringBuffer("Select TAppr.VISION_ID,"
				+ "TAppr.USER_NAME, TAppr.USER_LOGIN_ID, TAppr.USER_EMAIL_ID, "
				+ getDbFunction(Constants.DATEFUNC, null) + " (TAppr.LAST_ACTIVITY_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') LAST_ACTIVITY_DATE, TAppr.USER_GROUP_AT,"
				+ "TAppr.USER_GROUP, TAppr.USER_PROFILE_AT, TAppr.USER_PROFILE, TAppr.UPDATE_RESTRICTION, "
				+ "TAppr.GCID_ACCESS, TAppr.USER_STATUS_NT, TAppr.USER_STATUS,"
				+ getDbFunction(Constants.DATEFUNC, null) + " (TAppr.USER_STATUS_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') USER_STATUS_DATE, TAppr.MAKER, "
				+ "TAppr.VERIFIER, TAppr.INTERNAL_STATUS, TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR, "
				+ getDbFunction(Constants.DATEFUNC, null) + " (TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + " (TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, " + getDbFunction(Constants.DATEFUNC, null)
				+ " (TAppr.LAST_UNSUCCESSFUL_LOGIN_DATE, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null)
				+ "') LAST_UNSUCCESSFUL_LOGIN_DATE, TAppr.UNSUCCESSFUL_LOGIN_ATTEMPTS, TAppr.FILE_NAME, TAppr.USER_PHOTO, "
				+ "TAppr.ENABLE_WIDGETS, TAppr.APPLICATION_ACCESS, "
				+ " PWD_RESET_FLAG, "+getDbFunction(Constants.DATEDIFF, "LAST_PWD_RESET_DATE")+" AS DATE_DIFFERENCE, "
				+ " (SELECT APP_THEME FROM PRD_APP_THEME S1 WHERE S1.VISION_ID=TAPPR.VISION_ID AND APPLICATION_ID = '"+productName+"') APP_THEME,  "
				+ " (SELECT LANGUAGE FROM PRD_APP_THEME S1 WHERE S1.VISION_ID=TAPPR.VISION_ID AND APPLICATION_ID = '"+productName+"') LANGUAGE "
				+ " FROM ETL_USER_VIEW TAPPR WHERE (UPPER(USER_LOGIN_ID) = UPPER(?) OR UPPER(USER_EMAIL_ID) = UPPER(?)) ");
		Object objParams[] = { userIdentityAttValue, userIdentityAttValue };
		try {
			return getJdbcTemplate().query(strQueryAppr.toString(), objParams, visionUserMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<VisionUsersVb> getUserByVisionId(String visionId) {

		StringBuffer strQueryAppr = new StringBuffer("Select TAppr.VISION_ID,"
				+ "TAppr.USER_NAME, TAppr.USER_LOGIN_ID, TAppr.USER_EMAIL_ID, "
				+ getDbFunction(Constants.DATEFUNC, null) + " (TAppr.LAST_ACTIVITY_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') LAST_ACTIVITY_DATE, TAppr.USER_GROUP_AT,"
				+ "TAppr.USER_GROUP, TAppr.USER_PROFILE_AT, TAppr.USER_PROFILE, TAppr.UPDATE_RESTRICTION, "
				+ "TAppr.GCID_ACCESS, TAppr.USER_STATUS_NT, TAppr.USER_STATUS,"
				+ getDbFunction(Constants.DATEFUNC, null) + " (TAppr.USER_STATUS_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') USER_STATUS_DATE, TAppr.MAKER, "
				+ "TAppr.VERIFIER, TAppr.INTERNAL_STATUS, TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR, "
				+ getDbFunction(Constants.DATEFUNC, null) + " (TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + " (TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, " + getDbFunction(Constants.DATEFUNC, null)
				+ " (TAppr.LAST_UNSUCCESSFUL_LOGIN_DATE, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null)
				+ "') LAST_UNSUCCESSFUL_LOGIN_DATE, TAppr.UNSUCCESSFUL_LOGIN_ATTEMPTS, TAppr.FILE_NAME, TAppr.USER_PHOTO, "
				+ "TAppr.ENABLE_WIDGETS, TAppr.APPLICATION_ACCESS, "
				+ " PWD_RESET_FLAG, "+getDbFunction(Constants.DATEDIFF, "LAST_PWD_RESET_DATE")+" AS DATE_DIFFERENCE, "
				+ " (SELECT APP_THEME FROM PRD_APP_THEME S1 WHERE S1.VISION_ID=TAPPR.VISION_ID AND APPLICATION_ID = '"+productName+"') APP_THEME,  "
				+ " (SELECT LANGUAGE FROM PRD_APP_THEME S1 WHERE S1.VISION_ID=TAPPR.VISION_ID AND APPLICATION_ID = '"+productName+"') LANGUAGE "
				+ " FROM ETL_USER_VIEW TAPPR WHERE VISION_ID = ? ");
		Object objParams[] = { visionId };
		try {
			return getJdbcTemplate().query(strQueryAppr.toString(), objParams, visionUserMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper visionUserMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VisionUsersVb visionUsersVb = new VisionUsersVb();
				visionUsersVb.setVisionId(rs.getInt("VISION_ID"));
				if (ValidationUtil.isValid(rs.getString("USER_NAME")))
					visionUsersVb.setUserName(rs.getString("USER_NAME"));
				if (ValidationUtil.isValid(rs.getString("USER_LOGIN_ID")))
					visionUsersVb.setUserLoginId(rs.getString("USER_LOGIN_ID"));
				if (ValidationUtil.isValid(rs.getString("USER_EMAIL_ID")))
					visionUsersVb.setUserEmailId(rs.getString("USER_EMAIL_ID"));
				if (ValidationUtil.isValid(rs.getString("USER_STATUS_DATE")))
					visionUsersVb.setUserStatusDate(rs.getString("USER_STATUS_DATE"));
				visionUsersVb.setUserStatusNt(rs.getInt("USER_STATUS_NT"));
				visionUsersVb.setUserStatus(rs.getInt("USER_STATUS"));
				visionUsersVb.setUserProfileAt(rs.getInt("USER_PROFILE_AT"));
				if (ValidationUtil.isValid(rs.getString("USER_PROFILE")))
					visionUsersVb.setUserProfile(rs.getString("USER_PROFILE").trim());
				visionUsersVb.setUserGroupAt(rs.getInt("USER_GROUP_AT"));
				if (ValidationUtil.isValid(rs.getString("USER_GROUP")))
					visionUsersVb.setUserGroup(rs.getString("USER_GROUP").trim());
				if (ValidationUtil.isValid(rs.getString("LAST_ACTIVITY_DATE")))
					visionUsersVb.setLastActivityDate(rs.getString("LAST_ACTIVITY_DATE").trim());
				if (ValidationUtil.isValid(rs.getString("UPDATE_RESTRICTION")))
					visionUsersVb.setUpdateRestriction(rs.getString("UPDATE_RESTRICTION").trim());
				if (ValidationUtil.isValid(rs.getString("GCID_ACCESS"))) {
					visionUsersVb.setGcidAccess(rs.getString("GCID_ACCESS").trim());
				}
				visionUsersVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				visionUsersVb.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				if (ValidationUtil.isValid(rs.getString("LAST_UNSUCCESSFUL_LOGIN_DATE")))
					visionUsersVb.setLastUnsuccessfulLoginDate(rs.getString("LAST_UNSUCCESSFUL_LOGIN_DATE"));
				if (ValidationUtil.isValid(rs.getString("UNSUCCESSFUL_LOGIN_ATTEMPTS")))
					visionUsersVb.setLastUnsuccessfulLoginAttempts(rs.getString("UNSUCCESSFUL_LOGIN_ATTEMPTS"));
				if (ValidationUtil.isValid(rs.getString("APPLICATION_ACCESS")))
					visionUsersVb.setApplicationAccess(rs.getString("APPLICATION_ACCESS"));
				
				if(ValidationUtil.isValid(rs.getString("PWD_RESET_FLAG")))
					visionUsersVb.setPasswordResetFlag(rs.getString("PWD_RESET_FLAG"));
				else
					visionUsersVb.setPasswordResetFlag("Y");
				
				if(ValidationUtil.isValid(rs.getInt("DATE_DIFFERENCE")))
					visionUsersVb.setLastPwdResetCount(rs.getInt("DATE_DIFFERENCE"));
				
				if (ValidationUtil.isValid(rs.getString("APP_THEME"))) {
					visionUsersVb.setAppTheme(rs.getString("APP_THEME"));
				} else {
					visionUsersVb.setAppTheme("BLUE");
				}
				if (ValidationUtil.isValid(rs.getString("LANGUAGE"))) {
					visionUsersVb.setLanguage(rs.getString("LANGUAGE"));
				} else {
					visionUsersVb.setLanguage("EN");
				}
				return visionUsersVb;
			}
		};
		return mapper;
	}

	public boolean updateActivityDateByUserLoginId(VisionUsersVb visionUsersVb) {
		Object[] params = new Object[1];
		params[0] = visionUsersVb.getVisionId();
		int count = getJdbcTemplate().update(
				"Update ETL_USER_VIEW SET LAST_ACTIVITY_DATE = " + getDbFunction(Constants.SYSDATE, null) + " , "
						+ "LAST_UNSUCCESSFUL_LOGIN_DATE = null, UNSUCCESSFUL_LOGIN_ATTEMPTS = 0  WHERE VISION_ID = ?",
				params);
		return count == 1;
	}

	public List<UserRestrictionVb> getRestrictionTree() throws DataAccessException {
		String sql = "select MACROVAR_NAME,TAG_NAME, DISPLAY_NAME, MACROVAR_DESC from MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION' order by MACROVAR_NAME, TAG_NO";
		return getJdbcTemplate().query(sql, new ResultSetExtractor<List<UserRestrictionVb>>() {
			@Override
			public List<UserRestrictionVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<UserRestrictionVb> returnList = new ArrayList<UserRestrictionVb>();
				while (rs.next()) {
					String macroVar = rs.getString("MACROVAR_NAME");
					List<UserRestrictionVb> filteredList = returnList.stream()
							.filter(vb -> macroVar.equalsIgnoreCase(vb.getMacrovarName())).collect(Collectors.toList());
					if (filteredList != null && filteredList.size() > 0) {
						List<UserRestrictionVb> childrenList = filteredList.get(0).getChildren();
						childrenList.add(new UserRestrictionVb(macroVar, rs.getString("TAG_NAME"),
								rs.getString("DISPLAY_NAME"), rs.getString("MACROVAR_DESC")));
					} else {
						List<UserRestrictionVb> childrenList = new ArrayList<UserRestrictionVb>();
						childrenList.add(new UserRestrictionVb(macroVar, rs.getString("TAG_NAME"),
								rs.getString("DISPLAY_NAME"), rs.getString("MACROVAR_DESC")));
						UserRestrictionVb userRestrictionVb = new UserRestrictionVb();
						userRestrictionVb.setMacrovarName(macroVar);
						userRestrictionVb.setMacrovarDesc(rs.getString("MACROVAR_DESC"));
						userRestrictionVb.setChildren(childrenList);
						returnList.add(userRestrictionVb);
					}
				}
				return returnList;
			}
		});
	}

	public String getVisionDynamicHashVariable(String variableName) {
		String sql = "select VARIABLE_SCRIPT from vision_dynamic_hash_var where VARIABLE_NAME = ? ";
		Object[] args = { "VU_RESTRICTION_" + variableName };
		return getJdbcTemplate().queryForObject(sql, args, String.class);
	}

	public List<UserRestrictionVb> doUpdateRestrictionToUserObject(VisionUsersVb vObject,
			List<UserRestrictionVb> restrictionList) {
		Object args[] = { vObject.getVisionId() };
		Iterator<UserRestrictionVb> restrictionItr = restrictionList.iterator();
		while (restrictionItr.hasNext()) {
			UserRestrictionVb restrictionVb = restrictionItr.next();
			String restrictedValue = userObjectUpdate(vObject, restrictionVb.getMacrovarName(),
					getJdbcTemplate().query(restrictionVb.getRestrictionSql(), args, getMapperRestriction()));
			restrictionVb.setRestrictedValue(restrictedValue);
		}
		return restrictionList;
	}

	private RowMapper<AlphaSubTabVb> getMapperRestriction() {
		RowMapper<AlphaSubTabVb> mapper = new RowMapper<AlphaSubTabVb>() {
			public AlphaSubTabVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				AlphaSubTabVb alphaSubTabVb = new AlphaSubTabVb();
				alphaSubTabVb.setAlphaSubTab(rs.getString("RESTRICTION"));
				return alphaSubTabVb;
			}
		};
		return mapper;
	}

	private String userObjectUpdate(VisionUsersVb vObject, String category, List<AlphaSubTabVb> valueLst) {
		StringBuffer restrictValue = new StringBuffer();
		if (valueLst != null && valueLst.size() > 0) {
			restrictValue = formInConditionWithResultListForRestriction(valueLst);
			switch (category) {
			case "COUNTRY":
				restrictValue = new StringBuffer();
				Set countrySet = new HashSet();
				for (AlphaSubTabVb vObj : valueLst) {
					countrySet.add((vObj.getAlphaSubTab().split("-"))[0]);
				}
				int idx = 0;
				for (Object country : countrySet) {
					restrictValue.append("'" + country + "'");
					if (idx != countrySet.size() - 1)
						restrictValue.append(",");
					idx++;
				}
				break;
			case "COUNTRY-LE_BOOK":
				vObject.setCountry((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "COUNTRY-LE_BOOK-ACCOUNT_OFFICER":
				vObject.setAccountOfficer((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "COUNTRY-LE_BOOK-STAFF":
				vObject.setStaffId((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "LEGAL_VEHICLE":
				vObject.setLegalVehicle((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "LEGAL_VEHICLE-COUNTRY-LE_BOOK":
				vObject.setLegalVehicleCleb((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "OUC":
				vObject.setOucAttribute((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "PRODUCT":
				vObject.setProductAttribute((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			case "SBU":
				vObject.setSbuCode((restrictValue != null) ? String.valueOf(restrictValue) : "");
				break;
			default:
				break;
			}
		} else {
			restrictValue = null;
		}
		return (restrictValue != null) ? String.valueOf(restrictValue) : null;
	}

	private StringBuffer formInConditionWithResultListForRestriction(List<AlphaSubTabVb> valueLst) {
		StringBuffer restrictValue = new StringBuffer();
		if (valueLst != null && valueLst.size() > 0) {
			int idx = 1;
			for (AlphaSubTabVb vObj : valueLst) {
				restrictValue.append("'" + vObj.getAlphaSubTab() + "'");
				if (idx < valueLst.size())
					restrictValue.append(",");
				idx++;
			}
		} else {
			restrictValue = null;
		}
		return restrictValue;
	}

	public int insertUserLoginAudit(VisionUsersVb vObject) {
		String query = "";
		String node = System.getenv("RA_NODE_NAME");
		if (!ValidationUtil.isValid(node)) {
			node = "A1";
		}
		try {
			query = "Insert Into PRD_USERS_LOGIN_AUDIT (APPLICATION_ACCESS_AT,APPLICATION_ACCESS,USER_LOGIN_ID,VISION_ID,IP_ADDRESS,HOST_NAME,ACCESS_DATE,"
					+ "LOGIN_STATUS_AT,LOGIN_STATUS,COMMENTS,MAC_ADDRESS, NODE_NAME) " + " Values (?,?,?, ?, ?, ?, "
					+ getDbFunction(Constants.SYSDATE, null) + ", 1206, ?, ?, ?, ?)";
			Object[] args = { 8000, productName, vObject.getUserLoginId(), vObject.getVisionId(),
					vObject.getIpAddress(), vObject.getRemoteHostName(), vObject.getLoginStatus(),
					vObject.getComments(), vObject.getMacAddress(), node };
			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return 0;
		}
	}

	public int insertPrdRefreshToken(VisionUsersVb vObject, String refreshToken, String strTokenCreatedDate, String strTokenExpirationDate,
		String a_PublicToken, String j_PrivateToken) {
		String query = " insert into PRD_REFRESH_TOKEN (R_SESSION_ID, VISION_ID, IP_ADDRESS, HOSTNAME, MAC_ADDRESS, TOKEN_STATUS_NT, "
				+ " TOKEN_STATUS, TOKEN_CREATED_DATE, VALID_TILL, COMMENTS, A_PUBLIC_TOKEN, J_PRIVATE_TOKEN, REFRESH_TOKEN) values (?, ?, ?, ?, ?, 8001, 0, "
				+ getDbFunction(Constants.TO_DATE_MM, strTokenCreatedDate) + ", "
				+ getDbFunction(Constants.TO_DATE_MM, strTokenExpirationDate) + ", ?, ?, ?, ?)";
		Object[] args = { vObject.getrSessionId(), vObject.getVisionId(), vObject.getIpAddress(),
				vObject.getRemoteHostName(), vObject.getMacAddress(), vObject.getComments(), a_PublicToken,
				j_PrivateToken };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(refreshToken) ? refreshToken : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	public int writePrdBearerToken(VisionUsersVb vObject, String bearerToken,  String strTokenCreatedDate, String expiration) {
		String query = " insert into PRD_BEARER_TOKEN ( R_SESSION_ID, B_SESSION_ID, VISION_ID, IP_ADDRESS, HOSTNAME, MAC_ADDRESS, "
				+ " TOKEN_STATUS_NT, TOKEN_STATUS, TOKEN_CREATED_DATE, VALID_TILL, APPLICATION_ACCESS_AT, APPLICATION_ACCESS, "
				+ " COMMENTS, UTILIZATION_COUNT, BEARER_TOKEN) values ( ?, ?, ?, ?, ?, ?, 8001, 0, "
				+ getDbFunction(Constants.TO_DATE_MM, strTokenCreatedDate) + ", " + getDbFunction(Constants.TO_DATE_MM, expiration)
				+ ", 8000, ?, ?, 0, ?) ";
		Object[] args = { vObject.getrSessionId(), vObject.getbSessionId(), vObject.getVisionId(),
				vObject.getIpAddress(), vObject.getRemoteHostName(), vObject.getMacAddress(), productName,
				vObject.getComments() };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(bearerToken) ? bearerToken : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	public int getBearerTokenCount(VisionUsersVb vObject) {
		String sql = "SELECT  count(1) FROM PRD_BEARER_TOKEN  WHERE VISION_ID = ? AND R_SESSION_ID = ? "
				+ "AND B_SESSION_ID = ?  AND APPLICATION_ACCESS = ? ";
		Object[] args = { vObject.getVisionId(), vObject.getrSessionId(), vObject.getbSessionId(), productName };
		return getJdbcTemplate().queryForObject(sql, args, Integer.class);
	}

	public BearerTokenVb getBearerTokenWithPrdID_rSessID_visionID(String rSessionId, String visionID) {
		String sql = "SELECT R_SESSION_ID, B_SESSION_ID, VISION_ID, BEARER_TOKEN, TOKEN_STATUS, IP_ADDRESS, HOSTNAME, MAC_ADDRESS, VALID_TILL, UTILIZATION_COUNT"
				+ " FROM PRD_BEARER_TOKEN  WHERE R_SESSION_ID = ? AND VISION_ID = ?  AND APPLICATION_ACCESS = ? ";
		Object[] args = { rSessionId, visionID, productName };
		return getJdbcTemplate().query(sql, args, new ResultSetExtractor<BearerTokenVb>() {
			@Override
			public BearerTokenVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				BearerTokenVb vb = null;
				if (rs.next()) {
					vb = new BearerTokenVb();
					vb.setrSessionId(rs.getString("R_SESSION_ID"));
					vb.setbSessionId(rs.getString("B_SESSION_ID"));
					vb.setVisionId(rs.getString("VISION_ID"));
					vb.setBearerToken(rs.getString("BEARER_TOKEN"));
					vb.setTokenStatus(rs.getInt("TOKEN_STATUS"));
					vb.setIpAddress(rs.getString("IP_ADDRESS"));
					vb.setHostname(rs.getString("HOSTNAME"));
					vb.setMacAddress(rs.getString("MAC_ADDRESS"));
					vb.setValidTill(rs.getString("VALID_TILL"));
					vb.setUtilizationCount(rs.getInt("UTILIZATION_COUNT"));
				}
				return vb;
			}
		});
	}

	public int updatePrdBearerToken(VisionUsersVb vObject, String bearerToken, String strTokenExpirationDate) {
		String query = "UPDATE PRD_BEARER_TOKEN SET BEARER_TOKEN = ?, VALID_TILL = "
				+ getDbFunction(Constants.TO_DATE_MM, strTokenExpirationDate) + ", UTILIZATION_COUNT = 0 "
				+ " WHERE VISION_ID = ? AND R_SESSION_ID = ? AND B_SESSION_ID = ? ";
		Object[] args = { vObject.getVisionId(), vObject.getrSessionId(), vObject.getbSessionId() };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(bearerToken) ? bearerToken : "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	public int selectAndInsertPrdBearerTokenIntoAudit(VisionUsersVb vObject) {
		String sql = " SELECT R_SESSION_ID, B_SESSION_ID, VISION_ID, BEARER_TOKEN, IP_ADDRESS, HOSTNAME, MAC_ADDRESS, TOKEN_STATUS_NT, "
				+ " TOKEN_STATUS, TOKEN_CREATED_DATE, VALID_TILL, DATE_LAST_UTILIZED, APPLICATION_ACCESS_AT, APPLICATION_ACCESS, COMMENTS, UTILIZATION_COUNT "
				+ " FROM PRD_BEARER_TOKEN  WHERE VISION_ID = ? AND R_SESSION_ID = ? AND B_SESSION_ID = ?  AND APPLICATION_ACCESS = ? ";
		String instemplateSelPrep = " INSERT INTO PRD_BEARER_TOKEN_AUDIT ( R_SESSION_ID, B_SESSION_ID, VISION_ID, BEARER_TOKEN,"
				+ " IP_ADDRESS, HOSTNAME, MAC_ADDRESS, TOKEN_STATUS_NT, TOKEN_STATUS, TOKEN_CREATED_DATE, "
				+ " VALID_TILL, DATE_LAST_UTILIZED, APPLICATION_ACCESS_AT, APPLICATION_ACCESS, COMMENTS, UTILIZATION_COUNT )  "
				+ sql;
		Object[] args = { vObject.getVisionId(), vObject.getrSessionId(), vObject.getbSessionId(), productName };
		return getJdbcTemplate().update(instemplateSelPrep, args);
	}

	public int selectAndInsertPrdRefreshTokenIntoAudit(VisionUsersVb vObject) {
		String sql = " SELECT R_SESSION_ID, VISION_ID, REFRESH_TOKEN, IP_ADDRESS, HOSTNAME, MAC_ADDRESS, TOKEN_STATUS_NT,  "
				+ " TOKEN_STATUS, TOKEN_CREATED_DATE, VALID_TILL, DATE_LAST_UTILIZED, COMMENTS, A_PUBLIC_TOKEN, J_PRIVATE_TOKEN, UTILIZATION_COUNT "
				+ " FROM PRD_REFRESH_TOKEN  WHERE VISION_ID = ? AND R_SESSION_ID = ? ";
		String instemplateSelPrep = " INSERT INTO PRD_REFRESH_TOKEN_AUDIT ( R_SESSION_ID, VISION_ID, REFRESH_TOKEN, IP_ADDRESS,"
				+ " HOSTNAME, MAC_ADDRESS, TOKEN_STATUS_NT, "
				+ " TOKEN_STATUS, TOKEN_CREATED_DATE, VALID_TILL, DATE_LAST_UTILIZED, COMMENTS, A_PUBLIC_TOKEN, J_PRIVATE_TOKEN, UTILIZATION_COUNT)  "
				+ sql;
		Object[] args = { vObject.getVisionId(), vObject.getrSessionId() };
		return getJdbcTemplate().update(instemplateSelPrep, args);
	}

	public RefreshTokenVb getRefreshTokenInfoWith_rSessionID(String rSessionId) {
		String sql = "SELECT R_SESSION_ID, VISION_ID, REFRESH_TOKEN, J_PRIVATE_TOKEN, A_PUBLIC_TOKEN, TOKEN_STATUS,"
				+ " IP_ADDRESS, HOSTNAME, MAC_ADDRESS, VALID_TILL, UTILIZATION_COUNT"
				+ " FROM PRD_REFRESH_TOKEN  WHERE R_SESSION_ID = ? ";
		Object[] args = { rSessionId };
		return getJdbcTemplate().query(sql, args, new ResultSetExtractor<RefreshTokenVb>() {
			@Override
			public RefreshTokenVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				RefreshTokenVb vb = null;
				if (rs.next()) {
					vb = new RefreshTokenVb();
					vb.setrSessionId(rs.getString("R_SESSION_ID"));
					vb.setVisionId(rs.getString("VISION_ID"));
					vb.setRefreshToken(rs.getString("REFRESH_TOKEN"));
					vb.setjPrivateKey(rs.getString("J_PRIVATE_TOKEN"));
					vb.setTokenStatus(rs.getInt("TOKEN_STATUS"));
					vb.setIpAddress(rs.getString("IP_ADDRESS"));
					vb.setHostname(rs.getString("HOSTNAME"));
					vb.setMacAddress(rs.getString("MAC_ADDRESS"));
					vb.setValidTill(rs.getString("VALID_TILL"));
					vb.setaPublicKey(rs.getString("A_PUBLIC_TOKEN"));
					vb.setUtilizationCount(rs.getInt("UTILIZATION_COUNT"));
				}
				return vb;
			}
		});
	}

	public int updatePrdRefreshToken(VisionUsersVb vObject, String bearerToken, String strTokenExpirationDate) {
		String query = "UPDATE PRD_REFRESH_TOKEN SET REFRESH_TOKEN = ?, VALID_TILL = "
				+ getDbFunction(Constants.TO_DATE_MM, strTokenExpirationDate) + ", UTILIZATION_COUNT = 0 "
				+ " WHERE VISION_ID = ? AND R_SESSION_ID = ? ";
		Object[] args = { vObject.getVisionId(), vObject.getrSessionId() };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(bearerToken) ? bearerToken : "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	public int writePRD_Suspecious_Token_Audit(String rSessionId, String bSessionId, String refreshToken,
		String bearerToken, String tokenType, 
		String comments, VisionUsersVb vb) {
		String sql = "insert into PRD_Suspecious_Token_Audit ( R_SESSION_ID, B_SESSION_ID, REFRESH_TOKEN, BEARER_TOKEN, IP_ADDRESS, "
				+ " HOSTNAME, MAC_ADDRESS, TOKEN_TYPE, COMMENTS, DATE_LAST_MODIFIED, DATE_CREATION ) "
				+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + " )";
		Object[] args = { rSessionId, bSessionId, refreshToken, bearerToken, vb.getIpAddress(), vb.getRemoteHostName(),
				vb.getMacAddress(), tokenType, comments };
		return getJdbcTemplate().update(sql, args);
	}

	public int invalidateRefreshToken(String rSessionId, int tokenStatus, String visionId) {
		String sql = "update PRD_REFRESH_TOKEN set token_status = ? where Vision_id = ? and R_SESSION_ID = ? ";
		Object[] args = { tokenStatus, visionId, rSessionId };
		return getJdbcTemplate().update(sql, args);
	}

	public int insertBearerTokenCount(String bSessionId, String tokenCount) {
		String sql = "Insert into PRD_BEARER_TOKEN_COUNT (B_SESSION_ID, TOKEN_COUNT) values (?, ?)";
		Object[] args = { bSessionId, tokenCount };
		return getJdbcTemplate().update(sql, args);
	}

	public int getBearerTokenCount(String bSessionId, String tokenCount) {
		String sql = "SELECT  count(1) FROM PRD_BEARER_TOKEN_COUNT  WHERE  B_SESSION_ID = ? AND TOKEN_COUNT = ? ";
		Object[] args = { bSessionId, tokenCount };
		return getJdbcTemplate().queryForObject(sql, args, Integer.class);
	}

	public int invalidateBearerToken(String bSessionId, int tokenStatus, String visionId) {
		String sql = "update PRD_BEARER_TOKEN set token_status = ? where Vision_id = ? and B_SESSION_ID = ? ";
		Object[] args = { tokenStatus, visionId, bSessionId };
		return getJdbcTemplate().update(sql, args);
	}

	public int deleteRefreshTokenCount(String rSessionId) {
		String sql = "DELETE FROM PRD_REFRESH_TOKEN_COUNT WHERE R_SESSION_ID = ? ";
		Object[] args = { rSessionId };
		return getJdbcTemplate().update(sql, args);
	}

	public int insertRefreshTokenCount(String rSessionId, String tokenCount) {
		String sql = "Insert into PRD_REFRESH_TOKEN_COUNT (R_SESSION_ID, TOKEN_COUNT) values (?, ?)";
		Object[] args = { rSessionId, tokenCount };
		return getJdbcTemplate().update(sql, args);
	}

	public int getRefreshTokenCount(String rSessionId, String tokenCount) {
		String sql = "SELECT  count(1) FROM PRD_REFRESH_TOKEN_COUNT  WHERE  R_SESSION_ID = ? AND TOKEN_COUNT = ? ";
		Object[] args = { rSessionId, tokenCount };
		return getJdbcTemplate().queryForObject(sql, args, Integer.class);
	}

	public int increaseUtilizationCount_RefreshToken(String rSessionID) {
		String sql = "update PRD_REFRESH_TOKEN set UTILIZATION_COUNT = (UTILIZATION_COUNT+1) WHERE R_SESSION_ID = ?";
		Object[] args = { rSessionID };
		return getJdbcTemplate().update(sql, args);
	}

	public int increaseUtilizationCount_BearerToken(String rSessionID, String bSessionID) {
		String sql = "update PRD_BEARER_TOKEN set UTILIZATION_COUNT = (UTILIZATION_COUNT+1) WHERE R_SESSION_ID = ? AND B_SESSION_ID = ?  AND APPLICATION_ACCESS = ?";
		Object[] args = { rSessionID, bSessionID, productName };
		return getJdbcTemplate().update(sql, args);
	}

	public int updateAngularRSAPublicToken(String rSessionId, String angularRSAPublicToken) {
		String query = "UPDATE PRD_REFRESH_TOKEN SET A_PUBLIC_TOKEN = ? WHERE R_SESSION_ID = ? ";
		Object[] args = { angularRSAPublicToken, rSessionId };
		return getJdbcTemplate().update(query, args);
	}

	public int invalidateBearerTokenByR_SessionID(int tokenStatus, String visionId, String rSessionID) {
		String sql = "update PRD_BEARER_TOKEN set token_status = ? where Vision_id = ? and R_SESSION_ID = ? ";
		Object[] args = { tokenStatus, visionId, rSessionID };
		return getJdbcTemplate().update(sql, args);
	}

	public int invalidateRefreshTokenByVisionID(int tokenStatus, String visionId) {
		String sql = "update PRD_REFRESH_TOKEN set token_status = ? where Vision_id = ?";
		Object[] args = { tokenStatus, visionId };
		return getJdbcTemplate().update(sql, args);
	}

	public int invalidateBearerTokenByVisionID(int tokenStatus, String visionId) {
		String sql = "update PRD_BEARER_TOKEN set token_status = ? where Vision_id = ?";
		Object[] args = { tokenStatus, visionId };
		return getJdbcTemplate().update(sql, args);
	}

	public String getExistingVisionPwd(String visionId) {
		try {
			String sql = " SELECT PASSWORD FROM vision_users WHERE VISION_ID = ? ";
			Object[] args = { visionId };
			return getJdbcTemplate().queryForObject(sql, args, String.class);
		} catch (Exception e) {
			return "";
		}
	}

	public int updateUserPassword(String newPwd, String visionId) {
		String sql = "Update Vision_users set password = ? , PWD_RESET_FLAG = 'N', LAST_PWD_RESET_DATE = "
				+ getDbFunction(Constants.SYSDATE, null) + "  where VISION_ID = ?";
		Object[] args = {newPwd, visionId};
		return getJdbcTemplate().update(sql, args);
	}
	
	public int updateUnsuccessfulLoginAttempts(String userId) {
		String sql = "Update VISION_USERS SET LAST_UNSUCCESSFUL_LOGIN_DATE = "+getDbFunction(Constants.SYSDATE, null )+","
					+ "UNSUCCESSFUL_LOGIN_ATTEMPTS = "+getDbFunction(Constants.NVL, null)+"(UNSUCCESSFUL_LOGIN_ATTEMPTS,0)+ 1 WHERE (UPPER(USER_LOGIN_ID) = UPPER(?) OR UPPER(USER_EMAIL_ID) = UPPER(?)) ";
		Object[] params = {userId.toUpperCase(), userId.toUpperCase()};
		return getJdbcTemplate().update(sql, params);

	}

	public String getLoginUserXml() {
		setServiceDefaults();
		String xml = "";
		try {
			String sql = " Select USER_RESTRICTION_XML from VISION_USER_RESTRICTION where Vision_ID = ? ";
			Object args[] = { intCurrentUserId };
			xml = getJdbcTemplate().queryForObject(sql, args, String.class);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return xml;
	}
	protected void setServiceDefaults(){
		serviceName = "VisionUsers";
		serviceDesc = "Vision Users";
		tableName = "VISION_USERS";
		childTableName = "VISION_USERS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}
	
	
	public VisionUsersVb getLicenseDetails(VisionUsersVb lUser) {
		String sql = "SELECT LICENSE_STR, LICENSE_KEY, LICENSE_HASH_VALUE FROM PRD_LICENSE_DETAILS WHERE CLIENT_ID = ? AND STATUS = 0";
		Object[] params = { clientName };
		try {
			return getJdbcTemplate().query(sql, new ResultSetExtractor<VisionUsersVb>() {
				@Override
				public VisionUsersVb extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						lUser.setLicenseStr(rs.getString("LICENSE_STR"));
						lUser.setLicenseKey(rs.getString("LICENSE_KEY"));
						lUser.setLicenseHashValue(rs.getString("LICENSE_HASH_VALUE"));
					}
					return lUser;
				}
			}, params);
		} catch (Exception e) {
//			if ("Y".equalsIgnoreCase(enableDebug)) {
//				e.printStackTrace();
//			}
			return lUser;
		}
	}
}
