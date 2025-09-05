package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.LevelOfDisplayUserVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.ProfileData;

@Component
public class LevelOfDisplayDao<E extends CommonVb> extends AbstractCommonDao {

	public List<LevelOfDisplayVb> getQueryUserGroupProfile() throws DataAccessException {
		String sql = "SELECT USER_GROUP, USER_PROFILE FROM PRD_PROFILE_PRIVILEGES"
				+ " where PROFILE_STATUS = 0 AND APPLICATION_ACCESS = 'ETL' GROUP BY USER_GROUP, USER_PROFILE"
				+ " ORDER BY USER_GROUP, USER_PROFILE";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				LevelOfDisplayVb lodVb = new LevelOfDisplayVb();
				lodVb.setUserGroup(rs.getString("USER_GROUP"));
				lodVb.setUserProfile(rs.getString("USER_PROFILE"));
				return lodVb;
			}
		};
		List<LevelOfDisplayVb> lodVbList = getJdbcTemplate().query(sql, mapper);
		return lodVbList;
	}

	public List<ProfileData> getQueryUserGroup() throws DataAccessException {
		String sql = "SELECT DISTINCT USER_GROUP FROM PRD_PROFILE_PRIVILEGES  where PROFILE_STATUS = 0 "
				+ " AND APPLICATION_ACCESS = 'ETL' ORDER BY USER_GROUP";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setUserGroup(rs.getString("USER_GROUP"));
				return profileData;
			}
		};
		List<ProfileData> profileData = getJdbcTemplate().query(sql, mapper);
		return profileData;
	}

	public List<ProfileData> getQueryUserGroupBasedProfile(String userGroup) throws DataAccessException {
		String sql = "SELECT DISTINCT USER_PROFILE,USER_GROUP FROM PRD_PROFILE_PRIVILEGES where USER_GROUP =? "
				+ "  AND PROFILE_STATUS = 0 AND APPLICATION_ACCESS = 'ETL'  ORDER BY USER_GROUP";
		Object[] args= {userGroup};
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setUserGroup(rs.getString("USER_GROUP"));
				profileData.setUserProfile(rs.getString("USER_PROFILE"));
				return profileData;
			}
		};
		List<ProfileData> profileData = getJdbcTemplate().query(sql, mapper, args);
		return profileData;
	}

	public List getUserListExcludingUserGrpProf(String usrGroupAndProfile) {
		StringBuffer sql = new StringBuffer(
				"SELECT VISION_ID, USER_NAME, USER_LOGIN_ID, USER_GROUP, USER_PROFILE FROM ETL_USER_VIEW  WHERE USER_STATUS = 0");
		Object objParams[] = new Object[0];
		if (ValidationUtil.isValid(usrGroupAndProfile)) {
			objParams = new Object[1];
			objParams[0] = usrGroupAndProfile;
			sql.append(" AND USER_GROUP " + getDbFunction(Constants.PIPELINE, null) + " '-' "
					+ getDbFunction(Constants.PIPELINE, null) + " USER_PROFILE = ? ");
		}
		sql.append(" ORDER BY VISION_ID, USER_NAME ");
		try {
			return getJdbcTemplate().query(String.valueOf(sql), new RowMapper<LevelOfDisplayUserVb>() {
				@Override
				public LevelOfDisplayUserVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					return new LevelOfDisplayUserVb(rs.getInt("VISION_ID"), rs.getString("USER_NAME"),
							rs.getString("USER_LOGIN_ID"), rs.getString("USER_GROUP"), rs.getString("USER_PROFILE"));
				}

			}, objParams);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
}
