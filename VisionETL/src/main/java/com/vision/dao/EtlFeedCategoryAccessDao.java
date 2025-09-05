package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedCategoryAccessVb;
import com.vision.vb.SmartSearchVb;

@Component

public class EtlFeedCategoryAccessDao extends AbstractDao<EtlFeedCategoryAccessVb> {

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String UserGroupAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 1, "TAppr.USER_GROUP",
			"USER_GROUP_DESC");
	String UserGroupAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 1, "TPend.USER_GROUP",
			"USER_GROUP_DESC");

	String UserProfileAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2, "TAppr.USER_PROFILE",
			"USER_PROFILE_DESC");
	String UserProfileAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2, "TPend.USER_PROFILE",
			"USER_PROFILE_DESC");

	String FeedSourceStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.FEED_SOURCE_STATUS",
			"FEED_SOURCE_STATUS_DESC");
	String FeedSourceStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.FEED_SOURCE_STATUS",
			"FEED_SOURCE_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryAccessVb vObject = new EtlFeedCategoryAccessVb();
				if (rs.getString("COUNTRY") != null) {
					vObject.setCountry(rs.getString("COUNTRY"));
				} else {
					vObject.setCountry("");
				}
				if (rs.getString("LE_BOOK") != null) {
					vObject.setLeBook(rs.getString("LE_BOOK"));
				} else {
					vObject.setLeBook("");
				}
				if (rs.getString("CATEGORY_ID") != null) {
					vObject.setCategoryId(rs.getString("CATEGORY_ID"));
				} else {
					vObject.setCategoryId("");
				}
				vObject.setUserGroupAt(rs.getInt("USER_GROUP_AT"));
				if (rs.getString("USER_GROUP") != null) {
					vObject.setUserGroup(rs.getString("USER_GROUP"));
				} else {
					vObject.setUserGroup("");
				}
				vObject.setUserProfileAt(rs.getInt("USER_PROFILE_AT"));
				if (rs.getString("USER_PROFILE") != null) {
					vObject.setUserProfile(rs.getString("USER_PROFILE"));
				} else {
					vObject.setUserProfile("");
				}
				vObject.setVisionId(rs.getInt("VISION_ID"));
				if (rs.getString("WRITE_FLAG") != null) {
					vObject.setWriteFlag(rs.getString("WRITE_FLAG"));
				} else {
					vObject.setWriteFlag("");
				}
				vObject.setFeedSourceStatusNt(rs.getInt("FEED_SOURCE_STATUS_NT"));
				vObject.setFeedSourceStatus(rs.getInt("FEED_SOURCE_STATUS"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				if (rs.getString("DATE_LAST_MODIFIED") != null) {
					vObject.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				} else {
					vObject.setDateLastModified("");
				}
				if (rs.getString("DATE_CREATION") != null) {
					vObject.setDateCreation(rs.getString("DATE_CREATION"));
				} else {
					vObject.setDateCreation("");
				}
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<EtlFeedCategoryAccessVb> getQueryPopupResults(EtlFeedCategoryAccessVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer(
				"Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.CATEGORY_ID,TAppr.USER_GROUP_AT,TAppr.USER_GROUP,TAppr.USER_PROFILE_AT,TAppr.USER_PROFILE,TAppr.VISION_ID,TAppr.WRITE_FLAG,TAppr.FEED_SOURCE_STATUS_NT,TAppr.FEED_SOURCE_STATUS,TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TAppr.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION from ETL_FEED_CATEGORY_ACCESS TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_FEED_CATEGORY_ACCESS_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.CATEGORY_ID = TPend.CATEGORY_ID )");
		StringBuffer strBufPending = new StringBuffer(
				"Select TPend.COUNTRY,TPend.LE_BOOK,TPend.CATEGORY_ID,TPend.USER_GROUP_AT,TPend.USER_GROUP,TPend.USER_PROFILE_AT,TPend.USER_PROFILE,TPend.VISION_ID,TPend.WRITE_FLAG,TPend.FEED_SOURCE_STATUS_NT,TPend.FEED_SOURCE_STATUS,TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TPend.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION from ETL_FEED_CATEGORY_ACCESS_PEND TPend ");
		try {
			if (dObj.getSmartSearchOpt() != null && dObj.getSmartSearchOpt().size() > 0) {
				int count = 1;
				for (SmartSearchVb data : dObj.getSmartSearchOpt()) {
					if (count == dObj.getSmartSearchOpt().size()) {
						data.setJoinType("");
					} else {
						if (!ValidationUtil.isValid(data.getJoinType()) && !("AND".equalsIgnoreCase(data.getJoinType())
								|| "OR".equalsIgnoreCase(data.getJoinType()))) {
							data.setJoinType("AND");
						}
					}
					String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());
					switch (data.getObject()) {
					case "country":
						CommonUtils.addToQuerySearch(" upper(TAppr.COUNTRY) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COUNTRY) " + val, strBufPending, data.getJoinType());
						break;

					case "leBook":
						CommonUtils.addToQuerySearch(" upper(TAppr.LE_BOOK) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LE_BOOK) " + val, strBufPending, data.getJoinType());
						break;

					case "categoryId":
						CommonUtils.addToQuerySearch(" upper(TAppr.CATEGORY_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.CATEGORY_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "userGroup":
						CommonUtils.addToQuerySearch(" upper(TAppr.USER_GROUP) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.USER_GROUP) " + val, strBufPending,
								data.getJoinType());
						break;

					case "userProfile":
						CommonUtils.addToQuerySearch(" upper(TAppr.USER_PROFILE) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.USER_PROFILE) " + val, strBufPending,
								data.getJoinType());
						break;

					case "visionId":
						CommonUtils.addToQuerySearch(" upper(TAppr.VISION_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.VISION_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "writeFlag":
						CommonUtils.addToQuerySearch(" upper(TAppr.WRITE_FLAG) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.WRITE_FLAG) " + val, strBufPending,
								data.getJoinType());
						break;

					case "feedSourceStatus":
						CommonUtils.addToQuerySearch(" upper(TAppr.FEED_SOURCE_STATUS) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.FEED_SOURCE_STATUS) " + val, strBufPending,
								data.getJoinType());
						break;

					case "recordIndicator":
						CommonUtils.addToQuerySearch(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending,
								data.getJoinType());
						break;

					default:
					}
					count++;
				}
			}
			String orderBy = " Order By COUNTRY, LE_BOOK, CATEGORY_ID  ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlFeedCategoryAccessVb> getQueryResults(EtlFeedCategoryAccessVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedCategoryAccessVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String(
				"Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.CATEGORY_ID,TAppr.USER_GROUP_AT,TAppr.USER_GROUP,TAppr.USER_PROFILE_AT, "
						+ " TAppr.USER_PROFILE,TAppr.VISION_ID,TAppr.WRITE_FLAG,TAppr.FEED_SOURCE_STATUS_NT,TAppr.FEED_SOURCE_STATUS, "
						+ " TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
						+ " " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_LAST_MODIFIED,  " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TAppr.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION From ETL_FEED_CATEGORY_ACCESS TAppr "
						+ " WHERE UPPER(TAppr.COUNTRY) = ?  AND UPPER(TAppr.LE_BOOK) = ?  AND UPPER(TAppr.CATEGORY_ID) = ?  ");
		String strQueryPend = new String(
				"Select TPend.COUNTRY,TPend.LE_BOOK,TPend.CATEGORY_ID,TPend.USER_GROUP_AT,TPend.USER_GROUP, "
						+ " TPend.USER_PROFILE_AT,TPend.USER_PROFILE,TPend.VISION_ID,TPend.WRITE_FLAG,TPend.FEED_SOURCE_STATUS_NT, "
						+ " TPend.FEED_SOURCE_STATUS,TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER, "
						+ " TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TPend.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED,  "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION From ETL_FEED_CATEGORY_ACCESS_PEND TPend "
						+ " WHERE UPPER(TPend.COUNTRY) = ?  AND UPPER(TPend.LE_BOOK) = ?  AND UPPER(TPend.CATEGORY_ID) = ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getCategoryId();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapper());
			}
			return collTemp;
		} catch (Exception ex) {
			return null;
		}
	}

	@Override
	protected List<EtlFeedCategoryAccessVb> selectApprovedRecord(EtlFeedCategoryAccessVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedCategoryAccessVb> doSelectPendingRecord(EtlFeedCategoryAccessVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFeedCategoryAccessVb records) {
		return records.getFeedSourceStatus();
	}

	@Override
	protected void setStatus(EtlFeedCategoryAccessVb vObject, int status) {
		vObject.setFeedSourceStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedCategoryAccessVb vObject) {
		String query = "Insert Into ETL_FEED_CATEGORY_ACCESS (COUNTRY, LE_BOOK, CATEGORY_ID, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, VISION_ID, WRITE_FLAG, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getUserGroupAt(),
				vObject.getUserGroup(), vObject.getUserProfileAt(), vObject.getUserProfile(), vObject.getVisionId(),
				vObject.getWriteFlag(), vObject.getFeedSourceStatusNt(), vObject.getFeedSourceStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFeedCategoryAccessVb vObject) {
		String query = "Update ETL_FEED_CATEGORY_ACCESS Set  USER_GROUP_AT = ?, USER_GROUP = ? , USER_PROFILE_AT = ?, USER_PROFILE = ?,  VISION_ID = ?, WRITE_FLAG = ?, FEED_SOURCE_STATUS_NT = ?, FEED_SOURCE_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ?   ";
		Object[] args = { vObject.getUserGroupAt(), vObject.getUserGroup(), vObject.getUserProfileAt(),
				vObject.getUserProfile(), vObject.getVisionId(), vObject.getWriteFlag(),
				vObject.getFeedSourceStatusNt(), vObject.getFeedSourceStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFeedCategoryAccessVb vObject) {
		String query = "Delete From ETL_FEED_CATEGORY_ACCESS Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ?   ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedCategoryAccessVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
		if (ValidationUtil.isValid(vObject.getCountry()))
			strAudit.append("COUNTRY" + auditDelimiterColVal + vObject.getCountry().trim());
		else
			strAudit.append("COUNTRY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLeBook()))
			strAudit.append("LE_BOOK" + auditDelimiterColVal + vObject.getLeBook().trim());
		else
			strAudit.append("LE_BOOK" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getCategoryId()))
			strAudit.append("CATEGORY_ID" + auditDelimiterColVal + vObject.getCategoryId().trim());
		else
			strAudit.append("CATEGORY_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("USER_GROUP_AT" + auditDelimiterColVal + vObject.getUserGroupAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getUserGroup()))
			strAudit.append("USER_GROUP" + auditDelimiterColVal + vObject.getUserGroup().trim());
		else
			strAudit.append("USER_GROUP" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("USER_PROFILE_AT" + auditDelimiterColVal + vObject.getUserProfileAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getUserProfile()))
			strAudit.append("USER_PROFILE" + auditDelimiterColVal + vObject.getUserProfile().trim());
		else
			strAudit.append("USER_PROFILE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("VISION_ID" + auditDelimiterColVal + vObject.getVisionId());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getWriteFlag()))
			strAudit.append("WRITE_FLAG" + auditDelimiterColVal + vObject.getWriteFlag().trim());
		else
			strAudit.append("WRITE_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_SOURCE_STATUS_NT" + auditDelimiterColVal + vObject.getFeedSourceStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_SOURCE_STATUS" + auditDelimiterColVal + vObject.getFeedSourceStatus());
		strAudit.append(auditDelimiter);

		strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + vObject.getRecordIndicatorNt());
		strAudit.append(auditDelimiter);

		strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + vObject.getRecordIndicator());
		strAudit.append(auditDelimiter);

		strAudit.append("MAKER" + auditDelimiterColVal + vObject.getMaker());
		strAudit.append(auditDelimiter);

		strAudit.append("VERIFIER" + auditDelimiterColVal + vObject.getVerifier());
		strAudit.append(auditDelimiter);

		strAudit.append("INTERNAL_STATUS" + auditDelimiterColVal + vObject.getInternalStatus());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateLastModified()))
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + vObject.getDateLastModified().trim());
		else
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateCreation()))
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + vObject.getDateCreation().trim());
		else
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		return strAudit.toString();
	}

	@Override
	protected void setServiceDefaults() {
		serviceName = "EtlFeedCategoryAccess";
		serviceDesc = "ETL Feed Category Access";
		tableName = "ETL_FEED_CATEGORY_ACCESS";
		childTableName = "ETL_FEED_CATEGORY_ACCESS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

}
