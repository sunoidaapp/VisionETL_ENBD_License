package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.ProfileData;
import com.vision.vb.RaProfileVb;
import com.vision.vb.SmartSearchVb;

@Component
public class RaProfilePrivilegesDao extends AbstractDao<RaProfileVb> {
	@Value("${app.productName}")
	private String productName;
	@Value("${app.databaseType}")
	private String databaseType;
	@Autowired
	CommonDao commonDao;

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				RaProfileVb profilePrivilegesVb = new RaProfileVb();
				profilePrivilegesVb.setUserGroup(rs.getString("USER_GROUP"));
				profilePrivilegesVb.setUserGroupDesc(rs.getString("USER_GROUP_Desc"));
				profilePrivilegesVb.setUserProfile(rs.getString("USER_PROFILE"));
				profilePrivilegesVb.setUserProfileDesc(rs.getString("USER_PROFILE_Desc"));
				profilePrivilegesVb.setMenuGroup(rs.getString("MENU_GROUP"));
				profilePrivilegesVb.setMenuGroupDesc(rs.getString("MENU_GROUP_Desc"));
				profilePrivilegesVb.setScreenName(rs.getString("SCREEN_NAME"));
				profilePrivilegesVb.setScreenNameDesc(rs.getString("MENU_NAME"));
				profilePrivilegesVb.setProfileAdd(rs.getString("P_ADD"));
				profilePrivilegesVb.setProfileModify(rs.getString("P_MODIFY"));
				profilePrivilegesVb.setProfileDelete(rs.getString("P_DELETE"));
				profilePrivilegesVb.setProfileInquiry(rs.getString("P_INQUIRY"));
				profilePrivilegesVb.setProfileVerification(rs.getString("P_VERIFICATION"));
				profilePrivilegesVb.setProfileUpload(rs.getString("P_EXCEL_UPLOAD"));
				profilePrivilegesVb.setProfileDownload(rs.getString("P_DOWNLOAD"));
				profilePrivilegesVb.setDateCreation(rs.getString("DATE_CREATION"));
				profilePrivilegesVb.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				profilePrivilegesVb.setProfileStatus(rs.getInt("PROFILE_STATUS"));
				profilePrivilegesVb.setProfileStatusDesc(rs.getString("PROFILE_STATUS_DESC"));
				profilePrivilegesVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				profilePrivilegesVb.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				profilePrivilegesVb.setMaker(rs.getInt("MAKER"));
				profilePrivilegesVb.setMakerName(rs.getString("MAKER_NAME"));
				profilePrivilegesVb.setVerifier(rs.getInt("VERIFIER"));
				profilePrivilegesVb.setVerifierName(rs.getString("VERIFIER_NAME"));
				//profilePrivilegesVb.setWriteFlag("Y");
				return profilePrivilegesVb;
			}
		};
		return mapper;
	}

	public List<RaProfileVb> getQueryPopupResults(RaProfileVb dObj) {
		StringBuffer strBufApprove = null;
		StringBuffer strBufPending = null;
		String strWhereNotExists = null;
		Vector<Object> params = new Vector<Object>();
		String userGrp = "";
		String userProfile = "";
		if (ValidationUtil.isValid(dObj.getUserGroup()) && !"ALL".equalsIgnoreCase(dObj.getUserGroup()))
			userGrp = " AND USER_GROUP = ? ";
		if (ValidationUtil.isValid(dObj.getUserProfile()) && !"ALL".equalsIgnoreCase(dObj.getUserProfile()))
			userProfile = " AND USER_PROFILE = ? ";
		strBufApprove = new StringBuffer(" SELECT * FROM"
				+ " (SELECT  USER_GROUP,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM  ALPHA_SUB_TAB  WHERE ALPHA_TAB = T1.USER_GROUP_AT  AND ALPHA_SUB_TAB = T1.USER_GROUP) USER_GROUP_DESC,"
				+ "  USER_PROFILE,"
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB  WHERE ALPHA_TAB = T1.USER_PROFILE_AT AND ALPHA_SUB_TAB = T1.USER_PROFILE) USER_PROFILE_DESC,"
				+ "  T1.SCREEN_NAME AS SCREEN_NAME,"
				+ "  T2.MENU_NAME, T2.MENU_GROUP,T2.MENU_SEQUENCE,T2.PARENT_SEQUENCE,"
				+ "(SELECT T3.MENU_GROUP_NAME FROM PRD_MENU_GROUP T3 WHERE T3.APPLICATION_ACCESS= ? AND T2.MENU_GROUP=T3.MENU_GROUP_SEQ)MENU_GROUP_DESC,"
				+ "  P_ADD,  P_MODIFY,  P_DELETE,  P_INQUIRY,  P_VERIFICATION,"
				+ "  P_EXCEL_UPLOAD,  P_DOWNLOAD,  T1.PROFILE_STATUS,"
				+ " (SELECT  NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB  WHERE NUM_TAB = T1.PROFILE_STATUS_NT AND NUM_SUB_TAB = T1.PROFILE_STATUS) PROFILE_STATUS_DESC,"
				+ "  T1.RECORD_INDICATOR,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM  NUM_SUB_TAB  WHERE  NUM_TAB = T1.RECORD_INDICATOR_NT  AND NUM_SUB_TAB = T1.RECORD_INDICATOR) RECORD_INDICATOR_DESC,"
				+ " T1.MAKER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
				+ getDbFunction(Constants.NVL, null) + "(T1.MAKER,0) ) MAKER_NAME, "
				+ " T1.VERIFIER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
				+ getDbFunction(Constants.NVL, null) + "(T1.VERIFIER,0) ) VERIFIER_NAME,  "
				+ getDbFunction(Constants.DATEFUNC, null) + "(T1.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED ," + getDbFunction(Constants.DATEFUNC, null) + "(T1.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION,  T1.DATE_LAST_MODIFIED DATE_LAST_MODIFIED_1 FROM"
				+ " PRD_PROFILE_PRIVILEGES T1, PRD_VISION_MENU T2 WHERE"
				+ " UPPER (T1.SCREEN_NAME) = UPPER (T2.MENU_PROGRAM) AND T1.APPLICATION_ACCESS = ?"
				+ " AND T1.APPLICATION_ACCESS = T2.APPLICATION_ACCESS " + userGrp + "  " + userProfile + " "
				+ " )TAPPR");

		strWhereNotExists = new String("NOT EXISTS (SELECT 'X' FROM PRD_PROFILE_PRIVILEGES_PEND TPEND"
				+ " WHERE     TPEND.APPLICATION_ACCESS = APPLICATION_ACCESS"
				+ " AND TPEND.USER_GROUP = TAPPR.USER_GROUP AND TPEND.USER_PROFILE = TAPPR.USER_PROFILE"
				+ " AND UPPER (TPEND.SCREEN_NAME) = UPPER (TAPPR.SCREEN_NAME))");

		strBufPending = new StringBuffer(" SELECT * FROM"
				+ " (SELECT  USER_GROUP,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM  ALPHA_SUB_TAB  WHERE ALPHA_TAB = T1.USER_GROUP_AT  AND ALPHA_SUB_TAB = T1.USER_GROUP) USER_GROUP_DESC,"
				+ "  USER_PROFILE,"
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB  WHERE ALPHA_TAB = T1.USER_PROFILE_AT AND ALPHA_SUB_TAB = T1.USER_PROFILE) USER_PROFILE_DESC,"
				+ "  T1.SCREEN_NAME AS SCREEN_NAME,"
				+ "  T2.MENU_NAME, T2.MENU_GROUP,T2.MENU_SEQUENCE,T2.PARENT_SEQUENCE,"
				+ "(SELECT T3.MENU_GROUP_NAME FROM PRD_MENU_GROUP T3 WHERE T3.APPLICATION_ACCESS= ? AND T2.MENU_GROUP=T3.MENU_GROUP_SEQ)MENU_GROUP_DESC,"
				+ "  P_ADD,  P_MODIFY,  P_DELETE,  P_INQUIRY,  P_VERIFICATION,"
				+ "  P_EXCEL_UPLOAD,  P_DOWNLOAD,  T1.PROFILE_STATUS,"
				+ " (SELECT  NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB  WHERE NUM_TAB = T1.PROFILE_STATUS_NT AND NUM_SUB_TAB = T1.PROFILE_STATUS) PROFILE_STATUS_DESC,"
				+ "  T1.RECORD_INDICATOR,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM  NUM_SUB_TAB  WHERE  NUM_TAB = T1.RECORD_INDICATOR_NT  AND NUM_SUB_TAB = T1.RECORD_INDICATOR) RECORD_INDICATOR_DESC,"
				+ " T1.MAKER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
				+ getDbFunction(Constants.NVL, null) + "(T1.MAKER,0) ) MAKER_NAME, "
				+ " T1.VERIFIER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
				+ getDbFunction(Constants.NVL, null) + "(T1.VERIFIER,0) ) VERIFIER_NAME,  "
				+ getDbFunction(Constants.DATEFUNC, null) + "(T1.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED ," + getDbFunction(Constants.DATEFUNC, null) + "(T1.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION,  T1.DATE_LAST_MODIFIED DATE_LAST_MODIFIED_1 FROM"
				+ " PRD_PROFILE_PRIVILEGES_PEND T1, PRD_VISION_MENU T2 WHERE"
				+ " UPPER (T1.SCREEN_NAME) = UPPER (T2.MENU_PROGRAM) AND T1.APPLICATION_ACCESS = ?"
				+ " AND T1.APPLICATION_ACCESS = T2.APPLICATION_ACCESS " + userGrp + "  " + userProfile + " "
				+ " )TPEND");

		params.addElement(productName);
		params.addElement(productName);

		if (ValidationUtil.isValid(dObj.getUserGroup()) && !"ALL".equalsIgnoreCase(dObj.getUserGroup()))
			params.addElement(dObj.getUserGroup());
		if (ValidationUtil.isValid(dObj.getUserProfile()) && !"ALL".equalsIgnoreCase(dObj.getUserProfile()))
			params.addElement(dObj.getUserProfile());

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
					case "userGroupDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.User_Group_desc) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.User_Group_desc) " + val, strBufPending,
								data.getJoinType());
						break;

					case "userProfileDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.User_Profile_desc) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.User_Profile_desc) " + val, strBufPending,
								data.getJoinType());
						break;

					case "menuGroupDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.Menu_Group_desc) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.Menu_Group_desc) " + val, strBufPending,
								data.getJoinType());
						break;

					case "screenNameDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.MENU_NAME) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.MENU_NAME) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileAdd":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_Add) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_Add) " + val, strBufPending, data.getJoinType());
						break;

					case "profileModify":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_Modify) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_Modify) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileDelete":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_Delete) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_Delete) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileInquiry":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_Inquiry) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_Inquiry) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileVerification":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_Verification) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_Verification) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileUpload":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_Excel_Upload) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_Excel_Upload) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileDownload":
						CommonUtils.addToQuerySearch(" upper(TAPPR.P_DOWNLOAD) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.P_DOWNLOAD) " + val, strBufPending,
								data.getJoinType());
						break;

					case "dateCreation":
						CommonUtils.addToQuerySearch(" upper(TAPPR.DATE_CREATION) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.DATE_CREATION) " + val, strBufPending,
								data.getJoinType());
						break;
					case "recordIndicatorDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.RECORD_INDICATOR_DESC) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.RECORD_INDICATOR_DESC) " + val, strBufPending,
								data.getJoinType());
						break;

					case "profileStatusDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.PROFILE_STATUS_DESC) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPEND.PROFILE_STATUS_DESC) " + val, strBufPending,
								data.getJoinType());
						break;

					default:
					}
					count++;
				}
			}
			String orderBy = " Order by DATE_LAST_MODIFIED_1 DESC ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error(((strBufApprove == null) ? "strBufApprove is Null" : strBufApprove.toString()));
				if (params != null)
					for (int i = 0; i < params.size(); i++)
						logger.error("objParams[" + i + "]" + params.get(i).toString());
			}
			return null;
		}
	}

	public List<RaProfileVb> getQueryResults(RaProfileVb dObj, int intStatus) {

		List<RaProfileVb> collTemp = null;
		final int intKeyFieldsCount = 5;
		String query = "";
		if (intStatus == Constants.STATUS_ZERO) {
			query = " SELECT USER_GROUP,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 1 AND ALPHA_SUB_TAB = USER_GROUP) USER_GROUP_DESC,"
					+ " USER_PROFILE,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2 AND ALPHA_SUB_TAB = USER_GROUP) USER_PROFILE_DESC,"
					+ " SCREEN_NAME, T2.MENU_PROGRAM, T2.MENU_NAME,T2.MENU_GROUP,"
					+ " (SELECT T3.MENU_GROUP_NAME FROM PRD_MENU_GROUP T3 WHERE APPLICATION_ACCESS=? AND T2.MENU_GROUP=T3.MENU_GROUP_SEQ)MENU_GROUP_DESC,"
					+ " TAPPR.P_ADD, TAPPR.P_MODIFY, TAPPR.P_DELETE, TAPPR.P_INQUIRY,"
					+ " TAPPR.P_VERIFICATION, TAPPR.P_EXCEL_UPLOAD, TAPPR.P_DOWNLOAD," +
					// " TAPPR.DATE_CREATION,"
					" TAPPR.PROFILE_STATUS, TAPPR.RECORD_INDICATOR,"
					+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = TAPPR.RECORD_INDICATOR_NT AND NUM_SUB_TAB = TAPPR.RECORD_INDICATOR) RECORD_INDICATOR_DESC,"
					+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = TAPPR.PROFILE_STATUS_NT AND NUM_SUB_TAB = TAPPR.PROFILE_STATUS) PROFILE_STATUS_DESC,"
					+ " TAPPR.MAKER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
					+ getDbFunction(Constants.NVL, null) + "(TAPPR.MAKER,0) ) MAKER_NAME, "
					+ " TAPPR.VERIFIER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
					+ getDbFunction(Constants.NVL, null) + "(TAPPR.VERIFIER,0) ) VERIFIER_NAME,  "
					+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
					+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
					+ "') DATE_LAST_MODIFIED ," + getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
					+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
					+ "') DATE_CREATION  FROM PRD_PROFILE_PRIVILEGES TAPPR, PRD_VISION_MENU T2"
					+ " WHERE UPPER (SCREEN_NAME) = UPPER (T2.MENU_PROGRAM) AND USER_GROUP = ?"
					+ " AND USER_PROFILE = ?   AND SCREEN_NAME = ? "
					+ " AND TAPPR.APPLICATION_ACCESS = T2.APPLICATION_ACCESS  AND TAPPR.APPLICATION_ACCESS = ?";
		} else {
			query = " SELECT USER_GROUP,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 1 AND ALPHA_SUB_TAB = USER_GROUP) USER_GROUP_DESC,"
					+ " USER_PROFILE,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2 AND ALPHA_SUB_TAB = USER_GROUP) USER_PROFILE_DESC,"
					+ " SCREEN_NAME, T2.MENU_PROGRAM, T2.MENU_NAME,T2.MENU_GROUP,"
					+ " (SELECT T3.MENU_GROUP_NAME FROM PRD_MENU_GROUP T3 WHERE APPLICATION_ACCESS=? AND T2.MENU_GROUP=T3.MENU_GROUP_SEQ)MENU_GROUP_DESC,"
					+ " TPEND.P_ADD, TPEND.P_MODIFY, TPEND.P_DELETE, TPEND.P_INQUIRY,"
					+ " TPEND.P_VERIFICATION, TPEND.P_EXCEL_UPLOAD, TPEND.P_DOWNLOAD," +
					// " TPEND.DATE_CREATION,"+
					" TPEND.PROFILE_STATUS, TPEND.RECORD_INDICATOR,"
					+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = TPEND.RECORD_INDICATOR_NT AND NUM_SUB_TAB = TPEND.RECORD_INDICATOR) RECORD_INDICATOR_DESC,"
					+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = TPEND.PROFILE_STATUS_NT AND NUM_SUB_TAB = TPEND.PROFILE_STATUS) PROFILE_STATUS_DESC,"
					+ " TPEND.MAKER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
					+ getDbFunction(Constants.NVL, null) + "(TPend.MAKER,0) ) MAKER_NAME, "
					+ " TPEND.VERIFIER,(SELECT MIN(USER_NAME) FROM VISION_USERS WHERE VISION_ID = "
					+ getDbFunction(Constants.NVL, null) + "(TPend.VERIFIER,0) ) VERIFIER_NAME , "
					+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
					+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
					+ "') DATE_LAST_MODIFIED ," + getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
					+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
					+ "') DATE_CREATION  FROM PRD_PROFILE_PRIVILEGES_PEND TPEND, PRD_VISION_MENU T2"
					+ " WHERE UPPER (SCREEN_NAME) = UPPER (T2.MENU_PROGRAM) AND USER_GROUP = ?"
					+ " AND USER_PROFILE = ?   AND SCREEN_NAME = ? "
					+ " AND TPEND.APPLICATION_ACCESS = T2.APPLICATION_ACCESS  AND TPEND.APPLICATION_ACCESS = ?";
		}

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = new String(productName);
		objParams[1] = new String(dObj.getUserGroup()); // [USER_GROUP]
		objParams[2] = new String(dObj.getUserProfile()); // [USER_PROFILE]
		objParams[3] = new String(dObj.getScreenName()); // [SCREEN NAME]
		objParams[4] = new String(productName);
		try {
			logger.info("Executing approved query");
			collTemp = getJdbcTemplate().query(query, objParams, getMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Error: getQueryResults Exception :   ");
				logger.error(((query == null) ? "strQueryAppr is null" : query.toString()));
			}
			return null;
		}
	}

	@Override
	protected List<RaProfileVb> selectApprovedRecord(RaProfileVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<RaProfileVb> doSelectPendingRecord(RaProfileVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected void setServiceDefaults() {
		serviceName = "ProfilePrivileges";
		serviceDesc = "ProfilePrivileges";
		tableName = "PRD_Profile_privileges";
		childTableName = "PRD_Profile_privileges";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	@Override
	protected int getStatus(RaProfileVb records) {
		return records.getProfileStatus();
	}

	@Override
	protected void setStatus(RaProfileVb vObject, int status) {
		vObject.setProfileStatus(status);
	}

	@Override
	protected int doInsertionAppr(RaProfileVb vObject) {
		String query = "";
		vObject.setProfileInquiry("Y");
		query = "Insert Into PRD_Profile_privileges(USER_GROUP, USER_PROFILE, SCREEN_NAME, "
				+ " P_ADD, P_MODIFY, P_DELETE, P_INQUIRY, P_VERIFICATION, P_EXCEL_UPLOAD,P_DOWNLOAD,"
				+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
				+ " DATE_LAST_MODIFIED, DATE_CREATION,Application_Access) "
				+ " Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, " + getDbFunction(Constants.SYSDATE, null)
				+ "," + getDbFunction(Constants.SYSDATE, null) + ",?)";

		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), vObject.getScreenName(),
				vObject.getProfileAdd(), vObject.getProfileModify(), vObject.getProfileDelete(),
				vObject.getProfileInquiry(), vObject.getProfileVerification(), vObject.getProfileUpload(),
				vObject.getProfileDownload(), vObject.getProfileStatus(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), productName };

		return getJdbcTemplate().update(query, args);
	}

	private int doInsertDashboardAccess(RaProfileVb vObject, String dashboardId) {
		String query = "";
		if ("ORACLE".equalsIgnoreCase(databaseType)) {
			query = "Insert Into PRD_DASHBOARD_ACCESS(  USER_GROUP,USER_PROFILE, DASHBOARD_ID, "
					+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
					+ " DATE_LAST_MODIFIED, DATE_CREATION,PRODUCT_NAME) "
					+ " Values (?, ?, ?, 0, 0, ?, ?, 0, SysDate, SysDate,'" + productName + "')";
		} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
			query = "Insert Into PRD_DASHBOARD_ACCESS(  USER_GROUP,USER_PROFILE, DASHBOARD_ID, "
					+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
					+ " DATE_LAST_MODIFIED, DATE_CREATION,PRODUCT_NAME) "
					+ " Values (?, ?, ?, 0, 0, ?, ?, 0, GetDate(), GetDate(),'" + productName + "')";
		}
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), dashboardId, vObject.getMaker(),
				vObject.getVerifier() };
		return getJdbcTemplate().update(query, args);
	}

	private int doInsertReportAccess(RaProfileVb vObject, String reportId) {
		String query = "";
		if ("ORACLE".equalsIgnoreCase(databaseType)) {
			query = "Insert Into PRD_REPORT_ACCESS(  USER_GROUP,USER_PROFILE, REPORT_ID, "
					+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
					+ " DATE_LAST_MODIFIED, DATE_CREATION,PRODUCT_NAME) "
					+ " Values (?, ?, ?, 0, 0, ?, ?, 0, SysDate,SysDate,'" + productName + "')";
		} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
			query = "Insert Into PRD_REPORT_ACCESS(  USER_GROUP,USER_PROFILE, REPORT_ID, "
					+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
					+ " DATE_LAST_MODIFIED, DATE_CREATION,PRODUCT_NAME) "
					+ " Values (?, ?, ?, 0, 0, ?, ?, 0, GetDate(), GetDate(),'" + productName + "')";
		}

		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), reportId, vObject.getMaker(),
				vObject.getVerifier() };
		return getJdbcTemplate().update(query, args);
	}

	private int doDeleteDashboardAccess(RaProfileVb vObject) {
		String query = "Delete From PRD_DASHBOARD_ACCESS Where USER_GROUP = ? And USER_PROFILE = ? and PRODUCT_NAME = '"
				+ productName + "'";
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile() };
		return getJdbcTemplate().update(query, args);
	}

	private int doDeleteReportAccess(RaProfileVb vObject) {
		String query = "Delete From PRD_REPORT_ACCESS Where USER_GROUP = ? And USER_PROFILE = ? and PRODUCT_NAME = '"
				+ productName + "'";
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(RaProfileVb vObject) {
		String query = "";
		query = "Insert Into PRD_Profile_privileges_PEND(  USER_GROUP, USER_PROFILE, SCREEN_NAME, "
				+ " P_ADD, P_MODIFY, P_DELETE, P_INQUIRY, P_VERIFICATION, P_EXCEL_UPLOAD,P_DOWNLOAD,"
				+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
				+ " DATE_LAST_MODIFIED, DATE_CREATION,APPLICATION_ACCESS) "
				+ " Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0," + getDbFunction(Constants.SYSDATE, null) + ","
				+ getDbFunction(Constants.SYSDATE, null) + ",?)";
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), vObject.getScreenName(),
				vObject.getProfileAdd(), vObject.getProfileModify(), vObject.getProfileDelete(),
				vObject.getProfileInquiry(), vObject.getProfileVerification(), vObject.getProfileUpload(),
				vObject.getProfileDownload(), vObject.getProfileStatus(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), productName };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(RaProfileVb vObject) {
		vObject.setProfileInquiry("Y");
		String query = "";
		query = "Insert Into PRD_Profile_privileges_PEND(  USER_GROUP, USER_PROFILE, SCREEN_NAME, "
				+ " P_ADD, P_MODIFY, P_DELETE, P_INQUIRY, P_VERIFICATION, P_EXCEL_UPLOAD,P_DOWNLOAD,"
				+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
				+ " DATE_LAST_MODIFIED, DATE_CREATION,APPLICATION_ACCESS) "
				+ " Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, " + getDbFunction(Constants.SYSDATE, null)
				+ ",?,?)";

		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), vObject.getScreenName(),
				vObject.getProfileAdd(), vObject.getProfileModify(), vObject.getProfileDelete(),
				vObject.getProfileInquiry(), vObject.getProfileVerification(), vObject.getProfileUpload(),
				vObject.getProfileDownload(), vObject.getProfileStatus(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getDateCreation(), productName };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(RaProfileVb vObject) {
		vObject.setProfileInquiry("Y");
		String query = "";
		query = "Update PRD_Profile_privileges Set "
				+ " P_ADD = ?, P_MODIFY = ?,P_DELETE = ?, P_INQUIRY = ?, P_VERIFICATION = ?, P_EXCEL_UPLOAD = ?,P_DOWNLOAD = ?,"
				+ "RECORD_INDICATOR = ?,PROFILE_STATUS = ?, MAKER = ?, VERIFIER = ?,  DATE_LAST_MODIFIED ="
				+ getDbFunction(Constants.SYSDATE, null)
				+ " Where USER_GROUP = ?  And USER_PROFILE = ? And SCREEN_NAME = ? and Application_Access = ?";
		Object args[] = { vObject.getProfileAdd(), vObject.getProfileModify(), vObject.getProfileDelete(),
				vObject.getProfileInquiry(), vObject.getProfileVerification(), vObject.getProfileUpload(),
				vObject.getProfileDownload(), vObject.getRecordIndicator(), vObject.getProfileStatus(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getUserGroup(), vObject.getUserProfile(),
				vObject.getScreenName(), productName };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(RaProfileVb vObject) {
		vObject.setProfileInquiry("Y");
		String query = "";
		query = "Update PRD_Profile_privileges_PEND Set "
				+ " P_ADD = ?, P_MODIFY = ?,P_DELETE = ?, P_INQUIRY = ?, P_VERIFICATION = ?, P_EXCEL_UPLOAD = ?,P_DOWNLOAD = ?,"
				+ "RECORD_INDICATOR = ?,PROFILE_STATUS = ?,MAKER = ?, VERIFIER = ?,  DATE_LAST_MODIFIED ="
				+ getDbFunction(Constants.SYSDATE, null)
				+ " Where USER_GROUP = ?  And USER_PROFILE = ? And SCREEN_NAME = ? and Application_Access = ?";

		Object args[] = { vObject.getProfileAdd(), vObject.getProfileModify(), vObject.getProfileDelete(),
				vObject.getProfileInquiry(), vObject.getProfileVerification(), vObject.getProfileUpload(),
				vObject.getProfileDownload(), vObject.getRecordIndicator(), vObject.getProfileStatus(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getUserGroup(), vObject.getUserProfile(),
				vObject.getScreenName(), productName };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(RaProfileVb vObject) {
		String query = "Delete From PRD_Profile_privileges Where USER_GROUP = ? And USER_PROFILE = ? And SCREEN_NAME = ? and Application_Access = ?";
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), vObject.getScreenName(), productName };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(RaProfileVb vObject) {
		String query = "Delete From PRD_Profile_privileges_PEND Where USER_GROUP = ? And USER_PROFILE = ? And SCREEN_NAME = ? and Application_Access = ?";
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), vObject.getScreenName(), productName };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String frameErrorMessage(RaProfileVb vObject, String strOperation) {
		// specify all the key fields and their values first
		String strErrMsg = new String("");
		try {
			strErrMsg = strErrMsg + " USER_GROUP:" + vObject.getUserGroup();
			strErrMsg = strErrMsg + " USER_PROFILE:" + vObject.getUserProfile();
			strErrMsg = strErrMsg + " SCREEN_NAME:" + vObject.getScreenName();
			// Now concatenate the error message that has been sent
			if ("Approve".equalsIgnoreCase(strOperation))
				strErrMsg = strErrMsg + " failed during approve Operation. Bulk Approval aborted !!";
			else
				strErrMsg = strErrMsg + " failed during reject Operation. Bulk Rejection aborted !!";
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error(strErrMsg, ex);
			}
			strErrorDesc = ex.getMessage();
			strErrMsg = strErrMsg + strErrorDesc;
			
		}
		// Return back the error message string
		return strErrMsg;
	}

	@Override
	protected String getAuditString(RaProfileVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
		try {
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

			strAudit.append("SCREEN_NAME" + auditDelimiterColVal + vObject.getScreenName());
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileAdd()))
				strAudit.append("P_ADD" + auditDelimiterColVal + vObject.getProfileAdd().trim());
			else
				strAudit.append("P_ADD" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileModify()))
				strAudit.append("P_MODIFY" + auditDelimiterColVal + vObject.getProfileModify().trim());
			else
				strAudit.append("P_MODIFY" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileDelete()))
				strAudit.append("P_DELETE" + auditDelimiterColVal + vObject.getProfileDelete().trim());
			else
				strAudit.append("P_DELETE" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileInquiry()))
				strAudit.append("P_INQUIRY" + auditDelimiterColVal + vObject.getProfileInquiry().trim());
			else
				strAudit.append("P_INQUIRY" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileVerification()))
				strAudit.append("P_VERIFICATION" + auditDelimiterColVal + vObject.getProfileVerification().trim());
			else
				strAudit.append("P_VERIFICATION" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileUpload()))
				strAudit.append("P_EXCEL_UPLOAD" + auditDelimiterColVal + vObject.getProfileUpload().trim());
			else
				strAudit.append("P_EXCEL_UPLOAD" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			if (ValidationUtil.isValid(vObject.getProfileDownload()))
				strAudit.append("P_DOWNLOAD" + auditDelimiterColVal + vObject.getProfileDownload().trim());
			else
				strAudit.append("P_DOWNLOAD" + auditDelimiterColVal + "NULL");
			strAudit.append(auditDelimiter);

			strAudit.append("PROFILE_STATUS_NT" + auditDelimiterColVal + vObject.getProfileStatusNt());
			strAudit.append(auditDelimiter);
			strAudit.append("PROFILE_STATUS" + auditDelimiterColVal + vObject.getProfileStatus());
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

		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			strAudit = strAudit.append(strErrorDesc);
			ex.printStackTrace();
		}
		return strAudit.toString();
	}

	public List<AlphaSubTabVb> findScreenName(int menuGroup) {
		String sql = " Select MENU_PROGRAM ALPHA_SUB_TAB,Menu_Name ALPHA_SUBTAB_DESCRIPTION from PRD_VISION_MENU where menu_Sequence <> Parent_Sequence "
		// String sql = " Select MENU_PROGRAM ALPHA_SUB_TAB,Menu_Name ALPHA_SUBTAB_DESCRIPTION from PRD_VISION_MENU where SEPARATOR = 'Y' "
				+ "  AND MENU_STATUS = 0 and Menu_Group = ? and Application_Access = ? order by menu_Sequence ";
		Object[] lParams = new Object[2];
		lParams[0] = menuGroup;
		lParams[1] = productName;
		return getJdbcTemplate().query(sql, lParams, getAlpSubTabMapper());
	}

	public List<NumSubTabVb> getTopLevelMenu() throws DataAccessException {
		String sql = " Select MENU_GROUP_SEQ,MENU_GROUP_NAME from PRD_MENU_GROUP where MENU_GROUP_Status = 0 and APPLICATION_ACCESS=? ";
		Object[] lParams = new Object[1];
		lParams[0] = productName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				NumSubTabVb vObject = new NumSubTabVb();
				vObject.setDescription(rs.getString("MENU_GROUP_NAME"));
				vObject.setNumSubTab(rs.getInt("MENU_GROUP_SEQ"));
				return vObject;
			}
		};
		List<NumSubTabVb> profileData = getJdbcTemplate().query(sql, lParams, mapper);
		return profileData;
	}

	// Get Dashboard List
	public List<AlphaSubTabVb> getDashboardList() {
		String sql = " SELECT DASHBOARD_ID ALPHA_SUB_TAB,DASHBOARD_NAME ALPHA_SUBTAB_DESCRIPTION FROM PRD_DASHBOARDS WHERE STATUS = 0 and product_Name = ?"
				+ " ORDER BY DASHBOARD_ID ";
		Object[] lParams = new Object[1];
		lParams[0] = productName;
		return getJdbcTemplate().query(sql, getAlpSubTabMapper());
	}

	protected RowMapper getAlpSubTabMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AlphaSubTabVb alphaSubTabVb = new AlphaSubTabVb();
				alphaSubTabVb.setAlphaSubTab(rs.getString("ALPHA_SUB_TAB"));
				alphaSubTabVb.setDescription(rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				return alphaSubTabVb;
			}
		};
		return mapper;
	}

	public List<AlphaSubTabVb> findUserGroupPP() {
		String sql = " Select distinct User_Group ALPHA_SUB_TAB,(Select alpha_subtab_description from Alpha_sub_tab "
				+ " where Alpha_tab = 1 and alpha_sub_tab=User_Group) ALPHA_SUBTAB_DESCRIPTION from PRD_PROFILE_PRIVILEGES  "
				+ " WHERE APPLICATION_ACCESS='" + productName + "' Union "
				+ " Select distinct User_Group ALPHA_SUB_TAB,(Select alpha_subtab_description from Alpha_sub_tab "
				+ " where Alpha_tab = 1 and alpha_sub_tab=User_Group) ALPHA_SUBTAB_DESCRIPTION from PRD_PROFILE_PRIVILEGES_PEND where Application_Access = '"
				+ productName + "' ";
		/*
		 * Object[] lParams = new Object[1]; lParams[0] = productName;
		 */
		return getJdbcTemplate().query(sql, getAlpSubTabMapper());
	}

	public List<AlphaSubTabVb> findUserProfilePP(String userGroup) {
		String sql = " Select distinct User_Profile ALPHA_SUB_TAB,(Select alpha_subtab_description from Alpha_sub_tab "
				+ " where Alpha_tab = 2 and alpha_sub_tab=User_Profile) ALPHA_SUBTAB_DESCRIPTION from PRD_PROFILE_PRIVILEGES where User_Group = ? "
				+ " AND APPLICATION_ACCESS= ? Union "
				+ " Select distinct User_Profile ALPHA_SUB_TAB,(Select alpha_subtab_description from Alpha_sub_tab "
				+ " where Alpha_tab = 2 and alpha_sub_tab=User_Profile) ALPHA_SUBTAB_DESCRIPTION from PRD_PROFILE_PRIVILEGES_PEND where User_Group = ? and Application_Access = ?";

		Object[] lParams = new Object[4];
		lParams[0] = userGroup;
		lParams[1] = productName;
		lParams[2] = userGroup;
		lParams[3] = productName;
		return getJdbcTemplate().query(sql, lParams, getAlpSubTabMapper());
	}

	public int doDeleteProfileDashboard(RaProfileVb vObject) {
		try {
			String query = "Delete From PRD_PROFILE_DASHBOARDS Where USER_GROUP = ? And USER_PROFILE = ? and Application_Access = ?";
			Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), productName };
			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			return 0;
		}
	}

	public int doInsertionProfileDashboard(RaProfileVb vObject) {
		String query = "";
		if ("ORACLE".equalsIgnoreCase(databaseType)) {
			query = "Insert Into PRD_PROFILE_DASHBOARDS(USER_GROUP, USER_PROFILE, HOME_DASHBOARD,"
					+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
					+ " DATE_LAST_MODIFIED, DATE_CREATION,APPLICATION_ACCESS) "
					+ " Values (?, ?, ?, 0, 0, ?, ?, 0, Sysdate, Sysdate,'" + productName + "')";
		} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
			query = "Insert Into PRD_PROFILE_DASHBOARDS(USER_GROUP, USER_PROFILE, HOME_DASHBOARD,"
					+ " PROFILE_STATUS, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,"
					+ " DATE_LAST_MODIFIED, DATE_CREATION,APPLICATION_ACCESS) "
					+ " Values (?, ?, ?, 0, 0, ?, ?, 0, GETDATE(), GETDATE(),'" + productName + "')";
		}
		Object args[] = { vObject.getUserGroup(), vObject.getUserProfile(), vObject.getHomeDashboard(),
				vObject.getMaker(), vObject.getVerifier() };

		return getJdbcTemplate().update(query, args);
	}

	// Get Dashboard List
	public List<AlphaSubTabVb> getAllDashboardList(String type) {
		String sql = "";
		try {
			if (type == "D")
				sql = " SELECT DASHBOARD_ID ALPHA_SUB_TAB,DASHBOARD_NAME ALPHA_SUBTAB_DESCRIPTION FROM PRD_DASHBOARDS WHERE STATUS = 0 and Application_ID = ? ORDER BY DASHBOARD_NAME ";
			else
				sql = " SELECT Report_ID ALPHA_SUB_TAB,Report_Title ALPHA_SUBTAB_DESCRIPTION FROM PRD_REPORT_MASTER WHERE Status = 0 and Application_ID = ? order by Report_Title ";

			Object[] lParams = new Object[1];
			lParams[0] = productName;
			return getJdbcTemplate().query(sql, lParams, getAlpSubTabMapper());
		} catch (Exception e) {
			return null;
		}
	}

	public List<AlphaSubTabVb> getSelectedDashboardList(RaProfileVb vObject) {
		String sql = "";
		try {
			if ("REPORTS".equalsIgnoreCase(vObject.getScreenName())
					|| "reports".equalsIgnoreCase(vObject.getScreenName()))
				sql = " SELECT REPORT_ID ALPHA_SUB_TAB,'' ALPHA_SUBTAB_DESCRIPTION FROM PRD_REPORT_ACCESS WHERE USER_GROUP='"
						+ vObject.getUserGroup() + "' AND USER_PROFILE='" + vObject.getUserProfile()
						+ "' AND product_Name ='" + productName + "' ORDER BY REPORT_ID ";
			else if ("DASHBOARDS".equalsIgnoreCase(vObject.getScreenName())
					|| "dashboards".equalsIgnoreCase(vObject.getScreenName()))
				sql = "  SELECT DASHBOARD_ID ALPHA_SUB_TAB,'' ALPHA_SUBTAB_DESCRIPTION FROM PRD_DASHBOARD_ACCESS WHERE USER_GROUP='"
						+ vObject.getUserGroup() + "' AND USER_PROFILE='" + vObject.getUserProfile()
						+ "' AND product_Name ='" + productName + "' ORDER BY DASHBOARD_ID ";

			return getJdbcTemplate().query(sql, getAlpSubTabMapper());
		} catch (Exception e) {
			return null;
		}
	}

	public ExceptionCode doInsertApprRecordForNonTrans(RaProfileVb vObject) throws RuntimeCustomException {
		List<RaProfileVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.ADD;
		strApproveOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			logger.error("Collection is null for Select Approved Record");
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<RaProfileVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				logger.error("Collection size is greater than zero - Duplicate record found, but inactive");
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				logger.error("Collection size is greater than zero - Duplicate record found");
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		// Try inserting the record
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setProfileStatus(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		retVal = doInsertionAppr(vObject);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		}
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		vObject.setDateCreation(systemDate);
		exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		/*
		 * exceptionCode = writeAuditLog(vObject, null); if(exceptionCode.getErrorCode()
		 * != Constants.SUCCESSFUL_OPERATION){ exceptionCode =
		 * getResultObject(Constants.AUDIT_TRAIL_ERROR); throw
		 * buildRuntimeCustomException(exceptionCode); }
		 */
		return exceptionCode;
	}

	public ExceptionCode doUpdateApprRecordForNonTrans(RaProfileVb vObject) throws RuntimeCustomException {
		List<RaProfileVb> collTemp = null;
		RaProfileVb vObjectlocal = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
		// Even if record is not there in Appr. table reject the record
		if (collTemp.size() == 0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		vObject.setDateCreation(vObjectlocal.getDateCreation());
		retVal = doUpdateAppr(vObject);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} /*
			 * else { insertReportDashboardAccess(vObject); exceptionCode =
			 * getResultObject(Constants.SUCCESSFUL_OPERATION); }
			 */
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		/*
		 * exceptionCode = writeAuditLog(vObject, vObjectlocal);
		 * if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION){
		 * exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR); throw
		 * buildRuntimeCustomException(exceptionCode); }
		 */
		exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}

	protected ExceptionCode doDeleteApprRecordForNonTrans(RaProfileVb vObject) throws RuntimeCustomException {
		List<RaProfileVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		RaProfileVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<RaProfileVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
		vObject.setDateCreation(vObjectlocal.getDateCreation());

		if (vObject.isStaticDelete()) {
			vObjectlocal.setMaker(getIntCurrentUserId());
			vObject.setVerifier(getIntCurrentUserId());
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
//			setStatus(vObject, Constants.PASSIVATE);
			setStatus(vObjectlocal, Constants.PASSIVATE);
			vObjectlocal.setVerifier(getIntCurrentUserId());
			vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
			retVal = doUpdateAppr(vObjectlocal);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
		} else {
			// delete the record from the Approve Table
			retVal = doDeleteAppr(vObject);

//			vObject.setRecordIndicator(-1);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		/*
		 * if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
		 * exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR); throw
		 * buildRuntimeCustomException(exceptionCode); }
		 */
		return exceptionCode;
	}

	private void insertReportDashboardAccess(RaProfileVb vObject) {
		try {
			if ("REPORTS".equalsIgnoreCase(vObject.getScreenName())
					|| "reports".equalsIgnoreCase(vObject.getScreenName())) {
				doDeleteReportAccess(vObject);
				if (vObject.getDashboardReportlst() != null && vObject.getDashboardReportlst().size() > 0) {
					vObject.getDashboardReportlst().forEach(reportVb -> {
						doInsertReportAccess(vObject, reportVb.getAlphaSubTab());
					});
				}
			} else if ("DASHBOARDS".equalsIgnoreCase(vObject.getScreenName())
					|| "dashboards".equalsIgnoreCase(vObject.getScreenName())) {
				doDeleteDashboardAccess(vObject);
				if (vObject.getDashboardReportlst() != null && vObject.getDashboardReportlst().size() > 0) {
					vObject.getDashboardReportlst().forEach(dashboardVb -> {
						doInsertDashboardAccess(vObject, dashboardVb.getAlphaSubTab());
					});
				}
			}
		} catch (Exception e) {
			logger.error("Error while inserting Dashboard Access:" + e.getMessage());

		}
	}

	protected ExceptionCode doInsertRecordForNonTrans(RaProfileVb vObject) throws RuntimeCustomException {
		List<RaProfileVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			// logger.error("Collection is null for Select Approved Record");
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int staticDeletionFlag = getStatus(((ArrayList<RaProfileVb>) collTemp).get(0));
			if (staticDeletionFlag == Constants.PASSIVATE) {
				// logger.info("Collection size is greater than zero - Duplicate record found,
				// but inactive");
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				// logger.info("Collection size is greater than zero - Duplicate record found");
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		// Try to see if the record already exists in the pending table, but not in
		// approved table
		collTemp = null;
		collTemp = doSelectPendingRecord(vObject);

		// The collTemp variable could not be null. If so, there is no problem fetching
		// data
		// return back error code to calling routine
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// if record already exists in pending table, modify the record
		if (collTemp.size() > 0) {
			RaProfileVb vObjectLocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
			if (vObjectLocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				exceptionCode = getResultObject(Constants.PENDING_FOR_ADD_ALREADY);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			// Try inserting the record
			vObject.setVerifier(0);
			vObject.setRecordIndicator(Constants.STATUS_INSERT);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
			vObject.setDateCreation(systemDate);
			retVal = doInsertionPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		}
	}

	protected ExceptionCode doUpdateRecordForNonTrans(RaProfileVb vObject) throws RuntimeCustomException {
		List<RaProfileVb> collTemp = null;
		RaProfileVb vObjectlocal = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		// Search if record already exists in pending. If it already exists, check for
		// status
		collTemp = doSelectPendingRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);

			// Check if the record is pending for deletion. If so return the error
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE) {
				exceptionCode = getResultObject(Constants.RECORD_PENDING_FOR_DELETION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				vObject.setVerifier(0);
				vObject.setRecordIndicator(Constants.STATUS_INSERT);
				retVal = doUpdatePend(vObject);
			} else {
				vObject.setVerifier(0);
				vObject.setRecordIndicator(Constants.STATUS_UPDATE);
				retVal = doUpdatePend(vObject);
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} else {
			collTemp = null;
			collTemp = selectApprovedRecord(vObject);

			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Even if record is not there in Appr. table reject the record
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			// This is required for Audit Trail.
			if (collTemp.size() > 0) {
				vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
				vObject.setDateCreation(vObjectlocal.getDateCreation());
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			// Record is there in approved, but not in pending. So add it to pending
			vObject.setVerifier(0);
			vObject.setRecordIndicator(Constants.STATUS_UPDATE);
			retVal = doInsertionPendWithDc(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
	}

	protected ExceptionCode doDeleteRecordForNonTrans(RaProfileVb vObject) throws RuntimeCustomException {
		RaProfileVb vObjectlocal = null;
		List<RaProfileVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		collTemp = selectApprovedRecord(vObject);

		if (collTemp == null) {
			// logger.error("Collection is null");
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
			int intStaticDeletionFlag = getStatus(vObjectlocal);
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// check to see if the record already exists in the pending table
		collTemp = doSelectPendingRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// If records are there, check for the status and decide what error to return
		// back
		if (collTemp.size() > 0) {
			exceptionCode = getResultObject(Constants.TRYING_TO_DELETE_APPROVAL_PENDING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// insert the record into pending table with status 3 - deletion
		if (vObjectlocal == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
			vObjectlocal.setDateCreation(vObject.getDateCreation());
		}
		// vObjectlocal.setDateCreation(vObject.getDateCreation());
		vObjectlocal.setMaker(getIntCurrentUserId());
		vObjectlocal.setRecordIndicator(Constants.STATUS_DELETE);
		vObjectlocal.setVerifier(0);
		retVal = doInsertionPendWithDc(vObjectlocal);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setRecordIndicator(Constants.STATUS_DELETE);
		vObject.setVerifier(0);
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	public ExceptionCode doRejectRecord(RaProfileVb vObject) throws RuntimeCustomException {
		RaProfileVb vObjectlocal = null;
		List<RaProfileVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strErrorDesc = "";
		strCurrentOperation = Constants.REJECT;
		vObject.setMaker(getIntCurrentUserId());
		try {
			if (vObject.getRecordIndicator() == 1 || vObject.getRecordIndicator() == 3)
				vObject.setRecordIndicator(0);
			else
				vObject.setRecordIndicator(-1);
			// See if such a pending request exists in the pending table
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);
			// Delete the record from the Pending table
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			// logger.error("Error in Reject.",ex);
			// logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	public ExceptionCode doApproveRecord(RaProfileVb vObject, boolean staticDelete) throws RuntimeCustomException {
		RaProfileVb oldContents = null;
		RaProfileVb vObjectlocal = null;
		List<RaProfileVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		try {
			if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
				exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}
			// See if such a pending request exists in the pending table
			vObject.setVerifier(getIntCurrentUserId());
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}

			vObjectlocal = ((ArrayList<RaProfileVb>) collTemp).get(0);

			if (vObjectlocal.getMaker() == getIntCurrentUserId()) {
				exceptionCode = getResultObject(Constants.MAKER_CANNOT_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// If it's NOT addition, collect the existing record contents from the
			// Approved table and keep it aside, for writing audit information later.
			if (vObjectlocal.getRecordIndicator() != Constants.STATUS_INSERT) {
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				oldContents = ((ArrayList<RaProfileVb>) collTemp).get(0);
			}

			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) { // Add authorization
				// Write the contents of the Pending table record to the Approved table
				vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
				vObjectlocal.setVerifier(getIntCurrentUserId());
				retVal = doInsertionAppr(vObjectlocal);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}

				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				vObject.setDateCreation(systemDate);
				strApproveOperation = Constants.ADD;
			} else if (vObjectlocal.getRecordIndicator() == Constants.STATUS_UPDATE) { // Modify authorization

				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}

				// If record already exists in the approved table, reject the addition
				if (collTemp.size() > 0) {
					// retVal = doUpdateAppr(vObjectlocal, MISConstants.ACTIVATE);
					vObjectlocal.setVerifier(getIntCurrentUserId());
					vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
					retVal = doUpdateAppr(vObjectlocal);
				}
				// Modify the existing contents of the record in Approved table
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				// Set the current operation to write to audit log
				strApproveOperation = Constants.MODIFY;
			} else if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE) { // Delete authorization
				if (staticDelete) {
					// Update the existing record status in the Approved table to delete
					setStatus(vObjectlocal, Constants.PASSIVATE);
					vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
					vObjectlocal.setVerifier(getIntCurrentUserId());
					retVal = doUpdateAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					setStatus(vObject, Constants.PASSIVATE);
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);

				} else {
					// Delete the existing record from the Approved table
					retVal = doDeleteAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);
				}
				// Set the current operation to write to audit log
				strApproveOperation = Constants.DELETE;
			} else {
				exceptionCode = getResultObject(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Delete the record from the Pending table
			retVal = deletePendingRecord(vObjectlocal);

			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Set the internal status to Approved
			vObject.setInternalStatus(0);
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE && !staticDelete) {
				exceptionCode = writeAuditLog(null, oldContents);
				vObject.setRecordIndicator(-1);
			} else
				exceptionCode = writeAuditLog(vObjectlocal, oldContents);

			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			// logger.error("Error in Approve.",ex);
			// logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}
	
	public String getMenuProgramForMenuGroup(String menuGroupSeq) {
		String sql = " SELECT MENU_PROGRAM FROM PRD_MENU_GROUP WHERE MENU_GROUP_STATUS = 0 AND APPLICATION_ACCESS = ? AND MENU_GROUP_SEQ = ? ";
		Object[] lParams = { productName, menuGroupSeq };
		return getJdbcTemplate().queryForObject(sql, lParams, String.class);
	}
}
