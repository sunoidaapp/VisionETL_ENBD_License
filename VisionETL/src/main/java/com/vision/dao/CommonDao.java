package com.vision.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonApiModel;
import com.vision.vb.CommonVb;
import com.vision.vb.MenuVb;
import com.vision.vb.ProfileData;
import com.vision.vb.VisionUsersVb;

@Component
public class CommonDao {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Value("${app.databaseType}")
	private String databaseType;

	@Value("${app.productName}")
	private String productName;
	
	@Value("${app.enableDebug}")
	protected String enableDebug;
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Autowired
	CommonApiDao commonApiDao;
	
	@Autowired
	LoginUserDao loginUserDao;

	public List<CommonVb> findVerificationRequiredAndStaticDelete(String pTableName) throws DataAccessException {

		String sql = "select DELETE_TYPE,VERIFICATION_REQD FROM VISION_TABLES where UPPER(TABLE_NAME) = UPPER(?)";
		Object[] lParams = new Object[1];
		lParams[0] = pTableName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setStaticDelete(
						rs.getString("DELETE_TYPE") == null || rs.getString("DELETE_TYPE").equalsIgnoreCase("S") ? true
								: false);
				commonVb.setVerificationRequired(rs.getString("VERIFICATION_REQD") == null
						|| rs.getString("VERIFICATION_REQD").equalsIgnoreCase("Y") ? true : false);
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = getJdbcTemplate().query(sql, lParams, mapper);
		if (commonVbs == null || commonVbs.isEmpty()) {
			commonVbs = new ArrayList<CommonVb>();
			CommonVb commonVb = new CommonVb();
			commonVb.setStaticDelete(true);
			commonVb.setVerificationRequired(true);
			commonVbs.add(commonVb);
		}
		return commonVbs;
	}

	public List<ProfileData> getTopLevelMenu() throws DataAccessException {
		String sql = " Select MENU_GROUP_SEQ,MENU_GROUP_NAME,MENU_GROUP_ICON, MENU_PROGRAM from PRD_MENU_GROUP where MENU_GROUP_Status = 0 and Application_Access = ? order by MENU_GROUP_SEQ ";
		Object[] lParams = { productName };
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setMenuItem(rs.getString("MENU_GROUP_NAME"));
				profileData.setMenuGroup(rs.getInt("MENU_GROUP_SEQ"));
				profileData.setMenuIcon(rs.getString("MENU_GROUP_ICON"));
				profileData.setMenuProgram(rs.getString("MENU_PROGRAM"));
				return profileData;
			}
		};
		List<ProfileData> profileData = getJdbcTemplate().query(sql, lParams, mapper);
		return profileData;
	}

	public ArrayList<MenuVb> getSubMenuItemsForMenuGroup(int menuGroup) throws DataAccessException {

		String sql = "SELECT * FROM PRD_vision_Menu WHERE MENU_GROUP = ? AND MENU_STATUS = 0 and Application_Access Like '%"
				+ productName + "%' and Menu_Sequence = Parent_Sequence ORDER BY PARENT_SEQUENCE, MENU_SEQUENCE";
		Object[] lParams = new Object[1];
		lParams[0] = menuGroup;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				MenuVb menuVb = new MenuVb();
				menuVb.setMenuProgram(rs.getString("MENU_PROGRAM"));
				menuVb.setMenuName(rs.getString("MENU_NAME"));
				menuVb.setMenuSequence(rs.getInt("MENU_SEQUENCE"));
				menuVb.setParentSequence(rs.getInt("PARENT_SEQUENCE"));
				menuVb.setSeparator(rs.getString("SEPARATOR"));
				menuVb.setMenuGroup(rs.getInt("MENU_GROUP"));
				menuVb.setMenuStatus(rs.getInt("MENU_STATUS"));
//				menuVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				return menuVb;
			}
		};
		ArrayList<MenuVb> menuList = (ArrayList<MenuVb>) getJdbcTemplate().query(sql, lParams, mapper);
		return menuList;
	}

	public ArrayList<MenuVb> getSubMenuItemsForSubMenuGroup(int menuGroup, int parentSequence, int visionId)
			throws DataAccessException {
		String sql = "";
		sql = " SELECT *       FROM PRD_vision_Menu t1, PRD_PROFILE_PRIVILEGES t2, ETL_USER_VIEW t3   "
				+ "    WHERE     MENU_GROUP = ?                                      "
				+ "  AND MENU_STATUS = 0 AND PROFILE_STATUS = 0 AND USER_STATUS = 0  "
				+ "          AND UPPER (MENU_NAME) != 'SEPERATOR'                    "
				+ "          AND t1.Application_Access = ?       "
				// + " AND Menu_Sequence <> Parent_Sequence "
				+ "          AND Parent_Sequence = ?            "
				+ "          AND T1.MENU_PROGRAM = t2.screen_Name                    "
				+ "          AND T1.Application_Access = t2.Application_Access       "
				+ "          AND t3.vision_ID = ?                  "
				+ "          AND t2.User_Group = t3.user_Group                       "
				+ "          AND T2.USER_PROFILE = t3.User_Profile                   "
				+ " ORDER BY PARENT_SEQUENCE, MENU_SEQUENCE                          ";

		Object[] lParams = new Object[4];
		lParams[0] = menuGroup;
		lParams[1] = productName;
		lParams[2] = parentSequence;
		lParams[3] = visionId;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				MenuVb menuVb = new MenuVb();
				menuVb.setMenuProgram(rs.getString("MENU_PROGRAM"));
				menuVb.setMenuName(rs.getString("MENU_NAME"));
				menuVb.setMenuSequence(rs.getInt("MENU_SEQUENCE"));
				menuVb.setParentSequence(rs.getInt("PARENT_SEQUENCE"));
				menuVb.setSeparator(rs.getString("SEPARATOR"));
				menuVb.setMenuGroup(rs.getInt("MENU_GROUP"));
				menuVb.setMenuStatus(rs.getInt("MENU_STATUS"));
				menuVb.setProfileAdd(rs.getString("P_ADD"));
				menuVb.setProfileModify(rs.getString("P_MODIFY"));
				menuVb.setProfileDelete(rs.getString("P_DELETE"));
				menuVb.setProfileView(rs.getString("P_INQUIRY"));
				menuVb.setProfileVerification(rs.getString("P_VERIFICATION"));
				menuVb.setProfileUpload(rs.getString("P_EXCEL_UPLOAD"));
				menuVb.setProfileDownload(rs.getString("P_DOWNLOAD"));
				return menuVb;
			}
		};
		ArrayList<MenuVb> menuList = (ArrayList<MenuVb>) getJdbcTemplate().query(sql, lParams, mapper);
		return menuList;
	}

	public String findVisionVariableValue(String pVariableName) throws DataAccessException {
		if (!ValidationUtil.isValid(pVariableName)) {
			return null;
		}
		String sql = "select VALUE FROM VISION_VARIABLES where UPPER(VARIABLE) = UPPER(?)";
		Object[] lParams = new Object[1];
		lParams[0] = pVariableName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setMakerName(rs.getString("VALUE"));
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = getJdbcTemplate().query(sql, lParams, mapper);
		if (commonVbs != null && !commonVbs.isEmpty()) {
			return commonVbs.get(0).getMakerName();
		}
		return null;
	}

	public void findDefaultHomeScreen(VisionUsersVb vObject) throws DataAccessException {
		int count = 0;
		String sql = "SELECT COUNT(1) FROM PWT_REPORT_SUITE WHERE VISION_ID = ? "  ;
		Object[] lParams = { vObject.getVisionId() };
		count = getJdbcTemplate().queryForObject(sql, lParams, Integer.class);
		/*
		 * if(count>0){ vObject.setDefaultHomeScreen(true); }
		 */
	}

	public int getMaxOfId() {
		String sql = "select max(vision_id) from (Select max(vision_id) vision_id from ETL_USER_VIEW UNION ALL select Max(vision_id) from ETL_USER_VIEW)";
		int i = getJdbcTemplate().queryForObject(sql, Integer.class);
		return i;
	}

	public String getVisionBusinessDate(String countryLeBook) {
		Object args[] = { countryLeBook };
		return getJdbcTemplate().queryForObject("select " + AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
				+ "(BUSINESS_DATE,'" + AbstractCommonDao.getDbFunction(Constants.DD_Mon_RRRR, null)
				+ "') BUSINESS_DATE  from Vision_Business_Day  WHERE COUNTRY "
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " '-' "
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " LE_BOOK=? and APPLICATION_ID='ETL' ",
				String.class, args);
	}

	public String getVisionCurrentYearMonth() {
		return getJdbcTemplate().queryForObject("select " + AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
				+ "(to_date(CURRENT_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') CURRENT_YEAR_MONTH  from V_Curr_Year_Month",
				String.class);
	}

	public int getUploadCount() {
		String sql = "Select count(1) from Vision_Upload where Upload_Status = 1 AND FILE_NAME LIKE '%XLSX'";
		int i = getJdbcTemplate().queryForObject(sql, Integer.class);
		return i;
	}

	public int doPasswordResetInsertion(VisionUsersVb vObject) {
		Date oldDate = new Date();
		SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		String resetValidity = df.format(oldDate);
		if (ValidationUtil.isValid(vObject.getPwdResetTime())) {
			Date newDate = DateUtils.addHours(oldDate, Integer.parseInt(vObject.getPwdResetTime()));
			resetValidity = df.format(newDate);
		}
		String query = "Insert Into FORGOT_PASSWORD ( VISION_ID, RESET_DATE, RESET_VALIDITY, RS_STATUS_NT, RS_STATUS)"
				+ "Values (?, " + AbstractCommonDao.getDbFunction(Constants.SYSDATE, null) + ", "
				+ AbstractCommonDao.getDbFunction(Constants.TO_DATE, resetValidity) + " , ?, ?)";
		Object[] args = { vObject.getVisionId(), vObject.getUserStatusNt(), vObject.getUserStatus() };
		return getJdbcTemplate().update(query, args);
	}

	public String getSystemDate() {
		String fromDual = AbstractCommonDao.getDbFunction(Constants.DUAL, null);
		String sql = "SELECT " + AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null) + "( "
				+ AbstractCommonDao.getDbFunction(Constants.SYSDATE, null) + " , '"
				+ AbstractCommonDao.getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ AbstractCommonDao.getDbFunction(Constants.TIME, null) + "') AS SYSDATE1 ";
		if (ValidationUtil.isValid(fromDual)) {
			sql = sql + fromDual;
		}
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("SYSDATE1"));
			}
		};
		return (String) getJdbcTemplate().queryForObject(sql, mapper);
	}

	public String getSystemDate12Hr() {
		String fromDual = AbstractCommonDao.getDbFunction(Constants.DUAL, null);
		String sql = "SELECT " + AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null) + "( "
				+ AbstractCommonDao.getDbFunction(Constants.SYSDATE, null) + " , '"
				+ AbstractCommonDao.getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ AbstractCommonDao.getDbFunction(Constants.TIME, null) + "') AS SYSDATE1 ";
		if (ValidationUtil.isValid(fromDual)) {
			sql = sql + fromDual;
		}
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("SYSDATE1"));
			}
		};
		return (String) getJdbcTemplate().queryForObject(sql, mapper);
	}

	public String getScriptValue(String pVariableName) throws DataAccessException, Exception {
		Object params[] = { pVariableName };
		String sql = new String(
				"select VARIABLE_SCRIPT from VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_TYPE = 2  AND UPPER(VARIABLE_NAME)=UPPER(?) ");
		return getJdbcTemplate().queryForObject(sql, params, String.class);
	}

	public List<ProfileData> getTopLevelMenu(int visionId, String adMembers) throws DataAccessException {

		adMembers = adMembers.replaceAll("DC=ubagroup,DC=com", "").replaceAll("OU=Groups", "").replaceAll("CN=", "")
				.replaceAll("", "").replaceAll("OU=", "");
		String arrayMember[] = adMembers.split(",");
		StringBuffer memberOf = new StringBuffer();
		int count = 0;
		for (String member : arrayMember) {
			if (ValidationUtil.isValid(member)) {
				memberOf.append("'" + member.trim().toUpperCase() + "'");
				if (count + 1 != arrayMember.length) {
					memberOf.append(",");
				}
			}
			count++;
		}
		String node = System.getenv("VISION_NODE_NAME");
		if (!ValidationUtil.isValid(node)) {
			node = "A1";
		}
		String sql = "WITH   VISION_USER_PROFILE_V1 AS  ( "
				+ " SELECT TRIM(VU.USER_GRP_PROFILE) VU_GRP_PROFILE FROM ETL_USER_VIEW VU WHERE VU.VISION_ID = '"
				+ visionId + "'  UNION ALL "
				+ " SELECT TRIM(VU.AUTO_GRP_PROFILE) VU_GRP_PROFILE FROM ETL_USER_VIEW VU WHERE VU.VISION_ID = '"
				+ visionId + "' AND VU.ALLOW_AUTO_PROFILE_FLAG = 'Y'  UNION ALL  SELECT VULINK.USER_GROUP"
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + "'-'" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null)
				+ "VULINK.USER_PROFILE VU_GRP_PROFILE "
				+ " FROM VISION_AD_PROFILE_LINK VULINK   WHERE UPPER(VULINK.AD_MEMBER_GROUP) IN (" + memberOf.toString()
				+ ")   AND (SELECT VU.ALLOW_AD_PROFILE_FLAG FROM ETL_USER_VIEW VU WHERE VU.VISION_ID = '"
				+ visionId + "') = 'Y'  AND VULINK.PROFILE_LINKER_STATUS = 0  ),  VISION_USER_PROFILE_V2 AS  ( "
				+ " SELECT LISTAGG(V1.VU_GRP_PROFILE,',') WITHIN GROUP (ORDER BY NULL) VU_GRP_PROFILE FROM VISION_USER_PROFILE_V1 V1 "
				+ " ),  VISION_USER_PROFILE_V3 AS  ( "
				+ " SELECT FN_RS_PARSESTRING(FN_RS_PARSESTRING(V2.VU_GRP_PROFILE,LEVEL,','),1,'-') VU_GRP, "
				+ " FN_RS_PARSESTRING(FN_RS_PARSESTRING(V2.VU_GRP_PROFILE,LEVEL,','),2,'-') VU_PROFILE "
				+ " FROM VISION_USER_PROFILE_V2 V2 "
				+ " CONNECT BY LEVEL <= (LENGTH(V2.VU_GRP_PROFILE)-LENGTH(REPLACE(V2.VU_GRP_PROFILE,',',''))+1) "
				+ " ),  VISION_USER_PROFILE_V4 AS  ( "
				+ " SELECT DISTINCT V3.VU_GRP,V3.VU_PROFILE FROM VISION_USER_PROFILE_V3 V3  ), "
				+ " VISION_USER_PROFILE AS  (  SELECT NST.NUM_SUBTAB_DESCRIPTION MENU_GROUP_DESC, "
				+ "       PP.MENU_GROUP,        VM.MENU_PROGRAM,        VM.PARENT_SEQUENCE, "
				+ "       VM.MENU_SEQUENCE,		VM.MENU_NAME, VM.SEPARATOR, "
				+ "       MAX(PP.MENU_ICON) MENU_ICON,        MAX(PP.P_ADD) P_ADD, "
				+ "       MAX(PP.P_MODIFY) P_MODIFY,        MAX(PP.P_DELETE) P_DELETE, "
				+ "       MAX(PP.P_INQUIRY) P_INQUIRY,        MAX(PP.P_VERIFICATION) P_VERIFICATION, "
				+ "       MAX(PP.P_EXCEL_UPLOAD) P_EXCEL_UPLOAD, "
				+ "       ROW_NUMBER() OVER (PARTITION BY VM.MENU_PROGRAM ORDER BY PP.MENU_GROUP) LOCAL_ROWNUM "
				+ " FROM VISION_USER_PROFILE_V4 V4,       PROFILE_PRIVILEGES PP,      VISION_MENU VM, "
				+ "     NUM_SUB_TAB NST  WHERE V4.VU_GRP = PP.USER_GROUP "
				+ "  AND V4.VU_PROFILE = PP.USER_PROFILE   AND NST.NUM_TAB = PP.MENU_GROUP_NT "
				+ "  AND NST.NUM_SUB_TAB = PP.MENU_GROUP   AND PP.MENU_GROUP = VM.MENU_GROUP "
				+ " and VM.MENU_STATUS = 0 " +
				// " AND VM.SEPARATOR !='Y' "+
				"  AND INSTR(','" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null)
				+ " "+ AbstractCommonDao.getDbFunction(Constants.NVL, null)+" (PP. EXCLUDE_MENU_PROGRAM_LIST,'@!XYZ!@')" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + "',',','"
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + "VM.MENU_PROGRAM" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null)
				+ "',') = 0   AND INSTR(','" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null)
				+ " "+ AbstractCommonDao.getDbFunction(Constants.NVL, null)+" (VM.MENU_NODE_VISIBILITY,'@!XYZ!@')" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + "',','," + node
				+ ",') > 0 "
				+ " GROUP BY NST.NUM_SUBTAB_DESCRIPTION,          PP.MENU_GROUP,          VM.MENU_PROGRAM, "
				+ "         VM.PARENT_SEQUENCE,          VM.MENU_SEQUENCE, "
				+ "		  VM.MENU_NAME, VM.SEPARATOR  ) "
				+ " SELECT * FROM VISION_USER_PROFILE VUP WHERE VUP.LOCAL_ROWNUM = 1 "
				+ " ORDER BY VUP.MENU_GROUP,VUP.PARENT_SEQUENCE,VUP.MENU_SEQUENCE";

		Object[] lParams = new Object[1];
		lParams[0] = visionId;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setMenuItem(rs.getString("MENU_GROUP_DESC"));
				profileData.setMenuGroup(rs.getInt("MENU_GROUP"));
				profileData.setMakerName(rs.getString("MENU_PROGRAM"));
				profileData.setVerifierName(rs.getString("MENU_NAME"));
				profileData.setMaker(rs.getInt("PARENT_SEQUENCE"));
				profileData.setVerifier(rs.getInt("MENU_SEQUENCE"));
				profileData.setProfileAdd(rs.getString("P_ADD"));
				profileData.setProfileModify(rs.getString("P_MODIFY"));
				profileData.setProfileDelete(rs.getString("P_DELETE"));
				profileData.setProfileInquiry(rs.getString("P_INQUIRY"));
				profileData.setProfileVerification(rs.getString("P_VERIFICATION"));
				profileData.setProfileUpload(rs.getString("P_EXCEL_UPLOAD"));
				profileData.setMenuIcon(rs.getString("MENU_ICON"));
				profileData.setDateCreation(rs.getString("SEPARATOR"));
				return profileData;
			}
		};
		List<ProfileData> profileData = getJdbcTemplate().query(sql, mapper);
		return profileData;
	}

	public static String getMacAddress(String ip) throws IOException {
		String address = null;
		String str = "";
		String macAddress = "";
		try {
			String cmd = "arp -a " + ip;
			Scanner s = new Scanner(Runtime.getRuntime().exec(cmd).getInputStream());
			Pattern pattern = Pattern
					.compile("(([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2})|(([0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4})");
			try {
				while (s.hasNext()) {
					str = s.next();
					Matcher matcher = pattern.matcher(str);
					if (matcher.matches()) {
						break;
					} else {
						str = null;
					}
				}
			} finally {
				s.close();
			}
			if (!ValidationUtil.isValid(str)) {
				return ip;
			}
			return (str != null) ? str.toUpperCase() : null;

			/*
			 * 
			 * InetAddress inetAddress = InetAddress.getByName(ip); NetworkInterface network
			 * = NetworkInterface.getByInetAddress(inetAddress.getLoopbackAddress()); byte[]
			 * mac = network.getHardwareAddress();
			 * 
			 * StringBuilder sb = new StringBuilder(); if(mac != null){ for (int i = 0; i <
			 * mac.length; i++) { sb.append(String.format("%02X%s", mac[i], (i < mac.length
			 * - 1) ? "-" : "")); } macAddress = sb.toString(); }
			 */
		} catch (Exception ex) {
			ex.printStackTrace();
			return ip;
		}
	}

	public ArrayList<MenuVb> getSubMenuItemsForMenuGroup(int menuGroup, String excludeMenuProgramList)
			throws DataAccessException {
		StringBuffer sql = new StringBuffer("SELECT * FROM VISION_MENU WHERE MENU_GROUP = ? AND MENU_STATUS = 0 ");
		String execludeMneu = "";
		if (excludeMenuProgramList.contains(",")) {
			String accounts[] = excludeMenuProgramList.split(",");
			int count = 0;
			for (String menuProgram : accounts) {
				if (ValidationUtil.isValid(menuProgram)) {
					// sql.append(" AND (ACCOUNT_NUMBER LIKE '%"+acctNumber+"%' OR ACCOUNT_NAME LIKE
					// '%"+acctNumber+"%') ");
					execludeMneu = execludeMneu + "'" + menuProgram + "' ";
					if (count + 1 != accounts.length) {
						execludeMneu = execludeMneu + ",";
					}
				}
				count++;
			}
			sql.append(" AND MENU_PROGRAM not in (" + execludeMneu + ")");
		} else if (ValidationUtil.isValid(excludeMenuProgramList)) {
			sql.append(" AND MENU_PROGRAM not in ('" + excludeMenuProgramList + "')");
		}
		sql.append(" ORDER BY PARENT_SEQUENCE, MENU_SEQUENCE");

		Object[] lParams = new Object[1];
		lParams[0] = menuGroup;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				MenuVb menuVb = new MenuVb();
				menuVb.setMenuProgram(rs.getString("MENU_PROGRAM"));
				menuVb.setMenuName(rs.getString("MENU_NAME"));
				menuVb.setMenuSequence(rs.getInt("MENU_SEQUENCE"));
				menuVb.setParentSequence(rs.getInt("PARENT_SEQUENCE"));
				menuVb.setSeparator(rs.getString("SEPARATOR"));
				menuVb.setMenuGroup(rs.getInt("MENU_GROUP"));
				menuVb.setMenuStatus(rs.getInt("MENU_STATUS"));
				menuVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				return menuVb;
			}
		};
		ArrayList<MenuVb> menuList = (ArrayList<MenuVb>) getJdbcTemplate().query(sql.toString(), lParams, mapper);
		return menuList;
	}

	public int updateWidgetCreationStagingTable(String tableName) {
		String sql = "UPDATE VWC_STAGGING_TABLE_LOGGING SET PROCESSED = 'Y' , DATE_LAST_MODIFIED = "
				+ AbstractCommonDao.getDbFunction(Constants.SYSDATE, null) + " WHERE TABLE_NAME = ? ";
		Object[] args = {tableName};
		return getJdbcTemplate().update(sql,args);
	}

	// This Function is Hard Coded for SBU Logic for Fidelity
	public String getVisionSbu(String parentVal) {
		String sbu = "";
		try {
			sbu = getJdbcTemplate().queryForObject(
					" SELECT DISTINCT  ''''" + AbstractCommonDao.getDbFunction(Constants.PIPELINE, null)
							+ " LISTAGG (VISION_SBU, ''',''') WITHIN GROUP (ORDER BY VISION_SBU)"
							+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " '''' "
							+ " Sbu FROM (SELECT DISTINCT VISION_SBU FROM VISION_SBU_MDM WHERE    VISION_SBU IN ("
							+ parentVal + ") OR PARENT_SBU IN (" + parentVal + ") OR DIVISON IN (" + parentVal + ") "
							+ " OR BANK_GROUP IN (" + parentVal + ") ) ",
					String.class);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				System.out.println("Error while getting SBU:" + e.getMessage());
				e.printStackTrace();
			}
			sbu = "";
		}
		return sbu;
	}

	public String getUserHomeDashboard(String userGroupProfile) {
		String homeDashboard = "NA";
		try {
			String query = " Select " + AbstractCommonDao.getDbFunction(Constants.NVL, null)
					+ " (Home_dashboard,'NA') from PRD_PROFILE_DASHBOARDS where "
					+ AbstractCommonDao.getDbFunction(Constants.NVL, null) + " (User_group, '')"
					+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + "'-'"
					+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " "
					+ AbstractCommonDao.getDbFunction(Constants.NVL, null)
					+ " (User_Profile, '') in (?) and Application_access = ? ";
			Object[] args = { userGroupProfile, productName };
			return getJdbcTemplate().queryForObject(query, String.class, args);
		} catch (Exception e) {
			return homeDashboard;
		}
	}

	public List getAllBusinessDate() {
		try {
			String query = "";
			if ("MSSQL".equalsIgnoreCase(databaseType)) {
				query = " SELECT COUNTRY+'-'+LE_BOOK as COUNTRY,  FORMAT(BUSINESS_DATE,'dd-MMM-yyyy') VBD, "
						+ " BUSINESS_YEAR_MONTH VBM,  FORMAT(BUSINESS_WEEK_DATE,'dd-MMM-yyyy') VBW, "
						+ " BUSINESS_QTR_YEAR_MONTH VBQ  FROM VISION_BUSINESS_DAY ";
			} else if ("ORACLE".equalsIgnoreCase(databaseType)) {
				query = " SELECT COUNTRY||'-'||LE_BOOK COUNTRY,  "+AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)+"(BUSINESS_DATE,'DD-Mon-RRRR') VBD, "
						+ " "+AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)+"(TO_DATE(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBM, "
						+ " "+AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)+"(BUSINESS_WEEK_DATE,'DD-Mon-RRRR') VBW, "
						+ " "+AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)+"(TO_DATE(BUSINESS_QTR_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBQ "
						+ " FROM VISION_BUSINESS_DAY WHERE  APPLICATION_ID='ETL' ";
			}
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataQuery(query);
			List resultlst = (List) exceptionCode.getResponse();
			return resultlst;
		} catch (Exception e) {
			return null;
		}
	}

	public ExceptionCode getReqdConnection(Connection conExt, String connectionName) {
		ExceptionCode exceptionCodeCon = new ExceptionCode();
		try {
			if (!ValidationUtil.isValid(connectionName) || "DEFAULT".equalsIgnoreCase(connectionName)) {
				conExt = commonApiDao.getConnection();
				exceptionCodeCon.setResponse(conExt);
			} else {
				String dbScript = getScriptValue(connectionName);
				if (!ValidationUtil.isValid(dbScript)) {
					exceptionCodeCon.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCodeCon.setErrorMsg("DB Connection Name is Invalid");
					return exceptionCodeCon;
				}
				exceptionCodeCon = CommonUtils.getConnection(dbScript);
			}
			exceptionCodeCon.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCodeCon.setErrorMsg(e.getMessage());
			exceptionCodeCon.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCodeCon;
	}

	public String getCurrentDateInfo(String option, String ctryLeBook) {
		String val = "";
		String sql = "";
		try {
			switch (option) {
			case "CYM":
				if ("ORACLE".equalsIgnoreCase(databaseType)) {
					sql = "SELECT TO_CHAR(To_Date(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') FROM VISION_BUSINESS_DAY "
							+ " WHERE COUNTRY||'-'||LE_BOOK = ? and APPLICATION_ID='ETL' ";
				} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
					sql = "SELECT BUSINESS_YEAR_MONTH   FROM VISION_BUSINESS_DAY  WHERE COUNTRY+'-'+LE_BOOK = ? and APPLICATION_ID='ETL'";
				}
				break;
			case "CY":
				if ("ORACLE".equalsIgnoreCase(databaseType)) {
					sql = "SELECT TO_CHAR(To_Date(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') FROM VISION_BUSINESS_DAY "
							+ " WHERE COUNTRY||'-'||LE_BOOK = ? and APPLICATION_ID='ETL' ";
				} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
					sql = "SELECT BUSINESS_YEAR_MONTH   FROM VISION_BUSINESS_DAY  WHERE COUNTRY+'-'+LE_BOOK = ? and APPLICATION_ID='ETL' ";
				}
				break;
			default:
				if ("ORACLE".equalsIgnoreCase(databaseType)) {
					sql = "SELECT TO_CHAR(To_Date(BUSINESS_DATE),'DD-Mon-RRRR') FROM VISION_BUSINESS_DAY "
							+ " WHERE COUNTRY||'-'||LE_BOOK = ? and APPLICATION_ID='ETL' ";
				} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
					sql = "SELECT FORMAT(BUSINESS_DATE,'dd-MMM-yyyy')  BUSINESS_DATE FROM VISION_BUSINESS_DAY WHERE COUNTRY+'-'+LE_BOOK = ? and APPLICATION_ID='ETL' ";
				}
				break;
			}
			String args[] = { ctryLeBook };
			val = getJdbcTemplate().queryForObject(sql, args, String.class);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return val;
	}

	
	public List getDateFormatforCaption() {
		try {
			String query = "";
			VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
			if ("MSSQL".equalsIgnoreCase(databaseType)) {
				query = " SELECT COUNTRY+'-'+LE_BOOK as COUNTRY,  FORMAT(BUSINESS_DATE,'dd-MMM-yyyy') VBD, "
						+ " BUSINESS_YEAR_MONTH VBM,  FORMAT(BUSINESS_WEEK_DATE,'dd-MMM-yyyy') VBW, "
						+ " BUSINESS_QTR_YEAR_MONTH VBQ  FROM VISION_BUSINESS_DAY WHERE  APPLICATION_ID='ETL'";
			} else if ("ORACLE".equalsIgnoreCase(databaseType)) {
				query = " SELECT COUNTRY||'-'||LE_BOOK COUNTRY,   "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(BUSINESS_DATE,'DD-Mon-RRRR') VBD,   "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(TO_DATE(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBM,   "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(BUSINESS_WEEK_DATE,'DD-Mon-RRRR') VBW,   "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(TO_DATE(BUSINESS_QTR_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBQ,   "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null) + "(BUSINESS_DATE,'RRRR') CY, "
						+ "  (select value from vision_variables where variable = 'CURRENT_MONTH') CM,   (SELECT "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(CURRENT_TIMESTAMP, 'HH24:MI:SS') FROM dual) SYSTIME, "
						+ "  (SELECT SYSDATE FROM DUAL) SYSTEMDATE,   "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(PREV_BUSINESS_DATE,'DD-Mon-RRRR') PVBD, " + // #VBD-1#
						"   " + AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(ADD_MONTHS(BUSINESS_DATE,-1),'DD-Mon-RRRR') PMVBD ,    "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(ADD_MONTHS(BUSINESS_DATE,-12),'DD-Mon-RRRR') PYVBD,    "
						+ AbstractCommonDao.getDbFunction(Constants.DATEFUNC, null)
						+ "(ADD_MONTHS(BUSINESS_DATE,-1)-1,'DD-Mon-RRRR') PMPVBD " + // #PMVBD-1#
						"  FROM VISION_BUSINESS_DAY   WHERE  APPLICATION_ID='ETL' AND COUNTRY = '"
						+ visionUsersVb.getCountry() + "' AND LE_BOOK = '" + visionUsersVb.getLeBook() + "'";
			}
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataQuery(query);
			List resultlst = (List) exceptionCode.getResponse();
			return resultlst;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}
	
	public String getRestrictionsByUsers(String screenName, String operation) throws DataAccessException {
		if (!ValidationUtil.isValid(screenName)) {
			return null;
		}
		VisionUsersVb usersVb = CustomContextHolder.getContext();
		String column = "P_ADD";
		if ("MODIFY".equalsIgnoreCase(operation.toUpperCase()))
			column = "P_MODIFY";
		if ("DELETE".equalsIgnoreCase(operation.toUpperCase()))
			column = "P_DELETE";
		if ("APPROVE".equalsIgnoreCase(operation.toUpperCase()) || "REJECT".equalsIgnoreCase(operation.toUpperCase()))
			column = "P_VERIFICATION";
		if ("DOWNLOAD".equalsIgnoreCase(operation.toUpperCase()))
			column = "P_DOWNLOAD";
		if ("UPLOAD".equalsIgnoreCase(operation.toUpperCase()))
			column = "P_EXCEL_UPLOAD";
		String sql = "SELECT " + column + " USER_RESTRINCTION FROM PRD_PROFILE_PRIVILEGES "
				+ "WHERE APPLICATION_ACCESS = ?  AND upper(SCREEN_NAME) = upper(?)  AND USER_GROUP = ? "
				+ " AND USER_PROFILE = ? ";
		Object[] lParams = new Object[4];
		lParams[0] = productName;
		lParams[1] = screenName;
		lParams[2] = usersVb.getUserGroup();
		lParams[3] = usersVb.getUserProfile();
		RowMapper mapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setMakerName(rs.getString("USER_RESTRINCTION"));
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = getJdbcTemplate().query(sql, lParams, mapper);
		if (commonVbs != null && !commonVbs.isEmpty()) {
			return commonVbs.get(0).getMakerName();
		}
		return null;
	}
	
	public String applyUserRestriction(String sqlQuery) {
		VisionUsersVb visionUserVb = CustomContextHolder.getContext();
		visionUserVb = getRestrictionInfo(visionUserVb);
		//VU_CLEB,VU_CLEB_AO,VU_CLEB_LV,VU_SBU,VU_PRODUCT,VU_OUC
		if(sqlQuery.contains("#VU_CLEB")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB(", visionUserVb.getCountry(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_CLEB_AO")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_AO(", visionUserVb.getAccountOfficer(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_CLEB_LV")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_LV(", visionUserVb.getLegalVehicle(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_LV_CLEB"))
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_LV_CLEB(", visionUserVb.getLegalVehicleCleb(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_SBU")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_SBU(", visionUserVb.getSbuCode(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_PRODUCT")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_PRODUCT(", visionUserVb.getProductAttribute(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_OUC")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_OUC(", visionUserVb.getOucAttribute(),visionUserVb.getUpdateRestriction());	
		/*if (sqlQuery.contains("#VU_CLEB_SOC_TL"))
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_SOC_TL(", visionUserVb.getClebTransline(),
					visionUserVb.getUpdateRestriction());
		if (sqlQuery.contains("#VU_CLEB_SOC_BL"))
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_SOC_BL(", visionUserVb.getClebBusinessline(),
					visionUserVb.getUpdateRestriction());
		if (sqlQuery.contains("#VU_CLEB_SOC")) {
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_SOC(", visionUserVb.getClebTrasnBusline(),
					visionUserVb.getUpdateRestriction());
		}*/
		if (sqlQuery.contains("#VU_CLEB_AOAS"))
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_AOAS(", visionUserVb.getAccountOfficer(),
					visionUserVb.getUpdateRestriction());
		/*if (sqlQuery.contains("#VU_OTHERS"))
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_OTHERS(", visionUserVb.getOtherAttr(),
					visionUserVb.getUpdateRestriction());*/
		return sqlQuery;
	}
	
	public String replacehashPrompt(String query, String restrictStr, String restrictVal, String updateRestriction) {
		try {
			String replaceStr = "";
			String orgSbuStr = StringUtils.substringBetween(query, restrictStr, ")#");
			if (ValidationUtil.isValid(orgSbuStr)) {
				if (orgSbuStr.contains("OR") && "Y".equalsIgnoreCase(updateRestriction) && ValidationUtil.isValid(restrictVal)) {
					if (orgSbuStr.contains("OR") && "Y".equalsIgnoreCase(updateRestriction)) {
						StringJoiner conditionjoiner = new StringJoiner(" OR ");
						String[] arrsplit = orgSbuStr.split("OR");
						for (String str : arrsplit) {
							String st = str + " IN (" + restrictVal + ")";
							conditionjoiner.add(st);
						}
						replaceStr = " AND (" + conditionjoiner + ")";
					}
				} else if ("Y".equalsIgnoreCase(updateRestriction) && ValidationUtil.isValid(restrictVal)) {
					replaceStr = " AND " + orgSbuStr + " IN (" + restrictVal + ")";
				}
				restrictStr = restrictStr.replace("(", "\\(");
				orgSbuStr = orgSbuStr.replace("|", "\\|");
				orgSbuStr = orgSbuStr.replace("+", "\\+");
				query = query.replaceAll(restrictStr + orgSbuStr + "\\)#", replaceStr);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return query;
	}

	public String[][] PromptSplit(String promptValue, String query, String twoD_arr[][], int ctr1) {
		String promptArrComma[] = promptValue.split(",");
		HashMap<String, String> prompt1Map = new HashMap<>();
		int ctr = 1;
		for (String str : promptArrComma) {
			prompt1Map.put("" + ctr, str);
			ctr++;
		}
		if (prompt1Map.size() > 0) {
			for (Map.Entry<String, String> set : prompt1Map.entrySet()) {
				String totalSplit[] = set.getValue().split("-");
				int ctr2 = 0;
				for (String str : totalSplit) {
					str = str.replaceAll("'", "");
					if (ValidationUtil.isValid(twoD_arr[ctr1][ctr2])) {
						twoD_arr[ctr1][ctr2] = twoD_arr[ctr1][ctr2] + ",'" + str + "'";
					} else {
						twoD_arr[ctr1][ctr2] = "'" + str + "'";
					}
					ctr2++;
				}
			}
		}
		return twoD_arr;
	}
	private VisionUsersVb getRestrictionInfo(VisionUsersVb vObject) {
		try {
			String restrictionXml = loginUserDao.getLoginUserXml();
			vObject.setCountry(CommonUtils.getValueForXmlTag(restrictionXml,"COUNTRY-LE_BOOK"));
			vObject.setAccountOfficer(CommonUtils.getValueForXmlTag(restrictionXml,"COUNTRY-LE_BOOK-ACCOUNT_OFFICER"));
			vObject.setLegalVehicle(CommonUtils.getValueForXmlTag(restrictionXml,"LEGAL_VEHICLE"));
			vObject.setLegalVehicleCleb(CommonUtils.getValueForXmlTag(restrictionXml,"LEGAL_VEHICLE-COUNTRY-LE_BOOK"));
			vObject.setOucAttribute(CommonUtils.getValueForXmlTag(restrictionXml,"OUC"));
			vObject.setProductAttribute(CommonUtils.getValueForXmlTag(restrictionXml,"PRODUCT"));
			vObject.setSbuCode(CommonUtils.getValueForXmlTag(restrictionXml,"SBU"));
			/*vObject.setClebTransline(CommonUtils.getValueForXmlTag(restrictionXml,"COUNTRY-LE_BOOK-TRANSLINE"));
			vObject.setClebTrasnBusline(CommonUtils.getValueForXmlTag(restrictionXml,"COUNTRY-LE_BOOK-TRANBUSLINE"));
			vObject.setClebBusinessline(CommonUtils.getValueForXmlTag(restrictionXml,"COUNTRY-LE_BOOK-BUSINESSLINE"));
			vObject.setOtherAttr(CommonUtils.getValueForXmlTag(restrictionXml,"OTHERS"));*/
		} catch (Exception e) {
			e.printStackTrace();
		}
		return vObject;
	}

	public static String getReferenceNumforAudit() {
		try {
			String strDay = "";
			String strMonth = "";
			String strYear = "";
			String strHour = "";
			String strMin = "";
			String strSec = "";
			String strMSec = "";
			String strAMPM = "";

			Calendar c = Calendar.getInstance();
			strMonth = c.get(Calendar.MONTH) + 1 + "";
			strDay = c.get(Calendar.DATE) + "";
			strYear = c.get(Calendar.YEAR) + "";
			strAMPM = c.get(Calendar.AM_PM) + "";
			strMin = c.get(Calendar.MINUTE) + "";
			strSec = c.get(Calendar.SECOND) + "";
			strMSec = c.get(Calendar.MILLISECOND) + "";

			if (strAMPM.equals("1"))
				strHour = (c.get(Calendar.HOUR) + 12) + "";
			else
				strHour = c.get(Calendar.HOUR) + "";

			if (strHour.length() == 1)
				strHour = "0" + strHour;

			if (strMin.length() == 1)
				strMin = "0" + strMin;

			if (strSec.length() == 1)
				strSec = "0" + strSec;

			if (strMSec.length() == 1)
				strMSec = "00" + strMSec;
			else if (strMSec.length() == 2)
				strMSec = "0" + strMSec;

			if (strMonth.length() == 1)
				strMonth = "0" + strMonth;

			if (strDay.length() == 1)
				strDay = "0" + strDay;

			return strYear + strMonth + strDay + strHour + strMin + strSec + strMSec;
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}

	public ArrayList getCommonResultDataQuery(CommonApiModel vObject) {
		try {
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataFetch(vObject);
			ArrayList resultlst = (ArrayList) exceptionCode.getResponse();
			return resultlst;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/* From Studio For PRD_PROFILE_PRIVILEGES_NEW
	 * 
	 * 
	 * 
	 * public List<ProfileData> getTopLevelMenu(VisionUsersVb visionUsersVb) throws DataAccessException {
		String sql = "Select Distinct MENU_GROUP_SEQ,MENU_GROUP_NAME,MENU_GROUP_ICON,MENU_PROGRAM, "
				+ " T2.P_ADD,T2.P_MODIFY,P_DELETE,P_INQUIRY,P_VERIFICATION,P_EXCEL_UPLOAD,P_DOWNLOAD,Menu_Display_Order "
				+ " from PRD_MENU_GROUP T1,PRD_PROFILE_PRIVILEGES_NEW T2  where T1.MENU_GROUP_Status = 0 "
				+ " and T1.Application_Access = ?  AND T2.USER_GROUP "
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " '-' "
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " T2.USER_PROFILE IN ("
				+ visionUsersVb.getUserGrpProfile() + ") AND T1.Application_Access = T2.Application_Access "
				+ " AND T1.MENU_GROUP_SEQ = T2.MENU_GROUP ORDER BY Menu_Display_Order ";
		Object[] lParams = new Object[1];
		lParams[0] = productName;
		// lParams[1] = visionUsersVb.getUserGrpProfile();
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setMenuItem(rs.getString("MENU_GROUP_NAME"));
				profileData.setMenuGroup(rs.getInt("MENU_GROUP_SEQ"));
				profileData.setMenuIcon(rs.getString("MENU_GROUP_ICON"));
				profileData.setMenuProgram(rs.getString("MENU_PROGRAM"));
				profileData.setProfileAdd(rs.getString("P_ADD"));
				profileData.setProfileModify(rs.getString("P_MODIFY"));
				profileData.setProfileDelete(rs.getString("P_DELETE"));
				profileData.setProfileView(rs.getString("P_INQUIRY"));
				profileData.setProfileVerification(rs.getString("P_VERIFICATION"));
				profileData.setProfileUpload(rs.getString("P_EXCEL_UPLOAD"));
				profileData.setProfileDownload(rs.getString("P_DOWNLOAD"));
				return profileData;
			}
		};
		List<ProfileData> profileData = getJdbcTemplate().query(sql, lParams, mapper);
		return profileData;
	}
	
	public ArrayList<MenuVb> getSubMenuItemsForSubMenuGroup(int menuGroup, int parentSequence, VisionUsersVb visionUsersVb)
			throws DataAccessException {
		String sql = " SELECT * FROM PRD_VISION_MENU t1,PRD_PROFILE_PRIVILEGES_NEW T2  WHERE "
				+ " T1.MENU_GROUP = ?  AND PARENT_SEQUENCE = ?  AND MENU_STATUS = 0  "
				+ " AND SEPARATOR = 'N'  AND T1.APPLICATION_ACCESS = ?  AND T1.MENU_GROUP = T2.MENU_GROUP "
				+ " AND T1.APPLICATION_ACCESS = T2.APPLICATION_ACCESS  AND T2.USER_GROUP "
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " '-' "
				+ AbstractCommonDao.getDbFunction(Constants.PIPELINE, null) + " T2.USER_PROFILE IN ("
				+ visionUsersVb.getUserGrpProfile() + ")  ORDER BY MENU_SEQUENCE ";
		
		Object[] lParams = new Object[3];
		lParams[0] = menuGroup;
		lParams[1] = parentSequence;
		lParams[2] = productName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				MenuVb menuVb = new MenuVb();
				menuVb.setMenuProgram(rs.getString("MENU_PROGRAM"));
				menuVb.setMenuName(rs.getString("MENU_NAME"));
				menuVb.setMenuSequence(rs.getInt("MENU_SEQUENCE"));
				menuVb.setParentSequence(rs.getInt("PARENT_SEQUENCE"));
				menuVb.setSeparator(rs.getString("SEPARATOR"));
				menuVb.setMenuGroup(rs.getInt("MENU_GROUP"));
				menuVb.setMenuStatus(rs.getInt("MENU_STATUS"));
				menuVb.setProfileAdd(rs.getString("P_ADD"));
				menuVb.setProfileModify(rs.getString("P_MODIFY"));
				menuVb.setProfileDelete(rs.getString("P_DELETE"));
				menuVb.setProfileView(rs.getString("P_INQUIRY"));
				menuVb.setProfileVerification(rs.getString("P_VERIFICATION"));
				menuVb.setProfileUpload(rs.getString("P_EXCEL_UPLOAD"));
				menuVb.setProfileDownload(rs.getString("P_DOWNLOAD"));
				return menuVb;
			}
		};
		ArrayList<MenuVb> menuList = (ArrayList<MenuVb>) getJdbcTemplate().query(sql, lParams, mapper);
		return menuList;
	}*/
}