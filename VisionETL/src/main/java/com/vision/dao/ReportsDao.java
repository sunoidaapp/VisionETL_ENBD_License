package com.vision.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.ChartUtils;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.Paginationhelper;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.PrdQueryConfig;
import com.vision.vb.PromptTreeVb;
import com.vision.vb.RCReportFieldsVb;
import com.vision.vb.ReportFilterVb;
import com.vision.vb.ReportUserDefVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.VisionUsersVb;

@Component
public class ReportsDao extends AbstractDao<ReportsVb> implements ServletContextAware {
	private ServletContext servletContext;

	public void setServletContext(ServletContext arg0) {
		servletContext = arg0;
	}

	@Value("${app.productName}")
	private String productName;
	@Autowired
	CommonDao commonDao;
	@Autowired
	ChartUtils chartUtils;
	@Autowired
	CommonApiDao commonApiDao;
	@Value("${app.databaseType}")
	private String databaseType;
	
	final String themeXml = "<TABLE-WIDTH>97</TABLE-WIDTH>				   "
			+ "<TITLE-COLOR>1e9b6e</TITLE-COLOR>                           "
			+ "<HEAD>                                                      "
			+ "    <FONT-COLOR>3c4043</FONT-COLOR>                         "
			+ "    <FONT-SIZE>13</FONT-SIZE>                               "
			+ "    <FONT-WEIGHT>500</FONT-WEIGHT>                          "
			+ "    <FONT-STYLE>NORMAL</FONT-STYLE>                         "
			+ "    <BG-COLOR>FFFFFF</BG-COLOR>                             "
			+ "    <TABLE-BORDER-ENABLE>N</TABLE-BORDER-ENABLE>            "
			+ "    <TABLE-BORDER-COLOR></TABLE-BORDER-COLOR>               "
			+ "	   <TABLE-BORDER-PIXEL></TABLE-BORDER-PIXEL>               "
			+ "    <HEADER-BORDER-ENABLE>N</HEADER-BORDER-ENABLE>          "
			+ "    <HEADER-BORDER-COLOR></HEADER-BORDER-COLOR>             "
			+ "    <LINE-HEIGHT>24</LINE-HEIGHT>                           "
			+ "</HEAD>                                                     "
			+ "<BODY>                                                      "
			+ "    <FONT-COLOR>616161</FONT-COLOR>                         "
			+ "    <FONT-SIZE>13</FONT-SIZE>                               "
			+ "    <FONT-WEIGHT>NORMAL</FONT-WEIGHT>                       "
			+ "    <FONT-STYLE>NORMAL</FONT-STYLE>                         "
			+ "    <SUBTOTAL-FONT-COLOR>3c4044</SUBTOTAL-FONT-COLOR>       "
			+ "    <ROW-ALT>N</ROW-ALT>                                    "
			+ "    <ROW-BG-COLOR>FFFFFF</ROW-BG-COLOR>                     "
			+ "    <COL-ALT>N</COL-ALT>                                    "
			+ "    <COLUMN-BORDER-ENABLE>N</COLUMN-BORDER-ENABLE>          "
			+ "    <COLUMN-BORDER-COLOR></COLUMN-BORDER-COLOR>             "
			+ "    <ROW-BORDER-ENABLE>Y</ROW-BORDER-ENABLE>                "
			+ "    <ROW-BORDER-COLOR>E9E9E9</ROW-BORDER-COLOR>             "
			+ "    <ROW-BORDER-STYLE>solid</ROW-BORDER-STYLE>              "
			+ "    <LINE-HEIGHT>24</LINE-HEIGHT>                           "
			+ "</BODY>                                                     ";

	public List<ReportsVb> getReportList(String reportGroup, String applicationId) throws DataAccessException {
		VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
		String sql = "";
		try {
			if ("ORACLE".equalsIgnoreCase(databaseType)) {
				sql = " Select T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
						+ " (SELECT NVL(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
						+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
						+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,T2.SCALING_FACTOR, "
						+ "	(SELECT NVL(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
						+ " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
						+ ") REPORT_ORIENTATION  FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
						+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP||'-'||T1.USER_PROFILE = ? "
						+ " AND T2.APPLICATION_ID = ? AND T2.REPORT_GROUP = ? AND T2.STATUS = 0 AND T2.report_type != 'W'";
			} else {
				sql = " Select T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
						+ " (SELECT ISNULL(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
						+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
						+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,T2.SCALING_FACTOR, "
						+ "	(SELECT ISNULL(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
						+ " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
						+ ") REPORT_ORIENTATION  FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
						+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP+'-'+T1.USER_PROFILE = ? "
						+ " AND T2.APPLICATION_ID = ? AND T2.REPORT_GROUP = ? AND T2.STATUS = 0 AND T2.report_type != 'W'";
			}
			String orderBy = " ORDER BY REPORT_ORDER";
			sql = sql + orderBy;
			Object[] lParams = new Object[3];
			lParams[0] = visionUsersVb.getUserGrpProfile();
			lParams[1] = applicationId;
			lParams[2] = reportGroup;
			return getJdbcTemplate().query(sql, lParams, getReportListMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}

	}

	protected RowMapper getReportListMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setReportId(rs.getString("REPORT_ID"));
				vObject.setReportTitle(rs.getString("REPORT_TITLE"));
				vObject.setFilterFlag(rs.getString("FILTER_FLAG"));
				vObject.setFilterRefCode(rs.getString("FILTER_REF_CODE"));
				vObject.setApplyUserRestrct(rs.getString("APPLY_USER_RESTRCT"));
				vObject.setCurrentLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setNextLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setGroupingFlag(rs.getString("GROUPING_FLAG"));
				vObject.setMaxRecords(rs.getInt("MAX_PERPAGE"));
				vObject.setReportTypeAT(rs.getInt("Report_Type_AT"));
				vObject.setReportType(rs.getString("Report_Type"));
				vObject.setTemplateId(rs.getString("Template_ID"));
				vObject.setScalingFactor(rs.getString("SCALING_FACTOR"));
				vObject.setReportOrientation(rs.getString("REPORT_ORIENTATION"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<ReportFilterVb> getReportFilterDetail(String filterRefCode) {
		setServiceDefaults();
		List<ReportFilterVb> collTemp = null;
		try {
			String query = " SELECT FILTER_REF_CODE,FILTER_XML,USER_RESTRICTION_XML,USER_RESTRICT_FLAG FROM PRD_REPORT_FILTERS"
					+ " WHERE STATUS = 0 AND FILTER_REF_CODE = ? ";
			collTemp = getJdbcTemplate().query(query, getReportFilterMapper(), new Object[] { filterRefCode });
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getReportFilterMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportFilterVb vObject = new ReportFilterVb();
				vObject.setFilterRefCode(rs.getString("FILTER_REF_CODE"));
				vObject.setFilterRefXml(rs.getString("FILTER_XML"));
				vObject.setUserRestrictionXml(rs.getString("USER_RESTRICTION_XML"));
				vObject.setUserRestrictFlag(rs.getString("USER_RESTRICT_FLAG"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<ReportsVb> findReportCategory(String productId) throws DataAccessException {
		String sql = "SELECT DISTINCT REPORT_GROUP ALPHA_SUB_TAB,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "
				+ " ALPHA_TAB = 6016 AND ALPHA_SUB_TAB = REPORT_GROUP) ALPHA_SUBTAB_DESCRIPTION "
				+ " FROM PRD_REPORT_MASTER WHERE APPLICATION_ID = ? ORDER BY ALPHA_SUB_TAB ";
		Object[] lParams = new Object[1];
		lParams[0] = productId;
		return getJdbcTemplate().query(sql, lParams, getCategoryMapper());
	}

	private RowMapper getCategoryMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setReportCategory(rs.getString("ALPHA_SUB_TAB"));
				vObject.setCategoryDesc(rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}

	@SuppressWarnings("unchecked")
	public ExceptionCode extractReportData(ReportsVb vObj, Connection conExt) {
		ArrayList datalst = new ArrayList();
		ExceptionCode exceptionCode = new ExceptionCode();
		int totalRows = 0;
		Statement stmt1 = null;
		String resultFetchTable = "";
		String tmpTableOrg = String.valueOf("TMP" + System.currentTimeMillis()) + vObj.getSubReportId();
		String tmpTableGrp = String.valueOf("TMPG" + System.currentTimeMillis()) + vObj.getSubReportId();
		try {
			String sqlQuery = vObj.getFinalExeQuery();
			// totalRows = (int) vObj.getTotalRows();
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			String createTabScript = "SELECT * FROM (" + sqlQuery + ") TA1 ";
			Boolean formatTypeAvail = false;
			ResultSet rsFormatCnt = null;
			if ("ORACLE".equalsIgnoreCase(databaseType)) {
				totalRows = stmt1.executeUpdate("CREATE TABLE " + tmpTableOrg + " AS (" + createTabScript + ")");

				rsFormatCnt = stmt1.executeQuery("SELECT * FROM USER_TAB_COLS WHERE TABLE_NAME = '" + tmpTableOrg
						+ "' AND COLUMN_NAME = 'FORMAT_TYPE' ");

				while (rsFormatCnt.next()) {
					formatTypeAvail = true;
				}
			} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
				totalRows = stmt1.executeUpdate("Select * into " + tmpTableOrg + " FROM (" + createTabScript + ") A");

				rsFormatCnt = stmt1.executeQuery("SELECT * FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME = '"
						+ tmpTableOrg + "' AND COLUMN_NAME = 'FORMAT_TYPE' ");

				while (rsFormatCnt.next()) {
					formatTypeAvail = true;
				}
			}
			rsFormatCnt.close();

			resultFetchTable = tmpTableOrg;
			String formatTypeCond = "";
			if (formatTypeAvail && "Y".equalsIgnoreCase(vObj.getApplyGrouping()))
				formatTypeCond = "WHERE FORMAT_TYPE NOT IN ('S','FT') ";
			else
				formatTypeCond = "";
			if ("Y".equalsIgnoreCase(vObj.getApplyGrouping())) {
				String showMeasures = "";
				if (ValidationUtil.isValid(vObj.getShowMeasures())) {
					showMeasures = "," + vObj.getShowMeasures();
				} else
					showMeasures = "";

				String query = "SELECT " + vObj.getShowDimensions() + showMeasures + " FROM (" + tmpTableOrg + ") "
						+ formatTypeCond + " GROUP BY " + vObj.getShowDimensions();
				if ("ORACLE".equalsIgnoreCase(databaseType)) {
					totalRows = stmt1.executeUpdate("CREATE TABLE " + tmpTableGrp + " AS (" + query + ")");
				} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
					totalRows = stmt1.executeUpdate("Select * into " + tmpTableGrp + " FROM (" + query + ") A");
				}
				resultFetchTable = tmpTableGrp;
			}
			if (totalRows == 0) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found!!");
				return exceptionCode;
			}

			Paginationhelper<ReportsVb> paginationhelper = new Paginationhelper<ReportsVb>();
			/*
			 * Report Suite having the functionality of Pagination for every 5000 rows (Rows
			 * per page in parameterized in Vision_Variable
			 * PRD_REPORT_MAXROW/PRD_REPORT_MAXFETCH
			 */

			String sqlTempTable = "SELECT * FROM " + resultFetchTable + "";

			sqlQuery = paginationhelper.reportFetchPage(ValidationUtil.convertQuery(sqlTempTable, vObj.getSortField()),
					vObj.getCurrentPage(), vObj.getMaxRecords(), totalRows);

			ResultSet rsData = stmt1.executeQuery(sqlQuery);// Final Result Data Query Execution

			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			HashMap<String, String> columns = new HashMap<String, String>();
			ArrayList<String> collst = new ArrayList<>();
			Boolean columnHeaderFetch = false;
			int rowNum = 1;
			while (rsData.next()) {
				columns = new HashMap<String, String>();
				for (int cn = 1; cn <= colCount; cn++) {
					String columnName = metaData.getColumnName(cn);
					if ("DD_KEY_ID".equalsIgnoreCase(columnName.toUpperCase())) {
						columns.put("DDKEYID", rsData.getString(columnName));
					} else {
						columns.put(columnName.toUpperCase(), rsData.getString(columnName));
					}
					if (!columnHeaderFetch)
						collst.add(columnName.toUpperCase());
				}
				columnHeaderFetch = true;
				if (!columnHeaderFetch) {
					StringJoiner missingColumns = new StringJoiner(",");
					vObj.getColumnHeaderslst().forEach(colHeadersDataVb -> {
						if (!collst.contains(colHeadersDataVb.getDbColumnName())) {
							missingColumns.add(colHeadersDataVb.getDbColumnName());
						}
					});
					if (ValidationUtil.isValid(missingColumns.toString())) {
						exceptionCode.setErrorMsg(
								missingColumns + "these Source columns are not maintained  in the Result set");
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						break;
					}
				}
				columns.put("INDEXING", "" + rowNum);// This is only for front-end scroll purpose not used anywhere in
														// report.
				datalst.add(columns);
				if (rsData.getRow() == vObj.getMaxRecords())
					break;

				columnHeaderFetch = true;
				rowNum++;
			}
			rsData.close();
			// HashMap<String,String> columns =
			// (HashMap<String,String>)getJdbcTemplate().query(sqlQuery, mapper);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(datalst);
			exceptionCode.setRequest(totalRows);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			if (!ValidationUtil.isValid(exceptionCode.getErrorMsg())) {
				exceptionCode.setErrorMsg(e.getMessage());
			}
			return exceptionCode;
		} finally {
			try {
				if ("ORACLE".equalsIgnoreCase(databaseType)) {
					stmt1.executeUpdate("DROP TABLE " + tmpTableOrg + " PURGE ");
					if ("Y".equalsIgnoreCase(vObj.getApplyGrouping()))
						stmt1.executeUpdate("DROP TABLE " + tmpTableGrp + " PURGE ");
				} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
					stmt1.executeUpdate("DROP TABLE " + tmpTableOrg);
					if ("Y".equalsIgnoreCase(vObj.getApplyGrouping()))
						stmt1.executeUpdate("DROP TABLE " + tmpTableGrp);
				}
				stmt1.close();
			} catch (SQLException e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
			}
		}
		return exceptionCode;
	}

	public List<ReportsVb> getSubReportDetail(ReportsVb dObj) {
		List<ReportsVb> collTemp = null;
		try {
			/*
			 * String query =
			 * "SELECT REPORT_ID,SUBREPORT_SEQ,SUB_REPORT_ID,DATA_REF_ID,COUNT_FETCH_FLAG,DD_FLAG, "
			 * + "REPORT_ORIENTATION,PDF_GROUP_COLUMNS,PARENT_SUBREPORT_ID,"+commonDao.
			 * getDbFunction(""+getDbFunction("NVL")+"")
			 * +"(PDF_WIDTH_GRPERCENT,0) PDF_WIDTH_GRPERCENT FROM PRD_REPORT_DETAILS WHERE SUBREPORT_SEQ = ? AND REPORT_ID = ? "
			 * + "AND STATUS = 0";
			 */

			String query = "SELECT REPORT_ID,SUBREPORT_SEQ,SUB_REPORT_ID,DATA_REF_ID,COUNT_FETCH_FLAG,DD_FLAG, "
					+ "REPORT_ORIENTATION,PDF_GROUP_COLUMNS,PARENT_SUBREPORT_ID,SORTING_ENABLE,SEARCH_ENABLE,"
					+ getDbFunction(Constants.NVL, null) + "(PDF_WIDTH_GRPERCENT,0) PDF_WIDTH_GRPERCENT,"
					+ "(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "
					+ " ALPHA_TAB = 7005 AND ALPHA_SUB_TAB = T1.CHART_TYPE) CHART_TYPE,OBJECT_TYPE,REPORT_HELP,REPORT_DESIGN_XML "
					+ " FROM PRD_REPORT_DETAILS T1 WHERE SUBREPORT_SEQ = ? AND REPORT_ID = ? AND STATUS = 0";

			Object[] lParams = new Object[2];
			lParams[0] = dObj.getNextLevel();
			lParams[1] = dObj.getReportId();
			collTemp = getJdbcTemplate().query(query, lParams, getSubReportsMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getSubReportsMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb reportsVb = new ReportsVb();
				reportsVb.setReportId(rs.getString("REPORT_ID"));
				reportsVb.setIntReportSeq(rs.getInt("SUBREPORT_SEQ"));
				reportsVb.setCurrentLevel(rs.getString("SUBREPORT_SEQ").replaceAll(".0", ""));
				reportsVb.setSubReportId(rs.getString("SUB_REPORT_ID"));
				reportsVb.setDataRefId(rs.getString("DATA_REF_ID"));
				reportsVb.setFetchFlag(rs.getString("COUNT_FETCH_FLAG"));
				reportsVb.setDdFlag(rs.getString("DD_FLAG"));
				reportsVb.setReportOrientation(
						ValidationUtil.isValid(rs.getString("REPORT_ORIENTATION")) ? rs.getString("REPORT_ORIENTATION")
								: "P");
				reportsVb.setPdfGroupColumn(rs.getString("PDF_GROUP_COLUMNS"));
				reportsVb.setParentSubReportID(rs.getString("PARENT_SUBREPORT_ID"));
				reportsVb.setSortFlag(rs.getString("SORTING_ENABLE"));
				reportsVb.setSearchFlag(rs.getString("SEARCH_ENABLE"));
				reportsVb.setPdfGrwthPercent(rs.getInt("PDF_WIDTH_GRPERCENT"));
				reportsVb.setChartType(rs.getString("CHART_TYPE"));
				reportsVb.setObjectType(rs.getString("OBJECT_TYPE"));
				reportsVb.setReportInfo(rs.getString("REPORT_HELP"));
				if (ValidationUtil.isValid(rs.getString("REPORT_DESIGN_XML"))) {
					JSONObject xmlJSONObj = XML.toJSONObject(rs.getString("REPORT_DESIGN_XML"));
					int PRETTY_PRINT_INDENT_FACTOR = 4;
					String resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
					reportsVb.setReportDesignXml(resultData);
				}

				return reportsVb;
			}
		};
		return mapper;
	}

	public String getNextLevel(ReportsVb dObj) {
		String nextLevel = "";
		String query = "";
		try {
			query = " SELECT " + getDbFunction(Constants.NVL, null)
					+ "(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE SUBREPORT_SEQ > ? "
					+ " AND REPORT_ID = ?  ";
			Object[] lParams = new Object[2];
			lParams[0] = dObj.getCurrentLevel();
			lParams[1] = dObj.getReportId();
			nextLevel = getJdbcTemplate().queryForObject(query, lParams, String.class);
			return nextLevel;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return "0";
		}
	}

	public List<ColumnHeadersVb> getReportColumns(ReportsVb dObj) {
		List<ColumnHeadersVb> collTemp = null;
		try {
			String query = " Select REPORT_ID,SUB_REPORT_ID,COLUMN_XML from PRD_REPORT_COLUMN "
					+ " WHERE REPORT_ID = ? AND SUB_REPORT_ID = ?";

			Object[] lParams = new Object[2];
			lParams[0] = dObj.getReportId();
			lParams[1] = dObj.getSubReportId();
			collTemp = getJdbcTemplate().query(query, lParams, getReportColumnHeadersMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getReportColumnHeadersMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ColumnHeadersVb columnHeadersVb = new ColumnHeadersVb();
				columnHeadersVb.setReportId(rs.getString("REPORT_ID"));
				columnHeadersVb.setSubReportId(rs.getString("SUB_REPORT_ID"));
				columnHeadersVb.setColumnXml(rs.getString("COLUMN_XML"));
				return columnHeadersVb;
			}
		};
		return mapper;
	}

	public List<PrdQueryConfig> getSqlQuery(String dataRefId) {
		List<PrdQueryConfig> collTemp = null;
		try {
			String query = "SELECT DATA_REF_ID, QUERY, DATA_REF_TYPE,DB_CONNECTION_NAME from PRD_QUERY_CONFIG "
					+ "WHERE DATA_REF_ID = ? AND STATUS = 0 ";

			Object[] lParams = new Object[1];
			lParams[0] = dataRefId;
			collTemp = getJdbcTemplate().query(query, lParams, getSqlQueryMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getSqlQueryMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				PrdQueryConfig prdQueryConfig = new PrdQueryConfig();
				prdQueryConfig.setDataRefId(rs.getString("DATA_REF_ID"));
				prdQueryConfig.setQueryProc(rs.getString("QUERY"));
				prdQueryConfig.setDataRefType(rs.getString("DATA_REF_TYPE"));
				prdQueryConfig.setDbConnectionName(rs.getString("DB_CONNECTION_NAME"));
				return prdQueryConfig;
			}
		};
		return mapper;
	}

	public int insertReportsAudit(ReportsVb dObj, String auditMsg, String type) {
		VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
		String referenceId = CommonDao.getReferenceNumforAudit();
		String promptXml = convertToXml(dObj);
		if (!ValidationUtil.isValid(promptXml))
			promptXml = "";
		int retVal = 0;
		String query = "";
		try {
			query = "Insert Into PRD_AUDIT_REPORTS (REFERENCE_NO,REPORT_ID, PROMPT_XML,"
					+ " RUN_DATE ,AUDIT_MESSAGE,USER_LOGIN_ID,VISION_ID,IP_ADDRESS,	 MAC_ADDRESS,HOST_NAME,TYPE) "
					+ " Values (?,?,?," + getDbFunction(Constants.SYSDATE, null) + ",?,?,?,?,?,?,?)";
			Object[] args = { referenceId, dObj.getReportId(), promptXml, auditMsg, visionUsersVb.getUserLoginId(),
					visionUsersVb.getVisionId(), visionUsersVb.getIpAddress(), visionUsersVb.getMacAddress(),
					visionUsersVb.getRemoteHostName(), type };
			retVal = getJdbcTemplate().update(query, args);
			return retVal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	public ExceptionCode callProcforReportData(ReportsVb vObject, String procedure) {
		setServiceDefaults();
		strCurrentOperation = "Query";
		strErrorDesc = "";
		Connection conExt = null;
		CallableStatement cs = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		PromptTreeVb promptTreeVb = new PromptTreeVb();
		try {
			if (!ValidationUtil.isValid(vObject.getDbConnection())
					|| "DEFAULT".equalsIgnoreCase(vObject.getDbConnection())) {
				conExt = getConnection();
			} else {
				String dbScript = commonDao.getScriptValue(vObject.getDbConnection());
				ExceptionCode exceptionCodeCon = CommonUtils.getConnection(dbScript);
				if (exceptionCodeCon != null && exceptionCodeCon.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					conExt = (Connection) exceptionCodeCon.getResponse();
				} else {
					exceptionCode = exceptionCodeCon;
					return exceptionCode;
				}
			}
			if (!ValidationUtil.isValid(procedure)) {
				strErrorDesc = "Invalid Procedure in PRD_QUERY_CONFIG table for report Id[" + vObject.getReportId()
						+ "].";
				throw buildRuntimeCustomException(getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION));
			}
			String sessionId = String.valueOf(System.currentTimeMillis()) + vObject.getCurrentLevel().substring(0, 1);
			/*
			 * System.out.println("Date:" + new Date() + "ReportID[" + vObject.getReportId()
			 * + "]SubReport[" + vObject.getSubReportId() + "]Visionid[" + intCurrentUserId
			 * + "]Session[" + sessionId + "]");
			 */
			cs = conExt.prepareCall("{call " + procedure + "}");
			/*
			 * cs.setString(1, String.valueOf(vObject.getReportId())); cs.setString(2,
			 * String.valueOf(vObject.getSubReportId())); cs.setString(3,
			 * String.valueOf(intCurrentUserId)); cs.setString(4, sessionId);
			 * cs.setString(5,vObject.getScalingFactor()); cs.setString(6,
			 * vObject.getPromptValue1().replaceAll("'", "")); cs.setString(7,
			 * vObject.getPromptValue2().replaceAll("'", "")); cs.setString(8,
			 * vObject.getPromptValue3().replaceAll("'", "")); cs.setString(9,
			 * vObject.getPromptValue4().replaceAll("'", "")); cs.setString(10,
			 * vObject.getPromptValue5().replaceAll("'", "")); cs.setString(11,
			 * vObject.getPromptValue6().replaceAll("'", "")); cs.setString(12,
			 * vObject.getPromptValue7().replaceAll("'", "")); cs.setString(13,
			 * vObject.getPromptValue8().replaceAll("'", "")); cs.setString(14,
			 * vObject.getPromptValue9().replaceAll("'", "")); cs.setString(15,
			 * vObject.getPromptValue10().replaceAll("'", "")); cs.setString(16,
			 * vObject.getDrillDownKey1().replaceAll("'", "")); cs.setString(17,
			 * vObject.getDrillDownKey2().replaceAll("'", "")); cs.setString(18,
			 * vObject.getDrillDownKey3().replaceAll("'", "")); cs.setString(19,
			 * vObject.getDrillDownKey4().replaceAll("'", "")); cs.setString(20,
			 * vObject.getDrillDownKey5().replaceAll("'", "")); cs.setString(21,
			 * vObject.getDrillDownKey6().replaceAll("'", "")); cs.setString(22,
			 * vObject.getDrillDownKey7().replaceAll("'", "")); cs.setString(23,
			 * vObject.getDrillDownKey8().replaceAll("'", "")); cs.setString(24,
			 * vObject.getDrillDownKey9().replaceAll("'", "")); cs.setString(25,
			 * vObject.getDrillDownKey10().replaceAll("'", "")); cs.setString(26,
			 * vObject.getDrillDownKey0().replaceAll("'", ""));
			 */
			cs.registerOutParameter(1, java.sql.Types.VARCHAR); // Status
			cs.registerOutParameter(2, java.sql.Types.VARCHAR); // Error Message
			cs.registerOutParameter(3, java.sql.Types.VARCHAR); // Table Name
			cs.registerOutParameter(4, java.sql.Types.VARCHAR); // Column Headers
			cs.execute();
			exceptionCode.setErrorCode(cs.getInt(1));
			exceptionCode.setErrorMsg(cs.getString(2));
			promptTreeVb.setTableName(cs.getString(3));
			promptTreeVb.setColumnHeaderTable(cs.getString(4));
			promptTreeVb.setSessionId(vObject.getDateCreation());
			promptTreeVb.setReportId(vObject.getSubReportId());
			exceptionCode.setResponse(promptTreeVb);
			cs.close();
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setErrorCode(Constants.PASSIVATE);
			return exceptionCode;
		} finally {
			JdbcUtils.closeStatement(cs);
			try {
				conExt.close();
			} catch (SQLException e) {
				// e.printStackTrace();
			}
			// DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
		return exceptionCode;
	}

	public void deleteTempTable(String tableName) {
		try {
			String query = " DROP TABLE " + tableName + " PURGE ";
			getJdbcTemplate().update(query);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		}
	}

	private String convertToXml(ReportsVb vObject) {
		String promptsXml = "";
		promptsXml = "<Prompts>";
		String promptvalue1 = "<Prompt1>" + vObject.getPromptValue1() + "</Prompt1>";
		promptsXml = promptsXml + promptvalue1;
		String promptvalue2 = "<Prompt2>" + vObject.getPromptValue2() + "</Prompt2>";
		promptsXml = promptsXml + promptvalue2;
		String promptvalue3 = "<Prompt3>" + vObject.getPromptValue3() + "</Prompt3>";
		promptsXml = promptsXml + promptvalue3;
		String promptvalue4 = "<Prompt4>" + vObject.getPromptValue4() + "</Prompt4>";
		promptsXml = promptsXml + promptvalue4;
		String promptvalue5 = "<Prompt5>" + vObject.getPromptValue5() + "</Prompt5>";
		promptsXml = promptsXml + promptvalue5;
		String promptvalue6 = "<Prompt6>" + vObject.getPromptValue6() + "</Prompt6>";
		promptsXml = promptsXml + promptvalue6;
		String promptvalue7 = "<Prompt7>" + vObject.getPromptValue7() + "</Prompt7>";
		promptsXml = promptsXml + promptvalue7;
		String promptvalue8 = "<Prompt8>" + vObject.getPromptValue8() + "</Prompt8>";
		promptsXml = promptsXml + promptvalue8;
		String promptvalue9 = "<Prompt9>" + vObject.getPromptValue9() + "</Prompt9>";
		promptsXml = promptsXml + promptvalue9;
		String promptvalue10 = "<Prompt10>" + vObject.getPromptValue10() + "</Prompt10>";
		promptsXml = promptsXml + promptvalue10;
		promptsXml = promptsXml + "</Prompts>";
		return promptsXml;
	}

	public List<ReportsVb> getReportsDetail(ReportsVb dObj) {
		List<ReportsVb> collTemp = null;
		try {
			String query = " SELECT * FROM (SELECT REPORT_ID,SUBREPORT_SEQ,SUB_REPORT_ID,DATA_REF_ID,COUNT_FETCH_FLAG,DD_FLAG,"
					+ " REPORT_ORIENTATION, PARENT_SUBREPORT_ID, OBJECT_TYPE_AT, OBJECT_TYPE,"
					+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "
					+ " ALPHA_TAB = 7005 AND ALPHA_SUB_TAB = CHART_TYPE) CHART_TYPE, CHART_TYPE_AT, "
					+ " CASE WHEN SUB_REPORT_ID != PARENT_SUBREPORT_ID THEN 'Y'"
					// + " (SELECT DD_FLAG FROM PRD_REPORT_DETAILS S2 WHERE T1.PARENT_SUBREPORT_ID =
					// S2.SUB_REPORT_ID AND T1.PARENT_SUBREPORT_ID = S2.REPORT_ID) "
					+ " ELSE 'N' END ISDRILLDOWN "
					+ " FROM PRD_REPORT_DETAILS T1 WHERE REPORT_ID = ? ) A1 WHERE A1.ISDRILLDOWN = 'N' "
					+ " ORDER BY A1.SUB_REPORT_ID";
			Object[] lParams = new Object[1];
			lParams[0] = dObj.getReportId();
			// lParams[1] = dObj.getReportId();
			collTemp = getJdbcTemplate().query(query, lParams, getInteractiveReportsMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getInteractiveReportsMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb reportsVb = new ReportsVb();
				reportsVb.setReportId(rs.getString("REPORT_ID"));
				reportsVb.setIntReportSeq(rs.getInt("SUBREPORT_SEQ"));
				reportsVb.setSubReportId(rs.getString("SUB_REPORT_ID"));
				reportsVb.setDataRefId(rs.getString("DATA_REF_ID"));
				reportsVb.setFetchFlag(rs.getString("COUNT_FETCH_FLAG"));
				reportsVb.setDdFlag(rs.getString("DD_FLAG"));
				reportsVb.setReportOrientation(rs.getString("REPORT_ORIENTATION"));
				reportsVb.setParentSubReportID(rs.getString("PARENT_SUBREPORT_ID"));
				reportsVb.setObjectTypeAT(rs.getInt("OBJECT_TYPE_AT"));
				reportsVb.setObjectType(rs.getString("OBJECT_TYPE"));
				reportsVb.setChartType(rs.getString("CHART_TYPE"));
				reportsVb.setChartTypeAT(rs.getInt("CHART_TYPE_AT"));
				reportsVb.setCurrentLevel(rs.getString("SUBREPORT_SEQ").replaceAll(".0", ""));
				reportsVb.setNextLevel(rs.getString("SUBREPORT_SEQ").replaceAll(".0", ""));
				return reportsVb;
			}
		};
		return mapper;
	}

	public ExceptionCode getChartReportData(ReportsVb vObject, String orginalQuery, Connection conExt)
			throws SQLException {
		ExceptionCode exceptionCode = new ExceptionCode();
		Statement stmt = null;
		List collTemp = new ArrayList();
		ResultSet rs = null;
		try {
			stmt = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(orginalQuery);
			/*
			 * CachedRowSetImpl rsChild = new CachedRowSetImpl(); rsChild = new
			 * CachedRowSetImpl(); rsChild.populate(rs);
			 */

			CachedRowSet rsChild = RowSetProvider.newFactory().createCachedRowSet();
			rsChild.populate(rs);
			exceptionCode = chartUtils.getChartXML(vObject.getChartType(), vObject.getColHeaderXml(), rs, rsChild,
					vObject.getWidgetTheme());
			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			String chartResultXml = exceptionCode.getResponse().toString();
			if (ValidationUtil.isValid(chartResultXml)) {
				chartResultXml = replaceTagValues(chartResultXml);
			}
			exceptionCode.setResponse(chartResultXml);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			if (!ValidationUtil.isValid(exceptionCode.getErrorMsg())) {
				exceptionCode.setErrorMsg(e.getMessage());
			}
			return exceptionCode;
		}
	}

	public String getIntReportNextLevel(ReportsVb dObj) {
		String nextLevel = "";
		String query = "";
		try {
			query = " SELECT " + getDbFunction(Constants.NVL, null)
					+ "(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE SUBREPORT_SEQ > ? "
					+ " AND REPORT_ID = ?  ";

			Object[] lParams = new Object[2];
			lParams[0] = dObj.getCurrentLevel();
			lParams[1] = dObj.getReportId();
			// lParams[2] = dObj.getSubReportId();
			nextLevel = getJdbcTemplate().queryForObject(query, lParams, String.class);
			return nextLevel;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return "0";
		}
	}

	public String getDateFormat(String promptDate, String format) {
		setServiceDefaults();
		String query = "";
		String fromDual = getDbFunction(Constants.DUAL, null);

		if ("PYM".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),-1),'RRRRMM') ";
		} else if ("NYM".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),+1),'RRRRMM') ";
		} else if ("PM".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),-1),'MM') ";
		} else if ("NM".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),+1),'MM') ";
		} else if ("CM".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),0),'MM') ";
		} else if ("CY".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),0),'RRRR') ";
		} else if ("PY".equalsIgnoreCase(format)) {
			query = "Select " + getDbFunction(Constants.DATEFUNC, null) + "(Add_Months(To_Date(" + promptDate
					+ ",'RRRRMM'),-12),'RRRR') ";
		}
		if (ValidationUtil.isValid(fromDual)) {
			query = query + fromDual;
		}

		try {
			String i = getJdbcTemplate().queryForObject(query, null, String.class);
			return i;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return "";
		}
	}

	@Override
	protected void setServiceDefaults() {
		serviceName = "ReportsDao";
		serviceDesc = "ReportsDao";
		tableName = "PRD_REPORTS";
		childTableName = "PRD_REPORTS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	public ExceptionCode getTreePromptData(ReportFilterVb vObject, PrdQueryConfig prdQueryConfigVb) {
		setServiceDefaults();
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection conExt = null;
		List<PromptTreeVb> tempPromptsList = new ArrayList<PromptTreeVb>();
		Statement stmt1 = null;
		ResultSet rs1 = null;
		try {
			ExceptionCode exConnection = commonDao.getReqdConnection(conExt, prdQueryConfigVb.getDbConnectionName());
			if (exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection) exConnection.getResponse();
			} else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String query = "";
			String sessionId = String.valueOf(System.currentTimeMillis());
			CallableStatement cs = null;
			cs = conExt.prepareCall("{call " + prdQueryConfigVb.getQueryProc() + "}");
			cs.setString(1, "" + intCurrentUserId);
			cs.setString(2, sessionId);
			cs.setString(3, vObject.getFilterSourceId());
			cs.registerOutParameter(4, java.sql.Types.VARCHAR);// filterString
			cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Status
			cs.registerOutParameter(6, java.sql.Types.VARCHAR); // Error Message
			ResultSet rs = cs.executeQuery();
			PromptTreeVb promptTreeVb = new PromptTreeVb();
			promptTreeVb.setFilterString(cs.getString(4));
			promptTreeVb.setStatus(cs.getString(5));
			promptTreeVb.setErrorMessage(cs.getString(6));
			rs.close();
			if (promptTreeVb != null && "0".equalsIgnoreCase(promptTreeVb.getStatus())) {
				vObject.setFilterString(promptTreeVb.getFilterString());
				query = "SELECT FIELD_1, FIELD_2, FIELD_3, FIELD_4, PROMPT_ID FROM PROMPTS_STG WHERE VISION_ID = '"
						+ intCurrentUserId + "' AND SESSION_ID= '" + sessionId + "' AND PROMPT_ID = '"
						+ vObject.getFilterSourceId() + "' ";
				if (ValidationUtil.isValid(promptTreeVb.getFilterString())) {
					query = query + " " + promptTreeVb.getFilterString();
				}
				rs1 = stmt1.executeQuery(query);
				while (rs1.next()) {
					PromptTreeVb Obj = new PromptTreeVb();
					Obj.setField1(rs1.getString("FIELD_1"));
					Obj.setField2(rs1.getString("FIELD_2"));
					Obj.setField3(rs1.getString("FIELD_3"));
					Obj.setField4(rs1.getString("FIELD_4"));
					Obj.setPromptId(rs1.getString("PROMPT_ID"));
					tempPromptsList.add(Obj);
				}
				query = "DELETE FROM PROMPTS_STG WHERE VISION_ID = '" + intCurrentUserId + "' AND SESSION_ID= '"
						+ sessionId + "' AND PROMPT_ID = '" + vObject.getFilterSourceId() + "' ";
				stmt1.executeUpdate(query);
			}
			if (tempPromptsList == null && tempPromptsList.size() == 0) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No data found");
				return exceptionCode;
			}
			// return tempPromptsList;
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(tempPromptsList);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		} finally {
			try {
				if (stmt1 != null)
					stmt1.close();
				if (conExt != null)
					conExt.close();
				if (rs1 != null)
					rs1.close();
			} catch (SQLException e) {
				// e.printStackTrace();
			}
		}
		return exceptionCode;

	}

	private RowMapper getPromptTreeMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				PromptTreeVb promptTreeVb = new PromptTreeVb();
				promptTreeVb.setField1(rs.getString("FIELD_1"));
				promptTreeVb.setField2(rs.getString("FIELD_2"));
				promptTreeVb.setField3(rs.getString("FIELD_3"));
				promptTreeVb.setField4(rs.getString("FIELD_4"));
				promptTreeVb.setPromptId(rs.getString("PROMPT_ID"));
				return promptTreeVb;
			}
		};
		return mapper;
	}

	class TreePromptCallableStatement implements CallableStatementCreator, CallableStatementCallback {
		private ReportFilterVb vObject = null;
		private String currentTimeAsSessionId = null;
		private String visionId = null;
		private String procSrc = null;
		private Connection conExt = null;

		public TreePromptCallableStatement(ReportFilterVb vObject, String currentTimeAsSessionId, String visionId,
				String procSrc, Connection conExt) {
			this.vObject = vObject;
			this.currentTimeAsSessionId = currentTimeAsSessionId;
			this.visionId = visionId;
			this.procSrc = procSrc;
			this.conExt = conExt;
		}

		public CallableStatement createCallableStatement(Connection connection) throws SQLException {
			CallableStatement cs = connection.prepareCall("{call " + procSrc + "}");
			cs.setString(1, visionId);
			cs.setString(2, currentTimeAsSessionId);
			cs.setString(3, vObject.getFilterSourceId());
			cs.registerOutParameter(4, java.sql.Types.VARCHAR);// filterString
			cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Status
			cs.registerOutParameter(6, java.sql.Types.VARCHAR); // Error Message
			return cs;
		}

		public Object doInCallableStatement(CallableStatement cs) throws SQLException, DataAccessException {
			ResultSet rs = cs.executeQuery();
			PromptTreeVb promptTreeVb = new PromptTreeVb();
			promptTreeVb.setFilterString(cs.getString(4));
			promptTreeVb.setStatus(cs.getString(5));
			promptTreeVb.setErrorMessage(cs.getString(6));
			rs.close();
			return promptTreeVb;
		}
	}

	public List<ColumnHeadersVb> getColumnHeaderFromTable(PromptTreeVb promptTreeVb) {
		setServiceDefaults();
		int intKeyFieldsCount = 3;
		String query = new String(
				"SELECT REPORT_ID, SESSION_ID, LABEL_ROW_NUM, LABEL_COL_NUM, CAPTION, COLUMN_WIDTH, COL_TYPE, ROW_SPAN, "
						+ "COL_SPAN, NUMERIC_COLUMN_NO,UPPER(DB_COLUMN) DB_COLUMN, "
						+ "COLUMN_WIDTH,SUM_FLAG,DRILLDOWN_LABEL_FLAG,SCALING,DECIMAL_CNT,GROUPING_FLAG,COLOR_DIFF "
						+ " FROM ? WHERE REPORT_ID = ? AND SESSION_ID=? ORDER BY LABEL_ROW_NUM,LABEL_COL_NUM");
		Object params[] = new Object[intKeyFieldsCount];
		params[0] = promptTreeVb.getColumnHeaderTable();
		params[1] = promptTreeVb.getReportId();
		params[2] = promptTreeVb.getSessionId();
		try {
			return getJdbcTemplate().query(query, params, getColumnHeadersTableMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getColumnHeadersTableMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ColumnHeadersVb columnHeadersVb = new ColumnHeadersVb();
				columnHeadersVb.setReportId(rs.getString("REPORT_ID"));
				columnHeadersVb.setSessionId(rs.getString("SESSION_ID"));
				columnHeadersVb.setLabelRowNum(rs.getInt("LABEL_ROW_NUM"));
				columnHeadersVb.setLabelColNum(rs.getInt("LABEL_COL_NUM"));
				columnHeadersVb.setCaption(rs.getString("CAPTION"));
				columnHeadersVb.setColType(rs.getString("COL_TYPE"));
				columnHeadersVb.setRowSpanNum(rs.getInt("ROW_SPAN"));
				columnHeadersVb.setRowspan(rs.getInt("ROW_SPAN"));
				columnHeadersVb.setNumericColumnNo(rs.getInt("NUMERIC_COLUMN_NO"));
				columnHeadersVb.setColSpanNum(rs.getInt("COL_SPAN"));
				columnHeadersVb.setColspan(rs.getInt("COL_SPAN"));
				columnHeadersVb.setDbColumnName(rs.getString("DB_COLUMN"));
				if (!ValidationUtil.isValid(columnHeadersVb.getDbColumnName()))
					columnHeadersVb.setDbColumnName(columnHeadersVb.getCaption().toUpperCase());
				columnHeadersVb.setColumnWidth(rs.getString("COLUMN_WIDTH"));
				columnHeadersVb.setSumFlag(rs.getString("SUM_FLAG"));
				String drillDownLabel = rs.getString("DRILLDOWN_LABEL_FLAG");
				if (ValidationUtil.isValid(drillDownLabel) && "Y".equalsIgnoreCase(drillDownLabel)) {
					columnHeadersVb.setDrillDownLabel(true);
				} else {
					columnHeadersVb.setDrillDownLabel(false);
				}
				columnHeadersVb.setScaling(rs.getString("SCALING"));
				columnHeadersVb.setDecimalCnt(rs.getString("DECIMAL_CNT"));
				columnHeadersVb.setGroupingFlag(rs.getString("GROUPING_FLAG"));
				if (ValidationUtil.isValid(rs.getString("COLOR_DIFF")))
					columnHeadersVb.setColorDiff(rs.getString("COLOR_DIFF"));

				/*
				 * String groupingFlag = rs.getString("GROUPING_FLAG"); if
				 * (ValidationUtil.isValid(groupingFlag) && "Y".equalsIgnoreCase(groupingFlag))
				 * { columnHeadersVb.setGroupingFlag(true); } else {
				 * columnHeadersVb.setGroupingFlag(false); }
				 */
				return columnHeadersVb;
			}
		};
		return mapper;
	}

	public int deleteReportsStgData(PromptTreeVb vObject) {
		String query = "DELETE FROM REPORTS_STG WHERE REPORT_ID = ?  And SESSION_ID = ? ";
		Object args[] = { vObject.getReportId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteColumnHeadersData(PromptTreeVb vObject) {
		String query = "DELETE FROM COLUMN_HEADERS_STG WHERE REPORT_ID = ?  And SESSION_ID = ? ";
		Object args[] = { vObject.getReportId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public ExceptionCode getComboPromptData(ReportFilterVb vObject, PrdQueryConfig prdQueryConfigVb) {
		setServiceDefaults();
		ExceptionCode exceptionCode = new ExceptionCode();
		String query = "SELECT FIELD_1, FIELD_2, FIELD_3, FIELD_4, PROMPT_ID FROM PROMPTS_STG WHERE VISION_ID = ? AND SESSION_ID= ? AND PROMPT_ID = ? ORDER BY SORT_FIELD ";
		Connection conExt = null;
		strCurrentOperation = "Prompts";
		CallableStatement cs = null;
		Statement stmt1 = null;
		try {
			String sessionId = String.valueOf(System.currentTimeMillis());
			ExceptionCode exConnection = commonDao.getReqdConnection(conExt, prdQueryConfigVb.getDbConnectionName());
			if (exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection) exConnection.getResponse();
			} else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			cs = conExt.prepareCall("{call " + prdQueryConfigVb.getQueryProc() + "}");
			int parameterCount = prdQueryConfigVb.getQueryProc().split("\\?").length - 1;
			if (parameterCount > 5) {
				cs.setString(1, String.valueOf(intCurrentUserId));
				cs.setString(2, sessionId);
				cs.setString(3, vObject.getFilterSourceId());
				cs.setString(4, vObject.getFilter1Val());
				cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Status
				cs.registerOutParameter(6, java.sql.Types.VARCHAR); // Error Message
			} else {
				cs.setString(1, String.valueOf(intCurrentUserId));
				cs.setString(2, sessionId);
				cs.setString(3, vObject.getFilterSourceId());
				cs.registerOutParameter(4, java.sql.Types.VARCHAR); // Status
				cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Error Message
			}
			ResultSet rs = cs.executeQuery();
			PromptTreeVb promptTreeVb = new PromptTreeVb();
			if (parameterCount > 5) {
				promptTreeVb.setStatus(cs.getString(5));
				promptTreeVb.setErrorMessage(cs.getString(6));
			} else {
				promptTreeVb.setStatus(cs.getString(4));
				promptTreeVb.setErrorMessage(cs.getString(5));
			}
			rs.close();
			if (promptTreeVb != null && "0".equalsIgnoreCase(promptTreeVb.getStatus())) {
				String[] params = new String[3];
				params[0] = String.valueOf(intCurrentUserId);
				params[1] = sessionId;
				params[2] = vObject.getFilterSourceId();
				if (ValidationUtil.isValid(vObject.getFilterString())) {
					query = query + " " + vObject.getFilterString();
				}
				String resultQuery = "";
				resultQuery = query;
				for (int i = 0; i < params.length; i++) {
					resultQuery = resultQuery.replaceFirst("\\?", "'?'");
					resultQuery = resultQuery.replaceFirst("\\?", params[i]);
				}
				LinkedHashMap<String, String> comboValueMap = new LinkedHashMap<String, String>();
				exceptionCode = getReportPromptsFilterValue(prdQueryConfigVb, resultQuery);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION
						&& exceptionCode.getResponse() != null)
					comboValueMap = (LinkedHashMap<String, String>) exceptionCode.getResponse();
				query = query.toUpperCase();
				if (query.indexOf("ORDER BY") > 0) {
					query = query.substring(query.indexOf("FROM "), query.indexOf("ORDER BY") - 1);
				} else {
					query = query.substring(query.indexOf("FROM "), query.length());
				}
				for (int i = 0; i < params.length; i++) {
					if (query.contains("?"))
						query = query.replaceFirst("\\?", "'" + params[i] + "'");
				}
				query = "DELETE " + query;
				stmt1.executeUpdate(query);
				// int count = getJdbcTemplate().update(query, params);

				exceptionCode.setResponse(comboValueMap);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} else if (promptTreeVb != null && "1".equalsIgnoreCase(promptTreeVb.getStatus())) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			// throw new RuntimeCustomException(promptTreeVb.getErrorMessage());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
		} finally {
			JdbcUtils.closeStatement(cs);
			try {
				conExt.close();
			} catch (SQLException e) {
				// e.printStackTrace();
			}
			// DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
		return exceptionCode;
	}

	public ExceptionCode getReportFilterValue(String sourceQuery) {
		ExceptionCode exceptionCode = new ExceptionCode();
		LinkedHashMap<String, String> filterMapNew = new LinkedHashMap<String, String>();
		ResultSetExtractor mapper = new ResultSetExtractor() {
			public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
				ResultSetMetaData metaData = rs.getMetaData();
				LinkedHashMap<String, String> filterMap = new LinkedHashMap<String, String>();
				int colCount = metaData.getColumnCount();
				while (rs.next()) {
					if (colCount == 1)
						filterMap.put("@" + rs.getString(1), rs.getString(1));
					else
						filterMap.put("@" + rs.getString(1), rs.getString(2));
				}
				return filterMap;
			}
		};
		filterMapNew = (LinkedHashMap<String, String>) getJdbcTemplate().query(sourceQuery, mapper);
		exceptionCode.setResponse(filterMapNew);
		return exceptionCode;
	}

	public ExceptionCode getReportPromptsFilterValue(PrdQueryConfig vObject, String query) {
		ExceptionCode exceptionCode = new ExceptionCode();
		LinkedHashMap<String, String> filterMap = new LinkedHashMap<String, String>();
		Connection conExt = null;
		Statement stmt1 = null;
		try {
			ExceptionCode exConnection = commonDao.getReqdConnection(conExt, vObject.getDbConnectionName());
			if (exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection) exConnection.getResponse();
			} else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			if (ValidationUtil.isValid(query))
				vObject.setQueryProc(query);
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet rsData = stmt1.executeQuery(vObject.getQueryProc());
			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			while (rsData.next()) {
				if (colCount == 1)
					filterMap.put("@" + rsData.getString(1), rsData.getString(1));
				else
					filterMap.put("@" + rsData.getString(1), rsData.getString(2));
			}
			if (filterMap == null && filterMap.isEmpty()) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				return exceptionCode;
			}
			exceptionCode.setResponse(filterMap);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		} finally {
			try {
				if (stmt1 != null)
					stmt1.close();
				if (conExt != null)
					conExt.close();
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		return exceptionCode;
		// return (LinkedHashMap<String, String>) getJdbcTemplate().query(sourceQuery,
		// mapper);
	}

	public String getReportFilterDefaultValue(String sourceQuery) {
		ResultSetExtractor mapper = new ResultSetExtractor() {
			String defaultValue = "";

			public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
				ResultSetMetaData metaData = rs.getMetaData();
				while (rs.next()) {
					defaultValue = rs.getString(1);
				}
				return defaultValue;
			}
		};
		return (String) getJdbcTemplate().query(sourceQuery, mapper);
	}

	public List<RCReportFieldsVb> getCBKReportData(ReportsVb reportsVb, PromptTreeVb prompts, long min, long max) {
		String query = "";
		try {
			query = "SELECT TAB_ID, ROW_ID, COL_ID, CELL_DATA, COL_TYPE, CREATE_NEW,SHEET_NAME,FORMAT_TYPE,FILE_NAME FROM "
					+ "TEMPLATES_STG WHERE SORT_FIELD <=? AND SORT_FIELD >? AND SESSION_ID=? ORDER BY TAB_ID,ROW_ID,COL_ID";
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					List<RCReportFieldsVb> result = new ArrayList<RCReportFieldsVb>();
					while (rs.next()) {
						RCReportFieldsVb fieldsVb = new RCReportFieldsVb();
						fieldsVb.setTabelId(rs.getString("TAB_ID"));
						fieldsVb.setRowId(rs.getString("ROW_ID"));
						fieldsVb.setColId(rs.getString("COL_ID"));
						fieldsVb.setValue1(rs.getString("CELL_DATA"));
						fieldsVb.setColType(rs.getString("COL_TYPE"));
						fieldsVb.setSheetName(rs.getString("SHEET_NAME"));
						fieldsVb.setRowStyle(rs.getString("FORMAT_TYPE"));
						fieldsVb.setExcelFileName(rs.getString("FILE_NAME"));
						// M - Mandatory create new Line. Y -- Create new Line only if does not exists
						// N-- Do not create new Line.
						fieldsVb.setCreateNew("Y".equalsIgnoreCase(rs.getString("CREATE_NEW"))
								|| "M".equalsIgnoreCase(rs.getString("CREATE_NEW")) ? true : false);
						fieldsVb.setCreateNewRow(rs.getString("CREATE_NEW"));
						result.add(fieldsVb);
					}
					return result;
				}
			};
			Object[] promptValues = new Object[3];
			promptValues[0] = max;
			promptValues[1] = min;
			promptValues[2] = prompts.getSessionId();
			/*
			 * int retVal =InsetAuditTrialDataForReports(reportWriterVb, prompts);
			 * if(retVal!=Constants.SUCCESSFUL_OPERATION){
			 * logger.error("Error  inserting into rs_Schedule Audit"); }
			 */

			return (List<RCReportFieldsVb>) getJdbcTemplate().query(query, promptValues, mapper);
		} catch (BadSqlGrammarException ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			strErrorDesc = parseErrorMsg(new UncategorizedSQLException("", "", ex.getSQLException()));
			throw buildRuntimeCustomException(getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION));
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			strErrorDesc = ex.getMessage();
			throw buildRuntimeCustomException(getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION));
		}
	}

	public void callProcToCleanUpTables(PromptTreeVb promptTree) {
		Connection con = null;
		CallableStatement cs = null;
		try {
			con = getConnection();
			cs = con.prepareCall("{call PR_RS_CLEANUP(?, ?, ?, ?, ?)}");
			cs.setString(1, String.valueOf(intCurrentUserId));// Report Id
			cs.setString(2, promptTree.getSessionId());// Group Report Id
			cs.setString(3, promptTree.getReportId());// Group Report Id
			cs.registerOutParameter(4, java.sql.Types.VARCHAR);// Chart type list
			cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Status
			ResultSet rs = cs.executeQuery();
			rs.close();
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		} finally {
			JdbcUtils.closeStatement(cs);
			DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
	}

	public List<ReportsVb> findApplicationCategory() throws DataAccessException {
		VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
		String applicationAccess = visionUsersVb.getApplicationAccess();
		if (!"REPORTS".equalsIgnoreCase(productName)) {
			applicationAccess = "'" + productName + "'";
		}
		String sql = " SELECT DISTINCT APPLICATION_ID ALPHA_SUB_TAB,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "
				+ " ALPHA_TAB = 8000 AND ALPHA_SUB_TAB = APPLICATION_ID) ALPHA_SUBTAB_DESCRIPTION "
				+ " FROM PRD_REPORT_MASTER where APPLICATION_ID IN (" + applicationAccess
				+ ") ORDER BY ALPHA_SUBTAB_DESCRIPTION ";
		return getJdbcTemplate().query(sql, getAppCategoryMapper());
	}

	private RowMapper getAppCategoryMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setApplicationId(rs.getString("ALPHA_SUB_TAB"));
				vObject.setApplicationIdDesc(rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<ReportsVb> getWidgetList() throws DataAccessException {
		VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
		String sql = "";
		try {

			sql = " Select DISTINCT T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
					+ " (SELECT " + getDbFunction(Constants.NVL, null)
					+ "(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
					+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
					+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,"
					+ "(SELECT " + getDbFunction(Constants.NVL, null) + "(MIN (SCALING_FACTOR), 0) "
					+ "        FROM PRD_REPORT_DETAILS "
					+ "       WHERE REPORT_ID = T2.REPORT_ID AND SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS "
					+ "      WHERE REPORT_ID = T2.REPORT_ID)       )      SCALING_FACTOR,T2.REPORT_TYPE, "
					+ "	(SELECT " + getDbFunction(Constants.NVL, null)
					+ "(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
					+ " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
					+ ") REPORT_ORIENTATION,(SELECT OBJECT_TYPE FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID  "
					+ "   AND SUBREPORT_SEQ= (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) OBJECT_TYPE, "
					+ " (SELECT CHART_TYPE FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID  "
					+ "    AND SUBREPORT_SEQ= (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) CHART_TYPE, "
					+ " WIDGET_THEME,REPORT_PERIOD, (SELECT SORTING_ENABLE FROM PRD_REPORT_DETAILS S1 "
					+ " WHERE S1.REPORT_ID = T2.REPORT_ID AND SUBREPORT_SEQ = (SELECT MIN (SUBREPORT_SEQ) "
					+ " FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) SORTING_ENABLE,"
					+ " (SELECT WIDGET_PAGINATION FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID "
					+ " AND SUBREPORT_SEQ = (SELECT MIN (SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 "
					+ " WHERE S1.REPORT_ID = T2.REPORT_ID)) WIDGET_PAGINATION"
					+ " FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
					+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP"
					+ getDbFunction(Constants.PIPELINE, null) + "'-'" + getDbFunction(Constants.PIPELINE, null)
					+ "T1.USER_PROFILE IN (" + visionUsersVb.getUserGrpProfile() + ") "
					+ " AND T2.STATUS = 0 AND REPORT_TYPE ='W'  AND T2.APPLICATION_ID =  ? ";

			String orderBy = " ORDER BY REPORT_TITLE";
			sql = sql + orderBy;
			Object[] lParams = new Object[1];
			lParams[0] = productName;
			return getJdbcTemplate().query(sql, lParams, getWidgetListMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}

	}

	protected RowMapper getWidgetListMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setReportId(rs.getString("REPORT_ID"));
				vObject.setReportTitle(rs.getString("REPORT_TITLE"));
				vObject.setFilterFlag(rs.getString("FILTER_FLAG"));
				vObject.setFilterRefCode(rs.getString("FILTER_REF_CODE"));
				vObject.setApplyUserRestrct(rs.getString("APPLY_USER_RESTRCT"));
				vObject.setCurrentLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setNextLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setGroupingFlag(rs.getString("GROUPING_FLAG"));
				vObject.setMaxRecords(rs.getInt("MAX_PERPAGE"));
				vObject.setReportTypeAT(rs.getInt("Report_Type_AT"));
				vObject.setReportType(rs.getString("REPORT_TYPE"));
				vObject.setTemplateId(rs.getString("Template_ID"));
				vObject.setReportOrientation(rs.getString("REPORT_ORIENTATION"));
				vObject.setObjectType(rs.getString("OBJECT_TYPE"));
				vObject.setChartType(rs.getString("CHART_TYPE"));
				String widgetTheme = rs.getString("WIDGET_THEME");
			    vObject.setWidgetTheme(widgetTheme);
			    JSONObject xmlJSONObj = XML.toJSONObject(themeXml);
				int PRETTY_PRINT_INDENT_FACTOR = 4;
				String resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
				vObject.setReportDesignXml(resultData);
				/*if(ValidationUtil.isValid(widgetTheme)) {
					String widgetThemePath = servletContext.getRealPath("/WEB-INF/classes/Widget_Theme/");
					widgetThemePath = widgetThemePath+widgetTheme+".txt";
					String data = "";
					try {
						data = new String(Files.readAllBytes(Paths.get(widgetThemePath)));
						JSONObject xmlJSONObj = XML.toJSONObject(data);
						int PRETTY_PRINT_INDENT_FACTOR = 4;
						String resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
						vObject.setReportDesignXml(resultData);
					} catch (IOException e) {
						//System.out.println(e.getMessage());
					}catch(Exception e) {
						//System.out.println(e.getMessage());
					}
				}*/
				vObject.setScalingFactor(rs.getString("SCALING_FACTOR"));
				/*if(ValidationUtil.isValid(rs.getString("SCALING_FACTOR"))) {
					if("1000000".equalsIgnoreCase(rs.getString("SCALING_FACTOR"))) {
						vObject.setScalingFactor("(In Millions)");
					}else if("1000".equalsIgnoreCase(rs.getString("SCALING_FACTOR"))) {
						vObject.setScalingFactor("(In Thousands)");
					}else if("1000000000".equalsIgnoreCase(rs.getString("SCALING_FACTOR"))) {
						vObject.setScalingFactor("(In Billions)");
					}else {
						vObject.setScalingFactor("");
					}
				}*/
				if(ValidationUtil.isValid(rs.getString("REPORT_PERIOD"))) {
					VisionUsersVb visionUsers = CustomContextHolder.getContext();
					String country = "";
					String leBook = "";
					if(ValidationUtil.isValid(visionUsers.getCountry())) {
						country = visionUsers.getCountry();
					}else {
						country = commonDao.findVisionVariableValue("DEFAULT_COUNTRY");
					}
					if(ValidationUtil.isValid(visionUsers.getLeBook())) {
						leBook = visionUsers.getLeBook();
					}else {
						leBook = commonDao.findVisionVariableValue("DEFAULT_LE_BOOK");
					}
					vObject.setReportPeriod(commonDao.getCurrentDateInfo(rs.getString("REPORT_PERIOD"), country+"-"+leBook));
					// vObject.setReportPeriodFormat(ValidationUtil.isValid(rs.getString("REPORT_PERIOD")) ? rs.getString("REPORT_PERIOD") : "");
				}
				vObject.setSortFlag(rs.getString("SORTING_ENABLE"));
				vObject.setWidgetPagination(rs.getInt("WIDGET_PAGINATION"));
				return vObject;
			}
		};
		return mapper;
	}

	public int saveWidget(ReportsVb dObj) {
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "Insert Into PRD_USER_WIDGETS (APPLICATION_ID,VISION_ID, TEMPLATE_ID,WIDGET_IDS,WIDGET_THEME,USER_WIDGET_STATUS,MAKER,VERIFIER, "
					+ " DATE_LAST_MODIFIED,DATE_CREATION) Values (?,?,?,?,?,?,?,?,"
					+ getDbFunction(Constants.SYSDATE, null) + "," + getDbFunction(Constants.SYSDATE, null) + ")";

			Object[] args = { productName, intCurrentUserId, dObj.getTemplateId(), dObj.getSavedWidgetId(),
					dObj.getWidgetTheme(), 0, intCurrentUserId, intCurrentUserId };
			retVal = getJdbcTemplate().update(query, args);
			return retVal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	public int userWidgetExists(ReportsVb dObj) {
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "SELECT COUNT(1) FROM PRD_USER_WIDGETS WHERE APPLICATION_ID = ? AND VISION_ID = ? ";
			Object[] args = { productName, intCurrentUserId };
			retVal = getJdbcTemplate().queryForObject(query, args, Integer.class);
			return retVal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	public int deleteWidgetExists(ReportsVb dObj) {
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "DELETE FROM PRD_USER_WIDGETS WHERE APPLICATION_ID = ? AND VISION_ID = ? ";
			Object[] args = { productName, intCurrentUserId };
			retVal = getJdbcTemplate().update(query, args);
			return retVal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	public List<ReportsVb> getUserWidgets() throws DataAccessException {
		setServiceDefaults();
		try {
			String sql = " SELECT TEMPLATE_ID,WIDGET_IDS,WIDGET_THEME FROM PRD_USER_WIDGETS WHERE APPLICATION_ID = ? AND USER_WIDGET_STATUS = 0 AND VISION_ID = ? ";
			Object[] lParams = new Object[2];
			lParams[0] = productName;
			lParams[1] = intCurrentUserId;
			return getJdbcTemplate().query(sql, lParams, getUserWidgetMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}

	}

	protected RowMapper getUserWidgetMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setTemplateId(rs.getString("Template_ID"));
				vObject.setSavedWidgetId(rs.getString("WIDGET_IDS"));
				vObject.setWidgetTheme(rs.getString("WIDGET_THEME"));
				return vObject;
			}
		};
		return mapper;
	}

	private String replaceTagValues(String chartXml) {
		String chartReplaceXml = chartXml;
		chartReplaceXml = chartReplaceXml.replaceAll("categoryL1", "category");
		chartReplaceXml = chartReplaceXml.replaceAll("categoryL2", "category");
		chartReplaceXml = chartReplaceXml.replaceAll("categoryL3", "category");
		return chartReplaceXml;
	}

	public ExceptionCode getTilesReportData(ReportsVb vObject, String orginalQuery, Connection conExt)
			throws SQLException {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		setServiceDefaults();
		Statement stmt = null;
		ResultSet rs = null;
		String resultData = "";
		DecimalFormat dfDec = new DecimalFormat("0.00");
		DecimalFormat dfNoDec = new DecimalFormat("0");
		List<PrdQueryConfig> sqlQueryList = new ArrayList<PrdQueryConfig>();
		PrdQueryConfig prdQueryConfig = new PrdQueryConfig();
		ExceptionCode exceptionCode = new ExceptionCode();
		Statement stmt1 = null;
		try {
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			ResultSet rsData = stmt1.executeQuery(orginalQuery);
			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			HashMap<String, String> columns = new HashMap<String, String>();
			Boolean dataAvail = false;
			while (rsData.next()) {
				for (int cn = 1; cn <= colCount; cn++) {
					String columnName = metaData.getColumnName(cn);
					columns.put(columnName.toUpperCase(), rsData.getString(columnName));
				}
				dataAvail = true;
				break;
			}
			rsData.close();
			if (!dataAvail) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found");
				return exceptionCode;
			}
			String fieldProp = vObject.getColHeaderXml();
			fieldProp = ValidationUtil.isValid(fieldProp) ? fieldProp.replaceAll("\n", "").replaceAll("\r", "") : "";
			for (int ctr = 1; ctr <= 5; ctr++) {
				String placeHolder = CommonUtils.getValueForXmlTag(fieldProp, "PLACEHOLDER" + ctr);
				String sourceCol = CommonUtils.getValueForXmlTag(placeHolder, "SOURCECOL");
				String dataType = CommonUtils.getValueForXmlTag(placeHolder, "DATA_TYPE");
				String numberFormat = CommonUtils.getValueForXmlTag(placeHolder, "NUMBERFORMAT");
				String scaling = CommonUtils.getValueForXmlTag(placeHolder, "SCALING");
				if (!ValidationUtil.isValid(placeHolder) || !ValidationUtil.isValid(sourceCol)) {
					continue;
				}
				if (ValidationUtil.isValid(sourceCol)) {
					String fieldValue = columns.get(sourceCol);
					if (!ValidationUtil.isValid(fieldValue))
						continue;
					/* Double val = 0.00; */
					String prefix = "";
					if (ValidationUtil.isValid(scaling) && "Y".equalsIgnoreCase(scaling)
							&& ValidationUtil.isValid(fieldValue)) {
						Double dbValue = Math.abs(Double.parseDouble(fieldValue));
						if (dbValue > 1000000000) {
							dbValue = Double.parseDouble(fieldValue) / 1000000000;
							prefix = "B";
						} else if (dbValue > 1000000) {
							dbValue = Double.parseDouble(fieldValue) / 1000000;
							prefix = "M";
						} else if (dbValue > 10000) {
							dbValue = Double.parseDouble(fieldValue) / 1000;
							prefix = "K";
						}
						String afterDecimalVal = String.valueOf(dbValue);
						if (!afterDecimalVal.contains("E")) {
							afterDecimalVal = afterDecimalVal.substring(afterDecimalVal.indexOf(".") + 1);
							if (Double.parseDouble(afterDecimalVal) > 0)
								fieldValue = dfDec.format(dbValue) + " " + prefix;
							else
								fieldValue = dfNoDec.format(dbValue) + " " + prefix;
						} else {
							fieldValue = "0.00";
						}
					}
					if (ValidationUtil.isValid(fieldValue) && ValidationUtil.isValid(numberFormat)
							&& "Y".equalsIgnoreCase(numberFormat) && !ValidationUtil.isValid(prefix)) {
						DecimalFormat decimalFormat = new DecimalFormat("#,###.##");
						double tmpVal = Double.parseDouble(fieldValue);
						fieldValue = decimalFormat.format(tmpVal);
					}
					/* if(ValidationUtil.isValid(fieldValue)) */
					fieldProp = fieldProp.replaceAll(sourceCol, fieldValue);
				}
			}
			String drillDownKey = CommonUtils.getValueForXmlTag(fieldProp, "DRILLDOWN_KEY");
			if (ValidationUtil.isValid(drillDownKey) && "DD_KEY_ID".equalsIgnoreCase(drillDownKey)) {
				String value = columns.get(drillDownKey);
				if (ValidationUtil.isValid(value))
					fieldProp = fieldProp.replaceAll(drillDownKey, value);
				else
					fieldProp = fieldProp.replaceAll(drillDownKey, "");
			}
			/*
			 * String colorCode = CommonUtils.getValueForXmlTag(fieldProp,"ColorCode");
			 * String applyTextColor =
			 * CommonUtils.getValueForXmlTag(fieldProp,"APPLY_TEXT_COLOR"); String colorDiff
			 * = CommonUtils.getValueForXmlTag(fieldProp,"COLOR_DIFF");
			 */
			int PRETTY_PRINT_INDENT_FACTOR = 4;
			JSONObject xmlJSONObj = XML.toJSONObject(fieldProp);
			resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
			exceptionCode.setResponse(resultData);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
		} finally {
			if (stmt1 != null)
				stmt1.close();
			if (conExt != null)
				conExt.close();
		}
		return exceptionCode;
	}

	public int saveReportUserDef(ReportUserDefVb dObj) {
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "Insert Into PRD_REPORT_USER_DEF (VISION_ID, REPORT_ID,SUB_REPORT_ID,GROUP_COLUMNS,SORTING_COLUMN,SEARCH_COLUMN,HIDE_COLUMNS,"
					+ " DATA_FILTER_1,DATA_FILTER_2,DATA_FILTER_3,DATA_FILTER_4,DATA_FILTER_5, "
					+ " DATA_FILTER_6,DATA_FILTER_7,DATA_FILTER_8,DATA_FILTER_9,DATA_FILTER_10, "
					+ " PROMPT_VALUE_1,PROMPT_VALUE_2,PROMPT_VALUE_3,PROMPT_VALUE_4,PROMPT_VALUE_5, "
					+ " PROMPT_VALUE_6,PROMPT_VALUE_7,PROMPT_VALUE_8,PROMPT_VALUE_9,PROMPT_VALUE_10, SCALING_FACTOR,GROUPING_FLAG, "
					+ " SHOW_MEASURES,SHOW_DIMENSIONS,CHART_TYPE_AT,CHART_TYPE, "
					+ " STATUS,RECORD_INDICATOR,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION) "
					+ "Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,?,?,"
					+ getDbFunction(Constants.SYSDATE, null) + "," + getDbFunction(Constants.SYSDATE, null) + ")";
			Object[] args = { intCurrentUserId, dObj.getReportId(), dObj.getSubReportId(), dObj.getGroupingColumn(),
					dObj.getSortColumn(), dObj.getSearchColumn(), dObj.getColumnsToHide(), dObj.getDataFilter1(),
					dObj.getDataFilter2(), dObj.getDataFilter3(), dObj.getDataFilter4(), dObj.getDataFilter5(),
					dObj.getDataFilter6(), dObj.getDataFilter7(), dObj.getDataFilter8(), dObj.getDataFilter9(),
					dObj.getDataFilter10(), dObj.getPromptValue1(), dObj.getPromptValue2(), dObj.getPromptValue3(),
					dObj.getPromptValue4(), dObj.getPromptValue5(), dObj.getPromptValue6(), dObj.getPromptValue7(),
					dObj.getPromptValue8(), dObj.getPromptValue9(), dObj.getPromptValue10(), dObj.getScalingFactor(),
					dObj.getApplyGrouping(), dObj.getShowMeasures(), dObj.getShowDimensions(), dObj.getChartTypeAT(),
					dObj.getChartType(), intCurrentUserId, intCurrentUserId };
			retVal = getJdbcTemplate().update(query, args);
			return retVal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	public int deleteReportUserDef(ReportUserDefVb vObject) {
		String query = "DELETE FROM PRD_REPORT_USER_DEF WHERE VISION_ID= ? AND  REPORT_ID = ?  And SUB_REPORT_ID = ? ";
		Object args[] = { intCurrentUserId, vObject.getReportId(), vObject.getSubReportId() };
		return getJdbcTemplate().update(query, args);
	}

	public int reportUserDefExists(ReportUserDefVb vObject) {
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "SELECT COUNT(1) FROM PRD_REPORT_USER_DEF WHERE  VISION_ID= ? AND  REPORT_ID = ?  And SUB_REPORT_ID = ? ";
			Object args[] = { intCurrentUserId, vObject.getReportId(), vObject.getSubReportId() };
			retVal = getJdbcTemplate().queryForObject(query, args, Integer.class);
			return retVal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	public List<ReportsVb> getSavedUserDefSetting(ReportsVb reportvb, Boolean checkFlag) throws DataAccessException {
		setServiceDefaults();
		Object[] lParams = null;
		try {
			String sql = "SELECT T1.REPORT_ID,T1.SUB_REPORT_ID,GROUP_COLUMNS,SORTING_COLUMN,SEARCH_COLUMN,HIDE_COLUMNS, "
					+ " DATA_FILTER_1,DATA_FILTER_2,DATA_FILTER_3,DATA_FILTER_4,DATA_FILTER_5, "
					+ " DATA_FILTER_6,DATA_FILTER_7,DATA_FILTER_8,DATA_FILTER_9,DATA_FILTER_10, "
					+ " PROMPT_VALUE_1,PROMPT_VALUE_2,PROMPT_VALUE_3,PROMPT_VALUE_1,PROMPT_VALUE_4,PROMPT_VALUE_5, "
					+ " PROMPT_VALUE_6,PROMPT_VALUE_7,PROMPT_VALUE_8,PROMPT_VALUE_9,PROMPT_VALUE_10, T1.SCALING_FACTOR,T1.GROUPING_FLAG,"
					+ " SHOW_MEASURES,SHOW_DIMENSIONS,T1.CHART_TYPE "
					+ " FROM PRD_REPORT_USER_DEF T1, PRD_REPORT_MASTER T2,PRD_REPORT_DETAILS T3"
					+ " WHERE T1.VISION_ID = ? AND T1.STATUS = 0  AND T1.REPORT_ID = T3.REPORT_ID "
					+ " AND T1.SUB_REPORT_ID = T3.SUB_REPORT_ID  AND T2.REPORT_ID = T3.REPORT_ID";
			if (checkFlag) {
				sql = sql + " AND T2.REPORT_TYPE != ?  AND T1.REPORT_ID = ?  AND T1.SUB_REPORT_ID = ? ";
				lParams = new Object[4];
				lParams[0] = intCurrentUserId;
				lParams[1] = "W";
				lParams[2] = reportvb.getReportId();
				lParams[3] = reportvb.getSubReportId();
			} else {
				sql = sql + " AND T2.REPORT_TYPE = ? ";
				lParams = new Object[2];
				lParams[0] = intCurrentUserId;
				lParams[1] = "W";
			}

			return getJdbcTemplate().query(sql, lParams, getUserDefSettingMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}

	}

	protected RowMapper getUserDefSettingMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportUserDefVb vObject = new ReportUserDefVb();
				vObject.setReportId(rs.getString("REPORT_ID"));
				vObject.setSubReportId(rs.getString("SUB_REPORT_ID"));
				vObject.setGroupingColumn(rs.getString("GROUP_COLUMNS"));
				vObject.setSortColumn(rs.getString("SORTING_COLUMN"));
				vObject.setSearchColumn(rs.getString("SEARCH_COLUMN"));
				vObject.setColumnsToHide(rs.getString("HIDE_COLUMNS"));
				vObject.setDataFilter1(rs.getString("DATA_FILTER_1"));
				vObject.setDataFilter2(rs.getString("DATA_FILTER_2"));
				vObject.setDataFilter3(rs.getString("DATA_FILTER_3"));
				vObject.setDataFilter4(rs.getString("DATA_FILTER_4"));
				vObject.setDataFilter5(rs.getString("DATA_FILTER_5"));
				vObject.setDataFilter6(rs.getString("DATA_FILTER_6"));
				vObject.setDataFilter7(rs.getString("DATA_FILTER_7"));
				vObject.setDataFilter8(rs.getString("DATA_FILTER_8"));
				vObject.setDataFilter9(rs.getString("DATA_FILTER_9"));
				vObject.setDataFilter10(rs.getString("DATA_FILTER_10"));
				vObject.setPromptValue1(rs.getString("PROMPT_VALUE_1"));
				vObject.setPromptValue2(rs.getString("PROMPT_VALUE_2"));
				vObject.setPromptValue3(rs.getString("PROMPT_VALUE_3"));
				vObject.setPromptValue4(rs.getString("PROMPT_VALUE_4"));
				vObject.setPromptValue5(rs.getString("PROMPT_VALUE_5"));
				vObject.setPromptValue6(rs.getString("PROMPT_VALUE_6"));
				vObject.setPromptValue7(rs.getString("PROMPT_VALUE_7"));
				vObject.setPromptValue8(rs.getString("PROMPT_VALUE_8"));
				vObject.setPromptValue9(rs.getString("PROMPT_VALUE_9"));
				vObject.setPromptValue10(rs.getString("PROMPT_VALUE_10"));
				vObject.setScalingFactor(rs.getString("SCALING_FACTOR"));
				vObject.setApplyGrouping(rs.getString("GROUPING_FLAG"));
				vObject.setShowMeasures(rs.getString("SHOW_MEASURES"));
				vObject.setShowDimensions(rs.getString("SHOW_DIMENSIONS"));
				vObject.setChartType(rs.getString("CHART_TYPE"));
				return vObject;
			}
		};
		return mapper;
	}

	public String getReportType(String reportId) {
		String reportType = "";
		String query = "";
		try {
			query = "SELECT REPORT_TYPE FROM PRD_REPORT_MASTER WHERE REPORT_ID = '" + reportId + "'";
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
		return getJdbcTemplate().queryForObject(query, String.class);
	}

	public List getChartList(String chartType) {
		String sql = "";
		List collTemp = new ArrayList<>();
		try {
			sql = "SELECT (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 7005 AND ALPHA_SUB_TAB = S1.ALPHA_SUB_TAB ) ALPHA_SUB_TAB, "
					+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 7501 AND ALPHA_SUB_TAB = S1.ALPHA_SUB_TAB ) DESCR "
					+ " FROM ALPHA_SUB_TAB S1 WHERE ALPHA_TAB = 7500 AND ALPHA_SUBTAB_DESCRIPTION IN (  "
					+ " SELECT T2.ALPHA_SUBTAB_DESCRIPTION TYPE  FROM ALPHA_SUB_TAB T1,   ALPHA_SUB_TAB T2  "
					+ " WHERE T1.ALPHA_TAB = 7005   AND T2.ALPHA_TAB = 7500   AND T1.ALPHA_SUB_TAB = '" + chartType
					+ "'   AND T1.ALPHA_SUB_TAB = T2.ALPHA_SUB_TAB) ORDER BY DESCR ";
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						LinkedHashMap<String, String> resultData = new LinkedHashMap<String, String>();
						for (int cn = 1; cn < 2; cn++) {
							resultData.put(rs.getString("ALPHA_SUB_TAB"), rs.getString("DESCR"));
						}
						collTemp.add(resultData);
					}
					return collTemp;
				}
			};
			return (List) getJdbcTemplate().query(sql, mapper);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return collTemp;
	}

	public int getNotificationCount() {
		try {
			String sql = "        Select count(*)             From ETL_FEED_AUDIT_TRAIL TAppr "
					+ "                 INNER JOIN ETL_FEED_MAIN T2 "
					+ "                 ON (TAppr.COUNTRY = T2.COUNTRY AND TAppr.LE_BOOK = T2.LE_BOOK AND TAppr.FEED_ID = T2.FEED_ID )"
					+ "                 INNER JOIN LE_BOOK LEB "
					+ "                ON (TAppr.COUNTRY = LEB.COUNTRY AND TAppr.LE_BOOK = LEB.LE_BOOK)";
			int i = getJdbcTemplate().queryForObject(sql, Integer.class);
			return i;

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return 0;
		}
	}

	public ExceptionCode getResultData(ReportsVb vObject, Boolean exportFlag) {
		ExceptionCode exceptionCode = new ExceptionCode();
		int totalRecords = 0;
		List datalst = new ArrayList();
		Connection conExt = null;
		try {
			ExceptionCode exConnection = commonDao.getReqdConnection(conExt, vObject.getDbConnection());
			if (exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection) exConnection.getResponse();
			} else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			if ("G".equalsIgnoreCase(vObject.getObjectType())) {
				// exceptionCode = extractReportData(vObject, conExt);
				if (exportFlag) {
					exceptionCode = extractReportData(vObject, conExt, exportFlag);
				} else {
					exceptionCode = extractReportData(vObject, conExt);
				}
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					datalst = (ArrayList) exceptionCode.getResponse();
					vObject.setGridDataSet(datalst);
					// Filter the Data which doensnot contains Format Type S
					List<Map<String, Object>> finalDatalst = (List<Map<String, Object>>) datalst.stream()
							.filter(hashmap -> ((HashMap<String, String>) hashmap).containsKey("FORMAT_TYPE"))
							.filter(hashmap -> !((HashMap<String, String>) hashmap).get("FORMAT_TYPE")
									.equalsIgnoreCase("S"))
							.collect(Collectors.toList());
					if (finalDatalst != null && !finalDatalst.isEmpty())
						vObject.setGridDataSet(finalDatalst);

					totalRecords = (int) exceptionCode.getRequest();
					vObject.setTotalRows(totalRecords);
					if (vObject.getCurrentPage() == 1) {
						List<Map<String, Object>> totallst = (List<Map<String, Object>>) datalst.stream()
								.filter(hashmap -> ((HashMap<String, String>) hashmap).containsKey("FORMAT_TYPE"))
								.filter(hashmap -> ((HashMap<String, String>) hashmap).get("FORMAT_TYPE")
										.equalsIgnoreCase("S"))
								.collect(Collectors.toList());
						vObject.setTotal(totallst);
						if (totallst == null || totallst.isEmpty()) {
							List<ReportsVb> sumStringLst = new ArrayList<>();
							StringJoiner sumString = new StringJoiner(",");
							vObject.getColumnHeaderslst().forEach(colHeadersVb -> {
								if (!"T".equalsIgnoreCase(colHeadersVb.getColType())
										&& (colHeadersVb.getColspan() == 0 || colHeadersVb.getColspan() == 1)
										&& "Y".equalsIgnoreCase(colHeadersVb.getSumFlag())) {
									sumString.add("SUM(" + colHeadersVb.getDbColumnName() + ") "
											+ colHeadersVb.getDbColumnName());
								}
							});
							ExceptionCode exceptionCode1 = new ExceptionCode();
							String query = null;
							if (sumString.length() > 0) {
								query = "SELECT " + sumString.toString() + ",'S' FORMAT_TYPE FROM ("
										+ vObject.getFinalExeQuery() + ") TOT ";
								exceptionCode1 = commonApiDao.getCommonResultDataQuery(query, conExt);
								if (exceptionCode1.getResponse() != null) {
									sumStringLst = (List<ReportsVb>) exceptionCode1.getResponse();
									vObject.setTotal(sumStringLst);
								}
							}
						}
					}
				} else {
					exceptionCode.setResponse(vObject);
					return exceptionCode;
				}
			} else if ("C".equalsIgnoreCase(vObject.getObjectType())) {
				exceptionCode = getChartReportData(vObject, vObject.getFinalExeQuery(), conExt);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					vObject.setChartData(exceptionCode.getResponse().toString());
				}
			} else if ("T".equalsIgnoreCase(vObject.getObjectType()) || "SC".equalsIgnoreCase(vObject.getObjectType())
					|| "S".equalsIgnoreCase(vObject.getObjectType())) {
				exceptionCode = getTilesReportData(vObject, vObject.getFinalExeQuery(), conExt);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					vObject.setTileData(exceptionCode.getResponse().toString());
				}
			}
			// exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(vObject);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Success");
		} catch (Exception e) {
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		} finally {
			try {
				conExt.close();
			} catch (Exception e) {
				// e.printStackTrace();
			}
			/*
			 * if(vObject.getPromptTree() != null) { PromptTreeVb promptTreeVb = new
			 * PromptTreeVb(); if(ValidationUtil.isValid(promptTreeVb.getTableName())) {
			 * if(!"REPORTS_STG".equalsIgnoreCase(promptTreeVb.getTableName()))
			 * reportsDao.deleteTempTable(promptTreeVb.getTableName()); else
			 * reportsDao.deleteReportsStgData(promptTreeVb); }
			 * if(ValidationUtil.isValid(promptTreeVb.getColumnHeaderTable()))
			 * reportsDao.deleteColumnHeadersData(promptTreeVb); }
			 */
		}
		return exceptionCode;
	}

	public String applyPrPromptChange(String sqlQuery, ReportsVb promptVb) {
		VisionUsersVb visionUserVb = CustomContextHolder.getContext();
		// VU_CLEB,VU_CLEB_AO,VU_CLEB_LV,VU_SBU,VU_PRODUCT,VU_OUC
		if (sqlQuery.contains("#PF_PROMPT_VALUE_1"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_1(", promptVb.getPromptValue1());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_2"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_2(", promptVb.getPromptValue2());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_3"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_3(", promptVb.getPromptValue3());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_4"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_4(", promptVb.getPromptValue4());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_5"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_5(", promptVb.getPromptValue5());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_6"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_6(", promptVb.getPromptValue6());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_7"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_7(", promptVb.getPromptValue7());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_8"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_8(", promptVb.getPromptValue8());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_9"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_9(", promptVb.getPromptValue9());
		if (sqlQuery.contains("#PF_PROMPT_VALUE_10"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_10(", promptVb.getPromptValue10());

		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_1"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_1(",
					promptVb.getPromptValue1().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_2"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_2(",
					promptVb.getPromptValue2().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_3"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_3(",
					promptVb.getPromptValue3().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_4"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_4(",
					promptVb.getPromptValue4().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_5"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_5(",
					promptVb.getPromptValue5().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_6"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_6(",
					promptVb.getPromptValue6().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_7"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_7(",
					promptVb.getPromptValue7().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_8"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_8(",
					promptVb.getPromptValue8().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_9"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_9(",
					promptVb.getPromptValue9().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_PROMPT_VALUE_10"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_PROMPT_VALUE_10(",
					promptVb.getPromptValue10().replaceAll("'", ""));

		if (sqlQuery.contains("#PF_DDKEY1"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY1(", promptVb.getDrillDownKey1());
		if (sqlQuery.contains("#PF_DDKEY2"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY2(", promptVb.getDrillDownKey2());
		if (sqlQuery.contains("#PF_DDKEY3"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY3(", promptVb.getDrillDownKey3());
		if (sqlQuery.contains("#PF_DDKEY4"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY4(", promptVb.getDrillDownKey4());
		if (sqlQuery.contains("#PF_DDKEY5"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY5(", promptVb.getDrillDownKey5());
		if (sqlQuery.contains("#PF_DDKEY6"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY6(", promptVb.getDrillDownKey6());
		if (sqlQuery.contains("#PF_DDKEY7"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY7(", promptVb.getDrillDownKey7());
		if (sqlQuery.contains("#PF_DDKEY8"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY8(", promptVb.getDrillDownKey8());
		if (sqlQuery.contains("#PF_DDKEY9"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY9(", promptVb.getDrillDownKey9());
		if (sqlQuery.contains("#PF_DDKEY10"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY10(", promptVb.getDrillDownKey10());
		if (sqlQuery.contains("#PF_DDKEY0"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY10(", promptVb.getDrillDownKey0());

		if (sqlQuery.contains("#PF_NS_DDKEY1"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY1(", promptVb.getDrillDownKey1().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY2"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY2(", promptVb.getDrillDownKey2().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY3"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY3(", promptVb.getDrillDownKey3().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY4"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY4(", promptVb.getDrillDownKey4().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY5"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY5(", promptVb.getDrillDownKey5().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY6"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY6(", promptVb.getDrillDownKey6().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY7"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY7(", promptVb.getDrillDownKey7().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY8"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY8(", promptVb.getDrillDownKey8().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY9"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY9(", promptVb.getDrillDownKey9().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY10"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY10(", promptVb.getDrillDownKey10().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_DDKEY0"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY10(", promptVb.getDrillDownKey0().replaceAll("'", ""));

		if (sqlQuery.contains("#PF_LOCAL_FILTER_1"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_1(", promptVb.getDataFilter1());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_2"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_2(", promptVb.getDataFilter2());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_3"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_3(", promptVb.getDataFilter3());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_4"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_4(", promptVb.getDataFilter4());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_5"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_5(", promptVb.getDataFilter5());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_6"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_6(", promptVb.getDataFilter6());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_7"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_7(", promptVb.getDataFilter7());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_8"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_8(", promptVb.getDataFilter8());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_9"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_9(", promptVb.getDataFilter9());
		if (sqlQuery.contains("#PF_LOCAL_FILTER_10"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER_10(", promptVb.getDataFilter10());

		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_1"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_1(",
					promptVb.getDataFilter1().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_2"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_2(",
					promptVb.getDataFilter2().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_3"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_3(",
					promptVb.getDataFilter3().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_4"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_4(",
					promptVb.getDataFilter4().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_5"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_5(",
					promptVb.getDataFilter5().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_6"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_6(",
					promptVb.getDataFilter6().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_7"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_7(",
					promptVb.getDataFilter7().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_8"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_8(",
					promptVb.getDataFilter8().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_9"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_9(",
					promptVb.getDataFilter9().replaceAll("'", ""));
		if (sqlQuery.contains("#PF_NS_LOCAL_FILTER_10"))
			sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_NS_LOCAL_FILTER_10(",
					promptVb.getDataFilter10().replaceAll("'", ""));

		return sqlQuery;
	}

	public String pfPromptHashReplace(String query, String restrictStr, String restrictVal) {
		try {
			String replaceStr = "";
			String orgSbuStr = StringUtils.substringBetween(query, restrictStr, ")#");
			if (ValidationUtil.isValid(restrictVal) && !"'ALL'".equalsIgnoreCase(restrictVal))
				replaceStr = " AND " + orgSbuStr + " IN (" + restrictVal + ")";
			restrictStr = restrictStr.replace("(", "\\(");
			orgSbuStr = orgSbuStr.replace("|", "\\|");
			query = query.replaceAll(restrictStr + orgSbuStr + "\\)#", replaceStr);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return query;
	}

	public String applySpecialPrompts(String sqlQuery, ReportsVb promptVb) {
		try {
			String promptArray[][] = new String[10][10];
			if (ValidationUtil.isValid(promptVb.getPromptValue1())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue1(), sqlQuery, promptArray, 0);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue2())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue2(), sqlQuery, promptArray, 1);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue3())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue3(), sqlQuery, promptArray, 2);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue4())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue4(), sqlQuery, promptArray, 3);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue5())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue5(), sqlQuery, promptArray, 4);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue6())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue6(), sqlQuery, promptArray, 5);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue7())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue7(), sqlQuery, promptArray, 6);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue8())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue8(), sqlQuery, promptArray, 7);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue9())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue9(), sqlQuery, promptArray, 8);
			}
			if (ValidationUtil.isValid(promptVb.getPromptValue10())) {
				promptArray = commonDao.PromptSplit(promptVb.getPromptValue10(), sqlQuery, promptArray, 9);
			}
			for (int i = 1; i < 11; i++) {
				for (int j = 1; j < 11; j++) {
					if (ValidationUtil.isValid(promptArray[i - 1][j - 1])) {
						if (sqlQuery.contains("#PROMPT_VALUE_" + i + "." + j + "#"))
							sqlQuery = sqlQuery.replace("#PROMPT_VALUE_" + i + "." + j + "#",
									promptArray[i - 1][j - 1]);
						if (sqlQuery.contains("#NS_PROMPT_VALUE_" + i + "." + j + "#"))
							sqlQuery = sqlQuery.replace("#NS_PROMPT_VALUE_" + i + "." + j + "#",
									promptArray[i - 1][j - 1].replaceAll("'", ""));
						if (sqlQuery.contains("#PF_PROMPT_VALUE_" + i + "." + j + "#"))
							sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_PROMPT_VALUE_" + i + "." + j + "(",
									promptArray[i - 1][j - 1]);
					} else {
						sqlQuery = sqlQuery.replace("#PROMPT_VALUE_" + i + "." + j + "#", "NULL");
						sqlQuery = sqlQuery.replace("#NS_PROMPT_VALUE_" + i + "." + j + "#", "NULL");
					}
				}
			}

			String promptDrillDwnArray[][] = new String[10][10];
			if (ValidationUtil.isValid(promptVb.getDrillDownKey1())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey1(), sqlQuery, promptDrillDwnArray,
						0);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey2())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey2(), sqlQuery, promptDrillDwnArray,
						1);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey3())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey3(), sqlQuery, promptDrillDwnArray,
						2);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey4())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey4(), sqlQuery, promptDrillDwnArray,
						3);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey5())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey5(), sqlQuery, promptDrillDwnArray,
						4);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey6())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey6(), sqlQuery, promptDrillDwnArray,
						5);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey7())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey7(), sqlQuery, promptDrillDwnArray,
						6);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey8())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey8(), sqlQuery, promptDrillDwnArray,
						7);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey9())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey9(), sqlQuery, promptDrillDwnArray,
						8);
			}
			if (ValidationUtil.isValid(promptVb.getDrillDownKey10())) {
				promptDrillDwnArray = commonDao.PromptSplit(promptVb.getDrillDownKey10(), sqlQuery, promptDrillDwnArray,
						9);
			}
			for (int i = 1; i < 11; i++) {
				for (int j = 1; j < 11; j++) {
					if (ValidationUtil.isValid(promptDrillDwnArray[i - 1][j - 1])) {
						if (sqlQuery.contains("#DDKEY" + i + "." + j + "#"))
							sqlQuery = sqlQuery.replace("#DDKEY" + i + "." + j + "#",
									promptDrillDwnArray[i - 1][j - 1]);
						if (sqlQuery.contains("#NS_DDKEY" + i + "." + j + "#"))
							sqlQuery = sqlQuery.replace("#NS_DDKEY" + i + "." + j + "#",
									promptDrillDwnArray[i - 1][j - 1].replaceAll(",", ""));
						if (sqlQuery.contains("#PF_DDKEY" + i + "." + j + "#"))
							sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_DDKEY" + i + "." + j + "(",
									promptDrillDwnArray[i - 1][j - 1]);
					} else {
						sqlQuery = sqlQuery.replace("#DDKEY" + i + "." + j + "#", "NULL");
						sqlQuery = sqlQuery.replace("#NS_DDKEY" + i + "." + j + "#", "NULL");
					}
				}
			}

			String promptLocalFiltArray[][] = new String[10][10];
			if (ValidationUtil.isValid(promptVb.getDataFilter1())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter1(), sqlQuery, promptLocalFiltArray,
						0);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter2())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter2(), sqlQuery, promptLocalFiltArray,
						1);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter3())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter3(), sqlQuery, promptLocalFiltArray,
						2);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter4())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter4(), sqlQuery, promptLocalFiltArray,
						3);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter5())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter5(), sqlQuery, promptLocalFiltArray,
						4);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter6())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter6(), sqlQuery, promptLocalFiltArray,
						5);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter7())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter7(), sqlQuery, promptLocalFiltArray,
						6);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter8())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter8(), sqlQuery, promptLocalFiltArray,
						7);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter9())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter9(), sqlQuery, promptLocalFiltArray,
						8);
			}
			if (ValidationUtil.isValid(promptVb.getDataFilter10())) {
				promptLocalFiltArray = commonDao.PromptSplit(promptVb.getDataFilter10(), sqlQuery, promptLocalFiltArray,
						9);
			}
			for (int i = 1; i < 11; i++) {
				for (int j = 1; j < 11; j++) {
					if (ValidationUtil.isValid(promptLocalFiltArray[i - 1][j - 1])) {
//						if(sqlQuery.contains("#LOCAL_FILTER"+i+"."+j+"#"))
						sqlQuery = sqlQuery.replace("#LOCAL_FILTER" + i + "." + j + "#",
								promptLocalFiltArray[i - 1][j - 1]);
						if (sqlQuery.contains("#NS_LOCAL_FILTER" + i + "." + j + "#"))
							sqlQuery = sqlQuery.replace("#NS_LOCAL_FILTER" + i + "." + j + "#",
									promptLocalFiltArray[i - 1][j - 1].replaceAll(",", ""));
						if (sqlQuery.contains("#PF_LOCAL_FILTER" + i + "." + j + "#"))
							sqlQuery = pfPromptHashReplace(sqlQuery, "#PF_LOCAL_FILTER" + i + "." + j + "(",
									promptLocalFiltArray[i - 1][j - 1]);
					} else {
						sqlQuery = sqlQuery.replace("#LOCAL_FILTER" + i + "." + j + "#", "NULL");
						sqlQuery = sqlQuery.replace("#NS_LOCAL_FILTER" + i + "." + j + "#", "NULL");
					}
				}
			}
			return sqlQuery;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return sqlQuery;
		}
	}

	@SuppressWarnings("unchecked")
	public ExceptionCode extractReportData(ReportsVb vObj, Connection conExt, Boolean exportFlag) {
		ArrayList datalst = new ArrayList();
		ExceptionCode exceptionCode = new ExceptionCode();
		int totalRows = 0;
		Statement stmt1 = null;
		String resultFetchTable = "";
		String sqlQuery = "";
		String tempSequence = getMaxTempTableSeq();
		Paginationhelper<ReportsVb> paginationhelper = new Paginationhelper<ReportsVb>();
		String tmpTableOrg = String
				.valueOf("TMP_" + CustomContextHolder.getContext().getVisionId() + "_" + tempSequence);
		String tmpTableGrp = String
				.valueOf("TMPG_" + CustomContextHolder.getContext().getVisionId() + "_" + tempSequence);
		String finalTmpTable = "";
		Boolean tempTableAlreadyAvail = false;
		if (ValidationUtil.isValid(vObj.getActionType())) {
			tempTableAlreadyAvail = true;
		}

		try {
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			if (!tempTableAlreadyAvail) {

				sqlQuery = vObj.getFinalExeQuery();
				// totalRows = (int) vObj.getTotalRows();

				String createTabScript = "SELECT * FROM (" + sqlQuery + ") TA1";

				Boolean formatTypeAvail = false;
				ResultSet rsFormatCnt = null;
				if ("ORACLE".equalsIgnoreCase(databaseType)) {
					totalRows = stmt1.executeUpdate("CREATE TABLE " + tmpTableOrg + " AS (" + createTabScript + ")");

					rsFormatCnt = stmt1.executeQuery("SELECT * FROM USER_TAB_COLS WHERE TABLE_NAME = '"
							+ tmpTableOrg + "' AND COLUMN_NAME = 'FORMAT_TYPE' ");

					while (rsFormatCnt.next()) {
						formatTypeAvail = true;
					}
				} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
					totalRows = stmt1
							.executeUpdate("Select * into " + tmpTableOrg + " FROM (" + createTabScript + ") A");

					rsFormatCnt = stmt1.executeQuery("SELECT * FROM INFORMATION_SCHEMA.columns WHERE "
							+ "TABLE_NAME = '" + tmpTableOrg + "' AND COLUMN_NAME = 'FORMAT_TYPE' ");

					while (rsFormatCnt.next()) {
						formatTypeAvail = true;
					}
				}
				rsFormatCnt.close();

				resultFetchTable = tmpTableOrg;
				String formatTypeCond = "";
				if (formatTypeAvail && "Y".equalsIgnoreCase(vObj.getApplyGrouping()))
					formatTypeCond = "WHERE FORMAT_TYPE NOT IN ('S','FT') ";
				else
					formatTypeCond = "";
				if ("Y".equalsIgnoreCase(vObj.getApplyGrouping())) {
					String showMeasures = "";
					if (ValidationUtil.isValid(vObj.getShowMeasures())) {
						showMeasures = "," + vObj.getShowMeasures();
					} else
						showMeasures = "";

					String query = "SELECT " + vObj.getShowDimensions() + showMeasures + " FROM " + tmpTableOrg + " "
							+ formatTypeCond + " GROUP BY " + vObj.getShowDimensions();
					if ("ORACLE".equalsIgnoreCase(databaseType)) {
						totalRows = stmt1.executeUpdate("CREATE TABLE " + tmpTableGrp + " AS (" + query + ")");
					} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
						totalRows = stmt1.executeUpdate("Select * into " + tmpTableGrp + " FROM (" + query + ") A");
					}
					resultFetchTable = tmpTableGrp;
				}
				if (totalRows == 0) {
					exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					exceptionCode.setErrorMsg("No Records Found!!");
					return exceptionCode;
				}
				/*
				 * Report Suite having the functionality of Pagination for every 5000 rows (Rows
				 * per page in parameterized in Vision_Variable
				 * PRD_REPORT_MAXROW/PRD_REPORT_MAX_PERPAGE
				 */
				if (exportFlag) {
					finalTmpTable = String
							.valueOf("TMPF" + ThreadLocalRandom.current().nextLong(100000000000L, 1000000000000L))
							+ vObj.getSubReportId();
					String sqlTempTable = ValidationUtil.convertQuery("SELECT * FROM " + resultFetchTable,
							vObj.getSortField());
					if ("ORACLE".equalsIgnoreCase(databaseType)) {
						totalRows = stmt1.executeUpdate("CREATE TABLE " + finalTmpTable + " AS (" + sqlTempTable + ")");
						sqlTempTable = finalTmpTable;
					} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
						totalRows = stmt1
								.executeUpdate("Select * into " + finalTmpTable + " FROM (" + sqlTempTable + ") A");
						sqlTempTable = "Select * from " + finalTmpTable + " ";
					}
					sqlQuery = paginationhelper.reportFetchPage(sqlTempTable, vObj.getCurrentPage(),
							vObj.getMaxRecords(), totalRows);
					vObj.setActionType(finalTmpTable);
					vObj.setTotalRows(totalRows);
				} else {
					String sqlTempTable = "SELECT * FROM " + resultFetchTable + "";
					sqlQuery = paginationhelper.reportFetchPage(
							ValidationUtil.convertQuery(sqlTempTable, vObj.getSortField()), vObj.getCurrentPage(),
							vObj.getMaxRecords(), totalRows);
				}

			} else {
				if (exportFlag) {
					finalTmpTable = vObj.getActionType();
					String sqlTempTable = "Select * from " + finalTmpTable + " ";
					sqlQuery = paginationhelper.reportFetchPage(sqlTempTable, vObj.getCurrentPage(),
							vObj.getMaxRecords(), (int) vObj.getTotalRows());
					vObj.setActionType(finalTmpTable);
					vObj.setTotalRows(vObj.getTotalRows());
					float noOfPages = (float) vObj.getTotalRows() / (float) vObj.getMaxRecPerPage();
					int MaxPageNo = Math.round(noOfPages);
					if (vObj.getCurrentPage() > MaxPageNo)
						exportFlag = false;
				}
			}
			// Id["+vObj.getReportId()+"]SubReport["+vObj.getSubReportId()+"]");
			ResultSet rsData = stmt1.executeQuery(sqlQuery);// Final Result Data Query Execution
			// Id["+vObj.getReportId()+"]SubReport["+vObj.getSubReportId()+"]");

			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			HashMap<String, String> columns = new HashMap<String, String>();
			ArrayList<String> collst = new ArrayList<>();
			ArrayList<ColumnHeadersVb> colHeadersDetLst = new ArrayList<>();
			String reportUnusedColumns = commonDao.findVisionVariableValue("PRD_REPORT_UNUSED_COLS");
			String reportcolTypes = commonDao.findVisionVariableValue("PRD_REPORT_COL_TYPES");
			Boolean columnHeaderFetch = false;
			int rowNum = 1;
			int ctr = 0;
			while (rsData.next()) {
				columns = new HashMap<String, String>();
				for (int cn = 1; cn <= colCount; cn++) {
					ColumnHeadersVb colHeader = new ColumnHeadersVb();
					String columnName = metaData.getColumnName(cn);
					String colType = metaData.getColumnTypeName(cn);
					ctr++;
					if ("DD_KEY_ID".equalsIgnoreCase(columnName.toUpperCase())) {
						columns.put("DDKEYID", rsData.getString(columnName));
					} else {
						columns.put(columnName.toUpperCase(), rsData.getString(columnName));
					}
					if (!columnHeaderFetch) {
						collst.add(columnName.toUpperCase());
						if (vObj.getColumnHeaderslst() == null || vObj.getColumnHeaderslst().isEmpty()) {
							if (ValidationUtil.isValid(reportUnusedColumns)
									&& reportUnusedColumns.toUpperCase().contains(columnName.toUpperCase())) {
								ctr = ctr - 1;
								continue;
							}
							colHeader.setLabelColNum(ctr);
							colHeader.setLabelRowNum(1);
							// colHeader.setCaption(WordUtils.capitalizeFully(columnName.replaceAll("_", "
							// ")));
							colHeader.setCaption(columnName);
							colHeader.setDbColumnName(columnName.toUpperCase());
							if (ValidationUtil.isValid(reportcolTypes)
									&& reportcolTypes.toUpperCase().contains(colType.toUpperCase()))
								colHeader.setColType("T");
							else
								colHeader.setColType("N");
							colHeader.setRowspan(0);
							colHeader.setColspan(0);
							colHeader.setDrillDownLabel(false);
							colHeadersDetLst.add(colHeader);
						}

					}
				}
				columnHeaderFetch = true;
				if (!columnHeaderFetch) {
					StringJoiner missingColumns = new StringJoiner(",");
					vObj.getColumnHeaderslst().forEach(colHeadersDataVb -> {
						if (!collst.contains(colHeadersDataVb.getDbColumnName())) {
							missingColumns.add(colHeadersDataVb.getDbColumnName());
						}
					});
					if (ValidationUtil.isValid(missingColumns.toString())) {
						exceptionCode.setErrorMsg(
								missingColumns + "these Source columns are not maintained  in the Result set");
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						break;
					}
				}
				columns.put("INDEXING", "" + rowNum);// This is only for front-end scroll purpose not used anywhere in
														// report.
				datalst.add(columns);
				if (rsData.getRow() == vObj.getMaxRecords())
					break;

				columnHeaderFetch = true;
				rowNum++;
			}
			rsData.close();
			// HashMap<String,String> columns =
			// (HashMap<String,String>)getJdbcTemplate().query(sqlQuery, mapper);
			if (totalRows != 0) {
				vObj.setTotalRows(totalRows);
				exceptionCode.setRequest((int) totalRows);
			} else {
				vObj.setTotalRows(vObj.getTotalRows());
				exceptionCode.setRequest((int) vObj.getTotalRows());
			}

			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(datalst);
			exceptionCode.setOtherInfo(vObj);
			if (colHeadersDetLst != null && colHeadersDetLst.size() > 0)
				exceptionCode.setResponse1(colHeadersDetLst);
		} catch (Exception e) {
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			if (!ValidationUtil.isValid(exceptionCode.getErrorMsg())) {
				exceptionCode.setErrorMsg(e.getMessage());
			}
			return exceptionCode;
		} finally {
			if (!exportFlag) {
				try {
					if ("ORACLE".equalsIgnoreCase(databaseType)) {
						if (ValidationUtil.isValid(resultFetchTable))
							stmt1.executeUpdate("DROP TABLE " + resultFetchTable + " PURGE ");
						if (ValidationUtil.isValid(finalTmpTable))
							stmt1.executeUpdate("DROP TABLE " + finalTmpTable + " PURGE ");
						vObj.setActionType("");
					} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
						if (ValidationUtil.isValid(resultFetchTable))
							stmt1.executeUpdate("DROP TABLE " + resultFetchTable);
						if (ValidationUtil.isValid(finalTmpTable))
							stmt1.executeUpdate("DROP TABLE " + finalTmpTable);
						vObj.setActionType("");
					}
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return exceptionCode;
	}

	public String getMaxTempTableSeq() {
		StringBuffer strBufApprove = new StringBuffer();
		if ("ORACLE".equalsIgnoreCase(databaseType)) {
			strBufApprove = new StringBuffer("SELECT PRD_REP_MAX_SEQUENCE.NEXTVAL FROM dual");
		} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
			strBufApprove = new StringBuffer("SELECT NEXT VALUE FOR PRD_REP_MAX_SEQUENCE");
		}
		String max = getJdbcTemplate().queryForObject(strBufApprove.toString(), String.class);
		return max;
	}

	public String getDependentPrompt(String reportId, String depPromptString) {
		Object args[] = { reportId };
		return getJdbcTemplate().queryForObject(
				"SELECT " + depPromptString + " FROM PRD_REPORT_MASTER  WHERE REPORT_ID = ?", args, String.class);
	}
	
	public String getColumnXmlWithReportId(String reportId) {
		List<ColumnHeadersVb> collTemp = null;
		try {
			String query = " Select COLUMN_XML from PRD_REPORT_COLUMN WHERE REPORT_ID = ? ";
			Object[] lParams = { reportId };
			return getJdbcTemplate().queryForObject(query, lParams, String.class);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public ArrayList<ColumnHeadersVb> getColumnHeaders(String colHeadersXml,ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList<ColumnHeadersVb> colHeaders = new ArrayList<ColumnHeadersVb>();
		try {
			if (ValidationUtil.isValid(colHeadersXml)) {
				colHeadersXml = ValidationUtil.isValid(colHeadersXml)?colHeadersXml.replaceAll("\n", "").replaceAll("\r", ""): "";
				String colDetXml = CommonUtils.getValueForXmlTag(colHeadersXml, "COLUMNS");
				int colCount = 0 ;
				colCount = Integer.parseInt(CommonUtils.getValueForXmlTag(colDetXml, "COLUMN_COUNT"));
				for (int i = 1; i <= colCount; i++) {
					ColumnHeadersVb colHeadersVb = new ColumnHeadersVb();
					String refXml = CommonUtils.getValueForXmlTag(colDetXml, "COLUMN" + i);
					if (!ValidationUtil.isValid(refXml))
						continue;
					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "LABLE_ROW_NUM")))
						colHeadersVb.setLabelRowNum(Integer.parseInt(CommonUtils.getValueForXmlTag(refXml, "LABLE_ROW_NUM")));

					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "LABLE_COL_NUM")))
						colHeadersVb.setLabelColNum(Integer.parseInt(CommonUtils.getValueForXmlTag(refXml, "LABLE_COL_NUM")));
					
					String caption = CommonUtils.getValueForXmlTag(refXml, "CAPTION");
					//#PROMPT_VALUE_1#!@DD-Mon-RRRR!@-0
					
					if (caption.contains("PROMPT_VALUE")) {
						if (caption.contains("!@")) {
							caption = formQueryForCaption(caption, vObject);
						} else {
							caption = replacePromptVariables(caption, vObject, false);
						}
					}
					if(caption.contains("#")) {
						String cap  = caption.replaceAll("#", "");
						List resultLst = new ArrayList<>();
						resultLst = commonDao.getDateFormatforCaption();
						HashMap<String, String> dateMap = (HashMap<String, String>) resultLst.get(0);
						String val = dateMap.get(cap);
						caption  = caption.replaceAll(caption,val);
						//caption = replaceDate(caption, val);
					}
					
					colHeadersVb.setCaption(caption);
					
					//colHeadersVb.setCaption(CommonUtils.getValueForXmlTag(refXml, "CAPTION"));
					colHeadersVb.setColType(CommonUtils.getValueForXmlTag(refXml, "COL_TYPE"));

					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "ROW_SPAN")))
						colHeadersVb.setRowspan(Integer.parseInt(CommonUtils.getValueForXmlTag(refXml, "ROW_SPAN")));

					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "COL_SPAN")))
						colHeadersVb.setColspan(Integer.parseInt(CommonUtils.getValueForXmlTag(refXml, "COL_SPAN")));
					String dbColName = CommonUtils.getValueForXmlTag(refXml, "SOURCE_COLUMN").toUpperCase();
					colHeadersVb.setDbColumnName(dbColName);
					String drillDownLabel = CommonUtils.getValueForXmlTag(refXml, "DRILLDOWN_LABEL_FLAG");
					if (ValidationUtil.isValid(drillDownLabel) && "Y".equalsIgnoreCase(drillDownLabel)) {
						colHeadersVb.setDrillDownLabel(true);
					} else {
						colHeadersVb.setDrillDownLabel(false);
					}
					colHeadersVb.setScaling(CommonUtils.getValueForXmlTag(refXml, "SCALING"));
					colHeadersVb.setDecimalCnt(CommonUtils.getValueForXmlTag(refXml, "DECIMALCNT"));
					colHeadersVb.setColumnWidth(CommonUtils.getValueForXmlTag(refXml, "COLUMN_WIDTH"));
					colHeadersVb.setGroupingFlag(CommonUtils.getValueForXmlTag(refXml, "GROUPING_FLAG"));
					String sumFlag = CommonUtils.getValueForXmlTag(refXml, "SUM_FLAG");
					if (ValidationUtil.isValid(sumFlag)) {
						colHeadersVb.setSumFlag(sumFlag);
					} else {
						colHeadersVb.setSumFlag("N");
					}
					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "COLOR_DIFF")))
						colHeadersVb.setColorDiff(CommonUtils.getValueForXmlTag(refXml, "COLOR_DIFF"));
					
					 if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "DISPLAY_ORDER")))
						colHeadersVb.setDisplayOrder(CommonUtils.getValueForXmlTag(refXml, "DISPLAY_ORDER"));
					
					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "FONT_STYLE")))
						colHeadersVb.setFontStyle(CommonUtils.getValueForXmlTag(refXml, "FONT_STYLE"));
					
					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "BG_COLOR")))
						colHeadersVb.setBgColor(CommonUtils.getValueForXmlTag(refXml, "BG_COLOR"));
					
					if (ValidationUtil.isValid(CommonUtils.getValueForXmlTag(refXml, "FONT_COLOR")))
						colHeadersVb.setFontColor(CommonUtils.getValueForXmlTag(refXml, "FONT_COLOR"));
					
					colHeaders.add(colHeadersVb);
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return colHeaders;
	}
	
	public String formQueryForCaption(String caption,ReportsVb vObject) {
		String sql = "";
		String retStr = "";
		String promptStr = "";
		String[] captionArr = caption.split("!@");
		if (captionArr.length == 3) {
			promptStr = replacePromptVariables(captionArr[0], vObject, false);
			if ("ORACLE".equalsIgnoreCase(databaseType)) {
				if ("dd-Mon-RRRR".equalsIgnoreCase(captionArr[1]) || "DD-Mon-RRRR".equalsIgnoreCase(captionArr[1])) {
					sql = "Select To_Char(To_Date(" + promptStr + ")"+captionArr[2]+",'" + captionArr[1] + "')CAPTION from Dual";
					//Select To_Char(To_Date('10-Mar-2022')-1,'DD-Mon-RRRR') from Dual
				} else if ("Mon-RRRR".equalsIgnoreCase(captionArr[1])) {
					promptStr = promptStr.replaceAll("'", "");
					sql = "Select To_Char(add_months('01-"+promptStr+"',"+captionArr[2]+"),'"+captionArr[1]+"')CAPTION from Dual";
					//Select To_Char(add_months('01-Nov-2022',-2),'Mon-RRRR') from Dual
				}
			}
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataQuery(sql);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				List result = (ArrayList)exceptionCode.getResponse();
				if(result != null && result.size() > 0) {
					HashMap<String,String> map = (HashMap<String, String>)result.get(0);
					retStr = map.get("CAPTION");
				}
			}
		}
		
		return retStr;
	}
	
	public String replacePromptVariables(String reportQuery, ReportsVb promptsVb, Boolean isProcedure) {
		try {
			if (isProcedure)
				promptsVb = replaceProcedurePrompt(promptsVb);

			if(!ValidationUtil.isValid(promptsVb.getPromptValue2())) {
				promptsVb.setPromptValue2("'ALL-ALL'");
			}
			if(!ValidationUtil.isValid(promptsVb.getPromptValue3())) {
				promptsVb.setPromptValue3("'ALL-ALL'");
			}
			if(!ValidationUtil.isValid(promptsVb.getPromptValue4())) {
				promptsVb.setPromptValue4("'ALL-ALL'");
			}
			
			if (ValidationUtil.isValid(promptsVb.getPromptValue2())) {
				promptsVb.setPromptValue2(promptsVb.getPromptValue2().replaceAll("ALL-ALL", "ALL"));
			}
			if (ValidationUtil.isValid(promptsVb.getPromptValue3())) {
				promptsVb.setPromptValue3(promptsVb.getPromptValue3().replaceAll("ALL-ALL", "ALL"));
			}
			if (ValidationUtil.isValid(promptsVb.getPromptValue4())) {
				promptsVb.setPromptValue4(promptsVb.getPromptValue4().replaceAll("ALL-ALL", "ALL"));
			}
			
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_1#",
					ValidationUtil.isValid(promptsVb.getPromptValue1()) ? promptsVb.getPromptValue1() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_2#",
					ValidationUtil.isValid(promptsVb.getPromptValue2()) ? promptsVb.getPromptValue2() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_3#",
					ValidationUtil.isValid(promptsVb.getPromptValue3()) ? promptsVb.getPromptValue3() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_4#",
					ValidationUtil.isValid(promptsVb.getPromptValue4()) ? promptsVb.getPromptValue4() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_5#",
					ValidationUtil.isValid(promptsVb.getPromptValue5()) ? promptsVb.getPromptValue5() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_6#",
					ValidationUtil.isValid(promptsVb.getPromptValue6()) ? promptsVb.getPromptValue6() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_7#",
					ValidationUtil.isValid(promptsVb.getPromptValue7()) ? promptsVb.getPromptValue7() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_8#",
					ValidationUtil.isValid(promptsVb.getPromptValue8()) ? promptsVb.getPromptValue8() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_9#",
					ValidationUtil.isValid(promptsVb.getPromptValue9()) ? promptsVb.getPromptValue9() : "''");
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_10#",
					ValidationUtil.isValid(promptsVb.getPromptValue10()) ? promptsVb.getPromptValue10() : "''");

			reportQuery = reportQuery.replaceAll("#TIME_ZONE#",
					ValidationUtil.isValid(commonDao.findVisionVariableValue("TIME_ZONE"))
							? commonDao.findVisionVariableValue("TIME_ZONE")
							: "Asia/Dubai");
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_0#",
					promptsVb.getPromptValue0().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_1#",
					promptsVb.getPromptValue1().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_2#",
					promptsVb.getPromptValue2().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_3#",
					promptsVb.getPromptValue3().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_4#",
					promptsVb.getPromptValue4().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_5#",
					promptsVb.getPromptValue5().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_6#",
					promptsVb.getPromptValue6().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_7#",
					promptsVb.getPromptValue7().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_8#",
					promptsVb.getPromptValue8().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_9#",
					promptsVb.getPromptValue9().replaceAll("'", ""));
			reportQuery = reportQuery.replaceAll("#NS_PROMPT_VALUE_10#",
					promptsVb.getPromptValue10().replaceAll("'", ""));

			reportQuery = reportQuery.replaceAll("#DDKEY1#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey1()) ? promptsVb.getDrillDownKey1() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY2#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey2()) ? promptsVb.getDrillDownKey2() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY3#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey3()) ? promptsVb.getDrillDownKey3() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY4#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey4()) ? promptsVb.getDrillDownKey4() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY5#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey5()) ? promptsVb.getDrillDownKey5() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY6#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey6()) ? promptsVb.getDrillDownKey6() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY7#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey7()) ? promptsVb.getDrillDownKey7() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY8#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey8()) ? promptsVb.getDrillDownKey8() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY9#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey9()) ? promptsVb.getDrillDownKey9() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY10#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey10()) ? promptsVb.getDrillDownKey10() : "''");
			reportQuery = reportQuery.replaceAll("#DDKEY0#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey0()) ? promptsVb.getDrillDownKey0() : "''");

			reportQuery = reportQuery.replaceAll("#NS_DDKEY1#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey1())
							? promptsVb.getDrillDownKey1().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY2#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey2())
							? promptsVb.getDrillDownKey2().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY3#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey3())
							? promptsVb.getDrillDownKey3().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY4#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey4())
							? promptsVb.getDrillDownKey4().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY5#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey5())
							? promptsVb.getDrillDownKey5().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY6#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey6())
							? promptsVb.getDrillDownKey6().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY7#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey7())
							? promptsVb.getDrillDownKey7().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY8#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey8())
							? promptsVb.getDrillDownKey8().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY9#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey9())
							? promptsVb.getDrillDownKey9().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY10#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey10())
							? promptsVb.getDrillDownKey10().replaceAll("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_DDKEY0#",
					ValidationUtil.isValid(promptsVb.getDrillDownKey0())
							? promptsVb.getDrillDownKey0().replaceAll("'", "")
							: "''");

			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_1#",
					ValidationUtil.isValid(promptsVb.getDataFilter1()) ? promptsVb.getDataFilter1() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_2#",
					ValidationUtil.isValid(promptsVb.getDataFilter2()) ? promptsVb.getDataFilter2() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_3#",
					ValidationUtil.isValid(promptsVb.getDataFilter3()) ? promptsVb.getDataFilter3() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_4#",
					ValidationUtil.isValid(promptsVb.getDataFilter4()) ? promptsVb.getDataFilter4() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_5#",
					ValidationUtil.isValid(promptsVb.getDataFilter5()) ? promptsVb.getDataFilter5() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_6#",
					ValidationUtil.isValid(promptsVb.getDataFilter6()) ? promptsVb.getDataFilter6() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_7#",
					ValidationUtil.isValid(promptsVb.getDataFilter7()) ? promptsVb.getDataFilter7() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_8#",
					ValidationUtil.isValid(promptsVb.getDataFilter8()) ? promptsVb.getDataFilter8() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_9#",
					ValidationUtil.isValid(promptsVb.getDataFilter9()) ? promptsVb.getDataFilter9() : "''");
			reportQuery = reportQuery.replaceAll("#LOCAL_FILTER_10#",
					ValidationUtil.isValid(promptsVb.getDataFilter10()) ? promptsVb.getDataFilter10() : "''");

			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_1#",
					ValidationUtil.isValid(promptsVb.getDataFilter1()) ? promptsVb.getDataFilter1().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_2#",
					ValidationUtil.isValid(promptsVb.getDataFilter2()) ? promptsVb.getDataFilter2().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_3#",
					ValidationUtil.isValid(promptsVb.getDataFilter3()) ? promptsVb.getDataFilter3().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_4#",
					ValidationUtil.isValid(promptsVb.getDataFilter4()) ? promptsVb.getDataFilter4().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_5#",
					ValidationUtil.isValid(promptsVb.getDataFilter5()) ? promptsVb.getDataFilter5().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_6#",
					ValidationUtil.isValid(promptsVb.getDataFilter6()) ? promptsVb.getDataFilter6().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_7#",
					ValidationUtil.isValid(promptsVb.getDataFilter7()) ? promptsVb.getDataFilter7().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_8#",
					ValidationUtil.isValid(promptsVb.getDataFilter8()) ? promptsVb.getDataFilter8().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_9#",
					ValidationUtil.isValid(promptsVb.getDataFilter9()) ? promptsVb.getDataFilter9().replace("'", "")
							: "''");
			reportQuery = reportQuery.replaceAll("#NS_LOCAL_FILTER_10#",
					ValidationUtil.isValid(promptsVb.getDataFilter10()) ? promptsVb.getDataFilter10().replace("'", "")
							: "''");

			reportQuery = reportQuery.replaceAll("#FILTER_POSITION#",
					ValidationUtil.isValid(promptsVb.getFilterPosition()) ? "'" + promptsVb.getFilterPosition() + "'"
							: "''");

			reportQuery = reportQuery.replaceAll("#REPORT_ID#", "'" + promptsVb.getReportId() + "'");
			reportQuery = reportQuery.replaceAll("#SUB_REPORT_ID#", "'" + promptsVb.getSubReportId() + "'");
			reportQuery = reportQuery.replaceAll("#VISION_ID#",
					"'" + CustomContextHolder.getContext().getVisionId() + "'");
			reportQuery = reportQuery.replaceAll("#SESSION_ID#", "'" + promptsVb.getDateCreation() + "'");
			reportQuery = reportQuery.replaceAll("#SCALING_FACTOR#",
					ValidationUtil.isValid(promptsVb.getScalingFactor()) ? promptsVb.getScalingFactor() : "1");

			if (promptsVb.getReportId().contains("MPR") && ValidationUtil.isValid(promptsVb.getPromptValue6())) {
				reportQuery = reportQuery.replaceAll("#PYM#", getDateFormat(promptsVb.getPromptValue6(), "PYM"));
				reportQuery = reportQuery.replaceAll("#NYM#", getDateFormat(promptsVb.getPromptValue6(), "NYM"));
				reportQuery = reportQuery.replaceAll("#PM#", getDateFormat(promptsVb.getPromptValue6(), "PM"));
				reportQuery = reportQuery.replaceAll("#NM#", getDateFormat(promptsVb.getPromptValue6(), "NM"));
				reportQuery = reportQuery.replaceAll("#CM#", getDateFormat(promptsVb.getPromptValue6(), "CM"));
				reportQuery = reportQuery.replaceAll("#CY#", getDateFormat(promptsVb.getPromptValue6(), "CY"));
				reportQuery = reportQuery.replaceAll("#PY#", getDateFormat(promptsVb.getPromptValue6(), "PY"));
			}

			reportQuery = commonDao.applyUserRestriction(reportQuery);
			reportQuery = applyPrPromptChange(reportQuery, promptsVb);
			reportQuery = applySpecialPrompts(reportQuery, promptsVb);

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return reportQuery;
	}
	
	public ReportsVb replaceProcedurePrompt(ReportsVb vObject) {
		vObject.setPromptValue1(
				ValidationUtil.isValid(vObject.getPromptValue1()) ? vObject.getPromptValue1().replaceAll("','", "'',''")
						: vObject.getPromptValue1());
		vObject.setPromptValue2(
				ValidationUtil.isValid(vObject.getPromptValue2()) ? vObject.getPromptValue2().replaceAll("','", "'',''")
						: vObject.getPromptValue2());
		vObject.setPromptValue3(
				ValidationUtil.isValid(vObject.getPromptValue3()) ? vObject.getPromptValue3().replaceAll("','", "'',''")
						: vObject.getPromptValue3());
		vObject.setPromptValue4(
				ValidationUtil.isValid(vObject.getPromptValue4()) ? vObject.getPromptValue4().replaceAll("','", "'',''")
						: vObject.getPromptValue4());
		vObject.setPromptValue5(
				ValidationUtil.isValid(vObject.getPromptValue5()) ? vObject.getPromptValue5().replaceAll("','", "'',''")
						: vObject.getPromptValue5());
		vObject.setPromptValue6(
				ValidationUtil.isValid(vObject.getPromptValue6()) ? vObject.getPromptValue6().replaceAll("','", "'',''")
						: vObject.getPromptValue6());
		vObject.setPromptValue7(
				ValidationUtil.isValid(vObject.getPromptValue7()) ? vObject.getPromptValue7().replaceAll("','", "'',''")
						: vObject.getPromptValue7());
		vObject.setPromptValue8(
				ValidationUtil.isValid(vObject.getPromptValue8()) ? vObject.getPromptValue8().replaceAll("','", "'',''")
						: vObject.getPromptValue8());
		vObject.setPromptValue9(
				ValidationUtil.isValid(vObject.getPromptValue9()) ? vObject.getPromptValue9().replaceAll("','", "'',''")
						: vObject.getPromptValue9());
		vObject.setPromptValue10(ValidationUtil.isValid(vObject.getPromptValue10())
				? vObject.getPromptValue10().replaceAll("','", "'',''")
				: vObject.getPromptValue10());
		vObject.setDataFilter1(
				ValidationUtil.isValid(vObject.getDataFilter1()) ? vObject.getDataFilter1().replaceAll("','", "'',''")
						: vObject.getDataFilter1());
		vObject.setDataFilter2(
				ValidationUtil.isValid(vObject.getDataFilter2()) ? vObject.getDataFilter2().replaceAll("','", "'',''")
						: vObject.getDataFilter2());
		vObject.setDataFilter3(
				ValidationUtil.isValid(vObject.getDataFilter3()) ? vObject.getDataFilter3().replaceAll("','", "'',''")
						: vObject.getDataFilter3());
		vObject.setDataFilter4(
				ValidationUtil.isValid(vObject.getDataFilter4()) ? vObject.getDataFilter4().replaceAll("','", "'',''")
						: vObject.getDataFilter4());
		vObject.setDataFilter5(
				ValidationUtil.isValid(vObject.getDataFilter5()) ? vObject.getDataFilter5().replaceAll("','", "'',''")
						: vObject.getDataFilter5());
		vObject.setDataFilter6(
				ValidationUtil.isValid(vObject.getDataFilter6()) ? vObject.getDataFilter6().replaceAll("','", "'',''")
						: vObject.getDataFilter6());
		vObject.setDataFilter7(
				ValidationUtil.isValid(vObject.getDataFilter7()) ? vObject.getDataFilter7().replaceAll("','", "'',''")
						: vObject.getDataFilter7());
		vObject.setDataFilter8(
				ValidationUtil.isValid(vObject.getDataFilter8()) ? vObject.getDataFilter8().replaceAll("','", "'',''")
						: vObject.getDataFilter8());
		vObject.setDataFilter9(
				ValidationUtil.isValid(vObject.getDataFilter9()) ? vObject.getDataFilter9().replaceAll("','", "'',''")
						: vObject.getDataFilter9());
		vObject.setDataFilter10(
				ValidationUtil.isValid(vObject.getDataFilter10()) ? vObject.getDataFilter10().replaceAll("','", "'',''")
						: vObject.getDataFilter10());
		return vObject;
	}
}
