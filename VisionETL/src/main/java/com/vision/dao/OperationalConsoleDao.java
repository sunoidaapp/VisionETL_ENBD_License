package com.vision.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.ValidationUtil;
import com.vision.vb.ConsoleDependencyFeedVb;
import com.vision.vb.DependencyCategoryVb;
import com.vision.vb.DependencyFeedVb;
import com.vision.vb.ETLAlertVb;
import com.vision.vb.ETLFeedEventBasedPropVb;
import com.vision.vb.ETLFeedProcessControlVb;
import com.vision.vb.ETLFeedTimeBasedPropVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedScheduleConfigVb;
import com.vision.vb.OperationalConsoleVb;
import com.vision.vb.SmartSearchVb;

@Component
public class OperationalConsoleDao extends AbstractDao<OperationalConsoleVb> {

	String pTableName = "";
	String templateSelPrep = "";
	String argtemplateSelPrep = "";
	String instemplateSelPrep = "";
	String etlProcesssTime = "";
	private final String category = "CATEGORY";
	
	@Value("${feed.upload.securedFtp}")
	private String securedFtp;
	
	@Value("${feed.upload.hostName}")
	private String hostName;
	
	@Value("${feed.upload.userName}")
	private String userName;
	
	@Value("${feed.upload.passWord}")
	private String passWord;
	
	@Value("${feed.upload.port}")
	private String port;
	
	@Value("${feed.upload.uploadDir}")
	private String uploadDir;
	
	
	/*private final String yetToStart = "YTS";
	private final String onHold = "ON_HOLD";
	private final String inProgress = "IN_PROGRESS";
	private final String success = "SUCCESS";
	private final String terminated = "TERMINATED";
	private final String errored = "ERRORED";*/

	@Autowired
	public EtlFeedScheduleConfigDao etlFeedScheduleConfigDao;
	
	@Autowired
	CommonDao commonDao;
	
	@Autowired
	EtlFeedMainDao etlFeedMainDao;

	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";

	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";

	String categoryApprDesc = "(SELECT CATEGORY_DESCRIPTION FROM ETL_FEED_CATEGORY WHERE LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY AND  CATEGORY_ID = TAppr.FEED_CATEGORY) FEED_CATEGORY_DESC";

	String FeedTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TAppr.FEED_TYPE",
			"FEED_TYPE_DESC");

	String FeedStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.ETL_PROCESS_STATUS",
			"ETL_PROCESS_STATUS_DESC");

	String currentProcessAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2112, "TAppr.CURRENT_PROCESS",
			"CURRENT_PROCESS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	String feedNameApprDesc = "(SELECT FEED_NAME FROM ETL_FEED_MAIN WHERE LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY AND  FEED_ID = TAppr.FEED_ID) FEED_NAME";

	String CompletionStatusAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2113, "T3.COMPLETION_STATUS",
			"MAIN_TABLE_STATUS_DESC");

	String ProcessControlStatusAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2113,
			"T4.COMPLETION_STATUS", "PROCESS_CONTROL_STATUS_DESC");

	public ArrayList getSingletonTabResults(OperationalConsoleVb vObject) {
		ArrayList result = new ArrayList();
		String strQueryAppr = new String("SELECT  COUNT(DISTINCT T1.COUNTRY" + getDbFunction(Constants.PIPELINE, null)
				+ "'-'" + getDbFunction(Constants.PIPELINE, null) + "T1.LE_BOOK"
				+ getDbFunction(Constants.PIPELINE, null) + "'-'" + getDbFunction(Constants.PIPELINE, null)
				+ "T1.FEED_CATEGORY) " + category + ",   COUNT(DISTINCT T1.COUNTRY"
				+ getDbFunction(Constants.PIPELINE, null) + "'-'" + getDbFunction(Constants.PIPELINE, null)
				+ "T1.LE_BOOK" + getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null) + "T1.FEED_ID " + getDbFunction(Constants.PIPELINE, null)
				+ "'-'" + getDbFunction(Constants.PIPELINE, null) + " T1.SESSION_ID ) Feed, "
				+ getDbFunction(Constants.NVL, null)
				+ "((SUM(CASE WHEN T1.COMPLETION_STATUS in ('P','R') THEN 1 ELSE 0 END)),0) Yet_to_Start,  "
				+ getDbFunction(Constants.NVL, null)
				+ " ((SUM(CASE WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 1 ELSE 0 END)),0) On_Hold,   "
				+ getDbFunction(Constants.NVL, null)
				+ " ((SUM(CASE WHEN (T1.CURRENT_PROCESS not like '%HOLD%' AND T1.COMPLETION_STATUS = 'I') THEN 1 ELSE 0 END)),0) In_Progress,  "
				+ " " + getDbFunction(Constants.NVL, null)
				+ " ((SUM(CASE WHEN T1.COMPLETION_STATUS in ('S') THEN 1 ELSE 0 END)),0) Success,   "
				+ getDbFunction(Constants.NVL, null)
				+ " ((SUM(CASE WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 1 ELSE 0 END)) ,0) Terminated,  "
				+ getDbFunction(Constants.NVL, null)
				+ " ((SUM(CASE WHEN (T1.CURRENT_PROCESS != 'TERMINATED' AND T1.COMPLETION_STATUS = 'E') THEN 1 ELSE 0 END)) ,0) Errored"
				+ " from ETL_FEED_PROCESS_CONTROL T1  "
				/*+ " INNER JOIN ETL_FEED_MAIN_PRS T2  "
				+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND  "
				+ " T1.FEED_ID = T2.FEED_ID AND T1.SESSION_ID = T2.SESSION_ID AND T2.FEED_STATUS = 0) "*/
				+ " INNER JOIN ETL_FEED_MAIN T3  ON (T1.COUNTRY = T3.COUNTRY AND T1.LE_BOOK = T3.LE_BOOK AND  "
				+ " T1.FEED_ID = T3.FEED_ID AND T3.FEED_STATUS = 0) "
				+ " INNER JOIN ETL_FEED_CATEGORY T4  ON (T1.COUNTRY = T4.COUNTRY AND T1.LE_BOOK = T4.LE_BOOK AND "
				+ " T1.FEED_CATEGORY = T4.CATEGORY_ID AND T4.CATEGORY_STATUS = 0) where 1=1 ");
		
		String queryWithFilter = cndApply(vObject, "1");
		String query = strQueryAppr.toString() + " " + queryWithFilter;
	/*	String[] singletonColArray = {category, yetToStart, onHold, inProgress, success, terminated, errored};
		String[] singletonFilterArray = {null, null, onHold, inProgress, success, terminated, errored};*/
		try {
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					int colCount = metaData.getColumnCount();
					Boolean dataPresent = false;
					while (rs.next()) {
						HashMap<String, String> resultData = new HashMap<String, String>();
						dataPresent = true;
						for (int cn = 1; cn <= colCount; cn++) {
							String columnName = metaData.getColumnName(cn);
							resultData.put(columnName.toUpperCase(), rs.getString(columnName));
						}
						result.add(resultData);
						/*int index = 0;
						for(String colName : singletonColArray) {
							index++;
							SingletonResponseVb resVb = new SingletonResponseVb();
							resVb.setOrder(index);
							resVb.setLabel(colName);
						}*/
						
					}
					return result;
				}
			};
			return (ArrayList) getJdbcTemplate().query(query.toString(), mapper);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<OperationalConsoleVb> getNotificationTabResults(OperationalConsoleVb dObj) {
		List<OperationalConsoleVb> collTemp = null;
		Vector<Object> params = new Vector<Object>();
		String strQueryAppr = new String("Select TAppr.COUNTRY ,TAppr.LE_BOOK ,TAppr.Country "
				+ getDbFunction(Constants.PIPELINE, null) + "' - '"
				+ getDbFunction(Constants.PIPELINE, null) + " LEB.leb_description LE_BOOK_DESC"
				+ ",TAppr.FEED_ID ,TAppr.FEED_ID " + getDbFunction(Constants.PIPELINE, null) + "' - '"
				+ getDbFunction(Constants.PIPELINE, null) + " T2.FEED_NAME FEED_NAME,TAppr.FEED_CATEGORY"
				+ ",TAppr.SESSION_ID,TAppr.ITERATION_COUNT,TAppr.VERSION_NUMBER,TAppr.AUDIT_SEQUENCE"
				+ ",TAppr.CURRENT_PROCESS_AT,TAppr.CURRENT_PROCESS,TAppr.AUDIT_DESCRIPTION "
				+ ",TAPPR.AUDIT_DESCRIPTION_DETAIL AUDIT_DESCRIPTION_DETAIL,TAppr.NOTIFICATION_FLAG"
				+ ",TAppr.ETL_AUDIT_STATUS_NT,TAppr.ETL_AUDIT_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION, T2.DISTINCT_FLAG "
				+ " From ETL_FEED_AUDIT_TRAIL TAppr  INNER JOIN ETL_FEED_MAIN T2 "
				+ " ON (TAppr.COUNTRY = T2.COUNTRY AND TAppr.LE_BOOK = T2.LE_BOOK AND TAppr.FEED_ID = T2.FEED_ID )"
				+ " INNER JOIN LE_BOOK LEB ON (TAppr.COUNTRY = LEB.COUNTRY AND TAppr.LE_BOOK = LEB.LE_BOOK) ");

		String strWhereNotExists = new String();
		String orderBy = " Order By DATE_CREATION asc";

		try {
			// collTemp =
			// getJdbcTemplate().query(strQueryAppr.toString(),getEtlFeedAuditMapper());
			// return collTemp;
			StringBuffer strBufApprove = new StringBuffer(strQueryAppr.toString());
			StringBuffer lPageQuery = new StringBuffer();
			lPageQuery.append(strBufApprove.toString());
			return getQueryPopupResultsForCLOB(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,
					getEtlFeedAuditMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	protected RowMapper getEtlFeedAuditMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb vObject = new OperationalConsoleVb();
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
				if (rs.getString("LE_BOOK_DESC") != null) {
					vObject.setLeBookDesc(rs.getString("LE_BOOK_DESC"));
				} else {
					vObject.setLeBookDesc("");
				}
				if (rs.getString("FEED_ID") != null) {
					vObject.setFeedId(rs.getString("FEED_ID"));
				} else {
					vObject.setFeedId("");
				}
				if (rs.getString("FEED_CATEGORY") != null) {
					vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
				} else {
					vObject.setFeedCategory("");
				}

				if (rs.getString("FEED_NAME") != null) {
					vObject.setFeedName(rs.getString("FEED_NAME"));
				} else {
					vObject.setFeedName("");
				}
				if (rs.getString("SESSION_ID") != null) {
					vObject.setSessionId(rs.getString("SESSION_ID"));
				} else {
					vObject.setSessionId("");
				}
				vObject.setIterationCount(rs.getInt("ITERATION_COUNT"));
				vObject.setVersionNumber(rs.getInt("VERSION_NUMBER"));

				vObject.setAuditSeq(rs.getInt("AUDIT_SEQUENCE"));
				vObject.setCurrentProcessAt(rs.getInt("CURRENT_PROCESS_AT"));
				if (rs.getString("CURRENT_PROCESS") != null) {
					vObject.setCurrentProcess(rs.getString("CURRENT_PROCESS"));
				} else {
					vObject.setCurrentProcess("");
				}

				if (rs.getString("AUDIT_DESCRIPTION") != null) {
					vObject.setAuditDesc(rs.getString("AUDIT_DESCRIPTION"));
				} else {
					vObject.setAuditDesc("");
				}

				if (rs.getString("AUDIT_DESCRIPTION_DETAIL") != null) {
					vObject.setAuditDescDetail(rs.getString("AUDIT_DESCRIPTION_DETAIL"));
				} else {
					vObject.setAuditDescDetail("");
				}
				vObject.setNotificationFlag(rs.getString("NOTIFICATION_FLAG"));

				vObject.setEtlAuditStatusNt(rs.getInt("ETL_AUDIT_STATUS_NT"));
				vObject.setEtlAuditStatus(rs.getInt("ETL_AUDIT_STATUS"));
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

	public List<OperationalConsoleVb> getQueryPopupResults(OperationalConsoleVb dObj) {
		Vector<Object> params = new Vector<Object>();
	
		StringBuffer strBufQuery = new StringBuffer("SELECT T1.SESSION_ID,T1.COUNTRY,  T1.LE_BOOK,  T1.COUNTRY "
				+ getDbFunction(Constants.PIPELINE, null) + "' - '" + getDbFunction(Constants.PIPELINE, null)
				+ " LEB.LEB_DESCRIPTION LE_BOOK_DESC,  T1.FEED_CATEGORY,  T1.FEED_CATEGORY "
				+ getDbFunction(Constants.PIPELINE, null) + "' - '" + getDbFunction(Constants.PIPELINE, null)
				+ " T2.CATEGORY_DESCRIPTION CATEGORY_DESC, T2.CATEGORY_DESCRIPTION CATEGORY_DESC_OB, "
				+ " T1.DEPENDENCY_CHECK_FLAG, "
				// + " T1.ITERATION_COUNT, T1.COMPLETION_STATUS, "
				+ "    CASE WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 1 THEN 'I' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 2 THEN 'T' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 3 THEN 'E' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 4 THEN 'H' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 5 THEN 'P' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 6 THEN 'R' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 7 THEN 'S' END COMPLETION_STATUS,   CASE WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 1 THEN 'In Progress' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 2 THEN 'Terminated' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 3 THEN 'Errored' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 4 THEN 'On hold' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 5 THEN 'Pending' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 6 THEN 'Reinitiated' WHEN (    MIN("
				+ "      CASE WHEN T1.COMPLETION_STATUS = 'I' THEN 1 WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 2 WHEN"
				+ "  T1.COMPLETION_STATUS = 'E' THEN 3 WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 4 WHEN T1.COMPLETION_STATUS = 'P' "
				+ " THEN 5 WHEN T1.COMPLETION_STATUS = 'R' THEN 6 WHEN T1.COMPLETION_STATUS = 'S' THEN 7 END"
				+ "    )  ) = 7 THEN 'Successful' END COMPLETION_STATUS_DESC,   SUM("
				+ "    CASE WHEN T1.COMPLETION_STATUS = 'S' THEN 1 ELSE 0 END  ) SUCCESS_CNT,   SUM("
				+ "    CASE WHEN T1.CURRENT_PROCESS = 'TERMINATED' THEN 1 ELSE 0 END  ) TERMINATE_CNT,   SUM("
				+ "    CASE WHEN (T1.CURRENT_PROCESS != 'TERMINATED' AND T1.COMPLETION_STATUS = 'E') THEN 1 ELSE 0 END"
				+ "  ) ERROR_CNT,   SUM(    CASE WHEN T1.COMPLETION_STATUS in ('P', 'R') THEN 1 ELSE 0 END"
				+ "  ) YTP_CNT,  SUM(    CASE WHEN T1.CURRENT_PROCESS like '%HOLD%' THEN 1 ELSE 0 END"
				+ "  ) HOLD_CNT,   SUM("
				+ "    CASE WHEN (T1.CURRENT_PROCESS not like '%HOLD%' AND T1.COMPLETION_STATUS = 'I') THEN 1 ELSE 0 END"
				+ "  ) IP_CNT, MIN(T1.PROCESS_DATE) PROCESS_DATE_OB, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(MIN(T1.PROCESS_DATE),'" + getDbFunction(Constants.DD_Mon_RRRR, null)
				+ "') PROCESS_DATE, TIME_ZONE, " + getDbFunction(Constants.DATEFUNC, null) + "( ("
				+ getDbFunction(Constants.FN_UNIX_TIME_TO_DATE_GRID1, null) + "(MIN (T1.NEXT_SCHEDULE_DATE), TIME_ZONE)),'"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') NEXT_SCHEDULE_DATE"
				+ " FROM ETL_FEED_PROCESS_CONTROL t1 INNER JOIN (SELECT DISTINCT COUNTRY, LE_BOOK, TIME_ZONE"
				+ " FROM VISION_BUSINESS_DAY) T3  ON (T1.COUNTRY = T3.country AND T1.LE_BOOK = T3.le_book)"
				+ " INNER JOIN LE_BOOK LEB ON (T1.COUNTRY = LEB.COUNTRY AND T1.LE_BOOK = LEB.LE_BOOK) "
				+ " INNER JOIN ETL_FEED_CATEGORY T2 "
				+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.FEED_CATEGORY = T2.CATEGORY_ID  "
				+ " AND T2.CATEGORY_STATUS = 0 )"
				+ " INNER JOIN ETL_FEED_MAIN T5  ON (T1.COUNTRY = T5.COUNTRY AND  T1.LE_BOOK = T5.LE_BOOK "
				+ " AND T1.FEED_ID = T5.FEED_ID AND T1.FEED_CATEGORY = T5.FEED_CATEGORY AND T5.FEED_STATUS=0) ");

		String queryWithFilter = cndApply(dObj, "1");

		StringBuffer strBufGroupBy = new StringBuffer("GROUP BY T1.SESSION_ID, T1.COUNTRY, T1.LE_BOOK,  T1.COUNTRY "
				+ getDbFunction(Constants.PIPELINE, null) + "' - '" + getDbFunction(Constants.PIPELINE, null)
				+ " LEB.LEB_DESCRIPTION,  T1.FEED_CATEGORY, T2.CATEGORY_DESCRIPTION, T1.DEPENDENCY_CHECK_FLAG, "
//				+ "T1.ITERATION_COUNT,T1.COMPLETION_STATUS, T1.VERSION_NUMBER,"
			// 	+ "TIME_ZONE ORDER BY NEXT_SCHEDULE_DATE"); // -- commented on 09_Nov_2023 for Pagination
				+ "TIME_ZONE ");

		/* Group By should be appended after Filter Conditions -- Commented on 16_Nov_2023
		 StringBuffer strBufApprove = new StringBuffer(strBufQuery.toString() + "" + queryWithFilter + "" + strBufGroupBy.toString());*/
		
		StringBuffer strBufApprove = new StringBuffer(strBufQuery.toString() + "" + queryWithFilter);
		
	//	String orderBy = " ORDER BY NEXT_SCHEDULE_DATE "; -- Commented on 07_Dec_2023
		String orderBy = " ORDER BY COUNTRY, LE_BOOK, PROCESS_DATE_OB, CATEGORY_DESC_OB ";
		String whereNotExistsQuery = null;
		StringBuffer pendingQuery = null;
		try {
			/*  Without pagination : 
			 * 
			List<OperationalConsoleVb> collTemp = null;
			collTemp = getJdbcTemplate().query(strBufApprove.toString()+orderBy, getMapper());
			return collTemp;  */
			if (dObj.getSmartSearchOpt() != null && dObj.getSmartSearchOpt().size() > 0) {
				for (SmartSearchVb data : dObj.getSmartSearchOpt()) {
					data.setJoinType("AND");
					String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());
					switch (data.getObject().toUpperCase()) {
					case "SESSIONID":
						CommonUtils.addToQuery(" upper(T1.SESSION_ID) " + val, strBufApprove);
						break;
					case "LEBOOKDESC":
						CommonUtils.addToQuery(" (upper(T1.COUNTRY) " + val, strBufApprove);
						strBufApprove.append(" OR upper(LEB.LEB_DESCRIPTION) "+val+" ) ");
						break;
					case "FEEDCATEGORYDESC":
						CommonUtils.addToQuery(" (upper(T1.FEED_CATEGORY) " + val, strBufApprove);
						strBufApprove.append(" OR upper(T2.CATEGORY_DESCRIPTION) "+val+" ) ");
						break;
					default:
						break;
					}
					
				}
			}
			strBufApprove.append(" "+strBufGroupBy);
			return getQueryPopupResults(dObj, pendingQuery, strBufApprove, whereNotExistsQuery, orderBy, params, getMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb vObject = new OperationalConsoleVb();
				vObject.setSessionId(rs.getString("SESSION_ID"));
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setLeBookDesc(rs.getString("LE_BOOK_DESC"));
				vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
				vObject.setFeedCategoryDesc(rs.getString("CATEGORY_DESC"));
//				vObject.setIterationCount(rs.getInt("ITERATION_COUNT"));
				vObject.setCompletionStatus(rs.getString("COMPLETION_STATUS"));
				vObject.setCompletionStatusDesc(rs.getString("COMPLETION_STATUS_DESC"));
				if (ValidationUtil.isValid(rs.getString("NEXT_SCHEDULE_DATE"))) {
					vObject.setNextScheduleDate(rs.getString("NEXT_SCHEDULE_DATE"));
				}
				vObject.setTimeZone(rs.getString("TIME_ZONE"));
				vObject.setProcessDate(rs.getString("PROCESS_DATE"));
				vObject.setCompletionErrcnt(rs.getInt("ERROR_CNT"));
				vObject.setCompletionSuccnt(rs.getInt("SUCCESS_CNT"));
				vObject.setCompletionYtpcnt(rs.getInt("YTP_CNT"));
				vObject.setCompletionInpcnt(rs.getInt("IP_CNT"));
				vObject.setTerminateCnt(rs.getInt("TERMINATE_CNT"));
				vObject.setHoldCnt(rs.getInt("HOLD_CNT"));
//				vObject.setVersionNumber(rs.getInt("VERSION_NUMBER"));
				vObject.setDependencyFlag(rs.getString("DEPENDENCY_CHECK_FLAG")); 
				return vObject;
			}
		};
		return mapper;
	}

	public List<OperationalConsoleVb> getOperationalConsoleGrid2Data(OperationalConsoleVb dObj) {
		if(!ValidationUtil.isValid(dObj.getSessionId())) {
			return null;
		}
		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufQuery = new StringBuffer("select T1.SESSION_ID, T1.COUNTRY ,T1.LE_BOOK "
				+ " ,T1.FEED_ID  ," + getDbFunction(Constants.DATEFUNC, null) + "(T1.PROCESS_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + "') PROCESS_DATE, T1.PROCESS_DATE PROCESS_DATE_OB, T2.LAST_EXTRACTION_DATE"
				+ " ,T1.NEXT_SCHEDULE_DATE ,T1.FEED_ID " + getDbFunction(Constants.PIPELINE, null)
				+ " ' - ' " + getDbFunction(Constants.PIPELINE, null) + " T2.FEED_NAME FEED_NAME, T2.FEED_NAME FEED_NAME_OB "
				+ " ,T2.FEED_TYPE  ,T4.BATCH_SIZE , (SELECT ALPHA_SUBTAB_DESCRIPTION "
				+ " FROM ALPHA_SUB_TAB  WHERE ALPHA_TAB = 2102 AND ALPHA_SUB_TAB = T2.FEED_TYPE) FEED_TYPE_DESC, "
				+ " T1.FEED_CATEGORY,  T1.ITERATION_COUNT,  T1.COMPLETION_STATUS, "
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION  FROM ALPHA_SUB_TAB "
				+ " WHERE ALPHA_TAB = 2113 AND ALPHA_SUB_TAB = T1.COMPLETION_STATUS) COMPLETION_STATUS_DESC, "
				+ " T1.CURRENT_PROCESS,  (SELECT ALPHA_SUBTAB_DESCRIPTION  FROM ALPHA_SUB_TAB "
				+ " WHERE ALPHA_TAB = 2112 AND ALPHA_SUB_TAB = T1.CURRENT_PROCESS) CURRENT_PROCESS_DESC, "
				// + "T1.START_TIME, T1.END_TIME, (T1.END_TIME - T1.START_TIME)/(1000*60) PROCESS_DURATION_MINS,"
				/* This is not working, query needs Group by. Start date, end date conversion happened in mapper
				 * + getDbFunction(Constants.DATEFUNC, null) + "( "
				+ getDbFunction(Constants.FN_UNIX_TIME_TO_DATE_GRID1, null) + "(MIN (T1.START_TIME), TIME_ZONE),'"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') START_TIME" + " , " + getDbFunction(Constants.DATEFUNC, null) + "( "
				+ getDbFunction(Constants.FN_UNIX_TIME_TO_DATE_GRID1, null) + "(MIN (T1.END_TIME), TIME_ZONE),'"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null) + "') END_TIME, "*/
				+ " T1.START_TIME, T1.END_TIME, "
				+ " T1.DEBUG_FLAG, T1.VERSION_NUMBER,T3.TIME_ZONE FROM ETL_FEED_PROCESS_CONTROL T1 "
				+ " INNER JOIN VISION_BUSINESS_DAY T3  ON (T3.COUNTRY = T1.COUNTRY AND T3.LE_BOOK = T1.LE_BOOK)"
				+ " INNER JOIN ETL_FEED_MAIN T2 "
				+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK and T1.FEED_ID=T2.FEED_ID AND T2.FEED_STATUS=0)"
				+ " INNER JOIN ETL_FEED_MAIN_PRS T4 "
				+ " ON (T1.COUNTRY = T4.COUNTRY AND T1.LE_BOOK = T4.LE_BOOK and T1.FEED_ID=T4.FEED_ID AND T1.SESSION_ID=T4.SESSION_ID) "
				+ " INNER JOIN ETL_FEED_CATEGORY T5 "
				+ " ON (T1.COUNTRY = T5.COUNTRY AND T1.LE_BOOK = T5.LE_BOOK AND T1.FEED_CATEGORY = T5.CATEGORY_ID  "
				+ " AND T5.CATEGORY_STATUS = 0 ) "
				+ " WHERE APPLICATION_ID='ETL'");

		String queryWithFilter = cndApply(dObj, "2");

		StringBuffer strBufApprove = new StringBuffer(strBufQuery.toString() + "" + queryWithFilter.toString());
		try {
		//	String orderBy = " Order By COUNTRY, LE_BOOK ,FEED_CATEGORY ";
			String orderBy = " ORDER BY COUNTRY, LE_BOOK, PROCESS_DATE_OB, FEED_NAME_OB ";
			/*StringBuffer lPageQuery = new StringBuffer();
			lPageQuery.append(strBufApprove.toString());*/
		
			if (dObj.getSmartSearchOpt() != null && dObj.getSmartSearchOpt().size() > 0) {
				for (SmartSearchVb data : dObj.getSmartSearchOpt()) {
					/*if (count == dObj.getSmartSearchOpt().size()) {
						data.setJoinType("");
					} else {
						if (!ValidationUtil.isValid(data.getJoinType()) && !("AND".equalsIgnoreCase(data.getJoinType())
								|| "OR".equalsIgnoreCase(data.getJoinType()))) {
							data.setJoinType("AND");
						}
					}*/
					data.setJoinType("AND");
					String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());
					switch (data.getObject().toUpperCase()) {
					case "FEEDNAME":
						CommonUtils.addToQuery(" ( UPPER(T1.FEED_ID) " + val, strBufApprove);
						strBufApprove.append(" OR UPPER(T2.FEED_NAME) "+val+" ) ");
						break;
					case "FEEDTYPEDESC":
						List<OperationalConsoleVb> feedTypeData = findFeedTypeByCustomFilter(val);
						String feedTData = "";
						for (int k = 0; k < feedTypeData.size(); k++) {
							String feedType = feedTypeData.get(k).getFeedType();
							if (!ValidationUtil.isValid(feedTData)) {
								feedTData = "'" + feedType + "'";
							} else {
								feedTData = feedTData + ",'" + feedType + "'";
							}
						}
						CommonUtils.addToQuery(" (T4.FEED_TYPE) IN (" + feedTData + ") ", strBufApprove);
						break;
					case "CURRENTPROCESSDESC":
						List<OperationalConsoleVb> completionStatusData = findCompletionStatusByCustomFilter(val);
						String compStatusData = "";
						for (int k = 0; k < completionStatusData.size(); k++) {
							String completionStatus = completionStatusData.get(k).getCompletionStatus();
							if (!ValidationUtil.isValid(compStatusData)) {
								compStatusData = "'" + completionStatus + "'";
							} else {
								compStatusData = compStatusData + ",'" + completionStatus + "'";
							}
						}
						CommonUtils.addToQuery(" (T1.CURRENT_PROCESS) IN (" + compStatusData + ") ", strBufApprove);
						break;
					default:
						break;
					}
					
				}
			}
			if(dObj.getMaxRecords() == -1) {
				return getJdbcTemplate().query(strBufApprove.toString()+orderBy, getGrid2Mapper());
			}
//			with pagination correct : 
			return getQueryPopupResults(dObj, null, strBufApprove, null, orderBy, params, getGrid2Mapper());
		//	wrong code with pagination : return getQueryPopupResultsPgn(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params, getGrid2Mapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	protected RowMapper getGrid2Mapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb vObject = new OperationalConsoleVb();
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setSessionId(rs.getString("SESSION_ID"));
				vObject.setFeedId(rs.getString("FEED_ID"));
				vObject.setProcessDate(rs.getString("PROCESS_DATE"));
				vObject.setFeedName(rs.getString("FEED_NAME"));
				vObject.setFeedType(rs.getString("FEED_TYPE"));
				vObject.setBatchSize(rs.getInt("BATCH_SIZE"));
				vObject.setFeedTypeDesc(rs.getString("FEED_TYPE_DESC"));
				vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
				vObject.setIterationCount(rs.getInt("ITERATION_COUNT"));
				vObject.setCompletionStatus(rs.getString("COMPLETION_STATUS"));
				vObject.setCompletionStatusDesc(rs.getString("COMPLETION_STATUS_DESC"));
				vObject.setCurrentProcess(rs.getString("CURRENT_PROCESS"));
				vObject.setCurrentProcessDesc(rs.getString("CURRENT_PROCESS_DESC"));
				vObject.setTimeZone(rs.getString("TIME_ZONE"));
				
				/*
				 * if (ValidationUtil.isValidUnixTimeStamp(rs.getString("START_TIME")) &&
				 * ValidationUtil.isValidUnixTimeStamp(rs.getString("END_TIME"))) {
				 * vObject.setDuration(differenceInTimeUnit(rs.getString("START_TIME"),
				 * rs.getString("END_TIME"))); }
				 */
				
			/*	if ("S".equalsIgnoreCase(vObject.getCompletionStatus())
						|| "E".equalsIgnoreCase(vObject.getCompletionStatus())
						|| "I".equalsIgnoreCase(vObject.getCompletionStatus())) {*/
				
				if (ValidationUtil.isValidUnixTimeStamp(rs.getString("START_TIME"))) {
					vObject.setDuration(differenceInTimeUnit(rs.getString("START_TIME"), rs.getString("END_TIME")));
				}
				if (ValidationUtil.isValid(rs.getString("NEXT_SCHEDULE_DATE"))) {
					vObject.setNextScheduleDate(printTimeFromMilliSecondsAsString(rs.getString("NEXT_SCHEDULE_DATE"),
							rs.getString("TIME_ZONE")));
				}
				if (ValidationUtil.isValid(rs.getString("LAST_EXTRACTION_DATE"))) {
					vObject.setLastExtractionDate(printTimeFromMilliSecondsAsString(
							rs.getString("LAST_EXTRACTION_DATE"), vObject.getTimeZone()));
				}
				vObject.setDebugFlag(rs.getString("DEBUG_FLAG"));
				vObject.setVersionNumber(rs.getInt("VERSION_NUMBER"));
				
				/*if (ValidationUtil.isValid(rs.getString("START_TIME"))) {
					vObject.setStartTime(printTimeFromMilliSecondsAsString(
							rs.getString("START_TIME"), vObject.getTimeZone()));
				}
				
				if (ValidationUtil.isValid(rs.getString("END_TIME")) && Integer.parseInt(rs.getString("END_TIME")) > 0) {
					vObject.setEndTime(printTimeFromMilliSecondsAsString(
							rs.getString("END_TIME"), vObject.getTimeZone()));
				}*/
				
				
				String startTimeStr = rs.getString("START_TIME");
				if (ValidationUtil.isValid(startTimeStr)) {
				    try {
				        long startTime = Long.parseLong(startTimeStr);
				        if (startTime > 0) {
				            vObject.setStartTime(printTimeFromMilliSecondsAsString(startTimeStr, vObject.getTimeZone()));
				        } else {
				            vObject.setStartTime("");
				        }
				    } catch (NumberFormatException e) {
				        vObject.setStartTime("");
				    }
				} else {
				    vObject.setStartTime("");
				}
				
				String endTimeStr = rs.getString("END_TIME");
				if (ValidationUtil.isValid(endTimeStr)) {
				    try {
				        long endTime = Long.parseLong(endTimeStr);
				        if (endTime > 0) {
				            vObject.setEndTime(printTimeFromMilliSecondsAsString(endTimeStr, vObject.getTimeZone()));
				        } else {
				            vObject.setEndTime("");
				        }
				    } catch (NumberFormatException e) {
				        vObject.setEndTime("");
				    }
				} else {
				    vObject.setEndTime("");
				}
				return vObject;
			}
		};
		return mapper;
	}
	
	
	public String differenceInTimeUnit(String startTime, String endTime) {
		long st, et;
		
		if(CommonUtils.isValidTimeStampMills(startTime))
			st = Long.parseLong(startTime);
		else 
			return null;
		
		if (ValidationUtil.isValid(endTime)) {
			if (CommonUtils.isValidTimeStampMills(endTime)) {
				et = Long.parseLong(endTime);
			} else {
				et = System.currentTimeMillis();
			}
		} else {
			et = System.currentTimeMillis();
		}
		
		// Calculate the difference in milliseconds
        long durationInMillis = et - st;

        // Convert milliseconds to hours, minutes, and seconds
        long hours = TimeUnit.MILLISECONDS.toHours(durationInMillis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(durationInMillis) - 
                       TimeUnit.HOURS.toMinutes(hours);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(durationInMillis) - 
                       TimeUnit.HOURS.toSeconds(hours) - 
                       TimeUnit.MINUTES.toSeconds(minutes);

        // Format the time difference as HH:MM:SS
        String timeDifference = String.format("%02d:%02d:%02d", hours, minutes, seconds);
        return timeDifference;
	}

	public String differenceInTimeUnit1(String startTimeString, String endTimeString) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		Long startTimeInMilliSeconds = Long.parseLong(startTimeString);
		Long endTimeInMilliSeconds = Long.parseLong(endTimeString);
		Date startTime = new Date();
		startTime.setTime(startTimeInMilliSeconds);
		sdf.setTimeZone(TimeZone.getTimeZone("Asia/Dubai"));
		Date endTime = new Date();
		endTime.setTime(endTimeInMilliSeconds);
		sdf.setTimeZone(TimeZone.getTimeZone("Asia/Dubai"));

		long diffInMilliSeconds = new Date(startTimeInMilliSeconds).getTime()
				- new Date(endTimeInMilliSeconds).getTime();

		/* Option 1 - Start */
		long hour = TimeUnit.MILLISECONDS.toHours(diffInMilliSeconds);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(diffInMilliSeconds);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(diffInMilliSeconds);
		String timeTaken = String.format("%s:%s:%s", String.valueOf(hour).replaceAll("-", ""),
				String.valueOf((hour * 60) - minutes).replaceAll("-", ""),
				String.valueOf(minutes * 60 - seconds).replaceAll("-", ""));
		/* Option 1 - End */

		String data = "";
		String[] timeTakenSTR = timeTaken.split(":");
		for (String tim : timeTakenSTR) {
			if (ValidationUtil.isValid(tim)) {
				if (tim.length() < 2) {
					tim = "0" + tim;
				}
				if (ValidationUtil.isValid(data)) {
					data = data + ":" + tim;
				} else {
					data = tim;
				}
			}
		}

		/* Option 2 - Start */
		long hour2 = (diffInMilliSeconds / (60 * 60 * 1000)) % 24;
		long minutes2 = (diffInMilliSeconds / (60 * 1000)) % 60;
		long seconds2 = (diffInMilliSeconds / 1000) % 60;
		String timeTaken2 = String.format("[%s] hours : [%s] mins : [%s] secs",
				String.valueOf(hour2).replaceAll("-", ""), String.valueOf(minutes2).replaceAll("-", ""),
				String.valueOf(seconds2).replaceAll("-", ""));
		/* Option 2 - End */
		return data;
	}

	public int doDeleteAppr(OperationalConsoleVb dObj) {
		String query = "Delete From ETL_FEED_PROCESS_CONTROL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_CATEGORY = ?  and SESSION_ID=?";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedCategory(), dObj.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int doDeleteFeedAppr(OperationalConsoleVb dObj) {
		String query = "Delete From ETL_FEED_PROCESS_CONTROL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_CATEGORY = ?  and SESSION_ID=? AND FEED_ID=?";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedCategory(), dObj.getSessionId(),
				dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteApprRecord(List<OperationalConsoleVb> vObjects, OperationalConsoleVb vObjectParam)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		setServiceDefaults();
		strCurrentOperation = Constants.DELETE;
		try {
			for (OperationalConsoleVb vObject : vObjects) {
				if (vObject.isChecked()) {
					vObject.setGrid(vObjectParam.getGrid());
					vObject.setStaticDelete(vObjectParam.isStaticDelete());
					vObject.setVerificationRequired(vObjectParam.isVerificationRequired());
					exceptionCode = doDeleteApprRecordForNonTrans(vObject);
					if (exceptionCode == null || exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	protected ExceptionCode doDeleteApprRecordForNonTrans(OperationalConsoleVb vObject) throws RuntimeCustomException {
		List<OperationalConsoleVb> collTemp = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		OperationalConsoleVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<OperationalConsoleVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<OperationalConsoleVb>) collTemp).get(0);
		
		boolean checkCompletionStatus = false;
		
		for(OperationalConsoleVb vb : collTemp) {
			if( (!"S".equalsIgnoreCase(vb.getCompletionStatus())) && (!"E".equalsIgnoreCase(vb.getCompletionStatus()))) {
				checkCompletionStatus = true;
				break;
			}
		}
		
		if(checkCompletionStatus) {
			if ("2".equalsIgnoreCase(vObject.getGrid())) {
				exceptionCode.setErrorMsg("Deletion is restricted to feeds in 'Success' or 'Errored' status.");
			} else {
				exceptionCode.setErrorMsg("Deletion is restricted to categories in 'Success' or 'Errored' status.");
			}
			
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		
		vObject.setDateCreation(vObjectlocal.getDateCreation());

		// move to process control history from etl feed process control
		
		if ("2".equalsIgnoreCase(vObject.getGrid())) {
			exceptionCode = insertEtlProcessHistory(vObject);
		} else {
			exceptionCode = insertEtlProcessHistoryByCategory(vObject);
		}
		
		
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			throw buildRuntimeCustomException(exceptionCode);
		}
		EtlFeedMainVb etlFeedMainVb = new EtlFeedMainVb();
		etlFeedMainVb.setCountry(vObject.getCountry());
		etlFeedMainVb.setLeBook(vObject.getLeBook());
		etlFeedMainVb.setFeedId(vObject.getFeedId());
		etlFeedMainVb.setSessionId(vObject.getSessionId());
		etlFeedMainVb.setFeedCategory(vObject.getFeedCategory());
		
		if ("2".equalsIgnoreCase(vObject.getGrid())) {
			etlFeedMainDao.deleteAllPrsData(etlFeedMainVb);
		} else {
			etlFeedMainDao.deleteAllPrsDataByCategory(etlFeedMainVb);
		}
		

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
			if ("2".equalsIgnoreCase(vObject.getGrid())) {
				retVal = doDeleteFeedAppr(vObject);
			} else {
				retVal = doDeleteAppr(vObject);
			}
//			vObject.setRecordIndicator(-1);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
		}
		
		/* Delete in FeedFolder - Start */
		
		String filePattern = null;
		
		if ("2".equalsIgnoreCase(vObject.getGrid())) {
			filePattern = vObject.getFeedId() + "-" + vObject.getFeedCategory() + "-" + vObject.getSessionId() +".*";
		} else {
			filePattern = vObject.getFeedCategory() + "-" + vObject.getSessionId() +".*";
		}
		String sparkHome = System.getenv("SPARK_HOME");

		if (!ValidationUtil.isValid(sparkHome))
			sparkHome = "/home/vision/app/spark/spark";
		
		try {
		    if ("Y".equalsIgnoreCase(securedFtp)) {
		        JSch jsch = new JSch();
		        Session session = jsch.getSession(userName, hostName, Integer.parseInt(port));
		        session.setPassword(passWord);
		        java.util.Properties config = new java.util.Properties();
		        config.put("StrictHostKeyChecking", "no");
		        session.setConfig(config);
		        session.connect();
		        Channel channel = session.openChannel("sftp");
		        channel.connect();
		        ChannelSftp sftpChannel = (ChannelSftp) channel;
		        sftpChannel.cd(uploadDir);
		        String[] folderArray = getFolderList(sftpChannel, filePattern);
		        if (ValidationUtil.isValid(folderArray)) {
		        	for (String folderName : folderArray) {
						String appID = null;
						String appIdPath = uploadDir + "/" + folderName + "/AppID.txt";
						if (CommonUtils.isFileExist(sftpChannel, appIdPath)) {
							appID = readTxtFromFile(sftpChannel, appIdPath);
						}
						if(ValidationUtil.isValid(appID) && CommonUtils.isFileExist(sftpChannel, sparkHome+"/work/"+appID)) {
							recursiveFolderDelete(sftpChannel, sparkHome+"/work/"+appID);
						}
						recursiveFolderDelete(sftpChannel, uploadDir + "/" + folderName);
					}
		        }
		        sftpChannel.disconnect();
		        session.disconnect();
			} else {
				File baseDir = new File(uploadDir);
				String[] folderArray = getFolderList(baseDir, filePattern);
				if (ValidationUtil.isValid(folderArray)) {
					for (String folderName : folderArray) {
						String appIDFilePath = uploadDir + "/" + folderName + "/AppID.txt";
						if (CommonUtils.isFileExist(appIDFilePath)) {
							String appID = readTxtFromFile(appIDFilePath);
							if (ValidationUtil.isValid(appID) && CommonUtils.isFileExist(sparkHome + "/work/" + appID)) {
								recursiveFolderDelete(sparkHome + "/work/" + appID);
							}
							recursiveFolderDelete(uploadDir + "/" + folderName);
						}
					}
				}

			}
		} catch (Exception e) {
		    e.printStackTrace();
		    if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		
		/* Delete in FeedFolder - End */
			
		if ("2".equalsIgnoreCase(vObject.getGrid()) && retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			if ("2".equalsIgnoreCase(vObject.getGrid())) {
				retVal = updateFeedMainTableStatusForCS(vObjectlocal, "ETL_FEED_MAIN", "S");
			} else {
				retVal = updateFeedMainTableStatusForCSByCategory(vObjectlocal, "ETL_FEED_MAIN", "S");
			}
			
			if ("2".equalsIgnoreCase(vObject.getGrid()) && retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if ("2".equalsIgnoreCase(vObject.getGrid())) {
				retVal = updateETLFeedScheduleConfigProcessTbl(vObjectlocal, "PROCESS_TBL_STATUS", "YTP");
			} else {
				retVal = updateETLFeedScheduleConfigProcessTblByCategory(vObjectlocal, "PROCESS_TBL_STATUS", "YTP");
			}
			
			if ("2".equalsIgnoreCase(vObject.getGrid()) && retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		if (vObject.isStaticDelete()) {
			setStatus(vObjectlocal, Constants.STATUS_ZERO);
			setStatus(vObject, Constants.PASSIVATE);
			exceptionCode = writeAuditLog(vObject, vObjectlocal);
		} else {
			exceptionCode = writeAuditLog(null, vObject);
			vObject.setRecordIndicator(-1);
		}
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}
	
	public int updateFeedMainTableStatusForCS(OperationalConsoleVb vObject, String tableType, String fStatus) {
		String table = "ETL_FEED_MAIN";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_MAIN_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_MAIN_PEND";
		}
		String appendDateCol = "";
		if (ValidationUtil.isValid(vObject.getLastExtractionDate())) {
			appendDateCol = " , LAST_EXTRACTION_DATE = '" + vObject.getLastExtractionDate() + "' ";
		}
		String query = "update " + table + " set COMPLETION_STATUS=? " + appendDateCol
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  and FEED_CATEGORY = ?";
		Object[] args = { fStatus, vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getFeedCategory() };
		return getJdbcTemplate().update(query, args);
	}

	public int doUpdateLastExtractionDate(OperationalConsoleVb vObject) {
		String query = "update ETL_FEED_MAIN set LAST_EXTRACTION_DATE=? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  and FEED_CATEGORY = ?";
		Object[] args = { vObject.getLastExtractionDate(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId(), vObject.getFeedCategory() };
		return getJdbcTemplate().update(query, args);
	}

	public int getMaxOfVersionNumber(OperationalConsoleVb dObj) {
		String strQueryAppr = "select max(VERSION_NUMBER) VERSION_NUMBER from ETL_FEED_PROCESS_cONTROL"
				+ " Where COUNTRY = '" + dObj.getCountry().toUpperCase() + "'  AND LE_BOOK = '"
				+ dObj.getLeBook().toUpperCase() + "'  and SESSION_ID= '" + dObj.getSessionId()
				+ "'  AND FEED_CATEGORY = '" + dObj.getFeedCategory().toUpperCase() + "' ";

		if (ValidationUtil.isValid(dObj.getGrid()) && "2".equalsIgnoreCase(dObj.getGrid())) {
			strQueryAppr = strQueryAppr + " AND UPPER(FEED_ID) = '" + dObj.getFeedId().toUpperCase() + "' ";
		}
		return getJdbcTemplate().queryForObject(strQueryAppr, Integer.class);
	}


	public List<OperationalConsoleVb> getFeedDetails(OperationalConsoleVb dObj) {
		List<OperationalConsoleVb> collTemp = null;
		String strQueryAppr = " SELECT T1.EFFECTIVE_DATE, T1.COUNTRY, T1.LE_BOOK, T1.LAST_EXTRACTION_DATE, T1.FEED_ID, T1.FEED_TYPE, "
				+ " T1.FEED_CATEGORY, T2.SCHEDULE_PROP_CONTEXT, T2.SCHEDULE_TYPE, T3.SOURCE_CONNECTOR_ID, T4.DESTINATION_CONNECTOR_ID, T5.TIME_ZONE, T2.PROCESS_DATE_TYPE "
				+ " FROM ETL_FEED_MAIN T1 jOIN ETL_FEED_SCHEDULE_CONFIG T2 on "
				+ " T1.COUNTRY= T2.COUNTRY and T1.LE_BOOK= T2.LE_BOOK and T1.FEED_ID = T2.FEED_ID "
				+ " JOIN  ETL_Feed_Source T3 ON "
				+ " T1.COUNTRY= T3.COUNTRY and T1.LE_BOOK= T3.LE_BOOK and T1.FEED_ID = T3.FEED_ID "
				+ " JOIN  ETL_Feed_destination T4 ON "
				+ " T1.COUNTRY= T4.COUNTRY and T1.LE_BOOK= T4.LE_BOOK and T1.FEED_ID = T4.FEED_ID "
				+ " JOIN  VISION_BUSINESS_DAY T5 ON  T1.COUNTRY= T5.COUNTRY and T1.LE_BOOK= T5.LE_BOOK "
				+ " WHERE T1.FEED_STATUS = 0 AND T1.FEED_ID = '" + dObj.getFeedId() + "' AND T1.FEED_CATEGORY = '"
				+ dObj.getFeedCategory() + "' and APPLICATION_ID='ETL' ";

		if (ValidationUtil.isValid(dObj.getCountry())) {
			strQueryAppr = strQueryAppr + " AND T1.COUNTRY ='" + dObj.getCountry() + "'";
		}
		if (ValidationUtil.isValid(dObj.getLeBook())) {
			strQueryAppr = strQueryAppr + " AND T1.LE_BOOK ='" + dObj.getLeBook() + "'";
		}

		try {
			collTemp = getJdbcTemplate().query(strQueryAppr.toString(),
					new ResultSetExtractor<List<OperationalConsoleVb>>() {
						@Override
						public List<OperationalConsoleVb> extractData(ResultSet rs)
								throws SQLException, DataAccessException {
							List<OperationalConsoleVb> returnList = new ArrayList<OperationalConsoleVb>();
							while (rs.next()) {
								OperationalConsoleVb processControlVb = new OperationalConsoleVb();
								String propContext = rs.getString("SCHEDULE_PROP_CONTEXT");
								ETLFeedTimeBasedPropVb timePropVb = new ETLFeedTimeBasedPropVb();
								ETLFeedEventBasedPropVb eventPropVb = new ETLFeedEventBasedPropVb();
								processControlVb.setEffectiveDate(rs.getString("EFFECTIVE_DATE"));
								processControlVb.setCountry(rs.getString("COUNTRY"));
								processControlVb.setLeBook(rs.getString("LE_BOOK"));
								processControlVb.setFeedId(rs.getString("FEED_ID"));
								processControlVb.setNextScheduleDate(dObj.getNextScheduleDate());
								processControlVb.setCurrentProcess("SUBMITTED");
								processControlVb.setCompletionStatus("P");
								processControlVb.setIterationCount(0);
								processControlVb.setAlertStatus("SUC");
								processControlVb.setFeedCategory(rs.getString("FEED_CATEGORY"));
								processControlVb.setEtlSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
								processControlVb.setDestinationConnectorId(rs.getString("DESTINATION_CONNECTOR_ID"));
								processControlVb.setScheduleType(rs.getString("SCHEDULE_TYPE"));
								processControlVb.setRecordIndicator(0);
								processControlVb.setVersionNumber(1);
								processControlVb.setDebugFlag("N");
								processControlVb.setFeedType(rs.getString("FEED_TYPE"));
								XmlMapper xmlMapper = new XmlMapper();
								if("TIME".equalsIgnoreCase(rs.getString("SCHEDULE_TYPE"))) {
									try {
										timePropVb = xmlMapper.readValue(rs.getString("SCHEDULE_PROP_CONTEXT"), ETLFeedTimeBasedPropVb.class);
									}  catch (Exception e) {
										if ("Y".equalsIgnoreCase(enableDebug)) {
											e.printStackTrace();
										}
									}
									processControlVb.setTimeBasedScheduleProp(timePropVb);
									processControlVb.setReadinessScriptID(timePropVb.getReadinessScriptID());
									processControlVb.setPreScriptID(timePropVb.getPreScriptID());
									processControlVb.setPostScriptID(timePropVb.getPostScriptID());
								} else if ("EVENT".equalsIgnoreCase(rs.getString("SCHEDULE_TYPE"))) {
									eventPropVb.setType(CommonUtils.getValueForXmlTag(propContext, "type"));
									eventPropVb.setReadinessScriptID(CommonUtils.getValueForXmlTag(propContext, "readinessScriptID"));
									eventPropVb.setPreScriptID(CommonUtils.getValueForXmlTag(propContext, "preScriptID"));
									eventPropVb.setPostScriptID(CommonUtils.getValueForXmlTag(propContext, "postScriptID"));
									
									processControlVb.setEventBasedScheduleProp(eventPropVb);
									processControlVb.setEventType(eventPropVb.getType());
									processControlVb.setReadinessScriptID(eventPropVb.getReadinessScriptID());
									processControlVb.setPreScriptID(eventPropVb.getPreScriptID());
									processControlVb.setPostScriptID(eventPropVb.getPostScriptID());
								}
								returnList.add(processControlVb);
							}
							return returnList;
						}
					});
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	protected ExceptionCode insertEtlProcessHistory(OperationalConsoleVb dObj) {
		/*String strQueryAppr = "insert into ETL_FEED_PROCESS_HISTORY SELECT * FROM  ETL_FEED_PROCESS_CONTROL "
				+ " Where COUNTRY = ?  AND LE_BOOK = ?    and SESSION_ID=? AND FEED_CATEGORY = ? AND UPPER(FEED_ID) = ?";*/
		
		String strQueryAppr = "insert into ETL_FEED_PROCESS_HISTORY(COUNTRY,LE_BOOK,FEED_ID,PROCESS_DATE,SESSION_ID,START_TIME,END_TIME,TRIGGERED_BY"
				+ ",TERMINATED_BY,REINITIATED_BY,CURRENT_PROCESS_AT,CURRENT_PROCESS,ITERATION_COUNT"
				+ ",COMPLETION_STATUS_AT,COMPLETION_STATUS,ALERT_STATUS_AT,ALERT_STATUS,ETL_PROCESS_STATUS_NT"
				+ ",ETL_PROCESS_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS"
				+ ",DATE_LAST_MODIFIED,DATE_CREATION,EVENT_TYPE_AT,EVENT_TYPE,DEPENDENCY_FEED_ID,NEXT_SCHEDULE_DATE"
				+ ",FEED_CATEGORY,SCHEDULE_TYPE_AT,SCHEDULE_TYPE,DEBUG_FLAG,VERSION_NUMBER,READINESS_SCRIPT_ID"
				+ ",READINESS_START_TIME,READINESS_END_TIME,PRE_SCRIPT_START_TIME,PRE_SCRIPT_END_TIME"
				+ ",EXTRACTION_START_TIME,EXTRACTION_END_TIME,TRANSFORMATION_START_TIME,TRANSFORMATION_END_TIME"
				+ ",LOADING_START_TIME,LOADING_END_TIME,POST_SCRIPT_START_TIME,POST_SCRIPT_END_TIME,PRE_SCRIPT_ID"
				+ ",POST_SCRIPT_ID,SOURCE_RECORD_COUNT,TO_UPDATE_COUNT,TO_INSERT_COUNT,SUCCESSFUL_UPDATE_COUNT"
				+ ",SUCCESSFUL_INSERT_COUNT,FEED_INSERTION_TYPE_AT,FEED_INSERTION_TYPE"
				+ " ) SELECT COUNTRY,LE_BOOK,FEED_ID,PROCESS_DATE,SESSION_ID,START_TIME,END_TIME,TRIGGERED_BY "
				+ ",TERMINATED_BY,REINITIATED_BY,CURRENT_PROCESS_AT,CURRENT_PROCESS,ITERATION_COUNT"
				+ ",COMPLETION_STATUS_AT,COMPLETION_STATUS,ALERT_STATUS_AT,ALERT_STATUS,ETL_PROCESS_STATUS_NT"
				+ ",ETL_PROCESS_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS"
				+ ",DATE_LAST_MODIFIED,DATE_CREATION,EVENT_TYPE_AT,EVENT_TYPE,DEPENDENCY_FEED_ID,NEXT_SCHEDULE_DATE"
				+ ",FEED_CATEGORY,SCHEDULE_TYPE_AT,SCHEDULE_TYPE,DEBUG_FLAG,VERSION_NUMBER,READINESS_SCRIPT_ID"
				+ ",READINESS_START_TIME,READINESS_END_TIME,PRE_SCRIPT_START_TIME,PRE_SCRIPT_END_TIME"
				+ ",EXTRACTION_START_TIME,EXTRACTION_END_TIME,TRANSFORMATION_START_TIME,TRANSFORMATION_END_TIME"
				+ ",LOADING_START_TIME,LOADING_END_TIME,POST_SCRIPT_START_TIME,POST_SCRIPT_END_TIME,PRE_SCRIPT_ID"
				+ ",POST_SCRIPT_ID,SOURCE_RECORD_COUNT,TO_UPDATE_COUNT,TO_INSERT_COUNT,SUCCESSFUL_UPDATE_COUNT"
				+ ",SUCCESSFUL_INSERT_COUNT,FEED_INSERTION_TYPE_AT,FEED_INSERTION_TYPE"
				+ " FROM  ETL_FEED_PROCESS_CONTROL  Where COUNTRY = ? AND LE_BOOK = ? and SESSION_ID=? AND FEED_CATEGORY = ? AND UPPER(FEED_ID) = ?";

		Object objParams[] = {dObj.getCountry(), dObj.getLeBook(), dObj.getSessionId(), dObj.getFeedCategory().toUpperCase(), dObj.getFeedId().toUpperCase()};

		try {
			getJdbcTemplate().update(strQueryAppr, objParams);
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			strErrorDesc = ex.getMessage();
			ExceptionCode exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			return exceptionCode;
		}
	}

	protected ExceptionCode insertEtlProcessHistoryByCategory(OperationalConsoleVb dObj) {
		/*String strQueryAppr = "insert into ETL_FEED_PROCESS_HISTORY SELECT * FROM  ETL_FEED_PROCESS_CONTROL "
				+ " Where COUNTRY = ?  AND LE_BOOK = ?    and SESSION_ID=? AND FEED_CATEGORY = ? ";*/
		
		String strQueryAppr = "insert into ETL_FEED_PROCESS_HISTORY ( COUNTRY,LE_BOOK,FEED_ID,PROCESS_DATE,SESSION_ID,START_TIME,END_TIME,TRIGGERED_BY"
				+ ",TERMINATED_BY,REINITIATED_BY,CURRENT_PROCESS_AT,CURRENT_PROCESS,ITERATION_COUNT"
				+ ",COMPLETION_STATUS_AT,COMPLETION_STATUS,ALERT_STATUS_AT,ALERT_STATUS,ETL_PROCESS_STATUS_NT"
				+ ",ETL_PROCESS_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS"
				+ ",DATE_LAST_MODIFIED,DATE_CREATION,EVENT_TYPE_AT,EVENT_TYPE,DEPENDENCY_FEED_ID,NEXT_SCHEDULE_DATE"
				+ ",FEED_CATEGORY,SCHEDULE_TYPE_AT,SCHEDULE_TYPE,DEBUG_FLAG,VERSION_NUMBER,READINESS_SCRIPT_ID"
				+ ",READINESS_START_TIME,READINESS_END_TIME,PRE_SCRIPT_START_TIME,PRE_SCRIPT_END_TIME"
				+ ",EXTRACTION_START_TIME,EXTRACTION_END_TIME,TRANSFORMATION_START_TIME,TRANSFORMATION_END_TIME"
				+ ",LOADING_START_TIME,LOADING_END_TIME,POST_SCRIPT_START_TIME,POST_SCRIPT_END_TIME,PRE_SCRIPT_ID"
				+ ",POST_SCRIPT_ID,SOURCE_RECORD_COUNT,TO_UPDATE_COUNT,TO_INSERT_COUNT,SUCCESSFUL_UPDATE_COUNT"
				+ ",SUCCESSFUL_INSERT_COUNT,FEED_INSERTION_TYPE_AT,FEED_INSERTION_TYPE"
				+ " ) SELECT COUNTRY,LE_BOOK,FEED_ID,PROCESS_DATE,SESSION_ID,START_TIME,END_TIME,TRIGGERED_BY "
				+ ",TERMINATED_BY,REINITIATED_BY,CURRENT_PROCESS_AT,CURRENT_PROCESS,ITERATION_COUNT"
				+ ",COMPLETION_STATUS_AT,COMPLETION_STATUS,ALERT_STATUS_AT,ALERT_STATUS,ETL_PROCESS_STATUS_NT"
				+ ",ETL_PROCESS_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS"
				+ ",DATE_LAST_MODIFIED,DATE_CREATION,EVENT_TYPE_AT,EVENT_TYPE,DEPENDENCY_FEED_ID,NEXT_SCHEDULE_DATE"
				+ ",FEED_CATEGORY,SCHEDULE_TYPE_AT,SCHEDULE_TYPE,DEBUG_FLAG,VERSION_NUMBER,READINESS_SCRIPT_ID"
				+ ",READINESS_START_TIME,READINESS_END_TIME,PRE_SCRIPT_START_TIME,PRE_SCRIPT_END_TIME"
				+ ",EXTRACTION_START_TIME,EXTRACTION_END_TIME,TRANSFORMATION_START_TIME,TRANSFORMATION_END_TIME"
				+ ",LOADING_START_TIME,LOADING_END_TIME,POST_SCRIPT_START_TIME,POST_SCRIPT_END_TIME,PRE_SCRIPT_ID"
				+ ",POST_SCRIPT_ID,SOURCE_RECORD_COUNT,TO_UPDATE_COUNT,TO_INSERT_COUNT,SUCCESSFUL_UPDATE_COUNT"
				+ ",SUCCESSFUL_INSERT_COUNT,FEED_INSERTION_TYPE_AT,FEED_INSERTION_TYPE"
				+ " FROM ETL_FEED_PROCESS_CONTROL Where COUNTRY = ? AND LE_BOOK = ? and SESSION_ID = ? AND FEED_CATEGORY = ? ";

		Object objParams[] = {dObj.getCountry(), dObj.getLeBook(), dObj.getSessionId(), dObj.getFeedCategory().toUpperCase()};

		try {
			getJdbcTemplate().update(strQueryAppr, objParams);
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			ExceptionCode exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			return exceptionCode;
		}
	}
	
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode reInitiateCategory(List<OperationalConsoleVb> categoryVbList) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			for (OperationalConsoleVb operationalConsoleVb : categoryVbList) {
				List<OperationalConsoleVb> feedVbLst = getQueryResults(operationalConsoleVb, 0);
				for (OperationalConsoleVb feedVb : feedVbLst) {
					if("S".equalsIgnoreCase(feedVb.getCompletionStatus()) || "E".equalsIgnoreCase(feedVb.getCompletionStatus())) {
						reInitiateFeed(feedVb);
					}
				}
			}
			exceptionCode.setErrorMsg("ETL Feed Process Control - Re-Initiate  - Successful");
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch(Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(e);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}
	
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode reInitiateFeedLvl(List<OperationalConsoleVb> feedVbList) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			for(OperationalConsoleVb feedVb: feedVbList) {
				reInitiateFeed(feedVb);
			}
			exceptionCode.setErrorMsg("ETL Feed Process Control - Re-Initiate  - Successful");
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch(Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(e);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}
	
	public void reInitiateFeed(OperationalConsoleVb vObject) {
		
		ExceptionCode exceptionCode = new ExceptionCode();
		OperationalConsoleVb existingRecordVb = new OperationalConsoleVb();
		
		DeepCopy<OperationalConsoleVb> deepCopy = new DeepCopy<OperationalConsoleVb>();
		OperationalConsoleVb clonedObject = deepCopy.copy(vObject);
		clonedObject.setGrid("2");
		List<OperationalConsoleVb> collTemp = getQueryResults(clonedObject, 0);

		if (collTemp == null || collTemp.size()==0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			existingRecordVb = collTemp.get(0);
		}
		/*Status Check for Re-initiate*/
		boolean checkCompletionStatus = false;
		for (OperationalConsoleVb vb : collTemp) {
			if (("S".equalsIgnoreCase(vb.getCompletionStatus())) && ("E".equalsIgnoreCase(vb.getCompletionStatus()))) {
				checkCompletionStatus = true;
				break;
			}
		}
		if (checkCompletionStatus) {
			return;
		}
		/*Status Check for Re-initiate*/
		exceptionCode = insertEtlProcessHistory(vObject);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			throw buildRuntimeCustomException(exceptionCode);
		}
		
		doDeleteFeedAppr(vObject);
		
		retVal = doInsertionProcessControl(vObject, existingRecordVb);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode.setErrorMsg("Insert failed in ETL_FEED_PROCESS_CONTROL");
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}
		
		EtlFeedMainVb etlFeedMainVb = new EtlFeedMainVb();
		etlFeedMainVb.setCountry(vObject.getCountry());
		etlFeedMainVb.setLeBook(vObject.getLeBook());
		etlFeedMainVb.setFeedId(vObject.getFeedId());
		etlFeedMainVb.setSessionId(vObject.getSessionId());
		etlFeedMainVb.setFeedCategory(vObject.getFeedCategory());
		etlFeedMainVb.setFeedType(existingRecordVb.getFeedType());
		etlFeedMainVb.setBatchSize(existingRecordVb.getBatchSize());

		etlFeedMainDao.deleteAllPrsData(etlFeedMainVb);
		collTemp = getFeedDetails(existingRecordVb);
		if (collTemp != null && collTemp.size() > 0) {
			existingRecordVb = collTemp.get(0);
		}
		StringJoiner etlSourceConnectorId = new StringJoiner(",");
		if (collTemp.size() > 1) {
			for (OperationalConsoleVb vb : collTemp) {
				etlSourceConnectorId.add(Constants.S_QUOTE + vb.getEtlSourceConnectorId() + Constants.S_QUOTE);
			}
			existingRecordVb.setEtlSourceConnectorId(etlSourceConnectorId.toString());
		} else {
			existingRecordVb.setEtlSourceConnectorId(
					Constants.S_QUOTE + existingRecordVb.getEtlSourceConnectorId() + Constants.S_QUOTE);
		}
		
		existingRecordVb.setSessionId(vObject.getSessionId());
		exceptionCode = doInsertionAppr(existingRecordVb, vObject.getSessionId());
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode.setErrorMsg("Insertion failed to PRS table");
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			retVal = updateFeedMainTableStatusForCS(existingRecordVb, "ETL_FEED_MAIN", "S");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			/* Get alert details - Start */
			ETLFeedProcessControlVb processControlVb = new ETLFeedProcessControlVb();
			processControlVb.setCountry(existingRecordVb.getCountry());
			processControlVb.setLeBook(existingRecordVb.getLeBook());
			processControlVb.setFeedId(existingRecordVb.getFeedId());
			processControlVb.setFeedCategory(existingRecordVb.getFeedCategory());
			processControlVb.setSessionId(existingRecordVb.getSessionId());
			processControlVb.setEventType("REINITIATE");
			exceptionCode = getAlertDetailsOfFeed(processControlVb);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if ("Y".equalsIgnoreCase(processControlVb.getAlertFlag())
						&& ValidationUtil.isValid(exceptionCode.getResponse()))
					processControlVb.setAlertVbMap((Map<String, ETLAlertVb>) exceptionCode.getResponse());
			}
			if (processControlVb.getAlertVbMap() != null && processControlVb.getAlertVbMap().size() > 0) {
				if (ValidationUtil.isValid(processControlVb.getEventType())
						&& processControlVb.getAlertVbMap().get(processControlVb.getEventType()) != null) {
					performAlertPush(processControlVb, processControlVb.getEventType());
				}
			}
		}
		
	}
	
	/*@Transactional
	public ExceptionCode reInitiateFeed(List<OperationalConsoleVb> vObjects, String grid) {
		OperationalConsoleVb vObjectlocal = null;
		setServiceDefaults();
		ExceptionCode exceptionCode = null;
		strCurrentOperation = "Re-Initiate";
		try {
			for (OperationalConsoleVb vObject : vObjects) {
				// if(vObject.isChecked() &&
				// "FIN".equalsIgnoreCase(vObject.getCurrentProcess())) {
				// check to see if the record already exists in the approved table
				vObject.setGrid(grid);
				List<OperationalConsoleVb> vb = getQueryResults(vObject, 0);
				vObjectlocal = vb.get(0);
				// If records are there, check for the status and decide what error to return
				// back
				if (vObjectlocal == null) {
					exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
					throw buildRuntimeCustomException(exceptionCode);
				}
				// move to process control history from etl feed process control
				exceptionCode = insertEtlProcessHistory(vObject);
				if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (ValidationUtil.isValid(grid) && "2".equalsIgnoreCase(grid)) {
					retVal = doDeleteFeedAppr(vObject);
				} else {
					retVal = doDeleteAppr(vObject);
				}
				retVal = doInsertionProcessControl(vObject, vObjectlocal);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setErrorMsg("Insert failed in ETL_FEED_PROCESS_CONTROL");
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				} else {
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				}
				EtlFeedMainVb etlFeedMainVb = new EtlFeedMainVb();
				etlFeedMainVb.setCountry(vObject.getCountry());
				etlFeedMainVb.setLeBook(vObject.getLeBook());
				etlFeedMainVb.setFeedId(vObject.getFeedId());
				etlFeedMainVb.setSessionId(vObject.getSessionId());
				etlFeedMainVb.setFeedCategory(vObject.getFeedCategory());
				etlFeedMainVb.setFeedType(vObject.getFeedType());
				etlFeedMainVb.setBatchSize(vObject.getBatchSize());

				etlFeedMainDao.deleteAllPrsData(etlFeedMainVb);
				List<OperationalConsoleVb> collTemp = getFeedDetails(vObjectlocal);
				if (collTemp != null && collTemp.size() > 0) {
					vObjectlocal = collTemp.get(0);
				}
				
				StringJoiner etlSourceConnectorId = new StringJoiner(",");
				if (collTemp.size() > 1) {
					for (OperationalConsoleVb vbNew : collTemp) {
						etlSourceConnectorId.add(Constants.S_QUOTE + vbNew.getEtlSourceConnectorId() + Constants.S_QUOTE);
					}
					vObjectlocal.setEtlSourceConnectorId(etlSourceConnectorId.toString());
				} else {
					vObjectlocal.setEtlSourceConnectorId(
							Constants.S_QUOTE + vObjectlocal.getEtlSourceConnectorId() + Constants.S_QUOTE);
				}
				String sessionId = String.valueOf(System.currentTimeMillis());
				vObjectlocal.setSessionId(vObject.getSessionId());
		//		vObjectlocal.setFeedType(vObject.getFeedType());
				vObjectlocal.setBatchSize(vObject.getBatchSize());
				vObjectlocal.setDebugFlag(vObject.getDebugFlag());
				vObjectlocal.setLastExtractionDate(vObject.getLastExtractionDate());
				exceptionCode = doInsertionAppr(vObjectlocal, vObject.getSessionId());
				if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setErrorMsg("Insertion failed to PRS table");
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				} else {
					retVal = updateFeedMainTableStatusForCS(vObjectlocal, "ETL_FEED_MAIN", "S");
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					 Get alert details - Start 
					ETLFeedProcessControlVb processControlVb = new ETLFeedProcessControlVb();
					processControlVb.setCountry(vObjectlocal.getCountry());
					processControlVb.setLeBook(vObjectlocal.getLeBook());
					processControlVb.setFeedId(vObjectlocal.getFeedId());
					processControlVb.setFeedCategory(vObjectlocal.getFeedCategory());
					processControlVb.setSessionId(vObjectlocal.getSessionId());
					processControlVb.setEventType("REINITIATE");
					exceptionCode = getAlertDetailsOfFeed(processControlVb);
					if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
						if ("Y".equalsIgnoreCase(processControlVb.getAlertFlag())
								&& ValidationUtil.isValid(exceptionCode.getResponse()))
							processControlVb.setAlertVbMap((Map<String, ETLAlertVb>) exceptionCode.getResponse());
					}
					if (processControlVb.getAlertVbMap() != null && processControlVb.getAlertVbMap().size() > 0) {
						if (ValidationUtil.isValid(processControlVb.getEventType())
								&& processControlVb.getAlertVbMap().get(processControlVb.getEventType()) != null) {
							performAlertPush(processControlVb, processControlVb.getEventType());
						}
					}
					 Get alert details - End 
				}
				// }
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage().trim();
			// ex.printStackTrace();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}*/

	protected int updateETLFeedScheduleConfigProcessTbl(OperationalConsoleVb vObject, String columnName, String value) {
		String query = "update ETL_FEED_SCHEDULE_CONFIG set " + columnName
				+ " = ? where COUNTRY = ? AND LE_BOOK = ? AND FEED_ID = ? ";
		Object[] args = { value, vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	protected ExceptionCode updateRecord(OperationalConsoleVb dObj) {
		int intKeyFieldsCount = 8;
		String query = "Update ETL_FEED_PROCESS_CONTROL set READINESS_START_TIME ='',READINESS_END_TIME ='',"
				+ "PRE_SCRIPT_START_TIME='',PRE_SCRIPT_END_TIME='',EXTRACTION_START_TIME='',"
				+ "EXTRACTION_END_TIME='',TRANSFORMATION_START_TIME='',"
				+ "TRANSFORMATION_END_TIME='',LOADING_START_TIME='',LOADING_END_TIME='',"
				+ "POST_SCRIPT_START_TIME='',POST_SCRIPT_END_TIME='',current_process =?  ,"
				+ " COMPLETION_STATUS=?, ITERATION_COUNT =?, VERSION_NUMBER =?"
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_CATEGORY = ?  and SESSION_ID=?";
		Object params[] = new Object[intKeyFieldsCount];
		params[0] = new String(dObj.getCurrentProcess());
		params[1] = new String(dObj.getCompletionStatus());
		params[2] = new Integer(dObj.getIterationCount());
		params[3] = new Integer(dObj.getVersionNumber());
		params[4] = new String(dObj.getCountry());
		params[5] = new String(dObj.getLeBook());
		params[6] = new String(dObj.getFeedCategory());
		params[7] = new String(dObj.getSessionId());
		try {
			getJdbcTemplate().update(query, params);
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			strErrorDesc = ex.getMessage();
			ExceptionCode exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			return exceptionCode;
		}
	}
	
	public int updateRecordForTerminationStatus(OperationalConsoleVb dObj) {
		Date now = new Date();
		String query = "UPDATE ETL_FEED_PROCESS_CONTROL SET CURRENT_PROCESS = ?, "
				+ " COMPLETION_STATUS = ?, TERMINATED_BY = ?, END_TIME = (CASE WHEN START_TIME IS NOT NULL AND START_TIME > 0 Then ?  else END_TIME end ) "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND FEED_CATEGORY = ?  AND SESSION_ID = ? ";

		Object params[] = { dObj.getCurrentProcess(), dObj.getCompletionStatus(), dObj.getTerminatedBy(), now.getTime(),
				dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId(), dObj.getFeedCategory(), dObj.getSessionId() };
		try {
			return getJdbcTemplate().update(query, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return 0;
		}
	}

	protected ExceptionCode updateFeedRecord(OperationalConsoleVb dObj) {
		int intKeyFieldsCount = 10;
		String query = "Update ETL_FEED_PROCESS_CONTROL set READINESS_START_TIME ='',READINESS_END_TIME ='',"
				+ "PRE_SCRIPT_START_TIME='',PRE_SCRIPT_END_TIME='',EXTRACTION_START_TIME='',"
				+ "EXTRACTION_END_TIME='',TRANSFORMATION_START_TIME='',"
				+ "TRANSFORMATION_END_TIME='',LOADING_START_TIME='',LOADING_END_TIME='',"
				+ "POST_SCRIPT_START_TIME='',POST_SCRIPT_END_TIME='',current_process =? ,COMPLETION_STATUS=?, DEBUG_FLAG =?, "
				+ " ITERATION_COUNT=?, VERSION_NUMBER=? WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_CATEGORY = ?  and SESSION_ID=? and FEED_ID=?";
		Object params[] = new Object[intKeyFieldsCount];
		params[0] = new String(dObj.getCurrentProcess());
		params[1] = new String(dObj.getCompletionStatus());
		params[2] = new String(dObj.getDebugFlag());
		params[3] = new Integer(dObj.getIterationCount());
		params[4] = new Integer(dObj.getVersionNumber());
		params[5] = new String(dObj.getCountry());
		params[6] = new String(dObj.getLeBook());
		params[7] = new String(dObj.getFeedCategory());
		params[8] = new String(dObj.getSessionId());
		params[9] = new String(dObj.getFeedId());
		try {
			getJdbcTemplate().update(query, params);
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			strErrorDesc = ex.getMessage();
			ExceptionCode exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			return exceptionCode;
		}
	}

	public Map<String, String> getFeedCategory() {
		String sql = "select COUNTRY, LE_BOOK, CATEGORY_ID, catEGORY_dESCRIPTION from ETL_feed_category";
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					String key = rs.getString("COUNTRY") + "-" + rs.getString("LE_BOOK");
					String value = rs.getString("CATEGORY_ID") + "-" + rs.getString("catEGORY_dESCRIPTION");
					if (returnMap.containsKey(key)) {
						String val = returnMap.get(key);
						returnMap.remove(key);
						returnMap.put(key, val + "," + value);

					} else {
						returnMap.put(key, value);
					}
				}
				return returnMap;
			}
		};
		return (Map<String, String>) getJdbcTemplate().query(sql, rseObj);
	}

	public Map<String, String> getFeedIDBasedonCategory() {
		String sql = "SELECT  t1.COUNTRY, t1.LE_BOOK, FEED_NAME ,T1.FEED_ID,T1.FEED_CATEGORY FROM ETL_FEED_PROCESS_CONTROL t1, "
				+ " ETL_FEED_MAIN T2 WHERE T2.FEED_STATUS =0 AND T1.COUNTRY=T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.FEED_ID=T2.FEED_ID "
				+ " AND T1.FEED_CATEGORY =T2.FEED_CATEGORY";
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					String key = rs.getString("COUNTRY") + "-" + rs.getString("LE_BOOK") + "-"
							+ rs.getString("FEED_CATEGORY");
					String value = rs.getString("FEED_ID") + "-" + rs.getString("FEED_NAME");
					if (returnMap.containsKey(key)) {
						String val = returnMap.get(key);
						returnMap.remove(key);
						returnMap.put(key, val + "," + value);

					} else {
						returnMap.put(key, value);
					}
				}
				return returnMap;
			}
		};
		return (Map<String, String>) getJdbcTemplate().query(sql, rseObj);
	}

	public Map<String, String> getBusinessDateTimeZone() {
		OperationalConsoleVb vObject = new OperationalConsoleVb();
		String sql = "select COUNTRY, LE_BOOK, TIME_ZONE, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(BUSINESS_DATE,'" + getDbFunction(Constants.DD_Mon_RRRR, null)
				+ "') BUSINESS_DATE  from Vision_Business_Day WHERE APPLICATION_ID='ETL'";
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					String key = rs.getString("COUNTRY") + "-" + rs.getString("LE_BOOK");
					String value = rs.getString("BUSINESS_DATE") + vObject.getAuditDelimiter() + rs.getString("TIME_ZONE");
					if (returnMap.containsKey(key)) {
						String val = returnMap.get(key);
						returnMap.remove(key);
						returnMap.put(key, val + "," + value);

					} else {
						returnMap.put(key, value);
					}
				}
				return returnMap;
			}
		};
		return (Map<String, String>) getJdbcTemplate().query(sql, rseObj);
	}

	@SuppressWarnings("unchecked")
	public List<OperationalConsoleVb> findFeed(OperationalConsoleVb vObject) throws DataAccessException {
		String sql = "";
		if (!ValidationUtil.isValid(vObject.getSessionId())) {
			/*sql = "SELECT   T1.COUNTRY,                                         "+
					"  T1.LE_BOOK,  T1.FEED_ID,                                   "+
					"  T1.FEED_NAME, T1.FEED_TYPE,                                "+
					"  T1.BATCH_SIZE,                                             "+
					"  T1.LAST_EXTRACTION_DATE                                    "+
					" FROM  ETL_FEED_MAIN T1   WHERE                              "+
					"  EXISTS ( SELECT                                            "+
					"      SOURCE_CONNECTOR_ID CONNECTOR_ID                       "+
					"    FROM  ETL_FEED_SOURCE TS                                 "+
					"    WHERE                                                    "+
					"      EXISTS (                                               "+
					"        SELECT  CONNECTOR_ID                                 "+
					"        FROM   ETL_CONNECTOR TC                              "+
					"        WHERE                                                "+
					"          TC.COUNTRY = TS.COUNTRY                            "+
					"          AND TC.LE_BOOK = TS.LE_BOOK                        "+
					"          AND TC.CONNECTOR_ID = TS.SOURCE_CONNECTOR_ID       "+
					"          AND TC.CONNECTION_STAUTS = 0                       "+
					"      )                                                      "+
					"      AND TS.COUNTRY = T1.COUNTRY                            "+
					"      AND TS.LE_BOOK = T1.LE_BOOK                            "+
					"      AND TS.FEED_ID = T1.FEED_ID  )                         "+
					"  AND EXISTS (                                               "+
					"    SELECT                                                   "+
					"      DESTINATION_CONNECTOR_ID CONNECTOR_ID                  "+
					"    FROM                                                     "+
					"      ETL_FEED_DESTINATION TD                                "+
					"    WHERE                                                    "+
					"      EXISTS (                                               "+
					"        SELECT                                               "+
					"          CONNECTOR_ID                                       "+
					"        FROM                                                 "+
					"          ETL_CONNECTOR TC                                   "+
					"        WHERE                                                "+
					"          TC.COUNTRY = TD.COUNTRY                            "+
					"          AND TC.LE_BOOK = TD.LE_BOOK                        "+
					"          AND TC.CONNECTOR_ID = TD.DESTINATION_CONNECTOR_ID  "+
					"          AND TC.CONNECTION_STAUTS = 0  )                    "+
					"      AND TD.COUNTRY = T1.COUNTRY                            "+
					"      AND TD.LE_BOOK = T1.LE_BOOK                            "+
					"      AND TD.FEED_ID = T1.FEED_ID  )                         "+
					"  AND T1.FEED_STATUS = 0                                     "+
					"  AND T1.FEED_CATEGORY = ?                                   "+
					"  and T1.COUNTRY = ?                                         "+
					"  AND T1.LE_BOOK = ?                                         "+
					" ORDER BY   FEED_NAME                                        ";*/
			
			sql = " SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME, T1.FEED_TYPE, T1.BATCH_SIZE, T1.LAST_EXTRACTION_DATE,  "
					+ " (CASE WHEN TSC.SCHEDULE_PROP_CONTEXT LIKE '%<type>M</type>%' THEN 'Y' ELSE 'N' END) AS MANUAL_FEED_FLAG "
					+ " FROM ETL_FEED_MAIN T1 LEFT JOIN ETL_FEED_SCHEDULE_CONFIG TSC "
					+ "     ON ( T1.COUNTRY = TSC.COUNTRY AND T1.LE_BOOK = TSC.LE_BOOK "
					+ "     AND T1.FEED_ID = TSC.FEED_ID ) "
					/*+ "  LEFT JOIN ETL_FEED_PROCESS_CONTROL FPC ON (  T1.COUNTRY = FPC.COUNTRY "
					+ "  AND T1.LE_BOOK = FPC.LE_BOOK  AND T1.FEED_ID = FPC.FEED_ID ) "*/
					+ " WHERE T1.COUNTRY = ? AND T1.LE_BOOK = ? AND T1.FEED_CATEGORY = ? AND T1.FEED_STATUS = 0 "
					+ " AND EXISTS(SELECT SOURCE_CONNECTOR_ID CONNECTOR_ID FROM ETL_FEED_SOURCE TS "
					+ "                 WHERE EXISTS (SELECT CONNECTOR_ID FROM ETL_CONNECTOR TC "
					+ "                                 WHERE TC.COUNTRY = TS.COUNTRY AND TC.LE_BOOK = TS.LE_BOOK "
					+ "                                 AND TC.CONNECTOR_ID = TS.SOURCE_CONNECTOR_ID AND TC.CONNECTION_STAUTS = 0) "
					+ "                 AND TS.COUNTRY = T1.COUNTRY AND TS.LE_BOOK = T1.LE_BOOK AND TS.FEED_ID = T1.FEED_ID) "
					+ " AND EXISTS(SELECT DESTINATION_CONNECTOR_ID CONNECTOR_ID FROM ETL_FEED_DESTINATION TD "
					+ "                 WHERE EXISTS (SELECT CONNECTOR_ID FROM ETL_CONNECTOR TC "
					+ "                                 WHERE TC.COUNTRY = TD.COUNTRY AND TC.LE_BOOK = TD.LE_BOOK "
					+ "                                 AND TC.CONNECTOR_ID = TD.DESTINATION_CONNECTOR_ID AND TC.CONNECTION_STAUTS = 0) "
					+ "                 AND TD.COUNTRY = T1.COUNTRY AND TD.LE_BOOK = T1.LE_BOOK AND TD.FEED_ID = T1.FEED_ID) "
					+ " ORDER BY FEED_NAME ";
			Object args[] = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory()};
			return getJdbcTemplate().query(sql, args, getDependencyFeedMapper());
		} else {
			/*sql = "SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME, T1.FEED_TYPE, T1.BATCH_SIZE, T1.LAST_EXTRACTION_DATE FROM ETL_FEED_MAIN T1 "
					+ " WHERE T1.FEED_STATUS=0 AND  T1.FEED_CATEGORY=? and  T1.COUNTRY = ? AND T1.LE_BOOK = ?  AND"
					+ " NOT EXISTS (select * from ETL_FEED_PROCESS_CONTROL T2 WHERE T1.FEED_ID = T2.FEED_ID and "
					+ " T2.SESSION_ID='" + vObject.getSessionId() + "') ORDER BY FEED_NAME";*/
			sql = " SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME, T1.FEED_TYPE, T1.BATCH_SIZE, T1.LAST_EXTRACTION_DATE, "
					+ " (CASE WHEN TSC.SCHEDULE_PROP_CONTEXT LIKE '%<type>M</type>%' THEN 'Y' ELSE 'N' END) AS MANUAL_FEED_FLAG  "
					+ " FROM ETL_FEED_MAIN T1 LEFT JOIN ETL_FEED_SCHEDULE_CONFIG TSC "
					+ "     ON ( T1.COUNTRY = TSC.COUNTRY AND T1.LE_BOOK = TSC.LE_BOOK "
					+ "     AND T1.FEED_ID = TSC.FEED_ID )"
					/*+ "  LEFT JOIN ETL_FEED_PROCESS_CONTROL FPC ON (  T1.COUNTRY = FPC.COUNTRY "
					+ "  AND T1.LE_BOOK = FPC.LE_BOOK  AND T1.FEED_ID = FPC.FEED_ID ) "*/
					+ " WHERE T1.COUNTRY = ? AND T1.LE_BOOK = ? AND T1.FEED_CATEGORY = ? AND T1.FEED_STATUS = 0 "
					+ " AND NOT EXISTS(SELECT 1 FROM ETL_FEED_PROCESS_CONTROL T2  "
					+ " 					WHERE T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK "
					+ " 					AND T1.FEED_ID = T2.FEED_ID AND T2.SESSION_ID = ?) "
					+ " AND EXISTS(SELECT SOURCE_CONNECTOR_ID CONNECTOR_ID FROM ETL_FEED_SOURCE TS "
					+ "                 WHERE EXISTS (SELECT CONNECTOR_ID FROM ETL_CONNECTOR TC "
					+ "                                 WHERE TC.COUNTRY = TS.COUNTRY AND TC.LE_BOOK = TS.LE_BOOK "
					+ "                                 AND TC.CONNECTOR_ID = TS.SOURCE_CONNECTOR_ID AND TC.CONNECTION_STAUTS = 0) "
					+ "                 AND TS.COUNTRY = T1.COUNTRY AND TS.LE_BOOK = T1.LE_BOOK AND TS.FEED_ID = T1.FEED_ID) "
					+ " AND EXISTS(SELECT DESTINATION_CONNECTOR_ID CONNECTOR_ID FROM ETL_FEED_DESTINATION TD "
					+ "                 WHERE EXISTS (SELECT CONNECTOR_ID FROM ETL_CONNECTOR TC "
					+ "                                 WHERE TC.COUNTRY = TD.COUNTRY AND TC.LE_BOOK = TD.LE_BOOK "
					+ "                                 AND TC.CONNECTOR_ID = TD.DESTINATION_CONNECTOR_ID AND TC.CONNECTION_STAUTS = 0) "
					+ "                 AND TD.COUNTRY = T1.COUNTRY AND TD.LE_BOOK = T1.LE_BOOK AND TD.FEED_ID = T1.FEED_ID) "
					+ " ORDER BY FEED_NAME ";
			Object args[] = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory(), vObject.getSessionId()};
			return getJdbcTemplate().query(sql, args, getDependencyFeedMapper());
		}
		
	}

	protected RowMapper getDependencyFeedMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb vObject = new OperationalConsoleVb();
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setFeedId(rs.getString("FEED_ID"));
				vObject.setFeedType(rs.getString("FEED_TYPE"));
				vObject.setBatchSize(rs.getInt("BATCH_SIZE"));
				vObject.setFeedId(rs.getString("FEED_ID"));
				vObject.setFeedName(rs.getString("FEED_NAME"));
				vObject.setLastExtractionDate(rs.getString("LAST_EXTRACTION_DATE"));
				if (ValidationUtil.isValid(rs.getString("LAST_EXTRACTION_DATE"))) {
					vObject.setLastExtractionDate(printTimeFromMilliSecondsAsString(
							rs.getString("LAST_EXTRACTION_DATE"), vObject.getTimeZone()));
				}
				vObject.setEventType(rs.getString("MANUAL_FEED_FLAG"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<OperationalConsoleVb> getQueryResults(OperationalConsoleVb dObj, int intStatus) {
		setServiceDefaults();
		List<OperationalConsoleVb> collTemp = null;
		int intKeyFieldsCount = 4;
		if (ValidationUtil.isValid(dObj.getGrid()) && "2".equalsIgnoreCase(dObj.getGrid())) {
			intKeyFieldsCount = 5;
		}
		String query = "select TAPPR.FEED_INSERTION_TYPE, TAPPR.COUNTRY, TAPPR.LE_BOOK, TAPPR.FEED_ID, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.PROCESS_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + "') PROCESS_DATE, "
				+ " TAPPR.SESSION_ID, TAPPR.START_TIME, TAPPR.END_TIME, TAPPR.TRIGGERED_BY, TAPPR.TERMINATED_BY, TAPPR.REINITIATED_BY,"
				+ " TAPPR.CURRENT_PROCESS_AT, TAPPR.CURRENT_PROCESS, TAPPR.ITERATION_COUNT, "
				+ " TAPPR.COMPLETION_STATUS_AT, TAPPR.COMPLETION_STATUS, TAPPR.ALERT_STATUS_AT,"
				+ " TAPPR.ALERT_STATUS, TAPPR.ETL_PROCESS_STATUS_NT, TAPPR.ETL_PROCESS_STATUS,"
				+ " TAPPR.RECORD_INDICATOR_NT, TAPPR.RECORD_INDICATOR, "
				+ " TAPPR.MAKER, TAPPR.VERIFIER, TAPPR.INTERNAL_STATUS, TAPPR.DATE_LAST_MODIFIED, TAPPR.DATE_CREATION,"
				+ " TAPPR.EVENT_TYPE_AT,EVENT_TYPE, TAPPR.DEPENDENCY_FEED_ID, TAPPR.NEXT_SCHEDULE_DATE, TAPPR.FEED_CATEGORY, "
				+ " TAPPR.SCHEDULE_TYPE_AT, TAPPR.SCHEDULE_TYPE, TAPPR.VERSION_NUMBER, T1.FEED_TYPE, T2.SCHEDULE_PROP_CONTEXT, T1.BATCH_SIZE "
				+ " FROM ETL_FEED_PROCESS_CONTROL TAPPR INNER JOIN ETL_FEED_MAIN T1 "
				+ " ON (TAPPR.COUNTRY = T1.COUNTRY AND TAPPR.LE_BOOK = T1.LE_BOOK AND TAPPR.FEED_ID = T1.FEED_ID AND "
				+ " TAPPR.FEED_CATEGORY = T1.FEED_CATEGORY ) INNER JOIN ETL_FEED_SCHEDULE_CONFIG T2"
				+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.FEED_ID = T2.FEED_ID) "
				+ " WHERE UPPER(TAppr.COUNTRY) = ?  AND UPPER(TAppr.LE_BOOK) = ? "
				+ " AND UPPER(TAppr.SESSION_ID) = ? AND UPPER(TAppr.FEED_CATEGORY) = ? ";
		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry().toUpperCase();
		objParams[1] = dObj.getLeBook().toUpperCase();
		objParams[2] = dObj.getSessionId();
		objParams[3] = dObj.getFeedCategory().toUpperCase();
		if (ValidationUtil.isValid(dObj.getGrid()) && "2".equalsIgnoreCase(dObj.getGrid())) {
			objParams[4] = dObj.getFeedId().toUpperCase();
			query = query + " AND UPPER(TAppr.FEED_ID) = ?";
		}
		/*if (terminateFlag) {
			query = query + " AND TAPPR.COMPLETION_STATUS IN ('P', 'I', 'R') ";
		}*/
		try {
			collTemp = getJdbcTemplate().query(query, objParams, getLocalMapper());
			return collTemp.stream().filter(obj -> obj != null).collect(Collectors.toList());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	protected RowMapper getLocalMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				try {
					OperationalConsoleVb vObject = new OperationalConsoleVb();
					
					String propContext = rs.getString("SCHEDULE_PROP_CONTEXT");
					ETLFeedTimeBasedPropVb timePropVb = new ETLFeedTimeBasedPropVb();
					ETLFeedEventBasedPropVb eventPropVb = new ETLFeedEventBasedPropVb();
					
					vObject.setFeedInsertionType(rs.getString("FEED_INSERTION_TYPE"));
					vObject.setCountry(rs.getString("COUNTRY"));
					vObject.setLeBook(rs.getString("LE_BOOK"));
					vObject.setFeedId(rs.getString("FEED_ID"));
					vObject.setProcessDate(rs.getString("PROCESS_DATE"));
					vObject.setSessionId(rs.getString("SESSION_ID"));
					vObject.setEndTime(rs.getString("START_TIME"));
					vObject.setStartTime(rs.getString("END_TIME"));
					vObject.setTriggeredBy(rs.getInt("TRIGGERED_BY"));
					vObject.setTerminatedBy(rs.getInt("TERMINATED_BY"));
					vObject.setReinitiatedBy(rs.getInt("REINITIATED_BY"));
					vObject.setCurrentProcess(rs.getString("CURRENT_PROCESS"));
					vObject.setCurrentProcessAT(rs.getInt("CURRENT_PROCESS_AT"));
					vObject.setIterationCount(rs.getInt("ITERATION_COUNT"));
					vObject.setCompletionStatusAT(rs.getInt("COMPLETION_STATUS_AT"));
					vObject.setCompletionStatus(rs.getString("COMPLETION_STATUS"));
					vObject.setAlertStatusAT(rs.getInt("ALERT_STATUS_AT"));
					vObject.setAlertStatus(rs.getString("ALERT_STATUS"));
					vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
					vObject.setNextScheduleDate(rs.getString("NEXT_SCHEDULE_DATE"));
					vObject.setEtlProcessStatusNT(rs.getInt("ETL_PROCESS_STATUS_NT"));
					vObject.setEtlProcessStatus(rs.getString("ETL_PROCESS_STATUS"));
					vObject.setAlertStatusAT(rs.getInt("ALERT_STATUS_AT"));
					vObject.setAlertStatus(rs.getString("ALERT_STATUS"));
					vObject.setScheduleTypeAt(rs.getInt("SCHEDULE_TYPE_AT"));
					vObject.setScheduleType(rs.getString("SCHEDULE_TYPE"));
					vObject.setDependencyFeedId(rs.getString("DEPENDENCY_FEED_ID"));
					vObject.setVersionNumber(rs.getInt("version_number"));
					vObject.setFeedType(rs.getString("Feed_type"));
					vObject.setBatchSize(rs.getInt("BATCH_SIZE"));
					
					XmlMapper xmlMapper = new XmlMapper();
					if("TIME".equalsIgnoreCase(vObject.getScheduleType())) {
						timePropVb = xmlMapper.readValue(rs.getString("SCHEDULE_PROP_CONTEXT"), ETLFeedTimeBasedPropVb.class);
						vObject.setTimeBasedScheduleProp(timePropVb);
						vObject.setReadinessScriptID(timePropVb.getReadinessScriptID());
						vObject.setPreScriptID(timePropVb.getPreScriptID());
						vObject.setPostScriptID(timePropVb.getPostScriptID());
					} else if ("EVENT".equalsIgnoreCase(vObject.getScheduleType())) {
						
						eventPropVb.setType(CommonUtils.getValueForXmlTag(propContext, "type"));
						eventPropVb.setReadinessScriptID(CommonUtils.getValueForXmlTag(propContext, "readinessScriptID"));
						eventPropVb.setPreScriptID(CommonUtils.getValueForXmlTag(propContext, "preScriptID"));
						eventPropVb.setPostScriptID(CommonUtils.getValueForXmlTag(propContext, "postScriptID"));
						
						vObject.setEventBasedScheduleProp(eventPropVb);
						vObject.setEventType(eventPropVb.getType());
						vObject.setReadinessScriptID(eventPropVb.getReadinessScriptID());
						vObject.setPreScriptID(eventPropVb.getPreScriptID());
						vObject.setPostScriptID(eventPropVb.getPostScriptID());
						
						/*eventPropVb = xmlMapper.readValue(propContext, ETLFeedEventBasedPropVb.class);
						processControlVb.setEventBasedScheduleProp(eventPropVb);
						processControlVb.setEventType(eventPropVb.getType());
						if("D".equalsIgnoreCase(eventPropVb.getType()))
							processControlVb.setDependencyFeedId(eventPropVb.getDependency().getFeed().getId());
						processControlVb.setReadinessScriptID(eventPropVb.getReadinessScriptID());*/
					}
					return vObject;
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					return null;
				}
				
			}
		};
		return mapper;
	}

	public String getTimeZoneVisionBusinessDate(String countryLeBook) {
		String sql = "select TIME_ZONE  from Vision_Business_Day  WHERE COUNTRY "
				+ getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null) + " LE_BOOK=? AND UPPER(APPLICATION_ID) = 'ETL'";
		Object args[] = { countryLeBook };
		return getJdbcTemplate().queryForObject(sql, args, String.class);
	}

	public String calculateProcessDate(String processType, String country, String leBook) throws ParseException {
		String feedTimeZone = getTimeZoneVisionBusinessDate(country + "-" + leBook);
		// Get time zone based on country wise
		SimpleDateFormat specCountryDateZonedf = new SimpleDateFormat("dd-MMM-yyyy");
		TimeZone countryTimeZone = TimeZone.getTimeZone(feedTimeZone);
		specCountryDateZonedf.setTimeZone(countryTimeZone);

		String processDate = "";
		/*
		 * if(ValidationUtil.isValid(processType) &&
		 * "BDATE".equalsIgnoreCase(processType)) { processDate =
		 * getVisionBusinessDate(country+"-"+leBook); Date processDateZone =
		 * specCountryDateZonedf.parse(processDate); processDate =
		 * specCountryDateZonedf.format(processDateZone); } else { Date
		 * currentSystemDate = new Date(); processDate =
		 * specCountryDateZonedf.format(currentSystemDate); }
		 */

		if (ValidationUtil.isValid(processType) && (processType.toUpperCase()).startsWith("BDATE")) {

			String appID = "ETL";
			if (processType.contains("_")) {
				String[] arr = processType.split("_");
				appID = arr[1];
			}

			processDate = getVisionBusinessDate(country + "-" + leBook, appID);
			Date processDateZone = specCountryDateZonedf.parse(processDate);
			processDate = specCountryDateZonedf.format(processDateZone);
		} else {
			Date currentSystemDate = new Date();
			processDate = specCountryDateZonedf.format(currentSystemDate);
		}

		return processDate;
	}

	public String getVisionBusinessDate(String countryLeBook, String appID) {
		String sql = "select " + getDbFunction(Constants.DATEFUNC, null) + "(BUSINESS_DATE,'"
				+ getDbFunction(Constants.DD_Mon_RRRR, null)
				+ "') BUSINESS_DATE  from Vision_Business_Day  WHERE COUNTRY "
				+ getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null) + " LE_BOOK=? AND UPPER(APPLICATION_ID) = ?";
		Object args[] = { countryLeBook, appID.toUpperCase() };
		return getJdbcTemplate().queryForObject(sql, args, String.class);
	}

	protected int doInsertionAppr(OperationalConsoleVb vObject) {
		if (ValidationUtil.isValid(vObject.getTimeZone()) && ValidationUtil.isValid(vObject.getNextScheduleDate())) {
			if ((vObject.getNextScheduleDate().length() == 11 || vObject.getNextScheduleDate().length() < 11)
					&& vObject.getNextScheduleDate().contains("-"))
				vObject.setNextScheduleDate(vObject.getNextScheduleDate() + " 00:00:00");

			if (vObject.getNextScheduleDate().contains("-")) {
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
				SimpleDateFormat sdf1 = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
				sdf.setTimeZone(TimeZone.getTimeZone(vObject.getTimeZone()));
				Date date1 = null;
				Date date2 = null;
				try {
					date1 = sdf.parse(vObject.getNextScheduleDate());
					date2 = sdf1.parse(sdf1.format(date1));
				} catch (ParseException e) {
					// e.printStackTrace();
				}
				long millisecondsSinceUnixEpoch = date2.getTime();
				vObject.setNextScheduleDate(String.valueOf(millisecondsSinceUnixEpoch));
			}
		}
		vObject.setCurrentProcess("SUBMITTED");
		vObject.setCompletionStatus("P");
		vObject.setAlertStatus("YTP");
		vObject.setEtlProcessStatus("0");
		vObject.setMaker(getIntCurrentUserId());
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		vObject.setScheduleType(vObject.getScheduleType());
		if (ValidationUtil.isValid(vObject.getTriggeredBy())) {
			int intUser = (int) getIntCurrentUserId();
			vObject.setTriggeredBy(intUser);
		}
		if(!ValidationUtil.isValid(vObject.getDependencyFlag())) {
			vObject.setDependencyFlag("D");
		}
		String query = "Insert Into ETL_FEED_PROCESS_CONTROL ( COUNTRY, LE_BOOK, FEED_ID, PROCESS_DATE,"
				+ " SESSION_ID, START_TIME, END_TIME,  TRIGGERED_BY, TERMINATED_BY, REINITIATED_BY,"
				+ " CURRENT_PROCESS_AT, CURRENT_PROCESS, ITERATION_COUNT, "
				+ " COMPLETION_STATUS_AT, COMPLETION_STATUS, ALERT_STATUS_AT,"
				+ " ALERT_STATUS, ETL_PROCESS_STATUS_NT, ETL_PROCESS_STATUS,"
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ " EVENT_TYPE_AT,EVENT_TYPE, DEPENDENCY_FEED_ID, FEED_CATEGORY,NEXT_SCHEDULE_DATE,SCHEDULE_TYPE_AT,SCHEDULE_TYPE,DEBUG_FLAG, FEED_INSERTION_TYPE, "
				+ " PRE_SCRIPT_ID, POST_SCRIPT_ID, READINESS_SCRIPT_ID, DEPENDENCY_CHECK_FLAG)"
				+ " Values (?, ?, ?, " + getDbFunction(Constants.TO_DATE, vObject.getProcessDate())
				+ ", ?, ?, ?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ","
				+ getDbFunction(Constants.SYSDATE, null) + ",?,?, ?, ?,  ?,?,?,?,'A', ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId(),
				/*(ValidationUtil.isValid(vObject.getStartTime()) ? Long.parseLong(vObject.getStartTime()) : 0),
				(ValidationUtil.isValid(vObject.getEndTime()) ? Long.parseLong(vObject.getEndTime()) : 0),*/
				0,0,
				vObject.getTriggeredBy(), vObject.getTerminatedBy(), vObject.getReinitiatedBy(),
				vObject.getCurrentProcessAT(), vObject.getCurrentProcess(), vObject.getIterationCount(),
				vObject.getCompletionStatusAT(), vObject.getCompletionStatus(), vObject.getAlertStatusAT(),
				vObject.getAlertStatus(), vObject.getEtlProcessStatusNT(), vObject.getEtlProcessStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getEventTypeAt(), vObject.getEventType(),
				vObject.getDependencyFeedId(), vObject.getFeedCategory(),
				(ValidationUtil.isValid(vObject.getNextScheduleDate()) ? Long.parseLong(vObject.getNextScheduleDate()) : 0),
				vObject.getScheduleTypeAt(), vObject.getScheduleType(), vObject.getDebugFlag(),
				vObject.getPreScriptID(), vObject.getPostScriptID(), vObject.getReadinessScriptID(),
				vObject.getDependencyFlag()};
		return getJdbcTemplate().update(query, args);
	}

	protected int doInsertionProcessControl(OperationalConsoleVb vObject, OperationalConsoleVb existingRecordData) {
		vObject.setCountry(existingRecordData.getCountry());
		vObject.setLeBook(existingRecordData.getLeBook());
		vObject.setFeedId(existingRecordData.getFeedId());
		vObject.setSessionId(existingRecordData.getSessionId());
		vObject.setDateCreation(existingRecordData.getDateCreation());
		vObject.setIterationCount(0);
		vObject.setVersionNumber(existingRecordData.getVersionNumber() + 1);
		int intUser = (int) getIntCurrentUserId();
		vObject.setReinitiatedBy(intUser);
		vObject.setCurrentProcess("SUBMITTED");
		vObject.setCompletionStatus("R");
		vObject.setProcessDate(existingRecordData.getProcessDate());
		vObject.setNextScheduleDate(existingRecordData.getNextScheduleDate());
		vObject.setAlertStatus("SUC");
		vObject.setEtlProcessStatus("0");
		vObject.setMaker(getIntCurrentUserId());
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		vObject.setScheduleType(existingRecordData.getScheduleType());
		vObject.setPreScriptID(existingRecordData.getPreScriptID());
		vObject.setReadinessScriptID(existingRecordData.getReadinessScriptID());
		vObject.setPostScriptID(existingRecordData.getPostScriptID());

		// vObject.setScheduleType("EVENT");
		
		// check this later 
		existingRecordData.setDependencyFlag(vObject.getDependencyFlag());
		
		if(!ValidationUtil.isValid(vObject.getDependencyFlag())) {
			vObject.setDependencyFlag("D");
		}
		
		String query = "Insert Into ETL_FEED_PROCESS_CONTROL ( COUNTRY, LE_BOOK, FEED_ID, PROCESS_DATE,"
				+ " SESSION_ID, START_TIME, END_TIME,  TRIGGERED_BY, TERMINATED_BY, REINITIATED_BY,"
				+ " CURRENT_PROCESS_AT, CURRENT_PROCESS, ITERATION_COUNT, "
				+ " COMPLETION_STATUS_AT, COMPLETION_STATUS, ALERT_STATUS_AT,"
				+ " ALERT_STATUS, ETL_PROCESS_STATUS_NT, ETL_PROCESS_STATUS,"
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ " EVENT_TYPE_AT,EVENT_TYPE, DEPENDENCY_FEED_ID, FEED_CATEGORY,NEXT_SCHEDULE_DATE,SCHEDULE_TYPE_AT,SCHEDULE_TYPE,DEBUG_FLAG,"
				+ "FEED_INSERTION_TYPE, VERSION_NUMBER,  READINESS_SCRIPT_ID, PRE_SCRIPT_ID, POST_SCRIPT_ID, DEPENDENCY_CHECK_FLAG )"
				+ " Values (?, ?, ?, " + getDbFunction(Constants.TO_DATE, vObject.getProcessDate())
				+ " , ?, ?, ?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ","
				+ getDbFunction(Constants.SYSDATE, null) + ",?,?, ?, ?,  ?,?,?,?,'A', ?, ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId(),
				/*(ValidationUtil.isValid(vObject.getStartTime()) ? Long.parseLong(vObject.getStartTime()) : 0),
				(ValidationUtil.isValid(vObject.getEndTime()) ? Long.parseLong(vObject.getEndTime()) : 0),*/
				0,0,
				vObject.getTriggeredBy(), vObject.getTerminatedBy(), vObject.getReinitiatedBy(),
				vObject.getCurrentProcessAT(), vObject.getCurrentProcess(), vObject.getIterationCount(),
				vObject.getCompletionStatusAT(), vObject.getCompletionStatus(), vObject.getAlertStatusAT(),
				vObject.getAlertStatus(), vObject.getEtlProcessStatusNT(), vObject.getEtlProcessStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getEventTypeAt(), vObject.getEventType(),
				vObject.getDependencyFeedId(), vObject.getFeedCategory(),
				(ValidationUtil.isValid(vObject.getNextScheduleDate()) ? Long.parseLong(vObject.getNextScheduleDate()) : 0),
				vObject.getScheduleTypeAt(), vObject.getScheduleType(), vObject.getDebugFlag(),
				vObject.getVersionNumber(), vObject.getReadinessScriptID(), vObject.getPreScriptID(),
				vObject.getPostScriptID(), vObject.getDependencyFlag() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected List<OperationalConsoleVb> selectApprovedRecord(OperationalConsoleVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	public String cndApply(OperationalConsoleVb dObj, String grid) {
		StringBuffer formationstring = new StringBuffer();

		if (ValidationUtil.isValid(dObj.getPromptValue2())) {
			dObj.setPromptValue2(dObj.getPromptValue2().replaceAll("ALL-ALL", "ALL"));
		}
		if (ValidationUtil.isValid(dObj.getPromptValue3())) {
			dObj.setPromptValue3(dObj.getPromptValue3().replaceAll("ALL-ALL", "ALL"));
		}
		if (ValidationUtil.isValid(dObj.getPromptValue4())) {
			dObj.setPromptValue4(dObj.getPromptValue4().replaceAll("ALL-ALL", "ALL"));
		}
		if (grid.equalsIgnoreCase("2")) {
			if (ValidationUtil.isValid(dObj.getCountry()) && ValidationUtil.isValid(dObj.getLeBook())) {
				formationstring.append("AND (T1.COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
						+ getDbFunction(Constants.PIPELINE, null) + "T1.LE_BOOK IN ('" + dObj.getCountry() + '-'
						+ dObj.getLeBook() + "') OR 'ALL-ALL' IN ('" + dObj.getCountry() + '-' + dObj.getLeBook()
						+ "')) ");
			}
			if (ValidationUtil.isValid(dObj.getFeedCategory())) {
				formationstring.append("AND (T1.FEED_CATEGORY IN ('" + dObj.getFeedCategory() + "') OR 'ALL' IN ('"
						+ dObj.getFeedCategory() + "')) ");
			}
		}
		if (ValidationUtil.isValid(dObj.getPromptValue1())
				|| (ValidationUtil.isValid(dObj.getCountry()) && ValidationUtil.isValid(dObj.getLeBook()))) {
			if (!ValidationUtil.isValid(dObj.getPromptValue1())) {
				dObj.setPromptValue1(dObj.getCountry() + '-' + dObj.getLeBook());
			}
			if (ValidationUtil.isValid(dObj.getCountry()) && ValidationUtil.isValid(dObj.getLeBook())) {
				formationstring.append("AND (T1.COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
						+ getDbFunction(Constants.PIPELINE, null) + "T1.LE_BOOK IN ('" + dObj.getCountry() + '-'
						+ dObj.getLeBook() + "') OR 'ALL-ALL' IN ('" + dObj.getCountry() + '-' + dObj.getLeBook()
						+ "')) ");
			} else {
				formationstring.append("AND (T1.COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
						+ getDbFunction(Constants.PIPELINE, null)
						+ "T1.LE_BOOK IN (#PROMPT_VALUE_1#) OR 'ALL-ALL' IN (#PROMPT_VALUE_1#)) ");
			}
		}
		if (ValidationUtil.isValid(dObj.getPromptValue2()) || ValidationUtil.isValid(dObj.getFeedCategory())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue2())) {
				dObj.setPromptValue2(dObj.getFeedCategory());
			}
			if (ValidationUtil.isValid(dObj.getFeedCategory())) {
				formationstring.append("AND (T1.FEED_CATEGORY IN ('" + dObj.getFeedCategory() + "') OR 'ALL' IN ('"
						+ dObj.getFeedCategory() + "')) ");
			} else {
				formationstring.append("AND (T1.FEED_CATEGORY IN (#PROMPT_VALUE_2#) OR 'ALL' IN (#PROMPT_VALUE_2#)) ");
			}
		}
		if (ValidationUtil.isValid(dObj.getPromptValue3()) || ValidationUtil.isValid(dObj.getFeedId())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue3())) {
				dObj.setPromptValue3(dObj.getFeedId());
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				formationstring.append(
						"AND (T1.FEED_ID IN ('" + dObj.getFeedId() + "') OR 'ALL' IN ('" + dObj.getFeedId() + "')) ");
			} else {
				formationstring.append("AND (T1.FEED_ID IN (#PROMPT_VALUE_3#) OR 'ALL' IN (#PROMPT_VALUE_3#)) ");
			}
			if (ValidationUtil.isValid(dObj.getSessionId())) {
				formationstring.append("AND (T1.SESSION_ID = '" + dObj.getSessionId() + "' ) ");
			}
		}
		if (ValidationUtil.isValid(dObj.getPromptValue4()) || ValidationUtil.isValid(dObj.getCurrentProcess())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue4())) {
				dObj.setPromptValue4(dObj.getCurrentProcess());
			}
			if (ValidationUtil.isValid(dObj.getCurrentProcess())) {
				formationstring.append("AND (T1.CURRENT_PROCESS IN ('" + dObj.getCurrentProcess() + "') OR 'ALL' IN ('"
						+ dObj.getCurrentProcess() + "')) ");
			} else if (ValidationUtil.isValid(dObj.getPromptValue4())) {
				formationstring
						.append("AND (T1.CURRENT_PROCESS IN (#PROMPT_VALUE_4#) OR 'ALL' IN (#PROMPT_VALUE_4#)) ");
			}
		}
		if (ValidationUtil.isValid(dObj.getPromptValue5()) || ValidationUtil.isValid(dObj.getConnectorId())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue5())) {
				dObj.setPromptValue5(dObj.getConnectorId());
			}
			if (ValidationUtil.isValid(dObj.getConnectorId())) {
				formationstring.append("AND Exists (select 1 from ETL_CONNECTOR_PRS T6 "
						+ " where T1.COUNTRY = T6.COUNTRY  AND T1.LE_BOOK = T6.LE_BOOK "
						+ " AND T1.FEED_ID = T6.FEED_ID  AND T1.SESSION_ID = T6.SESSION_ID "
						+ " AND (T6.CONNECTOR_ID IN ('#PROMPT_VALUE_5#') OR 'ALL' IN ('#PROMPT_VALUE_5#'))) ");
			} else if (ValidationUtil.isValid(dObj.getPromptValue5())) {
				formationstring.append("AND Exists (select 1 from ETL_CONNECTOR_PRS T6 "
						+ " where T1.COUNTRY = T6.COUNTRY  AND T1.LE_BOOK = T6.LE_BOOK "
						+ " AND T1.FEED_ID = T6.FEED_ID  AND T1.SESSION_ID = T6.SESSION_ID "
						+ " AND (T6.CONNECTOR_ID IN (#PROMPT_VALUE_5#) OR 'ALL' IN (#PROMPT_VALUE_5#))) ");
			}
		}
		/*if (ValidationUtil.isValid(dObj.getPromptValue6()) || ValidationUtil.isValid(dObj.getProcessDate())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue6())) {
				dObj.setPromptValue6(dObj.getProcessDate());
			}
			if (ValidationUtil.isValid(dObj.getProcessDate())) {
				formationstring
						.append("AND (T1.PROCeSS_DATE IN ('#PROMPT_VALUE_6#') OR 'ALL' IN ('#PROMPT_VALUE_6#')) ");
			} else if (ValidationUtil.isValid(dObj.getPromptValue6())) {
				formationstring.append("AND (T1.PROCeSS_DATE IN (#PROMPT_VALUE_6#) OR 'ALL' IN (#PROMPT_VALUE_6#)) ");
			}
		}*/

		if (ValidationUtil.isValid(dObj.getPromptValue6()) || ValidationUtil.isValid(dObj.getProcessDate())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue6())) {
				dObj.setPromptValue6(dObj.getProcessDate());
			}
			if (ValidationUtil.isValid(dObj.getPromptValue6())) {
				formationstring.append(" AND  T1.PROCeSS_DATE =  "
						+ AbstractCommonDao.getDbFunction(Constants.CAST_AS_DATE, "#PROMPT_VALUE_6#") );
			}
		}

		/*
		 * if (ValidationUtil.isValid(dObj.getStartTime())) { formationstring.
		 * append("AND (T1.START_TIME IN (#PROMPT_VALUE_7#) OR 'ALL' IN (#PROMPT_VALUE_7#)) "
		 * ); } else if (ValidationUtil.isValid(dObj.getPromptValue7())) {
		 * formationstring.
		 * append("AND (T1.START_TIME IN (#PROMPT_VALUE_7#) OR 'ALL' IN (#PROMPT_VALUE_7#)) "
		 * ); }
		 * 
		 * if (ValidationUtil.isValid(dObj.getPromptValue8())) { formationstring.
		 * append("AND (T1.END_TIME IN (#PROMPT_VALUE_8#) OR 'ALL' IN (#PROMPT_VALUE_8#)) "
		 * ); } else if (ValidationUtil.isValid(dObj.getPromptValue8())) {
		 * formationstring.
		 * append("AND (T1.END_TIME IN (#PROMPT_VALUE_8#) OR 'ALL' IN (#PROMPT_VALUE_8#)) "
		 * ); }
		 */

		if (ValidationUtil.isValid(dObj.getPromptValue7()) || ValidationUtil.isValid(dObj.getStartTime())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue7())) {
				dObj.setPromptValue7(dObj.getStartTime());
			}
			formationstring.append(" and "
					+ AbstractCommonDao.getDbFunction(Constants.TO_DATE_NO_TIMESTAMP,
							" " + AbstractCommonDao.getDbFunction(Constants.FN_UNIX_TIME_TO_DATE, null)
									+ " (T1.START_TIME, '#TIME_ZONE#')")
					+ " >= " + AbstractCommonDao.getDbFunction(Constants.TO_DATE_NO_TIMESTAMP, "(#PROMPT_VALUE_7#)")
					+ " ");
		}

		if (ValidationUtil.isValid(dObj.getPromptValue8()) || ValidationUtil.isValid(dObj.getEndTime())) {
			if (!ValidationUtil.isValid(dObj.getPromptValue8())) {
				dObj.setPromptValue8(dObj.getEndTime());
			}
			formationstring.append(" and "
					+ AbstractCommonDao.getDbFunction(Constants.TO_DATE_NO_TIMESTAMP,
							" " + AbstractCommonDao.getDbFunction(Constants.FN_UNIX_TIME_TO_DATE, null)
									+ " (T1.END_TIME, '#TIME_ZONE#')")
					+ " <= " + AbstractCommonDao.getDbFunction(Constants.TO_DATE_NO_TIMESTAMP, "(#PROMPT_VALUE_8#)")
					+ " ");
		}

		if (ValidationUtil.isValid(dObj.getPromptValue0()) || (ValidationUtil.isValid(dObj.getCompletionStatus()))) {
			if (!ValidationUtil.isValid(dObj.getPromptValue0())) {
				dObj.setPromptValue0(dObj.getCompletionStatus());
			}

			/*if ((dObj.getPromptValue0()).contains(Constants.COMMA)) {
				String promptVal = Constants.S_QUOTE + dObj.getPromptValue0().replaceAll(Constants.COMMA, "','")
						+ Constants.S_QUOTE;
				dObj.setPromptValue0(promptVal);
				formationstring.append("AND (T1.COMPLETION_STATUS IN (#PROMPT_VALUE_0#)) ");
			} else {
				formationstring.append("AND (T1.COMPLETION_STATUS = ('#PROMPT_VALUE_0#') ) ");
			}*/
			
			if(dObj.getPromptValue0().contains(Constants.COMMA)) {
				String promptVal =  Constants.S_QUOTE+ dObj.getPromptValue0().replaceAll(Constants.COMMA, "','") +  Constants.S_QUOTE;
				dObj.setPromptValue0(promptVal);
				formationstring.append(" AND CASE " + 
						"		WHEN ('P' IN (#PROMPT_VALUE_0#) OR  'R' IN (#PROMPT_VALUE_0#) ) THEN " + //--Yet to Start
						" CASE " + 
						"                WHEN (T1.COMPLETION_STATUS = 'P' OR T1.COMPLETION_STATUS = 'R') THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'H' IN (#PROMPT_VALUE_0#) THEN " + // --Hold
						"            CASE " + 
						"                WHEN CURRENT_PROCESS LIKE '%HOLD%' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'I' IN (#PROMPT_VALUE_0#) THEN " + // --In-Progress
						"            CASE " + 
						"                WHEN CURRENT_PROCESS NOT LIKE '%HOLD%' AND T1.COMPLETION_STATUS = 'I' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"    WHEN 'S' IN (#PROMPT_VALUE_0#) THEN " + // --Success
						"            CASE " + 
						"                WHEN T1.COMPLETION_STATUS = 'S' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'E' IN (#PROMPT_VALUE_0#) THEN " + // --Error
						"            CASE " + 
						"                WHEN CURRENT_PROCESS != 'TERMINATED' AND T1.COMPLETION_STATUS = 'E' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'T' IN (#PROMPT_VALUE_0#) THEN " + // --Terminated 
						"            CASE " + 
						"                WHEN CURRENT_PROCESS = 'TERMINATED' AND T1.COMPLETION_STATUS = 'E' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"    END = 1" ); 
			} else {
				formationstring.append(" AND CASE " + 
						"		WHEN ('P' = '#PROMPT_VALUE_0#' OR  'R' = '#PROMPT_VALUE_0#') THEN " + //--Yet to Start
						" CASE " + 
						"                WHEN (T1.COMPLETION_STATUS = 'P' OR T1.COMPLETION_STATUS = 'R') THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'H' = '#PROMPT_VALUE_0#' THEN " + // --Hold
						"            CASE " + 
						"                WHEN CURRENT_PROCESS LIKE '%HOLD%' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'I' = '#PROMPT_VALUE_0#' THEN " + // --In-Progress
						"            CASE " + 
						"                WHEN CURRENT_PROCESS NOT LIKE '%HOLD%' AND T1.COMPLETION_STATUS = 'I' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"    WHEN 'S' = '#PROMPT_VALUE_0#' THEN " + // --Success
						"            CASE " + 
						"                WHEN T1.COMPLETION_STATUS = 'S' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'E' = '#PROMPT_VALUE_0#' THEN " + // --Error
						"            CASE " + 
						"                WHEN CURRENT_PROCESS != 'TERMINATED' AND T1.COMPLETION_STATUS = 'E' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"		WHEN 'T' = '#PROMPT_VALUE_0#' THEN " + // --Terminated 
						"            CASE " + 
						"                WHEN CURRENT_PROCESS = 'TERMINATED' AND T1.COMPLETION_STATUS = 'E' THEN 1" + 
						"                ELSE 0" + 
						"            END" + 
						"    END = 1" ); 
			}
		}

		if (ValidationUtil.isValid(dObj.getSessionId())) {
			formationstring.append("AND (T1.SESSION_ID = '" + dObj.getSessionId() + "' ) ");
		}
		String queryWithFilter = replacePromptVariables(formationstring.toString(), dObj);
		return queryWithFilter;
	}

	public String lstTimeZone() throws DataAccessException {
		String sql = "select TIME_ZONE FROM vision_business_day WHERE APPLICATION_ID='ETL'";
		/*RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setTimeZone(rs.getString("TIME_ZONE"));
				return commonVb;
			}

		};
		List<CommonVb> date = getJdbcTemplate().query(sql, mapper);
		return date;*/
		return getJdbcTemplate().queryForObject(sql, String.class);
	}

	@Override
	protected String getAuditString(OperationalConsoleVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getFeedId()))
			strAudit.append("FEED_ID" + auditDelimiterColVal + vObject.getFeedId().trim());
		else
			strAudit.append("FEED_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getProcessDate()))
			strAudit.append("PROCESS_DATE" + auditDelimiterColVal + vObject.getProcessDate().trim());
		else
			strAudit.append("PROCESS_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSessionId()))
			strAudit.append("SESSION_ID" + auditDelimiterColVal + vObject.getSessionId().trim());
		else
			strAudit.append("SESSION_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getStartTime()))
			strAudit.append("START_TIME" + auditDelimiterColVal + vObject.getStartTime().trim());
		else
			strAudit.append("START_TIME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEndTime()))
			strAudit.append("END_TIME" + auditDelimiterColVal + vObject.getEndTime().trim());
		else
			strAudit.append("END_TIME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getTriggeredBy()))
			strAudit.append("TRIGGERED_BY" + auditDelimiterColVal + vObject.getTriggeredBy());
		else
			strAudit.append("TRIGGERED_BY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getTerminatedBy()))
			strAudit.append("TERMINATED_BY" + auditDelimiterColVal + vObject.getTerminatedBy());
		else
			strAudit.append("TERMINATED_BY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getReinitiatedBy()))
			strAudit.append("REINITIATED_BY" + auditDelimiterColVal + vObject.getReinitiatedBy());
		else
			strAudit.append("REINITIATED_BY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("CURRENT_PROCESS_AT" + auditDelimiterColVal + vObject.getCurrentProcessAT());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getCurrentProcess()))
			strAudit.append("CURRENT_PROCESS" + auditDelimiterColVal + vObject.getCurrentProcess().trim());
		else
			strAudit.append("CURRENT_PROCESS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getIterationCount()))
			strAudit.append("ITERATION_COUNT" + auditDelimiterColVal + vObject.getIterationCount());
		else
			strAudit.append("ITERATION_COUNT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COMPLETION_STATUS_AT" + auditDelimiterColVal + vObject.getCompletionStatusAT());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getCompletionStatus()))
			strAudit.append("COMPLETION_STATUS" + auditDelimiterColVal + vObject.getCompletionStatus().trim());
		else
			strAudit.append("COMPLETION_STATUS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ALERT_STATUS_AT" + auditDelimiterColVal + vObject.getAlertStatusAT());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getAlertStatus()))
			strAudit.append("ALERT_STATUS" + auditDelimiterColVal + vObject.getAlertStatus().trim());
		else
			strAudit.append("ALERT_STATUS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_PROCESS_STATUS_NT" + auditDelimiterColVal + vObject.getEtlProcessStatusNT());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEtlProcessStatus()))
			strAudit.append("ETL_PROCESS_STATUS" + auditDelimiterColVal + vObject.getEtlProcessStatus().trim());
		else
			strAudit.append("ETL_PROCESS_STATUS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getRecordIndicatorNt()))
			strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + vObject.getRecordIndicatorNt());
		else
			strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getRecordIndicator()))
			strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + vObject.getRecordIndicator());
		else
			strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getMaker()))
			strAudit.append("MAKER" + auditDelimiterColVal + vObject.getMaker());
		else
			strAudit.append("MAKER" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getVerifier()))
			strAudit.append("VERIFIER" + auditDelimiterColVal + vObject.getVerifier());
		else
			strAudit.append("VERIFIER" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getInternalStatus()))
			strAudit.append("INTERNAL_STATUS" + auditDelimiterColVal + vObject.getInternalStatus());
		else
			strAudit.append("INTERNAL_STATUS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateLastModified()))
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + vObject.getDateLastModified());
		else
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateCreation()))
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + vObject.getDateCreation());
		else
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("EVENT_TYPE_AT" + auditDelimiterColVal + vObject.getEventTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEventType()))
			strAudit.append("EVENT_TYPE" + auditDelimiterColVal + vObject.getEventType().trim());
		else
			strAudit.append("EVENT_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDependencyFeedId()))
			strAudit.append("DEPENDENCY_FEED_ID" + auditDelimiterColVal + vObject.getDependencyFeedId().trim());
		else
			strAudit.append("DEPENDENCY_FEED_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getNextScheduleDate()))
			strAudit.append("NEXT_SCHEDULE_DATE" + auditDelimiterColVal + vObject.getNextScheduleDate().trim());
		else
			strAudit.append("NEXT_SCHEDULE_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFeedCategory()))
			strAudit.append("FEED_CATEGORY" + auditDelimiterColVal + vObject.getFeedCategory().trim());
		else
			strAudit.append("FEED_CATEGORY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("SCHEDULE_TYPE_AT" + auditDelimiterColVal + vObject.getScheduleTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getScheduleType()))
			strAudit.append("SCHEDULE_TYPE" + auditDelimiterColVal + vObject.getScheduleType().trim());
		else
			strAudit.append("SCHEDULE_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDebugFlag()))
			strAudit.append("DEBUG_FLAG" + auditDelimiterColVal + vObject.getDebugFlag().trim());
		else
			strAudit.append("DEBUG_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		return strAudit.toString();
	}

	public int insertETL_CONNECTOR(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, CONNECTOR_TYPE_AT, CONNECTOR_TYPE, CONNECTOR_ID, '" + sessionId
					+ "' as SESSION_ID, '" + vObject.getFeedId() + "' as FEED_ID, '" + vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, "
					+ " CONNECTION_NAME, CONNECTOR_SCRIPTS, ENDPOINT_TYPE_AT, ENDPOINT_TYPE, CONNECTION_STAUTS_NT, CONNECTION_STAUTS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION,DB_LINK_FLAG FROM ETL_CONNECTOR ";

			argtemplateSelPrep = " WHERE CONNECTOR_ID IN (" + vObject.getEtlSourceConnectorId() + ", '"
					+ vObject.getDestinationConnectorId() + "')";

			instemplateSelPrep = "INSERT INTO ETL_CONNECTOR_PRS (COUNTRY, LE_BOOK, CONNECTOR_TYPE_AT, CONNECTOR_TYPE, CONNECTOR_ID, SESSION_ID, FEED_ID, FEED_CATEGORY, "
					+ " CONNECTION_NAME, CONNECTOR_SCRIPTS, ENDPOINT_TYPE_AT, ENDPOINT_TYPE, CONNECTION_STAUTS_NT, CONNECTION_STAUTS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION,DB_LINK_FLAG)  (" + templateSelPrep + argtemplateSelPrep
					+ ")";

			getJdbcTemplate().update(instemplateSelPrep);
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_MQ_TABLE(OperationalConsoleVb vObject, String sessionId) {

		try {
			templateSelPrep = "SELECT CONNECTOR_ID, QUERY_ID, '" + sessionId + "' as SESSION_ID, '" + vObject.getFeedId()
					+ "' as FEED_ID, '" + vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, QUERY_DESCRIPTION, QUERY, VALIDATED_FLAG,"
					+ " QUERY_STAUTS_NT, QUERY_STAUTS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION, "
					+ " QUERY_CATEGORY_NT, QUERY_CATEGORY FROM ETL_MQ_TABLE ";

			argtemplateSelPrep = " WHERE CONNECTOR_ID IN (" + vObject.getEtlSourceConnectorId() + ", '"
					+ vObject.getDestinationConnectorId() + "')";

			instemplateSelPrep = "INSERT INTO ETL_MQ_TABLE_PRS (CONNECTOR_ID, QUERY_ID, SESSION_ID, FEED_ID, FEED_CATEGORY, QUERY_DESCRIPTION, QUERY, VALIDATED_FLAG,"
					+ " QUERY_STAUTS_NT, QUERY_STAUTS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION,  QUERY_CATEGORY_NT, QUERY_CATEGORY) ("
					+ templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_MQ_Columns(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT CONNECTOR_ID, QUERY_ID, COLUMN_ID, '" + sessionId + "' as SESSION_ID, '"
					+ vObject.getFeedId() + "' as FEED_ID, '" + vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, COLUMN_NAME, COLUMN_SORT_ORDER, "
					+ " COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH,"
					+ " DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT,"
					+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, MQ_COLUMN_STATUS_NT, MQ_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_MQ_Columns ";

			argtemplateSelPrep = " WHERE CONNECTOR_ID IN (" + vObject.getEtlSourceConnectorId() + ", '"
					+ vObject.getDestinationConnectorId() + "')";

			instemplateSelPrep = "INSERT INTO ETL_MQ_Columns_PRS (CONNECTOR_ID, QUERY_ID, COLUMN_ID, SESSION_ID, FEED_ID, FEED_CATEGORY, COLUMN_NAME, COLUMN_SORT_ORDER, "
					+ " COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, "
					+ " DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT,"
					+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, MQ_COLUMN_STATUS_NT, MQ_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FILE_UPLOAD_AREA(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT CONNECTOR_ID, FILE_ID, '" + sessionId + "' as SESSION_ID, '" + vObject.getFeedId()
					+ "' as FEED_ID, '" + vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, FILE_NAME, FILE_FORMAT_AT, FILE_FORMAT, "
					+ " FILE_TYPE_AT, FILE_TYPE, FILE_CONTEXT, FTP_DETAIL_SCRIPT, FILE_STATUS_NT,FILE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FILE_UPLOAD_AREA";

			argtemplateSelPrep = " WHERE CONNECTOR_ID IN (" + vObject.getEtlSourceConnectorId() + ", '"
					+ vObject.getDestinationConnectorId() + "')";

			instemplateSelPrep = "INSERT INTO ETL_FILE_UPLOAD_AREA_PRS (CONNECTOR_ID, FILE_ID, SESSION_ID, FEED_ID, FEED_CATEGORY, FILE_NAME, FILE_FORMAT_AT, FILE_FORMAT, "
					+ " FILE_TYPE_AT, FILE_TYPE, FILE_CONTEXT, FTP_DETAIL_SCRIPT, FILE_STATUS_NT,FILE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FILE_TABLE(OperationalConsoleVb vObject, String sessionId) {

		try {
			templateSelPrep = "SELECT CONNECTOR_ID, VIEW_NAME, '" + sessionId + "' as SESSION_ID, '" + vObject.getFeedId()
					+ "' as FEED_ID, '" + vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, FILE_ID, SHEET_NAME, FILE_STATUS_NT, FILE_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FILE_TABLE";

			argtemplateSelPrep = " WHERE CONNECTOR_ID IN (" + vObject.getEtlSourceConnectorId() + ", '"
					+ vObject.getDestinationConnectorId() + "')";

			instemplateSelPrep = "INSERT INTO ETL_FILE_TABLE_PRS (CONNECTOR_ID, VIEW_NAME, SESSION_ID, FEED_ID, FEED_CATEGORY, FILE_ID, SHEET_NAME, FILE_STATUS_NT, FILE_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FILE_COLUMNS(OperationalConsoleVb vObject, String sessionId) {

		try {
			templateSelPrep = "SELECT CONNECTOR_ID, VIEW_NAME, '" + sessionId + "' as SESSION_ID, '" + vObject.getFeedId()
					+ "' as FEED_ID, '" + vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER,"
					+ " COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT,"
					+ " NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,"
					+ " FILE_COLUMN_STATUS_NT,FILE_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FILE_COLUMNS ";

			argtemplateSelPrep = " WHERE CONNECTOR_ID IN (" + vObject.getEtlSourceConnectorId() + ", '"
					+ vObject.getDestinationConnectorId() + "')";

			instemplateSelPrep = "INSERT INTO ETL_FILE_COLUMNS_PRS (CONNECTOR_ID, VIEW_NAME, SESSION_ID, FEED_ID, FEED_CATEGORY, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER,"
					+ " COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT,"
					+ " NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,"
					+ " FILE_COLUMN_STATUS_NT,FILE_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_CONNECTOR_ACCESS() {
		pTableName = "ETL_CONNECTOR_ACCESS";
		try {
			/*
			 * templateSelPrep = "SELECT " +
			 * " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
			 * + " "+getDbFunction(Constants.SYSDATE,
			 * null)+"  DATE_LAST_MODIFIED, " +
			 * " "+getDbFunction(Constants.SYSDATE,
			 * null)+" DATE_CREATION FROM ETL_CONNECTOR_ACCESS ";
			 * 
			 * instemplateSelPrep = "INSERT INTO ETL_CONNECTOR_ACCESS (" +
			 * " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
			 * + " DATE_LAST_MODIFIED,  DATE_CREATION  ("+templateSelPrep+")";
			 */
			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_CATEGORY(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, CATEGORY_ID, ALERT_FLAG, CHANNEL_TYPE_SMS, CHANNEL_TYPE_EMAIL, '"
					+ sessionId + "' as SESSION_ID, '" + vObject.getFeedId() + "' as FEED_ID, '"
					+ vObject.getFeedCategory()
					+ "' as FEED_CATEGORY, CATEGORY_DESCRIPTION, CATEGORY_STATUS_NT, CATEGORY_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_CATEGORY ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND CATEGORY_ID ='" + vObject.getFeedCategory() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_CATEGORY_PRS (COUNTRY, LE_BOOK, CATEGORY_ID, ALERT_FLAG, CHANNEL_TYPE_SMS, "
					+ " CHANNEL_TYPE_EMAIL, SESSION_ID, FEED_ID, FEED_CATEGORY, CATEGORY_DESCRIPTION, CATEGORY_STATUS_NT, CATEGORY_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	/*public int insertETL_CATEGORY_ALERT_CONFIG(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, CATEGORY_ID, EVENT_TYPE, MATRIX_ID, " + sessionId
					+ " as SESSION_ID, '" + vObject.getFeedId() + "' as FEED_ID,  "
					+ "CATEGORY_ALERT_STATUS_NT, CATEGORY_ALERT_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION FROM ETL_CATEGORY_ALERT_CONFIG ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND CATEGORY_ID ='" + vObject.getFeedCategory() + "'";

			instemplateSelPrep = "INSERT INTO ETL_CATEGORY_ALERT_CONFIG_PRS (COUNTRY, LE_BOOK, CATEGORY_ID,  EVENT_TYPE, MATRIX_ID, "
					+ "SESSION_ID, FEED_ID,  CATEGORY_ALERT_STATUS_NT, CATEGORY_ALERT_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			// e.printStackTrace();
			return (Constants.ERRONEOUS_OPERATION);
		}
	}*/

	public int insertETL_FEED_CATEGORY_ACCESS(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, CATEGORY_ID, '" + sessionId
					+ "' as SESSION_ID,  USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT,"
					+ " USER_PROFILE, VISION_ID, WRITE_FLAG, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION FROM ETL_FEED_CATEGORY_ACCESS ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND CATEGORY_ID ='" + vObject.getFeedCategory() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_CATEGORY_ACCESS_PRS (COUNTRY, LE_BOOK, CATEGORY_ID, SESSION_ID, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT,"
					+ " USER_PROFILE, VISION_ID, WRITE_FLAG, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_MAIN(OperationalConsoleVb vObject, String sessionId) {
		pTableName = "ETL_FEED_MAIN";
		try {
			if (ValidationUtil.isValid(vObject.getLastExtractionDate())
					&& vObject.getLastExtractionDate().contains("-")) {
				if (vObject.getLastExtractionDate().length() == 11 || vObject.getLastExtractionDate().length() < 11) {
					vObject.setLastExtractionDate(vObject.getLastExtractionDate() + " 00:00:00");
				}
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
				SimpleDateFormat sdf1 = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
				vObject.setTimeZone(etlFeedScheduleConfigDao.getTimeZone(vObject.getCountry(), vObject.getLeBook()));
				sdf.setTimeZone(TimeZone.getTimeZone(vObject.getTimeZone()));
				Date date1 = null;
				Date date2 = null;
				try {
					date1 = sdf.parse(vObject.getLastExtractionDate());
					date2 = sdf1.parse(sdf1.format(date1));
				} catch (ParseException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
				long millisecondsSinceUnixEpoch = date2.getTime();
				vObject.setLastExtractionDate(String.valueOf(millisecondsSinceUnixEpoch));
			}
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, FEED_NAME, FEED_DESCRIPTION,  EFFECTIVE_DATE,   '"
					+ vObject.getLastExtractionDate() + "' as LAST_EXTRACTION_DATE, FEED_TYPE_AT, '"
					+ vObject.getFeedType() + "' as FEED_TYPE, FEED_CATEGORY, "
					+ " ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION, COMPLETION_STATUS_AT, COMPLETION_STATUS," + vObject.getBatchSize()
					+ " as BATCH_SIZE, DISTINCT_FLAG FROM ETL_FEED_MAIN ";
			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND FEED_ID ='" + vObject.getFeedId() + "'";
			instemplateSelPrep = "INSERT INTO ETL_FEED_MAIN_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, FEED_NAME, FEED_DESCRIPTION,"
					+ " EFFECTIVE_DATE, LAST_EXTRACTION_DATE, FEED_TYPE_AT, FEED_TYPE, FEED_CATEGORY, "
					+ " ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION, COMPLETION_STATUS_AT,COMPLETION_STATUS,BATCH_SIZE,DISTINCT_FLAG)  ("
					+ templateSelPrep + argtemplateSelPrep + ")";
			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_MAIN_ACCESS(OperationalConsoleVb vObject, String sessionId) {
		pTableName = "ETL_FEED_MAIN_ACCESS";
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID,  USER_GROUP_AT, USER_GROUP, "
					+ " USER_PROFILE_AT, USER_PROFILE, VISION_ID, WRITE_FLAG, "
					+ " FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_MAIN_ACCESS ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_MAIN_ACCESS_PRS (COUNTRY, LE_BOOK, FEED_ID,SESSION_ID , USER_GROUP_AT, USER_GROUP,"
					+ " USER_PROFILE_AT, USER_PROFILE, VISION_ID, WRITE_FLAG, "
					+ " FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_SOURCE(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId + "' as SESSION_ID,  SOURCE_CONNECTOR_ID,"
					+ " FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_SOURCE ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_SOURCE_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, SOURCE_CONNECTOR_ID,"
					+ " FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_TABLES(OperationalConsoleVb vObject, String sessionId) {
		pTableName = "ETL_FEED_TABLES";
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, TABLE_ID, SOURCE_CONNECTOR_ID,"
					+ " TABLE_NAME, TABLE_ALIAS_NAME, TABLE_SORT_ORDER, TABLE_TYPE_AT, TABLE_TYPE, "
					+ " QUERY_ID, CUSTOM_PARTITION_COLUMN_FLAG, PARTITION_COLUMN_NAME, BASE_TABLE_FLAG,"
					+ " FEED_TABLE_STATUS_NT, FEED_TABLE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_TABLES ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_TABLES_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, TABLE_ID, SOURCE_CONNECTOR_ID,"
					+ " TABLE_NAME, TABLE_ALIAS_NAME, TABLE_SORT_ORDER, TABLE_TYPE_AT, TABLE_TYPE, "
					+ " QUERY_ID, CUSTOM_PARTITION_COLUMN_FLAG, PARTITION_COLUMN_NAME, BASE_TABLE_FLAG,"
					+ " FEED_TABLE_STATUS_NT, FEED_TABLE_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION) (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_COLUMNS(OperationalConsoleVb vObject, String sessionId) {
		pTableName = "ETL_FEED_COLUMNS";
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, SOURCE_CONNECTOR_ID, TABLE_ID, "
					+ " COLUMN_ID, COLUMN_NAME, COLUMN_ALIAS_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT,"
					+ " COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT,DATE_FORMAT, PRIMARY_KEY_FLAG, "
					+ " PARTITION_COLUMN_FLAG, FILTER_CONTEXT, "
					+ " COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, EXPERSSION_TEXT,"
					+ " FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_COLUMNS ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_COLUMNS_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, SOURCE_CONNECTOR_ID, TABLE_ID, "
					+ " COLUMN_ID, COLUMN_NAME, COLUMN_ALIAS_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT,"
					+ " COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT,DATE_FORMAT, PRIMARY_KEY_FLAG, "
					+ " PARTITION_COLUMN_FLAG, FILTER_CONTEXT, "
					+ " COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, EXPERSSION_TEXT,"
					+ " FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_RELATION(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, FROM_TABLE_ID, TO_TABLE_ID, "
					+ " JOIN_TYPE_NT, JOIN_TYPE, FILTER_CONTEXT, RELATION_CONTEXT,"
					+ " FEED_RELATION_STATUS_NT, FEED_RELATION_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_RELATION ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_RELATION_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, FROM_TABLE_ID, TO_TABLE_ID, "
					+ " JOIN_TYPE_NT, JOIN_TYPE, FILTER_CONTEXT, RELATION_CONTEXT,"
					+ " FEED_RELATION_STATUS_NT, FEED_RELATION_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_EXTRACTION_SUMMARY_FIELDS(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId + "' as SESSION_ID,TABLE_ID, COL_ID, "
					+ " USER_ID, COL_NAME, ALIAS, CONDITION_OPERATOR,  VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, "
					+ " SORT_ORDER, AGG_FUNCTION,  GROUP_BY, COL_DISPLAY_TYPE, "
					+ " ETL_FIELDS_STATUS_NT, ETL_FIELDS_STATUS,  EXPERSSION_TEXT, JOIN_CONDITION, "
					+ " DYNAMIC_START_FLAG, DYNAMIC_END_FLAG, DYNAMIC_START_DATE, DYNAMIC_END_DATE, "
					+ " DYNAMIC_START_OPERATOR, DYNAMIC_END_OPERATOR, DYNAMIC_DATE_FORMAT, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED, "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION, SAMPLE_DATA_RULE, SAMPLE_DATA_CUSTOM_RULE ,"
					+ " SUMMARY_CRITERIA,SUMMARY_VALUE_1, SUMMARY_VALUE_2, "
					+ " SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE,"
					+ " SUMMARY_DYNAMIC_END_DATE, SUMMARY_DYNAMIC_START_OPERATOR, SUMMARY_DYNAMIC_END_OPERATOR, LINKED_COLUMN_ID "
					+ " FROM ETL_EXTRACTION_SUMMARY_FIELDS ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_EXTRACTION_SUMMARY_FIELDS_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, TABLE_ID, COL_ID, "
					+ " USER_ID, COL_NAME, ALIAS, CONDITION_OPERATOR,  VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, "
					+ " SORT_ORDER, AGG_FUNCTION,  GROUP_BY, COL_DISPLAY_TYPE, "
					+ " ETL_FIELDS_STATUS_NT, ETL_FIELDS_STATUS,  EXPERSSION_TEXT, JOIN_CONDITION, "
					+ " DYNAMIC_START_FLAG, DYNAMIC_END_FLAG, DYNAMIC_START_DATE, DYNAMIC_END_DATE, "
					+ " DYNAMIC_START_OPERATOR, DYNAMIC_END_OPERATOR, DYNAMIC_DATE_FORMAT, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED, DATE_CREATION, SAMPLE_DATA_RULE, SAMPLE_DATA_CUSTOM_RULE ,"
					+ " SUMMARY_CRITERIA,SUMMARY_VALUE_1, SUMMARY_VALUE_2, "
					+ " SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE,"
					+ " SUMMARY_DYNAMIC_END_DATE, SUMMARY_DYNAMIC_START_OPERATOR, SUMMARY_DYNAMIC_END_OPERATOR, LINKED_COLUMN_ID ) ("
					+ templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			// e.printStackTrace();
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_TRANSFORMATION(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId + "' as SESSION_ID, "
					+ " NODE_ID, NODE_NAME, NODE_DESCRIPTION, TRANSFORMATION_ID, X_AXIS, Y_AXIS, TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_TRANSFORMATION ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_TRANSFORMATION_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID,"
					+ " NODE_ID, NODE_NAME, NODE_DESCRIPTION, TRANSFORMATION_ID, X_AXIS, Y_AXIS, TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_TRAN_PARENT(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId + "' as SESSION_ID, "
					+ " NODE_ID, PARENT_NODE_ID,  PARENT_STATUS_NT, PARENT_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_TRAN_PARENT ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_TRAN_PARENT_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID,"
					+ " NODE_ID, PARENT_NODE_ID, PARENT_STATUS_NT, PARENT_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_TRAN_CHILD(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId + "' as SESSION_ID, "
					+ " NODE_ID, CHILD_NODE_ID,  CHILD_STATUS_NT, CHILD_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_TRAN_CHILD ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_TRAN_CHILD_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID,"
					+ " NODE_ID, CHILD_NODE_ID, CHILD_STATUS_NT, CHILD_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_CATEGORY_DEPENDENCIES(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, '" + sessionId + "' as SESSION_ID, '" + vObject.getFeedId()
					+ "' as FEED_ID, CATEGORY_ID, DEPENDENT_CATEGORY, CATEGORY_DEP_STATUS_NT, CATEGORY_DEP_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION "
					+ " FROM ETL_FEED_CATEGORY_DEPENDENCIES ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND CATEGORY_ID ='" + vObject.getFeedCategory() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_CATEGORY_DEP_PRS (COUNTRY, LE_BOOK, SESSION_ID, FEED_ID, CATEGORY_ID, DEPENDENT_CATEGORY, CATEGORY_DEP_STATUS_NT, CATEGORY_DEP_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)  ("
					+ templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_DEPENDENCIES(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = " SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' SESSION_ID, DEPENDENT_FEED_ID, FEED_DEP_STATUS_NT, FEED_DEP_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION "
					+ " FROM ETL_FEED_DEPENDENCIES ";

			argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
					+ "' AND FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_DEPENDENCIES_PRS ( COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, DEPENDENT_FEED_ID, FEED_DEP_STATUS_NT, FEED_DEP_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION )  ("
					+ templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_TRANSFORMED_COLUMNS(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID,  TRAN_COLUMN_ID, TRAN_COLUMN_NAME, "
					+ " TRAN_COLUMN_DESCRIPTION, TRAN_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH,"
					+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,  "
					+ " FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_TRANSFORMED_COLUMNS ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_TRANSFORMED_COLUMNS_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, TRAN_COLUMN_ID, TRAN_COLUMN_NAME,"
					+ " TRAN_COLUMN_DESCRIPTION, TRAN_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH,"
					+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, "
					+ " FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, "
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_DESTINATION(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, DESTINATION_CONNECTOR_ID,"
					+ " DESTINATION_CONTEXT, FEED_TRANSFORM_STATUS_NT, FEED_TRANSFORM_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_DESTINATION ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_DESTINATION_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, DESTINATION_CONNECTOR_ID,"
					+ " DESTINATION_CONTEXT, FEED_TRANSFORM_STATUS_NT, FEED_TRANSFORM_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_TARGET_COLUMNS(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, TARGET_COLUMN_ID, TARGET_COLUMN_NAME,"
					+ " TARGET_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT,DATE_FORMAT, "
					+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_TARGET_COLUMNS ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_TARGET_COLUMNS_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, TARGET_COLUMN_ID, TARGET_COLUMN_NAME,"
					+ " TARGET_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT,"
					+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_LOAD_MAPPING(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID,"
					+ " FEED_MAPPING_STATUS_NT, FEED_MAPPING_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION FROM ETL_FEED_LOAD_MAPPING ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_LOAD_MAPPING_PRS (COUNTRY, LE_BOOK, FEED_ID,SESSION_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID,"
					+ " FEED_MAPPING_STATUS_NT, FEED_MAPPING_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_INJECTION_CONFIG(OperationalConsoleVb vObject, String sessionId) {
		pTableName = "ETL_FEED_INJECTION_CONFIG";
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, INJECTION_TYPE_AT, INJECTION_TYPE,"
					+ " BENCH_MARK_COLUMN_ID, BENCH_MARK_RULE, FIRST_TIME_VARIENT_FLAG, FIRST_INJECTION_TYPE, "
					+ " ETL_INJECTION_STATUS_NT, ETL_INJECTION_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION FROM ETL_FEED_INJECTION_CONFIG ";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_INJECTION_CONFIG_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, INJECTION_TYPE_AT, INJECTION_TYPE,"
					+ " BENCH_MARK_COLUMN_ID, BENCH_MARK_RULE, FIRST_TIME_VARIENT_FLAG, FIRST_INJECTION_TYPE, "
					+ " ETL_INJECTION_STATUS_NT, ETL_INJECTION_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION)  (" + templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_ALERT_CONFIG(OperationalConsoleVb vObject, String sessionId) {
		try {
			String sql = "select ALERT_TYPE from ETL_FEED_SCHEDULE_CONFIG where FEED_ID = '"+vObject.getFeedId()+"' ";
			String feedAlertConfigFlag = getJdbcTemplate().queryForObject(sql, String.class);
			if ("C".equalsIgnoreCase(feedAlertConfigFlag)) {
				templateSelPrep = "SELECT COUNTRY, LE_BOOK, '" + sessionId + "' SESSION_ID, '" + vObject.getFeedId()
						+ "' FEED_ID, CATEGORY_ID, EVENT_TYPE, MATRIX_ID, "
						+ " CATEGORY_ALERT_STATUS_NT, CATEGORY_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, "
						+ " VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, EVENT_TYPE_AT FROM ETL_CATEGORY_ALERT_CONFIG ";

				argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
						+ "' AND CATEGORY_ID ='" + vObject.getFeedCategory() + "'";

				instemplateSelPrep = " Insert into ETL_CATEGORY_ALERT_CONFIG_PRS "
						+ " (COUNTRY, LE_BOOK, SESSION_ID, FEED_ID, CATEGORY_ID, EVENT_TYPE, MATRIX_ID, "
						+ " CATEGORY_ALERT_STATUS_NT, CATEGORY_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, "
						+ " VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, EVENT_TYPE_AT) ("
						+ templateSelPrep + argtemplateSelPrep + ")";
			} else {
				templateSelPrep = " SELECT COUNTRY, LE_BOOK, '" + sessionId
						+ "' SESSION_ID, FEED_ID, EVENT_TYPE_AT, EVENT_TYPE, "
						+ " MATRIX_ID, FEED_ALERT_STATUS_NT, FEED_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
						+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION FROM ETL_FEED_ALERT_CONFIG ";

				argtemplateSelPrep = " WHERE COUNTRY='" + vObject.getCountry() + "' AND LE_BOOK='" + vObject.getLeBook()
						+ "' AND FEED_ID ='" + vObject.getFeedId() + "'";

				instemplateSelPrep = " Insert into ETL_FEED_ALERT_CONFIG_PRS "
						+ " (COUNTRY, LE_BOOK, SESSION_ID, FEED_ID, EVENT_TYPE_AT, EVENT_TYPE, "
						+ " MATRIX_ID, FEED_ALERT_STATUS_NT, FEED_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
						+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) (" + templateSelPrep
						+ argtemplateSelPrep + ")";
			}
			getJdbcTemplate().update(instemplateSelPrep);
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}

	public int insertETL_FEED_SCHEDULE_CONFIG(OperationalConsoleVb vObject, String sessionId) {
		pTableName = "ETL_FEED_SCHEDULE_CONFIG";
		try {
			templateSelPrep = "SELECT COUNTRY, LE_BOOK, FEED_ID, '" + sessionId
					+ "' as SESSION_ID, SCHEDULE_DESCRIPTION, SCHEDULE_TYPE_AT,"
					+ " SCHEDULE_TYPE, PROCESS_DATE_TYPE_AT, PROCESS_DATE_TYPE, "
					+ " ETL_SCHEDULE_STATUS_NT, ETL_SCHEDULE_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
					+ getDbFunction(Constants.SYSDATE, null) + "  DATE_LAST_MODIFIED,  "
					+ getDbFunction(Constants.SYSDATE, null)
					+ " DATE_CREATION ,SCHEDULE_PROP_CONTEXT, PROCESS_TBL_STATUS_AT,"
					+ " PROCESS_TBL_STATUS, NEXT_SCHEDULE_DATE,"
					+ "NXT_DATE_AUTOMATION_TYPE, DUMMY_SCHEDULE_DATE, 	ALERT_TYPE_AT, ALERT_TYPE, CHANNEL_TYPE_SMS, CHANNEL_TYPE_EMAIL, "
					+ "ALERT_FLAG, SKIP_HOLIDAY_FLAG FROM ETL_FEED_SCHEDULE_CONFIG";

			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "'";

			instemplateSelPrep = "INSERT INTO ETL_FEED_SCHEDULE_CONFIG_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, SCHEDULE_DESCRIPTION, SCHEDULE_TYPE_AT,"
					+ " SCHEDULE_TYPE,  PROCESS_DATE_TYPE_AT, PROCESS_DATE_TYPE, "
					+ " ETL_SCHEDULE_STATUS_NT, ETL_SCHEDULE_STATUS,"
					+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION, SCHEDULE_PROP_CONTEXT, PROCESS_TBL_STATUS_AT,"
					+ " PROCESS_TBL_STATUS, NEXT_SCHEDULE_DATE,"
					+ "NXT_DATE_AUTOMATION_TYPE, DUMMY_SCHEDULE_DATE, 	ALERT_TYPE_AT, ALERT_TYPE, CHANNEL_TYPE_SMS, CHANNEL_TYPE_EMAIL,"
					+ " ALERT_FLAG, SKIP_HOLIDAY_FLAG) ("
					+ templateSelPrep + argtemplateSelPrep + ")";

			getJdbcTemplate().update(instemplateSelPrep);

			retVal = 1;
			return retVal;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return (Constants.ERRONEOUS_OPERATION);
		}
	}
	
	private int insertETL_FEED_TRAN_COLUMNS(OperationalConsoleVb vObject, String sessionId) {
		try {
			templateSelPrep = " Select COUNTRY, LE_BOOK, FEED_ID, '" + sessionId + "' as SESSION_ID, "
					+ "NODE_ID, COLUMN_ID, COLUMN_NAME, COLUMN_DESCRIPTION, "
					+ "DERIVED_COLUMN_FLAG, EXPRESSION_TYPE_AT, AGG_FUNCTION, "
					+ "GROUP_BY_FLAG, INPUT_FLAG, PARENT_NODE_ID, PARENT_COLUMN_ID,"
					+ "OUTPUT_FLAG, DATA_TYPE_AT, DATA_TYPE, COLUMN_SIZE,COLUMN_SCALE,NUMBER_FORMAT_NT,"
					+ "NUMBER_FORMAT, DATE_FORMAT_NT, DATE_FORMAT, COLUMN_STATUS_NT, "
					+ "COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED,"
					+ "DATE_CREATION, COLUMN_SORT_ORDER, SLAB_ID, ALIAS_NAME, LINKED_COLUMN, JOIN_OPERATOR, FILTER_CRITERIA,"
					+ "FILTER_VALUE_1, FILTER_VALUE_2, OUTPUT_COLUMN_NAME, EXPRESSION_TEXT, AGG_EXPRESSION_TEXT "
					+ " FROM ETL_FEED_TRAN_COLUMNS ";
			
			argtemplateSelPrep = " WHERE COUNTRY = '" + vObject.getCountry() + "' and LE_BOOK='" + vObject.getLeBook()
					+ "' and FEED_ID ='" + vObject.getFeedId() + "' ";

			instemplateSelPrep = " INSERT INTO ETL_FEED_TRAN_COLUMNS_PRS (COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, "
					+ "NODE_ID, COLUMN_ID, COLUMN_NAME, COLUMN_DESCRIPTION, "
					+ "DERIVED_COLUMN_FLAG, EXPRESSION_TYPE_AT, AGG_FUNCTION, "
					+ "GROUP_BY_FLAG, INPUT_FLAG, PARENT_NODE_ID, PARENT_COLUMN_ID,"
					+ "OUTPUT_FLAG, DATA_TYPE_AT, DATA_TYPE, COLUMN_SIZE,COLUMN_SCALE,NUMBER_FORMAT_NT,"
					+ "NUMBER_FORMAT, DATE_FORMAT_NT, DATE_FORMAT, COLUMN_STATUS_NT, "
					+ "COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED,"
					+ "DATE_CREATION, COLUMN_SORT_ORDER, SLAB_ID, ALIAS_NAME, LINKED_COLUMN, JOIN_OPERATOR, FILTER_CRITERIA,"
					+ "FILTER_VALUE_1, FILTER_VALUE_2, OUTPUT_COLUMN_NAME, EXPRESSION_TEXT, AGG_EXPRESSION_TEXT) ( "
					+ templateSelPrep + argtemplateSelPrep + " ) ";

			getJdbcTemplate().update(instemplateSelPrep);
			
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public ExceptionCode doInsertionAppr(OperationalConsoleVb vObject, String sessionId) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {

			retVal = insertETL_CONNECTOR(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			retVal = insertETL_MQ_TABLE(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_MQ_Columns(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FILE_UPLOAD_AREA(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FILE_TABLE(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FILE_COLUMNS(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			/*
			 * retVal =insertETL_CONNECTOR_ACCESS(); if (retVal ==
			 * Constants.ERRONEOUS_OPERATION){ exceptionCode = getResultObject(retVal);
			 * throw buildRuntimeCustomException(exceptionCode); }
			 */

			retVal = insertETL_FEED_CATEGORY(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			/* below method insertETL_ALERT_CONFIG is used instead of this
			 * 
			 * retVal = insertETL_CATEGORY_ALERT_CONFIG(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}*/

			/*
			 * retVal =insertETL_FEED_CATEGORY_ACCESS(vObject); if (retVal ==
			 * Constants.ERRONEOUS_OPERATION){ exceptionCode = getResultObject(retVal);
			 * throw buildRuntimeCustomException(exceptionCode); }
			 */

			retVal = insertETL_FEED_MAIN(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			/*
			 * retVal =insertETL_FEED_MAIN_ACCESS(vObject); if (retVal ==
			 * Constants.ERRONEOUS_OPERATION){ exceptionCode = getResultObject(retVal);
			 * throw buildRuntimeCustomException(exceptionCode); }
			 */

			retVal = insertETL_FEED_SOURCE(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_TABLES(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_COLUMNS(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_RELATION(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_EXTRACTION_SUMMARY_FIELDS(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_TRANSFORMATION(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_TRAN_PARENT(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_TRAN_CHILD(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			
			retVal = insertETL_FEED_TRAN_COLUMNS(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_CATEGORY_DEPENDENCIES(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_DEPENDENCIES(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_TRANSFORMED_COLUMNS(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_DESTINATION(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_TARGET_COLUMNS(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_LOAD_MAPPING(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_FEED_INJECTION_CONFIG(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			retVal = insertETL_ALERT_CONFIG(vObject, sessionId);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			/*
			 * retVal = insertETL_FEED_SCHEDULE_CONFIG(vObject, sessionId); if (retVal ==
			 * Constants.ERRONEOUS_OPERATION){ exceptionCode = getResultObject(retVal);
			 * throw buildRuntimeCustomException(exceptionCode); }
			 */

			/*
			 * retVal = insertETL_FEED_SCHEDULE_CONFIG(vObject, sessionId); if (retVal ==
			 * Constants.ERRONEOUS_OPERATION){ exceptionCode = getResultObject(retVal);
			 * throw buildRuntimeCustomException(exceptionCode); }
			 */
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			throw buildRuntimeCustomException(exceptionCode);
		}

	}

	public String printTimeFromMilliSecondsAsString(String nextScheduleDate, String timeZone) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
		Long timeInMilliSeconds = Long.parseLong(nextScheduleDate);
		Date now = new Date();
		now.setTime(timeInMilliSeconds);
		sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
		String output = sdf.format(now.getTime());
		return output;
	}

	public String replacePromptVariables(String query, OperationalConsoleVb vObj) {
		query = query.replaceAll("#PROMPT_VALUE_0#", vObj.getPromptValue0());
		query = query.replaceAll("#PROMPT_VALUE_1#", vObj.getPromptValue1());
		query = query.replaceAll("#PROMPT_VALUE_2#", vObj.getPromptValue2());
		query = query.replaceAll("#PROMPT_VALUE_3#", vObj.getPromptValue3());
		query = query.replaceAll("#PROMPT_VALUE_4#", vObj.getPromptValue4());
		query = query.replaceAll("#PROMPT_VALUE_5#", vObj.getPromptValue5());
		query = query.replaceAll("#PROMPT_VALUE_6#", vObj.getPromptValue6());
		query = query.replaceAll("#PROMPT_VALUE_7#", vObj.getPromptValue7());
		query = query.replaceAll("#PROMPT_VALUE_8#", vObj.getPromptValue8());
		query = query.replaceAll("#PROMPT_VALUE_9#", vObj.getPromptValue9());
		query = query.replaceAll("#PROMPT_VALUE_10#", vObj.getPromptValue10());
		query = query.replaceAll("#VISION_ID#", "" + CustomContextHolder.getContext().getVisionId());
		query = query.replaceAll("#TIME_ZONE#",
				ValidationUtil.isValid(commonDao.findVisionVariableValue("TIME_ZONE"))
						? commonDao.findVisionVariableValue("TIME_ZONE")
						: "Asia/Dubai");
		return query;
	}

	public List<OperationalConsoleVb> getAuditDetailsBasedOnFeed(String feedCat, String feedId, String sessionId,
			int versionNumber, int iterationCnt) {
		String sql = "select " + getDbFunction(Constants.DATEFUNC, null) + "(DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null) + "') "
				+ getDbFunction(Constants.PIPELINE, null) + " ' - Current Process [' "
				+ getDbFunction(Constants.PIPELINE, null) + " CURRENT_PROCESS "
				+ getDbFunction(Constants.PIPELINE, null) + " '] Audit [' "
				+ getDbFunction(Constants.PIPELINE, null) + " AUDIT_DESCRIPTION "
				+ getDbFunction(Constants.PIPELINE, null) + " '] Detail [' "
				+ getDbFunction(Constants.PIPELINE, null) + " AUDIT_DESCRIPTION_DETAIL "
				+ getDbFunction(Constants.PIPELINE, null) + " ']' value"
				+ " from ETL_FEED_AUDIT_TRAIL where FEED_CATEGORY = ? "
				+ " AND SESSION_ID = ? AND VERSION_NUMBER = ? AND ITERATION_COUNT = ? ";
				
		Object objParams[] = new Object[4];
		objParams[0] = feedCat;
		objParams[1] = sessionId;
		objParams[2] = versionNumber;
		objParams[3] = iterationCnt;

		if (ValidationUtil.isValid(feedId)) {
			objParams = new Object[5];
			objParams[0] = feedCat;
			objParams[1] = sessionId;
			objParams[2] = versionNumber;
			objParams[3] = iterationCnt;
			objParams[4] = feedId;
			sql = sql + " AND FEED_ID = ? ";
		}
		sql =  sql + " ORDER BY VERSION_NUMBER, ITERATION_COUNT, AUDIT_SEQUENCE, DATE_CREATION ";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb opVb = new OperationalConsoleVb();
				opVb.setAuditDesc(rs.getString("value"));
				return opVb;
			}
		};
		ArrayList<OperationalConsoleVb> auditLst = (ArrayList<OperationalConsoleVb>) getJdbcTemplate().query(sql,objParams,
				mapper);
		return auditLst;
	}

	@Override
	public void setServiceDefaults() {
		serviceName = "EtlFeedProccessControl";
		serviceDesc = "ETL Feed Process Control";
		tableName = "ETL_FEED_PROCESS_CONTROL";
		childTableName = "ETL_FEED_PROCESS_CONTROL";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	@Override
	protected ExceptionCode doInsertApprRecordForNonTrans(OperationalConsoleVb vObject) throws RuntimeCustomException {
		List<OperationalConsoleVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.ADD;
		strApproveOperation = Constants.ADD;
		setServiceDefaults();
		
		// To disable filter condition at Feed Lvl
		vObject.setGrid("1");
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if(ValidationUtil.isValid(vObject.getSessionId())) {
			// If session_ID is valid - means user is trying to add feed into an already existing category in the ETL_FEED_Process_Control table
			// So the collTemp at CategoryLvl(grid==1) should have value
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			// If session_ID is not valid - means user is trying to add new category into the ETL_FEED_Process_Control table
			// So the collTemp should be == 0
			if (collTemp.size() > 0) {
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			// New sessionID must be formed to be used to insert all the feeds of the Category
			vObject.setSessionId(String.valueOf(System.currentTimeMillis()));
		}
		
		
		if (vObject.getChilds() != null && vObject.getChilds().size() > 0) {
			// To enable filter condition at Feed Lvl
			vObject.setGrid("2");
			for (OperationalConsoleVb data : vObject.getChilds()) {
				// if(vObject.getFeedIdArr()!=null && vObject.getFeedIdArr().length>0) {
				// for(String feed: vObject.getFeedIdArr()) {
				vObject.setFeedId(data.getFeedId());
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				
				vObject.setMaker(getIntCurrentUserId());
				vObject.setFeedType(data.getFeedType());
				vObject.setBatchSize(data.getBatchSize());
				vObject.setDebugFlag(data.getDebugFlag());
			//	vObject.setLastExtractionDate(data.getLastExtractionDate());
				
				List<EtlFeedScheduleConfigVb> detailsLst = etlFeedMainDao.getDetails(data.getFeedId());
				
				if(detailsLst != null && detailsLst.size() > 0) {
					vObject.setScheduleType(detailsLst.get(0).getScheduleType());
				} else {
					exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
					throw buildRuntimeCustomException(exceptionCode);
				}
			
				/*
				 * String processType =
				 * etlFeedMainDao.getDetails(data.getFeedId()).get(0).getProcessDateType(); try
				 * { vObject.setProcessDate(calculateProcessDate(processType,
				 * vObject.getCountry(), vObject.getLeBook())); } catch (ParseException e) { //
				 *  // e.printStackTrace(); }
				 */
				// Try inserting the record
				vObject.setRecordIndicator(Constants.STATUS_ZERO);
				vObject.setVerifier(getIntCurrentUserId());
				List<OperationalConsoleVb>  schedulePropContext = getDetails(data.getFeedId());
				if(schedulePropContext != null && schedulePropContext.size() > 0) {
					vObject.setPreScriptID(schedulePropContext.get(0).getPreScriptID());
					vObject.setPostScriptID(schedulePropContext.get(0).getPostScriptID());
					vObject.setReadinessScriptID(schedulePropContext.get(0).getReadinessScriptID());
				}
				// Insert record into ETL_FEED_Process_Control table
				retVal = doInsertionAppr(vObject);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				} else {
					exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
				}
				OperationalConsoleVb vObj = new OperationalConsoleVb();
				if (retVal == Constants.SUCCESSFUL_OPERATION) {
					List<OperationalConsoleVb> feedDetails = getFeedDetails(vObject);
					if (feedDetails != null && feedDetails.size() > 0) {
						vObj = feedDetails.get(0);
					} else {
						exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
						throw buildRuntimeCustomException(exceptionCode);
					}
					vObj.setSessionId(vObject.getSessionId());
					vObj.setFeedType(vObject.getFeedType());
					vObj.setBatchSize(vObject.getBatchSize());
					vObj.setDebugFlag(vObject.getDebugFlag());
				//	vObj.setLastExtractionDate(vObject.getLastExtractionDate());
					// Insert PRS Records
					StringJoiner etlSourceConnectorId = new StringJoiner(",");
					if (collTemp.size() > 1) {
						for (OperationalConsoleVb vb : collTemp) {
							etlSourceConnectorId.add(Constants.S_QUOTE + vb.getEtlSourceConnectorId() + Constants.S_QUOTE);
						}
						vObj.setEtlSourceConnectorId(etlSourceConnectorId.toString());
					} else {
						vObj.setEtlSourceConnectorId(
								Constants.S_QUOTE + vObj.getEtlSourceConnectorId() + Constants.S_QUOTE);
					}
					exceptionCode = doInsertionAppr(vObj, vObj.getSessionId());
					if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						throw buildRuntimeCustomException(exceptionCode);
					} else {
						int retVal = doUpdateLastExtractionDate(vObj);
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
				String systemDate = getSystemDate();
				vObj.setDateLastModified(systemDate);
				vObj.setDateCreation(systemDate);
				exceptionCode = writeAuditLog(vObj, null);
				if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return exceptionCode;
	}

	/* Commented by Prakashika on 15_December_2023
	 * 
	 *  @Override
	protected ExceptionCode doInsertRecordForNonTrans(OperationalConsoleVb vObject) throws RuntimeCustomException {
		List<OperationalConsoleVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		if (vObject.getChilds() != null && vObject.getChilds().size() > 0) {
			for (OperationalConsoleVb data : vObject.getChilds()) {
				// if(vObject.getFeedIdArr()!=null && vObject.getFeedIdArr().length>0) {
				// for(String feed: vObject.getFeedIdArr()) {
				vObject.setFeedId(data.getFeedId());
				vObject.setMaker(getIntCurrentUserId());
				vObject.setDebugFlag(data.getDebugFlag());
				vObject.setLastExtractionDate(data.getLastExtractionDate());

				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				// If record already exists in the approved table, reject the addition
				if (collTemp.size() > 0) {
					int staticDeletionFlag = getStatus(((ArrayList<OperationalConsoleVb>) collTemp).get(0));
					if (staticDeletionFlag == Constants.PASSIVATE) {
						exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
						throw buildRuntimeCustomException(exceptionCode);
					} else {
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
					OperationalConsoleVb vObjectLocal = ((ArrayList<OperationalConsoleVb>) collTemp).get(0);
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
					String sessionId = String.valueOf(System.currentTimeMillis());
					if (!ValidationUtil.isValid(vObject.getSessionId())) {
						vObject.setSessionId(sessionId);
					}
					retVal = doInsertionPend(vObject);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					if (retVal == Constants.SUCCESSFUL_OPERATION) {
						List<OperationalConsoleVb> feedDetails = getFeedDetails(vObject);
						if (feedDetails != null && feedDetails.size() > 0) {
							vObject = feedDetails.get(0);
						}
						if (!ValidationUtil.isValid(vObject.getSessionId())) {
							vObject.setSessionId(sessionId);
						}
						StringJoiner etlSourceConnectorId = new StringJoiner(",");
						if (collTemp.size() > 1) {
							for (OperationalConsoleVb vb : collTemp) {
								etlSourceConnectorId.add(Constants.S_QUOTE + vb.getEtlSourceConnectorId() + Constants.S_QUOTE);
							}
							vObject.setEtlSourceConnectorId(etlSourceConnectorId.toString());
						} else {
							vObject.setEtlSourceConnectorId(
									Constants.S_QUOTE + vObject.getEtlSourceConnectorId() + Constants.S_QUOTE);
						}
						exceptionCode = doInsertionAppr(vObject, vObject.getSessionId());
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
			}
		}
		exceptionCode = getResultObject(retVal);
		return exceptionCode;
	} */ 

	public List<OperationalConsoleVb> getDetails(String feedId) {
		String sql = "SELECT SCHEDULE_TYPE, SCHEDULE_PROP_CONTEXT FROM ETL_FEED_SCHEDULE_config where  feed_id='"
				+ feedId + "'";
		try {
			return getJdbcTemplate().query(sql, getETLFeedScheduleMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	private RowMapper getETLFeedScheduleMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb vObj = new OperationalConsoleVb();
				vObj.setScheduleType(rs.getString("SCHEDULE_TYPE"));
				try {
					String propContext = rs.getString("SCHEDULE_PROP_CONTEXT");
					ETLFeedTimeBasedPropVb timePropVb = new ETLFeedTimeBasedPropVb();
					ETLFeedEventBasedPropVb eventPropVb = new ETLFeedEventBasedPropVb();
					XmlMapper xmlMapper = new XmlMapper();
					if ("TIME".equalsIgnoreCase(vObj.getScheduleType())) {
						timePropVb = xmlMapper.readValue(rs.getString("SCHEDULE_PROP_CONTEXT"),ETLFeedTimeBasedPropVb.class);
						vObj.setTimeBasedScheduleProp(timePropVb);
						vObj.setReadinessScriptID(timePropVb.getReadinessScriptID());
						vObj.setPreScriptID(timePropVb.getPreScriptID());
						vObj.setPostScriptID(timePropVb.getPostScriptID());
					} else if ("EVENT".equalsIgnoreCase(vObj.getScheduleType())) {
						eventPropVb.setType(CommonUtils.getValueForXmlTag(propContext, "type"));
						eventPropVb.setReadinessScriptID(CommonUtils.getValueForXmlTag(propContext, "readinessScriptID"));
						eventPropVb.setPreScriptID(CommonUtils.getValueForXmlTag(propContext, "preScriptID"));
						eventPropVb.setPostScriptID(CommonUtils.getValueForXmlTag(propContext, "postScriptID"));
						vObj.setEventBasedScheduleProp(eventPropVb);
						vObj.setEventType(eventPropVb.getType());
						vObj.setReadinessScriptID(eventPropVb.getReadinessScriptID());
						vObj.setPreScriptID(eventPropVb.getPreScriptID());
						vObj.setPostScriptID(eventPropVb.getPostScriptID());
					}
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
				return vObj;
			}
		};
		return mapper;
	}

	public List<OperationalConsoleVb> getDepErrorResults(OperationalConsoleVb dObj) {
		List<OperationalConsoleVb> collTemp = null;
//		StringBuffer strQueryAppr = new StringBuffer("SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME FEED_NAME," + 
//				"                T2.FEED_ID PARENT_FEED_ID, T2.FEED_NAME PARENT_FEED_NAME, T2.FEED_CATEGORY PARENT_FEED_CATEGORY," + 
//				"                T3.COMPLETION_STATUS MAIN_TABLE_STATUS, "+CompletionStatusAtApprDesc 
//				+ ",T4.COMPLETION_STATUS PROCESS_CONTROL_STATUS, "+ProcessControlStatusAtApprDesc
//				+ " ,T4.SESSION_ID" + 
//				"                FROM (SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.DEPENDENT_FEED_ID," + 
//				"                T2.FEED_NAME, T2.SESSION_ID" + 
//				"                FROM    ETL_FEED_DEPENDENCIES_PRS T1 INNER JOIN ETL_FEED_MAIN_PRS T2" + 
//				"                ON (    T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK" + 
//				"                AND T1.FEED_ID = T2.FEED_ID AND T1.SESSION_ID = T2.SESSION_ID)) T1" + 
//				"                INNER JOIN ETL_FEED_MAIN_PRS T2  ON (    T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK" + 
//				"                AND T1.DEPENDENT_FEED_ID = T2.FEED_ID AND T1.SESSION_ID = T2.SESSION_ID)" + 
//				"                INNER JOIN ETL_FEED_MAIN T3  ON (    T1.COUNTRY = T3.COUNTRY AND T1.LE_BOOK = T3.LE_BOOK" + 
//				"                AND T1.DEPENDENT_FEED_ID = T3.FEED_ID)  " + 
//				"                INNER JOIN ETL_FEED_PROCESS_CONTROL T4 ON (     T4.COUNTRY = T1.COUNTRY AND T4.LE_BOOK = T2.LE_BOOK" + 
//				"                AND T4.FEED_ID = T2.FEED_ID)");

		StringBuffer strQueryAppr = new StringBuffer("SELECT T1.COUNTRY,       T1.LE_BOOK,       T1.FEED_ID,"
				+ "       T1.FEED_NAME FEED_NAME,       T2.FEED_ID PARENT_FEED_ID,"
				+ "       T2.FEED_NAME PARENT_FEED_NAME,       T2.FEED_CATEGORY PARENT_FEED_CATEGORY,"
				+ "       T3.COMPLETION_STATUS MAIN_TABLE_STATUS,"
				+ "       T4.COMPLETION_STATUS PROCESS_CONTROL_STATUS,       T4.SESSION_ID"
				+ "  FROM (SELECT T1.COUNTRY,               T1.LE_BOOK,               T1.FEED_ID,"
				+ "               T1.DEPENDENT_FEED_ID,               T2.FEED_NAME,"
				+ "               T2.SESSION_ID,               T2.FEED_CATEGORY"
				+ "          FROM    ETL_FEED_DEPENDENCIES_PRS T1               INNER JOIN"
				+ "                  ETL_FEED_MAIN_PRS T2               ON (    T1.COUNTRY = T2.COUNTRY"
				+ "                   AND T1.LE_BOOK = T2.LE_BOOK                   AND T1.FEED_ID = T2.FEED_ID"
				+ "                   AND T1.SESSION_ID = T2.SESSION_ID)) T1       inner JOIN ETL_FEED_MAIN_PRS T2"
				+ "          ON (    T1.COUNTRY = T2.COUNTRY              AND T1.LE_BOOK = T2.LE_BOOK"
				+ "              AND T1.DEPENDENT_FEED_ID = T2.FEED_ID )       INNER JOIN ETL_FEED_MAIN T3"
				+ "          ON (    T1.COUNTRY = T3.COUNTRY              AND T1.LE_BOOK = T3.LE_BOOK"
				+ "              AND T1.DEPENDENT_FEED_ID = T3.FEED_ID)"
				+ "       INNER JOIN ETL_FEED_PROCESS_CONTROL T4          ON (    T4.COUNTRY = T1.COUNTRY"
				+ "              AND T4.LE_BOOK = T1.LE_BOOK              AND T4.FEED_ID = T1.FEED_ID AND T4.SESSION_ID = T1.SESSION_ID) union "
				+ "SELECT T1.COUNTRY,       T1.LE_BOOK,       T1.FEED_ID,       T1.FEED_NAME FEED_NAME,"
				+ "       T2.FEED_ID PARENT_FEED_ID,       T2.FEED_NAME PARENT_FEED_NAME,"
				+ "       T2.FEED_CATEGORY PARENT_FEED_CATEGORY,       T3.COMPLETION_STATUS MAIN_TABLE_STATUS,"
				+ "       T4.COMPLETION_STATUS PROCESS_CONTROL_STATUS,       T4.SESSION_ID"
				+ "  FROM (SELECT T1.COUNTRY,               T1.LE_BOOK,               T1.FEED_ID,"
				+ "               T1.CATEGORY_ID FEED_CATEGORY,               T1.DEPENDENT_CATEGORY,"
				+ "               T2.FEED_NAME,               T2.SESSION_ID"
				+ "          FROM    ETL_FEED_CATEGORY_DEP_PRS T1               INNER JOIN"
				+ "                  ETL_FEED_MAIN_PRS T2               ON (    T1.COUNTRY = T2.COUNTRY"
				+ "                   AND T1.LE_BOOK = T2.LE_BOOK                   AND T1.FEED_ID = T2.FEED_ID"
				+ "                   and t1.CATEGORY_ID = t2.FEED_CATEGORY"
				+ "                   AND T1.SESSION_ID = T2.SESSION_ID)) T1       inner JOIN ETL_FEED_MAIN_PRS T2"
				+ "          ON (    T1.COUNTRY = T2.COUNTRY              AND T1.LE_BOOK = T2.LE_BOOK"
				+ "              AND T1.DEPENDENT_CATEGORY = T2.FEED_CATEGORY )       INNER JOIN ETL_FEED_MAIN T3"
				+ "          ON (    T1.COUNTRY = T3.COUNTRY              AND T1.LE_BOOK = T3.LE_BOOK"
				+ "              AND T2.FEED_ID = T3.FEED_ID)       INNER JOIN ETL_FEED_PROCESS_CONTROL T4"
				+ "          ON (    T4.COUNTRY = T1.COUNTRY              AND T4.LE_BOOK = T1.LE_BOOK"
				+ "              AND T4.FEED_ID = T1.FEED_ID AND T4.SESSION_ID = T1.SESSION_ID) "
				+ "  WHERE T1.COUNTRY = ?  AND T1.LE_BOOK =  ?  AND  T1.FEED_ID = ?  AND T4.SESSION_ID = ? ");

		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId(), dObj.getSessionId() };
		try {
			collTemp = getJdbcTemplate().query(strQueryAppr.toString(), args, getEtlDepErrorMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	protected RowMapper getEtlDepErrorMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb vObject = new OperationalConsoleVb();
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
				if (rs.getString("FEED_ID") != null) {
					vObject.setFeedId(rs.getString("FEED_ID"));
				} else {
					vObject.setFeedId("");
				}
				if (rs.getString("PARENT_FEED_ID") != null) {
					vObject.setParentFeedId(rs.getString("PARENT_FEED_ID"));
				} else {
					vObject.setParentFeedId("");
				}
				if (rs.getString("PARENT_FEED_NAME") != null) {
					vObject.setParentFeedName(rs.getString("PARENT_FEED_NAME"));
				} else {
					vObject.setParentFeedName("");
				}
				if (rs.getString("PARENT_FEED_CATEGORY") != null) {
					vObject.setParentFeedCategory(rs.getString("PARENT_FEED_CATEGORY"));
				} else {
					vObject.setParentFeedCategory("");
				}
				if (rs.getString("FEED_NAME") != null) {
					vObject.setFeedName(rs.getString("FEED_NAME"));
				} else {
					vObject.setFeedName("");
				}
				if (rs.getString("SESSION_ID") != null) {
					vObject.setSessionId(rs.getString("SESSION_ID"));
				} else {
					vObject.setSessionId("");
				}
				if (rs.getString("MAIN_TABLE_STATUS") != null) {
					vObject.setMainTableStatus(rs.getString("MAIN_TABLE_STATUS"));
				} else {
					vObject.setMainTableStatus("");
				}
				/*
				 * if(rs.getString("MAIN_TABLE_STATUS_DESC")!= null){
				 * vObject.setMainTableStatusDesc(rs.getString("MAIN_TABLE_STATUS_DESC"));
				 * }else{ vObject.setMainTableStatusDesc(""); }
				 */
				if (rs.getString("PROCESS_CONTROL_STATUS") != null) {
					vObject.setProcessControlStatus(rs.getString("PROCESS_CONTROL_STATUS"));
				} else {
					vObject.setProcessControlStatus("");
				}
//				if(rs.getString("PROCESS_CONTROL_STATUS_DESC")!= null){ 
//					vObject.setProcessControlStatusDesc(rs.getString("PROCESS_CONTROL_STATUS_DESC"));
//				}else{
//					vObject.setProcessControlStatusDesc("");
//				}
				return vObject;
			}
		};
		return mapper;
	}

	@SuppressWarnings("deprecation")
	public ExceptionCode getAlertDetailsOfFeed(ETLFeedProcessControlVb processControlVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String sql = "select ALERT_TYPE from ETL_FEED_SCHEDULE_CONFIG where FEED_ID = ? ";
			Object[] args = { processControlVb.getFeedId() };

			processControlVb.setAlertType(getJdbcTemplate().queryForObject(sql, args, String.class));
			
			sql = "select ALERT_FLAG from ETL_FEED_SCHEDULE_CONFIG where FEED_ID = ? ";
			
			if ("C".equalsIgnoreCase(processControlVb.getAlertType())) {
				sql = "select ALERT_FLAG from ETL_FEED_CATEGORY_PRS where FEED_ID = ? AND SESSION_ID = ? " ;
				args = new Object[] { processControlVb.getFeedId(), processControlVb.getSessionId() };
			}
			processControlVb.setAlertFlag(getJdbcTemplate().queryForObject(sql, args, String.class));

			if ("Y".equalsIgnoreCase(processControlVb.getAlertFlag())) {
				Map<String, ETLAlertVb> alertVbMap = new HashMap<String, ETLAlertVb>();
				if ("C".equalsIgnoreCase(processControlVb.getAlertType())) {
					sql = " select T1.COUNTRY, T1.LE_BOOK, T1.CATEGORY_ID, T1.CHANNEL_TYPE_SMS, T1.CHANNEL_TYPE_EMAIL,"
							+ " UPPER(T2.EVENT_TYPE) EVENT_TYPE, T2.MATRIX_ID, "
							+ " T3.CONDITION, T3.CONDITION_EXECUTION_TYPE, T3.CONDITION_VALIDATION_TYPE, T3.CONDITION_PASS_VALUE "
							+ " from ETL_FEED_CATEGORY_PRS T1, ETL_CATEGORY_ALERT_CONFIG_PRS T2, ETL_ALERT_EVENT T3 "
							+ " WHERE T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.CATEGORY_ID = T2.CATEGORY_ID "
							+ " AND T1.FEED_ID = T2.FEED_ID AND T1.SESSION_ID = T2.SESSION_ID "
							+ " AND  T2.EVENT_TYPE = T3.EVENT_TYPE "
							+ " AND UPPER(T1.COUNTRY) = UPPER(?) AND UPPER(T1.LE_BOOK) = UPPER(?) AND UPPER(T1.CATEGORY_ID) = UPPER(?) "
							+ " AND UPPER(T1.FEED_ID) = UPPER(?) AND T1.SESSION_ID = ? AND T3.EVENT_ALERT_STATUS = 0 ";
					
					Object arg[] = { processControlVb.getCountry(), processControlVb.getLeBook(),
							processControlVb.getFeedCategory(), processControlVb.getFeedId(),
							processControlVb.getSessionId() };
					
					if (ValidationUtil.isValid(processControlVb.getEventType())) {
						sql = sql + " UPPER(T2.EVENT_TYPE) = UPPER('" + processControlVb.getEventType() + "')";
					}
					
					alertVbMap = getJdbcTemplate().query(sql, arg, getRSEforAlertDetails());
				} else {
					sql = " select T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.CHANNEL_TYPE_SMS, T1.CHANNEL_TYPE_EMAIL, UPPER(T2.EVENT_TYPE) EVENT_TYPE, T2.MATRIX_ID, "
							+ " T3.CONDITION, T3.CONDITION_EXECUTION_TYPE, T3.CONDITION_VALIDATION_TYPE, T3.CONDITION_PASS_VALUE "
							+ " from ETL_FEED_SCHEDULE_CONFIG T1, ETL_FEED_ALERT_CONFIG_PRS T2, ETL_ALERT_EVENT T3 "
							+ " WHERE T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.FEED_ID = T2.FEED_ID "
							+ " AND T2.EVENT_TYPE = T3.EVENT_TYPE "
							+ " AND UPPER(T1.COUNTRY) = UPPER(?) AND UPPER(T1.LE_BOOK) = UPPER(?) AND UPPER(T1.FEED_ID) = UPPER(?)"
							+ " AND T2.SESSION_ID = ? AND T3.EVENT_ALERT_STATUS = 0 ";
					
					Object arg[] = { processControlVb.getCountry(), processControlVb.getLeBook(),
							processControlVb.getFeedId(), processControlVb.getSessionId() };
					
					if (ValidationUtil.isValid(processControlVb.getEventType())) {
						sql = sql + " UPPER(T2.EVENT_TYPE) = UPPER('" + processControlVb.getEventType() + "')";
					}
					
					alertVbMap = getJdbcTemplate().query(sql, arg, getRSEforAlertDetails());
				}
				exceptionCode.setResponse(alertVbMap);
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	protected ResultSetExtractor<Map<String, ETLAlertVb>> getRSEforAlertDetails() {
		ResultSetExtractor<Map<String, ETLAlertVb>> rsExtrsctor = new ResultSetExtractor<Map<String, ETLAlertVb>>() {
			@Override
			public Map<String, ETLAlertVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				Map<String, ETLAlertVb> alertVbMap = new HashMap<String, ETLAlertVb>();
				while (rs.next()) {
					if (alertVbMap.get(rs.getString("EVENT_TYPE")) != null) {
						ETLAlertVb alertVb = alertVbMap.get(rs.getString("EVENT_TYPE"));
						List<String> matrixIdLst = alertVb.getMatrixIdLst();
						matrixIdLst.add(rs.getString("MATRIX_ID"));
						alertVb.setMatrixIdLst(matrixIdLst);
						alertVbMap.put(rs.getString("EVENT_TYPE"), alertVb);
					} else {
						ETLAlertVb alertVb = new ETLAlertVb();
						alertVb.setChannelTypeSms(rs.getString("CHANNEL_TYPE_SMS"));
						alertVb.setChannelTypeEmail(rs.getString("CHANNEL_TYPE_EMAIL"));
						alertVb.setEventType(rs.getString("EVENT_TYPE"));
						alertVb.setMatrixIdLst(new ArrayList<String>(Arrays.asList(rs.getString("MATRIX_ID"))));
						alertVb.setCondition(rs.getString("CONDITION"));
						alertVb.setConditionExecutionType(rs.getString("CONDITION_EXECUTION_TYPE"));
						alertVb.setConditionValidationType(rs.getString("CONDITION_VALIDATION_TYPE"));
						alertVb.setConditionPassValue(rs.getString("CONDITION_PASS_VALUE"));
						alertVbMap.put(alertVb.getEventType(), alertVb);
					}
				}
				return alertVbMap;
			}
		};
		return rsExtrsctor;
	}

	public ExceptionCode getAlertContentConfigMap(ETLFeedProcessControlVb processControlVb, String eventType) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String sql = "select UPPER(TAG_NAME) SUB_VARIANCE, UPPER(MACROVAR_DESC) CONTENT from MACROVAR_TAGGING "
					+ "where MACROVAR_NAME = 'ETL_ALERT_CONTENT' AND UPPER(MACROVAR_TYPE) = ? ";
			Object[] args = {eventType};
			Map<String, String> alertContentConfigMap = getJdbcTemplate().query(sql,
					new ResultSetExtractor<Map<String, String>>() {
						@Override
						public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
							Map<String, String> returnMap = new HashMap<String, String>();
							while (rs.next()) {
								returnMap.put(rs.getString("SUB_VARIANCE"),
										replaceMacroVarExtra(processControlVb, rs.getString("CONTENT")));
							}
							return returnMap;
						}
					}, args);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(alertContentConfigMap);
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}

	public String replaceMacroVarExtra(ETLFeedProcessControlVb processControlVb, String str) {
		ExceptionCode exceptionCode = replaceMacroVarValues(str, processControlVb);
		if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			return (String) exceptionCode.getResponse();
		} else {
			return str;
		}
	}

	public ExceptionCode replaceMacroVarValues(String str, ETLFeedProcessControlVb processControlVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String beforeChange = str;
		try {
			Pattern pattern = Pattern.compile("#(.*?)#", Pattern.DOTALL);
			Matcher matcher = pattern.matcher(str);
			while (matcher.find()) {
				if ("LAST_EXTRACTION_DATE".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#",
							processControlVb.getLastExtractionDateFormatted());
				if ("COUNTRY".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getCountry());
				if ("LE_BOOK".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getLeBook());
				if ("FEED_ID".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getFeedId());
				if ("FEED_CATEGORY".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getFeedCategory());
				if ("PROCESS_DATE".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getProcessDate());
				if ("SESSION_ID".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getSessionId());
				if ("TIME_ZONE".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getTimeZone());
				if ("FEED_NAME".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getFeedName());
				if ("DATE".equalsIgnoreCase(matcher.group(1))) {
					SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
					Date now = new Date();
					if (ValidationUtil.isValid(processControlVb.getTimeZone()))
						sdf.setTimeZone(TimeZone.getTimeZone(processControlVb.getTimeZone()));
					str = str.replaceAll("#" + matcher.group(1) + "#", sdf.format(now.getTime()));
				}
				if ("DATE_TIME".equalsIgnoreCase(matcher.group(1))) {
					SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
					Date now = new Date();
					if (ValidationUtil.isValid(processControlVb.getTimeZone()))
						sdf.setTimeZone(TimeZone.getTimeZone(processControlVb.getTimeZone()));
					str = str.replaceAll("#" + matcher.group(1) + "#", sdf.format(now.getTime()));
				}
				if ("VERSION_NUMBER".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#",
							String.valueOf(processControlVb.getVersionNumber()));
				if ("ITERATION_COUNT".equalsIgnoreCase(matcher.group(1)))
					str = str.replaceAll("#" + matcher.group(1) + "#",
							String.valueOf(processControlVb.getIterationCount()));
				if ("ALERT_SYSTEM_CONTENT".equalsIgnoreCase(matcher.group(1))) {
					if (ValidationUtil.isValid(processControlVb.getAlertSystemContent()))
						str = str.replaceAll("#" + matcher.group(1) + "#", processControlVb.getAlertSystemContent());
					else
						str = str.replaceAll("#" + matcher.group(1) + "#",
								Constants.UNKNOWN + Constants.CONTACT_SYSTEM_ADMIN);
				}
				if ((matcher.group(1).toUpperCase()).startsWith("VB")) {
					String replaceValue = getValueFromVisionBusinessDayTable(matcher.group(1));
					if (ValidationUtil.isValid(replaceValue))
						str = str.replaceAll("#" + matcher.group(1) + "#", replaceValue);
				}
			}
			// If "beforeChange" & "str" after the patternmatch is still the same, then
			// there can be no further change done to the String
			if (beforeChange.equalsIgnoreCase(str)) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(str);
			} else {
				matcher = pattern.matcher(str);
				// recursive call if Hash values still exists
				if (matcher.find()) {
					exceptionCode = replaceMacroVarValues(str, processControlVb);
					if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION)
						str = (String) exceptionCode.getResponse();
				} else {
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					exceptionCode.setResponse(str);
				}
			}
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}

	@SuppressWarnings("deprecation")
	public String getValueFromVisionBusinessDayTable(String hashVar) {
		try {
			String sql = new String("select HASH_VALUE from VISION_BUSINESS_DAY_NARROW WHERE HASH_VARIABLE = ?");

			Object[] args = { hashVar };
			return getJdbcTemplate().queryForObject(sql, args, String.class);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public void insertIntoAlertContent(ETLFeedProcessControlVb vObj, String eventType,
			Map<String, String> alertContentConfigMap) {
		try {
			ETLAlertVb alertVb = vObj.getAlertVbMap().get(eventType);
			for (String matrixID : alertVb.getMatrixIdLst()) {
				if ("Y".equalsIgnoreCase(alertVb.getChannelTypeEmail())) {
					String query = "INSERT INTO ETL_ALERT_CONTENT (MATRIX_ID, SUBJECT, CONTENT, COMPLETION_STATUS_AT, COMPLETION_STATUS, "
							+ " ALERT_CONTENT_STATUS_NT, ALERT_CONTENT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
							+ " DATE_LAST_MODIFIED, DATE_CREATION) values (?, ?, ?, ?, ?, "
							+ " ?, ?, ?, ?, ?, ?, ?,  " + getDbFunction(Constants.SYSDATE, null) + ", "
							+ getDbFunction(Constants.SYSDATE, null) + ")";
					Object[] args = { matrixID, alertContentConfigMap.get("SUBJECT"), alertContentConfigMap.get("BODY"),
							2113, "P", 1, 0, 7, 0, vObj.getMaker(), vObj.getVerifier(), vObj.getInternalStatus() };
					getJdbcTemplate().update(query, args);
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings({ "deprecation" })
	public ExceptionCode getDependencyDetailsSingleLvl(ConsoleDependencyFeedVb vObject,
			LinkedHashMap<String, ConsoleDependencyFeedVb> dependencyFeedDetailsMap) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String sql = "";
			if("D".equalsIgnoreCase(vObject.getDependencyFlag())){
				 sql = "SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME,  "
							+ "        T1.SESSION_ID, T1.FEED_CATEGORY,  "
							+ "        T5.COMPLETION_STATUS MAIN_TABLE_STATUS, T6.COMPLETION_STATUS PROCESS_CONTROL_STATUS, "
							+ "        T2.COUNTRY PARENT_COUNTRY, T2.LE_BOOK PARENT_LE_BOOK, T2.FEED_ID PARENT_FEED_ID, T2.FEED_NAME PARENT_FEED_NAME, "
							+ "        T2.SESSION_ID PARENT_SESSION_ID, T2.FEED_CATEGORY PARENT_FEED_CATEGORY, "
							+ "        T3.COMPLETION_STATUS PARENT_MAIN_TABLE_STATUS, T4.COMPLETION_STATUS PARENT_PROCESS_CONTROL_STATUS "
							+ "   FROM (SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.DEPENDENT_FEED_ID,  "
							+ "                T2.FEED_NAME, T2.SESSION_ID, T2.FEED_CATEGORY  "
							+ "           FROM    ETL_FEED_DEPENDENCIES_PRS T1                  INNER JOIN  "
							+ "                   ETL_FEED_MAIN_PRS T2  "
							+ "                ON (    T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK  "
							+ "                    AND T1.FEED_ID = T2.FEED_ID AND T1.SESSION_ID = T2.SESSION_ID)) T1  "
							+ "        INNER JOIN ETL_FEED_MAIN_PRS T2 "  /*Parent Feed MAIN_PRS */
							+ "           ON (    T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK  "
							+ "               AND T1.DEPENDENT_FEED_ID = T2.FEED_ID )  "
							+ "        INNER JOIN ETL_FEED_MAIN T3  "  /*Parent Feed MAIN */
							+ "           ON (    T2.COUNTRY = T3.COUNTRY AND T2.LE_BOOK = T3.LE_BOOK  "
							+ "               AND T2.FEED_ID = T3.FEED_ID)  "
							+ "        INNER JOIN ETL_FEED_PROCESS_CONTROL T4   "  /*Parent Feed Details */
							+ "           ON (    T2.COUNTRY = T4.COUNTRY AND T2.LE_BOOK = T4.LE_BOOK  "
							+ "               AND T2.FEED_ID = T4.FEED_ID AND T2.SESSION_ID = T4.SESSION_ID)         INNER JOIN ETL_FEED_MAIN T5  "  /* Current Feed MAIN */
							+ "           ON (    T1.COUNTRY = T5.COUNTRY AND T1.LE_BOOK = T5.LE_BOOK  "
							+ "               AND T1.FEED_ID = T5.FEED_ID)  "
							+ "        INNER JOIN ETL_FEED_PROCESS_CONTROL T6  "  /*Current Feed Details */
							+ "           ON (    T1.COUNTRY = T6.COUNTRY AND T1.LE_BOOK = T6.LE_BOOK  "
							+ "               AND T1.FEED_ID = T6.FEED_ID AND T1.SESSION_ID = T6.SESSION_ID) "
							+ "               where T1.feed_id = ? AND T1.SESSION_ID = ? UNION "
							+ " SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME,  "
							+ "        T1.SESSION_ID, T1.FEED_CATEGORY,  "
							+ "        T5.COMPLETION_STATUS MAIN_TABLE_STATUS, T6.COMPLETION_STATUS PROCESS_CONTROL_STATUS, "
							+ "        T2.COUNTRY PARENT_COUNTRY, T2.LE_BOOK PARENT_LE_BOOK, T2.FEED_ID PARENT_FEED_ID, T2.FEED_NAME PARENT_FEED_NAME, "
							+ "        T2.SESSION_ID PARENT_SESSION_ID, T2.FEED_CATEGORY PARENT_FEED_CATEGORY, "
							+ "        T3.COMPLETION_STATUS PARENT_MAIN_TABLE_STATUS, T4.COMPLETION_STATUS PARENT_PROCESS_CONTROL_STATUS "
							+ "  FROM (SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.CATEGORY_ID FEED_CATEGORY,  "
							+ "               T1.DEPENDENT_CATEGORY, T2.FEED_NAME, T2.SESSION_ID  "
							+ "          FROM    ETL_FEED_CATEGORY_DEP_PRS T1                 INNER JOIN  "
							+ "                  ETL_FEED_MAIN_PRS T2  "
							+ "               ON (    T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK  "
							+ "                   AND T1.FEED_ID = T2.FEED_ID and t1.CATEGORY_ID = t2.FEED_CATEGORY  "
							+ "                   AND T1.SESSION_ID = T2.SESSION_ID)) T1  "
							+ "       INNER JOIN ETL_FEED_MAIN_PRS T2  "
							+ "          ON (    T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK  "
							+ "              AND T1.DEPENDENT_CATEGORY = T2.FEED_CATEGORY )  "
							+ "       INNER JOIN ETL_FEED_MAIN T3  " /*Parent Feed MAIN*/
							+ "           ON (    T2.COUNTRY = T3.COUNTRY AND T2.LE_BOOK = T3.LE_BOOK  "
							+ "               AND T2.FEED_ID = T3.FEED_ID)  "
							+ "        INNER JOIN ETL_FEED_PROCESS_CONTROL T4  " /*Parent Feed Details*/
							+ "           ON (    T2.COUNTRY = T4.COUNTRY AND T2.LE_BOOK = T4.LE_BOOK  "
							+ "               AND T2.FEED_ID = T4.FEED_ID AND T2.SESSION_ID = T4.SESSION_ID) "
							+ "        INNER JOIN ETL_FEED_MAIN T5  " /* Current Feed MAIN*/
							+ "           ON (    T1.COUNTRY = T5.COUNTRY AND T1.LE_BOOK = T5.LE_BOOK  "
							+ "               AND T1.FEED_ID = T5.FEED_ID)  "
							+ "        INNER JOIN ETL_FEED_PROCESS_CONTROL T6 " /*Current Feed Details */
							+ "           ON (    T1.COUNTRY = T6.COUNTRY AND T1.LE_BOOK = T6.LE_BOOK  "
							+ "               AND T1.FEED_ID = T6.FEED_ID AND T1.SESSION_ID = T6.SESSION_ID) "
							+ "               where T1.feed_id = ? AND T1.SESSION_ID = ?";
			} else if ("F".equalsIgnoreCase(vObject.getDependencyFlag())) {
				sql = "  SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME, T1.SESSION_ID, T1.FEED_CATEGORY, T5.COMPLETION_STATUS MAIN_TABLE_STATUS, " + 
						"  T6.COMPLETION_STATUS PROCESS_CONTROL_STATUS, T1.COUNTRY PARENT_COUNTRY, T1.LE_BOOK PARENT_LE_BOOK, " + 
						getDbFunction(Constants.NVL, null)+"(T2.FEED_ID,T1.DEPENDENT_FEED_ID) PARENT_FEED_ID, " + 
						getDbFunction(Constants.NVL, null)+"(T2.FEED_NAME, T3.FEED_NAME) PARENT_FEED_NAME, " + 
						getDbFunction(Constants.NVL, null)+"(T2.SESSION_ID,0) PARENT_SESSION_ID, " + 
						getDbFunction(Constants.NVL, null)+"(T2.FEED_CATEGORY,T3.FEED_CATEGORY) PARENT_FEED_CATEGORY, " + 
						"  T3.COMPLETION_STATUS PARENT_MAIN_TABLE_STATUS, " + 
						getDbFunction(Constants.NVL, null)+"(T4.COMPLETION_STATUS,'P') PARENT_PROCESS_CONTROL_STATUS " + 
						"  FROM " + 
						"  (SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.DEPENDENT_FEED_ID, T2.FEED_NAME, T2.SESSION_ID, T2.FEED_CATEGORY " + 
						"      FROM ETL_FEED_DEPENDENCIES_PRS T1 " + 
						"      INNER JOIN ETL_FEED_MAIN_PRS T2 " + 
						"      ON ( T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.FEED_ID = T2.FEED_ID AND T1.SESSION_ID = T2.SESSION_ID)) T1 " + 
						/*"  left JOIN ETL_FEED_MAIN_PRS T2 " + 
						"  ON ( T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.DEPENDENT_FEED_ID = T2.FEED_ID ) " + */
						"  left JOIN ETL_FEED_PROCESS_CONTROL T7 "+
						"  ON ( T1.COUNTRY = T7.COUNTRY AND T1.LE_BOOK = T7.LE_BOOK AND T1.DEPENDENT_FEED_ID = T7.FEED_ID ) "+
						"  left JOIN ETL_FEED_MAIN_PRS T2 "+
						"  ON ( T7.COUNTRY = T2.COUNTRY AND T7.LE_BOOK = T2.LE_BOOK AND T7.FEED_ID = T2.FEED_ID and t7.SESSION_ID = t2.SESSION_ID) "+
						"  left JOIN ETL_FEED_MAIN T3 " + 
						"  ON ( T3.COUNTRY = T1.COUNTRY AND T3.LE_BOOK = T1.LE_BOOK AND T3.FEED_ID = T1.DEPENDENT_FEED_ID) " + 
						"  left JOIN ETL_FEED_PROCESS_CONTROL T4 " + 
						"  ON ( T2.COUNTRY = T4.COUNTRY AND T2.LE_BOOK = T4.LE_BOOK AND T2.FEED_ID = T4.FEED_ID AND T2.SESSION_ID = T4.SESSION_ID) " + 
						"  INNER JOIN ETL_FEED_MAIN T5 " + 
						"  ON ( T1.COUNTRY = T5.COUNTRY AND T1.LE_BOOK = T5.LE_BOOK AND T1.FEED_ID = T5.FEED_ID) " + 
						"  INNER JOIN ETL_FEED_PROCESS_CONTROL T6 " + 
						"  ON ( T1.COUNTRY = T6.COUNTRY AND T1.LE_BOOK = T6.LE_BOOK AND T1.FEED_ID = T6.FEED_ID AND T1.SESSION_ID = T6.SESSION_ID) " + 
						"  where T1.feed_id = ? AND T1.SESSION_ID = ? " + 
						"  UNION " + 
						"  SELECT T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.FEED_NAME, T1.SESSION_ID, T1.FEED_CATEGORY, T5.COMPLETION_STATUS MAIN_TABLE_STATUS, " + 
						"  T6.COMPLETION_STATUS PROCESS_CONTROL_STATUS, T1.COUNTRY PARENT_COUNTRY, T1.LE_BOOK PARENT_LE_BOOK, " + 
						getDbFunction(Constants.NVL, null)+"(T2.FEED_ID,T1.DEPENDENT_FEED_ID) PARENT_FEED_ID, " + 
						getDbFunction(Constants.NVL, null)+"(T2.FEED_NAME, T3.FEED_NAME) PARENT_FEED_NAME, " + 
						getDbFunction(Constants.NVL, null)+"(T2.SESSION_ID,0) PARENT_SESSION_ID, " + 
						getDbFunction(Constants.NVL, null)+"(T2.FEED_CATEGORY,T3.FEED_CATEGORY) PARENT_FEED_CATEGORY, " + 
						"  T3.COMPLETION_STATUS PARENT_MAIN_TABLE_STATUS, " + 
						getDbFunction(Constants.NVL, null)+"(T4.COMPLETION_STATUS,'P') PARENT_PROCESS_CONTROL_STATUS " + 
						"  FROM " + 
						"  (SELECT distinct T1.COUNTRY, T1.LE_BOOK, T1.FEED_ID, T1.CATEGORY_ID FEED_CATEGORY, T1.DEPENDENT_CATEGORY, T2.FEED_NAME, T2.SESSION_ID , T3.FEED_ID DEPENDENT_FEED_ID" + 
						"      FROM ETL_FEED_CATEGORY_DEP_PRS T1 " + 
						"      INNER JOIN ETL_FEED_MAIN_PRS T2 " + 
						"      ON ( T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.FEED_ID = T2.FEED_ID and t1.CATEGORY_ID = t2.FEED_CATEGORY AND T1.SESSION_ID = T2.SESSION_ID)" + 
						"  	INNER JOIN ETL_FEED_MAIN T3 ON ( T1.COUNTRY = T3.COUNTRY AND T1.LE_BOOK = T3.LE_BOOK AND T1.DEPENDENT_CATEGORY = T3.FEED_CATEGORY )" + 
						"  	) T1 " + 
						/*"  LEFT JOIN ETL_FEED_MAIN_PRS T2 " + 
						"  ON ( T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK AND T1.DEPENDENT_FEED_ID = T2.FEED_ID ) " + */
						"  left JOIN ETL_FEED_PROCESS_CONTROL T7 "+
						"  ON ( T1.COUNTRY = T7.COUNTRY AND T1.LE_BOOK = T7.LE_BOOK AND T1.DEPENDENT_CATEGORY = T7.FEED_CATEGORY ) "+
						"  left JOIN ETL_FEED_MAIN_PRS T2 "+
						"  ON ( T7.COUNTRY = T2.COUNTRY AND T7.LE_BOOK = T2.LE_BOOK AND T7.FEED_CATEGORY = T2.FEED_CATEGORY and t7.SESSION_ID = t2.SESSION_ID) "+
						"  LEFT JOIN ETL_FEED_MAIN T3 " + 
						"  ON ( T3.COUNTRY = T1.COUNTRY AND T3.LE_BOOK = T1.LE_BOOK AND T3.FEED_ID = T1.DEPENDENT_FEED_ID)  " + 
						"  LEFT JOIN ETL_FEED_PROCESS_CONTROL T4 " + 
						"  ON ( T2.COUNTRY = T4.COUNTRY AND T2.LE_BOOK = T4.LE_BOOK AND T2.FEED_ID = T4.FEED_ID AND T2.SESSION_ID = T4.SESSION_ID) " + 
						"  INNER JOIN ETL_FEED_MAIN T5 " + 
						"  ON ( T1.COUNTRY = T5.COUNTRY AND T1.LE_BOOK = T5.LE_BOOK AND T1.FEED_ID = T5.FEED_ID) " + 
						"  INNER JOIN ETL_FEED_PROCESS_CONTROL T6 " + 
						"  ON ( T1.COUNTRY = T6.COUNTRY AND T1.LE_BOOK = T6.LE_BOOK AND T1.FEED_ID = T6.FEED_ID AND T1.SESSION_ID = T6.SESSION_ID) " + 
						"  WHERE T1.FEED_ID = ? AND T1.SESSION_ID = ? ";
			} else {
				return exceptionCode;
			}
					
			Object args[] = { vObject.getFeedId(), vObject.getSessionId(), vObject.getFeedId(),
					vObject.getSessionId() };
			LinkedHashMap<String, ConsoleDependencyFeedVb> returnFeedDetailsMap = jdbcTemplate.query(sql, args,
					dependencyDetailsRSE(dependencyFeedDetailsMap, vObject));
			if (returnFeedDetailsMap == null) {
				String key = vObject.getFeedId() + "-" + vObject.getSessionId();
				if (dependencyFeedDetailsMap.get(key) == null) {
					dependencyFeedDetailsMap.put(key, vObject);
				}
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(returnFeedDetailsMap);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	/*
	 * dependencyFeedDetailsMap -> Takes dependencyFeedDetailsMap as input. Any new
	 * record of dependency will be appended into this map in addition to already
	 * existing records
	 */
	private ResultSetExtractor<LinkedHashMap<String, ConsoleDependencyFeedVb>> dependencyDetailsRSE(
			LinkedHashMap<String, ConsoleDependencyFeedVb> dependencyFeedDetailsMap,
			ConsoleDependencyFeedVb currentFeedVb) {
		final String HIPHEN = "-";
		final String COMMA = ",";
		LinkedHashMap<String, ConsoleDependencyFeedVb> returnFeedDetailsMap = new LinkedHashMap<String, ConsoleDependencyFeedVb>();
		/*
		 * key -> It is the combination of Feed_ID & Session_ID of the current feed for
		 * which we are trying to find the parents for if "dependencyFeedDetailsMap" has
		 * existing entry with the "key", it means it should just update the
		 * "parentUniqIdCollection"
		 */
		String key = currentFeedVb.getFeedId() + HIPHEN + currentFeedVb.getSessionId();
		ResultSetExtractor<LinkedHashMap<String, ConsoleDependencyFeedVb>> rse = new ResultSetExtractor<LinkedHashMap<String, ConsoleDependencyFeedVb>>() {
			@Override
			public LinkedHashMap<String, ConsoleDependencyFeedVb> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				/*
				 * isRecordAvailable -> This boolean flag helps in identifying if there is any
				 * new parent record currosponding to current query execution
				 * "isRecordAvailable" helps in breaking infinite loop in case if the feeds are
				 * configured in a dead-lock manner
				 */
//				boolean isRecordAvailable = false;

				while (rs.next()) {
					/*
					 * currentParentUniqIdCollection -> It is the comma separated string with
					 * combination of Parent_Feed_ID & Parent_Session_ID. This combination helps in
					 * identifying the exact parent node to which this current node is linked to.
					 */
					String currentParentUniqIdCollection = rs.getString("PARENT_FEED_ID") + HIPHEN
							+ rs.getString("PARENT_SESSION_ID");
					if (dependencyFeedDetailsMap.get(key) != null) {
						ConsoleDependencyFeedVb depDetailsVb = dependencyFeedDetailsMap.get(key);
						/*
						 * Make sure that the current currentParentUniqIdCollection is not already
						 * present in the "depDetailsVb.getParentUniqIdCollection()" object to avoid
						 * dead-lock If the currentParentUniqIdCollection value is already present, then
						 * we must skip appending the currentParentUniqIdCollection to the
						 * "parentUniqIdCollection" object of returnFeedDetailsMap
						 */
						if (!(depDetailsVb.getParentUniqIdCollection()).contains(currentParentUniqIdCollection)) {
//							isRecordAvailable = true;

							// Collect details of Parent Feed
							ConsoleDependencyFeedVb parentDetailsVb = new ConsoleDependencyFeedVb();
							parentDetailsVb.setCountry(rs.getString("PARENT_COUNTRY"));
							parentDetailsVb.setLeBook(rs.getString("PARENT_LE_BOOK"));
							parentDetailsVb.setFeedId(rs.getString("PARENT_FEED_ID"));
							parentDetailsVb.setFeedName(rs.getString("PARENT_FEED_NAME"));
							parentDetailsVb.setSessionId(rs.getString("PARENT_SESSION_ID"));
							parentDetailsVb.setFeedCategory(rs.getString("PARENT_FEED_CATEGORY"));
							parentDetailsVb.setMainTableStatus(rs.getString("PARENT_MAIN_TABLE_STATUS"));
							parentDetailsVb.setProcessControlStatus(rs.getString("PARENT_PROCESS_CONTROL_STATUS"));

							// Add the above Parent Feed to the Parent List of Current Feed
							depDetailsVb.getParentFeedList().add(parentDetailsVb);

							depDetailsVb.setParentUniqIdCollection(
									depDetailsVb.getParentUniqIdCollection() + COMMA + currentParentUniqIdCollection);
							returnFeedDetailsMap.put(key, depDetailsVb);
							dependencyFeedDetailsMap.put(key, depDetailsVb);
						}
					} else {
//						isRecordAvailable = true;
						ConsoleDependencyFeedVb depDetailsVb = new ConsoleDependencyFeedVb();
						depDetailsVb.setCountry(rs.getString("COUNTRY"));
						depDetailsVb.setLeBook(rs.getString("LE_BOOK"));
						depDetailsVb.setFeedId(rs.getString("FEED_ID"));
						depDetailsVb.setFeedName(rs.getString("FEED_NAME"));
						depDetailsVb.setSessionId(rs.getString("SESSION_ID"));
						depDetailsVb.setFeedCategory(rs.getString("FEED_CATEGORY"));
						depDetailsVb.setMainTableStatus(rs.getString("MAIN_TABLE_STATUS"));
						depDetailsVb.setProcessControlStatus(rs.getString("PROCESS_CONTROL_STATUS"));
						depDetailsVb.setParentUniqIdCollection(currentParentUniqIdCollection);

						// Collect details of Parent Feed
						ConsoleDependencyFeedVb parentDetailsVb = new ConsoleDependencyFeedVb();
						parentDetailsVb.setCountry(rs.getString("PARENT_COUNTRY"));
						parentDetailsVb.setLeBook(rs.getString("PARENT_LE_BOOK"));
						parentDetailsVb.setFeedId(rs.getString("PARENT_FEED_ID"));
						parentDetailsVb.setFeedName(rs.getString("PARENT_FEED_NAME"));
						parentDetailsVb.setSessionId(rs.getString("PARENT_SESSION_ID"));
						parentDetailsVb.setFeedCategory(rs.getString("PARENT_FEED_CATEGORY"));
						parentDetailsVb.setMainTableStatus(rs.getString("PARENT_MAIN_TABLE_STATUS"));
						parentDetailsVb.setProcessControlStatus(rs.getString("PARENT_PROCESS_CONTROL_STATUS"));

						// Add the detail of Parent Feed to the Parent List of Current Feed
						depDetailsVb.setParentFeedList(
								new ArrayList<ConsoleDependencyFeedVb>(Arrays.asList(parentDetailsVb)));

						returnFeedDetailsMap.put(key, depDetailsVb);
						dependencyFeedDetailsMap.put(key, depDetailsVb);
					}
				}
				/*
				 * If the current feed detail cannot fetch any record for above query - it means
				 * it has no parents. Hence, simply add the current ConsoleDependencyFeedVb to
				 * the "dependencyFeedDetailsMap" as a node without parent
				 */
				/*
				 * if(!isRecordAvailable && returnFeedDetailsMap.get(key) == null) {
				 * returnFeedDetailsMap.put(key, currentFeedVb); }
				 */
//				return (isRecordAvailable)?returnFeedDetailsMap:null;
//				return returnFeedDetailsMap;
				return (returnFeedDetailsMap.size() > 0) ? returnFeedDetailsMap : null;
			}
		};
		/*
		 * if(!flagCls.flag && dependencyFeedDetailsMap.get(key) == null) {
		 * System.out.println("F key:"+key); dependencyFeedDetailsMap.put(key,
		 * currentFeedVb); }
		 */
		return rse;
	}
	
	public ExceptionCode performAlertPush (ETLFeedProcessControlVb processControlVb, String eventType) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			boolean alertFlag = isAlertEnabled(processControlVb, eventType);
			if(alertFlag) {
				exceptionCode = getAlertContentConfigMap(processControlVb, eventType);
				if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					Map<String, String> alertContentConfigMap = (Map<String, String>) exceptionCode.getResponse();
					insertIntoAlertContent(processControlVb, eventType, alertContentConfigMap);
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	private boolean isAlertEnabled(ETLFeedProcessControlVb processControlVb, String eventType) {
		boolean returnFlag = false;
		try {
			ETLAlertVb alertVb = processControlVb.getAlertVbMap().get(eventType);
			if(ValidationUtil.isValid(alertVb.getCondition())) {
				
				/*CONDITION -> This is the string that will be use based on the "CONDITION_EXECUTION_TYPE" */
				/*CONDITION_EXECUTION_TYPE -> The value of "CONDITION" will be executed if CONDITION_EXECUTION_TYPE is "SQL" or considered as a method name if CONDITION_EXECUTION_TYPE is "RT_VAR"  */
				/*CONDITION_VALIDATION_TYPE -> Validation based on what? Record Count /Boolean/ Binary/ Constant based? */
				/*CONDITION_PASS_VALUE -> The result of executing the "CONDITION" will be compared against this "pass-value" based on the "CONDITION_VALID_CRITERIA" */
				/*CONDITION_VALID_CRITERIA -> This is the criteria based on which the CONDITION_PASS_VALUE will be compared with the execution result of CONDITION */
				
				if (ValidationUtil.isValid(alertVb.getConditionValidationType()) 
						&& ValidationUtil.isValid(alertVb.getConditionPassValue()) 
						&& ValidationUtil.isValid(alertVb.getConditionValidCriteria())) {
					String value = "";
					String condition = replaceMacroVarExtra(processControlVb, alertVb.getCondition());
					String passValue = replaceMacroVarExtra(processControlVb, alertVb.getConditionPassValue());
					if("SQL".equalsIgnoreCase(alertVb.getConditionExecutionType())) /*Compare with SQL output*/ {
						if ("REC_COUNT".equalsIgnoreCase(alertVb.getConditionValidationType())) {
							RowCountCallbackHandler rcbh = new RowCountCallbackHandler();
							getJdbcTemplate().query(condition, rcbh);
							int rowCount = rcbh.getRowCount();
							value = String.valueOf(rowCount);
						} else if ("BINARY".equalsIgnoreCase(alertVb.getConditionValidationType())
								|| "BOOLEAN".equalsIgnoreCase(alertVb.getConditionValidationType())
								|| "CONSTANT".equalsIgnoreCase(alertVb.getConditionValidationType())) {
							value = getJdbcTemplate().query(condition, new ResultSetExtractor<String>() {
								@Override
								public String extractData(ResultSet rs) throws SQLException, DataAccessException {
									if(rs.next()) {
										return rs.getString(1);
									} else {
										return null;
									}
								}
							});
						} else {
							value = null;
						}
					} else if("RT_VAR".equalsIgnoreCase(alertVb.getConditionExecutionType())) /*Compare with Real Time Variable*/ {
						String methodName = "get"+condition.substring(0, 1).toUpperCase()+condition.substring(1);
						Method mth = processControlVb.getClass().getDeclaredMethod(methodName);
						Object obj = mth.invoke(processControlVb);
						if(obj!=null)
							value = String.valueOf(obj);
					} else {
						value = null;
					}
					if(ValidationUtil.isValid(value)) {
						returnFlag = validateCondition(processControlVb, value,
								alertVb.getConditionValidCriteria(), passValue);
					} else {
						returnFlag = false;
					}
				} else {
					returnFlag = false;
				}
			} else {
				returnFlag = true;
			}
			return returnFlag;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			returnFlag = false;
		}
		return returnFlag;
	}
	
	private boolean validateCondition(ETLFeedProcessControlVb processControlVb, String value, String criteria, String passValue) {
		boolean returnFlag = false;
		try {
			switch(criteria) {
			case ">":
				returnFlag = (Integer.parseInt(value) > Integer.parseInt(passValue))?true:false;
				break;
			case "<":
				returnFlag = (Integer.parseInt(value) < Integer.parseInt(passValue))?true:false;
				break;
			case "=":
				returnFlag = (value.equalsIgnoreCase(passValue))?true:false;
				break;
			case ">=":
				returnFlag = (Integer.parseInt(value) >= Integer.parseInt(passValue))?true:false;
				break;
			case "<=":
				returnFlag = (Integer.parseInt(value) <= Integer.parseInt(passValue))?true:false;
				break;
			default:
				break;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return returnFlag;
	}
	
	public int updateFeedMainTableStatusForCSByCategory(OperationalConsoleVb vObject, String tableType, String fStatus) {
		String table = "ETL_FEED_MAIN";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_MAIN_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_MAIN_PEND";
		}
		String appendDateCol = "";
		if (ValidationUtil.isValid(vObject.getLastExtractionDate())) {
			appendDateCol = " , LAST_EXTRACTION_DATE = '" + vObject.getLastExtractionDate() + "' ";
		}
		String query = "update " + table + " set COMPLETION_STATUS=? " + appendDateCol
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  and FEED_CATEGORY = ?";
		Object[] args = { fStatus, vObject.getCountry(), vObject.getLeBook(), 
				vObject.getFeedCategory() };
		return getJdbcTemplate().update(query, args);
	}
	
	protected int updateETLFeedScheduleConfigProcessTblByCategory(OperationalConsoleVb vObject, String columnName, String value) {
		
		String query = "update ETL_FEED_SCHEDULE_CONFIG set "+columnName+"=?  WHERE EXISTS (  SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_SCHEDULE_CONFIG.FEED_ID  ) ";
		
		Object[] args = { value, vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public List<OperationalConsoleVb> findFeedTypeByCustomFilter(String val) throws DataAccessException {
		String sql = " SELECT ALPHA_SUB_TAB FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2102 and UPPER(ALPHA_SUBTAB_DESCRIPTION) " + val;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb opVb = new OperationalConsoleVb();
				opVb.setFeedType(rs.getString("ALPHA_SUB_TAB"));
				return opVb;
			}
		};
		return getJdbcTemplate().query(sql, mapper);
	}
	
	public List<OperationalConsoleVb> findCompletionStatusByCustomFilter(String val) throws DataAccessException {
		String sql = " SELECT ALPHA_SUB_TAB FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2112 and UPPER(ALPHA_SUBTAB_DESCRIPTION) " + val;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				OperationalConsoleVb opVb = new OperationalConsoleVb();
				opVb.setCompletionStatus(rs.getString("ALPHA_SUB_TAB"));
				return opVb;
			}
		};
		return getJdbcTemplate().query(sql, mapper);
	}
	
	private String readTxtFromFile(ChannelSftp sftpChannel, String fileName) throws SftpException, Exception {
		InputStream ins = sftpChannel.get(fileName);
		StringBuilder textBuilder = new StringBuilder();
		try (Reader reader = new BufferedReader(
				new InputStreamReader(ins, Charset.forName(StandardCharsets.UTF_8.name())))) {
			int c = 0;
			while ((c = reader.read()) != -1) {
				textBuilder.append((char) c);
			}
		}
		return textBuilder.toString().trim();
	}
	
	private String readTxtFromFile(String fileName) throws IOException {
		StringBuilder textBuilder = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
			String line;
			while ((line = reader.readLine()) != null) {
				textBuilder.append(line).append(System.lineSeparator());
			}
		}
		return textBuilder.toString().trim();
	}
	
	private void recursiveFolderDelete(ChannelSftp channelSftp, String path) throws SftpException {
		Collection<ChannelSftp.LsEntry> fileAndFolderList = channelSftp.ls(path);
		for (ChannelSftp.LsEntry item : fileAndFolderList) {
			if (!item.getAttrs().isDir()) {
				channelSftp.rm(path + "/" + item.getFilename());
			} else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
				try {
					channelSftp.rmdir(path + "/" + item.getFilename());
				} catch (Exception e) { 
					recursiveFolderDelete(channelSftp, path + "/" + item.getFilename());
				}
			}
		}
		channelSftp.rmdir(path); // delete the parent directory after empty
	}

	@SuppressWarnings("unchecked")
	private String[] getFolderList(ChannelSftp sftpChannel, String filePattern) throws SftpException {
		Vector<ChannelSftp.LsEntry> folders = sftpChannel.ls(".");
		List<String> matchingFolders = new ArrayList<>();
		for (ChannelSftp.LsEntry folder : folders) {
			String folderName = folder.getFilename();
			Pattern pattern = Pattern.compile(filePattern.replace("*", "(.*?)"));
			Matcher matcher = pattern.matcher(folderName);
			if (matcher.find()) {
				matchingFolders.add(folderName);
			}
		}
		return matchingFolders.toArray(new String[0]);
	}

	private String[] getFolderList(File baseDir, String filePattern) {
		List<String> matchingFolders = new ArrayList<>();
		Pattern pattern = Pattern.compile(filePattern.replace("*", "(.*?)"));
		File[] folders = baseDir.listFiles();
		if (folders != null) {
			for (File folder : folders) {
				String folderName = folder.getName();
				Matcher matcher = pattern.matcher(folderName);
				if (matcher.find()) {
					matchingFolders.add(folderName);
				}
			}
		}
		return matchingFolders.toArray(new String[0]);
	}
	
	public static void recursiveFolderDelete(String path) {
		try {
			Stream<Path> fileAndFolderList = Files.list(Paths.get(path));
			fileAndFolderList.forEach(filePath -> {
				try {
					if (Files.isDirectory(filePath)) {
						recursiveFolderDelete(filePath.toString());
					} else {
						Files.delete(filePath);
					}
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			});
			Files.delete(Paths.get(path));
			fileAndFolderList.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public List<OperationalConsoleVb> getAllChildDataForReinitiate(OperationalConsoleVb dObj) {
		if(!ValidationUtil.isValid(dObj.getSessionId())) {
			return null;
		}
		StringBuffer strBufQuery = new StringBuffer("select T1.SESSION_ID, T1.COUNTRY ,T1.LE_BOOK "
				+ " ,T1.FEED_ID  ," + getDbFunction(Constants.DATEFUNC, null) + "(T1.PROCESS_DATE, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + "') PROCESS_DATE, T1.PROCESS_DATE PROCESS_DATE_OB, T2.LAST_EXTRACTION_DATE"
				+ " ,T1.NEXT_SCHEDULE_DATE ,T1.FEED_ID " + getDbFunction(Constants.PIPELINE, null)
				+ " ' - ' " + getDbFunction(Constants.PIPELINE, null) + " T2.FEED_NAME FEED_NAME, T2.FEED_NAME FEED_NAME_OB "
				+ " ,T4.FEED_TYPE  ,T4.BATCH_SIZE , (SELECT ALPHA_SUBTAB_DESCRIPTION "
				+ " FROM ALPHA_SUB_TAB  WHERE ALPHA_TAB = 2102 AND ALPHA_SUB_TAB = T4.FEED_TYPE) FEED_TYPE_DESC, "
				+ " T1.FEED_CATEGORY,  T1.ITERATION_COUNT,  T1.COMPLETION_STATUS, "
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION  FROM ALPHA_SUB_TAB "
				+ " WHERE ALPHA_TAB = 2113 AND ALPHA_SUB_TAB = T1.COMPLETION_STATUS) COMPLETION_STATUS_DESC, "
				+ " T1.CURRENT_PROCESS,  (SELECT ALPHA_SUBTAB_DESCRIPTION  FROM ALPHA_SUB_TAB "
				+ " WHERE ALPHA_TAB = 2112 AND ALPHA_SUB_TAB = T1.CURRENT_PROCESS) CURRENT_PROCESS_desc, "
				//+ " T1.START_TIME, T1.END_TIME, (T1.END_TIME - T1.START_TIME)/(1000*60) PROCESS_DURATION_MINS,"
				+ " T1.START_TIME, T1.END_TIME, "
				+ " T1.DEBUG_FLAG, T1.VERSION_NUMBER,T3.TIME_ZONE FROM ETL_FEED_PROCESS_CONTROL T1 "
				+ " INNER JOIN VISION_BUSINESS_DAY T3  ON (T3.COUNTRY = T1.COUNTRY AND T3.LE_BOOK = T1.LE_BOOK)"
				+ " INNER JOIN ETL_FEED_MAIN T2 "
				+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK and T1.FEED_ID=T2.FEED_ID AND T2.FEED_STATUS=0)"
				+ " INNER JOIN ETL_FEED_MAIN_PRS T4 "
				+ " ON (T1.COUNTRY = T4.COUNTRY AND T1.LE_BOOK = T4.LE_BOOK and T1.FEED_ID=T4.FEED_ID AND T1.SESSION_ID=T4.SESSION_ID)"
				+ " WHERE APPLICATION_ID='ETL'");
		StringBuffer strBufApprove = new StringBuffer(strBufQuery.toString());
		try {
			if (ValidationUtil.isValid(dObj.getSessionId())) {
				strBufApprove.append("AND (T1.SESSION_ID = '" + dObj.getSessionId() + "' ) ");
			}
			if (ValidationUtil.isValid(dObj.getFeedCategory())) {
				strBufApprove.append("AND (T1.FEED_CATEGORY IN ('" + dObj.getFeedCategory() + "')) ");
			}
			String orderBy = " ORDER BY COUNTRY, LE_BOOK, PROCESS_DATE_OB, FEED_NAME_OB ";
			return getJdbcTemplate().query(strBufApprove.toString()+orderBy, getGrid2Mapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	
	public List<DependencyFeedVb> getAllFeedLvlDependencyMaintenance() {
		StringBuffer strBufQuery = new StringBuffer("SELECT T1.FEED_ID, T2.FEED_NAME, T1.DEPENDENT_FEED_ID, "
				+ " (SELECT FEED_NAME from ETL_FEED_MAIN T where T1.DEPENDENT_FEED_ID = T.FEED_ID) D_FEED_NAME"
				+ " from ETL_FEED_DEPENDENCIES T1 INNER JOIN ETL_FEED_MAIN T2 "
				+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK"
				+ " AND T1.FEED_ID = T2.FEED_ID AND T1.FEED_ID != T1.DEPENDENT_FEED_ID "
//				+ " AND FEED_DEP_STATUS = 0"
				+ ") order by FEED_NAME");

		return getJdbcTemplate().query(strBufQuery.toString(), new ResultSetExtractor<List<DependencyFeedVb>>() {
			@Override
			public List<DependencyFeedVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<DependencyFeedVb> dependencyFeedList = new ArrayList<>();
				while (rs.next()) {
					DependencyFeedVb vb = new DependencyFeedVb();
					vb.setFeedId(rs.getString("FEED_ID"));
					vb.setFeedName(rs.getString("FEED_NAME"));
					vb.setDependentFeedId(rs.getString("DEPENDENT_FEED_ID"));
					vb.setDependentFeedName(rs.getString("D_FEED_NAME"));
					dependencyFeedList.add(vb);
				}
				return dependencyFeedList;
			}
		});
	}
	
	public List<DependencyCategoryVb> getAllCatLvlDependencyMaintenance() {
		StringBuffer strBufQuery = new StringBuffer(
				"SELECT T1.CATEGORY_ID, T2.CATEGORY_DESCRIPTION, T1.DEPENDENT_CATEGORY,"
						+ " (SELECT CATEGORY_DESCRIPTION from ETL_FEED_CATEGORY T where T1.DEPENDENT_CATEGORY = T.CATEGORY_ID) D_CATEGORY_NAME"
						+ " from ETL_FEED_CATEGORY_DEPENDENCIES T1 INNER JOIN ETL_FEED_CATEGORY T2 "
						+ " ON (T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK"
						+ " AND T1.CATEGORY_ID = T2.CATEGORY_ID AND T1.CATEGORY_ID != T1.DEPENDENT_CATEGORY AND T1.CATEGORY_DEP_STATUS = 0)"
//						+ " AND CATEGORY_DESCRIPTION like '%" + dObj.getFeedCategory() + "%')"
						+ " ORDER BY CATEGORY_DESCRIPTION");
		return getJdbcTemplate().query(strBufQuery.toString(), new ResultSetExtractor<List<DependencyCategoryVb>>() {
			@Override
			public List<DependencyCategoryVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<DependencyCategoryVb> dependencyCategoryList = new ArrayList<>();
				while (rs.next()) {
					DependencyCategoryVb vb = new DependencyCategoryVb();
					vb.setCategoryId(rs.getString("CATEGORY_ID"));
					vb.setCategoryDesc(rs.getString("CATEGORY_DESCRIPTION"));
					vb.setDependentCategoryId(rs.getString("DEPENDENT_CATEGORY"));
					vb.setDependentCategoryName(rs.getString("D_CATEGORY_NAME"));
					dependencyCategoryList.add(vb);
				}
				return dependencyCategoryList;
			}
		});
	}
	
	public List<DependencyCategoryVb> getCategoryGroupedStatus(){
		StringBuffer strBufQuery = new StringBuffer("SELECT  Country, LE_Book, FEED_CATEGORY,"
				+ " CASE  WHEN COUNT(*) = SUM(CASE WHEN COMPLETION_STATUS = 'S' THEN 1 ELSE 0 END) THEN 'Success' "
				+ " ELSE 'Error'   END AS Grouped_Category_Status "
				+ " FROM ETL_FEED_PROCESS_CONTROL WHERE COMPLETION_STATUS != 'S' GROUP BY Country, LE_Book, FEED_CATEGORY ");
		return getJdbcTemplate().query(strBufQuery.toString(), new ResultSetExtractor<List<DependencyCategoryVb>>() {
			@Override
			public List<DependencyCategoryVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<DependencyCategoryVb> categoryLvlGroupedStatus = new ArrayList<>();
				while (rs.next()) {
					DependencyCategoryVb vb = new DependencyCategoryVb();
					vb.setCountry(rs.getString("Country"));
					vb.setLeBook(rs.getString("LE_Book"));
					vb.setCategoryId(rs.getString("FEED_CATEGORY"));
					vb.setGroupedCategoryStatus(rs.getString("Grouped_Category_Status"));
					categoryLvlGroupedStatus.add(vb);
				}
				return categoryLvlGroupedStatus;
			}
		});
	}
	
	public List<DependencyFeedVb> getFeedGroupedStatus(){
		StringBuffer strBufQuery = new StringBuffer("SELECT Feed_ID, "
				+ " CASE  WHEN COUNT(*) = SUM(CASE WHEN COMPLETION_STATUS = 'S' THEN 1 ELSE 0 END) THEN 'Success' "
				+ " ELSE 'Error' END AS Grouped_Status FROM ETL_FEED_PROCESS_CONTROL "
				+ " WHERE COMPLETION_STATUS!='S' "
				+ " GROUP BY Feed_ID, FEED_CATEGORY ");
		return getJdbcTemplate().query(strBufQuery.toString(), new ResultSetExtractor<List<DependencyFeedVb>>() {
			@Override
			public List<DependencyFeedVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<DependencyFeedVb> feedLvlGroupedStatus = new ArrayList<>();
				while(rs.next()) {
					DependencyFeedVb vb = new DependencyFeedVb();
					vb.setFeedId(rs.getString("Feed_ID"));
					vb.setGroupedStatus(rs.getString("Grouped_Status"));
					feedLvlGroupedStatus.add(vb);
				}
				return feedLvlGroupedStatus;
			}
		});
	}
	
	public List<OperationalConsoleVb> getAllFeedLvlStatusFromPRS_CtrlTable(){
		String sql = "SELECT  Country, LE_Book, FEED_CATEGORY, SESSION_ID, FEED_ID, "
				 + " COMPLETION_STATUS, CURRENT_PROCESS "
				 + " FROM ETL_FEED_PROCESS_CONTROL"
				 + " WHERE COMPLETION_STATUS!='S'";
		return getJdbcTemplate().query(sql, new ResultSetExtractor<List<OperationalConsoleVb>>() {
			@Override
			public List<OperationalConsoleVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<OperationalConsoleVb> feedLvlGroupedStatus = new ArrayList<>();
				while(rs.next()) {
					OperationalConsoleVb vb = new OperationalConsoleVb();
					vb.setCountry(rs.getString("COUNTRY"));
					vb.setLeBook(rs.getString("LE_BOOK"));
					vb.setFeedCategory(rs.getString("FEED_CATEGORY"));
					vb.setSessionId(rs.getString("SESSION_ID"));
					vb.setFeedId(rs.getString("FEED_ID"));
					vb.setCurrentProcess(rs.getString("CURRENT_PROCESS"));
					vb.setCompletionStatus(rs.getString("COMPLETION_STATUS"));
					feedLvlGroupedStatus.add(vb);
				}
				return feedLvlGroupedStatus;
			}
		});
	}
	
	public Set<String> getAllCatDependenciesParent(String categoryId, List<DependencyCategoryVb> catDepConfigList) {
        Set<String> dependencies = new HashSet<>();
        getAllCatDependenciesParentRecursive(categoryId, dependencies, catDepConfigList);
        return dependencies;
    }

    // Helper method to recursively find dependencies
	// Method to identify all the dependent parent corresponding to the category
    private void getAllCatDependenciesParentRecursive(String categoryId, Set<String> dependencies, List<DependencyCategoryVb> catDepConfigList) {
    	catDepConfigList.stream()
                .filter(dependency -> dependency.getCategoryId().equals(categoryId))
                .forEach(dependency -> {
                    String depCategoryId = dependency.getDependentCategoryId();
                    dependencies.add(depCategoryId);
                    getAllCatDependenciesParentRecursive(depCategoryId, dependencies, catDepConfigList);
                });
    }
    
	public String getDEPENDENCY_CHECK_FLAG(ConsoleDependencyFeedVb vb) {
		try {
			String strQueryAppr = "select DEPENDENCY_CHECK_FLAG from ETL_FEED_PROCESS_cONTROL" + " Where SESSION_ID= '"
					+ vb.getSessionId() + "'  AND FEED_ID = '" + vb.getFeedId().toUpperCase() + "' ";
			return getJdbcTemplate().queryForObject(strQueryAppr, String.class);
		} catch (Exception e) {
			return "D";
		}
	}

}
