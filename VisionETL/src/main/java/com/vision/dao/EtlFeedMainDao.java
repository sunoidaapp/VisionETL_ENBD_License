package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.ConnectorFileUploadMapperVb;
import com.vision.vb.DSConnectorVb;
import com.vision.vb.ETLFeedColumnsVb;
import com.vision.vb.ETLFeedTablesVb;
import com.vision.vb.EtlConnectorVb;
import com.vision.vb.EtlExtractionSummaryFieldsVb;
import com.vision.vb.EtlFeedAlertConfigVb;
import com.vision.vb.EtlFeedDestinationVb;
import com.vision.vb.EtlFeedInjectionConfigVb;
import com.vision.vb.EtlFeedLoadMappingVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedRelationVb;
import com.vision.vb.EtlFeedScheduleConfigVb;
import com.vision.vb.EtlFeedSourceVb;
import com.vision.vb.EtlFeedTranColumnVb;
import com.vision.vb.EtlFeedTranNodeVb;
import com.vision.vb.EtlFeedTranVb;
import com.vision.vb.EtlFeedTransformationMasterVb;
import com.vision.vb.EtlFeedTransformationVb;
import com.vision.vb.EtlForQueryReportVb;
import com.vision.vb.EtlTargetColumnsVb;
import com.vision.vb.EtlTransMasterInputSlabVb;
import com.vision.vb.EtlTransformedColumnsVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.RelationMapVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlFeedMainDao extends AbstractDao<EtlFeedMainVb> {

	@Autowired
	public EtlFileUploadAreaDao etlFileUploadAreaDao;

	@Autowired
	public EtlFileTableDao etlFileTableDao;

	@Autowired
	public EtlFileColumnsDao etlFileColumnsDao;

	@Autowired
	public EtlConnectorDao etlConnectorDao;

	@Autowired
	public EtlFeedCategoryDao etlFeedCategoryDao;

	@Autowired
	public EtlCategoryAlertconfigDao etlCategoryAlertconfigDao;

	@Autowired
	public EtlMqTableDao etlMqTableDao;

	@Autowired
	public EtlMqColumnsDao etlMqColumnsDao;

	@Autowired
	public EtlFeedSourceDao etlFeedSourceDao;

	@Autowired
	public EtlFeedTablesDao etlFeedTablesDao;

	@Autowired
	private VisionUsersDao visionUsersDao;

	@Autowired
	private NumSubTabDao numSubTabDao;

	@Autowired
	public EtlFeedColumnsDao etlFeedColumnsDao;

	@Autowired
	public EtlFeedRelationDao etlFeedRelationDao;

	@Autowired
	public EtlExtractionSummaryFieldsDao etlExtractionSummaryFieldsDao;

	@Autowired
	public EtlFeedTransformationDao etlFeedTransformationDao;

	@Autowired
	public EtlFeedTranColumnsDao etlFeedTranColumnsDao;

	@Autowired
	public EtlTransformedColumnsDao etlTransformedColumnsDao;

	@Autowired
	public EtlFeedDestinationDao etlFeedDestinationDao;

	@Autowired
	public EtlTargetColumnsDao etlTargetColumnsDao;

	@Autowired
	public EtlFeedLoadMappingDao etlFeedLoadMappingDao;

	@Autowired
	public EtlFeedInjectionConfigDao etlFeedInjectionConfigDao;

	@Autowired
	public EtlFeedScheduleConfigDao etlFeedScheduleConfigDao;

	@Autowired
	public EtlFeedAlertconfigDao etlFeedAlertconfigDao;
	
	@Autowired
	private AlphaSubTabDao alphaSubTabDao;

	@Value("${app.databaseType}")
	private String databaseType;

	@Autowired
	CommonDao commonDao;

	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";

	String categoryApprDesc = "(SELECT CATEGORY_DESCRIPTION FROM ETL_FEED_CATEGORY WHERE LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY AND  CATEGORY_ID = TAppr.FEED_CATEGORY) FEED_CATEGORY_DESC";
	String categoryPendDesc = "(SELECT CATEGORY_DESCRIPTION FROM ETL_FEED_CATEGORY WHERE LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY AND  CATEGORY_ID = TPend.FEED_CATEGORY) FEED_CATEGORY_DESC";

	String feedNameApprDesc = "(SELECT FEED_NAME FROM ETL_FEED_MAIN WHERE LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY AND  FEED_ID = TAppr.FEED_ID) FEED_NAME";

	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String FeedTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TAppr.FEED_TYPE",
			"FEED_TYPE_DESC");
	String FeedTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TPend.FEED_TYPE",
			"FEED_TYPE_DESC");

	String CompletionStatusAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2113,
			"TAppr.COMPLETION_STATUS", "COMPLETION_STATUS_DESC");
	String CompletionStatusAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2113,
			"TPend.COMPLETION_STATUS", "COMPLETION_STATUS_DESC");

	String FeedStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_STATUS",
			"FEED_STATUS_DESC");
	String FeedStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_STATUS",
			"FEED_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedMainVb vObject = new EtlFeedMainVb();
				vObject.setWriteFlag(
						ValidationUtil.isValid(rs.getString("WRITE_FLAG")) ? rs.getString("WRITE_FLAG") : "");
				if (rs.getString("COUNTRY") != null) {
					vObject.setCountry(rs.getString("COUNTRY"));
				} else {
					vObject.setCountry("");
				}
				if (rs.getString("COUNTRY_DESC") != null) {
					vObject.setCountryDesc(rs.getString("COUNTRY_DESC"));
				} else {
					vObject.setCountryDesc("");
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
				if (rs.getString("FEED_NAME") != null) {
					vObject.setFeedName(rs.getString("FEED_NAME"));
				} else {
					vObject.setFeedName("");
				}
				if (rs.getString("FEED_DESCRIPTION") != null) {
					vObject.setFeedDescription(rs.getString("FEED_DESCRIPTION"));
				} else {
					vObject.setFeedDescription("");
				}
				vObject.setBatchSize(rs.getInt("BATCH_SIZE"));
				/*if (rs.getString("LAST_EXTRACTION_DATE") != null) {
					vObject.setLastExtractionDate(rs.getString("LAST_EXTRACTION_DATE"));
				} else {
					vObject.setLastExtractionDate("");
				}*/
				if (ValidationUtil.isValid(rs.getString("LAST_EXTRACTION_DATE"))) {
					vObject.setTimeZone(
							getTimeZoneVisionBusinessDate(rs.getString("COUNTRY") + "-" + rs.getString("LE_BOOK")));
					vObject.setLastExtractionDate(printTimeFromMilliSecondsAsString(
							rs.getString("LAST_EXTRACTION_DATE"), vObject.getTimeZone()));
				} else {
					vObject.setLastExtractionDate("");
				}
				vObject.setFeedTypeAt(rs.getInt("FEED_TYPE_AT"));
				if (rs.getString("FEED_TYPE") != null) {
					vObject.setFeedType(rs.getString("FEED_TYPE"));
				} else {
					vObject.setFeedType("");
				}
				vObject.setCompletionStatusAt(rs.getInt("COMPLETION_STATUS_AT"));
				if (rs.getString("COMPLETION_STATUS") != null) {
					vObject.setCompletionStatus(rs.getString("COMPLETION_STATUS"));
				} else {
					vObject.setCompletionStatus("");
				}
				if (rs.getString("COMPLETION_STATUS_DESC") != null) {
					vObject.setCompletionStatusDesc(rs.getString("COMPLETION_STATUS_DESC"));
				} else {
					vObject.setCompletionStatusDesc("");
				}

				if (rs.getString("FEED_CATEGORY") != null) {
					vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
				} else {
					vObject.setFeedCategory("");
				}

				if (rs.getString("FEED_CATEGORY_DESC") != null) {
					vObject.setFeedCategoryDesc(rs.getString("FEED_CATEGORY_DESC"));
				} else {
					vObject.setFeedCategoryDesc("");
				}
				if (rs.getString("ALERT_MECHANISM") != null) {
					vObject.setAlertMechanism(rs.getString("ALERT_MECHANISM"));
				} else {
					vObject.setAlertMechanism("");
				}
				if (rs.getString("PRIVILAGE_MECHANISM") != null) {
					vObject.setPrivilageMechanism(rs.getString("PRIVILAGE_MECHANISM"));
				} else {
					vObject.setPrivilageMechanism("");
				}
				vObject.setFeedStatusNt(rs.getInt("FEED_STATUS_NT"));
				vObject.setFeedStatus(rs.getInt("FEED_STATUS"));
				vObject.setStatusDesc(rs.getString("FEED_STATUS_DESC"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
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
				vObject.setDistinctFlag(rs.getString("DISTINCT_FLAG"));
				return vObject;
			}
		};
		return mapper;
	}

	public String printTimeFromMilliSecondsAsString(String nextScheduleDate, String timeZone) {
		try {
			String output = nextScheduleDate;
			if (!nextScheduleDate.contains("-")) {
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
				Long timeInMilliSeconds = Long.parseLong(nextScheduleDate);
				Date now = new Date();
				now.setTime(timeInMilliSeconds);
				sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
				output = sdf.format(now.getTime());
			} else {
				if (nextScheduleDate.length() == 11 || nextScheduleDate.length() < 11) {
					nextScheduleDate = nextScheduleDate + " 00:00:00";
				} else {
					output="";
				}
			}
			return output;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return "";
		}
	}

	protected RowMapper getLocalMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedMainVb vObject = new EtlFeedMainVb();
				if (rs.getString("COUNTRY") != null) {
					vObject.setCountry(rs.getString("COUNTRY"));
				} else {
					vObject.setCountry("");
				}
				if (rs.getString("COUNTRY_DESC") != null) {
					vObject.setCountryDesc(rs.getString("COUNTRY_DESC"));
				} else {
					vObject.setCountryDesc("");
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
				if (rs.getString("FEED_NAME") != null) {
					vObject.setFeedName(rs.getString("FEED_NAME"));
				} else {
					vObject.setFeedName("");
				}
				if (rs.getString("FEED_DESCRIPTION") != null) {
					vObject.setFeedDescription(rs.getString("FEED_DESCRIPTION"));
				} else {
					vObject.setFeedDescription("");
				}
				vObject.setBatchSize(rs.getInt("BATCH_SIZE"));

				if (rs.getString("LAST_EXTRACTION_DATE") != null) {
					vObject.setLastExtractionDate(rs.getString("LAST_EXTRACTION_DATE"));
				} else {
					vObject.setLastExtractionDate("");
				}
				if (ValidationUtil.isValid(rs.getString("LAST_EXTRACTION_DATE"))) {
					vObject.setTimeZone(
							getTimeZoneVisionBusinessDate(rs.getString("COUNTRY") + "-" + rs.getString("LE_BOOK")));
					vObject.setLastExtractionDate(printTimeFromMilliSecondsAsString(
							rs.getString("LAST_EXTRACTION_DATE"), vObject.getTimeZone()));
				}
				vObject.setFeedTypeAt(rs.getInt("FEED_TYPE_AT"));
				if (rs.getString("FEED_TYPE") != null) {
					vObject.setFeedType(rs.getString("FEED_TYPE"));
				} else {
					vObject.setFeedType("");
				}
				vObject.setCompletionStatusAt(rs.getInt("COMPLETION_STATUS_AT"));
				if (rs.getString("COMPLETION_STATUS") != null) {
					vObject.setCompletionStatus(rs.getString("COMPLETION_STATUS"));
				} else {
					vObject.setCompletionStatus("");
				}
				if (rs.getString("COMPLETION_STATUS_DESC") != null) {
					vObject.setCompletionStatusDesc(rs.getString("COMPLETION_STATUS_DESC"));
				} else {
					vObject.setCompletionStatusDesc("");
				}
				if (rs.getString("FEED_CATEGORY") != null) {
					vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
				} else {
					vObject.setFeedCategory("");
				}

				if (rs.getString("FEED_CATEGORY_DESC") != null) {
					vObject.setFeedCategoryDesc(rs.getString("FEED_CATEGORY_DESC"));
				} else {
					vObject.setFeedCategoryDesc("");
				}
				if (rs.getString("ALERT_MECHANISM") != null) {
					vObject.setAlertMechanism(rs.getString("ALERT_MECHANISM"));
				} else {
					vObject.setAlertMechanism("");
				}
				if (rs.getString("PRIVILAGE_MECHANISM") != null) {
					vObject.setPrivilageMechanism(rs.getString("PRIVILAGE_MECHANISM"));
				} else {
					vObject.setPrivilageMechanism("");
				}
				vObject.setFeedStatusNt(rs.getInt("FEED_STATUS_NT"));
				vObject.setFeedStatus(rs.getInt("FEED_STATUS"));
				vObject.setStatusDesc(rs.getString("FEED_STATUS_DESC"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
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
				vObject.setDistinctFlag(rs.getString("DISTINCT_FLAG"));
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<EtlFeedMainVb> getQueryPopupResults(EtlFeedMainVb dObj) {
		Vector<Object> params = new Vector<Object>();
		setServiceDefaults();
		String accessQueyr = ", ETL_FEED_CATEGORY_ACCESS FCA, VISION_USERS VU "
				+ " WHERE (TAppr.FEED_CATEGORY =  FCA.CATEGORY_ID  AND TAppr.COUNTRY = FCA.COUNTRY "
				+ " AND TAppr.LE_BOOK = FCA.LE_BOOK  AND FCA.USER_GROUP = VU.USER_GROUP "
				+ " AND FCA.USER_PROFILE = VU.USER_PROFILE  AND VU.VISION_ID = " + intCurrentUserId + " ) ";

		StringBuffer strBufApprove = new StringBuffer();

		strBufApprove = new StringBuffer("Select FCA.WRITE_FLAG,TAppr.COUNTRY, " + CountryApprDesc + ",TAppr.LE_BOOK, "
				+ LeBookApprDesc + ",TAppr.FEED_ID,TAppr.FEED_NAME,TAppr.FEED_DESCRIPTION"
				+ ",TAppr.LAST_EXTRACTION_DATE,TAppr.BATCH_SIZE,TAppr.COMPLETION_STATUS_AT"
				+ ",TAppr.COMPLETION_STATUS, " + CompletionStatusAtApprDesc + ",TAppr.FEED_TYPE_AT"
				+ ",TAppr.FEED_TYPE, " + FeedTypeAtApprDesc + ",TAppr.FEED_CATEGORY, " + categoryApprDesc
				+ ",TAppr.ALERT_MECHANISM,TAppr.PRIVILAGE_MECHANISM,TAppr.FEED_STATUS_NT,TAppr.FEED_STATUS, "
				+ FeedStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR, "
				+ RecordIndicatorNtApprDesc + ",TAppr.MAKER, " + makerApprDesc + ",TAppr.VERIFIER, " + verifierApprDesc
				+ ",TAPPR.DATE_LAST_MODIFIED date_modified,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TAppr.DISTINCT_FLAG, 'APPROVE' DATA_FROM from ETL_FEED_MAIN TAppr " + accessQueyr);

		String strWhereNotExists = new String(" Not Exists (Select 'X' From ETL_FEED_MAIN_PEND TPend "
				+ "WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID )"
				+ " AND Not Exists (Select 'X' From ETL_FEED_MAIN_UPL TUpl "
				+ "WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID )");

		StringBuffer strBufPending = new StringBuffer();

		strBufPending = new StringBuffer("Select FCA.WRITE_FLAG,TPend.COUNTRY, " + CountryPendDesc + ",TPend.LE_BOOK, "
				+ LeBookTPendDesc + ",TPend.FEED_ID,TPend.FEED_NAME,TPend.FEED_DESCRIPTION"
				// + ",To_Char(TPend.LAST_EXTRACTION_DATE, 'DD-Mon-YYYY') LAST_EXTRACTION_DATE"
				+ ",TPend.BATCH_SIZE,TPend.LAST_EXTRACTION_DATE,TPend.COMPLETION_STATUS_AT"
				+ ",TPend.COMPLETION_STATUS, " + CompletionStatusAtPendDesc + ",TPend.FEED_TYPE_AT"
				+ ",TPend.FEED_TYPE, " + FeedTypeAtPendDesc + ",TPend.FEED_CATEGORY, " + categoryPendDesc
				+ ",TPend.ALERT_MECHANISM,TPend.PRIVILAGE_MECHANISM,TPend.FEED_STATUS_NT,TPend.FEED_STATUS, "
				+ FeedStatusNtPendDesc + ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR, "
				+ RecordIndicatorNtPendDesc + ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc
				+ ",TPEND.DATE_LAST_MODIFIED date_modified,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TPend.DISTINCT_FLAG, 'PEND' DATA_FROM  from ETL_FEED_MAIN_PEND TPend "
				+ accessQueyr.replaceAll("TAppr.", "TPend."));

		String strWhereNotExistsApprInUpl = new String(" Not Exists (Select 'X' From ETL_FEED_MAIN_UPL TUpl "
				+ "WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID) "
				+ " AND Not Exists (Select 'X' From ETL_FEED_MAIN_PEND TPend "
				+ "WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID )");

		String strWhereNotExistsPendInUpl = new String(" Not Exists (Select 'X' From ETL_FEED_MAIN_UPL TUpl "
				+ "WHERE TPend.COUNTRY = TUpl.COUNTRY AND TPend.LE_BOOK = TUpl.LE_BOOK AND TPend.FEED_ID = TUpl.FEED_ID) ");

		StringBuffer strBufUpl = new StringBuffer();

		strBufUpl = new StringBuffer("Select FCA.WRITE_FLAG,TUpl.COUNTRY, "
				+ CountryPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.LE_BOOK, "
				+ LeBookTPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.FEED_ID,TUpl.FEED_NAME,TUpl.FEED_DESCRIPTION"
				+ ",TUpl.BATCH_SIZE,TUpl.LAST_EXTRACTION_DATE,TUpl.COMPLETION_STATUS_AT,TUpl.COMPLETION_STATUS, "
				+ CompletionStatusAtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.FEED_TYPE_AT,TUpl.FEED_TYPE, "
				+ FeedTypeAtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.FEED_CATEGORY, "
				+ categoryPendDesc.replaceAll("TPend.", "TUpl.")
				+ ",TUpl.ALERT_MECHANISM,TUpl.PRIVILAGE_MECHANISM,TUpl.FEED_STATUS_NT,TUpl.FEED_STATUS, "
				+ FeedStatusNtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR, " + RecordIndicatorNtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.MAKER, "
				+ makerUplDesc + ",TUpl.VERIFIER, " + verifierUplDesc
				+ ",TUpl.DATE_LAST_MODIFIED date_modified,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TUpl.DISTINCT_FLAG, 'UPL' DATA_FROM  from ETL_FEED_MAIN_UPL TUpl "
				+ accessQueyr.replaceAll("TAppr.", "TUpl."));

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
						CommonUtils.addToQuery(" upper(TAppr.COUNTRY) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.COUNTRY) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.COUNTRY) " + val, strBufUpl);
						break;

					case "countryDesc":
						List<EtlFeedMainVb> countryData = findCountryByCustomFilter(val);
						String counData = "";
						if (countryData != null && countryData.size() > 0) {
							for (int k = 0; k < countryData.size(); k++) {
								String country = countryData.get(k).getCountry();
								if (!ValidationUtil.isValid(counData)) {
									counData = "'" + country + "'";
								} else {
									counData = counData + ",'" + country + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.COUNTRY) IN (" + counData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.COUNTRY) IN (" + counData + ") ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.COUNTRY) IN (" + counData + ") ", strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.COUNTRY) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.COUNTRY) IN ('') ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.COUNTRY) IN ('') ", strBufUpl);
						}
						break;
						
					case "leBook":
						CommonUtils.addToQuery(" upper(TAppr.LE_BOOK) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.LE_BOOK) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.LE_BOOK) " + val, strBufUpl);
						break;

					case "leBookDesc":
						List<EtlFeedMainVb> leBookData = findLeBookByCustomFilter(val);
						String lebData = "";
						if (leBookData != null && leBookData.size() > 0) {
							for (int k = 0; k < leBookData.size(); k++) {
								String Lebook = leBookData.get(k).getLeBook();
								if (!ValidationUtil.isValid(lebData)) {
									lebData = "'" + Lebook + "'";
								} else {
									lebData = lebData + ",'" + Lebook + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TAPPR.LE_BOOK) IN (" + lebData + ") ",
									strBufApprove);
							CommonUtils.addToQuery(" (TPend.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TPend.LE_BOOK) IN (" + lebData + ") ",
									strBufPending);
							CommonUtils.addToQuery(" (TUpl.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TUpl.LE_BOOK) IN (" + lebData + ") ",
									strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TAPPR.LE_BOOK) IN ('') ",
									strBufApprove);
							CommonUtils.addToQuery(" (TPend.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TPend.LE_BOOK) IN ('') ",
									strBufPending);
							CommonUtils.addToQuery(" (TUpl.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TUpl.LE_BOOK) IN ('') ",
									strBufUpl);
						}
						break;

					case "feedId":
						CommonUtils.addToQuery(" upper(TAppr.FEED_ID) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.FEED_ID) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.FEED_ID) " + val, strBufUpl);
						break;

					case "feedName":
						CommonUtils.addToQuery(" upper(TAppr.FEED_NAME) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.FEED_NAME) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.FEED_NAME) " + val, strBufUpl);
						break;

					case "feedDescription":
						CommonUtils.addToQuery(" upper(TAppr.FEED_DESCRIPTION) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.FEED_DESCRIPTION) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.FEED_DESCRIPTION) " + val, strBufUpl);
						break;

					case "feedType":
						CommonUtils.addToQuery(" upper(TAppr.FEED_TYPE) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.FEED_TYPE) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.FEED_TYPE) " + val, strBufUpl);
						break;

					case "completionStatus":
						CommonUtils.addToQuery(" upper(TAppr.COMPLETION_STATUS) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.COMPLETION_STATUS) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.COMPLETION_STATUS) " + val, strBufUpl);
						break;
						
					case "completionStatusDesc":
						List<AlphaSubTabVb> completionSTData = alphaSubTabDao.findAlphaSubTabByCustomFilterAndAlphaTab(val, 2113);
						String actData = "";
						if(completionSTData != null && completionSTData.size() > 0) {
							for (int k = 0; k < completionSTData.size(); k++) {
								String alphasubtab = completionSTData.get(k).getAlphaSubTab();
								if (!ValidationUtil.isValid(actData)) {
									actData = "'" + alphasubtab + "'";
								} else {
									actData = actData + ",'" + alphasubtab + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.COMPLETION_STATUS) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.COMPLETION_STATUS) IN (" + actData + ") ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.COMPLETION_STATUS) IN (" + actData + ") ", strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.COMPLETION_STATUS) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.COMPLETION_STATUS) IN ('') ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.COMPLETION_STATUS) IN ('') ", strBufUpl);
						}
						break;
				
					case "feedCategory":
						CommonUtils.addToQuery(" upper(TAppr.FEED_CATEGORY) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.FEED_CATEGORY) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.FEED_CATEGORY) " + val, strBufUpl);
						break;

					case "feedStatus":
						CommonUtils.addToQuery(" upper(TAppr.FEED_STATUS) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.FEED_STATUS) " + val, strBufPending);
						CommonUtils.addToQuery(" upper(TUpl.FEED_STATUS) " + val, strBufUpl);
						break;

					case "statusDesc":
						List<NumSubTabVb> numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 2000);
						actData = "";
						if (numSTData != null && numSTData.size() > 0) {
							for (int k = 0; k < numSTData.size(); k++) {
								int numsubtab = numSTData.get(k).getNumSubTab();
								if (!ValidationUtil.isValid(actData)) {
									actData = "'" + Integer.toString(numsubtab) + "'";
								} else {
									actData = actData + ",'" + Integer.toString(numsubtab) + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.FEED_STATUS) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.FEED_STATUS) IN (" + actData + ") ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.FEED_STATUS) IN (" + actData + ") ", strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.FEED_STATUS) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.FEED_STATUS) IN ('') ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.FEED_STATUS) IN ('') ", strBufUpl);
						}
						break;

					case "recordIndicator":
						CommonUtils.addToQuery(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending);
						CommonUtils.addToQuery("upper(TUpl.RECORD_INDICATOR) " + val, strBufUpl);
						break;

					case "recordIndicatorDesc":
						numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 7);
						actData = "";
						if (numSTData != null && numSTData.size() > 0) {
							for (int k = 0; k < numSTData.size(); k++) {
								int numsubtab = numSTData.get(k).getNumSubTab();
								if (!ValidationUtil.isValid(actData)) {
									actData = "'" + Integer.toString(numsubtab) + "'";
								} else {
									actData = actData + ",'" + Integer.toString(numsubtab) + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.RECORD_INDICATOR) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.RECORD_INDICATOR) IN (" + actData + ") ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.RECORD_INDICATOR) IN (" + actData + ") ", strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.RECORD_INDICATOR) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.RECORD_INDICATOR) IN ('') ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.RECORD_INDICATOR) IN ('') ", strBufUpl);
						}
						break;
						
					case "makerName":
						List<VisionUsersVb> muData = visionUsersDao.findUserIdByUserName(val);
						String actMData = "";
						if (muData != null && muData.size() > 0) {
							for (int k = 0; k < muData.size(); k++) {
								int visId = muData.get(k).getVisionId();
								if (!ValidationUtil.isValid(actMData)) {
									actMData = "'" + visId + "'";
								} else {
									actMData = actMData + ",'" + visId + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.MAKER) IN (" + actMData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.MAKER) IN (" + actMData + ") ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.MAKER) IN (" + actMData + ") ", strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.MAKER) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.MAKER) IN ('') ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.MAKER) IN ('') ", strBufUpl);
						}
						break;

					case "verifierName":
						muData = visionUsersDao.findUserIdByUserName(val);
						actMData = "";
						if (muData != null && muData.size() > 0) {
							for (int k = 0; k < muData.size(); k++) {
								int visId = muData.get(k).getVisionId();
								if (!ValidationUtil.isValid(actMData)) {
									actMData = "'" + Integer.toString(visId) + "'";
								} else {
									actMData = actMData + ",'" + Integer.toString(visId) + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.VERIFIER) IN (" + actMData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.VERIFIER) IN (" + actMData + ") ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.VERIFIER) IN (" + actMData + ") ", strBufUpl);
						} else {
							CommonUtils.addToQuery(" (TAPPR.VERIFIER) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.VERIFIER) IN ('') ", strBufPending);
							CommonUtils.addToQuery(" (TUpl.VERIFIER) IN ('') ", strBufUpl);
						}
						break;

					default:
						break;
					}
					count++;
				}
			}
			String orderBy = " Order By date_modified desc";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);

			/*
			 * if(dObj.isVerificationRequired()) lPageQuery.append(strBufPending.toString()
			 * + " Union " + strBufUpl.toString()+ " Union " + strBufApprove.toString());
			 * else lPageQuery.append(strBufApprove.toString() + " Union " +
			 * strBufUpl.toString());
			 */

			String col = "Select WRITE_FLAG, COUNTRY, COUNTRY_DESC, LE_BOOK, LE_BOOK_DESC, FEED_ID, FEED_NAME,"
					+ " FEED_DESCRIPTION,BATCH_SIZE, LAST_EXTRACTION_DATE, COMPLETION_STATUS_AT, COMPLETION_STATUS, "
					+ " COMPLETION_STATUS_DESC, FEED_TYPE_AT, FEED_TYPE, "
					+ "FEED_TYPE_DESC, FEED_CATEGORY, FEED_CATEGORY_DESC, "
					+ "ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, FEED_STATUS_DESC, "
					+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, RECORD_INDICATOR_DESC, MAKER, MAKER_NAME, "
					+ " VERIFIER, VERIFIER_NAME, DATE_MODIFIED, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,  DISTINCT_FLAG, DATA_FROM from (";
			StringBuffer lQuery1 = new StringBuffer();

			lQuery1.append(
					col + strBufPending.toString() + " ) TAPPR Union " + col + strBufUpl.toString() + ") TAPPR ");
			StringBuffer lQuery2 = new StringBuffer();
			lQuery2.append(
					col + strBufApprove.toString() + " ) TAPPR Union " + col + strBufUpl.toString() + ") TAPPR ");

			return getQueryPopupResults(dObj, lQuery1, lQuery2, strWhereNotExists, orderBy, params, getMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<EtlFeedMainVb> getQueryDetailResults(EtlFeedMainVb dObj) {
		Vector<Object> params = new Vector<Object>();
		setServiceDefaults();
		StringBuffer strBufApprove = new StringBuffer();

		strBufApprove = new StringBuffer("Select TAppr.COUNTRY, " + CountryApprDesc + ",TAppr.LE_BOOK, "
				+ LeBookApprDesc + ",TAppr.FEED_ID,TAppr.FEED_NAME,TAppr.FEED_DESCRIPTION,TAppr.BATCH_SIZE"
				+ ",TAppr.LAST_EXTRACTION_DATE,TAppr.COMPLETION_STATUS_AT,TAppr.COMPLETION_STATUS, "
				+ CompletionStatusAtApprDesc + ",TAppr.FEED_TYPE_AT,TAppr.FEED_TYPE, " + FeedTypeAtApprDesc
				+ ",TAppr.FEED_CATEGORY, " + categoryApprDesc
				+ ",TAppr.ALERT_MECHANISM,TAppr.PRIVILAGE_MECHANISM,TAppr.FEED_STATUS_NT,TAppr.FEED_STATUS, "
				+ FeedStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR, "
				+ RecordIndicatorNtApprDesc + ",TAppr.MAKER, " + makerApprDesc + ",TAppr.VERIFIER, " + verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TAppr.DISTINCT_FLAG, 'APPROVE' DATA_FROM  from ETL_FEED_MAIN TAppr ");

		String strWhereNotExists = new String(" Not Exists (Select 'X' From ETL_FEED_MAIN_PEND TPend "
				+ "WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID )");

		StringBuffer strBufPending = new StringBuffer();

		strBufPending = new StringBuffer("Select TPend.COUNTRY, " + CountryPendDesc + ",TPend.LE_BOOK, "
				+ LeBookTPendDesc + ",TPend.FEED_ID,TPend.FEED_NAME,TPend.FEED_DESCRIPTION,TPend.BATCH_SIZE"
				+ ",TPend.LAST_EXTRACTION_DATE,TPend.COMPLETION_STATUS_AT,TPend.COMPLETION_STATUS, "
				+ CompletionStatusAtPendDesc + ",TPend.FEED_TYPE_AT,TPend.FEED_TYPE, " + FeedTypeAtPendDesc
				+ ",TPend.FEED_CATEGORY, " + categoryPendDesc
				+ ",TPend.ALERT_MECHANISM,TPend.PRIVILAGE_MECHANISM,TPend.FEED_STATUS_NT,TPend.FEED_STATUS, "
				+ FeedStatusNtPendDesc + ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR, "
				+ RecordIndicatorNtPendDesc + ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TPend.DISTINCT_FLAG, 'PEND' DATA_FROM  from ETL_FEED_MAIN_PEND TPend ");

		String strWhereNotExistsApprInUpl = new String(" Not Exists (Select 'X' From ETL_FEED_MAIN_UPL TUpl "
				+ "WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID) ");

		String strWhereNotExistsPendInUpl = new String(" Not Exists (Select 'X' From ETL_FEED_MAIN_UPL TUpl "
				+ "WHERE TPend.COUNTRY = TUpl.COUNTRY AND TPend.LE_BOOK = TUpl.LE_BOOK AND TPend.FEED_ID = TUpl.FEED_ID) ");

		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY, "
				+ CountryPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.LE_BOOK, "
				+ LeBookTPendDesc.replaceAll("TPend.", "TUpl.")
				+ ",TUpl.FEED_ID,TUpl.FEED_NAME,TUpl.FEED_DESCRIPTION,TUpl.BATCH_SIZE"
				+ ",TUpl.LAST_EXTRACTION_DATE,TUpl.COMPLETION_STATUS_AT,TUpl.COMPLETION_STATUS, "
				+ CompletionStatusAtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.FEED_TYPE_AT,TUpl.FEED_TYPE, "
				+ FeedTypeAtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.FEED_CATEGORY, "
				+ categoryPendDesc.replaceAll("TPend.", "TUpl.")
				+ ",TUpl.ALERT_MECHANISM,TUpl.PRIVILAGE_MECHANISM,TUpl.FEED_STATUS_NT,TUpl.FEED_STATUS, "
				+ FeedStatusNtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR, " + RecordIndicatorNtPendDesc.replaceAll("TPend.", "TUpl.") + ",TUpl.MAKER, "
				+ makerUplDesc + ",TUpl.VERIFIER, " + verifierUplDesc + ",TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TUpl.DISTINCT_FLAG,'UPL' DATA_FROM  from ETL_FEED_MAIN_UPL TUpl ");

		try {
			if (ValidationUtil.isValid(dObj.getCountry())) {
				params.addElement(dObj.getCountry().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getLeBook())) {
				params.addElement(dObj.getLeBook().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				params.addElement(dObj.getFeedId().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
			}
			/*
			 * if (ValidationUtil.isValid(dObj.getFeedName())){ params.addElement("%" +
			 * dObj.getFeedName().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.FEED_NAME) LIKE ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FEED_NAME) LIKE ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.FEED_NAME) LIKE ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getFeedDescription())){ params.addElement("%" +
			 * dObj.getFeedDescription().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.FEED_DESCRIPTION) LIKE ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FEED_DESCRIPTION) LIKE ?",
			 * strBufPending); CommonUtils.addToQuery("UPPER(TUpl.FEED_DESCRIPTION) LIKE ?",
			 * strBufUpl); } if (ValidationUtil.isValid(dObj.getEffectiveDate())){
			 * params.addElement("%" + dObj.getEffectiveDate().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.EFFECTIVE_DATE) LIKE ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.EFFECTIVE_DATE) LIKE ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.EFFECTIVE_DATE) LIKE ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getLastExtractionDate())){ params.addElement("%"
			 * + dObj.getLastExtractionDate().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.LAST_EXTRACTION_DATE) LIKE ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.LAST_EXTRACTION_DATE) LIKE ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.LAST_EXTRACTION_DATE) LIKE ?", strBufUpl);
			 * } if (ValidationUtil.isValid(dObj.getFeedType())){ params.addElement("%" +
			 * dObj.getFeedType().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.FEED_TYPE) LIKE ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FEED_TYPE) LIKE ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.FEED_TYPE) LIKE ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getFeedCategory())){ params.addElement("%" +
			 * dObj.getFeedCategory().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.FEED_CATEGORY) LIKE ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FEED_CATEGORY) LIKE ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.FEED_CATEGORY) LIKE ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getAlertMechanism())){ params.addElement("%" +
			 * dObj.getAlertMechanism().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.ALERT_MECHANISM) LIKE ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.ALERT_MECHANISM) LIKE ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.ALERT_MECHANISM) LIKE ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getPrivilageMechanism())){ params.addElement("%"
			 * + dObj.getPrivilageMechanism().toUpperCase() + "%");
			 * CommonUtils.addToQuery("UPPER(TAppr.PRIVILAGE_MECHANISM) LIKE ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.PRIVILAGE_MECHANISM) LIKE ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.PRIVILAGE_MECHANISM) LIKE ?", strBufUpl);
			 * } if (ValidationUtil.isValid(dObj.getFeedStatus())){ params.addElement("" +
			 * dObj.getFeedStatus() + ""); CommonUtils.addToQuery("TAppr.FEED_STATUS = ?",
			 * strBufApprove); CommonUtils.addToQuery("TPend.FEED_STATUS = ?",
			 * strBufPending); CommonUtils.addToQuery("TUpl.FEED_STATUS = ?", strBufUpl); }
			 * if (dObj.getRecordIndicator() != -1){ if (dObj.getRecordIndicator() > 3){
			 * params.addElement(new Integer(0));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufUpl); }else{
			 * params.addElement(new Integer(dObj.getRecordIndicator()));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufUpl);
			 * 
			 * } }
			 */
			String orderBy = " Order By COUNTRY , LE_BOOK , FEED_ID ";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);
			StringBuffer lPageQuery = new StringBuffer();
			if (dObj.isVerificationRequired())
				lPageQuery.append(strBufPending.toString() + " Union " + strBufUpl.toString());
			else
				lPageQuery.append(strBufApprove.toString() + " Union " + strBufUpl.toString());
			return getQueryPopupResultsPgn(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,
					getLocalMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlFeedMainVb> getQueryResults(EtlFeedMainVb dObj, int intStatus) {
		List<EtlFeedMainVb> collTemp = new ArrayList<>();
		final int intKeyFieldsCount = 3;

		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY, " + CountryApprDesc + ",TAppr.LE_BOOK, "
				+ LeBookApprDesc + ",TAppr.FEED_ID,TAppr.FEED_NAME,TAppr.FEED_DESCRIPTION,TAppr.BATCH_SIZE"
				+ ",TAppr.LAST_EXTRACTION_DATE,TAppr.COMPLETION_STATUS_AT,TAppr.COMPLETION_STATUS, "
				+ CompletionStatusAtApprDesc + ",TAppr.FEED_TYPE_AT,TAppr.FEED_TYPE, " + FeedTypeAtApprDesc
				+ ",TAppr.FEED_CATEGORY, " + categoryApprDesc
				+ ",TAppr.ALERT_MECHANISM,TAppr.PRIVILAGE_MECHANISM,TAppr.FEED_STATUS_NT,TAppr.FEED_STATUS, "
				+ FeedStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR, "
				+ RecordIndicatorNtApprDesc + ",TAppr.MAKER, " + makerApprDesc + ",TAppr.VERIFIER, " + verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TAppr.DISTINCT_FLAG, 'APPROVE' DATA_FROM  from ETL_FEED_MAIN TAppr "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY, " + CountryPendDesc + ",TPend.LE_BOOK, "
				+ LeBookTPendDesc + ",TPend.FEED_ID,TPend.FEED_NAME,TPend.FEED_DESCRIPTION,TPend.BATCH_SIZE"
				+ ",TPend.LAST_EXTRACTION_DATE,TPend.COMPLETION_STATUS_AT,TPend.COMPLETION_STATUS, "
				+ CompletionStatusAtPendDesc + ",TPend.FEED_TYPE_AT,TPend.FEED_TYPE, " + FeedTypeAtPendDesc
				+ ",TPend.FEED_CATEGORY, " + categoryPendDesc
				+ ",TPend.ALERT_MECHANISM,TPend.PRIVILAGE_MECHANISM,TPend.FEED_STATUS_NT,TPend.FEED_STATUS, "
				+ FeedStatusNtPendDesc + ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR, "
				+ RecordIndicatorNtPendDesc + ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TPend.DISTINCT_FLAG, 'PEND' DATA_FROM  from ETL_FEED_MAIN_PEND TPend "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		StringBuffer strBufUpl = new StringBuffer("Select TPend.COUNTRY, " + CountryPendDesc + ",TPend.LE_BOOK, "
				+ LeBookTPendDesc + ",TPend.FEED_ID,TPend.FEED_NAME,TPend.FEED_DESCRIPTION,TPend.BATCH_SIZE"
				+ ",TPend.LAST_EXTRACTION_DATE,TPend.COMPLETION_STATUS_AT,TPend.COMPLETION_STATUS, "
				+ CompletionStatusAtPendDesc + ",TPend.FEED_TYPE_AT,TPend.FEED_TYPE, " + FeedTypeAtPendDesc
				+ ",TPend.FEED_CATEGORY, " + categoryPendDesc
				+ ",TPend.ALERT_MECHANISM,TPend.PRIVILAGE_MECHANISM,TPend.FEED_STATUS_NT,TPend.FEED_STATUS, "
				+ FeedStatusNtPendDesc + ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR, "
				+ RecordIndicatorNtPendDesc + ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION, TPend.DISTINCT_FLAG, 'UPL' DATA_FROM  from ETL_FEED_MAIN_UPL TPend "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strBufUpl.toString(), objParams, getLocalMapper());
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strBufApprove.toString(), objParams, getLocalMapper());
			} else {
				collTemp = getJdbcTemplate().query(strBufPending.toString(), objParams, getLocalMapper());
			}
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	@Override
	protected List<EtlFeedMainVb> selectApprovedRecord(EtlFeedMainVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedMainVb> doSelectPendingRecord(EtlFeedMainVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlFeedMainVb> doSelectUplRecord(EtlFeedMainVb vObject) {
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlFeedMainVb records) {

		return records.getFeedStatus();
	}

	@Override
	protected void setStatus(EtlFeedMainVb vObject, int status) {
		vObject.setFeedStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedMainVb vObject) {
		if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
			vObject.setCompletionStatus("S");
		}
		if (ValidationUtil.isValid(vObject.getLastExtractionDate()) && vObject.getLastExtractionDate().contains("-")) {
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
		String query = "Insert Into ETL_FEED_MAIN (BATCH_SIZE, COUNTRY, LE_BOOK, FEED_ID, "
				+ "FEED_NAME, FEED_DESCRIPTION, LAST_EXTRACTION_DATE, FEED_TYPE_AT, "
				+ "FEED_TYPE, FEED_CATEGORY, ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, "
				+ "COMPLETION_STATUS_AT,COMPLETION_STATUS, DISTINCT_FLAG)"
				+ "Values (?,?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ",?,?,?)";
		Object[] args = { vObject.getBatchSize(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getFeedName(), vObject.getFeedDescription(), vObject.getLastExtractionDate(),
				vObject.getFeedTypeAt(), vObject.getFeedType(), vObject.getFeedCategory(), vObject.getAlertMechanism(),
				vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCompletionStatusAt(), vObject.getCompletionStatus(),
				vObject.getDistinctFlag() };

		return getJdbcTemplate().update(query, args);
	}

	public int doInsertionUpl(EtlFeedMainVb vObject) {
		if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
			vObject.setCompletionStatus("S");
		}
		if (ValidationUtil.isValid(vObject.getLastExtractionDate()) && vObject.getLastExtractionDate().contains("-")) {
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

		String query = "Insert Into ETL_FEED_MAIN_UPL (BATCH_SIZE,COUNTRY, LE_BOOK, FEED_ID, "
				+ "FEED_NAME, FEED_DESCRIPTION, LAST_EXTRACTION_DATE, FEED_TYPE_AT, "
				+ "FEED_TYPE, FEED_CATEGORY, ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "COMPLETION_STATUS_AT,COMPLETION_STATUS,DISTINCT_FLAG)"
				+ "Values (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?)";
		Object[] args = { vObject.getBatchSize(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getFeedName(), vObject.getFeedDescription(), vObject.getLastExtractionDate(),
				vObject.getFeedTypeAt(), vObject.getFeedType(), vObject.getFeedCategory(), vObject.getAlertMechanism(),
				vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCompletionStatusAt(), vObject.getCompletionStatus(),
				vObject.getDistinctFlag() };
		return getJdbcTemplate().update(query, args);
	}

	public boolean feedExistenceCount(EtlFeedMainVb vObject) {
		boolean feedExists = false;
		String query = "SELECT COUNT(*) AS CNT FROM ( SELECT FEED_ID FROM ETL_FEED_MAIN_UPL"
				+ " UNION ALL SELECT FEED_ID FROM ETL_FEED_MAIN_PEND UNION ALL SELECT FEED_ID FROM ETL_FEED_MAIN ) abc "
				+ " WHERE FEED_ID= ?";
		Object[] args = { vObject.getFeedId() };
		String data = getJdbcTemplate().queryForObject(query, args, String.class);
		if (ValidationUtil.isValid(data) && !data.equalsIgnoreCase("0")) {
			feedExists = true;
		}
		return feedExists;
	}

	@Override
	protected int doInsertionPend(EtlFeedMainVb vObject) {
		if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
			vObject.setCompletionStatus("S");
		}
		if (ValidationUtil.isValid(vObject.getLastExtractionDate()) && vObject.getLastExtractionDate().contains("-")) {
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

		String query = "Insert Into ETL_FEED_MAIN_PEND (BATCH_SIZE, COUNTRY, LE_BOOK, FEED_ID, "
				+ "FEED_NAME, FEED_DESCRIPTIOn, LAST_EXTRACTION_DATE, FEED_TYPE_AT, "
				+ "FEED_TYPE, FEED_CATEGORY, ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "COMPLETION_STATUS_AT,COMPLETION_STATUS, DISTINCT_FLAG)"
				+ "Values (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ",?,?,?)";
		Object[] args = { vObject.getBatchSize(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getFeedName(), vObject.getFeedDescription(), vObject.getLastExtractionDate(),
				vObject.getFeedTypeAt(), vObject.getFeedType(), vObject.getFeedCategory(), vObject.getAlertMechanism(),
				vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCompletionStatusAt(), vObject.getCompletionStatus(),
				vObject.getDistinctFlag() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFeedMainVb vObject) {
		if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
			vObject.setCompletionStatus("S");
		}
		if (ValidationUtil.isValid(vObject.getLastExtractionDate()) && vObject.getLastExtractionDate().contains("-")) {
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
		String query = "Insert Into ETL_FEED_MAIN_PEND (BATCH_SIZE, COUNTRY, LE_BOOK, FEED_ID, "
				+ "FEED_NAME, FEED_DESCRIPTION, LAST_EXTRACTION_DATE, FEED_TYPE_AT, FEED_TYPE, FEED_CATEGORY, "
				+ "ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "COMPLETION_STATUS_AT,COMPLETION_STATUS,DISTINCT_FLAG)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + ",?,?,?)";
		Object[] args = { vObject.getBatchSize(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getFeedName(), vObject.getFeedDescription(), vObject.getLastExtractionDate(),
				vObject.getFeedTypeAt(), vObject.getFeedType(), vObject.getFeedCategory(), vObject.getAlertMechanism(),
				vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCompletionStatusAt(), vObject.getCompletionStatus(),
				vObject.getDistinctFlag() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionUplWithDc(EtlFeedMainVb vObject) {
		try {
			if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
				vObject.setCompletionStatus("S");
			}
			if (ValidationUtil.isValid(vObject.getLastExtractionDate())
					&& vObject.getLastExtractionDate().contains("-")) {
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
				SimpleDateFormat sdf1 = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
				vObject.setTimeZone(etlFeedScheduleConfigDao.getTimeZone(vObject.getCountry(), vObject.getLeBook()));
				sdf.setTimeZone(TimeZone.getTimeZone(vObject.getTimeZone()));
				Date date1 = null;
				Date date2 = null;
				/* try { */
				date1 = sdf.parse(vObject.getLastExtractionDate());
				date2 = sdf1.parse(sdf1.format(date1));
				/*
				 * } catch (ParseException e) { // e.printStackTrace(); }
				 */
				long millisecondsSinceUnixEpoch = date2.getTime();
				vObject.setLastExtractionDate(String.valueOf(millisecondsSinceUnixEpoch));
			}

			String query = "Insert Into ETL_FEED_MAIN_UPL (BATCH_SIZE, COUNTRY, LE_BOOK, FEED_ID, "
					+ "FEED_NAME, FEED_DESCRIPTION, LAST_EXTRACTION_DATE, FEED_TYPE_AT, FEED_TYPE, FEED_CATEGORY, "
					+ "ALERT_MECHANISM, PRIVILAGE_MECHANISM, FEED_STATUS_NT, FEED_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, "
					+ " VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
					+ "COMPLETION_STATUS_AT,COMPLETION_STATUS, DISTINCT_FLAG) "
					+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ getDbFunction(Constants.SYSDATE, null) + ", "
					+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + ",?,?,?)";

			Object[] args = { vObject.getBatchSize(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
					vObject.getFeedName(), vObject.getFeedDescription(), vObject.getLastExtractionDate(),
					vObject.getFeedTypeAt(), vObject.getFeedType(), vObject.getFeedCategory(),
					vObject.getAlertMechanism(), vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(),
					vObject.getFeedStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
					vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
					vObject.getCompletionStatusAt(), vObject.getCompletionStatus(), vObject.getDistinctFlag() };

			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			ExceptionCode ec = new ExceptionCode();
			ec.setErrorCode(Constants.ERRONEOUS_OPERATION);
			ec.setErrorMsg(e.getMessage());
			throw buildRuntimeCustomException(ec);
		}
	}

	@Override
	protected int doUpdateAppr(EtlFeedMainVb vObject) {
		if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
			vObject.setCompletionStatus("S");
		}
		if (ValidationUtil.isValid(vObject.getLastExtractionDate()) && vObject.getLastExtractionDate().contains("-")) {
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

		String query = "Update  ETL_FEED_MAIN Set BATCH_SIZE =?, COMPLETION_STATUS =?, FEED_NAME = ?, FEED_DESCRIPTION = ?,  LAST_EXTRACTION_DATE = ?, "
				+ "FEED_TYPE_AT = ?, FEED_TYPE = ?, FEED_CATEGORY = ?, ALERT_MECHANISM = ?, PRIVILAGE_MECHANISM = ?, FEED_STATUS_NT = ?, FEED_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DISTINCT_FLAG=? ,DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getBatchSize(), vObject.getCompletionStatus(), vObject.getFeedName(),
				vObject.getFeedDescription(), vObject.getLastExtractionDate(), vObject.getFeedTypeAt(),
				vObject.getFeedType(), vObject.getFeedCategory(), vObject.getAlertMechanism(),
				vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getDistinctFlag(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFeedMainVb vObject) {
		if (!ValidationUtil.isValid(vObject.getCompletionStatus())) {
			vObject.setCompletionStatus("S");
		}
		if (ValidationUtil.isValid(vObject.getLastExtractionDate()) && vObject.getLastExtractionDate().contains("-")) {
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

		String query = "Update ETL_FEED_MAIN_PEND Set BATCH_SIZE =?,  COMPLETION_STATUS =?,FEED_NAME = ?, FEED_DESCRIPTION = ?,  LAST_EXTRACTION_DATE = ?, "
				+ "FEED_TYPE_AT = ?, FEED_TYPE = ?, FEED_CATEGORY = ?, ALERT_MECHANISM = ?, PRIVILAGE_MECHANISM = ?, FEED_STATUS_NT = ?, FEED_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DISTINCT_FLAG=?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";

		Object[] args = { vObject.getBatchSize(), vObject.getCompletionStatus(), vObject.getFeedName(),
				vObject.getFeedDescription(), vObject.getLastExtractionDate(), vObject.getFeedTypeAt(),
				vObject.getFeedType(), vObject.getFeedCategory(), vObject.getAlertMechanism(),
				vObject.getPrivilageMechanism(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getDistinctFlag(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_MAIN Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_MAIN_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int deletePRSRecord(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_MAIN_PRS Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND SESSION_ID =?";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId(), dObj.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deleteUplRecord(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_MAIN_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_FEED_MAIN";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_MAIN_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_MAIN_PEND";
		}
		String query = "update " + table
				+ " set FEED_STATUS=? ,RECORD_INDICATOR = ? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedMainVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getFeedName()))
			strAudit.append("FEED_NAME" + auditDelimiterColVal + vObject.getFeedName().trim());
		else
			strAudit.append("FEED_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFeedDescription()))
			strAudit.append("FEED_DESCRIPTION" + auditDelimiterColVal + vObject.getFeedDescription().trim());
		else
			strAudit.append("FEED_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLastExtractionDate()))
			strAudit.append("LAST_EXTRACTION_DATE" + auditDelimiterColVal + vObject.getLastExtractionDate().trim());
		else
			strAudit.append("LAST_EXTRACTION_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_TYPE_AT" + auditDelimiterColVal + vObject.getFeedTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFeedType()))
			strAudit.append("FEED_TYPE" + auditDelimiterColVal + vObject.getFeedType().trim());
		else
			strAudit.append("FEED_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COMPLETION_STATUS_AT" + auditDelimiterColVal + vObject.getCompletionStatusAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getCompletionStatus()))
			strAudit.append("COMPLETION_STATUS" + auditDelimiterColVal + vObject.getCompletionStatus().trim());
		else
			strAudit.append("COMPLETION_STATUS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFeedCategory()))
			strAudit.append("FEED_CATEGORY" + auditDelimiterColVal + vObject.getFeedCategory().trim());
		else
			strAudit.append("FEED_CATEGORY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getAlertMechanism()))
			strAudit.append("ALERT_MECHANISM" + auditDelimiterColVal + vObject.getAlertMechanism().trim());
		else
			strAudit.append("ALERT_MECHANISM" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getPrivilageMechanism()))
			strAudit.append("PRIVILAGE_MECHANISM" + auditDelimiterColVal + vObject.getPrivilageMechanism().trim());
		else
			strAudit.append("PRIVILAGE_MECHANISM" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_STATUS_NT" + auditDelimiterColVal + vObject.getFeedStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_STATUS" + auditDelimiterColVal + vObject.getFeedStatus());
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
	public void setServiceDefaults() {
		serviceName = "EtlFeedMain";
		serviceDesc = "ETL Feed Configuration";
		tableName = "ETL_FEED_MAIN";
		childTableName = "ETL_FEED_MAIN";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	String ConnectorTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2100, "TAppr.CONNECTOR_TYPE",
			"CONNECTOR_TYPE_DESC");

	String EndpointTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2101, "TAppr.ENDPOINT_TYPE",
			"ENDPOINT_TYPE_DESC");

	public List<EtlConnectorVb> getAllConnections(EtlConnectorVb vObj) {
		List<EtlConnectorVb> collTemp = null;
		String que = "";
		String strQueryAppr = new String("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.CONNECTOR_TYPE_AT"
				+ ",TAppr.CONNECTOR_TYPE, " + ConnectorTypeAtApprDesc + ",TAppr.CONNECTOR_ID,TAppr.CONNECTION_NAME"
				+ ",TAppr.CONNECTOR_SCRIPTS,TAppr.ENDPOINT_TYPE_AT,TAppr.ENDPOINT_TYPE, " + EndpointTypeAtApprDesc
				+ ",TAppr.CONNECTION_STAUTS_NT,TAppr.CONNECTION_STAUTS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION  From ETL_CONNECTOR TAppr WHERE CONNECTION_STAUTS = 0 and country = ? AND LE_BOOK = ? ");

		Object objParams[] = new Object[2];
		objParams[0] = vObj.getCountry();
		objParams[1] = vObj.getLeBook();

		if (ValidationUtil.isValid(vObj.getEndpointType())) {
			que = strQueryAppr.toString() + "  AND CONNECTOR_TYPE!='SSH' and  ENDPOINT_TYPE in ('"
					+ vObj.getEndpointType() + "','B')";
		}
		if (ValidationUtil.isValid(vObj.getFeedType()) && "DB_LINK".equalsIgnoreCase(vObj.getFeedType())) {
			que = que + " AND DB_LINK_FLAG ='Y'";
		}
		que = que + " order by CONNECTOR_ID ";
		try {
			collTemp = getJdbcTemplate().query(que, objParams, getConnectorMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public RowMapper getConnectorMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorVb vObject = new EtlConnectorVb();
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
				vObject.setConnectorTypeAt(rs.getInt("CONNECTOR_TYPE_AT"));
				if (rs.getString("CONNECTOR_TYPE") != null) {
					vObject.setConnectorType(rs.getString("CONNECTOR_TYPE"));
				} else {
					vObject.setConnectorType("");
				}
				if (rs.getString("CONNECTOR_TYPE_DESC") != null) {
					vObject.setConnectorTypeDesc(rs.getString("CONNECTOR_TYPE_DESC"));
				} else {
					vObject.setConnectorTypeDesc("");
				}
				if (rs.getString("CONNECTOR_ID") != null) {
					vObject.setConnectorId(rs.getString("CONNECTOR_ID"));
				} else {
					vObject.setConnectorId("");
				}
				if (rs.getString("CONNECTION_NAME") != null) {
					vObject.setConnectionName(rs.getString("CONNECTION_NAME"));
				} else {
					vObject.setConnectionName("");
				}
				if (rs.getString("CONNECTOR_SCRIPTS") != null) {
					vObject.setConnectorScripts(rs.getString("CONNECTOR_SCRIPTS"));
				} else {
					vObject.setConnectorScripts("");
				}
				vObject.setEndpointTypeAt(rs.getInt("ENDPOINT_TYPE_AT"));
				if (rs.getString("ENDPOINT_TYPE") != null) {
					vObject.setEndpointType(rs.getString("ENDPOINT_TYPE"));
				} else {
					vObject.setEndpointType("");
				}
				if (rs.getString("ENDPOINT_TYPE_DESC") != null) {
					vObject.setEndpointTypeDesc(rs.getString("ENDPOINT_TYPE_DESC"));
				} else {
					vObject.setEndpointTypeDesc("");
				}
				vObject.setConnectionStatusNt(rs.getInt("CONNECTION_STAUTS_NT"));
				vObject.setConnectionStatus(rs.getInt("CONNECTION_STAUTS"));
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

	public NumSubTabVb getActiveNumTab(int ntValue, String databaseFilter) {
		String sql = "select * from NUM_SUB_TAB t where NUM_TAB= ? AND NUM_SUBTAB_DESCRIPTION= ?";
		Object[] args = { ntValue, databaseFilter };
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			public NumSubTabVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				NumSubTabVb numSubTabVb = new NumSubTabVb();
				while (rs.next()) {
					numSubTabVb.setNumSubTab(rs.getInt("NUM_SUB_TAB"));
					numSubTabVb.setDescription(rs.getString("NUM_SUBTAB_DESCRIPTION"));
				}
				return numSubTabVb;
			}
		};
		return (NumSubTabVb) getJdbcTemplate().query(sql, args, rseObj);
	}

	public Map<String, String> getDataTypeMap(String databaseType) {
		String sql = "select TAG_NAME, DISPLAY_NAME from MACROVAR_TAGGING t where MACROVAR_NAME = 'DATA_TYPE' AND UPPER(MACROVAR_TYPE) = UPPER(?) ORDER BY TAG_NO";
		Object[] args = { databaseType };
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					returnMap.put(rs.getString("TAG_NAME"), rs.getString("DISPLAY_NAME"));
				}
				return returnMap;
			}
		};
		return (Map<String, String>) getJdbcTemplate().query(sql, args, rseObj);
	}

	public String getQueryForTableAndView(String variableName) throws DataAccessException {
		String sql = "SELECT VARIABLE_SCRIPT FROM VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_NAME = ?";
		Object [] args= {variableName};
		return getJdbcTemplate().queryForObject(sql,args,String.class);
	}

	public String getRandomNumber() {
		Integer otp = new Random().nextInt(999999);
		int noOfOtpDigit = 6;
		while (Integer.toString(otp).length() != noOfOtpDigit) {
			otp = new Random().nextInt(999999);
		}
		String otpString = String.valueOf(otp);
		return otpString;
	}

	private String returnTableAliasName(EtlFeedMainVb vcMainVb, String tableId) {
		return vcMainVb.getEtlFeedTablesList().stream()
				.filter(treeVb -> tableId.equalsIgnoreCase(String.valueOf(treeVb.getTableId())))
				.collect(Collectors.collectingAndThen(Collectors.toList(), treeVbList -> {
					return ValidationUtil.isValidList(treeVbList) ? treeVbList.get(0).getTableAliasName() : "";
				}));
	}

	public String doInsertUpdateRelationsWthRegExp(EtlFeedMainVb vcMainVb, EtlFeedRelationVb relationVb) {
		final String DOT = ".";
		final String EQUAL = " = ";
		final String AND = " AND ";
		final String JOIN_SYMBOL = " (+) ";
		StringBuffer relationStr = new StringBuffer("<relation>");
		StringBuffer ansiiJoinStr = new StringBuffer();
		StringBuffer standardJoinStr = new StringBuffer();
		String fromTblAliasName = returnTableAliasName(vcMainVb, relationVb.getFromTableId());
		String toTblAliasName = returnTableAliasName(vcMainVb, relationVb.getToTableId());
		int index = 1;
		boolean isAndRequired = true;
		if (5 != relationVb.getJoinType()) { // Cross Join Chk - No join string for cross join
			if (ValidationUtil.isValid(relationVb.getCustomJoinString())) {
				relationStr.append("<customjoin>" + relationVb.getCustomJoinString() + "</customjoin>");
			} else if (ValidationUtil.isValidList(relationVb.getRelationScriptParsed())) {
				relationStr.append("<columnmapping>");
				for (RelationMapVb relationMapVb : relationVb.getRelationScriptParsed()) {
					isAndRequired = (index == relationVb.getRelationScriptParsed().size()) ? false : true;
					relationStr.append("<column>");
					relationStr.append("<fcolumn>" + relationMapVb.getfColumn() + "</fcolumn>");
					relationStr.append("<tcolumn>" + relationMapVb.gettColumn() + "</tcolumn>");

					ansiiJoinStr.append(
							fromTblAliasName + DOT + Constants.TICK + relationMapVb.getfColumn() + Constants.TICK);
					ansiiJoinStr.append(EQUAL);
					ansiiJoinStr.append(
							toTblAliasName + DOT + Constants.TICK + relationMapVb.gettColumn() + Constants.TICK);

					if (1 == relationVb.getJoinType()) {// Inner Join
						standardJoinStr.append(
								fromTblAliasName + DOT + Constants.TICK + relationMapVb.getfColumn() + Constants.TICK);
						standardJoinStr.append(EQUAL);
						standardJoinStr.append(
								toTblAliasName + DOT + Constants.TICK + relationMapVb.gettColumn() + Constants.TICK);
					} else if (2 == relationVb.getJoinType()) {// Left Join
						standardJoinStr.append(
								fromTblAliasName + DOT + Constants.TICK + relationMapVb.getfColumn() + Constants.TICK);
						standardJoinStr.append(EQUAL);
						standardJoinStr.append(toTblAliasName + DOT + Constants.TICK + relationMapVb.gettColumn()
								+ Constants.TICK + JOIN_SYMBOL);
					} else if (3 == relationVb.getJoinType()) {// Right Join
						standardJoinStr.append(fromTblAliasName + DOT + Constants.TICK + relationMapVb.getfColumn()
								+ Constants.TICK + JOIN_SYMBOL);
						standardJoinStr.append(EQUAL);
						standardJoinStr.append(
								toTblAliasName + DOT + Constants.TICK + relationMapVb.gettColumn() + Constants.TICK);
					}

					if (isAndRequired) {
						ansiiJoinStr.append(AND);
						if (4 != relationVb.getJoinType())// Outer Join(Not possible with standard join syntax)
							standardJoinStr.append(AND);
					}
					relationStr.append("</column>");
					index++;
				}
				relationStr.append("</columnmapping>");
				relationStr.append("<customjoin></customjoin>");
				relationStr.append("<ansii_joinstring>" + ansiiJoinStr + "</ansii_joinstring>");
				relationStr.append("<std_joinstring>" + standardJoinStr + "</std_joinstring>");
			}
		}
		relationStr.append("</relation>");
		relationVb.setRelationScript(String.valueOf(relationStr));
		String dataXmL = relationVb.getRelationScript();
		return dataXmL;
	}

	public void deleteAllUplData(EtlFeedMainVb vObject, String isValid) {
		if ("Y".equalsIgnoreCase(isValid)) {
			deleteUplRecord(vObject);
		}

		etlFeedSourceDao.deleteRecordByParent(vObject, "UPL");
		etlFeedTablesDao.deleteRecordByParent(vObject, "UPL");
		etlFeedColumnsDao.deleteRecordByParent(vObject, "UPL");
		etlFeedRelationDao.deleteRecordByParent(vObject, "UPL");

		etlExtractionSummaryFieldsDao.deleteRecordByParent(vObject, "UPL");

		etlFeedTransformationDao.deleteRecordByParent(vObject, "UPL");
		etlFeedTransformationDao.deleteRecordByParentNode(vObject, "UPL");
		etlFeedTransformationDao.deleteRecordByChildNode(vObject, "UPL");
		etlFeedTranColumnsDao.deleteRecordByParent(vObject, "UPL");

		etlFeedDestinationDao.deleteRecordByParent(vObject, "UPL");
		etlTransformedColumnsDao.deleteRecordByParent(vObject, "UPL");
		etlTargetColumnsDao.deleteRecordByParent(vObject, "UPL");
		etlFeedLoadMappingDao.deleteRecordByParent(vObject, "UPL");
		etlFeedInjectionConfigDao.deleteRecordByParent(vObject, "UPL");
		if ("Y".equalsIgnoreCase(isValid)) {
			etlFeedScheduleConfigDao.deleteRecordByParent(vObject, "UPL");
			etlFeedAlertconfigDao.deleteRecordByParent(vObject, "UPL");

		}
	}

	public void deleteAllPrsData(EtlFeedMainVb vObject) {
		deletePRSRecord(vObject);

		etlFileUploadAreaDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFileTableDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFileColumnsDao.deleteRecordByPRSParent(vObject, "PRS");

		etlConnectorDao.deleteRecordByPRSParent(vObject, "PRS");
		etlMqTableDao.deleteRecordByPRSParent(vObject, "PRS");
		etlMqColumnsDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedCategoryDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedCategoryDao.deleteRecordByPRSDependentParent(vObject, "PRS");
		etlCategoryAlertconfigDao.deleteRecordByPRSParent(vObject, "PRS");

		etlFeedSourceDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedTablesDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedColumnsDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedRelationDao.deleteRecordByPRSParent(vObject, "PRS");

		etlExtractionSummaryFieldsDao.deleteRecordByPRSParent(vObject, "PRS");

		etlFeedTransformationDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedTransformationDao.deleteRecordByPRSParentNode(vObject, "PRS");
		etlFeedTransformationDao.deleteRecordByPRSChildNode(vObject, "PRS");
		etlFeedTranColumnsDao.deleteRecordByPRSParent(vObject, "PRS");

		etlFeedDestinationDao.deleteRecordByPRSParent(vObject, "PRS");
		etlTransformedColumnsDao.deleteRecordByPRSParent(vObject, "PRS");
		etlTargetColumnsDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedLoadMappingDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedInjectionConfigDao.deleteRecordByPRSParent(vObject, "PRS");

		etlFeedScheduleConfigDao.deleteRecordByPRSParent(vObject, "PRS");
		etlFeedAlertconfigDao.deleteRecordByPRSParent(vObject, "PRS");

	}

	public void deleteAllPrsDataByCategory(EtlFeedMainVb vObject) {
		deletePRSRecordByCategory(vObject);

		etlFileUploadAreaDao.deleteRecordByPRSParentByCategory(vObject);
		etlFileTableDao.deleteRecordByPRSParentByCategory(vObject);
		etlFileColumnsDao.deleteRecordByPRSParentByCategory(vObject);

		etlConnectorDao.deleteRecordByPRSParentByCategory(vObject);
		etlMqTableDao.deleteRecordByPRSParentByCategory(vObject);
		etlMqColumnsDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedCategoryDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedCategoryDao.deleteRecordByPRSDependentParentByCategory(vObject);
		etlCategoryAlertconfigDao.deleteRecordByPRSParentByCategory(vObject);

		etlFeedSourceDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedTablesDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedColumnsDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedRelationDao.deleteRecordByPRSParentByCategory(vObject);

		etlExtractionSummaryFieldsDao.deleteRecordByPRSParentByCategory(vObject);

		etlFeedTransformationDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedTransformationDao.deleteRecordByPRSParentNodeByCategory(vObject);
		etlFeedTransformationDao.deleteRecordByPRSChildNodeByCategory(vObject);
		etlFeedTranColumnsDao.deleteRecordByPRSParentByCategory(vObject);

		etlFeedDestinationDao.deleteRecordByPRSParentByCategory(vObject);
		etlTransformedColumnsDao.deleteRecordByPRSParentByCategory(vObject);
		etlTargetColumnsDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedLoadMappingDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedInjectionConfigDao.deleteRecordByPRSParentByCategory(vObject);

		etlFeedScheduleConfigDao.deleteRecordByPRSParentByCategory(vObject);
		etlFeedAlertconfigDao.deleteRecordByParentByCategory(vObject);

	}
	public void deleteAllPendData(EtlFeedMainVb vObject, String isValid) {
		if ("Y".equalsIgnoreCase(isValid)) {
			deletePendingRecord(vObject);
		}
		etlFeedSourceDao.deleteRecordByParent(vObject, "PEND");
		etlFeedTablesDao.deleteRecordByParent(vObject, "PEND");
		etlFeedColumnsDao.deleteRecordByParent(vObject, "PEND");
		etlFeedRelationDao.deleteRecordByParent(vObject, "PEND");

		etlExtractionSummaryFieldsDao.deleteRecordByParent(vObject, "PEND");

		etlFeedTransformationDao.deleteRecordByParent(vObject, "PEND");
		etlFeedTransformationDao.deleteRecordByParentNode(vObject, "PEND");
		etlFeedTransformationDao.deleteRecordByChildNode(vObject, "PEND");
		etlFeedTranColumnsDao.deleteRecordByParent(vObject, "PEND");

		etlFeedDestinationDao.deleteRecordByParent(vObject, "PEND");
		etlTransformedColumnsDao.deleteRecordByParent(vObject, "PEND");
		etlTargetColumnsDao.deleteRecordByParent(vObject, "PEND");
		etlFeedLoadMappingDao.deleteRecordByParent(vObject, "PEND");
		etlFeedInjectionConfigDao.deleteRecordByParent(vObject, "PEND");

		if ("Y".equalsIgnoreCase(isValid)) {
			etlFeedScheduleConfigDao.deleteRecordByParent(vObject, "PEND");
			etlFeedAlertconfigDao.deleteRecordByParent(vObject, "PEND");
		}
	}

	public void deleteAllApproveData(EtlFeedMainVb vObject) {
		doDeleteAppr(vObject);
		etlFeedSourceDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedTablesDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedColumnsDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedRelationDao.deleteRecordByParent(vObject, "APPROVE");

		etlExtractionSummaryFieldsDao.deleteRecordByParent(vObject, "APPROVE");

		etlFeedTransformationDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedTransformationDao.deleteRecordByParentNode(vObject, "APPROVE");
		etlFeedTransformationDao.deleteRecordByChildNode(vObject, "APPROVE");
		etlFeedTranColumnsDao.deleteRecordByParent(vObject, "APPROVE");

		etlFeedDestinationDao.deleteRecordByParent(vObject, "APPROVE");
		etlTransformedColumnsDao.deleteRecordByParent(vObject, "APPROVE");
		etlTargetColumnsDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedLoadMappingDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedInjectionConfigDao.deleteRecordByParent(vObject, "APPROVE");

		etlFeedScheduleConfigDao.deleteRecordByParent(vObject, "APPROVE");
		etlFeedAlertconfigDao.deleteRecordByParent(vObject, "APPROVE");

	}

	public void updateAllApproveDataStatus(EtlFeedMainVb vObject, int status, String tableType) {
		updateStatusByParent(vObject, tableType, status);

		etlFeedSourceDao.updateStatusByParent(vObject, tableType, status);
		etlFeedTablesDao.updateStatusByParent(vObject, tableType, status);
		etlFeedColumnsDao.updateStatusByParent(vObject, tableType, status);
		etlFeedRelationDao.updateStatusByParent(vObject, tableType, status);

		etlExtractionSummaryFieldsDao.updateStatusByParent(vObject, tableType, status);

		etlFeedTransformationDao.updateStatusByParent(vObject, tableType, status);
		etlFeedTransformationDao.updateStatusByParentNode(vObject, tableType, status);
		etlFeedTransformationDao.updateStatusByChildNode(vObject, tableType, status);
		etlFeedTranColumnsDao.updateStatusByParent(vObject, tableType, status);

		etlFeedDestinationDao.updateStatusByParent(vObject, tableType, status);
		etlTransformedColumnsDao.updateStatusByParent(vObject, tableType, status);
		etlTargetColumnsDao.updateStatusByParent(vObject, tableType, status);
		etlFeedLoadMappingDao.updateStatusByParent(vObject, tableType, status);
		etlFeedInjectionConfigDao.updateStatusByParent(vObject, tableType, status);

		etlFeedScheduleConfigDao.updateStatusByParent(vObject, tableType, status);
		etlFeedAlertconfigDao.updateStatusByParent(vObject, tableType, status);

	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertApprRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		try {
			return doInsertApprRecordForNonTrans(vObject);
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

	protected ExceptionCode doInsertApprRecordForNonTrans(EtlFeedMainVb vObject) throws RuntimeCustomException {
		List<EtlFeedMainVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.ADD;
		strApproveOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFeedMainVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		collTemp = doSelectUplRecord(vObject);
		EtlFeedMainVb vObjectlocalUpl = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);

		vObjectlocalUpl.setMaker(getIntCurrentUserId());
		// Try inserting the record
		vObjectlocalUpl.setRecordIndicator(Constants.STATUS_ZERO);
		vObjectlocalUpl.setFeedStatus(Constants.PUBLISHED);
		vObject.setVerifier(getIntCurrentUserId());
		retVal = doInsertionAppr(vObjectlocalUpl);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		String systemDate = getSystemDate();
		vObjectlocalUpl.setDateLastModified(systemDate);
		vObjectlocalUpl.setDateCreation(systemDate);
		exceptionCode = writeAuditLog(vObjectlocalUpl, null);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}

		int pendingReocrds = 9999;
		// Source Data
		insertSourceDataApprove(vObject, pendingReocrds);

		// Extraction Data
		insertExtractionDataApprove(vObject, pendingReocrds);

		// Etl Transformation
		insertTransformationDataApprove(vObject, pendingReocrds);

		// Etl Feed Target Data
		insertTargetDataApprove(vObject, pendingReocrds);

		// Etl Feed Schedule Config
		insertScheduleDataApprove(vObject, pendingReocrds);

		deleteAllUplData(vObjectlocalUpl, "Y");

		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		try {

			return doInsertRecordForNonTrans(vObject);
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

	@Override
	protected ExceptionCode doInsertRecordForNonTrans(EtlFeedMainVb vObject) throws RuntimeCustomException {
		List<EtlFeedMainVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int staticDeletionFlag = getStatus(((ArrayList<EtlFeedMainVb>) collTemp).get(0));
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
			EtlFeedMainVb vObjectLocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
			if (vObjectLocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				exceptionCode = getResultObject(Constants.PENDING_FOR_ADD_ALREADY);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			// Try inserting the record

			collTemp = doSelectUplRecord(vObject);
			EtlFeedMainVb vObjectlocalUpl = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);

			vObjectlocalUpl.setVerifier(0);
			vObjectlocalUpl.setRecordIndicator(Constants.STATUS_INSERT);
			String systemDate = getSystemDate();
			vObjectlocalUpl.setDateLastModified(systemDate);
			vObjectlocalUpl.setDateCreation(systemDate);
			vObjectlocalUpl.setFeedStatus(Constants.WORK_IN_PROGRESS);
			retVal = doInsertionPend(vObjectlocalUpl);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Source Data
			insertSourceDataPend(vObject, systemDate);

			// Extraction Data
			insertExtractionDataPend(vObject, systemDate);

			// Etl Transformation
			insertTransformationDataPend(vObject, systemDate);

			// Etl Feed Target Data
			insertTargetDataPend(vObject, systemDate);

			// Etl Feed Schedule Config
			insertScheduleDataPend(vObject, systemDate);

			// Delete All UPL Tables Data
			deleteAllUplData(vObject, "Y");

			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			return exceptionCode;
		}
	}

	protected ExceptionCode insertSourceDataPend(EtlFeedMainVb vObject, String systemDate)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// Insert ETL Source Connections
		List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, 9999);
		if (etlFeedSourceList != null && !etlFeedSourceList.isEmpty()) {
			for (EtlFeedSourceVb sourceVb : etlFeedSourceList) {
				sourceVb.setVerifier(0);
				sourceVb.setRecordIndicator(Constants.STATUS_INSERT);
				sourceVb.setDateLastModified(systemDate);
				sourceVb.setDateCreation(systemDate);
				sourceVb.setFeedSourceStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedSourceDao.doInsertionPend(sourceVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Insert ETL Tables
		List<ETLFeedTablesVb> etlFeedTablesLists = etlFeedTablesDao.getQueryResultsByParent(vObject, 9999);
		if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
			for (ETLFeedTablesVb tablesVb : etlFeedTablesLists) {
				tablesVb.setVerifier(0);
				tablesVb.setRecordIndicator(Constants.STATUS_INSERT);
				tablesVb.setDateLastModified(systemDate);
				tablesVb.setDateCreation(systemDate);
				tablesVb.setTableTypeAt(2103);
				tablesVb.setFeedTableStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedTablesDao.doInsertionPend(tablesVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// Insert ETL Columns
		List<ETLFeedColumnsVb> etlFeedColumnsLists = etlFeedColumnsDao.getQueryResultsByParent(vObject, 9999);
		if (etlFeedColumnsLists != null && !etlFeedColumnsLists.isEmpty()) {
			for (ETLFeedColumnsVb columnsVb : etlFeedColumnsLists) {
				columnsVb.setVerifier(0);
				columnsVb.setRecordIndicator(Constants.STATUS_INSERT);
				columnsVb.setDateLastModified(systemDate);
				columnsVb.setDateCreation(systemDate);
				columnsVb.setFeedColumnStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedColumnsDao.doInsertionPend(columnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Insert ETL Relations
		List<EtlFeedRelationVb> etlFeedRelationLists = etlFeedRelationDao.getQueryResultsByParent(vObject, 9999);
		if (etlFeedRelationLists != null && !etlFeedRelationLists.isEmpty()) {
			for (EtlFeedRelationVb relationVb : etlFeedRelationLists) {
				relationVb.setVerifier(0);
				relationVb.setRecordIndicator(Constants.STATUS_INSERT);
				relationVb.setDateLastModified(systemDate);
				relationVb.setDateCreation(systemDate);
				relationVb.setFeedRelationStatus(Constants.WORK_IN_PROGRESS);
				relationVb.setFilterContext(relationVb.getFilterCondition());
				if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
					vObject.setEtlFeedTablesList(etlFeedTablesLists);
				}
				relationVb.setRelationContext(doInsertUpdateRelationsWthRegExp(vObject, relationVb));
				retVal = etlFeedRelationDao.doInsertionPend(relationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertExtractionDataPend(EtlFeedMainVb vObject, String systemDate)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<EtlExtractionSummaryFieldsVb> extractionSummaryFieldsList = etlExtractionSummaryFieldsDao
				.getQueryResultsByParent(vObject, 9999);
		if (extractionSummaryFieldsList != null && !extractionSummaryFieldsList.isEmpty()) {
			for (EtlExtractionSummaryFieldsVb summaryFieldsVb : extractionSummaryFieldsList) {
				summaryFieldsVb.setVerifier(0);
				summaryFieldsVb.setRecordIndicator(Constants.STATUS_INSERT);
				summaryFieldsVb.setDateLastModified(systemDate);
				summaryFieldsVb.setDateCreation(systemDate);
				summaryFieldsVb.setEtlFieldsStatus(Constants.WORK_IN_PROGRESS);
				summaryFieldsVb.setUserId(getIntCurrentUserId());
				retVal = etlExtractionSummaryFieldsDao.doInsertionPend(summaryFieldsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTransformationDataPend(EtlFeedMainVb vObject, String systemDate)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// ETL Feed Transformation
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, 9999);
		for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
			transformationVb.setVerifier(0);
			transformationVb.setRecordIndicator(Constants.STATUS_INSERT);
			transformationVb.setDateLastModified(systemDate);
			transformationVb.setDateCreation(systemDate);
			transformationVb.setTransformationStatus(Constants.WORK_IN_PROGRESS);
			retVal = etlFeedTransformationDao.doInsertionPend(transformationVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		// ETL Feed Transformation Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, 9999);
		for (EtlTransformedColumnsVb transformedColumnsVb : etlTransformedColumnsList) {
			transformedColumnsVb.setVerifier(0);
			transformedColumnsVb.setRecordIndicator(Constants.STATUS_INSERT);
			transformedColumnsVb.setDateLastModified(systemDate);
			transformedColumnsVb.setDateCreation(systemDate);
			transformedColumnsVb.setFeedColumnStatus(Constants.WORK_IN_PROGRESS);
			retVal = etlTransformedColumnsDao.doInsertionPend(transformedColumnsVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTargetDataPend(EtlFeedMainVb vObject, String systemDate)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;

		// ETL Transformed Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, 9999);
		if (etlTransformedColumnsList != null && !etlTransformedColumnsList.isEmpty()) {
			for (EtlTransformedColumnsVb columnsVb : etlTransformedColumnsList) {
				columnsVb.setVerifier(0);
				columnsVb.setRecordIndicator(Constants.STATUS_INSERT);
				columnsVb.setDateLastModified(systemDate);
				columnsVb.setDateCreation(systemDate);
				columnsVb.setFeedColumnStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlTransformedColumnsDao.doInsertionPend(columnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Feed Destination
		List<EtlFeedDestinationVb> etlFeedDestinationList = etlFeedDestinationDao.getQueryResultsByParent(vObject,
				9999);
		if (etlFeedDestinationList != null && !etlFeedDestinationList.isEmpty()) {
			for (EtlFeedDestinationVb feedDestinationVb : etlFeedDestinationList) {
				feedDestinationVb.setVerifier(0);
				feedDestinationVb.setRecordIndicator(Constants.STATUS_INSERT);
				feedDestinationVb.setDateLastModified(systemDate);
				feedDestinationVb.setDateCreation(systemDate);
				feedDestinationVb.setFeedTransformStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedDestinationDao.doInsertionPend(feedDestinationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// ETL Target Columns
		List<EtlTargetColumnsVb> etlTargetColumnsList = etlTargetColumnsDao.getQueryResultsByParent(vObject, 9999);
		if (etlTargetColumnsList != null && !etlTargetColumnsList.isEmpty()) {
			for (EtlTargetColumnsVb targetColumnsVb : etlTargetColumnsList) {
				targetColumnsVb.setVerifier(0);
				targetColumnsVb.setRecordIndicator(Constants.STATUS_INSERT);
				targetColumnsVb.setDateLastModified(systemDate);
				targetColumnsVb.setDateCreation(systemDate);
				targetColumnsVb.setFeedColumnStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlTargetColumnsDao.doInsertionPend(targetColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Feed Loading Mappings
		List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = etlFeedLoadMappingDao.getQueryResultsByParent(vObject,
				9999);
		if (etlFeedLoadMappingList != null && !etlFeedLoadMappingList.isEmpty()) {
			for (EtlFeedLoadMappingVb loadMappingVb : etlFeedLoadMappingList) {
				loadMappingVb.setVerifier(0);
				loadMappingVb.setRecordIndicator(Constants.STATUS_INSERT);
				loadMappingVb.setDateLastModified(systemDate);
				loadMappingVb.setDateCreation(systemDate);
				loadMappingVb.setFeedMappingStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedLoadMappingDao.doInsertionPend(loadMappingVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Etl Feed Injection Config
		List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = etlFeedInjectionConfigDao
				.getQueryResultsByParent(vObject, 9999);
		if (etlFeedInjectionConfigList != null && !etlFeedInjectionConfigList.isEmpty()) {
			for (EtlFeedInjectionConfigVb injectionConfigVb : etlFeedInjectionConfigList) {
				injectionConfigVb.setVerifier(0);
				injectionConfigVb.setRecordIndicator(Constants.STATUS_INSERT);
				injectionConfigVb.setDateLastModified(systemDate);
				injectionConfigVb.setDateCreation(systemDate);
				injectionConfigVb.setEtlInjectionStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedInjectionConfigDao.doInsertionPend(injectionConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTransformedDataPend(EtlFeedMainVb vObject, String systemDate)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();

		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, 9999);
		if (etlFeedTransformationList != null && !etlFeedTransformationList.isEmpty()) {
			for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
				transformationVb.setVerifier(0);
				transformationVb.setRecordIndicator(Constants.STATUS_INSERT);
				transformationVb.setDateLastModified(systemDate);
				transformationVb.setDateCreation(systemDate);
				transformationVb.setTransformationStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedTransformationDao.doInsertionPend(transformationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

				EtlFeedTranColumnVb colVb = new EtlFeedTranColumnVb();
				colVb.setCountry(transformationVb.getCountry());
				colVb.setLeBook(transformationVb.getLeBook());
				colVb.setFeedId(transformationVb.getFeedId());
				colVb.setNodeId(transformationVb.getNodeId());
				List<EtlFeedTranColumnVb> etlFeedTranColumnList = etlFeedTranColumnsDao.getQueryResultsByParent(colVb,
						9999);
				for (EtlFeedTranColumnVb columnNodeVb : etlFeedTranColumnList) {
					columnNodeVb.setCountry(transformationVb.getCountry());
					columnNodeVb.setLeBook(transformationVb.getLeBook());
					columnNodeVb.setFeedId(transformationVb.getFeedId());
					columnNodeVb.setNodeId(transformationVb.getNodeId());
					columnNodeVb.setVerifier(0);
					columnNodeVb.setRecordIndicator(Constants.STATUS_INSERT);
					columnNodeVb.setDateLastModified(systemDate);
					columnNodeVb.setDateCreation(systemDate);
					columnNodeVb.setColumnStatus(Constants.WORK_IN_PROGRESS);
					// retVal = etlFeedTransformationDao.doInsertionPend(transformationVb);
					retVal = etlFeedTranColumnsDao.doInsertionPend(columnNodeVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			List<EtlFeedTransformationVb> vParentLst = etlFeedTransformationDao.getQueryResultsByParentNode(vObject,
					9999);
			for (EtlFeedTransformationVb vParent : vParentLst) {
				vParent.setRecordIndicator(Constants.STATUS_INSERT);
				vParent.setTransformationStatus(Constants.WORK_IN_PROGRESS);
				// transformationVb.setParentNodeId(vParent.getParentNodeId());
				// retVal = etlFeedTransformationDao.doInsertionPend(transformationVb);
				retVal = etlFeedTransformationDao.doInsertionParentPend(vParent);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}

			List<EtlFeedTransformationVb> vChildLst = etlFeedTransformationDao.getQueryResultsByChildNode(vObject,
					9999);
			for (EtlFeedTransformationVb vChild : vChildLst) {
				vChild.setRecordIndicator(Constants.STATUS_INSERT);
				vChild.setTransformationStatus(Constants.WORK_IN_PROGRESS);
				// transformationVb.setChildNodeId(vChild.getChildNodeId());
				// retVal = etlFeedTransformationDao.doInsertionPend(transformationVb);
				retVal = etlFeedTransformationDao.doInsertionChildPend(vChild);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertScheduleDataPend(EtlFeedMainVb vObject, String systemDate)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao
				.getQueryResultsByParent(vObject, 9999);
		if (etlFeedScheduleConfigList != null && !etlFeedScheduleConfigList.isEmpty()) {
			for (EtlFeedScheduleConfigVb scheduleConfigVb : etlFeedScheduleConfigList) {
				scheduleConfigVb.setVerifier(0);
				scheduleConfigVb.setRecordIndicator(Constants.STATUS_INSERT);
				scheduleConfigVb.setDateLastModified(systemDate);
				scheduleConfigVb.setDateCreation(systemDate);
				scheduleConfigVb.setEtlScheduleStatus(Constants.WORK_IN_PROGRESS);
				etlFeedScheduleConfigDao.deletePendingRecord(scheduleConfigVb);
				retVal = etlFeedScheduleConfigDao.doInsertionPend(scheduleConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				etlFeedScheduleConfigDao.deleteFeedDependencyPendRecord(vObject);
				String xml = "";
				try {
					xml = convert(scheduleConfigVb.getDependencyFeedContext(), "root"); // This method converts json
																						// object to xml string
				} catch (Exception e) {
					xml = scheduleConfigVb.getDependencyFeedContext();
				}
				String freeTextXML = xml;
				String freeTextPanelXMLData = freeTextXML.replaceAll("\n", "").replaceAll("\r", "");
				Matcher colMatcher1 = Pattern.compile("\\<feed\\>(.*?)\\<\\/feed\\>", Pattern.DOTALL)
						.matcher("<feed\\>" + freeTextPanelXMLData + "<\\/feed\\>");
				while (colMatcher1.find()) {
					String colData = colMatcher1.group(1);
					String fId = CommonUtils.getValueForXmlTag(colData, "id");
					vObject.setDependencyFeedId(fId);
					etlFeedScheduleConfigDao.doInsertionFeedDependencyPend(vObject);
				}
			}
		}
		EtlFeedAlertConfigVb vObj = new EtlFeedAlertConfigVb();
		vObj.setCountry(vObject.getCountry());
		vObj.setLeBook(vObject.getLeBook());
		vObj.setFeedId(vObject.getFeedId());
		List<EtlFeedAlertConfigVb> etlFeedAlertLists = etlFeedAlertconfigDao.getQueryResults(vObj, 9999);
		if (etlFeedAlertLists != null && !etlFeedAlertLists.isEmpty()) {
			for (EtlFeedAlertConfigVb alertConfigVb : etlFeedAlertLists) {
				alertConfigVb.setVerifier(0);
				alertConfigVb.setRecordIndicator(Constants.STATUS_INSERT);
				alertConfigVb.setDateLastModified(systemDate);
				alertConfigVb.setDateCreation(systemDate);
				alertConfigVb.setFeedAlertStatus(Constants.WORK_IN_PROGRESS);
				retVal = etlFeedAlertconfigDao.doInsertionPend(alertConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doApproveForTransaction(EtlFeedMainVb vObject, boolean staticDelete)
			throws RuntimeCustomException {
		strErrorDesc = "";
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		return doApproveRecord(vObject, staticDelete);
	}

	public ExceptionCode doApproveRecord(EtlFeedMainVb vObject, boolean staticDelete) throws RuntimeCustomException {
		List<EtlFeedMainVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		try {
			EtlFeedMainVb vObjectlocalPend = new EtlFeedMainVb();
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_UPL_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObjectlocalPend = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);

			if (vObjectlocalPend.getMaker() == getIntCurrentUserId()) {
				exceptionCode = getResultObject(Constants.MAKER_CANNOT_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = insertApprovePublishRecord(vObject, false, vObjectlocalPend);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				deleteAllPendData(vObject, "Y");
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
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

	public ExceptionCode insertApprovePublishRecord(EtlFeedMainVb vObject, Boolean verifyFlag,
			EtlFeedMainVb vObjectlocalPend) {
		ExceptionCode exceptionCode = new ExceptionCode();
		setServiceDefaults();
		try {
			if (verifyFlag) {
				strCurrentOperation = Constants.PUBLISH;
				vObjectlocalPend.setVerifier(0);
				vObjectlocalPend.setFeedStatus(2); // published and work in Progress.
				// vObjectlocalUpl.setRecordIndicator(0); // Approved
				String systemDate = getSystemDate();
				vObjectlocalPend.setDateLastModified(systemDate);
				vObjectlocalPend.setDateCreation(systemDate);
				vObjectlocalPend.setMaker(getIntCurrentUserId());

				List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, 9999);
				List<EtlExtractionSummaryFieldsVb> extractionSummaryFieldsList = etlExtractionSummaryFieldsDao
						.getQueryResultsByParent(vObject, 9999);
				List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao
						.getQueryResultsByParent(vObject, 9999);
				boolean dataExists = true;
				if (etlFeedSourceList == null || etlFeedSourceList.isEmpty() || extractionSummaryFieldsList == null
						|| extractionSummaryFieldsList.isEmpty() || etlFeedScheduleConfigList == null
						|| etlFeedScheduleConfigList.isEmpty()) {
					dataExists = false;
				} else {
//					if("INJ".equalsIgnoreCase(vObject.getFeedType()) || "DB_LINK".equalsIgnoreCase(vObject.getFeedType())) {
					List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
							.getQueryResultsByParent(vObject, 9999);
					if (etlTransformedColumnsList == null || etlTransformedColumnsList.isEmpty()) {
						dataExists = false;
					}
//					}
					if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
						List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
								.getQueryResultsByParent(vObject, 9999);
						if (etlFeedTransformationList == null || etlFeedTransformationList.isEmpty()) {
							dataExists = false;
						}
					}
				}
				if (dataExists == false) {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Need to feed data for all tabs before publishing the Feed Id["
							+ vObjectlocalPend.getFeedId() + "]");
					return exceptionCode;
				}
				retVal = doInsertionPend(vObjectlocalPend);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				insertSourceDataPend(vObject, systemDate);
				insertExtractionDataPend(vObject, systemDate);
//				if("INJ".equalsIgnoreCase(vObject.getFeedType()) || "DB_LINK".equalsIgnoreCase(vObject.getFeedType())) {
				insertTargetDataPend(vObject, systemDate);
//				}
				if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
					insertTransformedDataPend(vObject, systemDate);
				}
				insertScheduleDataPend(vObject, systemDate);
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			} else {
				strCurrentOperation = Constants.APPROVE;
				if (vObjectlocalPend.getRecordIndicator() == Constants.DELETE_PENDING) {
					if (vObjectlocalPend.isStaticDelete()) {
						vObjectlocalPend.setRecordIndicator(0);
						updateAllApproveDataStatus(vObjectlocalPend, Constants.PUBLISHED_AND_DELETED, "APPR");
						exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
					} else {
						deleteAllApproveData(vObjectlocalPend);
						exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
					}
					return exceptionCode;
				} else if (vObjectlocalPend.getRecordIndicator() == Constants.MODIFY_PENDING) {
					deleteAllApproveData(vObjectlocalPend);
				} else if (vObjectlocalPend.getRecordIndicator() == Constants.STATUS_ZERO
						&& vObjectlocalPend.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
					deleteAllApproveData(vObjectlocalPend);
				}
				vObjectlocalPend.setRecordIndicator(Constants.STATUS_ZERO); // Approve
				vObjectlocalPend.setFeedStatus(Constants.STATUS_ZERO); // Approve
				vObjectlocalPend.setVerifier(intCurrentUserId);
				// vObjectlocalPend.setMaker(getIntCurrentUserId());
				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				vObject.setDateCreation(systemDate);
				retVal = doInsertionAppr(vObjectlocalPend);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				strApproveOperation = Constants.APPROVE;
				int uplReocrds = 9999;
				List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, uplReocrds);
				if (etlFeedSourceList == null || etlFeedSourceList.isEmpty()) {
					uplReocrds = 1;
				}
				insertSourceDataApprove(vObject, uplReocrds);
				insertExtractionDataApprove(vObject, uplReocrds);
//				if("INJ".equalsIgnoreCase(vObject.getFeedType()) || "DB_LINK".equalsIgnoreCase(vObject.getFeedType())) {
				insertTargetDataApprove(vObject, uplReocrds);
//				}
				if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
					insertTransformedDataApprove(vObject, uplReocrds);
				}
				insertScheduleDataApprove(vObject, uplReocrds);
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}
			return exceptionCode;
		} catch (Exception e) {
			strErrorDesc = e.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	protected ExceptionCode insertSourceDataApprove(EtlFeedMainVb vObject, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// Insert ETL Source Connections
		List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedSourceList != null && !etlFeedSourceList.isEmpty()) {
			for (EtlFeedSourceVb sourceVb : etlFeedSourceList) {
				sourceVb.setVerifier(intCurrentUserId);
				sourceVb.setRecordIndicator(Constants.STATUS_ZERO);
				sourceVb.setFeedSourceStatus(Constants.PUBLISHED);
				retVal = etlFeedSourceDao.doInsertionAppr(sourceVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Insert ETL Tables
		List<ETLFeedTablesVb> etlFeedTablesLists = etlFeedTablesDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
			for (ETLFeedTablesVb tablesVb : etlFeedTablesLists) {
				tablesVb.setVerifier(intCurrentUserId);
				tablesVb.setRecordIndicator(Constants.STATUS_ZERO);
				tablesVb.setFeedTableStatus(Constants.PUBLISHED);
				tablesVb.setTableTypeAt(2103);
				retVal = etlFeedTablesDao.doInsertionAppr(tablesVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// Insert ETL Columns
		List<ETLFeedColumnsVb> etlFeedColumnsLists = etlFeedColumnsDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedColumnsLists != null && !etlFeedColumnsLists.isEmpty()) {
			for (ETLFeedColumnsVb columnsVb : etlFeedColumnsLists) {
				columnsVb.setVerifier(intCurrentUserId);
				columnsVb.setRecordIndicator(Constants.STATUS_ZERO);
				columnsVb.setFeedColumnStatus(Constants.PUBLISHED);
				retVal = etlFeedColumnsDao.doInsertionAppr(columnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Insert ETL Relations
		List<EtlFeedRelationVb> etlFeedRelationLists = etlFeedRelationDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedRelationLists != null && !etlFeedRelationLists.isEmpty()) {
			for (EtlFeedRelationVb relationVb : etlFeedRelationLists) {
				relationVb.setVerifier(intCurrentUserId);
				relationVb.setRecordIndicator(Constants.STATUS_ZERO);
				relationVb.setFeedRelationStatus(Constants.PUBLISHED);
				relationVb.setFilterContext(relationVb.getFilterCondition());
				if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
					vObject.setEtlFeedTablesList(etlFeedTablesLists);
				}
				relationVb.setRelationContext(doInsertUpdateRelationsWthRegExp(vObject, relationVb));
				retVal = etlFeedRelationDao.doInsertionAppr(relationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertExtractionDataApprove(EtlFeedMainVb vObject, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		List<EtlExtractionSummaryFieldsVb> extractionSummaryFieldsList = etlExtractionSummaryFieldsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (extractionSummaryFieldsList != null && !extractionSummaryFieldsList.isEmpty()) {
			for (EtlExtractionSummaryFieldsVb summaryFieldsVb : extractionSummaryFieldsList) {
				summaryFieldsVb.setVerifier(0);
				summaryFieldsVb.setRecordIndicator(Constants.STATUS_ZERO);
				summaryFieldsVb.setEtlFieldsStatus(Constants.PUBLISHED);
				summaryFieldsVb.setUserId(getIntCurrentUserId());
				retVal = etlExtractionSummaryFieldsDao.doInsertionAppr(summaryFieldsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTransformationDataApprove(EtlFeedMainVb vObject, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// ETL Feed Transformation
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approveOrPend);
		for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
			transformationVb.setVerifier(0);
			transformationVb.setRecordIndicator(Constants.STATUS_ZERO);
			transformationVb.setTransformationStatus(Constants.PUBLISHED);
			retVal = etlFeedTransformationDao.doInsertionAppr(transformationVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		// ETL Feed Transformation Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		for (EtlTransformedColumnsVb transformedColumnsVb : etlTransformedColumnsList) {
			transformedColumnsVb.setVerifier(0);
			transformedColumnsVb.setRecordIndicator(Constants.STATUS_ZERO);
			transformedColumnsVb.setFeedColumnStatus(Constants.PUBLISHED);
			retVal = etlTransformedColumnsDao.doInsertionAppr(transformedColumnsVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTargetDataApprove(EtlFeedMainVb vObject, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;

		// ETL Transformed Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlTransformedColumnsList != null && !etlTransformedColumnsList.isEmpty()) {
			for (EtlTransformedColumnsVb transformedColumnsVb : etlTransformedColumnsList) {
				transformedColumnsVb.setVerifier(0);
				transformedColumnsVb.setRecordIndicator(Constants.STATUS_ZERO);
				transformedColumnsVb.setFeedColumnStatus(Constants.PUBLISHED);
				retVal = etlTransformedColumnsDao.doInsertionAppr(transformedColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// ETL Feed Destination
		List<EtlFeedDestinationVb> etlFeedDestinationList = etlFeedDestinationDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedDestinationList != null && !etlFeedDestinationList.isEmpty()) {
			for (EtlFeedDestinationVb feedDestinationVb : etlFeedDestinationList) {
				feedDestinationVb.setVerifier(0);
				feedDestinationVb.setRecordIndicator(Constants.STATUS_ZERO);
				feedDestinationVb.setFeedTransformStatus(Constants.PUBLISHED);
				retVal = etlFeedDestinationDao.doInsertionAppr(feedDestinationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// ETL Target Columns
		List<EtlTargetColumnsVb> etlTargetColumnsList = etlTargetColumnsDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlTargetColumnsList != null && !etlTargetColumnsList.isEmpty()) {
			for (EtlTargetColumnsVb targetColumnsVb : etlTargetColumnsList) {
				targetColumnsVb.setVerifier(0);
				targetColumnsVb.setRecordIndicator(Constants.STATUS_ZERO);
				targetColumnsVb.setFeedColumnStatus(Constants.PUBLISHED);
				retVal = etlTargetColumnsDao.doInsertionAppr(targetColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Feed Loading Mappings
		List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = etlFeedLoadMappingDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedLoadMappingList != null && !etlFeedLoadMappingList.isEmpty()) {
			for (EtlFeedLoadMappingVb loadMappingVb : etlFeedLoadMappingList) {
				loadMappingVb.setVerifier(0);
				loadMappingVb.setRecordIndicator(Constants.STATUS_ZERO);
				loadMappingVb.setFeedMappingStatus(Constants.PUBLISHED);
				retVal = etlFeedLoadMappingDao.doInsertionAppr(loadMappingVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Etl Feed Injection Config
		List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = etlFeedInjectionConfigDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedInjectionConfigList != null && !etlFeedInjectionConfigList.isEmpty()) {
			for (EtlFeedInjectionConfigVb injectionConfigVb : etlFeedInjectionConfigList) {
				injectionConfigVb.setVerifier(0);
				injectionConfigVb.setRecordIndicator(Constants.STATUS_ZERO);
				injectionConfigVb.setEtlInjectionStatus(Constants.PUBLISHED);
				retVal = etlFeedInjectionConfigDao.doInsertionAppr(injectionConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTransformedDataApprove(EtlFeedMainVb vObject, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;

		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedTransformationList != null && !etlFeedTransformationList.isEmpty()) {
			for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
				transformationVb.setVerifier(0);
				transformationVb.setRecordIndicator(Constants.STATUS_ZERO);
				transformationVb.setTransformationStatus(Constants.PUBLISHED);
				retVal = etlFeedTransformationDao.doInsertionAppr(transformationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

				EtlFeedTranColumnVb colVb = new EtlFeedTranColumnVb();
				colVb.setCountry(transformationVb.getCountry());
				colVb.setLeBook(transformationVb.getLeBook());
				colVb.setFeedId(transformationVb.getFeedId());
				colVb.setNodeId(transformationVb.getNodeId());
				List<EtlFeedTranColumnVb> etlFeedTranColumnList = etlFeedTranColumnsDao.getQueryResultsByParent(colVb,
						approveOrPend);
				for (EtlFeedTranColumnVb columnNodeVb : etlFeedTranColumnList) {
					columnNodeVb.setCountry(transformationVb.getCountry());
					columnNodeVb.setLeBook(transformationVb.getLeBook());
					columnNodeVb.setFeedId(transformationVb.getFeedId());
					columnNodeVb.setNodeId(transformationVb.getNodeId());
					columnNodeVb.setVerifier(0);
					columnNodeVb.setRecordIndicator(Constants.STATUS_ZERO);
					columnNodeVb.setColumnStatus(Constants.PUBLISHED);
					retVal = etlFeedTranColumnsDao.doInsertionAppr(columnNodeVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			List<EtlFeedTransformationVb> vParentLst = etlFeedTransformationDao.getQueryResultsByParentNode(vObject,
					approveOrPend);
			for (EtlFeedTransformationVb vParent : vParentLst) {
				vParent.setRecordIndicator(Constants.STATUS_ZERO);
				vParent.setTransformationStatus(Constants.PUBLISHED);
				// transformationVb.setParentNodeId(vParent.getParentNodeId());
				retVal = etlFeedTransformationDao.doInsertionParent(vParent);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}

			List<EtlFeedTransformationVb> vChildLst = etlFeedTransformationDao.getQueryResultsByChildNode(vObject,
					approveOrPend);
			for (EtlFeedTransformationVb vChild : vChildLst) {
				// transformationVb.setChildNodeId(vChild.getChildNodeId());
				vChild.setRecordIndicator(Constants.STATUS_ZERO);
				vChild.setTransformationStatus(Constants.PUBLISHED);
				retVal = etlFeedTransformationDao.doInsertionChild(vChild);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}

		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertScheduleDataApprove(EtlFeedMainVb vObject, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao
				.getQueryResultsByParent(vObject, approveOrPend);
		vObject.setFeedStatus(Constants.PUBLISHED);
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		if (etlFeedScheduleConfigList != null && !etlFeedScheduleConfigList.isEmpty()) {
			for (EtlFeedScheduleConfigVb scheduleConfigVb : etlFeedScheduleConfigList) {
				scheduleConfigVb.setVerifier(0);
				scheduleConfigVb.setRecordIndicator(Constants.STATUS_ZERO);
				scheduleConfigVb.setEtlScheduleStatus(Constants.PUBLISHED);
				retVal = etlFeedScheduleConfigDao.doInsertionAppr(scheduleConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				String xml = "";
				try {
					xml = convert(scheduleConfigVb.getDependencyFeedContext(), "root"); // This method converts json
																						// object to xml string
				} catch (Exception e) {
					xml = scheduleConfigVb.getDependencyFeedContext();
				}
				String freeTextXML = xml;
				String freeTextPanelXMLData = freeTextXML.replaceAll("\n", "").replaceAll("\r", "");
				Matcher colMatcher1 = Pattern.compile("\\<feed\\>(.*?)\\<\\/feed\\>", Pattern.DOTALL)
						.matcher("<feed\\>" + freeTextPanelXMLData + "<\\/feed\\>");
				while (colMatcher1.find()) {
					String colData = colMatcher1.group(1);
					String fId = CommonUtils.getValueForXmlTag(colData, "id");
					vObject.setDependencyFeedId(fId);
					etlFeedScheduleConfigDao.doInsertionFeedDependency(vObject);
				}
			}
		}
		EtlFeedAlertConfigVb vObj = new EtlFeedAlertConfigVb();
		vObj.setCountry(vObject.getCountry());
		vObj.setLeBook(vObject.getLeBook());
		vObj.setFeedId(vObject.getFeedId());
		List<EtlFeedAlertConfigVb> etlFeedAlertLists = etlFeedAlertconfigDao.getQueryResults(vObj, approveOrPend);
		if (etlFeedAlertLists != null && !etlFeedAlertLists.isEmpty()) {
			for (EtlFeedAlertConfigVb alertConfigVb : etlFeedAlertLists) {
				alertConfigVb.setVerifier(intCurrentUserId);
				alertConfigVb.setRecordIndicator(Constants.STATUS_ZERO);
				alertConfigVb.setFeedAlertStatus(Constants.PUBLISHED);
				retVal = etlFeedAlertconfigDao.doInsertionAppr(alertConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doRejectForTransaction(EtlFeedMainVb vObject) throws RuntimeCustomException {
		strErrorDesc = "";
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		return doRejectRecord(vObject);
	}

	public ExceptionCode doRejectRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		EtlFeedMainVb vObjectlocal = null;
		List<EtlFeedMainVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strErrorDesc = "";
		strCurrentOperation = Constants.REJECT;
		vObject.setMaker(getIntCurrentUserId());
		try {
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				deleteAllUplData(vObject, "Y");
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				deleteAllPendData(vObject, "Y");
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				deleteAllPendData(vObject, "Y");
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		try {
			return doDeleteRecordForNonTrans(vObject);
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

	protected ExceptionCode doDeleteRecordForNonTrans(EtlFeedMainVb vObject) throws RuntimeCustomException {
		EtlFeedMainVb vObjectlocal = null;
		List<EtlFeedMainVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		collTemp = selectApprovedRecord(vObject);

		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
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
			vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
			vObjectlocal.setDateCreation(vObject.getDateCreation());
		}
		// vObjectlocal.setDateCreation(vObject.getDateCreation());
		vObjectlocal.setMaker(getIntCurrentUserId());
		vObjectlocal.setRecordIndicator(Constants.STATUS_DELETE);
		vObjectlocal.setVerifier(0);
		retVal = doInsertionPendWithDc(vObjectlocal);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setRecordIndicator(Constants.STATUS_DELETE);
		vObject.setVerifier(0);

//		need to write methods for delete
		int approvePendRecord = 0;
		// Source Data
		insertSourceDataPendWithDC(vObject, Constants.STATUS_DELETE, approvePendRecord);

		// Extraction Data
		insertExtractionDataPendWithDC(vObject, Constants.STATUS_DELETE, approvePendRecord);

//		if("INJ".equalsIgnoreCase(vObject.getFeedType()) || "DB_LINK".equalsIgnoreCase(vObject.getFeedType())) {
		insertTargetDataPendWithDC(vObject, Constants.STATUS_DELETE, approvePendRecord);
//		}
		if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
			insertTransformationDataPendWithDC(vObject, Constants.STATUS_DELETE, approvePendRecord);
		}

		// Etl Feed Target Data
		insertTargetDataPendWithDC(vObject, Constants.STATUS_DELETE, approvePendRecord);

		// Etl Feed Schedule Config
		insertScheduleDataPendWithDC(vObject, Constants.STATUS_DELETE, approvePendRecord);

		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertSourceDataPendWithDC(EtlFeedMainVb vObject, int recordIndicator,
			int approvePendRecord) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// Insert ETL Source Connections
		List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, approvePendRecord);
		if (etlFeedSourceList != null && !etlFeedSourceList.isEmpty()) {
			for (EtlFeedSourceVb sourceVb : etlFeedSourceList) {
				sourceVb.setVerifier(0);
				sourceVb.setMaker(getIntCurrentUserId());
				sourceVb.setRecordIndicator(recordIndicator);
				sourceVb.setDateCreation(sourceVb.getDateCreation());
				retVal = etlFeedSourceDao.doInsertionPendWithDc(sourceVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Insert ETL Tables
		List<ETLFeedTablesVb> etlFeedTablesLists = etlFeedTablesDao.getQueryResultsByParent(vObject, approvePendRecord);
		if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
			for (ETLFeedTablesVb tablesVb : etlFeedTablesLists) {
				tablesVb.setVerifier(0);
				tablesVb.setMaker(getIntCurrentUserId());
				tablesVb.setRecordIndicator(recordIndicator);
				tablesVb.setDateCreation(tablesVb.getDateCreation());
				tablesVb.setTableTypeAt(2103);
				retVal = etlFeedTablesDao.doInsertionPendWithDc(tablesVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// Insert ETL Columns
		List<ETLFeedColumnsVb> etlFeedColumnsLists = etlFeedColumnsDao.getQueryResultsByParent(vObject,
				approvePendRecord);
		if (etlFeedColumnsLists != null && !etlFeedColumnsLists.isEmpty()) {
			for (ETLFeedColumnsVb columnsVb : etlFeedColumnsLists) {
				columnsVb.setVerifier(0);
				columnsVb.setMaker(getIntCurrentUserId());
				columnsVb.setRecordIndicator(recordIndicator);
				columnsVb.setDateCreation(columnsVb.getDateCreation());
				retVal = etlFeedColumnsDao.doInsertionPendWithDc(columnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// Insert ETL Relations
		List<EtlFeedRelationVb> etlFeedRelationLists = etlFeedRelationDao.getQueryResultsByParent(vObject,
				approvePendRecord);
		if (etlFeedRelationLists != null && !etlFeedRelationLists.isEmpty()) {
			for (EtlFeedRelationVb relationVb : etlFeedRelationLists) {
				relationVb.setVerifier(0);
				relationVb.setMaker(getIntCurrentUserId());
				relationVb.setRecordIndicator(recordIndicator);
				relationVb.setDateCreation(relationVb.getDateCreation());
				if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
					vObject.setEtlFeedTablesList(etlFeedTablesLists);
				}
				relationVb.setRelationContext(doInsertUpdateRelationsWthRegExp(vObject, relationVb));
				retVal = etlFeedRelationDao.doInsertionPendWithDc(relationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertExtractionDataPendWithDC(EtlFeedMainVb vObject, int recordIndicator,
			int approvePendRecord) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<EtlExtractionSummaryFieldsVb> extractionSummaryFieldsList = etlExtractionSummaryFieldsDao
				.getQueryResultsByParent(vObject, approvePendRecord);
		if (extractionSummaryFieldsList != null && !extractionSummaryFieldsList.isEmpty()) {
			for (EtlExtractionSummaryFieldsVb summaryFieldsVb : extractionSummaryFieldsList) {
				summaryFieldsVb.setVerifier(0);
				summaryFieldsVb.setMaker(getIntCurrentUserId());
				summaryFieldsVb.setRecordIndicator(recordIndicator);
				summaryFieldsVb.setDateCreation(summaryFieldsVb.getDateCreation());
				summaryFieldsVb.setUserId(getIntCurrentUserId());
				retVal = etlExtractionSummaryFieldsDao.doInsertionPendWithDc(summaryFieldsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTransformationDataPendWithDC(EtlFeedMainVb vObject, int recordIndicator,
			int approvePendRecord) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// ETL Feed Transformation
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approvePendRecord);
		if (etlFeedTransformationList != null && !etlFeedTransformationList.isEmpty()) {
			for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
				transformationVb.setVerifier(0);
				transformationVb.setMaker(getIntCurrentUserId());
				transformationVb.setRecordIndicator(recordIndicator);
				transformationVb.setDateCreation(transformationVb.getDateCreation());
				retVal = etlFeedTransformationDao.doInsertionPendWithDc(transformationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				List<EtlFeedTransformationVb> vParentLst = etlFeedTransformationDao.getQueryResultsByParentNode(vObject,
						approvePendRecord);
				for (EtlFeedTransformationVb vParent : vParentLst) {
					transformationVb.setParentNodeId(vParent.getParentNodeId());
					retVal = etlFeedTransformationDao.doInsertionParentPend(transformationVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}

				List<EtlFeedTransformationVb> vChildLst = etlFeedTransformationDao.getQueryResultsByChildNode(vObject,
						approvePendRecord);
				for (EtlFeedTransformationVb vChild : vChildLst) {
					transformationVb.setChildNodeId(vChild.getChildNodeId());
					retVal = etlFeedTransformationDao.doInsertionChildPend(transformationVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
				EtlFeedTranColumnVb colVb = new EtlFeedTranColumnVb();
				colVb.setCountry(transformationVb.getCountry());
				colVb.setLeBook(transformationVb.getLeBook());
				colVb.setFeedId(transformationVb.getFeedId());
				colVb.setNodeId(transformationVb.getNodeId());
				List<EtlFeedTranColumnVb> etlFeedTranColumnList = etlFeedTranColumnsDao.getQueryResultsByParent(colVb,
						approvePendRecord);
				for (EtlFeedTranColumnVb columnNodeVb : etlFeedTranColumnList) {
					columnNodeVb.setCountry(transformationVb.getCountry());
					columnNodeVb.setLeBook(transformationVb.getLeBook());
					columnNodeVb.setFeedId(transformationVb.getFeedId());
					columnNodeVb.setNodeId(transformationVb.getNodeId());
					columnNodeVb.setVerifier(0);
					columnNodeVb.setMaker(getIntCurrentUserId());
					columnNodeVb.setRecordIndicator(recordIndicator);
					columnNodeVb.setDateCreation(transformationVb.getDateCreation());
					retVal = etlFeedTransformationDao.doInsertionPendWithDc(transformationVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertTargetDataPendWithDC(EtlFeedMainVb vObject, int recordIndicator,
			int approvePendRecord) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;

		// ETL Transformed Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approvePendRecord);
		if (etlTransformedColumnsList != null && !etlTransformedColumnsList.isEmpty()) {
			for (EtlTransformedColumnsVb transformedColumnsVb : etlTransformedColumnsList) {
				transformedColumnsVb.setVerifier(0);
				transformedColumnsVb.setMaker(getIntCurrentUserId());
				transformedColumnsVb.setRecordIndicator(recordIndicator);
				transformedColumnsVb.setDateCreation(transformedColumnsVb.getDateCreation());
				retVal = etlTransformedColumnsDao.doInsertionPendWithDc(transformedColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		List<EtlFeedDestinationVb> etlFeedDestinationList = etlFeedDestinationDao.getQueryResultsByParent(vObject,
				approvePendRecord);
		if (etlFeedDestinationList != null && !etlFeedDestinationList.isEmpty()) {
			for (EtlFeedDestinationVb feedDestinationVb : etlFeedDestinationList) {
				feedDestinationVb.setVerifier(0);
				feedDestinationVb.setMaker(getIntCurrentUserId());
				feedDestinationVb.setRecordIndicator(recordIndicator);
				feedDestinationVb.setDateCreation(feedDestinationVb.getDateCreation());
				retVal = etlFeedDestinationDao.doInsertionPendWithDc(feedDestinationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// ETL Target Columns
		List<EtlTargetColumnsVb> etlTargetColumnsList = etlTargetColumnsDao.getQueryResultsByParent(vObject,
				approvePendRecord);
		if (etlTargetColumnsList != null && !etlTargetColumnsList.isEmpty()) {
			for (EtlTargetColumnsVb targetColumnsVb : etlTargetColumnsList) {
				targetColumnsVb.setVerifier(0);
				targetColumnsVb.setMaker(getIntCurrentUserId());
				targetColumnsVb.setRecordIndicator(recordIndicator);
				targetColumnsVb.setDateCreation(targetColumnsVb.getDateCreation());
				retVal = etlTargetColumnsDao.doInsertionPendWithDc(targetColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = etlFeedLoadMappingDao.getQueryResultsByParent(vObject,
				approvePendRecord);
		if (etlFeedLoadMappingList != null && !etlFeedLoadMappingList.isEmpty()) {
			for (EtlFeedLoadMappingVb loadMappingVb : etlFeedLoadMappingList) {
				loadMappingVb.setVerifier(0);
				loadMappingVb.setMaker(getIntCurrentUserId());
				loadMappingVb.setRecordIndicator(recordIndicator);
				loadMappingVb.setDateCreation(loadMappingVb.getDateCreation());
				retVal = etlFeedLoadMappingDao.doInsertionPendWithDc(loadMappingVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = etlFeedInjectionConfigDao
				.getQueryResultsByParent(vObject, approvePendRecord);
		if (etlFeedInjectionConfigList != null && !etlFeedInjectionConfigList.isEmpty()) {
			for (EtlFeedInjectionConfigVb injectionConfigVb : etlFeedInjectionConfigList) {
				injectionConfigVb.setVerifier(0);
				injectionConfigVb.setMaker(getIntCurrentUserId());
				injectionConfigVb.setRecordIndicator(recordIndicator);
				injectionConfigVb.setDateCreation(injectionConfigVb.getDateCreation());
				retVal = etlFeedInjectionConfigDao.doInsertionPendWithDc(injectionConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode insertScheduleDataPendWithDC(EtlFeedMainVb vObject, int recordIndicator,
			int approvePendRecord) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao
				.getQueryResultsByParent(vObject, approvePendRecord);
		if (etlFeedScheduleConfigList != null && !etlFeedScheduleConfigList.isEmpty()) {
			for (EtlFeedScheduleConfigVb scheduleConfigVb : etlFeedScheduleConfigList) {
				scheduleConfigVb.setVerifier(0);
				scheduleConfigVb.setMaker(getIntCurrentUserId());
				scheduleConfigVb.setRecordIndicator(recordIndicator);
				scheduleConfigVb.setDateCreation(scheduleConfigVb.getDateCreation());
				retVal = etlFeedScheduleConfigDao.doInsertionPendWithDc(scheduleConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				String xml = "";
				try {
					xml = convert(scheduleConfigVb.getDependencyFeedContext(), "root"); // This method converts json
																						// object to xml string
				} catch (Exception e) {
					xml = scheduleConfigVb.getDependencyFeedContext();
				}
				String freeTextXML = xml;
				String freeTextPanelXMLData = freeTextXML.replaceAll("\n", "").replaceAll("\r", "");
				Matcher colMatcher1 = Pattern.compile("\\<feed\\>(.*?)\\<\\/feed\\>", Pattern.DOTALL)
						.matcher("<feed\\>" + freeTextPanelXMLData + "<\\/feed\\>");
				while (colMatcher1.find()) {
					String colData = colMatcher1.group(1);
					String fId = CommonUtils.getValueForXmlTag(colData, "id");
					vObject.setDependencyFeedId(fId);
					etlFeedScheduleConfigDao.doInsertionFeedDependencyPend(vObject);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteApprRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		try {
			return doDeleteApprRecordForNonTrans(vObject);
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
	/*
	 * protected ExceptionCode doDeleteApprRecordForNonTrans(EtlFeedMainVb vObject)
	 * throws RuntimeCustomException { List<EtlFeedMainVb> collTemp = null;
	 * ExceptionCode exceptionCode = null; strApproveOperation = Constants.DELETE;
	 * strErrorDesc = ""; strCurrentOperation = Constants.DELETE;
	 * setServiceDefaults(); EtlFeedMainVb vObjectlocal = null;
	 * vObject.setMaker(getIntCurrentUserId());
	 * if("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))){ exceptionCode =
	 * getResultObject(Constants.BUILD_IS_RUNNING); throw
	 * buildRuntimeCustomException(exceptionCode); } collTemp =
	 * selectApprovedRecord(vObject); if (collTemp == null){ exceptionCode =
	 * getResultObject(Constants.ERRONEOUS_OPERATION); throw
	 * buildRuntimeCustomException(exceptionCode); } // If record already exists in
	 * the approved table, reject the addition if (collTemp.size() > 0 ){ int
	 * intStaticDeletionFlag =
	 * getStatus(((ArrayList<EtlFeedMainVb>)collTemp).get(0)); if
	 * (intStaticDeletionFlag == Constants.PASSIVATE){ exceptionCode =
	 * getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD); throw
	 * buildRuntimeCustomException(exceptionCode); } } else{ exceptionCode =
	 * getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD); throw
	 * buildRuntimeCustomException(exceptionCode); } vObjectlocal =
	 * ((ArrayList<EtlFeedMainVb>)collTemp).get(0);
	 * vObject.setDateCreation(vObjectlocal.getDateCreation());
	 * if(vObject.isStaticDelete()){ vObjectlocal.setMaker(getIntCurrentUserId());
	 * vObject.setVerifier(getIntCurrentUserId());
	 * vObject.setRecordIndicator(Constants.STATUS_ZERO); // setStatus(vObject,
	 * Constants.PASSIVATE); setStatus(vObjectlocal,
	 * Constants.PUBLISHED_AND_DELETED);
	 * vObjectlocal.setVerifier(getIntCurrentUserId());
	 * vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO); retVal =
	 * doUpdateAppr(vObjectlocal); String systemDate = getSystemDate();
	 * vObject.setDateLastModified(systemDate);
	 * 
	 * int approveRecord = 0; // Source Data updateSourceDataApprove(vObject,
	 * Constants.PUBLISHED_AND_DELETED, approveRecord);
	 * 
	 * // Extraction Data updateExtractionDataApprove(vObject,
	 * Constants.PUBLISHED_AND_DELETED, approveRecord);
	 * 
	 * // Etl Transformation updateTransformationDataApprove(vObject,
	 * Constants.PUBLISHED_AND_DELETED, approveRecord);
	 * 
	 * // Etl Feed Target Data updateTargetDataApprove(vObject,
	 * Constants.PUBLISHED_AND_DELETED, approveRecord);
	 * 
	 * // Etl Feed Schedule Config updateScheduleDataApprove(vObject,
	 * Constants.PUBLISHED_AND_DELETED, approveRecord);
	 * 
	 * }else{ //delete the record from the Approve Table retVal =
	 * doDeleteAppr(vObject); // vObject.setRecordIndicator(-1); String systemDate =
	 * getSystemDate(); vObject.setDateLastModified(systemDate);
	 * 
	 * //Delete All Child Table data deleteAllApproveData(vObject);
	 * 
	 * } if (retVal != Constants.SUCCESSFUL_OPERATION){ exceptionCode =
	 * getResultObject(retVal); throw buildRuntimeCustomException(exceptionCode); }
	 * if(vObject.isStaticDelete()){ setStatus(vObjectlocal,
	 * Constants.PUBLISHED_AND_DELETED); setStatus(vObject, Constants.PASSIVATE);
	 * exceptionCode = writeAuditLog(vObject,vObjectlocal); }else{ exceptionCode =
	 * writeAuditLog(null,vObject); vObject.setRecordIndicator(-1); }
	 * if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION){
	 * exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR); throw
	 * buildRuntimeCustomException(exceptionCode); } return exceptionCode; }
	 */

	protected ExceptionCode updateSourceDataApprove(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// Source Table
		List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedSourceList != null && !etlFeedSourceList.isEmpty()) {
			EtlFeedSourceVb vObj = new EtlFeedSourceVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			/*
			 * if(!verReqd) { etlFeedSourceDao.doDeleteAppr(vObj); }
			 */
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedSourceList() != null && !vObject.getEtlFeedSourceList().isEmpty()) {
				} else {
					vObject.setEtlFeedSourceList(etlFeedSourceList);
				}
			} else {
				vObject.setEtlFeedSourceList(etlFeedSourceList);
			}
		}
		if (vObject.getEtlFeedSourceList() != null) {
			for (EtlFeedSourceVb sourceVb : vObject.getEtlFeedSourceList()) {
				sourceVb.setCountry(vObject.getCountry());
				sourceVb.setLeBook(vObject.getLeBook());
				sourceVb.setFeedId(vObject.getFeedId());
				sourceVb.setVerifier(verifier);
				sourceVb.setMaker(intCurrentUserId);
				sourceVb.setRecordIndicator(recordInd); // Approve
				sourceVb.setFeedSourceStatus(status);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedSourceDao.doInsertionPend(sourceVb);
				} else {
					retVal = etlFeedSourceDao.doInsertionUpl(sourceVb);
				}
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// update ETL Tables
		List<ETLFeedTablesVb> etlFeedTablesLists = etlFeedTablesDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
			ETLFeedTablesVb vObj = new ETLFeedTablesVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedTablesList() != null && !vObject.getEtlFeedTablesList().isEmpty()) {
				} else {
					vObject.setEtlFeedTablesList(etlFeedTablesLists);
				}
			} else {
				vObject.setEtlFeedTablesList(etlFeedTablesLists);
			}
		}
		if (vObject.getEtlFeedTablesList() != null && vObject.getEtlFeedTablesList().size() > 0) {
			for (ETLFeedTablesVb tablesVb : vObject.getEtlFeedTablesList()) {
				tablesVb.setCountry(vObject.getCountry());
				tablesVb.setLeBook(vObject.getLeBook());
				tablesVb.setFeedId(vObject.getFeedId());
				tablesVb.setVerifier(verifier);
				tablesVb.setMaker(intCurrentUserId);
				tablesVb.setRecordIndicator(recordInd); // Approve
				tablesVb.setFeedTableStatus(status);
				tablesVb.setTableTypeAt(2103);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedTablesDao.doInsertionPend(tablesVb);
				} else {
					retVal = etlFeedTablesDao.doInsertionUpl(tablesVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// update ETL Columns
		List<ETLFeedColumnsVb> etlFeedColumnsLists = etlFeedColumnsDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedColumnsLists != null && !etlFeedColumnsLists.isEmpty()) {
			ETLFeedColumnsVb vObj = new ETLFeedColumnsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedColumnsList() != null && !vObject.getEtlFeedColumnsList().isEmpty()) {
				} else {
					vObject.setEtlFeedColumnsList(etlFeedColumnsLists);
				}
			} else {
				vObject.setEtlFeedColumnsList(etlFeedColumnsLists);
			}
		}
		if (vObject.getEtlFeedColumnsList() != null) {
			for (ETLFeedColumnsVb columnsVb : vObject.getEtlFeedColumnsList()) {
				columnsVb.setVerifier(verifier);
				columnsVb.setMaker(intCurrentUserId);
				columnsVb.setRecordIndicator(recordInd);
				columnsVb.setFeedColumnStatus(status);
				columnsVb.setCountry(vObject.getCountry());
				columnsVb.setLeBook(vObject.getLeBook());
				columnsVb.setFeedId(vObject.getFeedId());
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedColumnsDao.doInsertionPend(columnsVb);
				} else {
					retVal = etlFeedColumnsDao.doInsertionUpl(columnsVb);
				}
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// update ETL Relations
		List<EtlFeedRelationVb> etlFeedRelationLists = etlFeedRelationDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedRelationLists != null && !etlFeedRelationLists.isEmpty()) {
			EtlFeedRelationVb vObj = new EtlFeedRelationVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedRelationList() != null && !vObject.getEtlFeedRelationList().isEmpty()) {
				} else {
					vObject.setEtlFeedRelationList(etlFeedRelationLists);
				}
			} else {
				vObject.setEtlFeedRelationList(etlFeedRelationLists);
			}
		}
		if (vObject.getEtlFeedRelationList() != null) {
			for (EtlFeedRelationVb relationVb : vObject.getEtlFeedRelationList()) {
				relationVb.setVerifier(verifier);
				relationVb.setMaker(intCurrentUserId);
				relationVb.setRecordIndicator(recordInd);
				relationVb.setFeedRelationStatus(status);
				relationVb.setFilterContext(relationVb.getFilterCondition());
				if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
					vObject.setEtlFeedTablesList(etlFeedTablesLists);
				}
				relationVb.setRelationContext(doInsertUpdateRelationsWthRegExp(vObject, relationVb));
				relationVb.setCountry(vObject.getCountry());
				relationVb.setLeBook(vObject.getLeBook());
				relationVb.setFeedId(vObject.getFeedId());
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedRelationDao.doInsertionPend(relationVb);
				} else {
					retVal = etlFeedRelationDao.doInsertionUpl(relationVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateSourceDataUpl(EtlFeedMainVb vObject, int status, int approveOrPend)
			throws RuntimeCustomException {
		if (vObject.getTabId() == 2) {
			ExceptionCode exceptionCode = new ExceptionCode();

			// delete ETL_FEED_SOURCE_UPL
			etlFeedSourceDao.deleteUplRecord(
					new EtlFeedSourceVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

			if (vObject.getEtlFeedSourceList() != null) {
				for (EtlFeedSourceVb sourceVb : vObject.getEtlFeedSourceList()) {
					sourceVb.setVerifier(intCurrentUserId);
					sourceVb.setMaker(intCurrentUserId);
					sourceVb.setRecordIndicator(Constants.STATUS_ZERO); // Approve
					sourceVb.setFeedSourceStatus(status);
					sourceVb.setCountry(vObject.getCountry());
					sourceVb.setLeBook(vObject.getLeBook());
					sourceVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedSourceDao.doInsertionUpl(sourceVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			// delete ETL_FEED_TABLES_UPL
			etlFeedTablesDao.deleteUplRecord(
					new ETLFeedTablesVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

			if (vObject.getEtlFeedTablesList() != null) {
				for (ETLFeedTablesVb tablesVb : vObject.getEtlFeedTablesList()) {
					tablesVb.setVerifier(intCurrentUserId);
					tablesVb.setMaker(intCurrentUserId);
					tablesVb.setRecordIndicator(Constants.STATUS_ZERO); // Approve
					tablesVb.setFeedTableStatus(status);
					tablesVb.setCountry(vObject.getCountry());
					tablesVb.setLeBook(vObject.getLeBook());
					tablesVb.setFeedId(vObject.getFeedId());
					tablesVb.setTableTypeAt(2103);
					retVal = etlFeedTablesDao.doInsertionUpl(tablesVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			// delete ETL_FEED_COLUMNS_UPL
			etlFeedColumnsDao.deleteUplRecord(
					new ETLFeedColumnsVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

			if (vObject.getEtlFeedColumnsList() != null) {
				for (ETLFeedColumnsVb columnsVb : vObject.getEtlFeedColumnsList()) {
					columnsVb.setVerifier(intCurrentUserId);
					columnsVb.setMaker(intCurrentUserId);
					columnsVb.setRecordIndicator(Constants.STATUS_ZERO);
					columnsVb.setFeedColumnStatus(status);
					columnsVb.setCountry(vObject.getCountry());
					columnsVb.setLeBook(vObject.getLeBook());
					columnsVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedColumnsDao.doInsertionUpl(columnsVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			// delete ETL_FEED_RELATION_UPL
			etlFeedRelationDao.deleteUplRecord(
					new EtlFeedRelationVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

			// Select Table List to form the Table Relation for table join
			List<ETLFeedTablesVb> etlFeedTablesLists = etlFeedTablesDao.getQueryResultsByParent(vObject, approveOrPend);

			if (vObject.getEtlFeedRelationList() != null) {
				for (EtlFeedRelationVb relationVb : vObject.getEtlFeedRelationList()) {
					relationVb.setVerifier(intCurrentUserId);
					relationVb.setMaker(intCurrentUserId);
					relationVb.setRecordIndicator(Constants.STATUS_ZERO);
					relationVb.setFeedRelationStatus(status);
					relationVb.setFilterContext(relationVb.getFilterCondition());
					relationVb.setFeedRelationStatus(status);
					relationVb.setCountry(vObject.getCountry());
					relationVb.setLeBook(vObject.getLeBook());
					relationVb.setFeedId(vObject.getFeedId());
					if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
						vObject.setEtlFeedTablesList(etlFeedTablesLists);
					}
					relationVb.setRelationContext(doInsertUpdateRelationsWthRegExp(vObject, relationVb));
					retVal = etlFeedRelationDao.doInsertionUpl(relationVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateSourceDataPend(EtlFeedMainVb vObject, int status, int approveOrPend, Boolean verReqd,
			int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// Source Table
		List<EtlFeedSourceVb> etlFeedSourceList = etlFeedSourceDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedSourceList != null && !etlFeedSourceList.isEmpty()) {
			if (vObject.getEtlFeedSourceList() == null) {
				vObject.setEtlFeedSourceList(etlFeedSourceList);
			}
			EtlFeedSourceVb vObj = new EtlFeedSourceVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedSourceDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedSourceList(etlFeedSourceList);
		}
		if (vObject.getEtlFeedSourceList() != null) {
			for (EtlFeedSourceVb sourceVb : vObject.getEtlFeedSourceList()) {
				sourceVb.setVerifier(verifier);
				sourceVb.setMaker(intCurrentUserId);
				sourceVb.setRecordIndicator(recordInd); // Approve
				sourceVb.setFeedSourceStatus(status);
				sourceVb.setCountry(vObject.getCountry());
				sourceVb.setLeBook(vObject.getLeBook());
				sourceVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedSourceDao.doInsertionUpl(sourceVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// update ETL Tables
		List<ETLFeedTablesVb> etlFeedTablesLists = etlFeedTablesDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
			if (vObject.getEtlFeedTablesList() == null) {
				vObject.setEtlFeedTablesList(etlFeedTablesLists);
			}
			ETLFeedTablesVb vObj = new ETLFeedTablesVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedTablesDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedTablesList(etlFeedTablesLists);
		}
		if (vObject.getEtlFeedTablesList() != null) {
			for (ETLFeedTablesVb tablesVb : vObject.getEtlFeedTablesList()) {
				tablesVb.setVerifier(verifier);
				tablesVb.setMaker(intCurrentUserId);
				tablesVb.setRecordIndicator(recordInd); // Approve
				tablesVb.setFeedTableStatus(status);
				tablesVb.setCountry(vObject.getCountry());
				tablesVb.setLeBook(vObject.getLeBook());
				tablesVb.setFeedId(vObject.getFeedId());
				tablesVb.setTableTypeAt(2103);
				retVal = etlFeedTablesDao.doInsertionUpl(tablesVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// update ETL Columns
		List<ETLFeedColumnsVb> etlFeedColumnsLists = etlFeedColumnsDao.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedColumnsLists != null && !etlFeedColumnsLists.isEmpty()) {
			if (vObject.getEtlFeedColumnsList() == null) {
				vObject.setEtlFeedColumnsList(etlFeedColumnsLists);
			}
			ETLFeedColumnsVb vObj = new ETLFeedColumnsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedColumnsDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedColumnsList(etlFeedColumnsLists);
		}
		if (vObject.getEtlFeedColumnsList() != null) {
			for (ETLFeedColumnsVb columnsVb : vObject.getEtlFeedColumnsList()) {
				columnsVb.setVerifier(verifier);
				columnsVb.setMaker(intCurrentUserId);
				columnsVb.setRecordIndicator(recordInd);
				columnsVb.setFeedColumnStatus(status);
				columnsVb.setCountry(vObject.getCountry());
				columnsVb.setLeBook(vObject.getLeBook());
				columnsVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedColumnsDao.doInsertionUpl(columnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// update ETL Relations
		List<EtlFeedRelationVb> etlFeedRelationLists = etlFeedRelationDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedRelationLists != null && !etlFeedRelationLists.isEmpty()) {
			if (vObject.getEtlFeedRelationList() == null) {
				vObject.setEtlFeedRelationList(etlFeedRelationLists);
			}
			EtlFeedRelationVb vObj = new EtlFeedRelationVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedRelationDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedRelationList(etlFeedRelationLists);
		}
		if (vObject.getEtlFeedRelationList() != null) {
			for (EtlFeedRelationVb relationVb : vObject.getEtlFeedRelationList()) {
				relationVb.setVerifier(verifier);
				relationVb.setMaker(intCurrentUserId);
				relationVb.setRecordIndicator(recordInd);
				relationVb.setFeedRelationStatus(status);
				relationVb.setFilterContext(relationVb.getFilterCondition());
				if (etlFeedTablesLists != null && !etlFeedTablesLists.isEmpty()) {
					vObject.setEtlFeedTablesList(etlFeedTablesLists);
				}
				relationVb.setRelationContext(doInsertUpdateRelationsWthRegExp(vObject, relationVb));
				relationVb.setCountry(vObject.getCountry());
				relationVb.setLeBook(vObject.getLeBook());
				relationVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedRelationDao.doInsertionUpl(relationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateExtractionDataApprove(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// if(vObject.getTabId()<3) {
		List<EtlExtractionSummaryFieldsVb> extractionSummaryFieldsList = etlExtractionSummaryFieldsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (extractionSummaryFieldsList != null && !extractionSummaryFieldsList.isEmpty()) {
			EtlExtractionSummaryFieldsVb vObj = new EtlExtractionSummaryFieldsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlExtractionSummaryFieldsList() != null
						&& !vObject.getEtlExtractionSummaryFieldsList().isEmpty()) {
				} else {
					vObject.setEtlExtractionSummaryFieldsList(extractionSummaryFieldsList);
				}
			} else {
				vObject.setEtlExtractionSummaryFieldsList(extractionSummaryFieldsList);
			}
		}
		if (vObject.getEtlExtractionSummaryFieldsList() != null) {
			for (EtlExtractionSummaryFieldsVb summaryFieldsVb : vObject.getEtlExtractionSummaryFieldsList()) {
				summaryFieldsVb.setVerifier(verifier);
				summaryFieldsVb.setMaker(intCurrentUserId);
				summaryFieldsVb.setRecordIndicator(recordInd);
				summaryFieldsVb.setEtlFieldsStatus(status);
				summaryFieldsVb.setCountry(vObject.getCountry());
				summaryFieldsVb.setLeBook(vObject.getLeBook());
				summaryFieldsVb.setFeedId(vObject.getFeedId());
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlExtractionSummaryFieldsDao.doInsertionPend(summaryFieldsVb);
				} else {
					retVal = etlExtractionSummaryFieldsDao.doInsertionUpl(summaryFieldsVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
//		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateExtractionDataUpl(EtlFeedMainVb vObject, int status, int approveOrPend)
			throws RuntimeCustomException {
		if (vObject.getTabId() == 3) {
			ExceptionCode exceptionCode = null;
			// delete ETL_EXTRACTION_SUMMARY_FIELDS_UPL
			etlExtractionSummaryFieldsDao.deleteUplRecord(
					new EtlExtractionSummaryFieldsVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));
			if (vObject.getEtlExtractionSummaryFieldsList() != null
					&& vObject.getEtlExtractionSummaryFieldsList().size() > 0) {
				for (EtlExtractionSummaryFieldsVb summaryFieldsVb : vObject.getEtlExtractionSummaryFieldsList()) {
					summaryFieldsVb.setVerifier(intCurrentUserId);
					summaryFieldsVb.setMaker(intCurrentUserId);
					summaryFieldsVb.setRecordIndicator(Constants.STATUS_ZERO);
					summaryFieldsVb.setEtlFieldsStatus(status);
					summaryFieldsVb.setCountry(vObject.getCountry());
					summaryFieldsVb.setLeBook(vObject.getLeBook());
					summaryFieldsVb.setFeedId(vObject.getFeedId());
					retVal = etlExtractionSummaryFieldsDao.doInsertionUpl(summaryFieldsVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTransformationDataApprove(EtlFeedMainVb vObject, int status, int approveOrPend)
			throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// ETL Feed Transformation
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approveOrPend);
		for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
			transformationVb.setVerifier(intCurrentUserId);
			transformationVb.setRecordIndicator(Constants.STATUS_ZERO);
			transformationVb.setTransformationStatus(status);
			retVal = etlFeedTransformationDao.doUpdateAppr(transformationVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		for (EtlTransformedColumnsVb transformedColumnsVb : etlTransformedColumnsList) {
			transformedColumnsVb.setVerifier(intCurrentUserId);
			transformedColumnsVb.setRecordIndicator(Constants.STATUS_ZERO);
			transformedColumnsVb.setFeedColumnStatus(status);
			retVal = etlTransformedColumnsDao.doUpdateAppr(transformedColumnsVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTargetDataApprove(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// if(vObject.getTabId() <4) {
		// ETL Transformed Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlTransformedColumnsList != null && !etlTransformedColumnsList.isEmpty()) {
			EtlTransformedColumnsVb vObj = new EtlTransformedColumnsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlTransformedColumnsList() != null
						&& !vObject.getEtlTransformedColumnsList().isEmpty()) {
				} else {
					vObject.setEtlTransformedColumnsList(etlTransformedColumnsList);
				}
			} else {
				vObject.setEtlTransformedColumnsList(etlTransformedColumnsList);
			}
		}
		if (vObject.getEtlTransformedColumnsList() != null) {
			for (EtlTransformedColumnsVb transformedColumnsVb : vObject.getEtlTransformedColumnsList()) {
				transformedColumnsVb.setCountry(vObject.getCountry());
				transformedColumnsVb.setLeBook(vObject.getLeBook());
				transformedColumnsVb.setFeedId(vObject.getFeedId());
				transformedColumnsVb.setVerifier(verifier);
				transformedColumnsVb.setMaker(intCurrentUserId);
				transformedColumnsVb.setRecordIndicator(recordInd);
				transformedColumnsVb.setFeedColumnStatus(status);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlTransformedColumnsDao.doInsertionPend(transformedColumnsVb);
				} else {
					retVal = etlTransformedColumnsDao.doInsertionUpl(transformedColumnsVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Feed Destination
		List<EtlFeedDestinationVb> etlFeedDestinationList = etlFeedDestinationDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedDestinationList != null && !etlFeedDestinationList.isEmpty()) {
			EtlFeedDestinationVb vObj = new EtlFeedDestinationVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedDestinationList() != null && !vObject.getEtlFeedDestinationList().isEmpty()) {
				} else {
					vObject.setEtlFeedDestinationList(etlFeedDestinationList);
				}
			} else {
				vObject.setEtlFeedDestinationList(etlFeedDestinationList);
			}
		}
		if (vObject.getEtlFeedDestinationList() != null) {
			for (EtlFeedDestinationVb feedDestinationVb : vObject.getEtlFeedDestinationList()) {
				feedDestinationVb.setCountry(vObject.getCountry());
				feedDestinationVb.setLeBook(vObject.getLeBook());
				feedDestinationVb.setFeedId(vObject.getFeedId());
				feedDestinationVb.setVerifier(verifier);
				feedDestinationVb.setMaker(intCurrentUserId);
				feedDestinationVb.setRecordIndicator(recordInd);
				feedDestinationVb.setFeedTransformStatus(status);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedDestinationDao.doInsertionPend(feedDestinationVb);
				} else {
					retVal = etlFeedDestinationDao.doInsertionUpl(feedDestinationVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Target Columns
		List<EtlTargetColumnsVb> etlTargetColumnsList = etlTargetColumnsDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlTargetColumnsList != null && !etlTargetColumnsList.isEmpty()) {
			EtlTargetColumnsVb vObj = new EtlTargetColumnsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlTargetColumnsList() != null && !vObject.getEtlTargetColumnsList().isEmpty()) {
				} else {
					vObject.setEtlTargetColumnsList(etlTargetColumnsList);
				}
			} else {
				vObject.setEtlTargetColumnsList(etlTargetColumnsList);
			}
		}
		if (vObject.getEtlTargetColumnsList() != null) {
			for (EtlTargetColumnsVb targetColumnsVb : vObject.getEtlTargetColumnsList()) {
				targetColumnsVb.setCountry(vObject.getCountry());
				targetColumnsVb.setLeBook(vObject.getLeBook());
				targetColumnsVb.setFeedId(vObject.getFeedId());
				targetColumnsVb.setVerifier(verifier);
				targetColumnsVb.setMaker(intCurrentUserId);
				targetColumnsVb.setRecordIndicator(recordInd);
				targetColumnsVb.setFeedColumnStatus(status);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlTargetColumnsDao.doInsertionPend(targetColumnsVb);
				} else {
					retVal = etlTargetColumnsDao.doInsertionUpl(targetColumnsVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Feed Loading Mappings
		List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = etlFeedLoadMappingDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedLoadMappingList != null && !etlFeedLoadMappingList.isEmpty()) {
			EtlFeedLoadMappingVb vObj = new EtlFeedLoadMappingVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedLoadMappingList() != null && !vObject.getEtlFeedLoadMappingList().isEmpty()) {
				} else {
					vObject.setEtlFeedLoadMappingList(etlFeedLoadMappingList);
				}
			} else {
				vObject.setEtlFeedLoadMappingList(etlFeedLoadMappingList);
			}
		}
		if (vObject.getEtlFeedLoadMappingList() != null) {
			for (EtlFeedLoadMappingVb loadMappingVb : vObject.getEtlFeedLoadMappingList()) {
				loadMappingVb.setCountry(vObject.getCountry());
				loadMappingVb.setLeBook(vObject.getLeBook());
				loadMappingVb.setFeedId(vObject.getFeedId());
				loadMappingVb.setVerifier(verifier);
				loadMappingVb.setMaker(intCurrentUserId);
				loadMappingVb.setRecordIndicator(recordInd);
				loadMappingVb.setFeedMappingStatus(status);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedLoadMappingDao.doInsertionPend(loadMappingVb);
				} else {
					retVal = etlFeedLoadMappingDao.doInsertionUpl(loadMappingVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// Etl Feed Injection Config
		List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = etlFeedInjectionConfigDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedInjectionConfigList != null && !etlFeedInjectionConfigList.isEmpty()) {
			EtlFeedInjectionConfigVb vObj = new EtlFeedInjectionConfigVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedInjectionConfigList() != null
						&& !vObject.getEtlFeedInjectionConfigList().isEmpty()) {
				} else {
					vObject.setEtlFeedInjectionConfigList(etlFeedInjectionConfigList);
				}
			} else {
				vObject.setEtlFeedInjectionConfigList(etlFeedInjectionConfigList);
			}
		}
		if (vObject.getEtlFeedInjectionConfigList() != null) {
			for (EtlFeedInjectionConfigVb injectionConfigVb : vObject.getEtlFeedInjectionConfigList()) {
				injectionConfigVb.setCountry(vObject.getCountry());
				injectionConfigVb.setLeBook(vObject.getLeBook());
				injectionConfigVb.setFeedId(vObject.getFeedId());
				injectionConfigVb.setVerifier(verifier);
				injectionConfigVb.setMaker(intCurrentUserId);
				injectionConfigVb.setRecordIndicator(recordInd);
				injectionConfigVb.setEtlInjectionStatus(status);
				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedInjectionConfigDao.doInsertionPend(injectionConfigVb);
				} else {
					retVal = etlFeedInjectionConfigDao.doInsertionUpl(injectionConfigVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
//		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTransformedDataApprove(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// if(vObject.getTabId() <4) {
		// ETL Transformed Columns

		// if (vObject.getEtlFeedTranList() != null &&
		// !vObject.getEtlFeedTranList().isEmpty()) {
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedTransformationList != null && !etlFeedTransformationList.isEmpty()) {
			EtlFeedTransformationVb vObj = new EtlFeedTransformationVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			vObj.setNodeId(vObject.getNodeId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedTranList() != null && !vObject.getEtlFeedTranList().isEmpty()) {
				} else {
					vObject.setEtlFeedTransformationList(etlFeedTransformationList);
				}
			} else {
				vObject.setEtlFeedTransformationList(etlFeedTransformationList);
			}
		}

		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			List<EtlFeedTranVb> etlTranList = new ArrayList<EtlFeedTranVb>();

			List<EtlFeedTranNodeVb> transformationNodeInputList = new ArrayList<EtlFeedTranNodeVb>();
			if (vObject.getEtlFeedTransformationList() != null) {
				for (EtlFeedTransformationVb transformationVb : vObject.getEtlFeedTransformationList()) {
					EtlFeedTranVb tranVb = new EtlFeedTranVb();
					tranVb.setCountry(transformationVb.getCountry());
					tranVb.setLeBook(transformationVb.getLeBook());
					tranVb.setFeedId(transformationVb.getFeedId());
					vObject.setNodeId(transformationVb.getNodeId());

					List<EtlFeedTransformationVb> vParentLst = etlFeedTransformationDao
							.getQueryResultsByParentNode(vObject, approveOrPend);
					List<EtlFeedTranNodeVb> parentTransformationList = new ArrayList<EtlFeedTranNodeVb>();
					for (EtlFeedTransformationVb vParent : vParentLst) {
						EtlFeedTranNodeVb parentVb = new EtlFeedTranNodeVb(vParent.getParentNodeId());
						parentTransformationList.add(parentVb);
					}

					List<EtlFeedTransformationVb> vChildLst = etlFeedTransformationDao
							.getQueryResultsByChildNode(vObject, approveOrPend);
					List<EtlFeedTranNodeVb> childTransformationList = new ArrayList<EtlFeedTranNodeVb>();
					for (EtlFeedTransformationVb vChild : vChildLst) {
						EtlFeedTranNodeVb childVb = new EtlFeedTranNodeVb(vChild.getChildNodeId());
						childTransformationList.add(childVb);
					}

					EtlFeedTranColumnVb columnNodeVb = new EtlFeedTranColumnVb();
					columnNodeVb.setCountry(transformationVb.getCountry());
					columnNodeVb.setLeBook(transformationVb.getLeBook());
					columnNodeVb.setFeedId(transformationVb.getFeedId());
					columnNodeVb.setNodeId(transformationVb.getNodeId());

					List<EtlFeedTranColumnVb> etlFeedTranColumnList = etlFeedTranColumnsDao
							.getQueryResultsByParent(columnNodeVb, approveOrPend);

					EtlFeedTranNodeVb nodeVb = new EtlFeedTranNodeVb(transformationVb.getNodeId(),
							transformationVb.getNodeName(), transformationVb.getNodeDesc(), transformationVb.getxAxis(),
							transformationVb.getyAxis(), transformationVb.getTransformationId(),
							parentTransformationList, childTransformationList,
							transformationVb.getTransformationStatus(), transformationVb.getMinimizeFlag(),
							transformationVb.getGroupId(), transformationVb.getJoinType(),
							transformationVb.getCustomFlag(), transformationVb.getCustomExpression());
					nodeVb.setTranColumnList(etlFeedTranColumnList);

					transformationNodeInputList.add(nodeVb);
					tranVb.setTransformationNodeInputList(transformationNodeInputList);
					etlTranList.add(tranVb);
				}
				vObject.setEtlFeedTranList(etlTranList);
			}
			if (vObject.getEtlFeedTranList() != null && vObject.getEtlFeedTranList().size() > 0) {
				EtlFeedTranVb transformedVb = vObject.getEtlFeedTranList().get(0);
				// for (EtlFeedTranVb transformedVb : vObject.getEtlFeedTranList()) {
				for (EtlFeedTranNodeVb transformedNodeVb : transformedVb.getTransformationNodeInputList()) {
					vObject.setNodeId(transformedNodeVb.getNodeId());
					EtlFeedTransformationVb vObj = new EtlFeedTransformationVb();
					vObj.setCountry(vObject.getCountry());
					vObj.setLeBook(vObject.getLeBook());
					vObj.setFeedId(vObject.getFeedId());
					vObj.setNodeId(vObject.getNodeId());
					vObj.setVerifier(verifier);
					vObj.setMaker(intCurrentUserId);
					vObj.setRecordIndicator(recordInd);
					vObj.setTransformationStatus(status);
					vObj.setNodeName(transformedNodeVb.getNodeName());
					vObj.setNodeDesc(transformedNodeVb.getNodeDesc());
					vObj.setTransformationId(transformedNodeVb.getTransformationId());
					vObj.setxAxis(transformedNodeVb.getxAxis());
					vObj.setyAxis(transformedNodeVb.getyAxis());
					vObj.setMinimizeFlag(transformedNodeVb.getMinimizeFlag());
					vObj.setJoinType(transformedNodeVb.getJoinType());
					vObj.setGroupId(transformedNodeVb.getGroupId());
					vObj.setCustomFlag(transformedNodeVb.getCustomFlag());
					vObj.setCustomExpression(transformedNodeVb.getCustomExpression());
					if (recordInd == Constants.DELETE_PENDING) {
						retVal = etlFeedTransformationDao.doInsertionPend(vObj);
					} else {
						retVal = etlFeedTransformationDao.doInsertionUpl(vObj);
					}
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					for (EtlFeedTranNodeVb parentNodeVb : transformedNodeVb.getParentTransformationList()) {
						vObj.setParentNodeId(parentNodeVb.getNodeId());
						if (recordInd == Constants.DELETE_PENDING) {
							retVal = etlFeedTransformationDao.doInsertionParentPend(vObj);
						} else {
							retVal = etlFeedTransformationDao.doInsertionParentUpl(vObj);
						}
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
					for (EtlFeedTranNodeVb childNodeVb : transformedNodeVb.getChildTransformationList()) {
						vObj.setChildNodeId(childNodeVb.getNodeId());
						if (recordInd == Constants.DELETE_PENDING) {
							retVal = etlFeedTransformationDao.doInsertionChildPend(vObj);
						} else {
							retVal = etlFeedTransformationDao.doInsertionChildUpl(vObj);
						}
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
					for (EtlFeedTranColumnVb columnNodeVb : transformedNodeVb.getTranColumnList()) {
						columnNodeVb.setCountry(vObject.getCountry());
						columnNodeVb.setLeBook(vObject.getLeBook());
						columnNodeVb.setFeedId(vObject.getFeedId());
						columnNodeVb.setNodeId(vObject.getNodeId());
						if (recordInd == Constants.DELETE_PENDING) {
							retVal = etlFeedTranColumnsDao.doInsertionPend(columnNodeVb);
						} else {
							retVal = etlFeedTranColumnsDao.doInsertionUpl(columnNodeVb);
						}
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
				// }
			}
		}
		// }
//		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTargetDataPend(EtlFeedMainVb vObject, int status, int approveOrPend, Boolean verReqd,
			int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// if (vObject.getTabId() < 4 || vObject.getTabId() == 5) {
		// ETL Transformed Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlTransformedColumnsList != null && !etlTransformedColumnsList.isEmpty()) {
			if (vObject.getEtlTransformedColumnsList() == null) {
				vObject.setEtlTransformedColumnsList(etlTransformedColumnsList);
			}
			EtlTransformedColumnsVb vObj = new EtlTransformedColumnsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlTransformedColumnsDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlTransformedColumnsList(etlTransformedColumnsList);
		}
		if (vObject.getEtlTransformedColumnsList() != null) {
			for (EtlTransformedColumnsVb transformedColumnsVb : vObject.getEtlTransformedColumnsList()) {
				transformedColumnsVb.setVerifier(verifier);
				transformedColumnsVb.setMaker(intCurrentUserId);
				transformedColumnsVb.setRecordIndicator(recordInd);
				transformedColumnsVb.setFeedColumnStatus(status);
				transformedColumnsVb.setCountry(vObject.getCountry());
				transformedColumnsVb.setLeBook(vObject.getLeBook());
				transformedColumnsVb.setFeedId(vObject.getFeedId());
				retVal = etlTransformedColumnsDao.doInsertionUpl(transformedColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Feed Destination
		List<EtlFeedDestinationVb> etlFeedDestinationList = etlFeedDestinationDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedDestinationList != null && !etlFeedDestinationList.isEmpty()) {
			if (vObject.getEtlFeedDestinationList() == null) {
				vObject.setEtlFeedDestinationList(etlFeedDestinationList);
			}
			EtlFeedDestinationVb vObj = new EtlFeedDestinationVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedDestinationDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedDestinationList(etlFeedDestinationList);
		}
		if (vObject.getEtlFeedDestinationList() != null) {
			for (EtlFeedDestinationVb feedDestinationVb : vObject.getEtlFeedDestinationList()) {
				feedDestinationVb.setVerifier(intCurrentUserId);
				feedDestinationVb.setMaker(intCurrentUserId);
				feedDestinationVb.setRecordIndicator(recordInd);
				feedDestinationVb.setFeedTransformStatus(status);
				feedDestinationVb.setCountry(vObject.getCountry());
				feedDestinationVb.setLeBook(vObject.getLeBook());
				feedDestinationVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedDestinationDao.doInsertionUpl(feedDestinationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}

		// ETL Target Columns
		List<EtlTargetColumnsVb> etlTargetColumnsList = etlTargetColumnsDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlTargetColumnsList != null && !etlTargetColumnsList.isEmpty()) {
			if (vObject.getEtlTargetColumnsList() == null) {
				vObject.setEtlTargetColumnsList(etlTargetColumnsList);
			}
			EtlTargetColumnsVb vObj = new EtlTargetColumnsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlTargetColumnsDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlTargetColumnsList(etlTargetColumnsList);
		}
		if (vObject.getEtlTargetColumnsList() != null) {
			for (EtlTargetColumnsVb targetColumnsVb : vObject.getEtlTargetColumnsList()) {
				targetColumnsVb.setVerifier(intCurrentUserId);
				targetColumnsVb.setMaker(intCurrentUserId);
				targetColumnsVb.setRecordIndicator(recordInd);
				targetColumnsVb.setFeedColumnStatus(status);
				targetColumnsVb.setCountry(vObject.getCountry());
				targetColumnsVb.setLeBook(vObject.getLeBook());
				targetColumnsVb.setFeedId(vObject.getFeedId());
				retVal = etlTargetColumnsDao.doInsertionUpl(targetColumnsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = etlFeedLoadMappingDao.getQueryResultsByParent(vObject,
				approveOrPend);
		if (etlFeedLoadMappingList != null && !etlFeedLoadMappingList.isEmpty()) {
			if (vObject.getEtlFeedLoadMappingList() == null) {
				vObject.setEtlFeedLoadMappingList(etlFeedLoadMappingList);
			}
			EtlFeedLoadMappingVb vObj = new EtlFeedLoadMappingVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedLoadMappingDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedLoadMappingList(etlFeedLoadMappingList);
		}
		if (vObject.getEtlFeedLoadMappingList() != null) {
			for (EtlFeedLoadMappingVb loadMappingVb : vObject.getEtlFeedLoadMappingList()) {
				loadMappingVb.setVerifier(intCurrentUserId);
				loadMappingVb.setMaker(intCurrentUserId);
				loadMappingVb.setRecordIndicator(recordInd);
				loadMappingVb.setFeedMappingStatus(status);
				loadMappingVb.setCountry(vObject.getCountry());
				loadMappingVb.setLeBook(vObject.getLeBook());
				loadMappingVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedLoadMappingDao.doInsertionUpl(loadMappingVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = etlFeedInjectionConfigDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedInjectionConfigList != null && !etlFeedInjectionConfigList.isEmpty()) {
			if (vObject.getEtlFeedInjectionConfigList() == null) {
				vObject.setEtlFeedInjectionConfigList(etlFeedInjectionConfigList);
			}
			EtlFeedInjectionConfigVb vObj = new EtlFeedInjectionConfigVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedInjectionConfigDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedInjectionConfigList(etlFeedInjectionConfigList);
		}
		if (vObject.getEtlFeedInjectionConfigList() != null) {
			for (EtlFeedInjectionConfigVb injectionConfigVb : vObject.getEtlFeedInjectionConfigList()) {
				injectionConfigVb.setVerifier(intCurrentUserId);
				injectionConfigVb.setMaker(intCurrentUserId);
				injectionConfigVb.setRecordIndicator(recordInd);
				injectionConfigVb.setEtlInjectionStatus(status);
				injectionConfigVb.setCountry(vObject.getCountry());
				injectionConfigVb.setLeBook(vObject.getLeBook());
				injectionConfigVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedInjectionConfigDao.doInsertionUpl(injectionConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// }
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTransformedDataPend(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}

//		if (vObject.getTabId() < 4 || vObject.getTabId() == 5) {
		// ETL Transformed Columns
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedTransformationList != null && !etlFeedTransformationList.isEmpty()) {
			if (vObject.getEtlFeedTransformationList() == null) {
				vObject.setEtlFeedTransformationList(etlFeedTransformationList);
			}
			EtlFeedTransformationVb vObj = new EtlFeedTransformationVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedTransformationDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedTransformationList(etlFeedTransformationList);
		}
		if (vObject.getEtlFeedTransformationList() != null) {
			for (EtlFeedTransformationVb transformationVb : vObject.getEtlFeedTransformationList()) {
				transformationVb.setVerifier(verifier);
				transformationVb.setMaker(intCurrentUserId);
				transformationVb.setRecordIndicator(recordInd);
				transformationVb.setTransformationStatus(status);
				transformationVb.setCountry(vObject.getCountry());
				transformationVb.setLeBook(vObject.getLeBook());
				transformationVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedTransformationDao.doInsertionUpl(transformationVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				EtlFeedTranVb tranVb = new EtlFeedTranVb();
				tranVb.setCountry(transformationVb.getCountry());
				tranVb.setLeBook(transformationVb.getLeBook());
				tranVb.setFeedId(transformationVb.getFeedId());

				vObject.setNodeId(transformationVb.getNodeId());

				List<EtlFeedTransformationVb> vParentLst = etlFeedTransformationDao.getQueryResultsByParentNode(vObject,
						approveOrPend);
				etlFeedTransformationDao.deletePendingRecordParentNode(transformationVb);
				for (EtlFeedTransformationVb vParent : vParentLst) {
					transformationVb.setParentNodeId(vParent.getParentNodeId());
					transformationVb.setCountry(vObject.getCountry());
					transformationVb.setLeBook(vObject.getLeBook());
					transformationVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedTransformationDao.doInsertionParentUpl(vParent);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
				List<EtlFeedTransformationVb> vChildLst = etlFeedTransformationDao.getQueryResultsByChildNode(vObject,
						approveOrPend);
				etlFeedTransformationDao.deletePendingRecordChildNode(transformationVb);
				for (EtlFeedTransformationVb vChild : vChildLst) {
					transformationVb.setChildNodeId(vChild.getChildNodeId());
					transformationVb.setCountry(vObject.getCountry());
					transformationVb.setLeBook(vObject.getLeBook());
					transformationVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedTransformationDao.doInsertionChildUpl(transformationVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
				EtlFeedTranColumnVb colVb = new EtlFeedTranColumnVb();
				colVb.setCountry(transformationVb.getCountry());
				colVb.setLeBook(transformationVb.getLeBook());
				colVb.setFeedId(transformationVb.getFeedId());
				colVb.setNodeId(transformationVb.getNodeId());
				List<EtlFeedTranColumnVb> etlFeedTranColumnList = etlFeedTranColumnsDao.getQueryResultsByParent(colVb,
						approveOrPend);
				etlFeedTranColumnsDao.deletePendingRecord(colVb);
				for (EtlFeedTranColumnVb columnNodeVb : etlFeedTranColumnList) {
					columnNodeVb.setCountry(transformationVb.getCountry());
					columnNodeVb.setLeBook(transformationVb.getLeBook());
					columnNodeVb.setFeedId(transformationVb.getFeedId());
					columnNodeVb.setNodeId(transformationVb.getNodeId());
					columnNodeVb.setVerifier(verifier);
					columnNodeVb.setMaker(intCurrentUserId);
					columnNodeVb.setRecordIndicator(recordInd);
					columnNodeVb.setColumnStatus(transformationVb.getTransformationStatus());
					retVal = etlFeedTranColumnsDao.doInsertionUpl(columnNodeVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
		}
		// }
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTargetDataUpl(EtlFeedMainVb vObject, int status, int approveOrPend)
			throws RuntimeCustomException {

		if (vObject.getTabId() == 4) {
			ExceptionCode exceptionCode = null;
			// ETL Transformed Columns
			if (vObject.getEtlTransformedColumnsList() != null) {

				// delete ETL_TRANSFORMED_COLUMNS_UPL
				etlTransformedColumnsDao.deleteUplRecord(
						new EtlTransformedColumnsVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

				for (EtlTransformedColumnsVb transformedColumnsVb : vObject.getEtlTransformedColumnsList()) {
					transformedColumnsVb.setVerifier(intCurrentUserId);
					transformedColumnsVb.setMaker(intCurrentUserId);
					transformedColumnsVb.setRecordIndicator(Constants.STATUS_ZERO);
					transformedColumnsVb.setFeedColumnStatus(status);
					transformedColumnsVb.setCountry(vObject.getCountry());
					transformedColumnsVb.setLeBook(vObject.getLeBook());
					transformedColumnsVb.setFeedId(vObject.getFeedId());
					retVal = etlTransformedColumnsDao.doInsertionUpl(transformedColumnsVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			// ETL Feed Destination
			if (vObject.getEtlFeedDestinationList() != null) {

				etlFeedDestinationDao.deleteUplRecord(
						new EtlFeedDestinationVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

				for (EtlFeedDestinationVb feedDestinationVb : vObject.getEtlFeedDestinationList()) {
					feedDestinationVb.setVerifier(intCurrentUserId);
					feedDestinationVb.setMaker(intCurrentUserId);
					feedDestinationVb.setRecordIndicator(Constants.STATUS_ZERO);
					feedDestinationVb.setFeedTransformStatus(status);
					feedDestinationVb.setCountry(vObject.getCountry());
					feedDestinationVb.setLeBook(vObject.getLeBook());
					feedDestinationVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedDestinationDao.doInsertionUpl(feedDestinationVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			// ETL Target Columns
			if (vObject.getEtlTargetColumnsList() != null) {

				etlTargetColumnsDao.deleteUplRecord(
						new EtlTargetColumnsVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

				for (EtlTargetColumnsVb targetColumnsVb : vObject.getEtlTargetColumnsList()) {
					targetColumnsVb.setVerifier(intCurrentUserId);
					targetColumnsVb.setMaker(intCurrentUserId);
					targetColumnsVb.setRecordIndicator(Constants.STATUS_ZERO);
					targetColumnsVb.setFeedColumnStatus(status);
					targetColumnsVb.setCountry(vObject.getCountry());
					targetColumnsVb.setLeBook(vObject.getLeBook());
					targetColumnsVb.setFeedId(vObject.getFeedId());
					retVal = etlTargetColumnsDao.doInsertionUpl(targetColumnsVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}

			// ETL Feed Loading Mappings
			if (vObject.getEtlFeedLoadMappingList() != null) {

				etlFeedLoadMappingDao.deleteUplRecord(
						new EtlFeedLoadMappingVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

				for (EtlFeedLoadMappingVb loadMappingVb : vObject.getEtlFeedLoadMappingList()) {
					loadMappingVb.setVerifier(intCurrentUserId);
					loadMappingVb.setMaker(intCurrentUserId);
					loadMappingVb.setRecordIndicator(Constants.STATUS_ZERO);
					loadMappingVb.setFeedMappingStatus(status);
					loadMappingVb.setCountry(vObject.getCountry());
					loadMappingVb.setLeBook(vObject.getLeBook());
					loadMappingVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedLoadMappingDao.doInsertionUpl(loadMappingVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
			// Etl Feed Injection Config
			if (vObject.getEtlFeedInjectionConfigList() != null) {

				etlFeedInjectionConfigDao.deleteUplRecord(
						new EtlFeedInjectionConfigVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));
				for (EtlFeedInjectionConfigVb injectionConfigVb : vObject.getEtlFeedInjectionConfigList()) {
					injectionConfigVb.setVerifier(intCurrentUserId);
					injectionConfigVb.setMaker(intCurrentUserId);
					injectionConfigVb.setRecordIndicator(Constants.STATUS_ZERO);
					injectionConfigVb.setEtlInjectionStatus(status);
					injectionConfigVb.setCountry(vObject.getCountry());
					injectionConfigVb.setLeBook(vObject.getLeBook());
					injectionConfigVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedInjectionConfigDao.doInsertionUpl(injectionConfigVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTransformedDataUpl(EtlFeedMainVb vObject, int status, int approveOrPend)
			throws RuntimeCustomException {
		if (vObject.getTabId() == 6) {
			ExceptionCode exceptionCode = null;
			// ETL Transformed Columns

			etlFeedTransformationDao.deleteUplRecord(
					new EtlFeedTransformationVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));

			if (vObject.getEtlFeedTranList() != null) {
				for (EtlFeedTranVb transformedVb : vObject.getEtlFeedTranList()) {
					for (EtlFeedTranNodeVb transformedNodeVb : transformedVb.getTransformationNodeInputList()) {
						vObject.setNodeId(transformedNodeVb.getNodeId());
						EtlFeedTransformationVb vObj = new EtlFeedTransformationVb();
						vObj.setCountry(vObject.getCountry());
						vObj.setLeBook(vObject.getLeBook());
						vObj.setFeedId(vObject.getFeedId());
						vObj.setNodeId(transformedNodeVb.getNodeId());
						vObj.setNodeName(transformedNodeVb.getNodeName());
						vObj.setNodeDesc(transformedNodeVb.getNodeDesc());
						vObj.setTransformationId(transformedNodeVb.getTransformationId());
						vObj.setxAxis(transformedNodeVb.getxAxis());
						vObj.setyAxis(transformedNodeVb.getyAxis());
						vObj.setMinimizeFlag(transformedNodeVb.getMinimizeFlag());
						vObj.setVerifier(intCurrentUserId);
						vObj.setMaker(intCurrentUserId);
						vObj.setRecordIndicator(Constants.STATUS_ZERO);
						vObj.setTransformationStatus(status);
						vObj.setGroupId(transformedNodeVb.getGroupId());
						vObj.setJoinType(transformedNodeVb.getJoinType());
						vObj.setCustomExpression(transformedNodeVb.getCustomExpression());
						retVal = etlFeedTransformationDao.doInsertionUpl(vObj);
						if (retVal == Constants.ERRONEOUS_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
						etlFeedTransformationDao.deleteRecordByParentNode(vObject, "UPL");
						for (EtlFeedTranNodeVb parentNodeVb : transformedNodeVb.getParentTransformationList()) {
							vObj.setParentNodeId(parentNodeVb.getNodeId());
							vObj.setCountry(vObject.getCountry());
							vObj.setLeBook(vObject.getLeBook());
							vObj.setFeedId(vObject.getFeedId());
							vObj.setVerifier(intCurrentUserId);
							vObj.setMaker(intCurrentUserId);
							retVal = etlFeedTransformationDao.doInsertionParentUpl(vObj);
							if (retVal == Constants.ERRONEOUS_OPERATION) {
								exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode);
							}
						}
						etlFeedTransformationDao.deleteRecordByChildNode(vObject, "UPL");
						for (EtlFeedTranNodeVb childNodeVb : transformedNodeVb.getChildTransformationList()) {
							vObj.setChildNodeId(childNodeVb.getNodeId());
							vObj.setCountry(vObject.getCountry());
							vObj.setLeBook(vObject.getLeBook());
							vObj.setFeedId(vObject.getFeedId());
							vObj.setVerifier(intCurrentUserId);
							vObj.setMaker(intCurrentUserId);
							retVal = etlFeedTransformationDao.doInsertionChildUpl(vObj);
							if (retVal == Constants.ERRONEOUS_OPERATION) {
								exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode);
							}
						}
						EtlFeedTranColumnVb colVb = new EtlFeedTranColumnVb();
						colVb.setCountry(vObj.getCountry());
						colVb.setLeBook(vObj.getLeBook());
						colVb.setFeedId(vObj.getFeedId());
						colVb.setNodeId(vObj.getNodeId());
						etlFeedTranColumnsDao.deleteUplRecord(colVb);
						for (EtlFeedTranColumnVb columnNodeVb : transformedNodeVb.getTranColumnList()) {
							columnNodeVb.setCountry(vObj.getCountry());
							columnNodeVb.setLeBook(vObj.getLeBook());
							columnNodeVb.setFeedId(vObj.getFeedId());
							columnNodeVb.setNodeId(vObj.getNodeId());
							columnNodeVb.setVerifier(intCurrentUserId);
							columnNodeVb.setMaker(intCurrentUserId);
							columnNodeVb.setRecordIndicator(Constants.STATUS_INSERT);
							retVal = etlFeedTranColumnsDao.doInsertionUpl(columnNodeVb);
							if (retVal == Constants.ERRONEOUS_OPERATION) {
								exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode);
							}
						}
					}
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateScheduleDataApprove(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
		// if(vObject.getTabId()<5) {
		List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedScheduleConfigList != null && !etlFeedScheduleConfigList.isEmpty()) {
			EtlFeedScheduleConfigVb vObj = new EtlFeedScheduleConfigVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedScheduleConfigList() != null
						&& !vObject.getEtlFeedScheduleConfigList().isEmpty()) {
				} else {
					vObject.setEtlFeedScheduleConfigList(etlFeedScheduleConfigList);
				}
			} else {
				vObject.setEtlFeedScheduleConfigList(etlFeedScheduleConfigList);
			}
		}
		if (vObject.getEtlFeedScheduleConfigList() != null) {
			for (EtlFeedScheduleConfigVb scheduleConfigVb : vObject.getEtlFeedScheduleConfigList()) {
				scheduleConfigVb.setCountry(vObject.getCountry());
				scheduleConfigVb.setLeBook(vObject.getLeBook());
				scheduleConfigVb.setFeedId(vObject.getFeedId());
				scheduleConfigVb.setVerifier(verifier);
				scheduleConfigVb.setMaker(intCurrentUserId);
				scheduleConfigVb.setRecordIndicator(recordInd);
				scheduleConfigVb.setEtlScheduleStatus(status);
				String xml = "";
				try {
					xml = convert(scheduleConfigVb.getDependencyFeedContext(), "root"); // This method converts json
																						// object to xml string
				} catch (Exception e) {
					xml = scheduleConfigVb.getDependencyFeedContext();
				}
				String freeTextXML = xml;
				String freeTextPanelXMLData = freeTextXML.replaceAll("\n", "").replaceAll("\r", "");
				Matcher colMatcher1 = Pattern.compile("\\<feed\\>(.*?)\\<\\/feed\\>", Pattern.DOTALL)
						.matcher("<feed\\>" + freeTextPanelXMLData + "<\\/feed\\>");

				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedScheduleConfigDao.doInsertionPend(scheduleConfigVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					etlFeedScheduleConfigDao.deleteFeedDependencyPendRecord(vObject);
					while (colMatcher1.find()) {
						String colData = colMatcher1.group(1);
						String fId = CommonUtils.getValueForXmlTag(colData, "id");
						vObject.setDependencyFeedId(fId);
						etlFeedScheduleConfigDao.doInsertionFeedDependencyPend(vObject);
					}
				} else {
					retVal = etlFeedScheduleConfigDao.doInsertionUpl(scheduleConfigVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					etlFeedScheduleConfigDao.deleteFeedDependencyUplRecord(vObject);
					while (colMatcher1.find()) {
						String colData = colMatcher1.group(1);
						String fId = CommonUtils.getValueForXmlTag(colData, "id");
						vObject.setDependencyFeedId(fId);
						etlFeedScheduleConfigDao.doInsertionFeedDependencyUpl(vObject);
					}
				}
			}
		}
		EtlFeedAlertConfigVb vObj = new EtlFeedAlertConfigVb();
		vObj.setCountry(vObject.getCountry());
		vObj.setLeBook(vObject.getLeBook());
		vObj.setFeedId(vObject.getFeedId());
		List<EtlFeedAlertConfigVb> etlFeedAlertLists = etlFeedAlertconfigDao.getQueryResults(vObj, approveOrPend);
		if (etlFeedAlertLists != null && !etlFeedAlertLists.isEmpty()) {
		}
		if (recordInd == Constants.DELETE_PENDING || status == Constants.MODIFY_PENDING) {
			if (status == Constants.MODIFY_PENDING) {
				if (vObject.getEtlFeedAlertConfiglst() != null && !vObject.getEtlFeedAlertConfiglst().isEmpty()) {
				} else {
					vObject.setEtlFeedAlertConfiglst(etlFeedAlertLists);
				}
			} else {
				vObject.setEtlFeedAlertConfiglst(etlFeedAlertLists);
			}
		}
		if (vObject.getEtlFeedAlertConfiglst() != null) {
			for (EtlFeedAlertConfigVb alertConfigVb : vObject.getEtlFeedAlertConfiglst()) {
				alertConfigVb.setVerifier(verifier);
				alertConfigVb.setMaker(intCurrentUserId);
				alertConfigVb.setRecordIndicator(recordInd);
				alertConfigVb.setFeedAlertStatus(status);
				alertConfigVb.setCountry(vObject.getCountry());
				alertConfigVb.setLeBook(vObject.getLeBook());
				alertConfigVb.setFeedId(vObject.getFeedId());

				if (recordInd == Constants.DELETE_PENDING) {
					retVal = etlFeedAlertconfigDao.doInsertionPend(alertConfigVb);
				} else {
					retVal = etlFeedAlertconfigDao.doInsertionUpl(alertConfigVb);
				}

				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

			}
		}
//		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateScheduleDataPend(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
//		if (vObject.getTabId() < 5 || vObject.getTabId() == 5) {
		List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (etlFeedScheduleConfigList != null && !etlFeedScheduleConfigList.isEmpty()) {
			if (vObject.getEtlFeedScheduleConfigList() == null) {
				vObject.setEtlFeedScheduleConfigList(etlFeedScheduleConfigList);
			}
			EtlFeedScheduleConfigVb vObj = new EtlFeedScheduleConfigVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlFeedScheduleConfigDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedScheduleConfigList(etlFeedScheduleConfigList);
		}
		if (vObject.getEtlFeedScheduleConfigList() != null) {
			for (EtlFeedScheduleConfigVb scheduleConfigVb : vObject.getEtlFeedScheduleConfigList()) {
				scheduleConfigVb.setCountry(vObject.getCountry());
				scheduleConfigVb.setLeBook(vObject.getLeBook());
				scheduleConfigVb.setFeedId(vObject.getFeedId());
				scheduleConfigVb.setVerifier(verifier);
				scheduleConfigVb.setMaker(intCurrentUserId);
				scheduleConfigVb.setRecordIndicator(recordInd);
				scheduleConfigVb.setEtlScheduleStatus(status);
				retVal = etlFeedScheduleConfigDao.doInsertionUpl(scheduleConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				String xml = "";
				try {
					xml = convert(scheduleConfigVb.getDependencyFeedContext(), "root"); // This method converts json
																						// object to xml string
				} catch (Exception e) {
					xml = scheduleConfigVb.getDependencyFeedContext();
				}
				String freeTextXML = xml;
				String freeTextPanelXMLData = freeTextXML.replaceAll("\n", "").replaceAll("\r", "");
				Matcher colMatcher1 = Pattern.compile("\\<feed\\>(.*?)\\<\\/feed\\>", Pattern.DOTALL)
						.matcher("<feed\\>" + freeTextPanelXMLData + "<\\/feed\\>");
				while (colMatcher1.find()) {
					String colData = colMatcher1.group(1);
					String fId = CommonUtils.getValueForXmlTag(colData, "id");
					vObject.setDependencyFeedId(fId);
					etlFeedScheduleConfigDao.doInsertionFeedDependencyUpl(vObject);
				}
			}
		}
		EtlFeedAlertConfigVb vObj = new EtlFeedAlertConfigVb();
		vObj.setCountry(vObject.getCountry());
		vObj.setLeBook(vObject.getLeBook());
		vObj.setFeedId(vObject.getFeedId());
		List<EtlFeedAlertConfigVb> etlFeedAlertLists = etlFeedAlertconfigDao.getQueryResults(vObj, approveOrPend);
		if (etlFeedAlertLists != null && !etlFeedAlertLists.isEmpty()) {
			if (vObject.getEtlFeedAlertConfiglst() == null) {
				vObject.setEtlFeedAlertConfiglst(etlFeedAlertLists);
			}
			etlFeedAlertconfigDao.deletePendingRecordAll(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlFeedAlertConfiglst(etlFeedAlertLists);
		}
		if (vObject.getEtlFeedAlertConfiglst() != null) {
			for (EtlFeedAlertConfigVb alertConfigVb : vObject.getEtlFeedAlertConfiglst()) {
				alertConfigVb.setVerifier(verifier);
				alertConfigVb.setMaker(intCurrentUserId);
				alertConfigVb.setRecordIndicator(recordInd);
				alertConfigVb.setFeedAlertStatus(status);
				alertConfigVb.setCountry(vObject.getCountry());
				alertConfigVb.setLeBook(vObject.getLeBook());
				alertConfigVb.setFeedId(vObject.getFeedId());
				retVal = etlFeedAlertconfigDao.doInsertionUpl(alertConfigVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

			}
		}
		// }
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateScheduleDataUpl(EtlFeedMainVb vObject, int status, int approveOrPend)
			throws RuntimeCustomException {
		if (vObject.getTabId() == 5) {
			ExceptionCode exceptionCode = null;
			etlFeedScheduleConfigDao.deleteUplRecord(
					new EtlFeedScheduleConfigVb(vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()));
			if (vObject.getEtlFeedScheduleConfigList() != null) {
				for (EtlFeedScheduleConfigVb scheduleConfigVb : vObject.getEtlFeedScheduleConfigList()) {
					scheduleConfigVb.setCountry(vObject.getCountry());
					scheduleConfigVb.setLeBook(vObject.getLeBook());
					scheduleConfigVb.setFeedId(vObject.getFeedId());
					scheduleConfigVb.setVerifier(intCurrentUserId);
					scheduleConfigVb.setMaker(intCurrentUserId);
					scheduleConfigVb.setRecordIndicator(Constants.STATUS_ZERO);
					scheduleConfigVb.setEtlScheduleStatus(status);
					scheduleConfigVb.setCountry(vObject.getCountry());
					scheduleConfigVb.setLeBook(vObject.getLeBook());
					scheduleConfigVb.setFeedId(vObject.getFeedId());
					String scheduleCon = scheduleConfigVb.getSchedulePropContext();
					
					retVal = etlFeedScheduleConfigDao.doInsertionUpl(scheduleConfigVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					String xml = "";
					try {
						xml = convert(scheduleConfigVb.getDependencyFeedContext(), "root"); // This method converts json
																							// object to xml string
					} catch (Exception e) {
						xml = scheduleConfigVb.getDependencyFeedContext();
					}
					String freeTextXML = xml;
					String freeTextPanelXMLData = freeTextXML.replaceAll("\n", "").replaceAll("\r", "");
					Matcher colMatcher1 = Pattern.compile("\\<feed\\>(.*?)\\<\\/feed\\>", Pattern.DOTALL)
							.matcher("<feed\\>" + freeTextPanelXMLData + "<\\/feed\\>");
					while (colMatcher1.find()) {
						String colData = colMatcher1.group(1);
						String fId = CommonUtils.getValueForXmlTag(colData, "id");
						vObject.setDependencyFeedId(fId);
						etlFeedScheduleConfigDao.doInsertionFeedDependencyUpl(vObject);
					}
				}
			}
			etlFeedAlertconfigDao.deleteUplRecordByParent(vObject);
			if (vObject.getEtlFeedAlertConfiglst() != null) {
				for (EtlFeedAlertConfigVb alertConfigVb : vObject.getEtlFeedAlertConfiglst()) {
					alertConfigVb.setVerifier(intCurrentUserId);
					alertConfigVb.setMaker(intCurrentUserId);
					alertConfigVb.setRecordIndicator(Constants.STATUS_ZERO);
					alertConfigVb.setFeedAlertStatus(status);
					alertConfigVb.setCountry(vObject.getCountry());
					alertConfigVb.setLeBook(vObject.getLeBook());
					alertConfigVb.setFeedId(vObject.getFeedId());
					retVal = etlFeedAlertconfigDao.doInsertionUpl(alertConfigVb);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}

				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doUpdateApprRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		try {
			return doUpdateApprRecordForNonTrans(vObject);
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doUpdateRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		try {
			return doUpdateRecordForNonTrans(vObject);
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

	protected ExceptionCode updateExtractionDataPend(EtlFeedMainVb vObject, int status, int approveOrPend,
			Boolean verReqd, int recordInd) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		long verifier = 0;
		if (!verReqd) {
			verifier = intCurrentUserId;
		}
//		if (vObject.getTabId() < 3 || vObject.getTabId() == 5) {
		List<EtlExtractionSummaryFieldsVb> extractionSummaryFieldsList = etlExtractionSummaryFieldsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		if (extractionSummaryFieldsList != null && !extractionSummaryFieldsList.isEmpty()) {
			if (vObject.getEtlExtractionSummaryFieldsList() == null) {
				vObject.setEtlExtractionSummaryFieldsList(extractionSummaryFieldsList);
			}
			EtlExtractionSummaryFieldsVb vObj = new EtlExtractionSummaryFieldsVb();
			vObj.setCountry(vObject.getCountry());
			vObj.setLeBook(vObject.getLeBook());
			vObj.setFeedId(vObject.getFeedId());
			etlExtractionSummaryFieldsDao.deletePendingRecord(vObj);
		}
		if (recordInd == Constants.DELETE_PENDING) {
			vObject.setEtlExtractionSummaryFieldsList(extractionSummaryFieldsList);
		}
		if (vObject.getEtlExtractionSummaryFieldsList() != null) {
			for (EtlExtractionSummaryFieldsVb summaryFieldsVb : vObject.getEtlExtractionSummaryFieldsList()) {
				summaryFieldsVb.setVerifier(verifier);
				summaryFieldsVb.setMaker(intCurrentUserId);
				summaryFieldsVb.setRecordIndicator(recordInd);
				summaryFieldsVb.setEtlFieldsStatus(status);
				summaryFieldsVb.setCountry(vObject.getCountry());
				summaryFieldsVb.setLeBook(vObject.getLeBook());
				summaryFieldsVb.setFeedId(vObject.getFeedId());
				retVal = etlExtractionSummaryFieldsDao.doInsertionUpl(summaryFieldsVb);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		// }
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected ExceptionCode updateTransformationDataPend(EtlFeedMainVb vObject, int status, int recordIndicator,
			int approveOrPend) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		// ETL Feed Transformation
		List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
				.getQueryResultsByParent(vObject, approveOrPend);
		for (EtlFeedTransformationVb transformationVb : etlFeedTransformationList) {
			transformationVb.setVerifier(0);
			transformationVb.setRecordIndicator(recordIndicator);
			transformationVb.setTransformationStatus(status);
			retVal = etlFeedTransformationDao.doUpdatePend(transformationVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		// ETL Feed Transformation Columns
		List<EtlTransformedColumnsVb> etlTransformedColumnsList = etlTransformedColumnsDao
				.getQueryResultsByParent(vObject, approveOrPend);
		for (EtlTransformedColumnsVb transformedColumnsVb : etlTransformedColumnsList) {
			transformedColumnsVb.setVerifier(0);
			transformedColumnsVb.setRecordIndicator(recordIndicator);
			transformedColumnsVb.setFeedColumnStatus(status);
			retVal = etlTransformedColumnsDao.doUpdatePend(transformedColumnsVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	public List<ETLFeedTablesVb> selectConnectorQuery(String connectorId, String excludeTableListStr) {
		List<ETLFeedTablesVb> result = new ArrayList<ETLFeedTablesVb>();
		String query = "SELECT QUERY_ID from ETL_MQ_TABLE WHERE QUERY_ID NOT IN "+excludeTableListStr+" and CONNECTOR_ID= ?";
		Object [] args = {connectorId};
		try {
			ResultSetExtractor<List<ETLFeedTablesVb>> rse = new ResultSetExtractor<List<ETLFeedTablesVb>>() {
				public List<ETLFeedTablesVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
					int ctr = 0;
					while (rs.next()) {
						ETLFeedTablesVb vobj = new ETLFeedTablesVb();
						vobj.setSourceConnectorId(connectorId);
						vobj.setTableId(ctr);
						vobj.setTableName(rs.getString("QUERY_ID"));
						vobj.setTableAliasName(rs.getString("QUERY_ID"));
						vobj.setTableSortOrder(ctr);
						vobj.setTableType("M");
						vobj.setQueryId(rs.getString("QUERY_ID"));
						vobj.setPartitionRequiredFlag("Y");
						vobj.setCustomPartitionColumnFlag("N");
						vobj.setPartitionColumnName("");
						vobj.setBaseTableFlag("N");
						result.add(vobj);
						ctr++;
					}
					return result;
				}
			};
			getJdbcTemplate().query(query,args, rse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public List<String> selectConnectorQueryList(String connectorId, String excludeTableListStr) {
		List<String> strconTablList = new ArrayList<String>();
		String query = "";
		Object[] args = { connectorId };
		if (ValidationUtil.isValid(excludeTableListStr)) {
			query = "SELECT QUERY_ID from ETL_MQ_TABLE WHERE QUERY_ID NOT IN " + excludeTableListStr
					+ " and CONNECTOR_ID= ? ";
		} else {
			query = "SELECT QUERY_ID from ETL_MQ_TABLE WHERE CONNECTOR_ID= ? ";

		}
		try {
			ResultSetExtractor<List<String>> rse = new ResultSetExtractor<List<String>>() {
				public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
					int ctr = 0;
					while (rs.next()) {
						strconTablList.add(rs.getString("QUERY_ID"));
						ctr++;
					}
					return strconTablList;
				}
			};
			getJdbcTemplate().query(query, args, rse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return strconTablList;
	}

	public List<ETLFeedTablesVb> selectConnectorFileTables(String connectorId, String excludeTableListStr) {
		List<ETLFeedTablesVb> result = new ArrayList<ETLFeedTablesVb>();
		String query = "";
		if (ValidationUtil.isValid(excludeTableListStr)) {
			query = "SELECT CONNECTOR_ID, VIEW_NAME FROM ETL_FILE_TABLE WHERE VIEW_NAME NOT IN " + excludeTableListStr
					+ " and UPPER(CONNECTOR_ID) = UPPER(?) ORDER BY VIEW_NAME";
		} else {
			query = "SELECT CONNECTOR_ID, VIEW_NAME FROM ETL_FILE_TABLE WHERE UPPER(CONNECTOR_ID) = UPPER(?) ORDER BY VIEW_NAME";
		}
		Object[] args = { connectorId };
		try {
			ResultSetExtractor<List<ETLFeedTablesVb>> rse = new ResultSetExtractor<List<ETLFeedTablesVb>>() {
				public List<ETLFeedTablesVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
					int ctr = 0;
					while (rs.next()) {
						ETLFeedTablesVb vobj = new ETLFeedTablesVb();
						vobj.setSourceConnectorId(connectorId);
						vobj.setTableId(ctr);
						vobj.setTableName(rs.getString("VIEW_NAME"));
						vobj.setTableAliasName(rs.getString("VIEW_NAME"));
						vobj.setTableSortOrder(ctr);
						vobj.setTableType("F");
						vobj.setQueryId("TEST");
						vobj.setPartitionRequiredFlag("Y");
						vobj.setCustomPartitionColumnFlag("N");
						vobj.setPartitionColumnName("");
						vobj.setBaseTableFlag("N");
						result.add(vobj);
						ctr++;
					}
					return result;
				}
			};
			getJdbcTemplate().query(query, args, rse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public List<String> selectConnectorFileTablesList(String connectorId, String excludeTableListStr) {
		List<String> result = new ArrayList<String>();
		String query = "";
		Object[] args = { connectorId };
		if (ValidationUtil.isValid(excludeTableListStr)) {
			query = "SELECT CONNECTOR_ID, VIEW_NAME FROM ETL_FILE_TABLE WHERE VIEW_NAME NOT IN " + excludeTableListStr
					+ " and UPPER(CONNECTOR_ID) = UPPER(?) ORDER BY VIEW_NAME";
		} else {
			query = "SELECT CONNECTOR_ID, VIEW_NAME FROM ETL_FILE_TABLE WHERE UPPER(CONNECTOR_ID) = UPPER(?) ORDER BY VIEW_NAME";
		}
		try {
			ResultSetExtractor<List<String>> rse = new ResultSetExtractor<List<String>>() {
				public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
					int ctr = 0;
					while (rs.next()) {
						result.add(rs.getString("VIEW_NAME"));
						ctr++;
					}
					return result;
				}
			};
			getJdbcTemplate().query(query, args, rse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public List<ETLFeedColumnsVb> selectConnectorQueryColumns(String connectorId, String queryId) {
		List<ETLFeedColumnsVb> arrayListCol = new ArrayList<ETLFeedColumnsVb>();
		String query = "SELECT CONNECTOR_ID, QUERY_ID, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE, "
				+ "COLUMN_LENGTH, DATE_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG"
				+ " from ETL_MQ_COLUMNS WHERE CONNECTOR_ID= ? and QUERY_ID= ?";
		Object[] args = { connectorId, queryId };
		try {
			ResultSetExtractor<List<ETLFeedColumnsVb>> rse = new ResultSetExtractor<List<ETLFeedColumnsVb>>() {
				public List<ETLFeedColumnsVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						ETLFeedColumnsVb vobj = new ETLFeedColumnsVb();
						vobj.setSourceConnectorId(connectorId);
						vobj.setTableId(queryId);
						vobj.setColumnId(rs.getString("COLUMN_ID"));
						vobj.setColumnName(rs.getString("COLUMN_NAME"));
						vobj.setColumnAliasName(rs.getString("COLUMN_NAME"));
						vobj.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
						vobj.setColumnDataType(rs.getString("COLUMN_DATATYPE"));
						vobj.setColumnLength(rs.getString("COLUMN_LENGTH"));
						vobj.setDateFormat(rs.getString("DATE_FORMAT"));
						vobj.setColExperssionType("DBCOL");
						vobj.setExperssionText("");
						vobj.setPrimaryKeyFlag(rs.getString("PRIMARY_KEY_FLAG"));
						vobj.setPartitionColumnFlag(rs.getString("PARTITION_COLUMN_FLAG"));
						vobj.setFilterContext("");
						arrayListCol.add(vobj);
					}
					return arrayListCol;
				}
			};
			getJdbcTemplate().query(query, args, rse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return arrayListCol;
	}

	public List<ETLFeedColumnsVb> selectConnectorFileColumns(String connectorId, String tableName) {
		List<ETLFeedColumnsVb> arrayListCol = new ArrayList<ETLFeedColumnsVb>();
		String query = "SELECT CONNECTOR_ID, VIEW_NAME, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE, "
				+ "COLUMN_LENGTH, DATE_FORMAT,  PRIMARY_KEY_FLAG,PARTITION_COLUMN_FLAG "
				+ " from ETL_FILE_COLUMNS WHERE CONNECTOR_ID= ? and VIEW_NAME= ?";
		Object[] args = { connectorId, tableName };
		try {
			ResultSetExtractor<List<ETLFeedColumnsVb>> rse = new ResultSetExtractor<List<ETLFeedColumnsVb>>() {
				public List<ETLFeedColumnsVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						ETLFeedColumnsVb vobj = new ETLFeedColumnsVb();
						vobj.setSourceConnectorId(connectorId);
						vobj.setTableId(tableName);
						vobj.setColumnId(rs.getString("COLUMN_ID"));
						vobj.setColumnName(rs.getString("COLUMN_NAME"));
						vobj.setColumnAliasName(rs.getString("COLUMN_NAME"));
						vobj.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
						vobj.setColumnDataType(rs.getString("COLUMN_DATATYPE"));
						vobj.setColumnLength(rs.getString("COLUMN_LENGTH"));
						vobj.setDateFormat(rs.getString("DATE_FORMAT"));
						vobj.setPrimaryKeyFlag(rs.getString("PRIMARY_KEY_FLAG"));
						vobj.setPartitionColumnFlag(rs.getString("PARTITION_COLUMN_FLAG"));
						vobj.setColExperssionType("DBCOL");
						arrayListCol.add(vobj);
					}
					return arrayListCol;
				}
			};
			getJdbcTemplate().query(query, args, rse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return arrayListCol;
	}

	public Map<String, String> getDataTypeMap(int atValue, String databaseFilter) {
		String sql = "select * from ALPHA_SUB_TAB t where ALPHA_TAB= ? AND INTERNAL_STATUS= ?";
		Object [] args= {atValue,databaseFilter};
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					returnMap.put(rs.getString("ALPHA_SUB_TAB"), rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				}
				return returnMap;
			}
		};
		return (Map<String, String>) getJdbcTemplate().query(sql,args, rseObj);
	}

	/*public Map<String, String> getSchedulerScript() {
		String sql = "select SCRIPT_TYPE, SCRIPT_ID, SCRIPT_DESCRIPTION from ETL_CONNECTOR_SCRIPTS where SCRIPT_STATUS =0";
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					String key = rs.getString("SCRIPT_TYPE");
					String value = rs.getString("SCRIPT_ID") + "!@#" + rs.getString("SCRIPT_DESCRIPTION");
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
	}*/
	
	public Map<String, List<AlphaSubTabVb>> getSchedulerScript() {
		String sql = "select SCRIPT_TYPE, SCRIPT_ID, SCRIPT_DESCRIPTION from ETL_CONNECTOR_SCRIPTS where SCRIPT_STATUS =0";
		ResultSetExtractor<Map<String, List<AlphaSubTabVb>>> rseObj = new ResultSetExtractor<Map<String, List<AlphaSubTabVb>>>() {
			Map<String, List<AlphaSubTabVb>> returnMap = new HashMap<String, List<AlphaSubTabVb>>();
			@Override
			public Map<String, List<AlphaSubTabVb>> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					String key = rs.getString("SCRIPT_TYPE");
					AlphaSubTabVb alphaSubTabVb = new AlphaSubTabVb();
					alphaSubTabVb.setAlphaSubTab(rs.getString("SCRIPT_ID"));
					alphaSubTabVb.setDescription(rs.getString("SCRIPT_DESCRIPTION"));
					if (returnMap.containsKey(key)) {
						List<AlphaSubTabVb> list = returnMap.get(key);
						list.add(alphaSubTabVb);
						returnMap.put(key, list);
					} else {
						returnMap.put(key, new ArrayList<AlphaSubTabVb>(Arrays.asList(alphaSubTabVb)));
					}
				}
				return returnMap;
			}
		};
		return getJdbcTemplate().query(sql, rseObj);
	}

	/*public Map<String, String> geTransCategory() {
		String sql = " select T1.VRD_OBJECT_ID, T1.VRD_OBJECT_NAME, T1.VRD_OBJECT_CATEGORY,T2.OBJ_TAG_ID,T2.OBJ_TAG_DESC "
				+ ", " + getDbFunction(Constants.TO_CHAR, "T2.HTML_TAG_PROPERTY") + " HTML_TAG_PROPERTY "
				+ " FROM ETL_Transformation_Main T1 JOIN ETL_Transformation_Properties T2 ON "
				+ "T1.VRD_OBJECT_ID= T2.VRD_OBJECT_ID ";

		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					String key = rs.getString("VRD_OBJECT_ID");
					String value = rs.getString("VRD_OBJECT_NAME") + "@-@" + rs.getString("OBJ_TAG_ID") + "@-@"
							+ rs.getString("OBJ_TAG_DESC") + "@-@" + rs.getString("HTML_TAG_PROPERTY");
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
	}*/
	
	/*
	 * Note: Arg1: dbType relates to the VRD_OBJECT_ID of VRD_RS_OBJECTS Table Arg2:
	 * extractionQueryType is the indegator of which query needs to be fetched
	 * (Table List Query or Table Exclude Query or Column List Query)
	 */
	public String getQueriesForDatasource(String dbType, String extractionQueryType) {
		try {
			String sql = "select T2.VRD_OBJECT_ID, T2.OBJ_TAG_ID, T2.HTML_TAG_PROPERTY from VRD_RS_OBJECTS t1, VRD_OBJECT_PROPERTIES t2 "
					+ " where T1.VRD_OBJECT_ID = T2.VRD_OBJECT_ID  AND T1.VRD_OBJECT_CATEGORY = 'SCHEMA_SQL' "
					+ " AND T1.VRD_OBJECT_ID = ? AND T2.OBJ_TAG_ID = ?";
			Object[] args = { dbType, extractionQueryType };
			return getJdbcTemplate().query(sql, new ResultSetExtractor<String>() {

				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					if (rs.next()) {
						return rs.getString("HTML_TAG_PROPERTY");
					} else {
						return null;
					}
				}
			}, args);

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public DSConnectorVb getSpecificConnectorDetails(DSConnectorVb dsConnectorVb) throws DataAccessException {
		String sql = " SELECT CONNECTOR_ID, CONNECTOR_SCRIPTS VARIABLE_SCRIPT, CONNECTION_NAME DESCRIPTION, CASE CONNECTOR_TYPE "
				+ " WHEN 'S' THEN 'S' ELSE CONNECTOR_TYPE  END TYPE, CONNECTION_STAUTS STATUS, MAKER, VERIFIER, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED,  " + getDbFunction(Constants.DATEFUNC, null) + "(DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION FROM ETL_CONNECTOR WHERE UPPER(CONNECTOR_ID) = UPPER(?) ";
		Object [] args= {dsConnectorVb.getMacroVar()};

		ResultSetExtractor<DSConnectorVb> rse = new ResultSetExtractor<DSConnectorVb>() {
			@Override
			public DSConnectorVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				DSConnectorVb dsConnectorVb = null;
				if (rs.next()) {
					dsConnectorVb = new DSConnectorVb();
					dsConnectorVb.setMacroVar(rs.getString("CONNECTOR_ID"));
					dsConnectorVb.setMacroVarScript(rs.getString("VARIABLE_SCRIPT"));
					dsConnectorVb.setScriptType(rs.getString("TYPE"));
					dsConnectorVb.setDescription(rs.getString("DESCRIPTION"));
					dsConnectorVb.setInternalStatus(rs.getInt("STATUS"));
					dsConnectorVb.setMaker(rs.getLong("MAKER"));
					dsConnectorVb.setVerifier(rs.getLong("VERIFIER"));
					dsConnectorVb.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
					dsConnectorVb.setDateCreation(rs.getString("DATE_CREATION"));
					if (ValidationUtil.isValid(rs.getString("VARIABLE_SCRIPT"))
							&& "FILE".equalsIgnoreCase(rs.getString("TYPE"))) {
						String fileName = CommonUtils.getValue(rs.getString("VARIABLE_SCRIPT"), "NAME") + "."
								+ CommonUtils.getValue(rs.getString("VARIABLE_SCRIPT"), "EXTENSION");
						dsConnectorVb.setFileName(fileName);
						dsConnectorVb.setExtension(CommonUtils.getValue(rs.getString("VARIABLE_SCRIPT"), "EXTENSION"));
						String delimiter = CommonUtils.getValue(rs.getString("VARIABLE_SCRIPT"), "DELIMITER");
						dsConnectorVb.setDelimiter(delimiter);
						dsConnectorVb
								.setDateLastModified(CommonUtils.getValue(rs.getString("VARIABLE_SCRIPT"), "DATE"));
					}
				}
				return dsConnectorVb;
			}
		};
		return getJdbcTemplate().query(sql,args, rse);
	}

	@SuppressWarnings("deprecation")
	public List<ConnectorFileUploadMapperVb> getConnectorFileUploadMapperDetails(DSConnectorVb vObject) {
		String sql = "SELECT CONNECTOR_ID, FILE_TABLE_NAME, SELF_BI_MAPPING_TABLE_NAME FROM ETL_FILE_UPLOAD_AREA WHERE UPPER(CONNECTOR_ID) = UPPER(?) "
				+ "ORDER BY SELF_BI_MAPPING_TABLE_NAME";
		Object[] args = { vObject.getMacroVar() };
		return getJdbcTemplate().query(sql, args, new RowMapper<ConnectorFileUploadMapperVb>() {
			@Override
			public ConnectorFileUploadMapperVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				ConnectorFileUploadMapperVb connectorFileUploadMapperVb = new ConnectorFileUploadMapperVb();
				connectorFileUploadMapperVb.setConnectorId(rs.getString("CONNECTOR_ID"));
				connectorFileUploadMapperVb.setFileTableName(rs.getString("FILE_TABLE_NAME"));
				connectorFileUploadMapperVb.setSelfBiMappingTableName(rs.getString("SELF_BI_MAPPING_TABLE_NAME"));
				return connectorFileUploadMapperVb;
			}
		});
	}

	public ArrayList getData(String country, String leBook, String feedId, Integer joinSyntaxType, int approveOrPend) {
		String tblAppendStr = " ";
		if (approveOrPend == 9999) {
			tblAppendStr = "_UPL ";
		} else if (approveOrPend == 1) {
			tblAppendStr = "_PEND ";
		}

		ArrayList returnArrayList = new ArrayList(3);
		final HashMap<String, List> linkedTblsForIndividualTblHM = new HashMap<String, List>();
		final HashMap<String, List<String>> relationHM = new HashMap<String, List<String>>();
		final HashMap<String, ETLFeedTablesVb> tableDetailsHM = new HashMap<String, ETLFeedTablesVb>();
		String sql = "SELECT FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE, RELATION_CONTEXT,  FILTER_CONTEXT FROM ETL_FEED_RELATION"
				+ tblAppendStr + "WHERE COUNTRY = ? and LE_BOOK = ? and FEED_ID = ? ORDER BY FROM_TABLE_ID, TO_TABLE_ID";
	
		try {
			Object [] args= {country,leBook,feedId};
			getJdbcTemplate().query(sql, new RowCallbackHandler() {
				final String hyphen = "-";
				@Override
				public void processRow(ResultSet rs) throws SQLException, DataAccessException {
					String fromId = rs.getString("FROM_TABLE_ID");
					String toId = rs.getString("TO_TABLE_ID");
					if (linkedTblsForIndividualTblHM.get(fromId) == null) {
						linkedTblsForIndividualTblHM.put(fromId, new ArrayList<String>(Arrays.asList(toId)));
					} else {
						List linkedTblIdList = linkedTblsForIndividualTblHM.get(fromId);
						linkedTblIdList.add(toId);
						linkedTblsForIndividualTblHM.put(fromId, linkedTblIdList);
					}

					if (linkedTblsForIndividualTblHM.get(toId) == null) {
						linkedTblsForIndividualTblHM.put(toId, new ArrayList<String>(Arrays.asList(fromId)));
					} else {
						List linkedTblIdList = linkedTblsForIndividualTblHM.get(toId);
						linkedTblIdList.add(fromId);
						linkedTblsForIndividualTblHM.put(toId, linkedTblIdList);
					}

					String key = fromId + hyphen + toId;
					String relationType = rs.getString("JOIN_TYPE");
					String relation = "";
					if (ValidationUtil.isValid(rs.getString("RELATION_CONTEXT"))) {
						if (!"4".equalsIgnoreCase(relationType)) {
							relation = CommonUtils.getValueForXmlTag(rs.getString("RELATION_CONTEXT"), "customjoin");
							if (!ValidationUtil.isValid(relation)) {
								if (joinSyntaxType == 1)
									relation = CommonUtils.getValueForXmlTag(rs.getString("RELATION_CONTEXT"),
											"ansii_joinstring");
								else
									relation = CommonUtils.getValueForXmlTag(rs.getString("RELATION_CONTEXT"),
											"std_joinstring");
							}
						}
					}
					if (ValidationUtil.isValid(rs.getString("FILTER_CONTEXT"))) {
						relation = ValidationUtil.isValid(relation)
								? (relation + " AND " + rs.getString("FILTER_CONTEXT"))
								: rs.getString("FILTER_CONTEXT");
					}

					relationHM.put(key, Arrays.asList(relationType, relation));
				}

			}, args);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		sql = "SELECT VCT.COUNTRY, VCT.LE_BOOK, VCT.FEED_ID, VCT.TABLE_ID, VCT.SOURCE_CONNECTOR_ID, "
				+ " VCT.TABLE_NAME, VCT.TABLE_ALIAS_NAME, "
				+ " VCT.TABLE_SORT_ORDER, VCT.TABLE_TYPE_AT, VCT.TABLE_TYPE, VCT.QUERY_ID, "
				+ " VCT.CUSTOM_PARTITION_COLUMN_FLAG, VCT.PARTITION_COLUMN_NAME, VCT.BASE_TABLE_FLAG, "
				+ " VCC.COUNTRY, VCC.LE_BOOK,VCC.FEED_ID,  VCC.TABLE_ID, VCC.SOURCE_CONNECTOR_ID, "
				+ " VCC.COLUMN_ID, VCC.COLUMN_NAME, VCC.COLUMN_ALIAS_NAME, VCC.COLUMN_SORT_ORDER, VCC.COLUMN_DATATYPE, "
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2104 AND ALPHA_SUB_TAB = VCC.COLUMN_DATATYPE) COLUMN_DATATYPE_DESC,"
				+ " VCC.COLUMN_LENGTH, VCC.DATE_FORMAT, (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 1103 AND NUM_SUB_TAB = VCC.DATE_FORMAT) FORMAT_TYPE_DESC, "
				+ " VCC.COL_EXPERSSION_TYPE, (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2003 AND "
				+ " ALPHA_SUB_TAB = VCC.COL_EXPERSSION_TYPE) COL_EXPERSSION_TYPE_DESC, EXPERSSION_TEXT,"
				+ " VCC.PRIMARY_KEY_FLAG, VCC.PARTITION_COLUMN_FLAG,  FILTER_CONTEXT  FROM ETL_FEED_MAIN" + tblAppendStr
				+ " VC, ETL_FEED_TABLES" + tblAppendStr + " VCT, ETL_FEED_COLUMNS" + tblAppendStr
				+ " VCC  WHERE VC.FEED_ID = VCT.FEED_ID  AND VCT.FEED_ID = VCC.FEED_ID "
				+ " AND VCT.TABLE_ID = VCC.TABLE_ID  AND VCT.COUNTRY = ?  AND VCT.LE_BOOK = ?  AND VCT.FEED_ID = ? "
				+ " ORDER BY VCT.COUNTRY, VCT.LE_BOOK, VCT.FEED_ID, VCT.TABLE_ID, VCC.COLUMN_ID";
		try {
			Object [] args= {country,leBook,feedId};
			getJdbcTemplate().query(sql, new ResultSetExtractor<Object>() {
				List<ETLFeedColumnsVb> colVbList = new ArrayList<ETLFeedColumnsVb>();
				String chkTableId = null;
				ETLFeedTablesVb tableDetailsVb = new ETLFeedTablesVb();

				@Override
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						if (chkTableId == null) {
							chkTableId = rs.getString("TABLE_ID");
							tableDetailsVb = new ETLFeedTablesVb();
							tableDetailsVb.setCountry(rs.getString("COUNTRY"));
							tableDetailsVb.setLeBook(rs.getString("LE_BOOK"));
							tableDetailsVb.setFeedId(rs.getString("FEED_ID"));
							tableDetailsVb.setTableId(rs.getInt("TABLE_ID"));
							tableDetailsVb.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
							tableDetailsVb.setTableName(rs.getString("TABLE_NAME"));
							tableDetailsVb.setTableAliasName(rs.getString("TABLE_ALIAS_NAME"));
							tableDetailsVb.setTableSortOrder(rs.getInt("TABLE_SORT_ORDER"));
							tableDetailsVb.setTableTypeAt(rs.getInt("TABLE_TYPE_AT"));
							tableDetailsVb.setTableType(rs.getString("TABLE_TYPE"));
							tableDetailsVb.setQueryId(rs.getString("QUERY_ID"));
							tableDetailsVb.setCustomPartitionColumnFlag(rs.getString("CUSTOM_PARTITION_COLUMN_FLAG"));
							tableDetailsVb.setPartitionColumnName(rs.getString("PARTITION_COLUMN_NAME"));
							tableDetailsVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
						}

						if (!chkTableId.equalsIgnoreCase(rs.getString("TABLE_ID"))) {
							tableDetailsVb.setChildren(colVbList);
							tableDetailsHM.put(String.valueOf(tableDetailsVb.getTableId()), tableDetailsVb);
							colVbList = new ArrayList<ETLFeedColumnsVb>();
							chkTableId = rs.getString("TABLE_ID");
							tableDetailsVb = new ETLFeedTablesVb();
							tableDetailsVb.setCountry(rs.getString("COUNTRY"));
							tableDetailsVb.setLeBook(rs.getString("LE_BOOK"));
							tableDetailsVb.setFeedId(rs.getString("FEED_ID"));
							tableDetailsVb.setTableId(rs.getInt("TABLE_ID"));
							tableDetailsVb.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
							tableDetailsVb.setTableName(rs.getString("TABLE_NAME"));
							tableDetailsVb.setTableAliasName(rs.getString("TABLE_ALIAS_NAME"));
							tableDetailsVb.setTableSortOrder(rs.getInt("TABLE_SORT_ORDER"));
							tableDetailsVb.setTableTypeAt(rs.getInt("TABLE_TYPE_AT"));
							tableDetailsVb.setTableType(rs.getString("TABLE_TYPE"));
							tableDetailsVb.setQueryId(rs.getString("QUERY_ID"));
							tableDetailsVb.setCustomPartitionColumnFlag(rs.getString("CUSTOM_PARTITION_COLUMN_FLAG"));
							tableDetailsVb.setPartitionColumnName(rs.getString("PARTITION_COLUMN_NAME"));
							tableDetailsVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
						}

						ETLFeedColumnsVb columnDetailsVb = new ETLFeedColumnsVb();
						columnDetailsVb.setCountry(rs.getString("COUNTRY"));
						columnDetailsVb.setLeBook(rs.getString("LE_BOOK"));
						columnDetailsVb.setFeedId(rs.getString("FEED_ID"));
						columnDetailsVb.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
						columnDetailsVb.setTableId(rs.getString("TABLE_ID"));
						columnDetailsVb.setColumnId(rs.getString("COLUMN_ID"));
						columnDetailsVb.setColumnName(rs.getString("COLUMN_NAME"));
						columnDetailsVb.setColumnAliasName(rs.getString("COLUMN_ALIAS_NAME"));
						columnDetailsVb.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
						columnDetailsVb.setColumnDataType(rs.getString("COLUMN_DATATYPE"));
						columnDetailsVb.setExperssionText(rs.getString("EXPERSSION_TEXT"));
						columnDetailsVb.setColExperssionType(rs.getString("COL_EXPERSSION_TYPE"));
						columnDetailsVb.setColumnLength(rs.getString("COLUMN_LENGTH"));
						columnDetailsVb.setDateFormat(rs.getString("FORMAT_TYPE_DESC"));
						columnDetailsVb.setPrimaryKeyFlag(rs.getString("PRIMARY_KEY_FLAG"));
						columnDetailsVb.setPartitionColumnFlag(rs.getString("PARTITION_COLUMN_FLAG"));
						columnDetailsVb.setFilterContext(rs.getString("FILTER_CONTEXT"));
						colVbList.add(columnDetailsVb);
					}
					tableDetailsVb.setChildren(colVbList);

					String type = "FILE";
					if (tableDetailsVb.getTableType().equalsIgnoreCase("T")) {
						type = "MACROVAR";
					} else if (tableDetailsVb.getTableType().equalsIgnoreCase("M")) {
						type = "M_QUERY";
					}
					DSConnectorVb vObj = new DSConnectorVb();
					vObj.setMacroVar(tableDetailsVb.getSourceConnectorId());
					DSConnectorVb vObj1 = getSpecificConnectorDetails(vObj);
					tableDetailsVb.setDatabaseType(type);
					if (ValidationUtil.isValid(vObj1.getMacroVarScript())) {
						tableDetailsVb.setDatabaseConnectivityDetails(
								CommonUtils.getValue(vObj1.getMacroVarScript(), "DATABASE_TYPE"));
					}
					tableDetailsHM.put(String.valueOf(tableDetailsVb.getTableId()), tableDetailsVb);
					return null;
				}
			}, args);

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		returnArrayList.add(linkedTblsForIndividualTblHM);
		returnArrayList.add(relationHM);
		returnArrayList.add(tableDetailsHM);
		return returnArrayList;
	}

	public Integer returnBaseTableId(String country, String leBook, String feedId) {
		try {
			String sql = "SELECT BASE_TABLE_FLAG FROM ETL_FEED_TABLES_UPL WHERE COUNTRY= ? AND  LE_BOOK= ?  AND  FEED_ID=? ";
			Object[] args = { country, leBook, feedId };

			String baseFlagStr = getJdbcTemplate().query(sql, new ResultSetExtractor<String>() {
				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					if (rs.next()) {
						return rs.getString("BASE_TABLE_FLAG");
					} else {
						return null;
					}
				}
			},args);

			if (ValidationUtil.isValid(baseFlagStr)) {
				if ("Y".equalsIgnoreCase(baseFlagStr)) {
					sql = "SELECT TABLE_ID FROM ETL_FEED_TABLES_UPL WHERE COUNTRY= ? AND  LE_BOOK= ? AND  FEED_ID= ? AND BASE_TABLE_FLAG='Y' ORDER BY TABLE_ID";
					return getJdbcTemplate().query(sql, new ResultSetExtractor<Integer>() {
						@Override
						public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
							if (rs.next()) {
								return rs.getInt("TABLE_ID");
							} else {
								return 0;
							}
						}
					},args);

				} else {
					return null;
				}
			} else {
				return null;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public int createorInsertTable(String createScript) {
		return getJdbcTemplate().update(createScript);
	}

	public String getScriptValue(String pVariableName) throws DataAccessException, Exception {
		Object params[] = { pVariableName };
		String sql = new String(
				"select VARIABLE_SCRIPT from VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_TYPE = 2  AND UPPER(VARIABLE_NAME)=UPPER(?)");
		return getJdbcTemplate().queryForObject(sql, params, String.class);
	}

	@SuppressWarnings("unchecked")
	public List<EtlFeedMainVb> getCategoryListing(EtlFeedMainVb vObject) throws DataAccessException {
		String sql = "SELECT COUNTRY, LE_BOOK, CATEGORY_ID, CATEGORY_DESCRIPTION FROM ETL_FEED_CATEGORY WHERE COUNTRY= ? AND LE_BOOK= ? ORDER BY COUNTRY, LE_BOOK, CATEGORY_ID";
		Object[] args = { vObject.getCountry(), vObject.getLeBook() };
		return getJdbcTemplate().query(sql, args, getCategoryListingMapper());
	}

	protected RowMapper getCategoryListingMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedMainVb vObject = new EtlFeedMainVb();
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setFeedCategory(rs.getString("CATEGORY_ID"));
				vObject.setFeedCategoryDesc(rs.getString("CATEGORY_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}

	@SuppressWarnings("unchecked")
	public List<EtlFeedMainVb> findDependencyFeed(String pCategory, String pFeedID) throws DataAccessException {
		String sql = "SELECT COUNTRY, LE_BOOK, FEED_ID, FEED_NAME FROM ETL_FEED_MAIN WHERE FEED_CATEGORY = ? and FEED_ID != ? AND FEED_STATUS = 0 ORDER BY FEED_NAME";
		Object[] args = { pCategory, pFeedID };
		return getJdbcTemplate().query(sql, args, getDependencyFeedMapper());
	}

	protected RowMapper getDependencyFeedMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedMainVb vObject = new EtlFeedMainVb();
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setFeedId(rs.getString("FEED_ID"));
				vObject.setFeedName(rs.getString("FEED_NAME"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<EtlFeedTransformationMasterVb> getTransformationMasterCategorydetails(
			EtlFeedTransformationMasterVb vObj) throws DataAccessException {
		String sql = "select distinct TRANSFORMATION_CATEGORY from ETL_Transformation_Master";
		return getJdbcTemplate().query(sql, getTransfromationMasterCategoryMapper());
	}

	protected RowMapper getTransfromationMasterCategoryMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedTransformationMasterVb vObject = new EtlFeedTransformationMasterVb();
				vObject.setTransformationCategory(rs.getString("TRANSFORMATION_CATEGORY"));
				return vObject;
			}
		};
		return mapper;
	}

	public Map<String, List<EtlFeedTransformationMasterVb>> getTransformationMasterDetails()
			throws DataAccessException {
		String sql = "SELECT TRANSFORMATION_ID, SORT_ORDER, TRANSFORMATION_CATEGORY_AT, TRANSFORMATION_CATEGORY,"
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2128 AND "
				+ " ALPHA_SUB_TAB = TRANSFORMATION_CATEGORY) TRANSFORMATION_CATEGORY_DESC, "
				+ " TRANSFORMATION_NAME, TRANSFORMATION_DESCRIPTION, TRANSFORMATION_TYPE_AT, TRANSFORMATION_TYPE,"
				+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 2126 AND "
				+ " ALPHA_SUB_TAB = TRANSFORMATION_TYPE) TRANSFORMATION_TYPE_DESC,  IO_DATA_TYPE_AT, "
				+ " OUTPUT_COUNT, OUTPUT_TYPE, SYNTAX_EXPRESSION, EXAMPLE,"
				+ " TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS ,"
				+ " INPUT_SLAB_COUNT, IPS_1_DATATYPE, IPS_2_DATATYPE, IPS_3_DATATYPE, IPS_4_DATATYPE, "
				+ " IPS_1_NAME, IPS_1_DESC, IPS_2_NAME, IPS_2_DESC, IPS_3_NAME, IPS_3_DESC, IPS_4_NAME, IPS_4_DESC, "
				+ " IPS_1_ID,  IPS_2_ID, IPS_3_ID, IPS_4_ID ,"
				+ " IPS_1_MIN_COUNT, IPS_2_MIN_COUNT, IPS_3_MIN_COUNT, IPS_4_MIN_COUNT, "
				+ " IPS_1_MAX_COUNT, IPS_2_MAX_COUNT, IPS_3_MAX_COUNT, IPS_4_MAX_COUNT,"
				+ " IPS_1_VALUE_TYPE,IPS_2_VALUE_TYPE,IPS_3_VALUE_TYPE, IPS_4_VALUE_TYPE, "
				+ " IPS_1_VALUE,IPS_2_VALUE,IPS_3_VALUE,IPS_4_VALUE, "
				+ " IPS_1_DEFAULT_VALUE,IPS_2_DEFAULT_VALUE,IPS_3_DEFAULT_VALUE,IPS_4_DEFAULT_VALUE "
				+ " from ETL_TRANSFORMATION_MASTER  where TRANSFORMATION_STATUS = 0 ";

		return getJdbcTemplate().query(sql, new ResultSetExtractor<Map<String, List<EtlFeedTransformationMasterVb>>>() {

			@Override
			public Map<String, List<EtlFeedTransformationMasterVb>> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				Map<String, List<EtlFeedTransformationMasterVb>> returnMap = new HashMap<String, List<EtlFeedTransformationMasterVb>>();
				while (rs.next()) {
					EtlFeedTransformationMasterVb transMasterVb = new EtlFeedTransformationMasterVb();
					transMasterVb.setTransformationId(rs.getString("TRANSFORMATION_ID"));
					transMasterVb.setSortOrder(rs.getInt("SORT_ORDER"));
					transMasterVb.setTransformationCategoryAT(rs.getInt("TRANSFORMATION_CATEGORY_AT"));
					transMasterVb.setTransformationCategory(rs.getString("TRANSFORMATION_CATEGORY"));
					transMasterVb.setTransformationCategoryDesc(rs.getString("TRANSFORMATION_CATEGORY_DESC"));
					transMasterVb.setTransformationName(rs.getString("TRANSFORMATION_NAME"));
					transMasterVb.setTransformationDesc(rs.getString("TRANSFORMATION_DESCRIPTION"));
					transMasterVb.setTransformationTypeAT(rs.getInt("TRANSFORMATION_TYPE_AT"));
					transMasterVb.setTransformationType(rs.getString("TRANSFORMATION_TYPE"));
					transMasterVb.setTransformationTypeDesc(rs.getString("TRANSFORMATION_TYPE_DESC"));
					// transMasterVb.setInputCount(rs.getInt("INPUT_COUNT"));
					transMasterVb.setIoDataTypeAT(rs.getInt("IO_DATA_TYPE_AT"));
					// transMasterVb.setInputType(rs.getString("INPUT_TYPE"));
					// transMasterVb.setInputTypeDesc(rs.getString("INPUT_TYPE_DESC"));
					transMasterVb.setOutputCount(rs.getInt("OUTPUT_COUNT"));
					transMasterVb.setOutputType(rs.getString("OUTPUT_TYPE"));
					transMasterVb.setSyntaxExpresssion(rs.getString("SYNTAX_EXPRESSION"));
					transMasterVb.setExample(rs.getString("EXAMPLE"));
					transMasterVb.setTransformationStatusNt(rs.getInt("TRANSFORMATION_STATUS_NT"));
					transMasterVb.setTransformationStatus(rs.getInt("TRANSFORMATION_STATUS"));

					transMasterVb.setInputSlabCount(rs.getInt("INPUT_SLAB_COUNT"));

					int inputSlabCount = rs.getInt("INPUT_SLAB_COUNT");

					List<EtlTransMasterInputSlabVb> slabList = new ArrayList<>();

					for (int i = 1; i <= inputSlabCount; i++) {
						EtlTransMasterInputSlabVb vb = new EtlTransMasterInputSlabVb();
						vb.setIpsDatatype(ValidationUtil.isValid(rs.getString("IPS_" + i + "_DATATYPE"))
								? rs.getString("IPS_" + i + "_DATATYPE")
								: "");
						vb.setIpsName(ValidationUtil.isValid(rs.getString("IPS_" + i + "_NAME"))
								? rs.getString("IPS_" + i + "_NAME")
								: "");
						vb.setIpsDesc(ValidationUtil.isValid(rs.getString("IPS_" + i + "_DESC"))
								? rs.getString("IPS_" + i + "_DESC")
								: "");
						vb.setIpsId(rs.getInt("IPS_" + i + "_ID"));
						vb.setIpsMinCount(rs.getInt("IPS_" + i + "_MIN_COUNT"));
						vb.setIpsMaxCount(rs.getInt("IPS_" + i + "_MAX_COUNT"));
						vb.setIpsValue(ValidationUtil.isValid(rs.getString("IPS_" + i + "_VALUE"))
								? rs.getString("IPS_" + i + "_VALUE")
								: "");
						vb.setIpsValueType(ValidationUtil.isValid(rs.getString("IPS_" + i + "_VALUE_TYPE"))
								? rs.getString("IPS_" + i + "_VALUE_TYPE")
								: "");
						vb.setIpsDefaultValue(ValidationUtil.isValid(rs.getString("IPS_" + i + "_DEFAULT_VALUE"))
								? rs.getString("IPS_" + i + "_DEFAULT_VALUE")
								: "");
						slabList.add(vb);
					}

					transMasterVb.setSlabList(slabList);

					/*
					 * if(returnMap.get(transMasterVb.getTransformationCategory()) != null) {
					 * List<EtlFeedTransformationMasterVb> transMasterList =
					 * returnMap.get(transMasterVb.getTransformationCategory());
					 * transMasterList.add(transMasterVb);
					 * returnMap.put(transMasterVb.getTransformationCategory(), transMasterList); }
					 * else { returnMap.put(transMasterVb.getTransformationCategory(), new
					 * ArrayList<EtlFeedTransformationMasterVb>(Arrays.asList(transMasterVb))); }
					 */

					if (returnMap.get(transMasterVb.getTransformationCategoryDesc()) != null) {
						List<EtlFeedTransformationMasterVb> transMasterList = returnMap
								.get(transMasterVb.getTransformationCategoryDesc());
						transMasterList.add(transMasterVb);
						returnMap.put(transMasterVb.getTransformationCategoryDesc(), transMasterList);
					} else {
						returnMap.put(transMasterVb.getTransformationCategoryDesc(),
								new ArrayList<EtlFeedTransformationMasterVb>(Arrays.asList(transMasterVb)));
					}
				}
				return returnMap;
			}

		});
	}

	public Boolean CheckFeedExists(EtlFeedMainVb vObject) {
		boolean feedExists = false;
		for (int k = 0; k < 3; k++) {
			String tableName = "etl_feed_injection_config_upl";
			if (k == 1) {
				tableName = "etl_feed_injection_config_pend";
			} else if (k == 2) {
				tableName = "etl_feed_injection_config";
			}
			String sql = "SELECT Count(*) as cnt FROM " + tableName
					+ " WHERE COUNTRY = ? AND LE_BOOK= ? and FEED_ID= ? ";

			Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
			String data = getJdbcTemplate().queryForObject(sql, args, String.class);
			if (ValidationUtil.isValid(data) && !data.equalsIgnoreCase("0")) {
				feedExists = true;
				break;
			}
		}
		return feedExists;
	}

	public Boolean CheckRelationFeedExists(EtlForQueryReportVb vObject) {
		boolean feedExists = false;
		for (int k = 0; k < 3; k++) {
			String tableName = "ETL_FEED_RELATION_UPL";
			if (k == 1) {
				tableName = "ETL_FEED_RELATION_pend";
			} else if (k == 2) {
				tableName = "ETL_FEED_RELATION";
			}
			String sql = "SELECT Count(*) as cnt FROM " + tableName
					+ " WHERE COUNTRY = ? AND LE_BOOK= ? and FEED_ID= ?";
			Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
			String data = getJdbcTemplate().queryForObject(sql, args, String.class);
			if (ValidationUtil.isValid(data) && !data.equalsIgnoreCase("0")) {
				feedExists = true;
				break;
			}
		}
		return feedExists;
	}

	
	
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doUpdateRecordAppr(EtlFeedMainVb vObject) {// For verification Reqd False
		ExceptionCode exceptionCode = new ExceptionCode();
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		try {
			if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_DELETED) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Cannot Modify the Deleted Record!!");
				return exceptionCode;
			}
			
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				exceptionCode = doUpdateRecordPend_WORK_IN_PROGRESS(vObject);
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				exceptionCode = doUpdateRecordPend_PUBLISHED(vObject);
			}
			return exceptionCode;
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
	
	private ExceptionCode doUpdateRecordPend_PUBLISHED_AND_WORK_IN_PROGRESS(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		EtlFeedMainVb vObjectlocal = new EtlFeedMainVb();
		List collTemp = null;
		int approveOrPend = 0;
		int status = 0;
		
		status = Constants.WORK_IN_PROGRESS;
		approveOrPend = 1;
		collTemp = doSelectPendingRecord(vObject);
		List<EtlFeedMainVb> mainVbUplLst = doSelectUplRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else if (collTemp.size() > 0 && mainVbUplLst.isEmpty()) {
			vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
			retVal = deletePendingRecord(vObjectlocal);
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE) {
				exceptionCode = getResultObject(Constants.RECORD_PENDING_FOR_DELETION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			vObject.setVerifier(0);
			vObject.setMaker(intCurrentUserId);
			vObject.setFeedStatus(status);
			retVal = doInsertionUplWithDc(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else if (collTemp.size() == 0) {
			exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		} else if (!mainVbUplLst.isEmpty()) {
			exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		updateSourceDataPend(vObject, status, approveOrPend, false, Constants.MODIFY_PENDING);
		updateExtractionDataPend(vObject, status, approveOrPend, false, Constants.MODIFY_PENDING);
		updateTargetDataPend(vObject, status, approveOrPend, false, Constants.MODIFY_PENDING);
		if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
			updateTransformedDataPend(vObject, status, approveOrPend, false, Constants.MODIFY_PENDING);
		}
		updateScheduleDataPend(vObject, status, approveOrPend, false, Constants.MODIFY_PENDING);
		if ("Y".equalsIgnoreCase(vObject.getIsValidType()) && vObject.getTabId() == 1) {
			vObject.setFeedStatus(Constants.WORK_IN_PROGRESS);
			vObject.setRecordIndicator(2);
		}
		updateAllApproveDataStatus(vObject, status, "UPL");
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	
	}
	
	private ExceptionCode doUpdateRecordPend_WORK_IN_PROGRESS(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		EtlFeedMainVb vObjectlocal = new EtlFeedMainVb();
		List collTemp = null;
		int approveOrPend = 0;
		int status = 0;
		
		status = Constants.WORK_IN_PROGRESS;
		approveOrPend = 9999;
		collTemp = doSelectUplRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
			retVal = deleteUplRecord(vObjectlocal);
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE) {
				exceptionCode = getResultObject(Constants.RECORD_PENDING_FOR_DELETION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			vObject.setVerifier(0);
			vObject.setMaker(intCurrentUserId);
			vObject.setFeedStatus(status);
			retVal = doInsertionUplWithDc(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		updateSourceDataUpl(vObject, status, approveOrPend);
		updateExtractionDataUpl(vObject, status, approveOrPend);
		updateTargetDataUpl(vObject, status, approveOrPend);
		if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
			updateTransformedDataUpl(vObject, status, approveOrPend);
		}
		updateScheduleDataUpl(vObject, status, approveOrPend);
		if ("Y".equalsIgnoreCase(vObject.getIsValidType()) && vObject.getTabId() == 1) {
			vObject.setFeedStatus(Constants.WORK_IN_PROGRESS);
			vObject.setRecordIndicator(2);
		}
		updateAllApproveDataStatus(vObject, status, "UPL");
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}
	
	private ExceptionCode doUpdateRecordPend_PUBLISHED(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		EtlFeedMainVb vObjectlocal = new EtlFeedMainVb();
		List collTemp = null;
		int approveOrPend = 0;
		int status = 0;

		if (getStatusCnt(vObject.getFeedId()) > 0) {
			updateAllApproveDataStatus(vObject, Constants.PUBLISHED, "APPROVE");
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} else {
			status = Constants.WORK_IN_PROGRESS;
			approveOrPend = 0;
			collTemp = selectApprovedRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() > 0) {
				vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
				vObject.setDateCreation(vObjectlocal.getDateCreation());
				vObject.setVerifier(0);
				vObject.setMaker(intCurrentUserId);
				vObject.setFeedStatus(status);
				vObject.setRecordIndicator(Constants.MODIFY_PENDING); // Approve
				retVal = doInsertionUplWithDc(vObject);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}

			updateSourceDataApprove(vObject, status, approveOrPend, true, Constants.MODIFY_PENDING);
			updateExtractionDataApprove(vObject, status, approveOrPend, true, Constants.MODIFY_PENDING);
			updateTargetDataApprove(vObject, status, approveOrPend, true, Constants.MODIFY_PENDING);
			if ("TRAN".equalsIgnoreCase(vObject.getFeedType())) {
				updateTransformedDataApprove(vObject, status, approveOrPend, true, Constants.MODIFY_PENDING);
			}
			updateScheduleDataApprove(vObject, status, approveOrPend, true, Constants.MODIFY_PENDING);
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doUpdateRecordPend(EtlFeedMainVb vObject) {// For verification Reqd True
		ExceptionCode exceptionCode = new ExceptionCode();
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		try {
			if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_DELETED) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Cannot Modify the Deleted Record!!");
				return exceptionCode;
			}
			if (vObject.getRecordIndicator() == Constants.DELETE_PENDING) {
				exceptionCode = getResultObject(Constants.RECORD_PENDING_FOR_DELETION);
				return exceptionCode;
			}
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				exceptionCode = doUpdateRecordPend_WORK_IN_PROGRESS(vObject);
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				exceptionCode = doUpdateRecordPend_PUBLISHED_AND_WORK_IN_PROGRESS(vObject);
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				exceptionCode = doUpdateRecordPend_PUBLISHED(vObject);
			}
			return exceptionCode;
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteRecordAppr(EtlFeedMainVb vObject) {// For verification Reqd False
		ExceptionCode exceptionCode = new ExceptionCode();
		List collTemp = null;
		EtlFeedMainVb vObjectlocal = new EtlFeedMainVb();
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		try {
			int approveOrPend = 0;
			int status = 0;
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				status = Constants.WORK_IN_PROGRESS;
				approveOrPend = 9999;
				collTemp = doSelectUplRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
					deleteAllUplData(vObjectlocal, "Y");
				}
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				status = Constants.PUBLISHED_AND_WORK_IN_PROGRESS;
				approveOrPend = 1;
				collTemp = doSelectPendingRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
					deleteAllPendData(vObjectlocal, "Y");
				}
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				status = Constants.PUBLISHED;
				approveOrPend = 0;
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					if (vObject.isStaticDelete()) {
						updateAllApproveDataStatus(vObject, Constants.PUBLISHED_AND_DELETED, "APPROVE");
					} else {
						deleteAllApproveData(vObject);
					}
				}
			}
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteRecordPend(EtlFeedMainVb vObject) {// For verification Reqd True
		ExceptionCode exceptionCode = new ExceptionCode();
		List collTemp = null;
		EtlFeedMainVb vObjectlocal = new EtlFeedMainVb();
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		try {
			int approveOrPend = 0;
			int status = 0;
			if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_DELETED) {
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				return exceptionCode;
			}
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				status = Constants.WORK_IN_PROGRESS;
				approveOrPend = 9999;
				collTemp = doSelectUplRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
					deleteAllUplData(vObjectlocal, "Y");
				}
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				status = Constants.WORK_IN_PROGRESS;
				approveOrPend = 1;
				collTemp = doSelectPendingRecord(vObject);
				if (collTemp == null) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
					// retVal = deletePendingRecord(vObjectlocal);
					vObject.setDateCreation(vObjectlocal.getDateCreation());
					vObject.setVerifier(0);
					vObject.setMaker(intCurrentUserId);
					vObject.setFeedStatus(status);
					vObject.setRecordIndicator(Constants.DELETE_PENDING);
					retVal = doInsertionUplWithDc(vObject);
					updateSourceDataPend(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
					updateExtractionDataPend(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
					updateTargetDataPend(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
					updateScheduleDataPend(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
				}
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				status = Constants.PUBLISHED;
				approveOrPend = 0;
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()) {
					exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (collTemp.size() > 0) {
					vObjectlocal = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
					// retVal = doDeleteAppr(vObjectlocal);
					vObject.setDateCreation(vObjectlocal.getDateCreation());
					vObject.setVerifier(0);
					vObject.setMaker(intCurrentUserId);
					vObject.setFeedStatus(status);
					vObject.setRecordIndicator(Constants.DELETE_PENDING); // Approve
					retVal = doInsertionPendWithDc(vObject);
					updateSourceDataApprove(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
					updateExtractionDataApprove(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
					updateTargetDataApprove(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
					updateScheduleDataApprove(vObject, status, approveOrPend, true, Constants.DELETE_PENDING);
				}
			}
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doPublishForTransaction(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();
		strCurrentOperation = Constants.PUBLISH;
		try {
			List collTemp = doSelectUplRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_UPL_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			EtlFeedMainVb vObjectlocalUpl = ((ArrayList<EtlFeedMainVb>) collTemp).get(0);
			exceptionCode = insertApprovePublishRecord(vObject, vObject.isVerificationRequired(), vObjectlocalUpl);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				deleteAllUplData(vObject, "Y");
			}
			return exceptionCode;
		} catch (RuntimeCustomException ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	public String getConnectorReadinessScript(String connectorId) {
		String script = "";
		try {
			String query = " Select " + getDbFunction(Constants.CONVERT, null)
					+ "(Script) from etl_connector_Scripts where connector_id = ? order by SCRIPT_ID";
			Object [] args= {connectorId};
			return getJdbcTemplate().queryForObject(query, args, String.class);
		} catch (Exception e) {
			return script;
		}
	}

	public List<EtlFeedMainVb> findLeBookByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null)
				+ "LE_BOOK CL FROM LE_BOOK LEB WHERE LEB_STATUS=0 AND UPPER(LEB.LEB_DESCRIPTION) " + val
				+ "ORDER BY LE_BOOK";
		return getJdbcTemplate().query(sql, getMapperForLeBook());
	}

	protected RowMapper getMapperForLeBook() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedMainVb etlFeedCategoryVb = new EtlFeedMainVb();
				etlFeedCategoryVb.setLeBook(rs.getString("CL"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}

	public List<EtlFeedMainVb> findCountryByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY FROM COUNTRIES CT WHERE UPPER(CT.COUNTRY_DESCRIPTION) " + val + "ORDER BY COUNTRY";
		return getJdbcTemplate().query(sql, getMapperForCountry());
	}

	protected RowMapper getMapperForCountry() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedMainVb etlFeedCategoryVb = new EtlFeedMainVb();
				etlFeedCategoryVb.setCountry(rs.getString("COUNTRY"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
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

	public static String convert(String json, String root) throws JSONException {
		JSONObject jsonObject = new JSONObject(json);
		String xml = "<" + root + ">" + XML.toString(jsonObject) + "</" + root + ">";
		return xml;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doSaveFeedDataRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		try {
			return doSaveFeedDataUpl(vObject);
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

	protected ExceptionCode doSaveFeedDataUpl(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.ADD;
		strApproveOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());

		deleteUplRecord(vObject);
		// Try inserting the record
		int recordInd = 0;
		if (vObject.isVerificationRequired())
			recordInd = 2;
		vObject.setRecordIndicator(recordInd);
		vObject.setFeedStatus(Constants.WORK_IN_PROGRESS);

//		vObject.setVerifier(getIntCurrentUserId());
		retVal = doInsertionUpl(vObject);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		vObject.setDateCreation(systemDate);
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertTransformationMasterRecord(EtlFeedMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		try {
			vObject.setMaker(getIntCurrentUserId());
			// Try inserting the record
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			vObject.setFeedStatus(Constants.PUBLISHED);
			vObject.setVerifier(getIntCurrentUserId());
			retVal = doInsertionTransformationAppr(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			exceptionCode.setOtherInfo(vObject);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			exceptionCode.setOtherInfo(vObject);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	public int doInsertionTransformationAppr(EtlFeedMainVb vObject) {
		String query = "Insert Into ETL_TRANSFORMATION_SESSION (COUNTRY, LE_BOOK, FEED_ID, VISION_ID, SESSION_ID,  "
				+ " SESSION_STATUS_NT, SESSION_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?," + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), intCurrentUserId,
				vObject.getSessionId(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		return getJdbcTemplate().update(query, args);
	}

	public ExceptionCode doUpdateSessionForTransformation(EtlFeedMainVb vObject) {
		setServiceDefaults();
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String query = "UPDATE ETL_TRANSFORMATION_SESSION SET DATE_LAST_MODIFIED = "
					+ getDbFunction(Constants.SYSDATE, null) + " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   "
					+ " AND VISION_ID = ? AND SESSION_ID = ?  ";
			Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), intCurrentUserId,
					vObject.getSessionId() };
			int updateCount = getJdbcTemplate().update(query, args);
			if (updateCount == 1) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg(
						"No record or more than one record present in Session maintenance for details [Feed ID - "
								+ vObject.getFeedId() + " User ID - " + intCurrentUserId + " Session ID - "
								+ vObject.getSessionId() + "]");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in session maintenance. [Feed ID - " + vObject.getFeedId()
					+ " User ID - " + intCurrentUserId + " Session ID - " + vObject.getSessionId() + "]. Cause: "
					+ e.getMessage());
		}
		return exceptionCode;
	}

	public int getStatusCnt(String feedId) {
		String sql = "SELECT count(1) FROM etl_feed_main where feed_status =9 and feed_id= ? ";
		Object[] args = {feedId};
		int i = getJdbcTemplate().queryForObject(sql, args, Integer.class);
		return i;
	}

	public List<EtlFeedScheduleConfigVb> getDetails(String feedId) {
		List<EtlFeedScheduleConfigVb> collTemp = null;
		String sql = "SELECT SCHEDULE_TYPE, PROCESS_DATE_TYPE FROM ETL_FEED_SCHEDULE_config where  feed_id= ? ";
		try {
			Object[] args = {feedId};
			collTemp = getJdbcTemplate().query(sql, args, getETLFeedScheduleMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return collTemp;
		}
	}

	private RowMapper getETLFeedScheduleMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedScheduleConfigVb vObj = new EtlFeedScheduleConfigVb();
				vObj.setScheduleType(rs.getString("SCHEDULE_TYPE"));
				vObj.setProcessDateType(rs.getString("PROCESS_DATE_TYPE"));
				return vObj;
			}
		};
		return mapper;
	}

	public String getTimeZoneVisionBusinessDate(String countryLeBook) {
		String sql = "select TIME_ZONE  from Vision_Business_Day  WHERE COUNTRY "
				+ getDbFunction(Constants.PIPELINE, null) + "'-'" + getDbFunction(Constants.PIPELINE, null)
				+ " LE_BOOK= ? AND UPPER(APPLICATION_ID) = 'ETL'";
		Object args[] = { countryLeBook };
		return getJdbcTemplate().queryForObject(sql, args, String.class);
	}

	public String checkConnector(EtlFeedMainVb vObj) {
		String isValid = "Y";
		String sourceTableName = "ETL_FEED_SOURCE";
		try {
			if (vObj.getFeedStatus() == 1) {
				sourceTableName = "ETL_FEED_SOURCE_UPL";
			} else if (vObj.getFeedStatus() == 2) {
				sourceTableName = "ETL_FEED_SOURCE_PEND";
			}
			Object[] args = {vObj.getFeedId()};
			String que = "select count(*) from " + sourceTableName + " t1 "
					+ " join ETL_CONNECTOR t2 on t1.SOURCE_CONNECTOR_ID= t2.connector_id where feed_id=? ";

			int i = getJdbcTemplate().queryForObject(que, Integer.class);

			if (ValidationUtil.isValid(vObj.getFeedType()) && "DB_LINK".equalsIgnoreCase(vObj.getFeedType())) {
				que = que + " AND DB_LINK_FLAG ='Y'";
			} else {
				que = que + " AND DB_LINK_FLAG ='N'";
			}
			int k = getJdbcTemplate().queryForObject(que,args, Integer.class);
			if (k != i) {
				isValid = "N";
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return isValid;
	}

	public int deleteEtlTransformationSession(EtlFeedMainVb vObject, boolean schedulerFlag) {
		// System.out.println(" schedulerFlag " + schedulerFlag);
		if (schedulerFlag == false) { // Gives null pointer Exception for Scheduler
			setServiceDefaults();
		}
		String query = "DELETE FROM ETL_TRANSFORMATION_SESSION WHERE COUNTRY = ?  AND LE_BOOK = ?  AND "
				+ " FEED_ID = ?  AND SESSION_ID = ? and VISION_ID = ? ";
		long visionId = 0;
		if (schedulerFlag == true) {
			visionId = vObject.getVisionId();
		} else {
			visionId = intCurrentUserId;
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId(),
				visionId };
		for (int i = 0; i < args.length; i++) {
			// System.out.println("Index [" + i + "] Value [" + args[i] + "] ");
		}
		return getJdbcTemplate().update(query, args);
	}

	public List<EtlFeedMainVb> getEtlTransformationSession(String currentTime) {
		try {
			List<EtlFeedMainVb> vbs = new ArrayList<EtlFeedMainVb>();
			String query = "select COUNTRY, LE_BOOK, FEED_ID, SESSION_ID, VISION_ID "
					+ " FROM ETL_TRANSFORMATION_SESSION WHERE DATE_LAST_MODIFIED < "
					+ getDbFunction(Constants.TO_DATE, currentTime);

			ResultSetExtractor rseObj = new ResultSetExtractor() {
				public List<EtlFeedMainVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
					List<EtlFeedMainVb> vbs = new ArrayList<EtlFeedMainVb>();
					while (rs.next()) {
						EtlFeedMainVb vb = new EtlFeedMainVb();
						vb.setFeedId(rs.getString("FEED_ID"));
						vb.setSessionId(rs.getString("SESSION_ID"));
						vb.setCountry(rs.getString("COUNTRY"));
						vb.setLeBook(rs.getString("LE_BOOK"));
						vb.setVisionId(rs.getInt("VISION_ID"));
						vbs.add(vb);
					}
					return vbs;
				}
			};
			vbs = (List<EtlFeedMainVb>) getJdbcTemplate().query(query, rseObj);
			return vbs;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}

	}

	public String getMethodNameForTransId(String transId) {
		String methodName = "";
		try {
			String sql = "SELECT JAVA_METHOD_NAME from ETL_TRANSFORMATION_MASTER WHERE upper(TRANSFORMATION_ID) = ? AND TRANSFORMATION_STATUS = 0  ";
			Object[] args = {transId.toUpperCase()};
			methodName = getJdbcTemplate().queryForObject(sql,args, String.class);
			return methodName;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return "";
		}
	}

	public String getTransTypeForTransId(String transId) {
		String transType = "";
		try {
			String sql = "SELECT TRANSFORMATION_TYPE from ETL_TRANSFORMATION_MASTER WHERE upper(TRANSFORMATION_ID) = ? AND TRANSFORMATION_STATUS = 0  ";
			Object[] args = {transId.toUpperCase()};
			transType = getJdbcTemplate().queryForObject(sql, args, String.class);
			return transType;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return "";
		}
	}

	public List<ETLFeedTablesVb> getEtlSourceTableList(EtlFeedMainVb vObject, int approveOrPend) {
		List<ETLFeedTablesVb> collTemp = new ArrayList<ETLFeedTablesVb>();
		String tblAppendStr = " ";
		if (approveOrPend == 9999) {
			tblAppendStr = "_UPL ";
		} else if (approveOrPend == 1) {
			tblAppendStr = "_PEND ";
		}
		try {
			String sql = "SELECT t3.COUNTRY,                  t3.LE_BOOK,                            "
					+ "t3.FEED_ID,                            t3.TABLE_ID,                           "
					+ "t3.SOURCE_CONNECTOR_ID,                t3.TABLE_NAME,                         "
					+ "t3.TABLE_ALIAS_NAME,                   t3.TABLE_SORT_ORDER,                   "
					+ "t3.TABLE_TYPE_AT,                      t3.TABLE_TYPE,                         "
					+ "t3.QUERY_ID,                           t3.CUSTOM_PARTITION_COLUMN_FLAG,       "
					+ "t3.BASE_TABLE_FLAG,                    t3.FEED_TABLE_STATUS_NT,               "
					+ "t3.FEED_TABLE_STATUS,                  t3.RECORD_INDICATOR_NT,                "
					+ "t3.RECORD_INDICATOR,                   t3.MAKER,                              "
					+ "t3.VERIFIER,                           t3.INTERNAL_STATUS,                    "
					+ "t3.DATE_LAST_MODIFIED,                 t3.DATE_CREATION,                      "
					+ "t3.PARTITION_COLUMN_NAME               FROM etl_feed_main" + tblAppendStr + " t1  "
					+ "INNER JOIN ETL_FEED_SOURCE" + tblAppendStr + " t2      ON ( t1.COUNTRY = '"
					+ vObject.getCountry() + "' and t1.LE_BOOK = '" + vObject.getLeBook() + "' and  t1.FEED_ID = '"
					+ vObject.getFeedId() + "' AND T1.COUNTRY = T2.COUNTRY AND T1.LE_BOOK = T2.LE_BOOK "
					+ "AND t1.FEED_ID = t2.FEED_ID)           INNER JOIN ETL_FEED_TABLES" + tblAppendStr + " t3 "
					+ "ON (    t2.COUNTRY = t3.COUNTRY        AND t2.LE_BOOK = t3.LE_BOOK            "
					+ "AND t2.FEED_ID = t3.FEED_ID            AND t2.SOURCE_CONNECTOR_ID =           "
					+ "t3.SOURCE_CONNECTOR_ID)                ";
			collTemp = getJdbcTemplate().query(sql, new ResultSetExtractor<List<ETLFeedTablesVb>>() {
				@Override
				public List<ETLFeedTablesVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
					List<ETLFeedTablesVb> collTemp = new ArrayList<ETLFeedTablesVb>();
					while (rs.next()) {
						ETLFeedTablesVb vobj = new ETLFeedTablesVb();
						vobj.setCountry(rs.getString("COUNTRY"));
						vobj.setLeBook(rs.getString("LE_BOOK"));
						vobj.setFeedId(rs.getString("FEED_ID"));
						vobj.setTableId(rs.getInt("TABLE_ID"));
						vobj.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
						vobj.setTableName(rs.getString("TABLE_NAME"));
						vobj.setTableAliasName(rs.getString("TABLE_ALIAS_NAME"));
						vobj.setTableSortOrder(rs.getInt("TABLE_SORT_ORDER"));
						vobj.setTableTypeAt(rs.getInt("TABLE_TYPE_AT"));
						vobj.setTableType(rs.getString("TABLE_TYPE"));
						vobj.setQueryId(rs.getString("QUERY_ID"));
						vobj.setCustomPartitionColumnFlag(rs.getString("CUSTOM_PARTITION_COLUMN_FLAG"));
						vobj.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
						vobj.setFeedTableStatusNt(rs.getInt("FEED_TABLE_STATUS_NT"));
						vobj.setFeedTableStatus(rs.getInt("FEED_TABLE_STATUS"));
						vobj.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
						vobj.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
						vobj.setMaker(rs.getInt("MAKER"));
						vobj.setVerifier(rs.getInt("VERIFIER"));
						vobj.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
						vobj.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
						vobj.setDateCreation(rs.getString("DATE_CREATION"));
						vobj.setPartitionColumnName(rs.getString("PARTITION_COLUMN_NAME"));
						collTemp.add(vobj);
					}
					return collTemp;
				}
			});
			return collTemp;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}
	protected int deletePRSRecordByCategory(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_MAIN_PRS Where COUNTRY = ?  AND LE_BOOK = ?  and FEED_CATEGORY = ? AND SESSION_ID = ? ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedCategory(), dObj.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

}
