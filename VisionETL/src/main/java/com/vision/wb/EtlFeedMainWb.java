package com.vision.wb;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.apache.poi.util.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.vision.dao.AbstractDao;
import com.vision.dao.CommonDao;
import com.vision.dao.EtlExtractionSummaryFieldsDao;
import com.vision.dao.EtlFeedColumnsDao;
import com.vision.dao.EtlFeedDestinationDao;
import com.vision.dao.EtlFeedInjectionConfigDao;
import com.vision.dao.EtlFeedLoadMappingDao;
import com.vision.dao.EtlFeedMainDao;
import com.vision.dao.EtlFeedRelationDao;
import com.vision.dao.EtlFeedScheduleConfigDao;
import com.vision.dao.EtlFeedSourceDao;
import com.vision.dao.EtlFeedTablesDao;
import com.vision.dao.EtlFeedTranColumnsDao;
import com.vision.dao.EtlFeedTransformationDao;
import com.vision.dao.EtlTargetColumnsDao;
import com.vision.dao.EtlTransformedColumnsDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.TransformationFunctionsUtil;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.CommonVb;
import com.vision.vb.DSConnectorVb;
import com.vision.vb.ETLFeedColumnsVb;
import com.vision.vb.ETLFeedTablesVb;
import com.vision.vb.EtlExtractionSummaryFieldsVb;
import com.vision.vb.EtlFeedDestinationVb;
import com.vision.vb.EtlFeedInjectionConfigVb;
import com.vision.vb.EtlFeedLoadMappingVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedRelationVb;
import com.vision.vb.EtlFeedScheduleConfigVb;
import com.vision.vb.EtlFeedSourceVb;
import com.vision.vb.EtlFeedTranColumnVb;
import com.vision.vb.EtlFeedTranGroupVb;
import com.vision.vb.EtlFeedTranNodeVb;
import com.vision.vb.EtlFeedTranVb;
import com.vision.vb.EtlFeedTransformationMasterVb;
import com.vision.vb.EtlFeedTransformationVb;
import com.vision.vb.EtlForQueryReportVb;
import com.vision.vb.EtlTargetColumnsVb;
import com.vision.vb.EtlTransformedColumnsVb;
import com.vision.vb.ReviewResultVb;

@Component
@EnableAsync
public class EtlFeedMainWb extends AbstractDynaWorkerBean<EtlFeedMainVb>{

	private final String TICK = "`";
	private final String QUOTE = "'";
	private final String COMMA = ",";
	private final String SPACE = " ";
	private final String INNER_JOIN = " INNER JOIN ";
	private final String ON = " ON ";
	private final String ROWNUM = ".ROWNUM";
	private final String EQUAL = " = ";
	private final String OPEN_BRACKET = "(";
	private final String CLOSE_BRACKET = ")";
	private final String UNDESCORE = "_";
	
	@Value("${ftp.hostName}")
	private String hostName;
	
	@Value("${ftp.port}")
	private String port;

	@Value("${ftp.userName}")
	private String userName;

	@Value("${ftp.password}")
	private String password;
	
	@Value("${ftp.transSampleOperationDir}")
	private String transSampleOperationDir;
	
	@Value("${feed.upload.securedFtp}")
	private String securedFtp = "N";
	
	@Autowired 
	private EtlFeedMainDao etlFeedMainDao;

	@Autowired
	public EtlFeedSourceDao etlFeedSourceDao;
	
	@Autowired
	public EtlFeedTablesDao etlFeedTablesDao;
	
	@Autowired
	public EtlFeedColumnsDao etlFeedColumnsDao;
	
	@Autowired
	public EtlFeedRelationDao etlFeedRelationDao;
	
	@Autowired
	public EtlExtractionSummaryFieldsDao etlExtractionSummaryFieldsDao;
	
	@Autowired
	public EtlFeedTranColumnsDao etlFeedTranColumnsDao;

	@Autowired
	public EtlFeedTransformationDao etlFeedTransformationDao;

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
	private VisionUploadWb visionUploadWb;
	
	@Autowired
	private CommonDao commonDao;
	
	public ArrayList getPageLoadValues(){
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try{
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2102); // Feed Type
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(2000); // Status
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7); // Record Indicator
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2103); // Table Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2104); // Column Data Type
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1103); // Date Format
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1102); // Number Format
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(41); // Join Type
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1102); // Format Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2107); // Schedule Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2108); // Schedule Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2109); //Process Date Type Time Zone Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2110); // Audit Type
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_FEED_MAIN");
			arrListLocal.add(collTemp);
			Map<String, List<AlphaSubTabVb>> schedulerScripts = getEtlFeedMainDao().getSchedulerScript();
			//Map<String, String> schedulerScripts = getEtlFeedMainDao().getFeedCategory();
			arrListLocal.add(schedulerScripts);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1101); // JOIN Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2003); // Expression Text
			arrListLocal.add(collTemp);
//			Map<String, String> transCategory = getEtlFeedMainDao().geTransCategory();
			Map<String, String> transCategory = null;
			arrListLocal.add(transCategory);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2118); // schedule Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2124); // Feed Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2127); // Feed Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2113); // completion status
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2130);
			arrListLocal.add(collTemp);
			return arrListLocal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the Page load values.", ex);
			}
			return null;
		}
	}

	public ExceptionCode formConnectorFileTables(String macroVar, String dbScript, List<ETLFeedTablesVb> treeVbList) {
		List<List<String>> strdataList = new ArrayList<List<String>>();
		List<String> strtabList = new ArrayList<String>();
		ExceptionCode exceptionCode = new ExceptionCode();
		String excludeTableListStr = "";
		if (ValidationUtil.isValidList(treeVbList)) {
			StringBuffer excludeTableList = new StringBuffer("(");
			for (ETLFeedTablesVb treeVb : treeVbList) {
				if (treeVb.getTableType().equalsIgnoreCase("F")) {
					excludeTableList.append(QUOTE + treeVb.getTableName().toUpperCase() + QUOTE + COMMA);
				}
			}
			excludeTableList = new StringBuffer(new String(excludeTableList.substring(0, (excludeTableList.length() - 1))));
			excludeTableList.append(")");
			excludeTableListStr = String.valueOf(excludeTableList);
		}
		try {
			strtabList = etlFeedMainDao.selectConnectorFileTablesList(macroVar, excludeTableListStr);
			strdataList.add(strtabList);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(strdataList);
		return exceptionCode;
	}
	
	private String formExcludeTableString (List<ETLFeedTablesVb> treeVbList, String tableType) {
		if(!ValidationUtil.isValid(tableType)) {
			tableType ="T";
		}
		String excludeTableListStr = null;
		if(ValidationUtil.isValidList(treeVbList)) {
			StringBuffer excludeTableList = new StringBuffer("(");
			for(ETLFeedTablesVb treeVb : treeVbList) {
				if(treeVb.getTableType().equalsIgnoreCase(tableType) || !ValidationUtil.isValid(treeVb.getTableType())) {
					excludeTableList.append(QUOTE + treeVb.getTableName().toUpperCase() + QUOTE + COMMA);
				}
			}
			excludeTableList = new StringBuffer(new String(excludeTableList.substring(0, (excludeTableList.length()-1))));
			excludeTableList.append(")");
			if(ValidationUtil.isValid(excludeTableList) && !excludeTableList.toString().equalsIgnoreCase(")")) {
				excludeTableListStr = String.valueOf(excludeTableList);
			}
		}
		return excludeTableListStr;
	}
	
	public ExceptionCode formConnectorTables(String macroVar, String dbScript, List<ETLFeedTablesVb> treeVbList) {
		List<List<String>> strdataList = new ArrayList<List<String>>();
		List<String> strtabList = new ArrayList<String>();
		List<String> strconTablList = new ArrayList<String>();
		String excludeTableListStr = formExcludeTableString(treeVbList, "T");
		String excludelViewListStr = formExcludeTableString(treeVbList, "M");

		ExceptionCode exceptionCode = new ExceptionCode();
		exceptionCode = CommonUtils.getConnection(dbScript);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			return exceptionCode;
		}
		String dbLinkName = CommonUtils.getValue(dbScript, "DB_LINK_NAME");
		String dbUserName = CommonUtils.getValue(dbScript, "USER");
		if (!ValidationUtil.isValid(dbUserName))
			dbUserName = CommonUtils.getValue(dbScript, "DB_USER");
		String dbName = "";
		String dbSchema = CommonUtils.getValue(dbScript, "SCHEMA");
		if (!ValidationUtil.isValid(dbSchema)) {
			dbSchema = CommonUtils.getValue(dbScript, "DB_NAME");
		}
		if (!ValidationUtil.isValid(dbName) && !ValidationUtil.isValid(dbSchema)) {
			dbSchema = ValidationUtil.isValid(dbSchema) ? dbSchema : dbUserName;
		}

		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 = CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 = CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 = CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		String parentDbUserName = dbUserName;

		String extractionQueryType = ValidationUtil.isValid(excludeTableListStr) ? "TAB_EXCLUDE_SQL" : "TAB_SQL";
		String extractionQuery = etlFeedMainDao.getQueriesForDatasource(dataBaseType, extractionQueryType);
		if (!ValidationUtil.isValid(extractionQuery)) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in getting extractionQuery from VRD_OBJECT_PROPERTIES - DB_TYPE["
					+ dataBaseType + "] Extraction_Query_Type [" + extractionQueryType + "] ");
			return exceptionCode;
		}
		extractionQuery = extractionQuery.replaceAll("#EXCLUDE_TABLES#", excludeTableListStr);
		extractionQuery = extractionQuery.replaceAll("#PARENT_DB_USER#", parentDbUserName);
		extractionQuery = extractionQuery.replaceAll("#PARENT_DB_SCHEMA#", dbSchema);
		if (ValidationUtil.isValid(dbLinkName)) {
			if ("ORACLE".equalsIgnoreCase(dataBaseType)) {
				extractionQuery = extractionQuery.replaceAll("#DB_LINK_NAME#", "@" + dbLinkName);
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Problem in maintaining DB-Link Name");
				return exceptionCode;
			}
		} else {
			extractionQuery = extractionQuery.replaceAll("#DB_LINK_NAME#", "");
		}

		try (Connection con = (Connection) exceptionCode.getResponse();
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery(extractionQuery)) {
			if (ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if (ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if (ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);

			while (rs.next()) {
				strtabList.add(rs.getString("TABLE_NAME"));
			}
			strconTablList = etlFeedMainDao.selectConnectorQueryList(macroVar, excludelViewListStr);
			strdataList.add(strtabList);
			strdataList.add(strconTablList);
		} catch (SQLException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:" + e.getMessage());
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:" + e.getMessage());
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(strdataList);
		return exceptionCode;
	}
	public ExceptionCode formConnectorFileTableSpecificCols(String macroVar, String dbScript, String tableName, String tableType){
		ExceptionCode exceptionCode = new ExceptionCode();
		List<ETLFeedColumnsVb> arrayListCol = new ArrayList<ETLFeedColumnsVb>();
		if("F".equalsIgnoreCase(tableType)) {
			List<ETLFeedColumnsVb> arrayListQueryColumn= etlFeedMainDao.selectConnectorFileColumns(macroVar,tableName);
			arrayListCol = arrayListQueryColumn;
		}	
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(arrayListCol);
		return exceptionCode;
	}

	public ExceptionCode formConnectorTableSpecificCols(String macroVar, String dbScript, String tableName,
			String tableType) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<ETLFeedColumnsVb> arrayListCol = new ArrayList<ETLFeedColumnsVb>();
		try {
			if ("M".equalsIgnoreCase(tableType)) {
				List<ETLFeedColumnsVb> arrayListQueryColumn = etlFeedMainDao.selectConnectorQueryColumns(macroVar,
						tableName);
				arrayListCol = arrayListQueryColumn;
			} else {
				exceptionCode = CommonUtils.getConnection(dbScript);
				if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					return exceptionCode;
				}
				String dbLinkName = CommonUtils.getValue(dbScript, "DB_LINK_NAME");
				String dbUserName = CommonUtils.getValue(dbScript, "USER");
				if (!ValidationUtil.isValid(dbUserName))
					dbUserName = CommonUtils.getValue(dbScript, "DB_USER");
				String dbName = "";
				String dbSchema = CommonUtils.getValue(dbScript, "SCHEMA");
				if (!ValidationUtil.isValid(dbSchema)) {
					dbSchema = CommonUtils.getValue(dbScript, "DB_NAME");
				}
				if (!ValidationUtil.isValid(dbName) && !ValidationUtil.isValid(dbSchema)) {
					dbSchema = ValidationUtil.isValid(dbSchema) ? dbSchema : dbUserName;
				}
				String parentDbUserName = dbUserName;

				String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
				String dbSetParam1 = CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
				String dbSetParam2 = CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
				String dbSetParam3 = CommonUtils.getValue(dbScript, "DB_SET_PARAM3");

				String extractionQuery = etlFeedMainDao.getQueriesForDatasource(dataBaseType, "COL_SQL");
				if (!ValidationUtil.isValid(extractionQuery)) {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Problem in getting extractionQuery from VRD_OBJECT_PROPERTIES - DB_TYPE["
							+ dataBaseType + "] Extraction_Query_Type [COL_SQL] ");
					return exceptionCode;
				}
				String replacement = Matcher.quoteReplacement(tableName); 
				extractionQuery = extractionQuery.replaceAll("#TABLE_NAME#", replacement);
				extractionQuery = extractionQuery.replaceAll("#PARENT_DB_USER#", parentDbUserName);
				extractionQuery = extractionQuery.replaceAll("#PARENT_DB_SCHEMA#", dbSchema);
				if (ValidationUtil.isValid(dbLinkName)) {
					if ("ORACLE".equalsIgnoreCase(dataBaseType)) {
						extractionQuery = extractionQuery.replaceAll("#DB_LINK_NAME#", "@" + dbLinkName);
					} else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Problem in maintaining DB-Link Name");
						return exceptionCode;
					}
				} else {
					extractionQuery = extractionQuery.replaceAll("#DB_LINK_NAME#", "");
				}
				try (Connection con = (Connection) exceptionCode.getResponse();
						Statement stmt = con.createStatement();
						ResultSet rs = stmt.executeQuery(extractionQuery)) {
					if (ValidationUtil.isValid(dbSetParam1))
						stmt.executeUpdate(dbSetParam1);
					if (ValidationUtil.isValid(dbSetParam2))
						stmt.executeUpdate(dbSetParam2);
					if (ValidationUtil.isValid(dbSetParam3))
						stmt.executeUpdate(dbSetParam3);

					Map<String, String> dataTypeMap = etlFeedMainDao.getDataTypeMap(dataBaseType);
					if (dataTypeMap == null || dataTypeMap.size() == 0) {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Current ALPHA_SUB_TAB-2014/NUM_SUB_TAB-2004 is not maintained");
						return exceptionCode;
					}
					int i = 0;
					if (ValidationUtil.isValid(extractionQuery)) {
						while (rs.next()) {
							i++;
							ETLFeedColumnsVb vobj = new ETLFeedColumnsVb();
							vobj.setSourceConnectorId("");
							vobj.setTableId(tableName);
							vobj.setColumnId("" + i);
							vobj.setColumnName(rs.getString("COLUMN_NAME"));
							vobj.setColumnAliasName(rs.getString("COLUMN_NAME"));
							vobj.setColumnSortOrder(i);
							if (dataTypeMap != null && !dataTypeMap.isEmpty()
									&& ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))) {
								vobj.setColumnDataType(dataTypeMap.get(rs.getString("DATA_TYPE")));
							} else {
								vobj.setColumnDataType("Y");
							}
							vobj.setColumnLength(rs.getString("DATA_LENGTH"));
							if (ValidationUtil.isValid(rs.getString("DATA_TYPE"))
									&& "D".equalsIgnoreCase(rs.getString("DATA_TYPE"))) {
								vobj.setDateFormat("6");
							} else {
								vobj.setDateFormat("");
							}
							vobj.setPrimaryKeyFlag("N");
							vobj.setPartitionColumnFlag("N");
							vobj.setFilterContext("");
							vobj.setColExperssionType("DBCOL");
							vobj.setExperssionText("");
							arrayListCol.add(vobj);
						}
					}
				} catch (SQLException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Problem in gaining connection - Cause:" + e.getMessage());
					return exceptionCode;
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Problem in gaining connection - Cause:" + e.getMessage());
					return exceptionCode;
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in getting tables - Cause:" + e.getMessage());
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(arrayListCol);
		return exceptionCode;
	}

	public ExceptionCode publishRecord(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = null;
		DeepCopy<EtlFeedMainVb> deepCopy = new DeepCopy<EtlFeedMainVb>();
		EtlFeedMainVb clonedObject = null;
		try {
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			exceptionCode = etlFeedMainDao.doPublishForTransaction(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			/*if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				etlFeedMainDao.deleteAllUplData(vObject, "Y");
			}*/
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Publish Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}

	public ExceptionCode saveFeedDataIntoUplRecord(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = null;
		DeepCopy<EtlFeedMainVb> deepCopy = new DeepCopy<EtlFeedMainVb>();
		EtlFeedMainVb clonedObject = null;
		try {
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			doFormateData(vObject);
			exceptionCode = doValidate(vObject);
			if (exceptionCode != null && exceptionCode.getErrorMsg() != "") {
				return exceptionCode;
			}
			/*boolean feedexist = etlFeedMainDao.feedExistenceCount(vObject);
			while (feedexist) {
				vObject.setFeedId(vObject.getCountry() + "" + vObject.getLeBook() + "" + etlFeedMainDao.getRandomNumber());
				feedexist = etlFeedMainDao.feedExistenceCount(vObject);
			}*/
			
			feedID_ExistenceValidationAndUniqueCreation(vObject);
			exceptionCode = etlFeedMainDao.doSaveFeedDataRecord(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Insert Exception " + rex.getCode().getErrorMsg());
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	
	private void feedID_ExistenceValidationAndUniqueCreation(EtlFeedMainVb vObject) {
		boolean isFeedExistFlag = etlFeedMainDao.feedExistenceCount(vObject);
		if(isFeedExistFlag) {
			vObject.setFeedId(vObject.getCountry() + "" + vObject.getLeBook() + "" + etlFeedMainDao.getRandomNumber());
			feedID_ExistenceValidationAndUniqueCreation(vObject);
		}
	}
	
	public ExceptionCode getQuerySourceResults(EtlFeedMainVb vObject) {
		setVerifReqDeleteType(vObject);
		etlFeedSourceDao.setServiceDefaults();
		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}

		EtlFeedSourceVb sourceVb = new EtlFeedSourceVb();
		sourceVb.setCountry(vObject.getCountry());
		sourceVb.setLeBook(vObject.getLeBook());
		sourceVb.setFeedId(vObject.getFeedId());
		sourceVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlFeedSourceVb> collTemp = etlFeedSourceDao.getQueryResultsByParent(vObject, approveOrPend);
		vObject.setEtlFeedSourceList(collTemp);

		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}

	public ExceptionCode getQuerySourceTableResults(EtlFeedMainVb vObject) {
		setVerifReqDeleteType(vObject);
		etlFeedTablesDao.setServiceDefaults();

		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}

		ETLFeedTablesVb tablesVb = new ETLFeedTablesVb();
		tablesVb.setCountry(vObject.getCountry());
		tablesVb.setLeBook(vObject.getLeBook());
		tablesVb.setFeedId(vObject.getFeedId());
		tablesVb.setSourceConnectorId(vObject.getConnectorId());
		tablesVb.setVerificationRequired(vObject.isVerificationRequired());
		List<ETLFeedTablesVb> collTemp = etlFeedTablesDao.getQueryResultsByParent(vObject, approveOrPend);
		vObject.setEtlFeedTablesList(collTemp);

		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
	public ExceptionCode getQuerySourceTableColResults(EtlFeedMainVb vObject) {
		setVerifReqDeleteType(vObject);
		etlFeedColumnsDao.setServiceDefaults();
		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}

		ETLFeedColumnsVb columnsVb = new ETLFeedColumnsVb();
		columnsVb.setCountry(vObject.getCountry());
		columnsVb.setLeBook(vObject.getLeBook());
		columnsVb.setFeedId(vObject.getFeedId());
		columnsVb.setSourceConnectorId(vObject.getConnectorId());
		columnsVb.setTableId(vObject.getTableId());
		columnsVb.setVerificationRequired(vObject.isVerificationRequired());
		List<ETLFeedColumnsVb> collTemp = etlFeedColumnsDao.getQueryResultsByParent(vObject, approveOrPend);
		vObject.setEtlFeedColumnsList(collTemp);

		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}

	public ExceptionCode getQuerySourceRelationResults(EtlFeedMainVb vObject) {
		setVerifReqDeleteType(vObject);
		etlFeedRelationDao.setServiceDefaults();

		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}
		EtlFeedRelationVb relationVb = new EtlFeedRelationVb();
		relationVb.setCountry(vObject.getCountry());
		relationVb.setLeBook(vObject.getLeBook());
		relationVb.setFeedId(vObject.getFeedId());
		relationVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlFeedRelationVb> collTemp = etlFeedRelationDao.getQueryResultsByParent(vObject, approveOrPend);
		vObject.setEtlFeedRelationList(collTemp);

		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}

	public ExceptionCode getQueryMaterializedViewSetupResults(EtlFeedMainVb vObject){
		setVerifReqDeleteType(vObject);
		etlExtractionSummaryFieldsDao.setServiceDefaults();
		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}

		EtlExtractionSummaryFieldsVb summaryFieldsVb = new EtlExtractionSummaryFieldsVb();
		summaryFieldsVb.setCountry(vObject.getCountry());
		summaryFieldsVb.setLeBook(vObject.getLeBook());
		summaryFieldsVb.setFeedId(vObject.getFeedId());
		summaryFieldsVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlExtractionSummaryFieldsVb> collTemp = etlExtractionSummaryFieldsDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlExtractionSummaryFieldsList(collTemp);		
		
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
	// get  Transform Setup Data
	public ExceptionCode getQueryTransformSetupResults(EtlFeedMainVb vObject){
		ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
		try {
			setVerifReqDeleteType(vObject);
			etlFeedMainDao.setServiceDefaults();
			int approveOrPend = 0;
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				approveOrPend = 9999;
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				approveOrPend = 1;
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				approveOrPend = 0;
			}

			// get Etl Feed Transformation
			EtlFeedTransformationVb vobj = new EtlFeedTransformationVb();
			vobj.setCountry(vObject.getCountry());
			vobj.setLeBook(vObject.getLeBook());
			vobj.setFeedId(vObject.getFeedId());
			vobj.setVerificationRequired(vObject.isVerificationRequired());
			List<EtlFeedTransformationVb> etlFeedTransformationList = etlFeedTransformationDao
					.getQueryResultsByParent(vObject, approveOrPend);
			vObject.setEtlFeedTransformationList(etlFeedTransformationList);

			EtlFeedTranVb tranVb = new EtlFeedTranVb();
			tranVb.setCountry(vObject.getCountry());
			tranVb.setLeBook(vObject.getLeBook());
			tranVb.setFeedId(vObject.getFeedId());
			tranVb.setTransformationNodeInputList(new ArrayList<EtlFeedTranNodeVb>());

			if (vObject.getEtlFeedTransformationList() != null) {
				for (EtlFeedTransformationVb transformationVb : vObject.getEtlFeedTransformationList()) {

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
					columnNodeVb.setNodeId(vObject.getNodeId());

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

					tranVb.getTransformationNodeInputList().add(nodeVb);
				}
			}
			exceptionCode.setResponse(tranVb);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch(Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}
		// get  Target Data Load
	public ExceptionCode getQueryTargetLoadResults(EtlFeedMainVb vObject){
		setVerifReqDeleteType(vObject);
		etlFeedMainDao.setServiceDefaults();

		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}

		// get Etl Transformed Columns data
		EtlTransformedColumnsVb transformedColumnsVb = new EtlTransformedColumnsVb();
		transformedColumnsVb.setCountry(vObject.getCountry());
		transformedColumnsVb.setLeBook(vObject.getLeBook());
		transformedColumnsVb.setFeedId(vObject.getFeedId());
		transformedColumnsVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlTransformedColumnsVb> etlTransformedColumnsList  = etlTransformedColumnsDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlTransformedColumnsList(etlTransformedColumnsList);		
					
		//get Etl Feed Destination Connector
		EtlFeedDestinationVb destinationVb = new EtlFeedDestinationVb();
		destinationVb.setCountry(vObject.getCountry());
		destinationVb.setLeBook(vObject.getLeBook());
		destinationVb.setFeedId(vObject.getFeedId());
		destinationVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlFeedDestinationVb> etlFeedDestinationList = etlFeedDestinationDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlFeedDestinationList(etlFeedDestinationList);
		
		// get Etl Target Columns data
		EtlTargetColumnsVb targetColumnsVb = new EtlTargetColumnsVb();
		targetColumnsVb.setCountry(vObject.getCountry());
		targetColumnsVb.setLeBook(vObject.getLeBook());
		targetColumnsVb.setFeedId(vObject.getFeedId());
		targetColumnsVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlTargetColumnsVb> etlTargetColumnsList  = etlTargetColumnsDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlTargetColumnsList(etlTargetColumnsList);		
		
		// get Etl Feed Load Mapping Columns
		EtlFeedLoadMappingVb feedLoadMappingVb = new EtlFeedLoadMappingVb();
		feedLoadMappingVb.setCountry(vObject.getCountry());
		feedLoadMappingVb.setLeBook(vObject.getLeBook());
		feedLoadMappingVb.setFeedId(vObject.getFeedId());
		feedLoadMappingVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = etlFeedLoadMappingDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlFeedLoadMappingList(etlFeedLoadMappingList);
		
		// get Etl Feed Injection Config 
		EtlFeedInjectionConfigVb feedInjectionConfigVb = new EtlFeedInjectionConfigVb();
		feedInjectionConfigVb.setCountry(vObject.getCountry());
		feedInjectionConfigVb.setLeBook(vObject.getLeBook());
		feedInjectionConfigVb.setFeedId(vObject.getFeedId());
		feedInjectionConfigVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = etlFeedInjectionConfigDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlFeedInjectionConfigList(etlFeedInjectionConfigList);
		
		ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}
	

	// get ETL Feed Schedule Configuration
	public ExceptionCode getQueryScheduleResults(EtlFeedMainVb vObject){
		setVerifReqDeleteType(vObject);
		etlFeedMainDao.setServiceDefaults();
		
		int approveOrPend=0;
		if(vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		}else if(vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		}else if(vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}

		//get ETL Feed Schedule Configuration
		EtlFeedScheduleConfigVb scheduleConfigVb = new EtlFeedScheduleConfigVb();
		scheduleConfigVb.setCountry(vObject.getCountry());
		scheduleConfigVb.setLeBook(vObject.getLeBook());
		scheduleConfigVb.setFeedId(vObject.getFeedId());
		scheduleConfigVb.setVerificationRequired(vObject.isVerificationRequired());
		List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = etlFeedScheduleConfigDao.getQueryResultsByParent(vObject,approveOrPend);
		vObject.setEtlFeedScheduleConfigList(etlFeedScheduleConfigList);
		
		ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}
	
	public ExceptionCode insertApprPendRecord(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = null;
		DeepCopy<EtlFeedMainVb> deepCopy = new DeepCopy<EtlFeedMainVb>();
		EtlFeedMainVb clonedObject = null;
		try {
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			doFormateData(vObject);
			exceptionCode = doValidate(vObject);
			if (exceptionCode != null && exceptionCode.getErrorMsg() != "") {
				return exceptionCode;
			}
			if (!vObject.isVerificationRequired()) {
				exceptionCode = getScreenDao().doInsertApprRecord(vObject);
			} else {
				exceptionCode = getScreenDao().doInsertRecord(vObject);
			}
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Insert Exception " + rex.getCode().getErrorMsg());
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}

	@Override
	protected List<ReviewResultVb> transformToReviewResults(List<EtlFeedMainVb> approvedCollection, List<EtlFeedMainVb> pendingCollection) {
		ArrayList collTemp = getPageLoadValues();
		ResourceBundle rsb = CommonUtils.getResourceManger();
		if(pendingCollection != null)
			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));
		if(approvedCollection != null)
			getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));
		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();

		ReviewResultVb lCountry = new ReviewResultVb(rsb.getString("country"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getCountryDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getCountryDesc(),
				(!pendingCollection.get(0).getCountry().equals(approvedCollection.get(0).getCountry())));
		lResult.add(lCountry);

		ReviewResultVb lLeBook = new ReviewResultVb(rsb.getString("leBook"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getLeBookDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLeBookDesc(),
				(!pendingCollection.get(0).getLeBook().equals(approvedCollection.get(0).getLeBook())));
		lResult.add(lLeBook);

		ReviewResultVb lFeedId = new ReviewResultVb(rsb.getString("feedId"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getFeedId(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getFeedId());
		lResult.add(lFeedId);

		ReviewResultVb lFeedName = new ReviewResultVb(rsb.getString("feedName"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getFeedName(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getFeedName());
		lResult.add(lFeedName);

		ReviewResultVb lFeedDescription = new ReviewResultVb(rsb.getString("feedDescription"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getFeedDescription(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getFeedDescription());
		lResult.add(lFeedDescription);

		ReviewResultVb lEffectiveDate = new ReviewResultVb(rsb.getString("effectiveDate"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getEffectiveDate(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getEffectiveDate());
		lResult.add(lEffectiveDate);

		ReviewResultVb lLastExtractionDate = new ReviewResultVb(rsb.getString("lastExtractionDate"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getLastExtractionDate(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getLastExtractionDate());
		lResult.add(lLastExtractionDate);

		ReviewResultVb lFeedType = new ReviewResultVb(rsb.getString("feedType"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getFeedTypeDesc(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getFeedTypeDesc());
		lResult.add(lFeedType);

		ReviewResultVb lFeedCategory = new ReviewResultVb(rsb.getString("feedCategory"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getFeedCategoryDesc(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getFeedCategoryDesc());
		lResult.add(lFeedCategory);

		ReviewResultVb lAlertMechanism = new ReviewResultVb(rsb.getString("alertMechanism"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getAlertMechanism(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getAlertMechanism());
		lResult.add(lAlertMechanism);

		ReviewResultVb lPrivilageMechanism = new ReviewResultVb(rsb.getString("privilageMechanism"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getPrivilageMechanism(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getPrivilageMechanism());
		lResult.add(lPrivilageMechanism);

		ReviewResultVb lFeedStatus = new ReviewResultVb(rsb.getString("feedStatus"),
			(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getStatusDesc(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getStatusDesc());
		lResult.add(lFeedStatus);

		ReviewResultVb lRecordIndicator = new ReviewResultVb(rsb.getString("recordIndicator"),
				(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getRecordIndicatorDesc(),
				(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getRecordIndicatorDesc());
			lResult.add(lRecordIndicator);
		ReviewResultVb lMaker = new ReviewResultVb(rsb.getString("maker"),(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getMaker() == 0?"":pendingCollection.get(0).getMakerName(),
				(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getMaker() == 0?"":approvedCollection.get(0).getMakerName());
		lResult.add(lMaker);
		ReviewResultVb lVerifier = new ReviewResultVb(rsb.getString("verifier"),(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getVerifier() == 0?"":pendingCollection.get(0).getVerifierName(),
				(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getVerifier() == 0?"":approvedCollection.get(0).getVerifierName());
		lResult.add(lVerifier);
		ReviewResultVb lDateLastModified = new ReviewResultVb(rsb.getString("dateLastModified"),(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getDateLastModified(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getDateLastModified());
		lResult.add(lDateLastModified);
		ReviewResultVb lDateCreation = new ReviewResultVb(rsb.getString("dateCreation"),(pendingCollection == null || pendingCollection.isEmpty())?"":pendingCollection.get(0).getDateCreation(),
			(approvedCollection == null || approvedCollection.isEmpty())?"":approvedCollection.get(0).getDateCreation());
			lResult.add(lDateCreation);
			return lResult; 
			

}
	@Override
	protected AbstractDao<EtlFeedMainVb> getScreenDao() {
		return etlFeedMainDao;
	}
	@Override
	protected void setAtNtValues(EtlFeedMainVb vObject) {
		vObject.setFeedStatusNt(2000);
		vObject.setRecordIndicatorNt(7);
		vObject.setFeedTypeAt(2102);
	}
	
	@Override
	protected void setVerifReqDeleteType(EtlFeedMainVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_FEED_MAIN");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}

	public EtlFeedMainDao getEtlFeedMainDao() {
		return etlFeedMainDao;
	}

	public void setEtlFeedMainDao(EtlFeedMainDao etlFeedMainDao) {
		this.etlFeedMainDao = etlFeedMainDao;
	}

	public ExceptionCode getQueryFeedDetailResults(EtlFeedMainVb vObject){
		setVerifReqDeleteType(vObject);
		List<EtlFeedMainVb> collTemp = etlFeedMainDao.getQueryDetailResults(vObject);

		if(collTemp.size() == 0){
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}else{
			doSetDesctiptionsAfterQuery(collTemp);
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
	public String getDbScript(String getMacroVar) throws DataAccessException {
		try {
			return etlFeedMainDao.getScriptValue(getMacroVar);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e.getMessage());
		}
	}

	public DSConnectorVb getSpecificConnector(DSConnectorVb vObj) {
		try {
			return etlFeedMainDao.getSpecificConnectorDetails(vObj);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();

			}
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public Integer returnBaseTableId(String country, String leBook, String feedId) {
		try {
			Integer returnBaseTableId = etlFeedMainDao.returnBaseTableId(country,leBook, feedId);
			return returnBaseTableId;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	@SuppressWarnings("rawtypes")
	public ExceptionCode returnJoinCondition(EtlForQueryReportVb vObj, List<String> uniqueTableIdArray, Integer joinType, ArrayList getDataAL) {
		ExceptionCode exceptionCode = new ExceptionCode();
		etlFeedMainDao.setServiceDefaults();
		try {
			List<Integer> intTblIdList = uniqueTableIdArray.stream().mapToInt(e -> Integer.parseInt(e)).boxed().collect(Collectors.toList());
			Collections.sort(intTblIdList);
			uniqueTableIdArray = intTblIdList.stream().map(v -> String.valueOf(v)).collect(Collectors.toList());
			exceptionCode.setResponse(DynamicJoinNew.formDynamicJoinString("", joinType, uniqueTableIdArray, getDataAL, vObj.getBaseTableId()));
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			exceptionCode = CommonUtils.getResultObject(etlFeedMainDao.getServiceName(), Constants.WE_HAVE_ERROR_DESCRIPTION, "", "Exception in getting dynamic join string. Cause ["+e.getMessage()+"]");
			return exceptionCode;
		}
	}
	
	public ExceptionCode createFeedMain(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String tableHeader[] = vObject.getColumnHeaders().split("!@#");
			StringBuffer tableCreationScript = new StringBuffer();
			String tableName = "SAMPLE_ETL";
			tableCreationScript.append("CREATE TABLE "+tableName+"(");
			for(String str : tableHeader) {
				tableCreationScript.append(str +" VARCHAR2(50),");
			}
			String tableCrete = tableCreationScript.toString();
			tableCrete = tableCrete.substring(0, tableCrete.length() - 1);
			tableCrete= tableCrete+")";
			etlFeedMainDao.createorInsertTable("DROP TABLE "+tableName+"");
			int retVal = etlFeedMainDao.createorInsertTable(tableCrete);
			String insertInto = "";
			for(String str:vObject.getRowData()) {
				insertInto = "INSERT INTO "+tableName+" VALUES(";
				str = str.replaceAll("!@#", ",");
				insertInto = insertInto+str;
				insertInto=insertInto+")";
				retVal = etlFeedMainDao.createorInsertTable(tableCrete);
			}
		}catch (Exception e) {
		}
		return exceptionCode;
	}
	public String generateDynamicDate(String dynamicDate, Integer dynamicOperator,String dateFormat,String javaFormat) {
		SimpleDateFormat df = new SimpleDateFormat(javaFormat);
		Calendar cal = Calendar.getInstance();

		switch (dynamicDate.toLowerCase()) {
		case ("d"): {
			cal.add(Calendar.DATE, dynamicOperator);
			break;
		}
		case ("m"): {
			cal.add(Calendar.MONTH, dynamicOperator);
			break;
		}
		case ("y"): {
			cal.add(Calendar.YEAR, dynamicOperator);
			break;
		  }
		}
		return df.format(cal.getTime());
	}
	/*public List<EtlFeedMainVb> getCategoryListing(EtlFeedMainVb vObject) {
		List<EtlFeedMainVb> dependencylst = null;
		try {
			List<EtlFeedMainVb> categorylst = (List<EtlFeedMainVb>) etlFeedMainDao.getCategoryListing(vObject);
			for(EtlFeedMainVb reportVb : categorylst) {
				List<EtlFeedMainVb> feedCatlst = (List<EtlFeedMainVb>) etlFeedMainDao.findDependencyFeed(reportVb.getFeedCategory(), vObject.getFeedId());
				reportVb.setCategoryFeedlst(feedCatlst);
			}
			dependencylst = categorylst;
			//dependencylst= etlFeedMainDao.getCategoryListing(vObject);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return dependencylst;
	}*/
	
	public List<EtlFeedMainVb> getCategoryListing(EtlFeedMainVb vObject) {
		List<EtlFeedMainVb> dependencylst = null;
		try {
			List<EtlFeedMainVb> categorylst = (List<EtlFeedMainVb>) etlFeedMainDao.getCategoryListing(vObject);
			Iterator<EtlFeedMainVb> iterator = categorylst.iterator();
			while (iterator.hasNext()) {
				EtlFeedMainVb reportVb = iterator.next();
				List<EtlFeedMainVb> feedCatlst = (List<EtlFeedMainVb>) etlFeedMainDao.findDependencyFeed(reportVb.getFeedCategory(), vObject.getFeedId());
				if (feedCatlst == null || feedCatlst.isEmpty()) {
					iterator.remove();
				} else {
					reportVb.setCategoryFeedlst(feedCatlst);
				}
			}
			dependencylst = categorylst;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return dependencylst;
	}

	public List<EtlFeedMainVb> findDependencyFeed(EtlFeedMainVb vObject) {
		List<EtlFeedMainVb> dependencylst = null;
		try {
			dependencylst = etlFeedMainDao.findDependencyFeed(vObject.getFeedCategory(), vObject.getFeedId());
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return dependencylst;
	}
	public ExceptionCode modifyRecord(EtlFeedMainVb vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<EtlFeedMainVb> deepCopy = new DeepCopy<EtlFeedMainVb>();
		EtlFeedMainVb clonedObject = null;
		try {
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			if (vObject.isVerificationRequired()) {
				exceptionCode = etlFeedMainDao.doUpdateRecordPend(vObject);
			} else {
				exceptionCode = etlFeedMainDao.doUpdateRecordAppr(vObject);
			}
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Modify Exception " + rex.getCode().getErrorMsg());
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
				rex.printStackTrace();
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}

	public ExceptionCode deleteRecord(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = null;
		DeepCopy<EtlFeedMainVb> deepCopy = new DeepCopy<EtlFeedMainVb>();
		EtlFeedMainVb clonedObject = null;
		try {
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			if (vObject.isVerificationRequired()) {
				exceptionCode = etlFeedMainDao.doDeleteRecordPend(vObject);
			} else {
				exceptionCode = etlFeedMainDao.doDeleteRecordAppr(vObject);
			}
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Modify Exception " + rex.getCode().getErrorMsg());
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	public ExceptionCode getTransformationMasterDetails() {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Map<String, List<EtlFeedTransformationMasterVb>> transDetailMap = etlFeedMainDao.getTransformationMasterDetails();
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(transDetailMap);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode saveTransformationSession(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String sessionId = String.valueOf(System.currentTimeMillis());
		try {
			vObject.setSessionId(sessionId);
			exceptionCode = etlFeedMainDao.doInsertTransformationMasterRecord(vObject);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = visionUploadWb.loadTransformationDataView(vObject);
			}
		} catch (RuntimeCustomException rex) {
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(sessionId);
			return exceptionCode;
		} catch (Exception e) {
			exceptionCode.setOtherInfo(vObject.getSessionId());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
		exceptionCode.setOtherInfo(sessionId);
		return exceptionCode;
	}
	
	public ExceptionCode retrieveTransformationDataView(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = visionUploadWb.retrieveTransformationDataView(vObject);
		} catch (RuntimeCustomException rex) {
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(vObject.getSessionId());
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setOtherInfo(vObject.getSessionId());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
		return exceptionCode;
	}

	public ExceptionCode getSampleDataRuleDetails(EtlFeedMainVb etlFeedMainVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<EtlExtractionSummaryFieldsVb> materialisedColumnList = etlExtractionSummaryFieldsDao.getSampleDataRuleDetails(etlFeedMainVb);
			exceptionCode.setResponse(materialisedColumnList);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setOtherInfo(etlFeedMainVb);
		return exceptionCode;
	}

	public ExceptionCode getSampleTransformedDataByReadingOP_File(EtlFeedMainVb vObject, String nodeId) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			//update the DATE_LAST_MODIFIED column in the session table
			exceptionCode = etlFeedMainDao.doUpdateSessionForTransformation(vObject);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap = vObject.getEtlFeedNodeList().stream().collect(Collectors.toMap(vb -> vb.getNodeId(), vb -> vb));
				EtlFeedTranNodeVb selectedVb = nodeDetailByNodeIDMap.get(nodeId);
				if("read".equalsIgnoreCase(nodeId) || "N".equalsIgnoreCase(selectedVb.getNodeChange())) {
					exceptionCode = readDataFileIfAvailable(vObject.getFeedId(), vObject.getSessionId(), selectedVb);
					if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = processNodeDetailsToGetTransOutput(vObject, selectedVb, nodeDetailByNodeIDMap);
					}
				} else {
					exceptionCode = processNodeDetailsToGetTransOutput(vObject, selectedVb, nodeDetailByNodeIDMap);
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
	
	private ExceptionCode formQueryExecutable(List<EtlFeedTranNodeVb> exeOrderList) {
		ExceptionCode exceptionCode = new ExceptionCode();
		boolean dualFlag = false;
		for (EtlFeedTranNodeVb nodeVb : exeOrderList) {
			StringBuffer executableQuery = new StringBuffer();
			StringBuffer fromQuery = new StringBuffer();
			if ("T_STR_082".equalsIgnoreCase(nodeVb.getTransformationId())) {
				executableQuery.append(nodeVb.getTransformationSql());
			} else if (nodeVb.getParentTransformationList() != null && nodeVb.getParentTransformationList().size() > 0) {
				String firstParentViewName = "";
				// form FROM clause by joining all the parent View_Name
				for (EtlFeedTranNodeVb pVb : nodeVb.getParentTransformationList()) {
					EtlFeedTranNodeVb parentNodeVb = exeOrderList.stream()
							.filter(v -> (v.getNodeId()).equalsIgnoreCase(pVb.getNodeId())).findFirst().map(v -> v)
							.orElse(null);
					if (parentNodeVb != null) {
						String currentParentViewName = parentNodeVb.getViewName();
						if (!ValidationUtil.isValid(firstParentViewName)) {
							firstParentViewName = parentNodeVb.getViewName();
							fromQuery.append(TICK + currentParentViewName + TICK + SPACE + TICK + currentParentViewName + TICK);
						} else {
							fromQuery.append(INNER_JOIN + TICK + currentParentViewName + TICK + SPACE + TICK + currentParentViewName + TICK + ON);
							fromQuery.append(OPEN_BRACKET);
							fromQuery.append(TICK + firstParentViewName + TICK + ROWNUM + EQUAL + TICK + currentParentViewName + TICK + ROWNUM);
							fromQuery.append(CLOSE_BRACKET);
						}
					}
				}
				String selectQuery = "";
				if ("P".equalsIgnoreCase(nodeVb.getTransformationType())) {
					selectQuery = "Select " + TICK + firstParentViewName + TICK + ROWNUM + ", " + nodeVb.getTransformationSql();
				} else {
					selectQuery = "Select " + nodeVb.getTransformationSql();
				}
				executableQuery.append(selectQuery + " FROM " + fromQuery + SPACE + nodeVb.getTransSqlAppend());
			} else {
				dualFlag = true;
				executableQuery.append("Select " + nodeVb.getTransformationSql() + " FROM DUAL");
			}
			nodeVb.setTransSqlExecutable(String.valueOf(executableQuery));
		}
		
		//Add TICK symbol to viewName to avoid problem with special-characters in the viewName
		/*for (EtlFeedTranNodeVb nodeVb : exeOrderList) {
			nodeVb.setViewName(TICK+nodeVb.getViewName()+TICK);
		}*/
		exceptionCode.setResponse(exeOrderList);
		exceptionCode.setOtherInfo(dualFlag);
		return exceptionCode;
	}

	private List<EtlFeedTranNodeVb> invokeMethodBasedOnTransformationId(List<EtlFeedTranNodeVb> exeOrderList,
			String feedId, String sessionId) throws NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		TransformationFunctionsUtil classObj = new TransformationFunctionsUtil();
		for (EtlFeedTranNodeVb nodeVb : exeOrderList) {
			ArrayList<EtlFeedTranColumnVb> inputLst = new ArrayList<>();
			ArrayList<EtlFeedTranColumnVb> outputLst = new ArrayList<>();
			for (EtlFeedTranColumnVb vb : nodeVb.getTranColumnList()) {
				vb.setFeedId(feedId);
				vb.setSessionId(sessionId);
				if ("Y".equalsIgnoreCase(vb.getInputFlag())) {
					inputLst.add(vb);
				} else if ("Y".equalsIgnoreCase(vb.getOutputFlag()) && "N".equalsIgnoreCase(vb.getInputFlag())) {
					outputLst.add(vb);
				}
			}
			Map<String, List<EtlFeedTranColumnVb>> ioMap = new HashMap<String, List<EtlFeedTranColumnVb>>();
			String methodOutput = "";
			Method mth;
			/* For Filter transformation, check if the custom flag is set */
			boolean isFilterCustomFlag = false;
			if ("T_STR_073".equalsIgnoreCase(nodeVb.getTransformationId())
					&& "Y".equalsIgnoreCase(nodeVb.getCustomFlag())) {
				isFilterCustomFlag = true;
				nodeVb.setFilterCustomFlag(isFilterCustomFlag);
			}
			
			try {
				if (!"read".equalsIgnoreCase(nodeVb.getNodeId())) {
					String transType = etlFeedMainDao.getTransTypeForTransId(nodeVb.getTransformationId());
					String methodName = etlFeedMainDao.getMethodNameForTransId(nodeVb.getTransformationId());
					nodeVb.setMethodName(methodName);
					nodeVb.setTransformationType(transType);
					ioMap.put("input", inputLst);
					ioMap.put("output", outputLst);

					if ("T_STR_081".equalsIgnoreCase(nodeVb.getTransformationId())) {
						/* T_STR_081 - Aggregation transformation */
						mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class });
						String[] methodOutForAgg = (String[]) mth.invoke(classObj, ioMap);
						nodeVb.setTransformationSql(methodOutForAgg[0]);
						nodeVb.setTransSqlAppend(methodOutForAgg[1]);
					} else if ("T_STR_073".equalsIgnoreCase(nodeVb.getTransformationId())) {
						/* T_STR_073 - Filter transformation */
						mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(),
								new Class[] { Map.class, boolean.class });
						String[] methodOutForAgg = (String[]) mth.invoke(classObj, ioMap, isFilterCustomFlag);
						nodeVb.setTransformationSql(methodOutForAgg[0]);
						if (isFilterCustomFlag == false) {
							nodeVb.setTransSqlAppend(methodOutForAgg[1]);
						} else {
							nodeVb.setTransSqlAppend(" WHERE " + nodeVb.getCustomExpression());
						}
					} else if ("T_STR_082".equalsIgnoreCase(nodeVb.getTransformationId())) {
						/* T_STR_082 - Join transformation */
						mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(),
								new Class[] { Map.class, String.class });
						methodOutput = (String) mth.invoke(classObj, ioMap, nodeVb.getJoinType());
						nodeVb.setTransformationSql(methodOutput);
					} else {
						mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class });
						methodOutput = (String) mth.invoke(classObj, ioMap);
						nodeVb.setTransformationSql(methodOutput);
					}
				}
			} catch (Exception e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
				throw new RuntimeCustomException(String.format(
						"Problem with transformation maintenance. Feed_ID [%s] Session_ID [%s] Node_ID [%s] Node_Name [%s] Transformation_ID [%s] CAUSE [%s]",
						feedId, sessionId, nodeVb.getNodeId(), nodeVb.getNodeName(), nodeVb.getTransformationId(),
						e.getMessage()));

			}
			nodeVb.setViewName(nodeVb.getNodeId());
		}
		
		return exeOrderList;
	}

	private String formJsonString(Map<String, Object> exeOrderMap) {
		String jsonReponse = "{";
		Gson objGson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
		if (exeOrderMap.size() > 0) {
			for (Map.Entry<String, Object> entry : exeOrderMap.entrySet()) {
				String jsonRequestString = "\"" + entry.getKey() + "\" : " + objGson.toJson(entry.getValue());
				if (ValidationUtil.isValid(jsonReponse) && !"{".equalsIgnoreCase(jsonReponse)) {
					jsonReponse = jsonReponse + "," + jsonRequestString;
				} else {
					jsonReponse = jsonReponse + jsonRequestString;
				}
			}
			jsonReponse = jsonReponse + "}";
		}
		return jsonReponse;
	}

	private List<EtlFeedTranNodeVb> triggerRecursiveMthToCalculateExecutionOrderForTransList(String nodeId, Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap, EtlFeedMainVb vObject) {
		Map<String, Integer> exeOrderMap = new HashMap<String, Integer>();
		List<EtlFeedTranNodeVb> exeOrderList = new ArrayList<EtlFeedTranNodeVb>();
		exeOrderMap.put(nodeId, 0);
		
		EtlFeedTranNodeVb selectedNodeVb = nodeDetailByNodeIDMap.get(nodeId);
		String folderName = transSampleOperationDir+"/"+vObject.getFeedId()+"_"+vObject.getSessionId();
		logger.error(folderName);
		Session session = null;
		Channel channelSFTP = null;
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				// Create Session
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();

				// Get sftp channel
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channelSFTP;
				
				calExecutionOrder(selectedNodeVb, exeOrderMap, 1, nodeDetailByNodeIDMap, sftpChannel, folderName);
				
				sftpChannel.exit();
				do {
					Thread.sleep(1000);
				} while (!channelSFTP.isEOF());
			} else {
				calExecutionOrder(selectedNodeVb, exeOrderMap, 1, nodeDetailByNodeIDMap, null, folderName);
			}
		} catch (Exception e) {
			throw new RuntimeCustomException(e);
		} finally {
			try {
				if (channelSFTP != null)
					channelSFTP.disconnect();
			} catch (Exception e2) {
			}
			try {
				if (session != null)
					session.disconnect();
			} catch (Exception e2) {
			}
		}
		
		for (Map.Entry<String, Integer> entry : exeOrderMap.entrySet()) {
			String currentNodeId = entry.getKey();
			int currentNodeExeOrder = entry.getValue();
			EtlFeedTranNodeVb currentNodeVb = nodeDetailByNodeIDMap.get(currentNodeId);
			currentNodeVb.setExecutionOrder(currentNodeExeOrder);
			exeOrderList.add(currentNodeVb);
		}
		Collections.sort(exeOrderList);
		return exeOrderList;
	}
	
	private void calExecutionOrder(EtlFeedTranNodeVb nodeVb, Map<String, Integer> exeOrderMap, int currentLevel,
			Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap, ChannelSftp sftpChannel, String folderName) throws SftpException {
		
		boolean isParentAvailable = (nodeVb.getParentTransformationList() != null && !nodeVb.getParentTransformationList().isEmpty());
		
		boolean isNodeFileAvailable = false;
				
		//if(securedFtp && sftpChannel != null) {
		if ("Y".equalsIgnoreCase(securedFtp) && sftpChannel != null ) {
			isNodeFileAvailable = "N".equalsIgnoreCase(nodeVb.getNodeChange()) && CommonUtils.isFileExist(sftpChannel, folderName + "/" +nodeVb.getNodeId()+".csv"); 
		} else {
			isNodeFileAvailable = "N".equalsIgnoreCase(nodeVb.getNodeChange()) && CommonUtils.isFileExist(folderName + "/" +nodeVb.getNodeId()+".csv");
		}
		
		if (isParentAvailable && !isNodeFileAvailable) {
			for (EtlFeedTranNodeVb parentVb : nodeVb.getParentTransformationList()) {
				EtlFeedTranNodeVb pNodeVb = nodeDetailByNodeIDMap.get(parentVb.getNodeId());
				if (exeOrderMap.get(pNodeVb.getNodeId()) != null) {
					int existingLevel = exeOrderMap.get(pNodeVb.getNodeId());
					if (existingLevel < currentLevel) {
						exeOrderMap.put(pNodeVb.getNodeId(), currentLevel);
					}
				} else {
					exeOrderMap.put(pNodeVb.getNodeId(), currentLevel);
				}
				calExecutionOrder(pNodeVb, exeOrderMap, currentLevel + 1, nodeDetailByNodeIDMap, sftpChannel, folderName);
			}
		}
	}
	
	private ExceptionCode readDataFileIfAvailable(String feedId, String sessionId, EtlFeedTranNodeVb selectedNodeVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Session session = null;
		Channel channelSFTP = null;
		String folderName = feedId + "_" + sessionId;
		logger.error(folderName);
		boolean isNodeFileAvailable = false;
		InputStream inputStream = null;
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				// Create Session
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();

				// Get sftp channel
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channelSFTP;
				isNodeFileAvailable = CommonUtils.isFileExist(sftpChannel, transSampleOperationDir + "/" + folderName + "/" + selectedNodeVb.getNodeId() + ".csv");
			
				if("read".equalsIgnoreCase(selectedNodeVb.getNodeId()) || isNodeFileAvailable) {
					inputStream= sftpChannel.get(transSampleOperationDir + "/" + folderName + "/" +selectedNodeVb.getNodeId()+".csv");
					byte[] bytes = IOUtils.toByteArray(inputStream);
					exceptionCode.setResponse(bytes);
					exceptionCode.setOtherInfo(selectedNodeVb.getNodeId()+".csv");
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					return exceptionCode;
				}
				sftpChannel.exit();
				do {
					Thread.sleep(1000);
				} while (!channelSFTP.isEOF());
			} else {
				isNodeFileAvailable = CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + selectedNodeVb.getNodeId() + ".csv");
				logger.error("File Exist : "+isNodeFileAvailable);
				logger.error("Node Id : "+selectedNodeVb.getNodeId());
				if ("read".equalsIgnoreCase(selectedNodeVb.getNodeId()) || isNodeFileAvailable) {
					try {
						inputStream = new FileInputStream(transSampleOperationDir + "/" + folderName + "/" + selectedNodeVb.getNodeId() + ".csv");
						byte[] bytes = IOUtils.toByteArray(inputStream);
						exceptionCode.setResponse(bytes);
						exceptionCode.setOtherInfo(selectedNodeVb.getNodeId() + ".csv");
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					return exceptionCode;
				}
			}
		} catch (Exception e) {
			throw new RuntimeCustomException(e);
		} finally {
			try {
				if (channelSFTP != null)
					channelSFTP.disconnect();
			} catch (Exception e2) {
			}
			try {
				if (session != null)
					session.disconnect();
			} catch (Exception e2) {
			}
		}
		return exceptionCode;
	}
	
	
	private ExceptionCode processNodeDetailsToGetTransOutput(EtlFeedMainVb vObject, EtlFeedTranNodeVb selectedNodeVb, Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<EtlFeedTranNodeVb> exeOrderList  = triggerRecursiveMthToCalculateExecutionOrderForTransList(selectedNodeVb.getNodeId(), nodeDetailByNodeIDMap, vObject);
		Map<String, Object> returnMapFirInputJson  = new HashMap<>();
		exeOrderList = invokeMethodBasedOnTransformationId(exeOrderList, vObject.getFeedId(), vObject.getSessionId());
		exceptionCode = formQueryExecutable(exeOrderList);
		boolean dualFlag = (boolean) exceptionCode.getOtherInfo();
		exeOrderList = (List<EtlFeedTranNodeVb>) exceptionCode.getResponse();
		returnMapFirInputJson.put("fullRunFlag", "N");
		returnMapFirInputJson.put("debugFlag", "N");
		returnMapFirInputJson.put("dualFlag", (dualFlag)?"Y":"N");
		returnMapFirInputJson.put("folderName", vObject.getFeedId() + "_" + vObject.getSessionId());
		returnMapFirInputJson.put("transformationList", exeOrderList);
		returnMapFirInputJson.put("groupTransformationList", new ArrayList<EtlFeedTranGroupVb>());
		String jsonStr = formJsonString(returnMapFirInputJson);
		visionUploadWb.createInputFileInLocal(jsonStr, vObject.getFeedId()+"_"+vObject.getSessionId());
		return visionUploadWb.moveInputJson_TriggerSpark_ReadDataFile(vObject.getFeedId(), vObject.getSessionId(), selectedNodeVb);
	}
	
	public ExceptionCode getQueryTransformedLoadData(EtlFeedMainVb vObject){
		setVerifReqDeleteType(vObject);
		etlFeedTranColumnsDao.setServiceDefaults();
		int approveOrPend=0;
		if(vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		}else if(vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		}else if(vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}
		
		EtlFeedTranColumnVb dObj = new EtlFeedTranColumnVb();
		dObj.setCountry(vObject.getCountry());
		dObj.setLeBook(vObject.getLeBook());
		dObj.setFeedId(vObject.getFeedId());
		dObj.setVerificationRequired(vObject.isVerificationRequired());
		dObj.setNodeId("write");
		List<EtlFeedTranColumnVb> collTemp = etlFeedTranColumnsDao.getQueryResultsByParent(dObj,approveOrPend);
		vObject.setEtlFeedTranColumnList(collTemp);
		
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
	@Scheduled(fixedDelay = 900000, initialDelay = 900000)
	@Async
	public void clearTransformationSampleFolder() {
		String value = commonDao.findVisionVariableValue("ETL_TRANSFORMATION_RESIDUE_DELETE_LATENCY");
		if (!ValidationUtil.isValid(value)) {
			value = "6";
		}
		Date date = new Date();
		Calendar calender = Calendar.getInstance();
		calender.setTime(date);
		calender.add(Calendar.HOUR, -Integer.parseInt(value));
		Date decreasedDate = calender.getTime();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:s");
		String decreasedTime = formatter.format(decreasedDate);
		List<EtlFeedMainVb> vbs = new ArrayList<EtlFeedMainVb>();
		vbs = etlFeedMainDao.getEtlTransformationSession(decreasedTime);
		if (vbs != null && vbs.size() > 0) {
			for(EtlFeedMainVb vb : vbs) {
				visionUploadWb.deleteTransformationSession(vb, true);
			}
		}
	}
	
	public ExceptionCode doFullTransformationRunOnSampleData(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			//update the DATE_LAST_MODIFIED column in the session table
			exceptionCode = etlFeedMainDao.doUpdateSessionForTransformation(vObject);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap = vObject.getEtlFeedNodeList().stream().collect(Collectors.toMap(vb -> vb.getNodeId(), vb -> vb));
				EtlFeedTranNodeVb readVb = nodeDetailByNodeIDMap.get("read");
				EtlFeedTranNodeVb writeVb = nodeDetailByNodeIDMap.get("write");
				if("N".equalsIgnoreCase(writeVb.getNodeChange())) {
					exceptionCode = readDataFileIfAvailable(vObject.getFeedId(), vObject.getSessionId(), writeVb);
					if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = processNodeDetailsForFullRun(vObject, readVb, writeVb, nodeDetailByNodeIDMap);
					}
				} else {
					exceptionCode = processNodeDetailsForFullRun(vObject, readVb, writeVb, nodeDetailByNodeIDMap);
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
	
	private ExceptionCode processNodeDetailsForFullRun(EtlFeedMainVb vObject, EtlFeedTranNodeVb readNodeVb, EtlFeedTranNodeVb writeNodeVb, 
			Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap) throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ExceptionCode exceptionCode = new ExceptionCode();
		
		/* "exeOrderList" -> Calculated execution order for every Node of the Transformation*/
		List<EtlFeedTranNodeVb> exeOrderList = triggerRecursiveMthTocalculateExecutionOrderForFullRun(readNodeVb.getNodeId(), nodeDetailByNodeIDMap, vObject);
		
		/* "groupExeOrderList" -> Calculated Group_Lvl execution order */
		List<EtlFeedTranGroupVb> groupExeOrderList = calculateGroupLvlExecutionOrderForFullRun(exeOrderList);
		
		/* 
		 * 1. Invoke method based on Transformation_ID for every node
		 * 2. Since this is full run, need to form the syntax at group lvl
		 * */
		invokeMethodBasedOnTransformationIdForFullRun(groupExeOrderList, vObject.getFeedId(), vObject.getSessionId());
		
		/* Form executable Spark SQL query by using "TransformationSql" & "TransSqlAppend" at group_lvl */
		exceptionCode = formQueryExecutableForFullRun(groupExeOrderList);
		boolean dualFlag = (boolean) exceptionCode.getOtherInfo();
		groupExeOrderList = (List<EtlFeedTranGroupVb>) exceptionCode.getResponse();
		
		
		Map<String, Object> returnMapFirInputJson = new HashMap<>();
		returnMapFirInputJson.put("fullRunFlag", "Y");
		returnMapFirInputJson.put("debugFlag", "N");
		returnMapFirInputJson.put("dualFlag", (dualFlag)?"Y":"N");
		returnMapFirInputJson.put("folderName", vObject.getFeedId() + "_" + vObject.getSessionId());
		returnMapFirInputJson.put("transformationList", new ArrayList<EtlFeedTranNodeVb>());
		returnMapFirInputJson.put("groupTransformationList", groupExeOrderList);
		String jsonStr = formJsonString(returnMapFirInputJson);
		visionUploadWb.createInputFileInLocal(jsonStr, vObject.getFeedId()+"_"+vObject.getSessionId());
		return visionUploadWb.moveInputJson_TriggerSpark_ReadDataFile(vObject.getFeedId(), vObject.getSessionId(), writeNodeVb);
	}
	
	
	private List<EtlFeedTranNodeVb> triggerRecursiveMthTocalculateExecutionOrderForFullRun(String readNodeId, Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap, EtlFeedMainVb vObject) {
		Map<String, Integer> exeOrderMap = new HashMap<String, Integer>();
		List<EtlFeedTranNodeVb> exeOrderList = new ArrayList<EtlFeedTranNodeVb>();
		exeOrderMap.put(readNodeId, 0);
		
		EtlFeedTranNodeVb selectedNodeVb = nodeDetailByNodeIDMap.get(readNodeId);
				
		calExecutionOrderForFullRun(selectedNodeVb, exeOrderMap, 1, nodeDetailByNodeIDMap);
		
		for (Map.Entry<String, Integer> entry : exeOrderMap.entrySet()) {
			String currentNodeId = entry.getKey();
			int currentNodeExeOrder = entry.getValue();
			EtlFeedTranNodeVb currentNodeVb = nodeDetailByNodeIDMap.get(currentNodeId);
			currentNodeVb.setExecutionOrder(currentNodeExeOrder);
			exeOrderList.add(currentNodeVb);
		}
		Collections.sort(exeOrderList);
		
		return exeOrderList;
	}
	
	private void calExecutionOrderForFullRun(EtlFeedTranNodeVb nodeVb, Map<String, Integer> exeOrderMap, int currentLevel,
			Map<String, EtlFeedTranNodeVb> nodeDetailByNodeIDMap) {
		
		boolean isChildrenAvailable = (nodeVb.getChildTransformationList() != null && !nodeVb.getChildTransformationList().isEmpty());
		
		if (isChildrenAvailable) {
			for (EtlFeedTranNodeVb childVb : nodeVb.getChildTransformationList()) {
				EtlFeedTranNodeVb cNodeVb = nodeDetailByNodeIDMap.get(childVb.getNodeId());
				if (exeOrderMap.get(cNodeVb.getNodeId()) != null) {
					int existingLevel = exeOrderMap.get(cNodeVb.getNodeId());
					if (existingLevel < currentLevel) {
						exeOrderMap.put(cNodeVb.getNodeId(), currentLevel);
					}
				} else {
					exeOrderMap.put(cNodeVb.getNodeId(), currentLevel);
				}
				calExecutionOrderForFullRun(cNodeVb, exeOrderMap, currentLevel + 1, nodeDetailByNodeIDMap);
			}
		}
	}
	
	
	/*
	 * 1. Group Nodes based on Execution_Order & further based on Group_ID
	 * 2. Replace the column level "ParentNodeID" to link with the "Group Based View Name
	 * 3. Calculate Group_Lvl execution order
	 * */
	private List<EtlFeedTranGroupVb> calculateGroupLvlExecutionOrderForFullRun(List<EtlFeedTranNodeVb> exeOrderList){
		List<EtlFeedTranGroupVb> groupVbList = new ArrayList<EtlFeedTranGroupVb>();
		//groupLvlColumnParentIDMap -> With Column_ID, one can find the corresponding Group_UID
		Map<String, String> groupLvlColumnParentIDMap = new HashMap<String, String>(); //Key->columnID || Value->Group UID under which the node falls
		//groupLvlNodeIDMap -> With Node_ID, one can find the corresponding Group_UID
		Map<String, String> groupLvlNodeIDMap = new HashMap<String, String>(); //Key->nodeID || Value->Group UID under which the node falls

		/*
		 * "excutionOrderMap" -> Group Nodes based on Execution_Order (Key->Execution_Order, Value -> Map[Key->GroupID, Value -> List<EtlFeedTranNodeVb>]) & 
		 * further group the nodes inside every Execution_Order based on Group_ID (Key->GroupID, Value -> List<EtlFeedTranNodeVb>) */ 
		Map<Integer, Map<String, List<EtlFeedTranNodeVb>>> excutionOrderMap = new HashMap<Integer, Map<String, List<EtlFeedTranNodeVb>>>();
		for (EtlFeedTranNodeVb nodeVb : exeOrderList) {
			if(excutionOrderMap.get(nodeVb.getExecutionOrder())!=null) {
				Map<String, List<EtlFeedTranNodeVb>> groupMap = excutionOrderMap.get(nodeVb.getExecutionOrder());
				if(groupMap.get(nodeVb.getGroupId())!=null) {
					List<EtlFeedTranNodeVb> nodeList = groupMap.get(nodeVb.getGroupId());
					nodeList.add(nodeVb);
					groupMap.put(nodeVb.getGroupId(), nodeList);
				} else {
					groupMap.put(nodeVb.getGroupId(), new ArrayList<EtlFeedTranNodeVb>(Arrays.asList(nodeVb)));
				}
				excutionOrderMap.put(nodeVb.getExecutionOrder(), groupMap);
			} else {
				Map<String, List<EtlFeedTranNodeVb>> groupMap = new HashMap<String, List<EtlFeedTranNodeVb>>();
				groupMap.put(nodeVb.getGroupId(), new ArrayList<EtlFeedTranNodeVb>(Arrays.asList(nodeVb)));
				excutionOrderMap.put(nodeVb.getExecutionOrder(), groupMap);
			}
		}
		
		
		//1. Use "excutionOrderMap" to form a List of GroupVb with final-execution-order
		//2. Replace the column level "ParentNodeID" to link with the "Group Based View Name"
		
		/* previousViewName -> A combination of Group_ID and Execution order which can be considered as Parent node_ID of all nodes inside the current Group_ID */
		String previousViewName = "";
		for (Map.Entry<Integer, Map<String, List<EtlFeedTranNodeVb>>> entry : excutionOrderMap.entrySet()) {
			int currentExecutionOrder = entry.getKey();
			Map<String, List<EtlFeedTranNodeVb>> groupMap = entry.getValue();
			int groupExecutionOrder = 1;
			for (Map.Entry<String, List<EtlFeedTranNodeVb>> entryGroup : groupMap.entrySet()) {
				
				String currentGroupID = entryGroup.getKey();
				
				EtlFeedTranGroupVb groupVb = new EtlFeedTranGroupVb();
				groupVb.setGroupEO(Double.parseDouble(currentExecutionOrder+"."+groupExecutionOrder));
//				groupVb.setGroupEO(currentExecutionOrder+UNDESCORE+groupExecutionOrder);
				groupVb.setGroupID(currentGroupID);
				groupVb.setExecutionOrder(currentExecutionOrder);
				groupVb.setParentUID(previousViewName);

				/* currentViewName -> A combination of Group_ID and Execution order which can be considered as Parent node_ID of all nodes inside the current Group_ID */
				String currentViewName = currentGroupID+UNDESCORE+groupVb.getGroupEO();
				
				groupVb.setUID(currentViewName);
				groupVb.setViewName(currentViewName);
				
				List<EtlFeedTranNodeVb> nodeList = entryGroup.getValue();
				DeepCopy<List<EtlFeedTranNodeVb>> deepCopy = new DeepCopy<List<EtlFeedTranNodeVb>>();
				List<EtlFeedTranNodeVb> clonedNodeList = deepCopy.copy(nodeList);
				
				/* 
				 * 1. Insert record of GroupUID from which the column can be taken
				 * 2. Replace parentNodeID info of the column corresponding to the GroupUID from which the column can be taken
				 * */
				Set<String> parentGroupIdSet = new HashSet<String>();
				for(EtlFeedTranNodeVb nodeVb : clonedNodeList) {
					if("read".equalsIgnoreCase(nodeVb.getNodeId())) {
						groupVb.setTransformationType("A");
						groupVb.setUID("read");
						groupVb.setViewName("read");
					} else if("write".equalsIgnoreCase(nodeVb.getNodeId())) {
						groupVb.setTransformationType("A");
						groupVb.setUID("write");
						groupVb.setViewName("write");
					}
					groupVb.setTransformationID(nodeVb.getTransformationId());
					for(EtlFeedTranColumnVb colVb : nodeVb.getTranColumnList()) {
						// 1. insert record of GroupUID from which the column can be taken
						if(!"write".equalsIgnoreCase(groupVb.getUID())) {
							//"write" node cannot be parent_node of any columns, so we skip adding it to the list
							//groupLvlColumnParentIDMap -> With Column_ID, one can find the corresponding Group_UID
							groupLvlColumnParentIDMap.put(colVb.getColumnId(), groupVb.getUID());
						}
						// 2. replace parentNodeID info of the column corresponding to the GroupUID from which the column can be taken
						if(ValidationUtil.isValid(colVb.getParentNodeId()) && ValidationUtil.isValid(colVb.getParentColumnId())) {
							colVb.setCopyParentNodeId(colVb.getParentNodeId());
							colVb.setParentNodeId(groupLvlColumnParentIDMap.get(colVb.getParentColumnId()));
						}
					}
					
					/* Update Map "groupLvlNodeIDMap" with info of current NodeID belongs to this GroupUID */
					//groupLvlNodeIDMap -> With Node_ID, one can find the corresponding Group_UID
					groupLvlNodeIDMap.put(nodeVb.getNodeId(), groupVb.getUID());
					nodeVb.setGroupUID(groupVb.getUID());
					
					/* Based on Parent_Node_ID list of the current Node_ID, get the groupUID to which the Parent_Node_ID belongs to. 
					 * This groupUID is the parentGroupUID list of current groupUID*/
					if(nodeVb.getParentTransformationList()!=null && !nodeVb.getParentTransformationList().isEmpty()) {
						for(EtlFeedTranNodeVb pNodeVb : nodeVb.getParentTransformationList()) {
							parentGroupIdSet.add(groupLvlNodeIDMap.get(pNodeVb.getNodeId()));
						}
					}
					
				}
				
				if(parentGroupIdSet!=null && !parentGroupIdSet.isEmpty()) {
					List<EtlFeedTranGroupVb> parentGroupUIDList = new ArrayList<EtlFeedTranGroupVb>();
					for(String pGroupUID : parentGroupIdSet) {
						parentGroupUIDList.add(new EtlFeedTranGroupVb(pGroupUID));
					}
					groupVb.setParentGroupUIDList(parentGroupUIDList);
				}
				
				groupVb.setNodeList(clonedNodeList);
				groupVbList.add(groupVb);
				groupExecutionOrder++;
			}
		}
		Collections.sort(groupVbList);

		return groupVbList;
	}
	
	/*
	 * 1. Invoke methods to form transformation syntax for every Node
	 * 2. Combine Node_Lvl syntaxes at Group_Lvl and form "TransformationSql" & "TransSqlAppend" at group_lvl 
	 *  */
	private void invokeMethodBasedOnTransformationIdForFullRun(List<EtlFeedTranGroupVb> groupExeOrderList,
			String feedId, String sessionId) throws NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		TransformationFunctionsUtil classObj = new TransformationFunctionsUtil();
		Map<String, String> node_groupUID_Map = new HashMap<String, String>();
		for (EtlFeedTranGroupVb groupVb : groupExeOrderList) {
			if (!"read".equalsIgnoreCase(groupVb.getUID())) {
				for (EtlFeedTranNodeVb nodeVb : groupVb.getNodeList()) {
					
					node_groupUID_Map.put(nodeVb.getNodeId(), nodeVb.getGroupUID());
					
					/* Form Node Lvl Syntax */
					ArrayList<EtlFeedTranColumnVb> inputLst = new ArrayList<>();
					ArrayList<EtlFeedTranColumnVb> outputLst = new ArrayList<>();
					for (EtlFeedTranColumnVb vb : nodeVb.getTranColumnList()) {
						vb.setFeedId(feedId);
						vb.setSessionId(sessionId);
						if ("Y".equalsIgnoreCase(vb.getInputFlag())) {
							inputLst.add(vb);
						} else if ("Y".equalsIgnoreCase(vb.getOutputFlag()) && "N".equalsIgnoreCase(vb.getInputFlag())) {
							outputLst.add(vb);
						}
					}
					Map<String, List<EtlFeedTranColumnVb>> ioMap = new HashMap<String, List<EtlFeedTranColumnVb>>();
					String methodOutput = "";
					Method mth;
					/* For Filter transformation, check if the custom flag is set */
					boolean isFilterCustomFlag = false;
					if ("T_STR_073".equalsIgnoreCase(nodeVb.getTransformationId())
							&& "Y".equalsIgnoreCase(nodeVb.getCustomFlag())) {
						isFilterCustomFlag = true;
						nodeVb.setFilterCustomFlag(isFilterCustomFlag);
					}
					
					try {
						if (!"read".equalsIgnoreCase(nodeVb.getNodeId())) {
							String transType = etlFeedMainDao.getTransTypeForTransId(nodeVb.getTransformationId());
							String methodName = etlFeedMainDao.getMethodNameForTransId(nodeVb.getTransformationId());
							nodeVb.setMethodName(methodName);
							nodeVb.setTransformationType(transType);
							
							groupVb.setTransformationType(transType);
							
							ioMap.put("input", inputLst);
							ioMap.put("output", outputLst);
							
							if ("T_STR_081".equalsIgnoreCase(nodeVb.getTransformationId()) ) {
								/* T_STR_081 - Aggregation transformation */
								mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class });
								String[] methodOutForAgg = (String[]) mth.invoke(classObj, ioMap);
								nodeVb.setTransformationSql(methodOutForAgg[0]);
								nodeVb.setTransSqlAppend(methodOutForAgg[1]);
							} else if("T_STR_073".equalsIgnoreCase(nodeVb.getTransformationId())) {
								/* T_STR_073 - Filter transformation */
								mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class, boolean.class });
								String[] methodOutForAgg = (String[]) mth.invoke(classObj, ioMap, isFilterCustomFlag);
								nodeVb.setTransformationSql(methodOutForAgg[0]);
								if(isFilterCustomFlag == false) {
									nodeVb.setTransSqlAppend(methodOutForAgg[1]);
								} else {
									/* TO-DO 
									 * 1. Append Custom filter 'Custom_Expression' 
									 * 2. replace the Node_ID to the Group_UID */
									methodOutput = nodeVb.getCustomExpression();
									//replace the Node_ID to the Group_UID - Start
									for(EtlFeedTranColumnVb inputColVb:inputLst) {
										if(ValidationUtil.isValid(inputColVb.getCopyParentNodeId()) && node_groupUID_Map.get(inputColVb.getCopyParentNodeId())!=null && ValidationUtil.isValid(node_groupUID_Map.get(inputColVb.getCopyParentNodeId()))) {
											String value = TICK+inputColVb.getCopyParentNodeId()+TICK+".";//Node_ID - must be replaced
											String replaceWith = TICK+node_groupUID_Map.get(inputColVb.getCopyParentNodeId())+TICK+".";//Group_UID - To replace Node_ID
											methodOutput = methodOutput.replaceAll(value, replaceWith);
										}
									}
									//replace the Node_ID to the Group_UID - End
									
									nodeVb.setTransSqlAppend(" WHERE "+methodOutput);
								}
							} else if ("T_STR_082".equalsIgnoreCase(nodeVb.getTransformationId())) {
								/* T_STR_082 - Join transformation */
								mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class, String.class});
								methodOutput = (String) mth.invoke(classObj, ioMap, nodeVb.getJoinType());
								nodeVb.setTransformationSql(methodOutput);
							} else if("T_STR_085".equalsIgnoreCase(nodeVb.getTransformationId()) || "T_STR_086".equalsIgnoreCase(nodeVb.getTransformationId())){
								/* For Transformations that involves text input from client, which includes Node_ID as alias name before the column name in the text - We have 
								 * replace the Node_ID to the Group_UID 
								 * Example: Case_Condition transformation and Expression Transformation */
								mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class });
								methodOutput = (String) mth.invoke(classObj, ioMap);
								//replace the Node_ID to the Group_UID - Start
								for(EtlFeedTranColumnVb inputColVb:inputLst) {
									if(ValidationUtil.isValid(inputColVb.getCopyParentNodeId()) && node_groupUID_Map.get(inputColVb.getCopyParentNodeId())!=null && ValidationUtil.isValid(node_groupUID_Map.get(inputColVb.getCopyParentNodeId()))) {
										String value = TICK+inputColVb.getCopyParentNodeId()+TICK+".";//Node_ID - must be replaced
										String replaceWith = TICK+node_groupUID_Map.get(inputColVb.getCopyParentNodeId())+TICK+".";//Group_UID - To replace Node_ID
										methodOutput = methodOutput.replaceAll(value, replaceWith);
									}
								}
								//replace the Node_ID to the Group_UID - End
								nodeVb.setTransformationSql(methodOutput);
							} else {
								mth = classObj.getClass().getDeclaredMethod(nodeVb.getMethodName(), new Class[] { Map.class });
								methodOutput = (String) mth.invoke(classObj, ioMap);
								nodeVb.setTransformationSql(methodOutput);
							}
							groupVb.setTransSqlAppend(nodeVb.getTransSqlAppend());
						}
					} catch (Exception e) {
						// e.printStackTrace();
						throw new RuntimeCustomException(String.format(
								"Problem with transformation maintenance. Feed_ID [%s] Session_ID [%s] Node_ID [%s] Node_Name [%s] Transformation_ID [%s] CAUSE [%s]",
								feedId, sessionId, nodeVb.getNodeId(), nodeVb.getNodeName(),
								nodeVb.getTransformationId(), e.getMessage()));
					}
					nodeVb.setViewName(nodeVb.getNodeId());
				}
				StringBuffer groupTransformationSql = new StringBuffer();
				for (EtlFeedTranNodeVb nodeVb : groupVb.getNodeList()) {
					groupTransformationSql.append(nodeVb.getTransformationSql()+COMMA);
				}
				groupVb.setTransformationSql(groupTransformationSql.substring(0, groupTransformationSql.length() - 1));
			}
		}
		
	}
	
	/* Form executable Spark SQL query by using "TransformationSql" & "TransSqlAppend" at group_lvl */
	private ExceptionCode formQueryExecutableForFullRun(List<EtlFeedTranGroupVb> groupExeOrderList) {
		ExceptionCode exceptionCode = new ExceptionCode();
		boolean dualFlag = false;
		for (EtlFeedTranGroupVb groupVb : groupExeOrderList) {
			StringBuffer executableQuery = new StringBuffer();
			StringBuffer fromQuery = new StringBuffer();
			if ("T_STR_082".equalsIgnoreCase(groupVb.getTransformationID())) {
				executableQuery.append(groupVb.getTransformationSql());
			} else if (groupVb.getParentGroupUIDList()!=null && !groupVb.getParentGroupUIDList().isEmpty()) {
				String firstParentViewName = "";
				// form FROM clause by joining all the parent View_Name
				for (EtlFeedTranGroupVb pVb : groupVb.getParentGroupUIDList()) {
					EtlFeedTranGroupVb parentGroupVb = groupExeOrderList.stream()
							.filter(v -> (v.getUID()).equalsIgnoreCase(pVb.getUID())).findFirst().map(v -> v)
							.orElse(null);
					if (parentGroupVb != null) {
						String currentParentViewName = parentGroupVb.getViewName();
						if (!ValidationUtil.isValid(firstParentViewName)) {
							firstParentViewName = parentGroupVb.getViewName();
							fromQuery.append(TICK + currentParentViewName + TICK + SPACE + TICK + currentParentViewName + TICK);
						} else {
							fromQuery.append(INNER_JOIN + TICK + currentParentViewName + TICK + SPACE + TICK + currentParentViewName + TICK + ON);
							fromQuery.append(OPEN_BRACKET);
							fromQuery.append(TICK + firstParentViewName + TICK + ROWNUM + EQUAL + TICK + currentParentViewName + TICK + ROWNUM);
							fromQuery.append(CLOSE_BRACKET);
						}
					}
				}
				String selectQuery = "";
				if ("P".equalsIgnoreCase(groupVb.getTransformationType())) {
					selectQuery = "Select " + TICK + firstParentViewName + TICK + ROWNUM + ", " + groupVb.getTransformationSql();
				} else {
					selectQuery = "Select " + groupVb.getTransformationSql();
				}
				executableQuery.append(selectQuery + " FROM " + fromQuery + SPACE + groupVb.getTransSqlAppend());
			} else {
				dualFlag = true;
				executableQuery.append("Select " + groupVb.getTransformationSql() + " FROM DUAL");
			}
			groupVb.setTransSqlExecutable(String.valueOf(executableQuery));
		}
		
		//Add TICK symbol to viewName to avoid problem with special-characters in the viewName
		for (EtlFeedTranGroupVb groupVb : groupExeOrderList) {
			groupVb.setViewName(TICK+groupVb.getViewName()+TICK);			
		}
		exceptionCode.setResponse(groupExeOrderList);
		exceptionCode.setOtherInfo(dualFlag);
		return exceptionCode;
	}

	public ExceptionCode getPayloadDataForFullRun(EtlFeedMainVb vObject, int status) {
		ExceptionCode exceptionCode = new ExceptionCode();
		DeepCopy<EtlFeedMainVb> deepCopy = new DeepCopy<EtlFeedMainVb>();
		EtlFeedMainVb clonedObject = deepCopy.copy(vObject);
		try {
			List<EtlFeedTransformationVb> etlFeedNodeList = etlFeedTransformationDao.getQueryResultsByParent(vObject, status);
			List<EtlFeedTranNodeVb> finalEtlFeedNodeList = new ArrayList<>();
			for (EtlFeedTransformationVb transformationVb : etlFeedNodeList) {
				vObject.setNodeId(transformationVb.getNodeId());
				List<EtlFeedTransformationVb> parentList = etlFeedTransformationDao.getQueryResultsByParentNode(vObject,status);
				List<EtlFeedTransformationVb> childList = etlFeedTransformationDao.getQueryResultsByChildNode(vObject,status);
				List<EtlFeedTranNodeVb> parentTransformationList = new ArrayList<EtlFeedTranNodeVb>();
				List<EtlFeedTranNodeVb> childTransformationList = new ArrayList<EtlFeedTranNodeVb>();
				for(EtlFeedTransformationVb  vParent : parentList) {
					EtlFeedTranNodeVb parentVb = new EtlFeedTranNodeVb(vParent.getParentNodeId());
					parentTransformationList.add(parentVb);
				}
				for(EtlFeedTransformationVb  vChild : childList) {
					EtlFeedTranNodeVb childVb = new EtlFeedTranNodeVb(vChild.getChildNodeId());
					childTransformationList.add(childVb);
				}
				
				EtlFeedTranColumnVb dObj = new EtlFeedTranColumnVb();
				dObj.setCountry(transformationVb.getCountry());
				dObj.setLeBook(transformationVb.getLeBook());
				dObj.setFeedId(transformationVb.getFeedId());
				dObj.setNodeId(transformationVb.getNodeId());

				List<EtlFeedTranColumnVb> etlFeedTranColumnList = etlFeedTranColumnsDao.getQueryResults(dObj, status);
				
				EtlFeedTranNodeVb nodeVb = new EtlFeedTranNodeVb(transformationVb.getNodeId(),
						transformationVb.getNodeName(), transformationVb.getNodeDesc(), transformationVb.getxAxis(),
						transformationVb.getyAxis(), transformationVb.getTransformationId(),
						parentTransformationList, childTransformationList,
						transformationVb.getTransformationStatus(), transformationVb.getMinimizeFlag(),
						transformationVb.getGroupId(), transformationVb.getJoinType(),
						transformationVb.getCustomFlag(), transformationVb.getCustomExpression());
				nodeVb.setTranColumnList(etlFeedTranColumnList);
				finalEtlFeedNodeList.add(nodeVb);
			}

			vObject.setNodeId("");
			vObject.setEtlFeedNodeList(finalEtlFeedNodeList);
			exceptionCode.setOtherInfo(clonedObject);
			exceptionCode.setResponse(vObject);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("");
		} catch (Exception e) {
			exceptionCode.setOtherInfo(clonedObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("");
		}
		return exceptionCode;
	}

	public ExceptionCode deleteAllSourceTabData(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String tableType = "";
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				tableType = "UPL";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				tableType = "PEND";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED ) {
				tableType = "UPL"; // main table 
			}
			int retVal = 0;
			retVal= etlFeedSourceDao.deleteRecordByParent(vObject, tableType);
			retVal = etlFeedTablesDao.deleteRecordByParent(vObject, tableType);
			retVal = etlFeedColumnsDao.deleteRecordByParent(vObject, tableType);
			retVal = etlFeedRelationDao.deleteRecordByParent(vObject, tableType);
			if (retVal >= 1) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("Delete All Source Tab Data Successful !!");
			} else if(retVal == 0) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found !!");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Erroreous Operation !!");
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode deleteAllMaterializedTabData(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String tableType = "";
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				tableType = "UPL";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				tableType = "PEND";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				tableType = "UPL"; // main table
			}
			int retVal = 0;
			retVal = etlExtractionSummaryFieldsDao.deleteRecordByParent(vObject, tableType);
			if (retVal >= 1) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("Delete All Materialized Tab Data Successful !!");
			} else if (retVal == 0) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found !!");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Erroreous Operation !!");
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode deleteAllTransformationTabData(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String tableType = "";
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				tableType = "UPL";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				tableType = "PEND";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				tableType = "UPL"; // main table
			}
			int retVal = 0;
			retVal = etlFeedTransformationDao.deleteRecordByParent(vObject, tableType);
			retVal = etlFeedTransformationDao.deleteRecordByParentNode(vObject, tableType);
			retVal = etlFeedTransformationDao.deleteRecordByChildNode(vObject, tableType);
			retVal = etlFeedTranColumnsDao.deleteRecordByParent(vObject, tableType);
			if (retVal >= 1) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("Delete All Transformation Tab Data Successful !!");
			} else if (retVal == 0) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found !!");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg("Erroreous Operation !!");
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode deleteAllLoaderTabData(EtlFeedMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String tableType = "";
			if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
				tableType = "UPL";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
				tableType = "PEND";
			} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
				tableType = "UPL"; // main table
			}
			int retVal = 0;
			retVal = etlFeedDestinationDao.deleteRecordByParent(vObject, tableType);
			retVal = etlTransformedColumnsDao.deleteRecordByParent(vObject, tableType);
			retVal = etlTargetColumnsDao.deleteRecordByParent(vObject, tableType);
			retVal = etlFeedLoadMappingDao.deleteRecordByParent(vObject, tableType);
			retVal = etlFeedInjectionConfigDao.deleteRecordByParent(vObject, tableType);
			if (retVal >= 1) {
				exceptionCode.setErrorMsg("Delete All Loader Tab Data Successful !!");
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} else if (retVal == 0) {
				exceptionCode.setErrorMsg("No Records Found !!");
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg("Erroreous Operation !!");
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode getEtlSourceTableList(EtlFeedMainVb vObject) {
		setVerifReqDeleteType(vObject);
		etlFeedMainDao.setServiceDefaults();
		int approveOrPend = 0;
		if (vObject.getFeedStatus() == Constants.WORK_IN_PROGRESS) {
			approveOrPend = 9999;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			approveOrPend = 1;
		} else if (vObject.getFeedStatus() == Constants.PUBLISHED) {
			approveOrPend = 0;
		}
		List<ETLFeedTablesVb> collTemp = etlFeedMainDao.getEtlSourceTableList(vObject, approveOrPend);
		vObject.setEtlFeedTablesList(collTemp);
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
	@Override
	protected ExceptionCode doValidate(EtlFeedMainVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("ETLFeedConfiguration", operation);
		if(!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation +" "+Constants.userRestrictionMsg);
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
	
	public ExceptionCode generateFeedId(EtlFeedMainVb vObject) {
		String feedId = vObject.getCountry() + "" + vObject.getLeBook() + "" + etlFeedMainDao.getRandomNumber();
		ExceptionCode exceptionCode = new ExceptionCode();
		vObject.setFeedId(feedId);
		boolean feedexist = etlFeedMainDao.feedExistenceCount(vObject);
		while (feedexist) {
			feedId = vObject.getCountry() + "" + vObject.getLeBook() + "" + etlFeedMainDao.getRandomNumber();
			vObject.setFeedId(feedId);
			feedexist = etlFeedMainDao.feedExistenceCount(vObject);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(vObject);
		return exceptionCode;
	}
}
