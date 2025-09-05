package com.vision.wb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import com.vision.authentication.CustomContextHolder;
import com.vision.dao.AbstractDao;
import com.vision.dao.CommonApiDao;
import com.vision.dao.CommonDao;
import com.vision.dao.ReportsDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.CreateCsv;
import com.vision.util.DeepCopy;
import com.vision.util.ExcelExportUtil;
import com.vision.util.PDFExportUtil;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.PrdQueryConfig;
import com.vision.vb.PromptTreeVb;
import com.vision.vb.ReportFilterVb;
import com.vision.vb.ReportStgVb;
import com.vision.vb.ReportUserDefVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class ReportsWb extends AbstractWorkerBean<ReportsVb> implements ServletContextAware {
	public ApplicationContext applicationContext;
	private ServletContext servletContext;

	@Autowired
	private ReportsDao reportsDao;

	@Autowired
	private CommonDao commonDao;

	@Autowired
	private PDFExportUtil pdfExportUtil;

	@Autowired
	CommonApiDao commonApiDao;

	@Value("${app.databaseType}")
	private String databaseType;

	@Value("${app.productName}")
	private String productName;
	
	public void setServletContext(ServletContext arg0) {
		servletContext = arg0;
	}

	public static Logger logger = LoggerFactory.getLogger(ReportsWb.class);

	@Override
	protected void setAtNtValues(ReportsVb vObject) {

	}

	@Override
	protected void setVerifReqDeleteType(ReportsVb vObject) {
		vObject.setStaticDelete(false);
		vObject.setVerificationRequired(false);
	}

	@Override
	protected AbstractDao<ReportsVb> getScreenDao() {
		return reportsDao;
	}
	final String SERVICE_NAME = "PRD Reports";

	public ExceptionCode getReportList() {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			ArrayList<ReportsVb> applicationlst = (ArrayList<ReportsVb>) reportsDao.findApplicationCategory();
			for (ReportsVb reportVb : applicationlst) {
				ArrayList<ReportsVb> reportCatlst = (ArrayList<ReportsVb>) reportsDao
						.findReportCategory(reportVb.getApplicationId());
				ArrayList<ReportsVb> reportslstbyCategory = new ArrayList<ReportsVb>();
				ArrayList<ReportsVb> reportslst = new ArrayList<ReportsVb>();
				for (ReportsVb categoryVb : reportCatlst) {
					reportslstbyCategory = (ArrayList<ReportsVb>) reportsDao
							.getReportList(categoryVb.getReportCategory(), reportVb.getApplicationId());
					categoryVb.setReportslst(reportslstbyCategory);
					reportslst.add(categoryVb);
				}
				reportVb.setReportslst(reportslst);
			}
			exceptionCode.setResponse(applicationlst);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode reportFilterProcess(String filterRefCode) {
		List<ReportFilterVb> filterlst = new ArrayList<ReportFilterVb>();
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection conExt = null;
		try {
			List<ReportFilterVb> filterDetaillst = reportsDao.getReportFilterDetail(filterRefCode);
			if (filterDetaillst != null && filterDetaillst.size() > 0) {
				ReportFilterVb filterObj = filterDetaillst.get(0);
				String filterObjProp = ValidationUtil.isValid(filterObj.getFilterRefXml())
						? filterObj.getFilterRefXml().replaceAll("\n", "").replaceAll("\r", "")
						: "";
				String promptXml = CommonUtils.getValueForXmlTag(filterObjProp, "Prompts");
				int filterCnt = Integer.parseInt(CommonUtils.getValueForXmlTag(promptXml, "PromptCount"));
				for (int i = 1; i <= filterCnt; i++) {
					ReportFilterVb vObject = new ReportFilterVb();
					String refXml = CommonUtils.getValueForXmlTag(filterObjProp, "Prompt" + i);

					vObject.setFilterSeq(Integer.parseInt(CommonUtils.getValueForXmlTag(refXml, "Sequence")));
					vObject.setFilterLabel(CommonUtils.getValueForXmlTag(refXml, "Label"));
					vObject.setFilterType(CommonUtils.getValueForXmlTag(refXml, "Type"));
					vObject.setFilterSourceId(CommonUtils.getValueForXmlTag(refXml, "SourceId"));
					if (!ValidationUtil.isValid(vObject.getFilterSourceId())) {
						vObject.setFilterSourceId("DUMMY"); // Source Id will not be there for DATE/TEXTD filters
					}
					vObject.setDependencyFlag(CommonUtils.getValueForXmlTag(refXml, "DependencyFlag"));
					vObject.setDependentPrompt(CommonUtils.getValueForXmlTag(refXml, "DependentPrompt"));
					vObject.setMultiWidth(CommonUtils.getValueForXmlTag(refXml, "MultiWidth"));
					vObject.setMandatory(CommonUtils.getValueForXmlTag(refXml, "Mandatory"));
					vObject.setMultiSelect(CommonUtils.getValueForXmlTag(refXml, "MultiSelect"));
					vObject.setSpecificTab(CommonUtils.getValueForXmlTag(refXml, "SpecificTab")); // Only for Dashboards
					vObject.setDefaultValueId(CommonUtils.getValueForXmlTag(refXml, "DefaultValue"));
					vObject.setFilterRow(CommonUtils.getValueForXmlTag(refXml, "FilterRow"));
					vObject.setFilterDateFormat(CommonUtils.getValueForXmlTag(refXml, "DateFormat"));
					vObject.setFilterDateRestrict(CommonUtils.getValueForXmlTag(refXml, "DateRestrict"));
					if (ValidationUtil.isValid(vObject.getDefaultValueId())) {
						// defaultValueSrc = replaceFilterHashVariables(vObject.getDefaultValueId(),
						// vObject);
						// vObject.setFilterDefaultValue(reportsDao.getReportFilterValue(defaultValueSrc));
						LinkedHashMap<String, String> filterDefaultValueMap = new LinkedHashMap<String, String>();
						exceptionCode = getReportFilterSourceValue(vObject);
						LinkedHashMap<String, String> filterMap = (LinkedHashMap<String, String>) exceptionCode
								.getResponse();
						if (filterMap != null) {
							for (Map.Entry<String, String> entry : filterMap.entrySet()) {
								String key = entry.getKey();
								if (key.contains("@")) {
									key = key.replace("@", "");
									filterDefaultValueMap.put(key, entry.getValue());
								}

							}
							vObject.setFilterDefaultValue(filterDefaultValueMap);
						}
					}
					if (!ValidationUtil.isValid(vObject.getDependencyFlag())) {
						vObject.setDependencyFlag("N");
					}
					/*
					 * if ("N".equalsIgnoreCase(vObject.getDependencyFlag()) &&
					 * ValidationUtil.isValid(vObject.getFilterSourceId())) {
					 * vObject.setFilterSourceVal(getFilterSourceValue(vObject)); }
					 */
					filterlst.add(vObject);
				}
			}
			exceptionCode.setResponse(filterlst);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}

	public ExceptionCode getFilterSourceValue(ReportFilterVb vObject) {
		// LinkedHashMap<String, String> filterSourceVal = new LinkedHashMap<String,
		// String>();
		Connection conExt = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<PrdQueryConfig> sourceQuerylst = reportsDao.getSqlQuery(vObject.getFilterSourceId());
			if (sourceQuerylst != null && sourceQuerylst.size() > 0) {
				PrdQueryConfig sourceQueryDet = sourceQuerylst.get(0);
				if ("QUERY".equalsIgnoreCase(sourceQueryDet.getDataRefType())) {
					sourceQueryDet.setQueryProc(replaceFilterHashVariables(sourceQueryDet.getQueryProc(), vObject));
					exceptionCode = reportsDao.getReportFilterValue(sourceQueryDet.getQueryProc());
				} else if ("PROCEDURE".equalsIgnoreCase(sourceQueryDet.getDataRefType())) {
					exceptionCode = reportsDao.getComboPromptData(vObject, sourceQueryDet);

				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error(e.getMessage());
			}
		}
		return exceptionCode;
	}

	public String replaceFilterHashVariables(String query, ReportFilterVb vObject) {
		query = query.replaceAll("#VISION_ID#", "" + CustomContextHolder.getContext().getVisionId());

		vObject.setFilter0Val(vObject.getFilter0Val().replaceAll("''", ""));
		vObject.setFilter1Val(vObject.getFilter1Val().replaceAll("''", ""));
		vObject.setFilter2Val(vObject.getFilter2Val().replaceAll("''", ""));
		vObject.setFilter3Val(vObject.getFilter3Val().replaceAll("''", ""));
		vObject.setFilter4Val(vObject.getFilter4Val().replaceAll("''", ""));
		vObject.setFilter5Val(vObject.getFilter5Val().replaceAll("''", ""));

		vObject.setFilter6Val(vObject.getFilter6Val().replaceAll("''", ""));
		vObject.setFilter7Val(vObject.getFilter7Val().replaceAll("''", ""));
		vObject.setFilter8Val(vObject.getFilter8Val().replaceAll("''", ""));

		if (ValidationUtil.isValid(vObject.getFilter2Val())) {
			vObject.setFilter2Val(vObject.getFilter2Val().replaceAll("ALL-ALL", "ALL"));
		}
		if (ValidationUtil.isValid(vObject.getFilter3Val())) {
			vObject.setFilter3Val(vObject.getFilter3Val().replaceAll("ALL-ALL", "ALL"));
		}
		if (ValidationUtil.isValid(vObject.getFilter4Val())) {
			vObject.setFilter4Val(vObject.getFilter4Val().replaceAll("ALL-ALL", "ALL"));
		}

		query = query.replaceAll("#TIME_ZONE#",
				ValidationUtil.isValid(commonDao.findVisionVariableValue("TIME_ZONE"))
						? commonDao.findVisionVariableValue("TIME_ZONE")
						: "Asia/Dubai");

		query = query.replaceAll("#PROMPT_VALUE_0#",
				ValidationUtil.isValid(vObject.getFilter0Val()) ? vObject.getFilter0Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_1#",
				ValidationUtil.isValid(vObject.getFilter1Val()) ? vObject.getFilter1Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_2#",
				ValidationUtil.isValid(vObject.getFilter2Val()) ? vObject.getFilter2Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_3#",
				ValidationUtil.isValid(vObject.getFilter3Val()) ? vObject.getFilter3Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_4#",
				ValidationUtil.isValid(vObject.getFilter4Val()) ? vObject.getFilter4Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_5#",
				ValidationUtil.isValid(vObject.getFilter5Val()) ? vObject.getFilter5Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_6#",
				ValidationUtil.isValid(vObject.getFilter6Val()) ? vObject.getFilter6Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_7#",
				ValidationUtil.isValid(vObject.getFilter7Val()) ? vObject.getFilter7Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_8#",
				ValidationUtil.isValid(vObject.getFilter8Val()) ? vObject.getFilter8Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_9#",
				ValidationUtil.isValid(vObject.getFilter9Val()) ? vObject.getFilter9Val() : "''");
		query = query.replaceAll("#PROMPT_VALUE_10#",
				ValidationUtil.isValid(vObject.getFilter10Val()) ? vObject.getFilter10Val() : "''");

		query = query.replaceAll("#NS_PROMPT_VALUE_1#",
				ValidationUtil.isValid(vObject.getFilter1Val()) ? vObject.getFilter1Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_2#",
				ValidationUtil.isValid(vObject.getFilter2Val()) ? vObject.getFilter2Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_3#",
				ValidationUtil.isValid(vObject.getFilter3Val()) ? vObject.getFilter3Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_4#",
				ValidationUtil.isValid(vObject.getFilter4Val()) ? vObject.getFilter4Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_5#",
				ValidationUtil.isValid(vObject.getFilter5Val()) ? vObject.getFilter5Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_6#",
				ValidationUtil.isValid(vObject.getFilter6Val()) ? vObject.getFilter6Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_7#",
				ValidationUtil.isValid(vObject.getFilter7Val()) ? vObject.getFilter7Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_8#",
				ValidationUtil.isValid(vObject.getFilter8Val()) ? vObject.getFilter8Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_9#",
				ValidationUtil.isValid(vObject.getFilter9Val()) ? vObject.getFilter9Val().replaceAll("'", "") : "''");
		query = query.replaceAll("#NS_PROMPT_VALUE_10#",
				ValidationUtil.isValid(vObject.getFilter10Val()) ? vObject.getFilter10Val().replaceAll("'", "") : "''");

		ReportsVb promptsVb = new ReportsVb();
		promptsVb.setPromptValue1(vObject.getFilter1Val());
		promptsVb.setPromptValue2(vObject.getFilter2Val());
		promptsVb.setPromptValue3(vObject.getFilter3Val());
		promptsVb.setPromptValue4(vObject.getFilter4Val());
		promptsVb.setPromptValue5(vObject.getFilter5Val());
		promptsVb.setPromptValue6(vObject.getFilter6Val());
		promptsVb.setPromptValue7(vObject.getFilter7Val());
		promptsVb.setPromptValue8(vObject.getFilter8Val());
		promptsVb.setPromptValue9(vObject.getFilter9Val());
		promptsVb.setPromptValue10(vObject.getFilter10Val());

		query = commonDao.applyUserRestriction(query);
		query = reportsDao.applyPrPromptChange(query, promptsVb);
		query = reportsDao.applySpecialPrompts(query, promptsVb);

		return query;
	}

	public ExceptionCode getReportDetails(ReportsVb vObject) throws SQLException {
		ExceptionCode exceptionCode = new ExceptionCode();
		ExceptionCode exceptionCodeProc = new ExceptionCode();
		PrdQueryConfig prdQueryConfig = new PrdQueryConfig();
		List<PrdQueryConfig> sqlQueryList = new ArrayList<PrdQueryConfig>();
		DeepCopy<ReportsVb> clonedObj = new DeepCopy<ReportsVb>();
		PromptTreeVb promptTreeVb = new PromptTreeVb();
		String reportType = vObject.getReportType();
		ReportsVb subReportsVb = new ReportsVb();
		try {
			List<ReportsVb> reportDatalst = new ArrayList<ReportsVb>();
			if (!ValidationUtil.isValid(vObject.getObjectType())) {
				vObject.setObjectType("G");
			}
			subReportsVb = clonedObj.copy(vObject);
			if (ValidationUtil.isValid(vObject.getSubReportId())) {
				if (ValidationUtil.isValid(subReportsVb.getDdFlag())
						&& "Y".equalsIgnoreCase(subReportsVb.getDdFlag())) {
					reportDatalst = reportsDao.getSubReportDetail(vObject);
					if (reportDatalst != null && reportDatalst.size() > 0) {
						subReportsVb = reportDatalst.get(0);
						subReportsVb.setNextLevel(reportsDao.getIntReportNextLevel(subReportsVb));
					}
				} else {
					subReportsVb.setNextLevel("0");
				}

			} else {
				reportDatalst = reportsDao.getSubReportDetail(vObject);
				if (reportDatalst != null && reportDatalst.size() > 0) {
					subReportsVb = reportDatalst.get(0);
					if (ValidationUtil.isValid(subReportsVb.getDdFlag())
							&& "Y".equalsIgnoreCase(subReportsVb.getDdFlag())) {
						subReportsVb.setNextLevel(reportsDao.getNextLevel(subReportsVb));
					}
				} else {
					exceptionCode.setOtherInfo(vObject);
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg(
							"Report Levels Not Maintained for the ReportId[" + vObject.getReportId() + "] !!");
					return exceptionCode;
				}
			}
			sqlQueryList = reportsDao.getSqlQuery(subReportsVb.getDataRefId());
			if (sqlQueryList != null && sqlQueryList.size() > 0) {
				prdQueryConfig = sqlQueryList.get(0);
			} else {
				exceptionCode.setOtherInfo(vObject);
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode
						.setErrorMsg("Query not maintained for the Data Ref Id[" + subReportsVb.getDataRefId() + "]");
				return exceptionCode;
			}
			subReportsVb.setDbConnection(prdQueryConfig.getDbConnectionName());
			vObject.setDbConnection(prdQueryConfig.getDbConnectionName());
			ArrayList<ColumnHeadersVb> colHeaders = new ArrayList<ColumnHeadersVb>();
			List<ColumnHeadersVb> columnHeadersXmllst = reportsDao.getReportColumns(subReportsVb);
			String columnHeaderXml = "";
			if ((columnHeadersXmllst == null || columnHeadersXmllst.isEmpty())) {
				if ("QUERY".equalsIgnoreCase(prdQueryConfig.getDataRefType())) {
					exceptionCode.setOtherInfo(vObject);
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode
							.setErrorMsg("Column Headers Not Maintained for the ReportId[" + subReportsVb.getReportId()
									+ "] and Sub Report Id[" + subReportsVb.getSubReportId() + "] !!");
					return exceptionCode;
				}
			} else {
				columnHeaderXml = columnHeadersXmllst.get(0).getColumnXml();

			}
			subReportsVb.setReportTitle(CommonUtils.getValueForXmlTag(columnHeaderXml, "OBJECT_CAPTION"));
			subReportsVb.setGrandTotalCaption(CommonUtils.getValueForXmlTag(columnHeaderXml, "GRANDTOTAL_CAPTION"));

			if ("G".equalsIgnoreCase(subReportsVb.getObjectType())) {
				colHeaders = reportsDao.getColumnHeaders(columnHeaderXml, vObject);
				subReportsVb.setColumnHeaderslst(colHeaders);
			} else if ("C".equalsIgnoreCase(subReportsVb.getObjectType())
					|| "T".equalsIgnoreCase(subReportsVb.getObjectType())
					|| "SC".equalsIgnoreCase(subReportsVb.getObjectType())
					|| "S".equalsIgnoreCase(subReportsVb.getObjectType())) {
				subReportsVb.setColHeaderXml(columnHeaderXml);
			}
			String finalExeQuery = "";
			String maxRecords = commonDao.findVisionVariableValue("PRD_REPORT_MAXFETCH");
			subReportsVb.setMaxRecords(Integer.parseInt(ValidationUtil.isValid(maxRecords) ? maxRecords : "5000"));
			// prdQueryConfig.setQueryProc(prdQueryConfig.getQueryProc().toUpperCase());
			String queryTmp = prdQueryConfig.getQueryProc();
			queryTmp = queryTmp.toUpperCase();
			vObject.setDateCreation(
					String.valueOf(System.currentTimeMillis()) + vObject.getCurrentLevel().substring(0, 1));
			if ("QUERY".equalsIgnoreCase(prdQueryConfig.getDataRefType())) {
				if ("G".equalsIgnoreCase(subReportsVb.getObjectType())
						|| "T".equalsIgnoreCase(subReportsVb.getObjectType())
						|| "SC".equalsIgnoreCase(subReportsVb.getObjectType())
						|| "S".equalsIgnoreCase(subReportsVb.getObjectType())) {
					if (queryTmp.contains("ORDER BY")) {
						String orderBy = queryTmp.substring(queryTmp.lastIndexOf("ORDER BY"), queryTmp.length());
						prdQueryConfig.setQueryProc(
								prdQueryConfig.getQueryProc().substring(0, queryTmp.lastIndexOf("ORDER BY")));
						prdQueryConfig.setSortField(orderBy);
					}
					if (!ValidationUtil.isValid(prdQueryConfig.getSortField())) {
						exceptionCode.setOtherInfo(vObject);
						exceptionCode.setErrorMsg(
								"ORDER BY is mandatory in Query for [" + subReportsVb.getDataRefId() + "] !!");
						return exceptionCode;
					}
				}
				finalExeQuery = reportsDao.replacePromptVariables(prdQueryConfig.getQueryProc(), vObject, false);
			} else if ("PROCEDURE".equalsIgnoreCase(prdQueryConfig.getDataRefType())) {
				vObject.setSubReportId(subReportsVb.getSubReportId());
				if (vObject.getPromptTree() == null) {
					finalExeQuery = reportsDao.replacePromptVariables(prdQueryConfig.getQueryProc(), vObject, true);
					exceptionCodeProc = reportsDao.callProcforReportData(vObject, finalExeQuery);
				} else {
					exceptionCodeProc.setResponse(vObject.getPromptTree());
					exceptionCodeProc.setErrorCode(Constants.STATUS_ZERO);
				}
				if (exceptionCodeProc.getErrorCode() == Constants.STATUS_ZERO) {
					promptTreeVb = (PromptTreeVb) exceptionCodeProc.getResponse();
					if (ValidationUtil.isValid(promptTreeVb.getTableName())) {
						if ("REPORTS_STG".equalsIgnoreCase(promptTreeVb.getTableName().toUpperCase())) {
							finalExeQuery = "SELECT * FROM " + promptTreeVb.getTableName() + " WHERE SESSION_ID='"
									+ promptTreeVb.getSessionId() + "' AND REPORT_ID='" + promptTreeVb.getReportId()
									+ "' ";
						} else {
							finalExeQuery = "SELECT * FROM " + promptTreeVb.getTableName() + " ";
						}
						prdQueryConfig.setSortField("ORDER BY SORT_FIELD");
						if (ValidationUtil.isValid(promptTreeVb.getColumnHeaderTable())) {
							colHeaders = (ArrayList<ColumnHeadersVb>) reportsDao.getColumnHeaderFromTable(promptTreeVb);
							if (colHeaders != null && !colHeaders.isEmpty()) {
								subReportsVb.setColumnHeaderslst(colHeaders);
							}
						}
						if (!"CB".equalsIgnoreCase(reportType)) {
							if (subReportsVb.getColumnHeaderslst() == null
									|| subReportsVb.getColumnHeaderslst().isEmpty()) {
								exceptionCode.setOtherInfo(vObject);
								exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
								exceptionCode.setErrorMsg(
										"Column Headers Not Maintained for the ReportId[" + subReportsVb.getReportId()
												+ "] and Sub Report Id[" + subReportsVb.getSubReportId() + "] !!");
								return exceptionCode;
							}
						}
						subReportsVb.setPromptTree(promptTreeVb);
					} else {
						exceptionCode.setOtherInfo(vObject);
						exceptionCode.setErrorMsg(
								"Output Table not return from Procedure but the Procedure return Success Status");
						return exceptionCode;
					}
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg(exceptionCodeProc.getErrorMsg());
					exceptionCode.setOtherInfo(vObject);
					return exceptionCode;
				}
			}
			subReportsVb.setSortField(prdQueryConfig.getSortField());
			String masterReportType = reportsDao.getReportType(vObject.getReportId());
			if ("W".equalsIgnoreCase(masterReportType) && "C".equalsIgnoreCase(subReportsVb.getObjectType()))
				subReportsVb.setChartList(reportsDao.getChartList(vObject.getChartType()));

			// save user def settings
			List savedUserSetLst = new ArrayList<>();
			ArrayList<SmartSearchVb> smartSearchlst = new ArrayList<SmartSearchVb>();
			ReportUserDefVb reportUserDefVb = new ReportUserDefVb();
			if (!ValidationUtil.isValid(vObject.getRunType()))
				vObject.setRunType("R");

			if (!"W".equalsIgnoreCase(masterReportType) && "G".equalsIgnoreCase(vObject.getObjectType())
					&& "R".equalsIgnoreCase(vObject.getRunType())) {
				savedUserSetLst = reportsDao.getSavedUserDefSetting(subReportsVb, true);

			}
			if (savedUserSetLst != null && !savedUserSetLst.isEmpty())
				reportUserDefVb = (ReportUserDefVb) savedUserSetLst.get(0);

			if (!ValidationUtil.isValid(vObject.getColumnsToHide()))
				subReportsVb.setColumnsToHide(reportUserDefVb.getColumnsToHide());
			else
				subReportsVb.setColumnsToHide(vObject.getColumnsToHide());
			if (!ValidationUtil.isValid(vObject.getApplyGrouping()))
				subReportsVb.setApplyGrouping(reportUserDefVb.getApplyGrouping());
			else
				subReportsVb.setApplyGrouping(vObject.getApplyGrouping());
			if (!ValidationUtil.isValid(vObject.getShowDimensions()))
				subReportsVb.setShowDimensions(reportUserDefVb.getShowDimensions());
			else
				subReportsVb.setShowDimensions(vObject.getShowDimensions());
			if (!ValidationUtil.isValid(vObject.getShowMeasures()))
				subReportsVb.setShowMeasures(reportUserDefVb.getShowMeasures());
			else
				subReportsVb.setShowMeasures(vObject.getShowMeasures());
			if (ValidationUtil.isValid(reportUserDefVb.getSearchColumn())) {
				String[] searchArr = reportUserDefVb.getSearchColumn().split(",");
				for (String strArr : searchArr) {
					SmartSearchVb smartSearchVb = new SmartSearchVb();
					String[] searcCondArr = strArr.split("!@#");
					smartSearchVb.setObject(searcCondArr[0]);
					smartSearchVb.setCriteria(searcCondArr[1]);
					smartSearchVb.setValue(searcCondArr[2]);
					smartSearchlst.add(smartSearchVb);
				}
			}
			if ("Y".equalsIgnoreCase(subReportsVb.getApplyGrouping())) {
				// vObject.setScreenSortColumn("ORDER BY " + subReportsVb.getShowDimensions());
				subReportsVb.setSortField("ORDER BY " + subReportsVb.getShowDimensions());
				if (!ValidationUtil.isValid(vObject.getScreenSortColumn())
						&& ValidationUtil.isValid(reportUserDefVb.getSortColumn()))
					subReportsVb.setSortField(reportUserDefVb.getSortColumn());
				if (ValidationUtil.isValid(vObject.getScreenSortColumn()))
					subReportsVb.setSortField(vObject.getScreenSortColumn());
			} else {
				if (!ValidationUtil.isValid(vObject.getScreenSortColumn())
						&& !ValidationUtil.isValid(reportUserDefVb.getSortColumn()))
					subReportsVb.setSortField(prdQueryConfig.getSortField());
				else if (!ValidationUtil.isValid(vObject.getScreenSortColumn())
						&& ValidationUtil.isValid(reportUserDefVb.getSortColumn()))
					subReportsVb.setSortField(reportUserDefVb.getSortColumn());
				else
					subReportsVb.setSortField(vObject.getScreenSortColumn());
			}

			if (smartSearchlst != null && !smartSearchlst.isEmpty()
					&& !ValidationUtil.isValid(vObject.getSmartSearchOpt())) {
				vObject.setSmartSearchOpt(smartSearchlst);
			}
			if (vObject.getSmartSearchOpt() != null && vObject.getSmartSearchOpt().size() > 0) {
				finalExeQuery = "SELECT * FROM ( " + finalExeQuery + " ) TEMP WHERE ";
				for (int len = 0; len < vObject.getSmartSearchOpt().size(); len++) {
					SmartSearchVb data = vObject.getSmartSearchOpt().get(len);
					String searchVal = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());
					if (len > 0)
						finalExeQuery = finalExeQuery + " AND ";
					if ("MSSQL".equalsIgnoreCase(databaseType)) {
						if ("GREATER".equalsIgnoreCase(data.getCriteria())
								|| "GREATEREQUALS".equalsIgnoreCase(data.getCriteria())
								|| "LESSER".equalsIgnoreCase(data.getCriteria())
								|| "LESSEREQUALS".equalsIgnoreCase(data.getCriteria()))
							finalExeQuery = finalExeQuery + "(" + data.getObject() + ") " + searchVal;
						else
							finalExeQuery = finalExeQuery + " UPPER(" + data.getObject() + ") " + searchVal;
					} else {
						finalExeQuery = finalExeQuery + " UPPER(" + data.getObject() + ") " + searchVal;
					}
				}
			}

			subReportsVb.setReportUserDeflst(savedUserSetLst);
			subReportsVb.setFinalExeQuery(finalExeQuery);
			// subReportsVb.setSortField(prdQueryConfig.getSortField());
			subReportsVb.setPromptLabel(vObject.getPromptLabel());
			subReportsVb.setDbConnection(prdQueryConfig.getDbConnectionName());
			subReportsVb.setDrillDownLabel(vObject.getDrillDownLabel());
			subReportsVb.setCurrentPage(vObject.getCurrentPage());
			subReportsVb.setWidgetTheme(vObject.getWidgetTheme());

			exceptionCode.setResponse(subReportsVb);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Success");
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Exception while getting Report Data[" + vObject.getReportId() + "]SubSeq["
						+ vObject.getNextLevel() + "]...!!");
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setOtherInfo(subReportsVb);
			return exceptionCode;
		}
	}

	public ExceptionCode exportToPdf(int currentUserId, ReportsVb reportsVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = doValidate(reportsVb);
			if (exceptionCode != null && exceptionCode.getErrorMsg() != "") {
				return exceptionCode;
			}
			String applicationTheme = reportsVb.getApplicationTheme();
			String reportOrientation = reportsVb.getReportOrientation();
			String reportTitle = reportsVb.getReportTitle();
			String assetFolderUrl = servletContext.getRealPath("/WEB-INF/classes/images");
			// String assetFolderUrl = commonDao.findVisionVariableValue("MDM_IMAGE_PATH");
			String screenGroupColumn = reportsVb.getScreenGroupColumn();
			String screenSortColumn = reportsVb.getScreenSortColumn();
			String[] hiddenColumns = null;
			if (ValidationUtil.isValid(reportsVb.getColumnsToHide())) {
				hiddenColumns = reportsVb.getColumnsToHide().split("!@#");
			}
			reportsVb.setMaker(CustomContextHolder.getContext().getVisionId());
			exceptionCode = getReportDetails(reportsVb);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					reportsVb = (ReportsVb) exceptionCode.getResponse();
					// exceptionCode = getResultData(subReportsVb);
				}
			} else {
				return exceptionCode;
			}
			List<ColumnHeadersVb> colHeaderslst = reportsVb.getColumnHeaderslst();
			ArrayList<ColumnHeadersVb> updatedLst = new ArrayList<ColumnHeadersVb>();
			if (hiddenColumns != null) {
				for (int ctr = 0; ctr < hiddenColumns.length; ctr++) {
					updatedLst = formColumnHeader(colHeaderslst, hiddenColumns[ctr]);
					colHeaderslst = updatedLst;
				}
			}

			if (updatedLst != null && updatedLst.size() > 0) {
				int finalMaxRow = updatedLst.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
				for (ColumnHeadersVb colObj : updatedLst) {
					if (colObj.getRowspan() > finalMaxRow) {
						colObj.setRowspan(colObj.getRowspan() - finalMaxRow);
					}
				}
				colHeaderslst = updatedLst;

			}

			List<String> colTypes = new ArrayList<String>();
			Map<Integer, Integer> columnWidths = new HashMap<Integer, Integer>(colHeaderslst.size());
			for (int loopCnt = 0; loopCnt < colHeaderslst.size(); loopCnt++) {
				columnWidths.put(Integer.valueOf(loopCnt), Integer.valueOf(-1));
				ColumnHeadersVb colHVb = colHeaderslst.get(loopCnt);
				if (colHVb.getColspan() <= 1 && colHVb.getNumericColumnNo() != 99) {
					colTypes.add(colHVb.getColType());
				}
			}
			// logger.info("Pdf Export Data Extraction
			// Begin["+reportsVb.getReportId()+":"+reportsVb.getSubReportId()+"]");
			exceptionCode = reportsDao.getResultData(reportsVb, true);
			// logger.info("Pdf Export Data Extraction
			// End["+reportsVb.getReportId()+":"+reportsVb.getSubReportId()+"]");
			List<HashMap<String, String>> dataLst = null;
			List<HashMap<String, String>> totalLst = null;
			ReportsVb resultVb = new ReportsVb();
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					resultVb = (ReportsVb) exceptionCode.getResponse();
					dataLst = resultVb.getGridDataSet();
					totalLst = resultVb.getTotal();
				}
			}
			// logger.info("Pdf Export Data Write
			// Begin["+reportsVb.getReportId()+":"+reportsVb.getSubReportId()+"]");
			/*
			 * if(!reportOrientation.equalsIgnoreCase(reportsVb.getReportOrientation())) {
			 * reportsVb.setPdfGrwthPercent(0); }
			 */
			reportsVb.setReportTitle(reportTitle);
			reportsVb.setMaker(currentUserId);
			reportsVb.setReportOrientation(reportOrientation);
			reportsVb.setApplicationTheme(applicationTheme);
			getScreenDao().fetchMakerVerifierNames(reportsVb);
			// Grouping on PDF
			String[] capGrpCols = null;
			ArrayList<String> groupingCols = new ArrayList<String>();
			ArrayList<ColumnHeadersVb> columnHeadersFinallst = new ArrayList<ColumnHeadersVb>();
			colHeaderslst.forEach(colHeadersVb -> {
				if (colHeadersVb.getColspan() <= 1 && colHeadersVb.getNumericColumnNo() != 99) {
					columnHeadersFinallst.add(colHeadersVb);
				}
			});
			if (ValidationUtil.isValid(screenGroupColumn)) {
				reportsVb.setPdfGroupColumn(screenGroupColumn);
			}
			if (ValidationUtil.isValid(reportsVb.getPdfGroupColumn()))
				capGrpCols = reportsVb.getPdfGroupColumn().split("!@#");

			if (reportsVb.getTotalRows() <= reportsVb.getMaxRecords() && capGrpCols != null && capGrpCols.length > 0) {
				for (String grpStr : capGrpCols) {
					for (ColumnHeadersVb colHeader : columnHeadersFinallst) {
						if (grpStr.equalsIgnoreCase(colHeader.getCaption().toUpperCase())) {
							groupingCols.add(colHeader.getDbColumnName());
							break;
						}
					}
				}
			}
			final String[] grpColNames = capGrpCols;
			Map<String, List<HashMap<String, String>>> groupingMap = new HashMap<String, List<HashMap<String, String>>>();
			if (reportsVb.getTotalRows() <= reportsVb.getMaxRecords()
					&& (groupingCols != null && groupingCols.size() > 0)) {
				switch (groupingCols.size()) {
				case 1:
					groupingMap = dataLst.stream()
							.collect(Collectors.groupingBy(m -> (m.get(groupingCols.get(0))) == null ? ""
									: grpColNames[0] + ": " + m.get(groupingCols.get(0))));
					break;
				case 2:
					groupingMap = dataLst.stream()
							.collect(Collectors.groupingBy(
									m -> (m.get(groupingCols.get(0)) + " >> " + m.get(groupingCols.get(1))) == null ? ""
											: grpColNames[0] + ": " + m.get(groupingCols.get(0)) + " >> "
													+ grpColNames[1] + ": " + m.get(groupingCols.get(1))));
					break;
				case 3:
					groupingMap = dataLst.stream()
							.collect(Collectors.groupingBy(m -> (m.get(groupingCols.get(0)) + " >> "
									+ m.get(groupingCols.get(1)) + " >> " + m.get(groupingCols.get(2))) == null
											? ""
											: grpColNames[0] + ": " + m.get(groupingCols.get(0)) + " >> "
													+ grpColNames[1] + ": " + m.get(groupingCols.get(1)) + " >> "
													+ grpColNames[2] + ": " + m.get(groupingCols.get(2))));
					break;
				case 4:
					groupingMap = dataLst.stream().collect(
							Collectors.groupingBy(m -> (m.get(groupingCols.get(0)) + " >> " + m.get(groupingCols.get(1))
									+ " >> " + m.get(groupingCols.get(2)) + " >> " + m.get(groupingCols.get(3))) == null
											? ""
											: grpColNames[0] + ": " + m.get(groupingCols.get(0)) + " >> "
													+ grpColNames[1] + ": " + m.get(groupingCols.get(1)) + " >> "
													+ grpColNames[2] + ": " + m.get(groupingCols.get(2)) + " >> "
													+ grpColNames[2] + ": " + m.get(groupingCols.get(3))));
					break;
				case 5:
					groupingMap = dataLst.stream()
							.collect(Collectors.groupingBy(m -> (m.get(groupingCols.get(0)) + " >> "
									+ m.get(groupingCols.get(1)) + " >> " + m.get(groupingCols.get(2)) + " >> "
									+ m.get(groupingCols.get(3)) + " >> " + m.get(groupingCols.get(4))) == null
											? ""
											: grpColNames[0] + ": " + m.get(groupingCols.get(0)) + " >> "
													+ grpColNames[1] + ": " + m.get(groupingCols.get(1)) + " >> "
													+ grpColNames[2] + ": " + m.get(groupingCols.get(2)) + " >> "
													+ grpColNames[2] + ": " + m.get(groupingCols.get(3)) + " >> "
													+ grpColNames[2] + ": " + m.get(groupingCols.get(4))));
					break;
				}
				Map<String, List<HashMap<String, String>>> sortedMap = new TreeMap<String, List<HashMap<String, String>>>();
				if (ValidationUtil.isValid(screenSortColumn)) {
					if (screenSortColumn.contains(groupingCols.get(0))) {
						String value = screenSortColumn.substring(9, screenSortColumn.length()).toUpperCase();
						String[] col = value.split(",");
						for (int i = 0; i < col.length; i++) {
							if (col[i].contains(groupingCols.get(0))) {
								String val = col[i];
								if (val.contains("DESC")) {
									sortedMap = new TreeMap<String, List<HashMap<String, String>>>(
											Collections.reverseOrder());
									sortedMap.putAll(groupingMap);
								} else {
									sortedMap = new TreeMap<String, List<HashMap<String, String>>>(groupingMap);
								}
							}
						}
					} else {
						sortedMap = new TreeMap<String, List<HashMap<String, String>>>(groupingMap);
					}
				} else {
					sortedMap = new TreeMap<String, List<HashMap<String, String>>>(groupingMap);
				}

				exceptionCode = pdfExportUtil.exportToPdfRAWithGroup(colHeaderslst, dataLst, reportsVb, assetFolderUrl,
						colTypes, currentUserId, totalLst, sortedMap, columnHeadersFinallst);
			} else {
				exceptionCode = pdfExportUtil.exportToPdfRA(colHeaderslst, dataLst, reportsVb, assetFolderUrl, colTypes,
						currentUserId, totalLst, columnHeadersFinallst);
			}
			// logger.info("Pdf Export Data Write
			// End["+reportsVb.getReportId()+":"+reportsVb.getSubReportId()+"]");
		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}


	public ExceptionCode getTreePromptData(ReportFilterVb vObject) {
		List<PromptTreeVb> promptTree = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<PrdQueryConfig> sourceQuerylst = reportsDao.getSqlQuery(vObject.getFilterSourceId());
			if (sourceQuerylst != null && sourceQuerylst.size() > 0) {
				PrdQueryConfig sourceQueryDet = sourceQuerylst.get(0);
				exceptionCode = reportsDao.getTreePromptData(vObject, sourceQueryDet);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION
						&& exceptionCode.getResponse() != null) {
					promptTree = (List<PromptTreeVb>) exceptionCode.getResponse();
					promptTree = createPraentChildRelations(promptTree, vObject.getFilterString());
					exceptionCode.setResponse(promptTree);
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					exceptionCode.setErrorMsg("successful operation");
				}
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Query not maintained for the DATA_REF_ID " + vObject.getFilterSourceId());
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public List<PromptTreeVb> createPraentChildRelations(List<PromptTreeVb> promptTreeList, String filterString) {
		DeepCopy<PromptTreeVb> deepCopy = new DeepCopy<PromptTreeVb>();
		List<PromptTreeVb> lResult = new ArrayList<PromptTreeVb>(0);
		List<PromptTreeVb> promptTreeListCopy = new CopyOnWriteArrayList<PromptTreeVb>(
				deepCopy.copyCollection(promptTreeList));
		// Top Roots are added.
		for (PromptTreeVb promptVb : promptTreeListCopy) {
			if (promptVb.getField1().equalsIgnoreCase(promptVb.getField3())) {
				lResult.add(promptVb);
				promptTreeListCopy.remove(promptVb);
			}
		}
		// For each top node add all child's and to that child's add sub child's
		// recursively.
		for (PromptTreeVb promptVb : lResult) {
			addChilds(promptVb, promptTreeListCopy);
		}
		// Get the sub tree from the filter string if filter string is not null.
		if (ValidationUtil.isValid(filterString)) {
			lResult = getSubTreeFrom(filterString, lResult);
		}
		// set the empty lists to null. this is required for UI to display the leaf
		// nodes properly.
		nullifyEmptyList(lResult);
		return lResult;
	}

	private void addChilds(PromptTreeVb vObject, List<PromptTreeVb> promptTreeListCopy) {
		for (PromptTreeVb promptTreeVb : promptTreeListCopy) {
			if (vObject.getField1().equalsIgnoreCase(promptTreeVb.getField3())) {
				if (vObject.getChildren() == null) {
					vObject.setChildren(new ArrayList<PromptTreeVb>(0));
				}
				vObject.getChildren().add(promptTreeVb);
				addChilds(promptTreeVb, promptTreeListCopy);
			}
		}
	}

	private List<PromptTreeVb> getSubTreeFrom(String filterString, List<PromptTreeVb> result) {
		List<PromptTreeVb> lResult = new ArrayList<PromptTreeVb>(0);
		for (PromptTreeVb promptTreeVb : result) {
			if (promptTreeVb.getField1().equalsIgnoreCase(filterString)) {
				lResult.add(promptTreeVb);
				return lResult;
			} else if (promptTreeVb.getChildren() != null) {
				lResult = getSubTreeFrom(filterString, promptTreeVb.getChildren());
				if (lResult != null && !lResult.isEmpty())
					return lResult;
			}
		}
		return lResult;
	}

	private void nullifyEmptyList(List<PromptTreeVb> lResult) {
		for (PromptTreeVb promptTreeVb : lResult) {
			if (promptTreeVb.getChildren() != null) {
				nullifyEmptyList(promptTreeVb.getChildren());
			}
			if (promptTreeVb.getChildren() != null && promptTreeVb.getChildren().isEmpty()) {
				promptTreeVb.setChildren(null);
			}
		}
	}

	private ArrayList<ColumnHeadersVb> formColumnHeader(List<ColumnHeadersVb> orgColList, String hiddenColumn) {

		ArrayList<ColumnHeadersVb> updatedColList = new ArrayList<ColumnHeadersVb>();
		int maxHeaderRow = orgColList.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);

		ColumnHeadersVb matchingObj = orgColList.stream()
				.filter(p -> p.getDbColumnName().equalsIgnoreCase(hiddenColumn)).findAny().orElse(null);

		final int hiddenColNum = matchingObj.getLabelColNum();
		final int hiddenRowNum = matchingObj.getLabelRowNum();

		if (maxHeaderRow > 1) {
			int rowNum = hiddenRowNum - 1;
			for (int rnum = rowNum; rowNum >= 1; rowNum--) {
				ColumnHeadersVb Obj = new ColumnHeadersVb();
				try {
					Obj = orgColList.stream()
							.filter(p -> (p.getLabelRowNum() == rnum && p.getLabelColNum() <= hiddenColNum))
							.max(Comparator.comparingInt(ColumnHeadersVb::getLabelColNum)).get();
				} catch (Exception e) {

				}
				for (int i = 0; i < orgColList.size(); i++) {
					if (orgColList.get(i).equals(Obj)) {
						orgColList.get(i).setColspan(orgColList.get(i).getColspan() - 1);
						orgColList.get(i).setNumericColumnNo(99);
						if (orgColList.get(i).getColspan() == 0) {
							orgColList.remove(i);
						}
					}
				}
			}
		}
		for (ColumnHeadersVb colHeaderVb : orgColList) {
			if (colHeaderVb.getLabelColNum() > hiddenColNum) {
				colHeaderVb.setLabelColNum(colHeaderVb.getLabelColNum() - 1);
			}
			if (!colHeaderVb.equals(matchingObj))
				updatedColList.add(colHeaderVb);

		}
		return updatedColList;
	}

	public ExceptionCode getReportFilterSourceValue(ReportFilterVb vObject) {
		// LinkedHashMap<String, String> filterSourceVal = new LinkedHashMap<String,
		// String>();
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			if (!ValidationUtil.isValid(vObject.getDefaultValueId())) {
				vObject.setDefaultValueId(vObject.getFilterSourceId());
			}
			List<PrdQueryConfig> sourceQuerylst = reportsDao.getSqlQuery(vObject.getDefaultValueId());
			if (sourceQuerylst != null && sourceQuerylst.size() > 0) {
				PrdQueryConfig sourceQueryDet = sourceQuerylst.get(0);
				if ("QUERY".equalsIgnoreCase(sourceQueryDet.getDataRefType())) {
					sourceQueryDet.setQueryProc(replaceFilterHashVariables(sourceQueryDet.getQueryProc(), vObject));
					exceptionCode = reportsDao.getReportPromptsFilterValue(sourceQueryDet, null);
				} else if ("PROCEDURE".equalsIgnoreCase(sourceQueryDet.getDataRefType())) {
					exceptionCode = reportsDao.getComboPromptData(vObject, sourceQueryDet);
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

	public ExceptionCode exportReportToCsv(ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = doValidate(vObject);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}
			String reportTitle = "";
			reportTitle = vObject.getReportTitle();
			String[] hiddenColumns = null;
			if (ValidationUtil.isValid(vObject.getColumnsToHide())) {
				hiddenColumns = vObject.getColumnsToHide().split("!@#");
			}
			exceptionCode = getReportDetails(vObject);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					vObject = (ReportsVb) exceptionCode.getResponse();
				}
			} else {
				return exceptionCode;
			}
			List<ColumnHeadersVb> colHeaderslst = vObject.getColumnHeaderslst();
			List<ColumnHeadersVb> updatedLst = new ArrayList();
			List<ColumnHeadersVb> columnHeadersFinallst = new ArrayList<ColumnHeadersVb>();
			colHeaderslst.forEach(colHeadersVb -> {
				if (colHeadersVb.getColspan() <= 1) {
					columnHeadersFinallst.add(colHeadersVb);
				}
			});
			for (ColumnHeadersVb columnHeadersVb : columnHeadersFinallst) {
				if (hiddenColumns != null) {
					for (int ctr = 0; ctr < hiddenColumns.length; ctr++) {
						if (columnHeadersVb.getDbColumnName().equalsIgnoreCase(hiddenColumns[ctr])) {
							updatedLst.add(columnHeadersVb);
							break;
						}
					}
				}
			}

			if (updatedLst != null && !updatedLst.isEmpty()) {
				columnHeadersFinallst.removeAll(updatedLst);
			}
			exceptionCode = reportsDao.getResultData(vObject,true);
			List<HashMap<String, String>> dataLst = null;
			List<HashMap<String, String>> totalLst = null;
			ReportsVb resultVb = new ReportsVb();
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					resultVb = (ReportsVb) exceptionCode.getResponse();
					dataLst = resultVb.getGridDataSet();
					totalLst = resultVb.getTotal();
				}
			}
			vObject.setReportTitle(reportTitle);
			int currentUserId = CustomContextHolder.getContext().getVisionId();
			CreateCsv createCSV = new CreateCsv();
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			FileWriter fw = null;
			fw = new FileWriter(
					filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + currentUserId + ".csv");
			PrintWriter out = new PrintWriter(fw);
			int rowNum = 0;
			int ctr = 1;
			String csvSeperator = "";
			if("\t".equalsIgnoreCase(vObject.getCsvDelimiter()))
				csvSeperator = "	";
			else
				csvSeperator = vObject.getCsvDelimiter();
			rowNum = createCSV.writeHeadersToCsv(columnHeadersFinallst, vObject, fw, rowNum, out,csvSeperator);
			do {
				ctr++;
				rowNum = createCSV.writeDataToCsv(columnHeadersFinallst, dataLst, vObject, currentUserId, fw, rowNum,out,csvSeperator);
				vObject.setCurrentPage(ctr);
				dataLst = new ArrayList();
				exceptionCode = reportsDao.getResultData(vObject,true);
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					resultVb = (ReportsVb) exceptionCode.getResponse();
					dataLst = resultVb.getGridDataSet();
				}
			} while (dataLst != null && !dataLst.isEmpty());
			
			if(totalLst != null)
				rowNum = createCSV.writeDataToCsv(columnHeadersFinallst, totalLst, vObject, currentUserId, fw, rowNum,out,csvSeperator);
			
			out.flush();
			out.close();
			fw.close();
			exceptionCode.setResponse(filePath);
			exceptionCode.setOtherInfo(vObject.getReportTitle() + "_" + currentUserId);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	@SuppressWarnings("unchecked")
	public ExceptionCode getWidgetList() {
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList collTemp = new ArrayList();
		ArrayList<ReportFilterVb> filterLst = new ArrayList<>();
		ReportsVb reportVb = new ReportsVb();
		try {

			String defaultPrompt = commonDao.findVisionVariableValue("PRD_WIDGET_FILTER_" + productName + "");
			ExceptionCode globalFilterExcep = reportFilterProcess(defaultPrompt);
			ArrayList<ReportFilterVb> globalFilterLst = (ArrayList<ReportFilterVb>) globalFilterExcep.getResponse();
			ArrayList<ReportsVb> widgetlst = (ArrayList<ReportsVb>) reportsDao.getWidgetList();
			if (widgetlst != null && widgetlst.size() > 0) {
				for (ReportsVb reportsVb : widgetlst) {
					StringJoiner filterPosition = new StringJoiner("-");
					if ("Y".equalsIgnoreCase(reportsVb.getFilterFlag())) {
						HashMap<String, Object> filterMapVal = new HashMap<String, Object>();
						ExceptionCode exceptionCodeTemp = new ExceptionCode();
						exceptionCodeTemp = reportFilterProcess(reportsVb.getFilterRefCode());
						if (exceptionCodeTemp.getResponse() != null
								&& exceptionCodeTemp.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
							filterLst = (ArrayList<ReportFilterVb>) exceptionCodeTemp.getResponse();
						}
						if (filterLst != null && !filterLst.isEmpty()) {
							filterLst.forEach(vObj -> {
								// Global Filter Identifier Start
								Boolean globalFlag = false;
								List<ReportFilterVb> matchlst = globalFilterLst.stream()
										.filter(n -> n.getFilterSourceId().equalsIgnoreCase(vObj.getFilterSourceId()))
										.collect(Collectors.toList());

								if (matchlst != null && !matchlst.isEmpty()) {
									vObj.setGlobalFilter(true);
								} else {
									vObj.setGlobalFilter(false);
								}
								// End
								if (!vObj.getGlobalFilter()) {
									ExceptionCode exceptionCodeFilter = getFilterSourceValue(vObj);
									if (exceptionCodeFilter.getResponse() != null)
										vObj.setFilterValueMap(
												(LinkedHashMap<String, String>) exceptionCodeFilter.getResponse());
								}
							});
						}
						reportsVb.setFilterPosition(filterPosition.toString());
						List<ReportFilterVb> filterLstNew = filterLst.stream().filter(n -> n.getGlobalFilter() == false)
								.collect(Collectors.toList());

						int ctr = 1;
						for (ReportFilterVb vObj : filterLstNew) {
							vObj.setFilterSeq(ctr);
							ctr++;
						}
						filterLst.forEach(vObj -> {
							// Global Filter and Local Filter Identifier Start
							Boolean globalFlag = false;
							List<ReportFilterVb> matchlst = globalFilterLst.stream()
									.filter(n -> n.getFilterSourceId().equalsIgnoreCase(vObj.getFilterSourceId()))
									.collect(Collectors.toList());

							List<ReportFilterVb> localMatchlst = filterLstNew.stream()
									.filter(n -> n.getFilterSourceId().equalsIgnoreCase(vObj.getFilterSourceId()))
									.collect(Collectors.toList());

							if (matchlst != null && !matchlst.isEmpty()) {
								filterPosition.add("G" + matchlst.get(0).getFilterSeq());
							} else if (localMatchlst != null && !localMatchlst.isEmpty()) {
								filterPosition.add("L" + localMatchlst.get(0).getFilterSeq());
							}
							// End
						});
						reportsVb.setFilterPosition(filterPosition.toString());
						reportsVb.setReportFilters(filterLstNew);
					}
				}
			}
			collTemp.add(widgetlst);
			collTemp.add(defaultPrompt);
			ArrayList userWidgetlst = (ArrayList<ReportsVb>) reportsDao.getUserWidgets();
			collTemp.add(userWidgetlst);
			ArrayList savedUserDefLst = (ArrayList<ReportsVb>) reportsDao.getSavedUserDefSetting(reportVb, false);
			collTemp.add(savedUserDefLst);
			Integer notfiicationCount = reportsDao.getNotificationCount();
			collTemp.add(notfiicationCount);
			exceptionCode.setResponse(collTemp);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode deleteSavedWidget(ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		int retVal = 0;
		try {
			if (reportsDao.userWidgetExists(vObject) > 0) {
				retVal = reportsDao.deleteWidgetExists(vObject);
				if (retVal == Constants.ERRONEOUS_OPERATION) {
					exceptionCode.setErrorCode(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
					exceptionCode.setErrorMsg("Attempt to delete non-existant record!!");
				} else {
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					exceptionCode.setErrorMsg("User Preferance Widget Deleted Successfully");
				}
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode saveWidget(ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			deleteSavedWidget(vObject);
			int retVal = reportsDao.saveWidget(vObject);
			if (retVal == Constants.ERRONEOUS_OPERATION) {
				exceptionCode.setErrorCode(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
				exceptionCode.setErrorMsg("Unable to save widgets.Contact Admin!!");
			} else {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("User Preferance Widget Saved Successfully");
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode exportWidgetToPdf(String reportTitle, String promptLabel, String gridReportIds,
			String fileName, ArrayList<ReportsVb> reportslst, String appTheme, String base64Str) {
		ExceptionCode exceptionCode = new ExceptionCode();
		ReportsVb reportsVb = new ReportsVb();
		VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
		try {
			reportsVb.setMaker(visionUsersVb.getVisionId());
			reportsVb.setMakerName(visionUsersVb.getUserName());
			reportsVb.setReportTitle(reportTitle);
			reportsVb.setPromptLabel(promptLabel);
			reportsVb.setApplicationTheme(appTheme);
			String assetFolderUrl = servletContext.getRealPath("/WEB-INF/classes/images");
			if (ValidationUtil.isValid(gridReportIds)) {
				// Write Code for Grid Export
			}

			String screenCapturedPath = commonDao.findVisionVariableValue("PRD_EXPORT_PATH");
			String capturedImage = screenCapturedPath + fileName + ".png";

			// Converting Base64 to Image file
			String base64ImageString = base64Str.replace("data:image/png;base64,", "");
			byte[] data = DatatypeConverter.parseBase64Binary(base64ImageString);
			File fileImg = new File(capturedImage);
			if (fileImg.exists())
				fileImg.delete();
			fileImg.createNewFile();
			OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileImg));
			outputStream.write(data);
			outputStream.close();

			exceptionCode = pdfExportUtil.dashboardExportToPdf(capturedImage, reportsVb, assetFolderUrl, fileName,
					reportslst);
			File lFile = new File(capturedImage);
			if (lFile.exists()) {
				lFile.delete();
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode saveReportUserDef(ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			deleteSavedReportUserDef(vObject);
			if (vObject.getReportUserDeflst() != null && !vObject.getReportUserDeflst().isEmpty()) {
				vObject.getReportUserDeflst().forEach(userDefVb -> {
					int retVal = reportsDao.saveReportUserDef(userDefVb);
				});
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Widgets saved Successfully");
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode deleteSavedReportUserDef(ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		int retVal = 0;
		try {
			if (vObject.getReportUserDeflst() != null && vObject.getReportUserDeflst().size() > 0) {
				vObject.getReportUserDeflst().forEach(userDefVb -> {
					if (reportsDao.reportUserDefExists(userDefVb) > 0) {
						reportsDao.deleteReportUserDef(userDefVb);
					}
				});
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Widgets deleted Successfully");
			exceptionCode.setOtherInfo(vObject);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode saveReportSettings(ReportUserDefVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			if (reportsDao.reportUserDefExists(vObject) > 0)
				reportsDao.deleteReportUserDef(vObject);
			int retVal = reportsDao.saveReportUserDef(vObject);
			if (retVal == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("Report settings saved Successfully");
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	@Override
	protected ExceptionCode doValidate(ReportsVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("PilotConsole", operation);
		if (!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation + " " + Constants.userRestrictionMsg);
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}

	public ExceptionCode exportToXls(ReportsVb vObject, int currentUserId, String workBookCnt) {
		ExceptionCode exceptionCode = new ExceptionCode();
		int rowNum = 0;
		try {
			exceptionCode = doValidate(vObject);
			if (exceptionCode != null && exceptionCode.getErrorMsg() != "") {
				return exceptionCode;
			}
			String reportTitle = "";
			int screenFreeze = vObject.getFreezeColumn();
			String applicationTheme = vObject.getApplicationTheme();
			reportTitle = vObject.getReportTitle();
			String screenGroupColumn = vObject.getScreenGroupColumn();
			String screenSortColumn = vObject.getScreenSortColumn();
			String[] hiddenColumns = null;
			if (ValidationUtil.isValid(vObject.getColumnsToHide())) {
				hiddenColumns = vObject.getColumnsToHide().split("!@#");
			}
			exceptionCode = getReportDetails(vObject);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					vObject = (ReportsVb) exceptionCode.getResponse();
				}
			} else {
				return exceptionCode;
			}
			List<ColumnHeadersVb> colHeaderslst = vObject.getColumnHeaderslst();
			List<ColumnHeadersVb> updatedLst = vObject.getColumnHeaderslst();
			if (hiddenColumns != null) {
				for (int ctr = 0; ctr < hiddenColumns.length; ctr++) {
					updatedLst = formColumnHeader(updatedLst, hiddenColumns[ctr]);
				}
			}
			if (updatedLst != null && updatedLst.size() > 0) {
				int finalMaxRow = updatedLst.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
				for (ColumnHeadersVb colObj : updatedLst) {
					if (colObj.getRowspan() > finalMaxRow) {
						colObj.setRowspan(colObj.getRowspan() - finalMaxRow);
					}
				}
				colHeaderslst = updatedLst;
			}

			List<String> colTypes = new ArrayList<String>();
			Map<Integer, Integer> columnWidths = new HashMap<Integer, Integer>(colHeaderslst.size());
			List<ReportStgVb> reportsStgs = new ArrayList<ReportStgVb>();
			for (int loopCnt = 0; loopCnt < colHeaderslst.size(); loopCnt++) {
				columnWidths.put(Integer.valueOf(loopCnt), Integer.valueOf(-1));
				ColumnHeadersVb colHVb = colHeaderslst.get(loopCnt);
				if (colHVb.getColspan() <= 1 && colHVb.getNumericColumnNo() != 99) {
					colTypes.add(colHVb.getColType());
				}
			}
			int headerCnt = 0;

			String assetFolderUrl = servletContext.getRealPath("/WEB-INF/classes/images");
			
		/*	if("JBOSS".equalsIgnoreCase(serverType))
				assetFolderUrl = commonDao.findVisionVariableValue("PRD_IMAGES_PATH");*/
			
			// String assetFolderUrl = commonDao.findVisionVariableValue("MDM_IMAGE_PATH");
			// assetFolderUrl = "E:\\RA_Image\\";
			vObject.setReportTitle(reportTitle);
			vObject.setMaker(currentUserId);
			if (ValidationUtil.isValid(screenGroupColumn))
				vObject.setScreenGroupColumn(screenGroupColumn);
			if (ValidationUtil.isValid(screenSortColumn))
				vObject.setScreenSortColumn(screenSortColumn);
			getScreenDao().fetchMakerVerifierNames(vObject);
			if (ValidationUtil.isValid(applicationTheme))
				vObject.setApplicationTheme(applicationTheme);
			if (screenFreeze != 0) {
				vObject.setFreezeColumn(screenFreeze);
			}
			// ExcelExportUtil.createPrompts(vObject, new ArrayList(), sheet, workBook,
			// assetFolderUrl, styls, headerCnt);
			boolean createHeadersAndFooters = true;
			// Excel Report Header
			/// rowNum = ExcelExportUtil.writeHeadersRA(vObject, colHeaderslst, rowNum,
			// sheet, styls, colTypes,columnWidths);
			// logger.info("Excel Export Data Extraction
			// Begin["+vObject.getReportId()+":"+vObject.getSubReportId()+"]");
			exceptionCode = reportsDao.getResultData(vObject, true);
			// logger.info("Excel Export Data Extraction End["+
			// vObject.getReportId()+":"+vObject.getSubReportId()+"]");
			List<HashMap<String, String>> dataLst = null;
			List<HashMap<String, String>> totalLst = null;
			ReportsVb resultVb = new ReportsVb();
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					resultVb = (ReportsVb) exceptionCode.getResponse();
					dataLst = resultVb.getGridDataSet();
					totalLst = resultVb.getTotal();
				}
			}
			// logger.info("Excel Export Data Write Begin["+
			// vObject.getReportId()+":"+vObject.getSubReportId()+"]");
			SXSSFWorkbook workBook = new SXSSFWorkbook(500);
			String sheetName = vObject.getReportTitle().trim();
			// sheetName = WorkbookUtil.createSafeSheetName(sheetName);
			// vObject.setReportTitle(sheetName);
			SXSSFSheet sheet = (SXSSFSheet) workBook.createSheet("Summary");

			// SXSSFSheet sheet = (SXSSFSheet) workBook.getSheet(vObject.getReportTitle());
			Map<Integer, XSSFCellStyle> styls = ExcelExportUtil.createStyles(workBook, applicationTheme);
			ExcelExportUtil.createPromptsPage(vObject, sheet, workBook, assetFolderUrl, styls, headerCnt);
			sheet = (SXSSFSheet) workBook.createSheet("Report");
			int ctr = 1;
			int sheetCnt = 3;
			do {
				if ((rowNum + vObject.getMaxRecords()) > SpreadsheetVersion.EXCEL2007.getMaxRows()) {
					rowNum = 0;
					sheet = (SXSSFSheet) workBook.createSheet("" + sheetCnt);
					sheetCnt++;
					createHeadersAndFooters = true;
				}
				if (createHeadersAndFooters) {
					rowNum = ExcelExportUtil.writeHeadersRA(vObject, colHeaderslst, rowNum, sheet, styls, colTypes,
							columnWidths);
					sheet.createFreezePane(vObject.getFreezeColumn(), rowNum);
				}
				createHeadersAndFooters = false;
				// writing data into excel
				ctr++;
				rowNum = ExcelExportUtil.writeReportDataRA(workBook, vObject, colHeaderslst, dataLst, sheet, rowNum,
						styls, colTypes, columnWidths, false, assetFolderUrl);
				vObject.setCurrentPage(ctr);
				dataLst = new ArrayList();
				exceptionCode = reportsDao.getResultData(vObject, true);
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					resultVb = (ReportsVb) exceptionCode.getResponse();
					dataLst = resultVb.getGridDataSet();
				}
			} while (dataLst != null && !dataLst.isEmpty());

			if (totalLst != null)
				rowNum = ExcelExportUtil.writeReportDataRA(workBook, vObject, colHeaderslst, totalLst, sheet, rowNum,
						styls, colTypes, columnWidths, true, assetFolderUrl);

			headerCnt = colTypes.size();
			int noOfSheets = workBook.getNumberOfSheets();
			for (int a = 1; a < noOfSheets; a++) {
				sheet = (SXSSFSheet) workBook.getSheetAt(a);
				int loopCount = 0;
				for (loopCount = 0; loopCount < headerCnt; loopCount++) {
					sheet.setColumnWidth(loopCount, columnWidths.get(loopCount));
				}
			}
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			File lFile = new File(
					filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + currentUserId + ".xlsx");
			if (lFile.exists()) {
				lFile.delete();
			}
			lFile.createNewFile();
			FileOutputStream fileOS = new FileOutputStream(lFile);
			workBook.write(fileOS);
			fileOS.flush();
			fileOS.close();
			// logger.info("Excel Export Data Write
			// End["+vObject.getReportId()+":"+vObject.getSubReportId()+"]");
			exceptionCode.setResponse(filePath);
			exceptionCode.setOtherInfo(vObject.getReportTitle() + "_" + currentUserId);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
				logger.error("Report Export Excel Exception at: " + vObject.getReportId() + " : "
						+ vObject.getReportTitle());
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCode;
	}
	public String replacePromptValues(String promptVal,ReportsVb vObj) {
		String promptValue = "";
		try {
			switch (promptVal) {
			case "#PROMPT_VALUE_1#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue1()) ? vObj.getPromptValue1().replaceAll("'", "") : "";
				break;
			case "#PROMPT_VALUE_2#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue2()) ? vObj.getPromptValue2().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_3#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue3()) ? vObj.getPromptValue3().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_4#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue4()) ? vObj.getPromptValue4().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_5#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue5()) ? vObj.getPromptValue5().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_6#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue6()) ? vObj.getPromptValue6().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_7#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue7()) ? vObj.getPromptValue7().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_8#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue8()) ? vObj.getPromptValue8().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_9#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue9()) ? vObj.getPromptValue9().replaceAll("'", "") : "";break;
			case "#PROMPT_VALUE_10#":
				promptValue = ValidationUtil.isValid(vObj.getPromptValue10()) ? vObj.getPromptValue10().replaceAll("'", "") : "";break;
			}
		} catch (Exception e) {

		}
		return promptValue;
	}
	
	
}