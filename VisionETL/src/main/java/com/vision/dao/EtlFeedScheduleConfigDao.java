package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ETLFeedTimeBasedInclutionsPropVb;
import com.vision.vb.ETLFeedTimeBasedPropVb;
import com.vision.vb.EtlFeedAlertConfigVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedScheduleConfigVb;

@Component
public class EtlFeedScheduleConfigDao extends AbstractDao<EtlFeedScheduleConfigVb> {
	
	@Autowired
	EtlFeedAlertconfigDao etlFeedAlertconfigDao;

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";

	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String ScheduleTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2107, "TAppr.SCHEDULE_TYPE", "SCHEDULE_TYPE_DESC");
	String ScheduleTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2107, "TPend.SCHEDULE_TYPE", "SCHEDULE_TYPE_DESC");

	String AlertTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2140, "TAppr.ALERT_TYPE", "ALERT_TYPE_DESC");
	String AlertTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2140, "TPend.ALERT_TYPE", "ALERT_TYPE_DESC");

	String ProcessDateTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2108, "TAppr.PROCESS_DATE_TYPE", "PROCESS_DATE_TYPE_DESC");
	String ProcessDateTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2108, "TPend.PROCESS_DATE_TYPE", "PROCESS_DATE_TYPE_DESC");

	String ProcessTableStatusAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2119, "TAppr.PROCESS_TBL_STATUS", "PROCESS_TBL_STATUS_DESC");
	String ProcessTableStatusAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2119, "TPend.PROCESS_TBL_STATUS", "PROCESS_TBL_STATUS_DESC");

	String ScheduleTimeZoneAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2109, "TAppr.SCHEDULE_TIME_ZONE", "SCHEDULE_TIME_ZONE_DESC");
	String ScheduleTimeZoneAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2109, "TPend.SCHEDULE_TIME_ZONE", "SCHEDULE_TIME_ZONE_DESC");

	String EtlScheduleStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.ETL_SCHEDULE_STATUS", "ETL_SCHEDULE_STATUS_DESC");
	String EtlScheduleStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.ETL_SCHEDULE_STATUS", "ETL_SCHEDULE_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	
	String NxtDateAutomationTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2124, "TAppr.NXT_DATE_AUTOMATION_TYPE", "NXT_DATE_AUTOMATION_TYPE_DESC");
	String NxtDateAutomationTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2124, "TPend.NXT_DATE_AUTOMATION_TYPE", "NXT_DATE_AUTOMATION_TYPE_DESC");

	protected RowMapper getLocalMapper(String tableMap){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedScheduleConfigVb vObject = new EtlFeedScheduleConfigVb();
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
				if (rs.getString("SCHEDULE_DESCRIPTION") != null) {
					vObject.setScheduleDescription(rs.getString("SCHEDULE_DESCRIPTION"));
				} else {
					vObject.setScheduleDescription("");
				}
				vObject.setScheduleTypeAt(rs.getInt("SCHEDULE_TYPE_AT"));
				if (rs.getString("SCHEDULE_TYPE") != null) {
					vObject.setScheduleType(rs.getString("SCHEDULE_TYPE"));
				} else {
					vObject.setScheduleType("");
				}
				vObject.setSkipHolidayFlag(rs.getString("SKIP_HOLIDAY_FLAG"));
				vObject.setAlertTypeAt(rs.getInt("ALERT_TYPE_AT"));
				if (rs.getString("ALERT_TYPE") != null) {
					vObject.setAlertType(rs.getString("ALERT_TYPE"));
				} else {
					vObject.setAlertType("");
				}
				vObject.setChannelTypeSMS(rs.getString("CHANNEL_TYPE_SMS"));
				vObject.setChannelTypeEmail(rs.getString("CHANNEL_TYPE_EMAIL"));
				vObject.setAlertFlag(rs.getString("ALERT_FLAG"));

				if(rs.getString("SCHEDULE_PROP_CONTEXT")!= null){ 
					vObject.setSchedulePropContext(rs.getString("SCHEDULE_PROP_CONTEXT"));
				}else{
					vObject.setSchedulePropContext("");
				}
				if ("F".equalsIgnoreCase(vObject.getAlertType())) {
					EtlFeedAlertConfigVb vObj = new EtlFeedAlertConfigVb();
					vObj.setCountry(vObject.getCountry());
					vObj.setLeBook(vObject.getLeBook());
					vObj.setFeedId(vObject.getFeedId());
					int intStatus = 1;
					if ("UPL".equalsIgnoreCase(tableMap)) {
						intStatus = 9999;
					} else if ("MAIN".equalsIgnoreCase(tableMap)) {
						intStatus = 0;
					}
					List<EtlFeedAlertConfigVb> etlFeedAlertConfigLst = etlFeedAlertconfigDao.getQueryResults(vObj, intStatus);
					vObject.setEtlFeedAlertConfiglst(etlFeedAlertConfigLst);
				}
				
				if(ValidationUtil.isValid(rs.getString("FEED_ID")) && ValidationUtil.isValid(rs.getString("FEED_ID"))) {
					String check ="<root>";
					List<EtlFeedScheduleConfigVb> depFeed =getSavedDependencyFeed(vObject, tableMap);
					if(depFeed!=null && depFeed.size()>0) {
						check = check+"<dependency>";
						for(int k=0; k<depFeed.size();k++) {
							String tableDep = "ETL_FEED_MAIN";
							List<EtlFeedScheduleConfigVb> cate =getSavedDependencyFeedCategory(vObject, tableDep,depFeed.get(k).getFeedId());
							if(cate.isEmpty() || cate.size()==0) {
								tableDep = "ETL_FEED_MAIN_PEND";
								cate =getSavedDependencyFeedCategory(vObject, tableDep,depFeed.get(k).getFeedId());
							}
							if(cate.isEmpty() || cate.size()==0) {
								tableDep = "ETL_FEED_MAIN_UPL";
								cate =getSavedDependencyFeedCategory(vObject, tableDep,depFeed.get(k).getFeedId());
							}
							check = check+"<feed><country>"+vObject.getCountry()+"</country><lebook>"+vObject.getLeBook()+"</lebook><category>"+cate.get(0).getFeedCategory()+"</category><desc>"+cate.get(0).getScheduleDescription()+"</desc><id>"+depFeed.get(k).getFeedId()+"</id></feed>";
							depFeed.get(k);
						}
						check = check+"</dependency>";
					}
					check =check+"</root>";
					vObject.setDependencyFeedContext(check);
				}

				if (rs.getString("SCHEDULE_PROP_CONTEXT") != null) {
					vObject.setSchedulePropContext(rs.getString("SCHEDULE_PROP_CONTEXT"));
				} else {
					vObject.setSchedulePropContext("");
				}

				vObject.setProcessDateTypeAt(rs.getInt("PROCESS_DATE_TYPE_AT"));
				if (rs.getString("PROCESS_DATE_TYPE") != null) {
					vObject.setProcessDateType(rs.getString("PROCESS_DATE_TYPE"));
				} else {
					vObject.setProcessDateType("");
				}
				vObject.setProcessTableStatusAT(rs.getInt("PROCESS_TBL_STATUS_AT"));
				if (rs.getString("PROCESS_TBL_STATUS") != null) {
					vObject.setProcessTableStatus(rs.getString("PROCESS_TBL_STATUS"));
				} else {
					vObject.setProcessTableStatus("");
				}
				vObject.setNextScheduleDate(rs.getLong("NEXT_SCHEDULE_DATE"));
				vObject.setNxtDateAutomationType(rs.getString("NXT_DATE_AUTOMATION_TYPE"));
				vObject.setEtlScheduleStatusNt(rs.getInt("ETL_SCHEDULE_STATUS_NT"));
				vObject.setEtlScheduleStatus(rs.getInt("ETL_SCHEDULE_STATUS"));
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
				if (ValidationUtil.isValid(rs.getString("START_DATE"))
						&& !("01-Jan-1900".equalsIgnoreCase(rs.getString("START_DATE")))) {
					vObject.setStartDate(rs.getString("START_DATE"));
				}
				if (ValidationUtil.isValid(rs.getString("END_DATE"))
						&& !("01-Jan-1900".equalsIgnoreCase(rs.getString("END_DATE")))) {
					vObject.setEndDate(rs.getString("END_DATE"));
				}
				return vObject;
			}
		};
		return mapper;
	}
	@SuppressWarnings("unchecked")
	public List<EtlFeedScheduleConfigVb> getSavedDependencyFeed(EtlFeedScheduleConfigVb vObject,String tableName) throws DataAccessException {
		String tableDep = "ETL_FEED_DEPENDENCIES";
		if("PEND".equalsIgnoreCase(tableName)) {
			tableDep = "ETL_FEED_DEPENDENCIES_PEND";
		} else if("UPL".equalsIgnoreCase(tableName)) {
			tableDep = "ETL_FEED_DEPENDENCIES_UPL";
		}
		String sql = "SELECT t1.COUNTRY, t1.LE_BOOK, t1.dependent_feed_id feeD_id FROM "+tableDep+" t1" + 
				" where t1.COUNTRY ='"+vObject.getCountry()+"' AND t1.LE_BOOK ='"+vObject.getLeBook()+"' AND  t1.FEED_ID='"+vObject.getFeedId()+"'";
		return  getJdbcTemplate().query(sql,getDependencyFeedMapper());
	}
	
	public List<EtlFeedScheduleConfigVb> getSavedDependencyFeedCategory(EtlFeedScheduleConfigVb vObject,String tableName, String feedId){
			String sql= "select FEED_CATEGORY, feed_name from "+tableName+" t1"+
					" where  t1.COUNTRY ='"+vObject.getCountry()+"' AND t1.LE_BOOK ='"+vObject.getLeBook()+"' AND  t1.FEED_ID='"+feedId+"'";
			return  getJdbcTemplate().query(sql,getDependencyFeedCatMapper());
		}
		
	protected RowMapper getDependencyFeedCatMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedScheduleConfigVb vObject = new EtlFeedScheduleConfigVb();
				vObject.setFeedCategory(rs.getString("FEED_CATEGORY"));
				vObject.setScheduleDescription(rs.getString("feed_name"));
				return vObject;
			}
		};
		return mapper;
	}
	
	protected RowMapper getDependencyFeedMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedScheduleConfigVb vObject = new EtlFeedScheduleConfigVb();
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setFeedId(rs.getString("FEED_ID"));
				return vObject;
			}
		};
		return mapper;
	}
	
/*******Mapper End**********/
	public List<EtlFeedScheduleConfigVb> getQueryPopupResults(EtlFeedScheduleConfigVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.SCHEDULE_DESCRIPTION"
			+ ",TAppr.SCHEDULE_TYPE_AT"
			+ ",TAppr.SCHEDULE_TYPE"
			+ ",TAppr.ALERT_TYPE_AT"
			+ ",TAppr.ALERT_TYPE"
			+ ",TAPPR.CHANNEL_TYPE_SMS"
			+ ",TAPPR.CHANNEL_TYPE_EMAIL"
			+ ",TAPPR.ALERT_FLAG"
			+ ",TAPPR.SKIP_HOLIDAY_FLAG"
			+ ",TAppr.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TAppr.PROCESS_DATE_TYPE_AT"
			+ ",TAppr.PROCESS_DATE_TYPE"
			+ ",TAppr.PROCESS_TBL_STATUS_AT"
			+ ",TAppr.PROCESS_TBL_STATUS"
			+ ",TAppr.NEXT_SCHEDULE_DATE"
			+ ",TAppr.NXT_DATE_AUTOMATION_TYPE"
			+ ",TAppr.ETL_SCHEDULE_STATUS_NT"
			+ ",TAppr.ETL_SCHEDULE_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"
			+ " from ETL_FEED_SCHEDULE_CONFIG TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_SCHEDULE_CONFIG_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.SCHEDULE_DESCRIPTION"
			+ ",TPend.SCHEDULE_TYPE_AT"
			+ ",TPend.SCHEDULE_TYPE"
			+ ",TPend.ALERT_TYPE_AT"
			+ ",TPend.ALERT_TYPE"
			+ ",TPend.CHANNEL_TYPE_SMS"
			+ ",TPend.CHANNEL_TYPE_EMAIL"
			+ ",TPend.ALERT_FLAG"
			+ ",TPend.SKIP_HOLIDAY_FLAG"
			+ ",TPend.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TPend.PROCESS_DATE_TYPE_AT"
			+ ",TPend.PROCESS_DATE_TYPE"
			+ ",TPend.PROCESS_TBL_STATUS_AT"
			+ ",TPend.PROCESS_TBL_STATUS"
			+ ",TPend.NEXT_SCHEDULE_DATE"
			+ ",TPend.NXT_DATE_AUTOMATION_TYPE"
			+ ",TPend.ETL_SCHEDULE_STATUS_NT"
			+ ",TPend.ETL_SCHEDULE_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"
			+ " from ETL_FEED_SCHEDULE_CONFIG_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.SCHEDULE_DESCRIPTION"
			+ ",TUpl.SCHEDULE_TYPE_AT"
			+ ",TUpl.SCHEDULE_TYPE"
			+ ",TUpl.ALERT_TYPE_AT"
			+ ",TUpl.ALERT_TYPE"
			+ ",TUpl.CHANNEL_TYPE_SMS"
			+ ",TUpl.CHANNEL_TYPE_EMAIL"
			+ ",TUpl.ALERT_FLAG"
			+ ",TUpl.SKIP_HOLIDAY_FLAG"
			+ ",TUpl.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TUpl.PROCESS_DATE_TYPE_AT"
			+ ",TUpl.PROCESS_DATE_TYPE"
			+ ",TUpl.PROCESS_TBL_STATUS_AT"
			+ ",TUpl.PROCESS_TBL_STATUS"
			+ ",TUpl.NEXT_SCHEDULE_DATE"
			+ ",TUpl.NXT_DATE_AUTOMATION_TYPE"
			+ ",TUpl.ETL_SCHEDULE_STATUS_NT"
			+ ",TUpl.ETL_SCHEDULE_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"
			+" from ETL_FEED_SCHEDULE_CONFIG_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_SCHEDULE_CONFIG_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_SCHEDULE_CONFIG_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID) ");
		try
			{
				if (ValidationUtil.isValid(dObj.getCountry())){
						params.addElement(dObj.getCountry());
						CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getLeBook())){
						params.addElement(dObj.getLeBook());
						CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedId())){
						params.addElement(dObj.getFeedId());
						CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
				}
/*				if (ValidationUtil.isValid(dObj.getScheduleDescription())){
						params.addElement(dObj.getScheduleDescription());
						CommonUtils.addToQuery("UPPER(TAppr.SCHEDULE_DESCRIPTION) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.SCHEDULE_DESCRIPTION) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.SCHEDULE_DESCRIPTION) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getScheduleType())){
						params.addElement(dObj.getScheduleType());
						CommonUtils.addToQuery("UPPER(TAppr.SCHEDULE_TYPE) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.SCHEDULE_TYPE) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.SCHEDULE_TYPE) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getSchedulePropContext())){
						params.addElement(dObj.getSchedulePropContext());
						CommonUtils.addToQuery("UPPER(TAppr.SCHEDULE_PROP_CONTEXT) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.SCHEDULE_PROP_CONTEXT) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.SCHEDULE_PROP_CONTEXT) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getProcessDateType())){
						params.addElement(dObj.getProcessDateType());
						CommonUtils.addToQuery("UPPER(TAppr.PROCESS_DATE_TYPE) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.PROCESS_DATE_TYPE) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.PROCESS_DATE_TYPE) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getScheduleTimeZone())){
						params.addElement(dObj.getScheduleTimeZone());
						CommonUtils.addToQuery("UPPER(TAppr.SCHEDULE_TIME_ZONE) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.SCHEDULE_TIME_ZONE) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.SCHEDULE_TIME_ZONE) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getEtlScheduleStatus())){
						params.addElement(dObj.getEtlScheduleStatus());
						CommonUtils.addToQuery("TAppr.ETL_SCHEDULE_STATUS = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.ETL_SCHEDULE_STATUS = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.ETL_SCHEDULE_STATUS = ?", strBufUpl);
				}
				if (dObj.getRecordIndicator() != -1){
					if (dObj.getRecordIndicator() > 3){
						params.addElement(new Integer(0));
						CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
						CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
						CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufUpl);
					}else{
						 params.addElement(new Integer(dObj.getRecordIndicator()));
						 CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
						 CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
						 CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufUpl);
					}
				}*/
			String orderBy=" Order By COUNTRY, LE_BOOK, FEED_ID ";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);
			StringBuffer lPageQuery = new StringBuffer();
			if (dObj.isVerificationRequired())
				lPageQuery.append(strBufPending.toString() + " Union " + strBufUpl.toString());
			else
				lPageQuery.append(strBufApprove.toString() + " Union " + strBufUpl.toString());
			return getQueryPopupResultsPgn(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,
					getMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<EtlFeedScheduleConfigVb> getQueryResults(EtlFeedScheduleConfigVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedScheduleConfigVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.SCHEDULE_DESCRIPTION"
			+ ",TAppr.SCHEDULE_TYPE_AT"
			+ ",TAppr.SCHEDULE_TYPE"
			+ ",TAppr.ALERT_TYPE_AT"
			+ ",TAppr.ALERT_TYPE"
			+ ",TAppr.CHANNEL_TYPE_SMS"
			+ ",TAppr.CHANNEL_TYPE_EMAIL"
			+ ",TAppr.ALERT_FLAG"
			+ ",TAppr.SKIP_HOLIDAY_FLAG"
			+ ",TAppr.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TAppr.PROCESS_DATE_TYPE_AT"
			+ ",TAppr.PROCESS_DATE_TYPE"
			+ ",TAppr.PROCESS_TBL_STATUS_AT"
			+ ",TAppr.PROCESS_TBL_STATUS"
			+ ",TAppr.NEXT_SCHEDULE_DATE"
			+ ",TAppr.NXT_DATE_AUTOMATION_TYPE"
			+ ",TAppr.ETL_SCHEDULE_STATUS_NT"
			+ ",TAppr.ETL_SCHEDULE_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"
			+" From ETL_FEED_SCHEDULE_CONFIG TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.SCHEDULE_DESCRIPTION"
			+ ",TPend.SCHEDULE_TYPE_AT"
			+ ",TPend.SCHEDULE_TYPE"
			+ ",TPend.ALERT_TYPE_AT"
			+ ",TPend.ALERT_TYPE"
			+ ",TPend.CHANNEL_TYPE_SMS"
			+ ",TPend.CHANNEL_TYPE_EMAIL"
			+ ",TPend.ALERT_FLAG"
			+ ",TPend.SKIP_HOLIDAY_FLAG"
			+ ",TPend.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TPend.PROCESS_DATE_TYPE_AT"
			+ ",TPend.PROCESS_DATE_TYPE"
			+ ",TPend.PROCESS_TBL_STATUS_AT"
			+ ",TPend.PROCESS_TBL_STATUS"
			+ ",TPend.NEXT_SCHEDULE_DATE"
			+ ",TPend.NXT_DATE_AUTOMATION_TYPE"
			+ ",TPend.ETL_SCHEDULE_STATUS_NT"
			+ ",TPend.ETL_SCHEDULE_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"
			+" From ETL_FEED_SCHEDULE_CONFIG_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.SCHEDULE_DESCRIPTION"
			+ ",TUpl.SCHEDULE_TYPE_AT"
			+ ",TUpl.SCHEDULE_TYPE"
			+ ",TUpl.ALERT_TYPE_AT"
			+ ",TUpl.ALERT_TYPE"
			+ ",TUpl.CHANNEL_TYPE_SMS"
			+ ",TUpl.CHANNEL_TYPE_EMAIL"
			+ ",TUpl.ALERT_FLAG"
			+ ",TUpl.SKIP_HOLIDAY_FLAG"
			+ ",TUpl.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TUpl.PROCESS_DATE_TYPE_AT"
			+ ",TUpl.PROCESS_DATE_TYPE"
			+ ",TUpl.PROCESS_TBL_STATUS_AT"
			+ ",TUpl.PROCESS_TBL_STATUS"
			+ ",TUpl.NEXT_SCHEDULE_DATE"
			+ ",TUpl.NXT_DATE_AUTOMATION_TYPE"
			+ ",TUpl.ETL_SCHEDULE_STATUS_NT"
			+ ",TUpl.ETL_SCHEDULE_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"
			+" From ETL_FEED_SCHEDULE_CONFIG_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper());
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapper());
			}
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	
	public List<EtlFeedScheduleConfigVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedScheduleConfigVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.SCHEDULE_DESCRIPTION"
			+ ",TAppr.SCHEDULE_TYPE_AT"
			+ ",TAppr.SCHEDULE_TYPE"
			+ ",TAppr.ALERT_TYPE_AT"
			+ ",TAppr.ALERT_TYPE"
			+ ",TAppr.CHANNEL_TYPE_SMS"
			+ ",TAppr.CHANNEL_TYPE_EMAIL"
			+ ",TAppr.ALERT_FLAG"
			+ ",TAppr.SKIP_HOLIDAY_FLAG"
			+ ",TAppr.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TAppr.PROCESS_DATE_TYPE_AT"
			+ ",TAppr.PROCESS_DATE_TYPE"
			+ ",TAppr.PROCESS_TBL_STATUS_AT"
			+ ",TAppr.PROCESS_TBL_STATUS"
			+ ",TAppr.NEXT_SCHEDULE_DATE"
			+ ",TAppr.NXT_DATE_AUTOMATION_TYPE"
			+ ",TAppr.ETL_SCHEDULE_STATUS_NT"
			+ ",TAppr.ETL_SCHEDULE_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"			
			+" From ETL_FEED_SCHEDULE_CONFIG TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.SCHEDULE_DESCRIPTION"
			+ ",TPend.SCHEDULE_TYPE_AT"
			+ ",TPend.SCHEDULE_TYPE"
			+ ",TPend.ALERT_TYPE_AT"
			+ ",TPend.ALERT_TYPE"
			+ ",TPend.CHANNEL_TYPE_SMS"
			+ ",TPend.CHANNEL_TYPE_EMAIL"
			+ ",TPend.ALERT_FLAG"
			+ ",TPend.SKIP_HOLIDAY_FLAG"
			+ ",TPend.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TPend.PROCESS_DATE_TYPE_AT"
			+ ",TPend.PROCESS_DATE_TYPE"
			+ ",TPend.PROCESS_TBL_STATUS_AT"
			+ ",TPend.PROCESS_TBL_STATUS"
			+ ",TPend.NEXT_SCHEDULE_DATE"
			+ ",TPend.NXT_DATE_AUTOMATION_TYPE"
			+ ",TPend.ETL_SCHEDULE_STATUS_NT"
			+ ",TPend.ETL_SCHEDULE_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"			
			+" From ETL_FEED_SCHEDULE_CONFIG_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.SCHEDULE_DESCRIPTION"
			+ ",TUpl.SCHEDULE_TYPE_AT"
			+ ",TUpl.SCHEDULE_TYPE"
			+ ",TUpl.ALERT_TYPE_AT"
			+ ",TUpl.ALERT_TYPE"
			+ ",TUpl.CHANNEL_TYPE_SMS"
			+ ",TUpl.CHANNEL_TYPE_EMAIL"
			+ ",TUpl.ALERT_FLAG"
			+ ",TUpl.SKIP_HOLIDAY_FLAG"
			+ ",TUpl.SCHEDULE_PROP_CONTEXT SCHEDULE_PROP_CONTEXT"
			+ ",TUpl.PROCESS_DATE_TYPE_AT"
			+ ",TUpl.PROCESS_DATE_TYPE"
			+ ",TUpl.PROCESS_TBL_STATUS_AT"
			+ ",TUpl.PROCESS_TBL_STATUS"
			+ ",TUpl.NEXT_SCHEDULE_DATE"
			+ ",TUpl.NXT_DATE_AUTOMATION_TYPE"
			+ ",TUpl.ETL_SCHEDULE_STATUS_NT"
			+ ",TUpl.ETL_SCHEDULE_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.START_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') START_DATE"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.END_DATE, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+"') END_DATE"				
			+" From ETL_FEED_SCHEDULE_CONFIG_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getLocalMapper("UPL"));
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getLocalMapper("MAIN"));
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getLocalMapper("PEND"));
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
	protected List<EtlFeedScheduleConfigVb> selectApprovedRecord(EtlFeedScheduleConfigVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlFeedScheduleConfigVb> doSelectPendingRecord(EtlFeedScheduleConfigVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<EtlFeedScheduleConfigVb> doSelectUplRecord(EtlFeedScheduleConfigVb vObject){
		return getQueryResults(vObject, 9999);
	}


	@Override
	protected int getStatus(EtlFeedScheduleConfigVb records){return records.getEtlScheduleStatus();}


	@Override
	protected void setStatus(EtlFeedScheduleConfigVb vObject,int status){vObject.setEtlScheduleStatus(status);}

	  public static String convert(String json, String root) throws JSONException {
	      JSONObject jsonObject = new JSONObject(json);
	      String xml = "<"+root+">" + XML.toString(jsonObject) + "</"+root+">";
	      return xml;
	   }
 
	@Override
	protected int doInsertionAppr(EtlFeedScheduleConfigVb vObject) {
		if (ValidationUtil.isValid(vObject.getSchedulePropContext())
				&& !vObject.getSchedulePropContext().contains("<root>")) {
			String xml = convert(vObject.getSchedulePropContext(), "root"); // This method converts json object to xml
																			// string
			vObject.setSchedulePropContext(xml);
		}
		if (ValidationUtil.isValid(vObject.getScheduleType()) && vObject.getScheduleType().equalsIgnoreCase("TIME")) {
			if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
				Long nxtSDate = calculateNxtScheduleDate(vObject.getCountry(), vObject.getLeBook(),
						vObject.getSchedulePropContext(), getTimeZone(vObject.getCountry(), vObject.getLeBook()),
						vObject.getStartDate(), vObject.getEndDate());
				if (ValidationUtil.isValid(nxtSDate)) {
					vObject.setNextScheduleDate(nxtSDate);
				}
			}
		}
		String query = "Insert Into ETL_FEED_SCHEDULE_CONFIG (SKIP_HOLIDAY_FLAG, ALERT_FLAG, COUNTRY, LE_BOOK, FEED_ID, SCHEDULE_DESCRIPTION, SCHEDULE_TYPE_AT, "
				+ " SCHEDULE_TYPE, ALERT_TYPE,CHANNEL_TYPE_SMS, CHANNEL_TYPE_EMAIL ,PROCESS_DATE_TYPE_AT, PROCESS_DATE_TYPE,   "
				+ " ETL_SCHEDULE_STATUS_NT, ETL_SCHEDULE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ " PROCESS_TBL_STATUS_AT, PROCESS_TBL_STATUS, NEXT_SCHEDULE_DATE,NXT_DATE_AUTOMATION_TYPE, START_DATE, END_DATE, SCHEDULE_PROP_CONTEXT)"
				+ "Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null)
				+ ",?,?,?,?,?,?,?)";

		Object[] args = { vObject.getSkipHolidayFlag(), vObject.getAlertFlag(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getScheduleDescription(), vObject.getScheduleTypeAt(),
				vObject.getScheduleType(), vObject.getAlertType(), vObject.getChannelTypeSMS(),
				vObject.getChannelTypeEmail(), vObject.getProcessDateTypeAt(), vObject.getProcessDateType(),
				vObject.getEtlScheduleStatusNt(), vObject.getEtlScheduleStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getProcessTableStatusAT(), vObject.getProcessTableStatus(), vObject.getNextScheduleDate(),
				vObject.getNxtDateAutomationType(), vObject.getStartDate(), vObject.getEndDate() };

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
					String clobData = ValidationUtil.isValid(vObject.getSchedulePropContext())
							? vObject.getSchedulePropContext()
							: "";
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
	protected int deleteFeedDependencyRecord(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_DEPENDENCIES Where   COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int deleteFeedDependencyPendRecord(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_DEPENDENCIES_PEND Where   COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int deleteFeedDependencyUplRecord(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_FEED_DEPENDENCIES_UPL Where   COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int doInsertionFeedDependency(EtlFeedMainVb vObject) {
		String query = "Insert Into ETL_FEED_DEPENDENCIES ( COUNTRY, LE_BOOK, FEED_ID, DEPENDENT_FEED_ID, FEED_DEP_STATUS_NT, "
				+ "FEED_DEP_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?,"+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")";
		Object[] args = {vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId(),
				vObject.getDependencyFeedId(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);	
	}
	protected int doInsertionFeedDependencyUpl(EtlFeedMainVb vObject) {
		String query = "Insert Into ETL_FEED_DEPENDENCIES_UPL ( COUNTRY, LE_BOOK, FEED_ID, DEPENDENT_FEED_ID, FEED_DEP_STATUS_NT, "
				+ "FEED_DEP_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?,"+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")";
		Object[] args = {vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId(),
				vObject.getDependencyFeedId(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);	
	}

	protected int doInsertionFeedDependencyPend(EtlFeedMainVb vObject) {
		String query = "Insert Into ETL_FEED_DEPENDENCIES_PEND ( COUNTRY, LE_BOOK, FEED_ID, DEPENDENT_FEED_ID, FEED_DEP_STATUS_NT, "
				+ "FEED_DEP_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?,"+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")";
		Object[] args = {vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId(),
				vObject.getDependencyFeedId(), vObject.getFeedStatusNt(), vObject.getFeedStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);	
	}
	protected int doUpdateFeedDependencyPend(EtlFeedMainVb vObject) {
		int retVal = 0;
		String query = "Update ETL_FEED_DEPENDENCIES_PEND Set  FEED_DEP_STATUS_NT = ?, FEED_DEP_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND DEPENDENT_FEED_ID = ?";
		Object[] args = { vObject.getFeedStatusNt(), vObject.getFeedStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getDependencyFeedId()};
		try {
			retVal = getJdbcTemplate().update(query, args);
			if (retVal == 0) {
				retVal = doInsertionFeedDependencyPend(vObject);
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return retVal;
	}
	protected int doUpdateFeedDependency(EtlFeedMainVb vObject) {
		int retVal = 0;
		String query = "Update ETL_FEED_DEPENDENCIES Set  FEED_DEP_STATUS_NT = ?, FEED_DEP_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+"  "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND DEPENDENT_FEED_ID = ?";
		Object[] args = { vObject.getFeedStatusNt(), vObject.getFeedStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getDependencyFeedId()};
		try {
			retVal = getJdbcTemplate().update(query, args);
			if (retVal == 0) {
				retVal = doInsertionFeedDependency(vObject);
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return retVal;
	}

	@Override
	protected int doInsertionPend(EtlFeedScheduleConfigVb vObject){
		if(ValidationUtil.isValid(vObject.getSchedulePropContext()) && !vObject.getSchedulePropContext().contains("<root>")) {
		      String xml = convert(vObject.getSchedulePropContext(), "root"); // This method converts json object to xml string
			  vObject.setSchedulePropContext(xml); 
		}
		if (ValidationUtil.isValid(vObject.getScheduleType()) && vObject.getScheduleType().equalsIgnoreCase("TIME")) {
			if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
				Long nxtSDate = calculateNxtScheduleDate(vObject.getCountry(), vObject.getLeBook(),
						vObject.getSchedulePropContext(), getTimeZone(vObject.getCountry(), vObject.getLeBook()),
						vObject.getStartDate(), vObject.getEndDate());
				if (ValidationUtil.isValid(nxtSDate)) {
					vObject.setNextScheduleDate(nxtSDate);
				}
			}
		}
		String query =	"Insert Into ETL_FEED_SCHEDULE_CONFIG_PEND (SKIP_HOLIDAY_FLAG, ALERT_FLAG,COUNTRY, LE_BOOK, FEED_ID, SCHEDULE_DESCRIPTION, SCHEDULE_TYPE_AT, "
				+ " SCHEDULE_TYPE,  ALERT_TYPE,CHANNEL_TYPE_SMS, CHANNEL_TYPE_EMAIL ,PROCESS_DATE_TYPE_AT, PROCESS_DATE_TYPE,   "
				+ " ETL_SCHEDULE_STATUS_NT, ETL_SCHEDULE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ " PROCESS_TBL_STATUS_AT, PROCESS_TBL_STATUS, NEXT_SCHEDULE_DATE, NXT_DATE_AUTOMATION_TYPE, START_DATE, END_DATE, SCHEDULE_PROP_CONTEXT)"+ 
				" Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null)
				+ ",?,?,?,?,?,?,?)";
		Object[] args = { vObject.getSkipHolidayFlag(), vObject.getAlertFlag(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getScheduleDescription(), vObject.getScheduleTypeAt(),
				vObject.getScheduleType(), vObject.getAlertType(), vObject.getChannelTypeSMS(),
				vObject.getChannelTypeEmail(), vObject.getProcessDateTypeAt(), vObject.getProcessDateType(),
				vObject.getEtlScheduleStatusNt(), vObject.getEtlScheduleStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getProcessTableStatusAT(), vObject.getProcessTableStatus(), vObject.getNextScheduleDate(),
				vObject.getNxtDateAutomationType(), vObject.getStartDate(), vObject.getEndDate() };
		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for(int i=1;i<=argumentLength;i++){
						ps.setObject(i,args[i-1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getSchedulePropContext()) ? vObject.getSchedulePropContext():"";
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
	private static final Pattern TAG_REGEX = Pattern.compile("<startdate>(.+?)</startdate>", Pattern.DOTALL);

	private static String getTagValues(final String str) {
	    String tagValues = "";
	    Matcher matcher = TAG_REGEX.matcher(str);
	    while (matcher.find()) {
	    	tagValues =matcher.group(1);
	    }
	    return tagValues;
	}
	public String getTimeZone(String country, String leBook) {
		String sql = "select TIME_ZONE FROM vision_business_day where country ='"+country+"'  and le_book ='"+leBook+"' and application_id='ETL'";
		return getJdbcTemplate().queryForObject(sql, String.class);
	}
	public String printTimeFromMilliSecondsAsString(String nextScheduleDate, String timeZone ) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
		String[] dt = nextScheduleDate.split("-");
		if(dt[0].length()==1) {
			nextScheduleDate= "0"+dt[0]+"-"+dt[1]+"-"+dt[2]+" "+"00:00:00";
		}
		Long timeInMilliSeconds = Long.parseLong(nextScheduleDate);
		Date now = new Date();
		now.setTime(timeInMilliSeconds);
		sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
		String output = sdf.format(now.getTime());
		return output;
	}

	private Date returnDateWithHourMins(String hourMinStr, Date date, String timezone) {
		String hourMinStrArr[] = hourMinStr.split(":");
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone(timezone));
		c.setTime(date);
		c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hourMinStrArr[0]));
		c.set(Calendar.MINUTE, Integer.valueOf(hourMinStrArr[1]));
		c.set(Calendar.SECOND, 0);
		return c.getTime();
	}
	
	private String calculateAllHourlyTimeIntervals(String hourlyRange, String timezone, Date date) {
		StringBuffer returnHrBasket = new StringBuffer();
		String hrRangeArr[] = hourlyRange.split("-");
		String rangeStart = hrRangeArr[0];
		String rangeEnd = hrRangeArr[1];

		Date endTime = returnDateWithHourMins(rangeEnd, date, timezone);
		Date refTime = returnDateWithHourMins(rangeStart, date, timezone);
		do {
			Calendar c = Calendar.getInstance();
			c.setTimeZone(TimeZone.getTimeZone(timezone));
			c.setTime(refTime);

			returnHrBasket.append(c.get(Calendar.HOUR_OF_DAY) + ":" + c.get(Calendar.MINUTE) + "-");

			c.add(Calendar.HOUR_OF_DAY, 1);
			refTime = c.getTime();

		} while (refTime.before(endTime) || (refTime.compareTo(endTime) == 0));
		String returnString = String.valueOf(returnHrBasket);
		return returnString.substring(0, (returnString.length() - 1));
	}
	
	
	private Date getNextScheduleTimeGreaterThanCurrentTimeHourFrequency(String hourInclutionConf, Date currentTime, String timezone) {
		String allHourRanges[] = hourInclutionConf.split(",");
		Date scheduleTime = new Date();
		boolean isScheduleTimeCalculated = false;
		for(int allHrRangeIndex = 0;allHrRangeIndex<allHourRanges.length;allHrRangeIndex++) {
			if(!isScheduleTimeCalculated) {
				String allHourBasket = calculateAllHourlyTimeIntervals(allHourRanges[allHrRangeIndex], timezone, currentTime);
				String hourBasket[] = allHourBasket.split("-");
				for(int hourBasketIndex = 0;hourBasketIndex<hourBasket.length;hourBasketIndex++) {
					if(!isScheduleTimeCalculated) {
						String hourMinSplitArr[] = hourBasket[hourBasketIndex].split(":");
						Calendar c = Calendar.getInstance();
						c.setTimeZone(TimeZone.getTimeZone(timezone));
						c.setTime(currentTime);
						c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hourMinSplitArr[0]));
						c.set(Calendar.MINUTE, Integer.valueOf(hourMinSplitArr[1]));
						c.set(Calendar.SECOND, 0);
						scheduleTime = c.getTime();
						if(currentTime.before(scheduleTime) && !isScheduleTimeCalculated) {
							isScheduleTimeCalculated = true;
						}
					}
				}
			}
		}
		
		/* "isScheduleTimeCalculated" boolean variable is falSe bcz all the scheduled time for today is already crossed. 
		 * Now we hv to schedule the feed in the allowed time tomorrow.
		 * So we'll add 1 date to the current date and called this method in recursive manner.*/
		if(!isScheduleTimeCalculated) {
			Calendar c = Calendar.getInstance();
			c.setTimeZone(TimeZone.getTimeZone(timezone));
			c.setTime(currentTime); 
			c.add(Calendar.DATE, 1); //Add 1 date
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			Date increasedDate = c.getTime();
			scheduleTime = getNextScheduleTimeGreaterThanCurrentTimeHourFrequency(hourInclutionConf, increasedDate, timezone);
		}
		return scheduleTime;
	}
	
	private boolean isStartDateLessThanCurrentDate(String startDateStr, Date currentDate, String timezone) {
		boolean returnFlag = false;
		String startDateSplit[] = startDateStr.split("-");
		String date = startDateSplit[0];
		String month = String.valueOf(CommonUtils.returnIntegerOfMonthIn3Letter(startDateSplit[1]));
		String year = startDateSplit[2];
		Date startDate = returnDateWithDateMonthYearHrMinsSec(date, month, year, null, null, null, timezone);
		if(startDate.before(currentDate)) {
			returnFlag = true;
		}
		return returnFlag;
	}
	private Date returnDateWithDateMonthYearHrMinsSec(String date, String month, String year, String hour, String min, String sec, String timezone) {
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone(timezone));
		c.set(Calendar.DATE, ValidationUtil.isValid(date)?Integer.valueOf(date):1);
		c.set(Calendar.MONTH, ValidationUtil.isValid(month)?Integer.valueOf(month):0);
		c.set(Calendar.YEAR, ValidationUtil.isValid(year)?Integer.valueOf(year):1);
		c.set(Calendar.HOUR_OF_DAY, ValidationUtil.isValid(hour)?Integer.valueOf(hour):0);
		c.set(Calendar.MINUTE, ValidationUtil.isValid(min)?Integer.valueOf(min):0);
		c.set(Calendar.SECOND, ValidationUtil.isValid(sec)?Integer.valueOf(sec):0);
		return c.getTime();
	}
	private Date confirmDayOfWeekInclutionAndReturnProperDate(String dateInclutionArr[], Date date, String timezone, Date currentDate) {
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone(timezone));
		c.setTime(date);
		if(!isDayOfWeekIncluded(dateInclutionArr, c.get(Calendar.DAY_OF_WEEK)) || (c.getTime()).before(currentDate)) {
			c.add(Calendar.DATE, 1);
			date = c.getTime();
			date = confirmDayOfWeekInclutionAndReturnProperDate(dateInclutionArr, date, timezone, currentDate);
		}
		return date;
	}
	private Date confirmMonthOfYearInclutionAndReturnProperDate(String monthInclutionArr[], Date date, String timezone, Date currentDate) {
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone(timezone));
		c.setTime(date);
		int monthOfYear = c.get(Calendar.MONTH)+1; //"Calendar.MONTH" starts from 0. So, to make program flow easy, we add 1 to it
		if(!isMonthOfYearIncluded(monthInclutionArr, monthOfYear) || (c.getTime()).before(currentDate)) {
			c.add(Calendar.MONTH, 1);
			date = c.getTime();
			date = confirmMonthOfYearInclutionAndReturnProperDate(monthInclutionArr, date, timezone, currentDate);
		}
		return date;
	}
	private boolean isDayOfWeekIncluded(String dateInclutionArr[], int dayOfWeek) {
		boolean returnFlag = false;
		for(String dayOfWeekStr:dateInclutionArr) {
			if(!returnFlag && Integer.valueOf(dayOfWeekStr)==dayOfWeek) {
				returnFlag = true;
			}
		}
		return returnFlag;
	}

	private boolean isMonthOfYearIncluded(String monthInclutionArr[], int monthOfYear) {
		boolean returnFlag = false;
		for(String monthOfYearStr:monthInclutionArr) {
			if(!returnFlag && Integer.valueOf(monthOfYearStr)==monthOfYear) {
				returnFlag = true;
			}
		}
		return returnFlag;
	}
	private boolean isStartMonthLessThanCurrentMonth(String startDateStr, Date currentDate, String timezone) {
		boolean returnFlag = false;
		String startDateSplit[] = startDateStr.split("-");
		String month = String.valueOf(CommonUtils.returnIntegerOfMonthIn3Letter(startDateSplit[1]));
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone(timezone));
		c.setTime(currentDate);
		int currentMonth = c.get(Calendar.MONTH);
		if(Integer.valueOf(month)<currentMonth)
			returnFlag = true;
		return returnFlag;
	}

	private long calculateNxtScheduleDate(String country, String leBook, String schedulePropContext, String timezone, String startDate, String endDate) {
		Date now = new Date();
		try {
			XmlMapper xmlMapper = new XmlMapper();
			ETLFeedTimeBasedPropVb timePropVb = xmlMapper.readValue(schedulePropContext, ETLFeedTimeBasedPropVb.class);
			ETLFeedTimeBasedInclutionsPropVb inclutionVb = timePropVb.getInclutions();
			if("H".equalsIgnoreCase(timePropVb.getFrequency())) {
				String hourInclutionConf = inclutionVb.getHour();
				if(isStartDateLessThanCurrentDate(startDate, now, timezone)) {
					Date nxtScheduleDate = getNextScheduleTimeGreaterThanCurrentTimeHourFrequency(hourInclutionConf, now, timezone);
				//	return String.valueOf(nxtScheduleDate.getTime());
					return nxtScheduleDate.getTime();
				} else {
					/* Get Date, Month and Year from Start_Date */
					String startDateSplit[] = (startDate).split("-");
					String date = startDateSplit[0];
					String month = String.valueOf(CommonUtils.returnIntegerOfMonthIn3Letter(startDateSplit[1]));
					String year = startDateSplit[2];
					
					/* Get time(Hour and minute) from "hour" tag of "schedulePropContext"
					 * This is the minimum time in the time range configured*/
					String allHourRanges[] = hourInclutionConf.split(",");
					String hourBasket[] = allHourRanges[0].split("-");
					String hourMinSplitArr[] = hourBasket[0].split(":");
					String hour = hourMinSplitArr[0];
					String min = hourMinSplitArr[1];
					
					Date nxtScheduleDate = returnDateWithDateMonthYearHrMinsSec(date, month, year, hour, min, "00", timezone);
				//	return String.valueOf(nxtScheduleDate.getTime());
					return nxtScheduleDate.getTime();
				}
			} else if("D".equalsIgnoreCase(timePropVb.getFrequency())) {
				String dayInclutionConf = inclutionVb.getDay();
				String dayInclutionArr[] = dayInclutionConf.split(",");
				if(isStartDateLessThanCurrentDate(startDate, now, timezone)) {
					String dayTime = inclutionVb.getDaytime();
					String dayHrMinArr[] = dayTime.split(":");
					String hour = dayHrMinArr[0];
					String min = dayHrMinArr[1];
					
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TimeZone.getTimeZone(timezone));
					c.setTime(now);
					c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hour));
					c.set(Calendar.MINUTE, Integer.valueOf(min));
					c.set(Calendar.SECOND, 0);
					
					Date nxtScheduleDate = confirmDayOfWeekInclutionAndReturnProperDate(dayInclutionArr, c.getTime(), timezone, now);
				//	return String.valueOf(nxtScheduleDate.getTime());
					return nxtScheduleDate.getTime();
				} else {
					String startDateSplit[] = startDate.split("-");
					String date = startDateSplit[0];
					String month = String.valueOf(CommonUtils.returnIntegerOfMonthIn3Letter(startDateSplit[1]));
					String year = startDateSplit[2];
					
					String scheduledTimeOfDay = inclutionVb.getDaytime();
					String dayHrMinArr[] = scheduledTimeOfDay.split(":");
					String hour = dayHrMinArr[0];
					String min = dayHrMinArr[1];
					
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TimeZone.getTimeZone(timezone));
					c.set(Calendar.DATE, Integer.valueOf(date));
					c.set(Calendar.MONTH, Integer.valueOf(month));
					c.set(Calendar.YEAR, Integer.valueOf(year));
					c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hour));
					c.set(Calendar.MINUTE, Integer.valueOf(min));
					c.set(Calendar.SECOND, 0);
					Date nxtScheduleDate = confirmDayOfWeekInclutionAndReturnProperDate(dayInclutionArr, c.getTime(), timezone, now);
				//	return String.valueOf(nxtScheduleDate.getTime());
					return nxtScheduleDate.getTime();
				}
			} else if("M".equalsIgnoreCase(timePropVb.getFrequency())) {
				String monthInclutionConf = inclutionVb.getMonth();
				String monthInclutionArr[] = monthInclutionConf.split(",");
				if(isStartMonthLessThanCurrentMonth(startDate, now, timezone)) {
					String scheduledDateOfMonth = inclutionVb.getMonthdate();
					String scheduledTimeOfDay = inclutionVb.getDaytime();
					String dayHrMinArr[] = scheduledTimeOfDay.split(":");
					String hour = dayHrMinArr[0];
					String min = dayHrMinArr[1];
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TimeZone.getTimeZone(timezone));
					c.setTime(now);
					c.set(Calendar.DAY_OF_MONTH, Integer.valueOf(scheduledDateOfMonth));
					c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hour));
					c.set(Calendar.MINUTE, Integer.valueOf(min));
					c.set(Calendar.SECOND, 0);
					Date nxtScheduleDate = confirmMonthOfYearInclutionAndReturnProperDate(monthInclutionArr, c.getTime(), timezone, now);
				//	return String.valueOf(nxtScheduleDate.getTime());
					return nxtScheduleDate.getTime();
				} else {
					String startDateSplit[] = startDate.split("-");
					String date = inclutionVb.getMonthdate();
					String month = String.valueOf(CommonUtils.returnIntegerOfMonthIn3Letter(startDateSplit[1]));
					String year = startDateSplit[2];
					
					String scheduledTimeOfDay = inclutionVb.getDaytime();
					String dayHrMinArr[] = scheduledTimeOfDay.split(":");
					String hour = dayHrMinArr[0];
					String min = dayHrMinArr[1];
					
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TimeZone.getTimeZone(timezone));
					c.set(Calendar.DATE, Integer.valueOf(date));
					c.set(Calendar.MONTH, Integer.valueOf(month));
					c.set(Calendar.YEAR, Integer.valueOf(year));
					c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hour));
					c.set(Calendar.MINUTE, Integer.valueOf(min));
					c.set(Calendar.SECOND, 0);
					Date nxtScheduleDate = confirmMonthOfYearInclutionAndReturnProperDate(monthInclutionArr, c.getTime(), timezone, now);
					//return String.valueOf(nxtScheduleDate.getTime());
					return nxtScheduleDate.getTime();
				}
			} else if("C".equalsIgnoreCase(timePropVb.getFrequency())) {
				if(isStartDateLessThanCurrentDate(startDate, now, timezone)) {
					//return String.valueOf(now.getTime());
					return now.getTime();
				} else {
					String startDateSplit[] = startDate.split("-");
					String date = inclutionVb.getMonthdate();
					String month = String.valueOf(CommonUtils.returnIntegerOfMonthIn3Letter(startDateSplit[1]));
					String year = startDateSplit[2];
					
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TimeZone.getTimeZone(timezone));
					c.set(Calendar.DATE, Integer.valueOf(date));
					c.set(Calendar.MONTH, Integer.valueOf(month));
					c.set(Calendar.YEAR, Integer.valueOf(year));
					c.set(Calendar.HOUR_OF_DAY, 0);
					c.set(Calendar.MINUTE, 0);
					c.set(Calendar.SECOND, 0);
					//return String.valueOf(c.getTime());
					return c.getTime().getTime();
				}
			} else {
				return 0;
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return 0;
		}
	}
	

	@Override
	protected int doInsertionUpl(EtlFeedScheduleConfigVb vObject){
		
		if(ValidationUtil.isValid(vObject.getSchedulePropContext()) && !vObject.getSchedulePropContext().contains("<root>")) {
		      String xml = convert(vObject.getSchedulePropContext(), "root"); // This method converts json object to xml string
			  vObject.setSchedulePropContext(xml); 
		}
		/*String startdate = "";
		if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
			startdate = CommonUtils.getValueForXmlTag(vObject.getSchedulePropContext(), "startdate");
			if (!ValidationUtil.isValid(startdate)) {
				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
				startdate = sdf.format(date);
				String formStartDateTag = "<startdate>" + startdate + "</startdate>";
				String schedulePropContext = vObject.getSchedulePropContext().replaceAll("<startdate/>", formStartDateTag);
				vObject.setSchedulePropContext(schedulePropContext);
			}
		}*/
		
		if(ValidationUtil.isValid(vObject.getScheduleType()) && vObject.getScheduleType().equalsIgnoreCase("TIME")) {
			if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
				Long nxtSDate = calculateNxtScheduleDate(vObject.getCountry(), vObject.getLeBook(),
						vObject.getSchedulePropContext(), getTimeZone(vObject.getCountry(), vObject.getLeBook()),
						vObject.getStartDate(), vObject.getEndDate());
				if(ValidationUtil.isValid(nxtSDate)) {
					vObject.setNextScheduleDate(nxtSDate);
				}
			}
		}

		String query = " Insert Into ETL_FEED_SCHEDULE_CONFIG_UPL (SKIP_HOLIDAY_FLAG, ALERT_FLAG, COUNTRY, LE_BOOK, FEED_ID, SCHEDULE_DESCRIPTION, SCHEDULE_TYPE_AT, "
				+ " SCHEDULE_TYPE, ALERT_TYPE,CHANNEL_TYPE_SMS,CHANNEL_TYPE_EMAIL, PROCESS_DATE_TYPE_AT, PROCESS_DATE_TYPE,   "
				+ " ETL_SCHEDULE_STATUS_NT, ETL_SCHEDULE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ " PROCESS_TBL_STATUS_AT, PROCESS_TBL_STATUS, NEXT_SCHEDULE_DATE, NXT_DATE_AUTOMATION_TYPE, START_DATE, END_DATE, SCHEDULE_PROP_CONTEXT)"
				+ " Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null)
				+ ",?,?,?,?,?,?,?)";

		Object[] args = { vObject.getSkipHolidayFlag(), vObject.getAlertFlag(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getScheduleDescription(), vObject.getScheduleTypeAt(),
				vObject.getScheduleType(), vObject.getAlertType(), vObject.getChannelTypeSMS(),
				vObject.getChannelTypeEmail(), vObject.getProcessDateTypeAt(), vObject.getProcessDateType(),
				vObject.getEtlScheduleStatusNt(), vObject.getEtlScheduleStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getProcessTableStatusAT(), vObject.getProcessTableStatus(), vObject.getNextScheduleDate(),
				vObject.getNxtDateAutomationType(), vObject.getStartDate(), vObject.getEndDate() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for(int i=1;i<=argumentLength;i++){
						ps.setObject(i,args[i-1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getSchedulePropContext())?vObject.getSchedulePropContext():"";
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

	@Override
	protected int doInsertionPendWithDc(EtlFeedScheduleConfigVb vObject){
		if(ValidationUtil.isValid(vObject.getSchedulePropContext()) && !vObject.getSchedulePropContext().contains("<root>")) {
		      String xml = convert(vObject.getSchedulePropContext(), "root"); // This method converts json object to xml string
			  vObject.setSchedulePropContext(xml); 
		}
		if (ValidationUtil.isValid(vObject.getScheduleType()) && vObject.getScheduleType().equalsIgnoreCase("TIME")) {
			if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
				Long nxtSDate = calculateNxtScheduleDate(vObject.getCountry(), vObject.getLeBook(),
						vObject.getSchedulePropContext(), getTimeZone(vObject.getCountry(), vObject.getLeBook()),
						vObject.getStartDate(), vObject.getEndDate());
				if (ValidationUtil.isValid(nxtSDate)) {
					vObject.setNextScheduleDate(nxtSDate);
				}
			}
		}
		String query = " Insert Into ETL_FEED_SCHEDULE_CONFIG_PEND (SKIP_HOLIDAY_FLAG,ALERT_FLAG,COUNTRY, LE_BOOK, FEED_ID, SCHEDULE_DESCRIPTION, SCHEDULE_TYPE_AT, "
				+ " SCHEDULE_TYPE,  ALERT_TYPE, CHANNEL_TYPE_SMS, CHANNEL_TYPE_EMAIL, PROCESS_DATE_TYPE_AT, PROCESS_DATE_TYPE,   "
				+ " ETL_SCHEDULE_STATUS_NT, ETL_SCHEDULE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ " PROCESS_TBL_STATUS_AT, PROCESS_TBL_STATUS, NEXT_SCHEDULE_DATE,NXT_DATE_AUTOMATION_TYPE, START_DATE, END_DATE, SCHEDULE_PROP_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null)
				+ ",?,?,?,?,?,?,?)";

		Object[] args = { vObject.getSkipHolidayFlag(), vObject.getAlertFlag(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getScheduleDescription(), vObject.getScheduleTypeAt(),
				vObject.getScheduleType(), vObject.getAlertType(), vObject.getChannelTypeSMS(),
				vObject.getChannelTypeEmail(), vObject.getProcessDateTypeAt(), vObject.getProcessDateType(),
				vObject.getEtlScheduleStatusNt(), vObject.getEtlScheduleStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getProcessTableStatusAT(), vObject.getProcessTableStatus(), vObject.getNextScheduleDate(),
				vObject.getNxtDateAutomationType(), vObject.getStartDate(), vObject.getEndDate() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for(int i=1;i<=argumentLength;i++){
						ps.setObject(i,args[i-1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getSchedulePropContext())?vObject.getSchedulePropContext():"";
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

	@Override
	protected int doUpdateAppr(EtlFeedScheduleConfigVb vObject){
		if(ValidationUtil.isValid(vObject.getSchedulePropContext()) && !vObject.getSchedulePropContext().contains("<root>")) {
		      String xml = convert(vObject.getSchedulePropContext(), "root"); // This method converts json object to xml string
			  vObject.setSchedulePropContext(xml); 
		}
		
		if (ValidationUtil.isValid(vObject.getScheduleType()) && vObject.getScheduleType().equalsIgnoreCase("TIME")) {
			if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
				Long nxtSDate = calculateNxtScheduleDate(vObject.getCountry(), vObject.getLeBook(),
						vObject.getSchedulePropContext(), getTimeZone(vObject.getCountry(), vObject.getLeBook()),
						vObject.getStartDate(), vObject.getEndDate());
				if (ValidationUtil.isValid(nxtSDate)) {
					vObject.setNextScheduleDate(nxtSDate);
				}
			}
		}

	    String query = " Update ETL_FEED_SCHEDULE_CONFIG Set SCHEDULE_PROP_CONTEXT = ?,SCHEDULE_DESCRIPTION = ?, SCHEDULE_TYPE_AT = ?, SCHEDULE_TYPE = ?,  "
			+ "ALERT_TYPE=?, CHANNEL_TYPE_SMS=?,  CHANNEL_TYPE_EMAIL=?, ALERT_FLAG=?,SKIP_HOLIDAY_FLAG=?, NEXT_SCHEDULE_DATE =?,NXT_DATE_AUTOMATION_TYPE =?,"
			+ "PROCESS_DATE_TYPE_AT = ?, PROCESS_DATE_TYPE = ?, PROCESS_TBL_STATUS_AT = ?, PROCESS_TBL_STATUS = ?, ETL_SCHEDULE_STATUS_NT = ?, "
			+ "ETL_SCHEDULE_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, "
			+ "DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+", START_DATE = ?, END_DATE = ? WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getScheduleDescription(), vObject.getScheduleTypeAt(), vObject.getScheduleType(),
				vObject.getAlertType(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getAlertFlag(), vObject.getSkipHolidayFlag(), vObject.getNextScheduleDate(),
				vObject.getNxtDateAutomationType(), vObject.getProcessDateTypeAt(), vObject.getProcessDateType(),
				vObject.getProcessTableStatusAT(), vObject.getProcessTableStatus(), vObject.getEtlScheduleStatusNt(),
				vObject.getEtlScheduleStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getStartDate(),
				vObject.getEndDate(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getSchedulePropContext())?vObject.getSchedulePropContext():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					for(int i=1;i<=args.length;i++){
						ps.setObject(++psIndex,args[i-1]);	
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

	@Override
	protected int doUpdatePend(EtlFeedScheduleConfigVb vObject){
		if (ValidationUtil.isValid(vObject.getSchedulePropContext())
				&& !vObject.getSchedulePropContext().contains("<root>")) {
			String xml = convert(vObject.getSchedulePropContext(), "root"); // This method converts json object to xml string
			vObject.setSchedulePropContext(xml);
		}
		if (ValidationUtil.isValid(vObject.getScheduleType()) && vObject.getScheduleType().equalsIgnoreCase("TIME")) {
			if (ValidationUtil.isValid(vObject.getSchedulePropContext())) {
				Long nxtSDate = calculateNxtScheduleDate(vObject.getCountry(), vObject.getLeBook(),
						vObject.getSchedulePropContext(), getTimeZone(vObject.getCountry(), vObject.getLeBook()),
						vObject.getStartDate(), vObject.getEndDate());
				if (ValidationUtil.isValid(nxtSDate)) {
					vObject.setNextScheduleDate(nxtSDate);
				}
			}
		}
		String query = "Update ETL_FEED_SCHEDULE_CONFIG_PEND Set SCHEDULE_PROP_CONTEXT = ?,SCHEDULE_DESCRIPTION = ?, SCHEDULE_TYPE_AT = ?, SCHEDULE_TYPE = ?,  "
			+ "ALERT_TYPE=?, CHANNEL_TYPE_SMS=?,  CHANNEL_TYPE_EMAIL=?,ALERT_FLAG =?,SKIP_HOLIDAY_FLAG =?, NEXT_SCHEDULE_DATE =?,NXT_DATE_AUTOMATION_TYPE =?,"
			+ "PROCESS_DATE_TYPE_AT = ?, PROCESS_DATE_TYPE = ?, PROCESS_TBL_STATUS_AT = ?, PROCESS_TBL_STATUS = ?, ETL_SCHEDULE_STATUS_NT = ?, "
			+ "ETL_SCHEDULE_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, "
			+ "DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+", START_DATE = ?, END_DATE = ?  WHERE COUNTRY = ? AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getScheduleDescription(), vObject.getScheduleTypeAt(), vObject.getScheduleType(),
				vObject.getAlertType(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getAlertFlag(), vObject.getSkipHolidayFlag(), vObject.getNextScheduleDate(),
				vObject.getNxtDateAutomationType(), vObject.getProcessDateTypeAt(), vObject.getProcessDateType(),
				vObject.getProcessTableStatusAT(), vObject.getProcessTableStatus(), vObject.getEtlScheduleStatusNt(),
				vObject.getEtlScheduleStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getStartDate(),
				vObject.getEndDate(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getSchedulePropContext())?vObject.getSchedulePropContext():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					for(int i=1;i<=args.length;i++){
						ps.setObject(++psIndex,args[i-1]);	
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

	@Override
	protected int doDeleteAppr(EtlFeedScheduleConfigVb vObject){
		String query = "Delete From ETL_FEED_SCHEDULE_CONFIG Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedScheduleConfigVb vObject){
		String tableDep = "ETL_FEED_DEPENDENCIES_PEND";
		String query1 = "Delete From "+tableDep+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args1 = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		getJdbcTemplate().update(query1,args1);
				
		String query = "Delete From ETL_FEED_SCHEDULE_CONFIG_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deleteUplRecord(EtlFeedScheduleConfigVb vObject){
		String tableDep = "ETL_FEED_DEPENDENCIES_UPL";
		String query1 = "Delete From "+tableDep+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args1 = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		getJdbcTemplate().update(query1,args1);
		
		String query = "Delete From ETL_FEED_SCHEDULE_CONFIG_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){

		String tableDep = "ETL_FEED_DEPENDENCIES";
		if("UPL".equalsIgnoreCase(tableType)) {
			tableDep = "ETL_FEED_DEPENDENCIES_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			tableDep = "ETL_FEED_DEPENDENCIES_PEND";
		}
		String query1 = "Delete From "+tableDep+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args1 = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		getJdbcTemplate().update(query1,args1);
		
		String table = "ETL_FEED_SCHEDULE_CONFIG";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SCHEDULE_CONFIG_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SCHEDULE_CONFIG_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
		
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String tableDep = "ETL_FEED_DEPENDENCIES_PRS";
		String query1 = "Delete From "+tableDep+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args1 = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query1,args1);
		
		/*String table = "ETL_FEED_SCHEDULE_CONFIG_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);*/
	}
	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_FEED_SCHEDULE_CONFIG";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SCHEDULE_CONFIG_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SCHEDULE_CONFIG_PEND";
		}
		String query = "update "+table+" set ETL_SCHEDULE_STATUS = ? ,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(EtlFeedScheduleConfigVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getScheduleDescription()))
			strAudit.append("SCHEDULE_DESCRIPTION" + auditDelimiterColVal + vObject.getScheduleDescription().trim());
		else
			strAudit.append("SCHEDULE_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("SCHEDULE_TYPE_AT" + auditDelimiterColVal + vObject.getScheduleTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getScheduleType()))
			strAudit.append("SCHEDULE_TYPE" + auditDelimiterColVal + vObject.getScheduleType().trim());
		else
			strAudit.append("SCHEDULE_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ALERT_TYPE_AT" + auditDelimiterColVal + vObject.getAlertTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getAlertType()))
			strAudit.append("ALERT_TYPE" + auditDelimiterColVal + vObject.getAlertType().trim());
		else
			strAudit.append("ALERT_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ALERT_FLAG" + auditDelimiterColVal + vObject.getAlertFlag());
		strAudit.append(auditDelimiter);

		strAudit.append("SKIP_HOLIDAY_FLAG" + auditDelimiterColVal + vObject.getSkipHolidayFlag());
		strAudit.append(auditDelimiter);

		strAudit.append("CHANNEL_TYPE_SMS" + auditDelimiterColVal + vObject.getChannelTypeSMS());
		strAudit.append(auditDelimiter);

		strAudit.append("CHANNEL_TYPE_EMAIL" + auditDelimiterColVal + vObject.getChannelTypeEmail());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSchedulePropContext()))
			strAudit.append("SCHEDULE_PROP_CONTEXT" + auditDelimiterColVal + vObject.getSchedulePropContext().trim());
		else
			strAudit.append("SCHEDULE_PROP_CONTEXT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("PROCESS_DATE_TYPE_AT" + auditDelimiterColVal + vObject.getProcessDateTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getProcessDateType()))
			strAudit.append("PROCESS_DATE_TYPE" + auditDelimiterColVal + vObject.getProcessDateType().trim());
		else
			strAudit.append("PROCESS_DATE_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("NEXT_SCHEDULE_DATE" + auditDelimiterColVal + vObject.getNextScheduleDate());
		strAudit.append(auditDelimiter);

		strAudit.append("NXT_DATE_AUTOMATION_TYPE" + auditDelimiterColVal + vObject.getNxtDateAutomationType());
		strAudit.append(auditDelimiter);

		strAudit.append("PROCESS_TBL_STATUS_AT" + auditDelimiterColVal + vObject.getProcessTableStatusAT());
		strAudit.append(auditDelimiter);

		strAudit.append("PROCESS_TBL_STATUS" + auditDelimiterColVal + vObject.getProcessTableStatus());
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_SCHEDULE_STATUS_NT" + auditDelimiterColVal + vObject.getEtlScheduleStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_SCHEDULE_STATUS" + auditDelimiterColVal + vObject.getEtlScheduleStatus());
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

		if (ValidationUtil.isValid(vObject.getStartDate()))
			strAudit.append("START_DATE" + auditDelimiterColVal + vObject.getStartDate().trim());
		else
			strAudit.append("START_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEndDate()))
			strAudit.append("END_DATE" + auditDelimiterColVal + vObject.getEndDate().trim());
		else
			strAudit.append("END_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		return strAudit.toString();
	}

	@Override
	protected void setServiceDefaults(){
		serviceName = "EtlFeedScheduleConfig";
		serviceDesc = "ETL Feed Schedule Config";
		tableName = "ETL_FEED_SCHEDULE_CONFIG";
		childTableName = "ETL_FEED_SCHEDULE_CONFIG";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String query1 = "delete FROM ETL_FEED_DEPENDENCIES_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_DEPENDENCIES_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_FEED_DEPENDENCIES_PRS.SESSION_ID ) ";
		Object[] args1 = {  vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query1,args1);
		
		/*String query = "delete FROM ETL_FEED_SCHEDULE_CONFIG_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_SCHEDULE_CONFIG_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_FEED_SCHEDULE_CONFIG_PRS.SESSION_ID ) ";
		Object[] args = {  vObject.getFeedCategory() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);*/
	}
}