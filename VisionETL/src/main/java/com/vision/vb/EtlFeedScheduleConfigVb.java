package com.vision.vb;

import java.util.List;

public class EtlFeedScheduleConfigVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String feedCategory = "";
	private String scheduleDescription = "";
	private int scheduleTypeAt = 2108;
	private String scheduleType = "";
	private String schedulePropContext = "";
	private int processDateTypeAt = 2109;
	private String processDateType = "";
	private int etlScheduleStatusNt = 2000;
	private int etlScheduleStatus = 0;

	private int processTableStatusAT = 2119;
	private String processTableStatus = "YTP";
	private long nextScheduleDate = 0L;
	private String nxtDateAutomationType = "D";
	private String dependencyFeedContext = "";
	private int alertTypeAt = 2140;
	private String alertType = "C";
	private String channelTypeSMS = "N";
	private String channelTypeEmail = "N";
	private String alertFlag = "N";
	private String skipHolidayFlag = "N";

	List<EtlFeedAlertConfigVb> etlFeedAlertConfiglst = null;

	private String startDate = "";
	private String endDate = "";

	public EtlFeedScheduleConfigVb() {
	}

	public EtlFeedScheduleConfigVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
	}

	public String getChannelTypeSMS() {
		return channelTypeSMS;
	}

	public void setChannelTypeSMS(String channelTypeSMS) {
		this.channelTypeSMS = channelTypeSMS;
	}

	public String getChannelTypeEmail() {
		return channelTypeEmail;
	}

	public void setChannelTypeEmail(String channelTypeEmail) {
		this.channelTypeEmail = channelTypeEmail;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCountry() {
		return country;
	}

	public void setLeBook(String leBook) {
		this.leBook = leBook;
	}

	public String getLeBook() {
		return leBook;
	}

	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}

	public String getFeedId() {
		return feedId;
	}

	public void setScheduleDescription(String scheduleDescription) {
		this.scheduleDescription = scheduleDescription;
	}

	public String getScheduleDescription() {
		return scheduleDescription;
	}

	public void setScheduleTypeAt(int scheduleTypeAt) {
		this.scheduleTypeAt = scheduleTypeAt;
	}

	public int getScheduleTypeAt() {
		return scheduleTypeAt;
	}

	public void setScheduleType(String scheduleType) {
		this.scheduleType = scheduleType;
	}

	public String getScheduleType() {
		return scheduleType;
	}

	public void setSchedulePropContext(String schedulePropContext) {
		this.schedulePropContext = schedulePropContext;
	}

	public String getSchedulePropContext() {
		return schedulePropContext;
	}

	public void setProcessDateTypeAt(int processDateTypeAt) {
		this.processDateTypeAt = processDateTypeAt;
	}

	public int getProcessDateTypeAt() {
		return processDateTypeAt;
	}

	public void setProcessDateType(String processDateType) {
		this.processDateType = processDateType;
	}

	public String getProcessDateType() {
		return processDateType;
	}

	public void setEtlScheduleStatusNt(int etlScheduleStatusNt) {
		this.etlScheduleStatusNt = etlScheduleStatusNt;
	}

	public int getEtlScheduleStatusNt() {
		return etlScheduleStatusNt;
	}

	public void setEtlScheduleStatus(int etlScheduleStatus) {
		this.etlScheduleStatus = etlScheduleStatus;
	}

	public int getEtlScheduleStatus() {
		return etlScheduleStatus;
	}

	public int getProcessTableStatusAT() {
		return processTableStatusAT;
	}

	public void setProcessTableStatusAT(int processTableStatusAT) {
		this.processTableStatusAT = processTableStatusAT;
	}

	public String getProcessTableStatus() {
		return processTableStatus;
	}

	public void setProcessTableStatus(String processTableStatus) {
		this.processTableStatus = processTableStatus;
	}

	public long getNextScheduleDate() {
		return nextScheduleDate;
	}

	public void setNextScheduleDate(long nextScheduleDate) {
		this.nextScheduleDate = nextScheduleDate;
	}

	public String getNxtDateAutomationType() {
		return nxtDateAutomationType;
	}

	public void setNxtDateAutomationType(String nxtDateAutomationType) {
		this.nxtDateAutomationType = nxtDateAutomationType;
	}

	public String getDependencyFeedContext() {
		return dependencyFeedContext;
	}

	public void setDependencyFeedContext(String dependencyFeedContext) {
		this.dependencyFeedContext = dependencyFeedContext;
	}

	public String getFeedCategory() {
		return feedCategory;
	}

	public void setFeedCategory(String feedCategory) {
		this.feedCategory = feedCategory;
	}

	public int getAlertTypeAt() {
		return alertTypeAt;
	}

	public void setAlertTypeAt(int alertTypeAt) {
		this.alertTypeAt = alertTypeAt;
	}

	public String getAlertType() {
		return alertType;
	}

	public void setAlertType(String alertType) {
		this.alertType = alertType;
	}

	public List<EtlFeedAlertConfigVb> getEtlFeedAlertConfiglst() {
		return etlFeedAlertConfiglst;
	}

	public void setEtlFeedAlertConfiglst(List<EtlFeedAlertConfigVb> etlFeedAlertConfiglst) {
		this.etlFeedAlertConfiglst = etlFeedAlertConfiglst;
	}

	public String getAlertFlag() {
		return alertFlag;
	}

	public void setAlertFlag(String alertFlag) {
		this.alertFlag = alertFlag;
	}

	public String getSkipHolidayFlag() {
		return skipHolidayFlag;
	}

	public void setSkipHolidayFlag(String skipHolidayFlag) {
		this.skipHolidayFlag = skipHolidayFlag;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

}