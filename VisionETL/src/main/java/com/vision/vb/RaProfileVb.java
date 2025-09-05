package com.vision.vb;

import java.io.Serializable;
import java.util.List;

public class RaProfileVb extends CommonVb implements Serializable{
	
	private static final long serialVersionUID = -7365141127791003770L;
	private int	userGroupAt =  1;
	private String	userGroup = "";
	private String	userGroupDesc = "";
	private int	userProfileAt =  2;
	private String	userProfile = "";
	private String	userProfileDesc = "";
	private int	menuGroupNt =  176;	
	private String menuGroup = "";
	private String menuGroupDesc = "";
	private String screenName = "";
	private String screenNameDesc = "";
	private String profileAdd  = "N";
	private String profileDelete = "N";
	private String profileInquiry = "N";
	private String profileVerification = "N"; 
	private String profileModify = "N";
	private String profileUpload = "N";
	private String profileDownload = "N";
	private int	profileStatusNt =  1;
	private int	profileStatus = -1;	
	private String profileStatusDesc = "";
	private String homeDashboard ="";
	List<SmartSearchVb> smartSearchOpt = null;
	private List<AlphaSubTabVb> dashboardReportlst = null;
	
	public int getUserGroupAt() {
		return userGroupAt;
	}

	public void setUserGroupAt(int userGroupAt) {
		this.userGroupAt = userGroupAt;
	}

	public String getUserGroup() {
		return userGroup;
	}

	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}

	public String getUserGroupDesc() {
		return userGroupDesc;
	}

	public void setUserGroupDesc(String userGroupDesc) {
		this.userGroupDesc = userGroupDesc;
	}

	public int getUserProfileAt() {
		return userProfileAt;
	}

	public void setUserProfileAt(int userProfileAt) {
		this.userProfileAt = userProfileAt;
	}

	public String getUserProfile() {
		return userProfile;
	}

	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile;
	}

	public String getUserProfileDesc() {
		return userProfileDesc;
	}

	public void setUserProfileDesc(String userProfileDesc) {
		this.userProfileDesc = userProfileDesc;
	}

	public int getMenuGroupNt() {
		return menuGroupNt;
	}

	public void setMenuGroupNt(int menuGroupNt) {
		this.menuGroupNt = menuGroupNt;
	}
	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public String getProfileAdd() {
		return profileAdd;
	}

	public void setProfileAdd(String profileAdd) {
		this.profileAdd = profileAdd;
	}

	public String getProfileDelete() {
		return profileDelete;
	}

	public void setProfileDelete(String profileDelete) {
		this.profileDelete = profileDelete;
	}

	public String getProfileInquiry() {
		return profileInquiry;
	}

	public void setProfileInquiry(String profileInquiry) {
		this.profileInquiry = profileInquiry;
	}

	public String getProfileVerification() {
		return profileVerification;
	}

	public void setProfileVerification(String profileVerification) {
		this.profileVerification = profileVerification;
	}

	public String getProfileModify() {
		return profileModify;
	}

	public void setProfileModify(String profileModify) {
		this.profileModify = profileModify;
	}

	public String getProfileUpload() {
		return profileUpload;
	}

	public void setProfileUpload(String profileUpload) {
		this.profileUpload = profileUpload;
	}

	public String getProfileDownload() {
		return profileDownload;
	}

	public void setProfileDownload(String profileDownload) {
		this.profileDownload = profileDownload;
	}

	public int getProfileStatusNt() {
		return profileStatusNt;
	}

	public void setProfileStatusNt(int profileStatusNt) {
		this.profileStatusNt = profileStatusNt;
	}

	public int getProfileStatus() {
		return profileStatus;
	}

	public void setProfileStatus(int profileStatus) {
		this.profileStatus = profileStatus;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

	public String getMenuGroup() {
		return menuGroup;
	}

	public void setMenuGroup(String menuGroup) {
		this.menuGroup = menuGroup;
	}

	public String getMenuGroupDesc() {
		return menuGroupDesc;
	}

	public void setMenuGroupDesc(String menuGroupDesc) {
		this.menuGroupDesc = menuGroupDesc;
	}

	public String getHomeDashboard() {
		return homeDashboard;
	}

	public void setHomeDashboard(String homeDashboard) {
		this.homeDashboard = homeDashboard;
	}

	public List<AlphaSubTabVb> getDashboardReportlst() {
		return dashboardReportlst;
	}

	public void setDashboardReportlst(List<AlphaSubTabVb> dashboardReportlst) {
		this.dashboardReportlst = dashboardReportlst;
	}

	public String getScreenNameDesc() {
		return screenNameDesc;
	}

	public void setScreenNameDesc(String screenNameDesc) {
		this.screenNameDesc = screenNameDesc;
	}
	
	public String getProfileStatusDesc() {
		return profileStatusDesc;
	}

	public void setProfileStatusDesc(String profileStatusDesc) {
		this.profileStatusDesc = profileStatusDesc;
	}
}
