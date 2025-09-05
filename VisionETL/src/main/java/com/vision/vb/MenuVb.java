package com.vision.vb;

import java.util.ArrayList;

public class MenuVb extends CommonVb {

	private static final long serialVersionUID = -2010835009684844752L;
	private String menuProgram = "";
	private String menuName = "";
	private int menuSequence = 0;
	private int parentSequence = 0;
	private String separator = "-1";
	private int menuGroupNt = 0;
	private int menuGroup = -1;
	private int menuStatusNt = 0;
	private int menuStatus = -1;
	private String applicationAccess = "";
	private String profileAdd  = "N";
	private String profileDelete = "N";
	private String profileView = "N";
	private String profileVerification = "N"; 
	private String profileModify = "N";
	private String profileUpload = "N";
	private String profileDownload = "N";
	private ArrayList<MenuVb> children = null;
	private String menuIcon = "";
	
	public String getMenuProgram() {
		return menuProgram;
	}
	public void setMenuProgram(String menuProgram) {
		this.menuProgram = menuProgram;
	}
	public String getMenuName() {
		return menuName;
	}
	public void setMenuName(String menuName) {
		this.menuName = menuName;
	}
	public int getMenuSequence() {
		return menuSequence;
	}
	public void setMenuSequence(int menuSequence) {
		this.menuSequence = menuSequence;
	}
	public int getParentSequence() {
		return parentSequence;
	}
	public void setParentSequence(int parentSequence) {
		this.parentSequence = parentSequence;
	}
	public String getSeparator() {
		return separator;
	}
	public void setSeparator(String separator) {
		this.separator = separator;
	}
	public int getMenuGroupNt() {
		return menuGroupNt;
	}
	public void setMenuGroupNt(int menuGroupNt) {
		this.menuGroupNt = menuGroupNt;
	}
	public int getMenuGroup() {
		return menuGroup;
	}
	public void setMenuGroup(int menuGroup) {
		this.menuGroup = menuGroup;
	}
	public int getMenuStatusNt() {
		return menuStatusNt;
	}
	public void setMenuStatusNt(int menuStatusNt) {
		this.menuStatusNt = menuStatusNt;
	}
	public int getMenuStatus() {
		return menuStatus;
	}
	public void setMenuStatus(int menuStatus) {
		this.menuStatus = menuStatus;
		
	}
	public ArrayList<MenuVb> getChildren() {
		return children;
	}
	public void setChildren(ArrayList<MenuVb> children) {
		this.children = children;
	}
	public String getApplicationAccess() {
		return applicationAccess;
	}
	public void setApplicationAccess(String applicationAccess) {
		this.applicationAccess = applicationAccess;
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
	public String getProfileView() {
		return profileView;
	}
	public void setProfileView(String profileView) {
		this.profileView = profileView;
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
	public String getMenuIcon() {
		return menuIcon;
	}
	public void setMenuIcon(String menuIcon) {
		this.menuIcon = menuIcon;
	}
	
}
