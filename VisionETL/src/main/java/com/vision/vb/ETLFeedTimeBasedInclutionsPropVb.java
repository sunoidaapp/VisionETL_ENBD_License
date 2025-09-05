package com.vision.vb;

import java.io.Serializable;

public class ETLFeedTimeBasedInclutionsPropVb implements Serializable {
	
	private String hour = "";
	private String day = "";
	private String month = "";
	private String daytime = "";
	private String monthdate = "";
	private String interval = "";

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getDaytime() {
		return daytime;
	}

	public void setDaytime(String daytime) {
		this.daytime = daytime;
	}

	public String getMonthdate() {
		return monthdate;
	}

	public void setMonthdate(String monthdate) {
		this.monthdate = monthdate;
	}

	public String getInterval() {
		return interval;
	}

	public void setInterval(String interval) {
		this.interval = interval;
	}

}
