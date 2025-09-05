package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.DashboardDesignVb;
import com.vision.vb.DashboardLODWrapperVb;
import com.vision.vb.LevelOfDisplayUserVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.WidgetDesignVb;

@Component
public class DashboardDesignDao extends AbstractDao<DashboardDesignVb> {

	@Value("${app.databaseType}")
	private String databaseType;

	@Override
	protected void setServiceDefaults() {
		serviceName = "Dashboard";
		serviceDesc = "Dashboard";
		tableName = "VDD_RS_DASHBOARDS_WIP";
		childTableName = "VDD_RS_WIDGETS_WIP";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		userGroup = CustomContextHolder.getContext().getUserGroup();
		userProfile = CustomContextHolder.getContext().getUserProfile();
	}

	private ResultSetExtractor<DashboardDesignVb> rowExtractor() {

		ResultSetExtractor<DashboardDesignVb> resultExtractor = new ResultSetExtractor<DashboardDesignVb>() {
			@Override
			public DashboardDesignVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				DashboardDesignVb dashboardVb = null;
				List<WidgetDesignVb> widgetVbList = new ArrayList<WidgetDesignVb>();
				boolean isFirst = true;
				while (rs.next()) {
					if (isFirst) {
						String dashData = rs.getString("DASHBOARD_DESIGN");
						ExceptionCode exception = CommonUtils.XmlToJson(dashData);
						dashboardVb = new DashboardDesignVb();
						dashboardVb.setDashboardId(rs.getString("DASHBOARD_ID"));
						dashboardVb.setDashboardName(rs.getString("DASHBOARD_NAME"));
						if (exception.getErrorCode() == Constants.SUCCESSFUL_OPERATION)
							dashboardVb.setDashboardDesignJson(exception.getResponse() + "");
						dashboardVb.setDashboardDesc(rs.getString("DASHBOARD_DESC"));
						dashboardVb.setRsdStatus(rs.getInt("RSD_STATUS"));
						dashData = rs.getString("FILTER_CONTEXT");
						if (dashData != null && dashData.length() > 1) {
							exception = CommonUtils.XmlToJson(dashData);
							dashboardVb.setFilterContextJson(exception.getResponse() + "");
						}
						isFirst = false;
					}
					WidgetDesignVb widgetVb = new WidgetDesignVb();
					widgetVb.setDashboardId(rs.getString("DASHBOARD_ID"));
					widgetVb.setSubDashboardId(rs.getInt("SUB_DASHBOARD_ID"));
					widgetVb.setWidgetId(rs.getString("WIDGETS_ID"));
					String widgetData = rs.getString("WIDGET_CONTEXT");
					if (widgetData != null) {
						widgetVb.setWidgetContextJson(CommonUtils.XmlToJson(widgetData).getResponse() + "");
					}
					String widgetFilterData = rs.getString("WIDGET_FILTER_CONTEXT");
					if (widgetFilterData != null) {
						widgetVb.setFilterContextJson(CommonUtils.XmlToJson(widgetFilterData).getResponse() + "");
					}

					widgetVbList.add(widgetVb);
				}
				if (dashboardVb != null)
					dashboardVb.setChildren(widgetVbList);
				return dashboardVb;
			}

		};

		return resultExtractor;
	}

	public DashboardDesignVb getQueryResults(DashboardDesignVb vObj, int intStatus, boolean appendStatus) {
		setServiceDefaults();
		String strQueryAppr = "SELECT DASH.DASHBOARD_ID,DASHBOARD_NAME, DASH.DASHBOARD_DESC, DASH.DASHBOARD_DESIGN,DASH.FILTER_CONTEXT,DASH.RSD_STATUS,wid.SUB_DASHBOARD_ID,wid.WIDGETS_ID,wid.WIDGET_CONTEXT,wid.WIDGET_FILTER_CONTEXT "
				+ " fROM VDD_RS_WIDGETS wid left join VDD_RS_DASHBOARDS DASH ON WID.DASHBOARD_ID = DASH.DASHBOARD_ID  WHERE UPPER(DASH.DASHBOARD_ID) = UPPER(?) ";
		String strQueryApprStatus = " AND DASH.RSD_STATUS = " + vObj.getRsdStatus();
		String strQueryPend = "SELECT DASH.DASHBOARD_ID,DASHBOARD_NAME, DASH.DASHBOARD_DESC, DASH.DASHBOARD_DESIGN,DASH.FILTER_CONTEXT,DASH.RSD_STATUS,wid.SUB_DASHBOARD_ID,wid.WIDGETS_ID,wid.WIDGET_CONTEXT,wid.WIDGET_FILTER_CONTEXT "
				+ " fROM VDD_RS_WIDGETS_WIP wid left join VDD_RS_DASHBOARDS_WIP DASH ON wid.DASHBOARD_ID = DASH.DASHBOARD_ID  WHERE UPPER(DASH.DASHBOARD_ID) = UPPER(?)  ";
		String strQueryPendStatus = " AND DASH.RSD_STATUS = " + vObj.getRsdStatus();

		if (appendStatus) {
			strQueryAppr += strQueryApprStatus;
			strQueryPend += strQueryPendStatus;
		}
		String[] args = { vObj.getDashboardId() };
//		SqlRowSet rowset = null;
		int row = 1;
		try {
			if (intStatus == 0) {
				return getJdbcTemplate().query(strQueryAppr.toString(), args, rowExtractor());
			} else {
				return getJdbcTemplate().query(strQueryPend.toString(), args, rowExtractor());
			}
			/*
			 * if (intStatus == 0) { rowset =
			 * getJdbcTemplate().queryForRowSet(strQueryAppr.toString(),args); } else {
			 * rowset = getJdbcTemplate().queryForRowSet(strQueryPend.toString(),args); }
			 * 
			 * DashboardDesignVb dashboardVb = null; List<WidgetDesignVb> widgetVbList = new
			 * ArrayList<WidgetDesignVb>();
			 * 
			 * while (rowset.next()) { if (row == 1) { String dashData =
			 * rowset.getString("DASHBOARD_DESIGN"); ExceptionCode exception =
			 * CommonUtils.XmlToJson(dashData); dashboardVb = new DashboardDesignVb();
			 * dashboardVb.setDashboardId(rowset.getString("DASHBOARD_ID"));
			 * dashboardVb.setDashboardName(rowset.getString("DASHBOARD_NAME")); if
			 * (exception.getErrorCode() == Constants.SUCCESSFUL_OPERATION)
			 * dashboardVb.setDashboardDesignJson(exception.getResponse() + "");
			 * dashboardVb.setDashboardDesc(rowset.getString("DASHBOARD_DESC"));
			 * dashboardVb.setRsdStatus(rowset.getInt("RSD_STATUS")); dashData =
			 * rowset.getString("FILTER_CONTEXT"); if (dashData != null && dashData.length()
			 * > 1) { exception = CommonUtils.XmlToJson(dashData);
			 * dashboardVb.setFilterContextJson(exception.getResponse() + ""); } }
			 * WidgetDesignVb widgetVb = new WidgetDesignVb();
			 * widgetVb.setDashboardId(rowset.getString("DASHBOARD_ID"));
			 * widgetVb.setSubDashboardId(rowset.getInt("SUB_DASHBOARD_ID"));
			 * widgetVb.setWidgetId(rowset.getString("WIDGETS_ID")); String widgetData =
			 * rowset.getString("WIDGET_CONTEXT"); if (widgetData != null) {
			 * widgetVb.setWidgetContextJson(CommonUtils.XmlToJson(widgetData).getResponse()
			 * + ""); } String widgetFilterData = rowset.getString("WIDGET_FILTER_CONTEXT");
			 * if (widgetFilterData != null) {
			 * widgetVb.setFilterContextJson(CommonUtils.XmlToJson(widgetFilterData).
			 * getResponse() + ""); }
			 * 
			 * widgetVbList.add(widgetVb); row++; } if (dashboardVb != null)
			 * dashboardVb.setChildren(widgetVbList);
			 * 
			 * return dashboardVb;
			 */

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public int doInsertionApprforDashboardIntoWIP(DashboardDesignVb vObj) throws RuntimeCustomException {

		setServiceDefaults();
		if (ValidationUtil.isValid(vObj.getFilterContextJson())) {
			ExceptionCode exceptionFilterCode = CommonUtils.jsonToXml(vObj.getFilterContextJson());
			if (exceptionFilterCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				vObj.setFilterContext(exceptionFilterCode.getResponse() + "");
			}
		}
		if (ValidationUtil.isValid(vObj.getDashboardDesignJson())) {
			ExceptionCode exceptionDashboardDesign = CommonUtils.jsonToXml(vObj.getDashboardDesignJson());
			if (exceptionDashboardDesign.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				vObj.setDashboardDesign(exceptionDashboardDesign.getResponse() + "");
			}
		}
		// generating Dynamic DashboardID
		String dashboardId = generateDashboardID(vObj.getMaker());
		vObj.setDashboardId(dashboardId);
		String dashboardSql = "INSERT INTO VDD_RS_DASHBOARDS_WIP "
				+ " (DASHBOARD_ID,DASHBOARD_NAME, DASHBOARD_DESC,RSD_STATUS_NT, RSD_STATUS, "
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION, DASHBOARD_DESIGN, FILTER_CONTEXT) "
				+ " VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?)";

		Object[] argsdash = { vObj.getDashboardId(), vObj.getDashboardName(), vObj.getDashboardDesc(),
				vObj.getRsdStatusNt(), vObj.getRsdStatus(), vObj.getRecordIndicatorNt(), vObj.getRecordIndicator(),
				intCurrentUserId, intCurrentUserId, 0 };

		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int argumentLength = argsdash.length;
				PreparedStatement ps = connection.prepareStatement(dashboardSql);
				for (int i = 1; i <= argumentLength; i++) {
					ps.setObject(i, argsdash[i - 1]);
				}

				String clobData = ValidationUtil.isValid(vObj.getDashboardDesign()) ? vObj.getDashboardDesign() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

				clobData = ValidationUtil.isValid(vObj.getFilterContext()) ? vObj.getFilterContext() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				return ps;
			}
		});

		String widgetSql = "INSERT INTO VDD_RS_WIDGETS_WIP "
				+ " (DASHBOARD_ID, SUB_DASHBOARD_ID,WIDGETS_ID,RSW_STATUS_NT, RSW_STATUS, "
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION, WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ",?,?)";

		for (WidgetDesignVb widgetVb : vObj.getChildren()) {

			if (ValidationUtil.isValid(widgetVb.getWidgetContextJson())) {
				ExceptionCode exceptionwidgetCode = CommonUtils.jsonToXml(widgetVb.getWidgetContextJson());
				if (exceptionwidgetCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					widgetVb.setWidgetContext(exceptionwidgetCode.getResponse() + "");
				}
			}
			if (ValidationUtil.isValid(widgetVb.getFilterContextJson())) {
				ExceptionCode exceptionfilterCode = CommonUtils.jsonToXml(widgetVb.getFilterContextJson());
				if (exceptionfilterCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					widgetVb.setFilterContext(exceptionfilterCode.getResponse() + "");
				}
			}
			Object[] argswidget = { vObj.getDashboardId(), widgetVb.getSubDashboardId(), widgetVb.getWidgetId(),
					vObj.getRsdStatusNt(), vObj.getRsdStatus(), widgetVb.getRecordIndicatorNt(),
					widgetVb.getRecordIndicator(), intCurrentUserId, intCurrentUserId, 0 };

			getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = argswidget.length;
					PreparedStatement ps = connection.prepareStatement(widgetSql);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, argswidget[i - 1]);
					}
					String clobwidgetData = ValidationUtil.isValid(widgetVb.getWidgetContext())
							? widgetVb.getWidgetContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobwidgetData), clobwidgetData.length());

					String clobFilterData = ValidationUtil.isValid(widgetVb.getFilterContext())
							? widgetVb.getFilterContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobFilterData), clobFilterData.length());
					return ps;
				}
			});
		}
		String Usersql = "INSERT INTO VDD_DASHBOARDS_LOD_USER_LVL "
				+ " (DASHBOARD_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
				+ " RECORD_INDICATOR,RECORD_INDICATOR_NT, VISION_ID,DASHBOARD_PRIVATE_FLAG,DASHBOARD_WRITE_FLAG) "
				+ "VALUES (?,?,?," + getDbFunction(Constants.SYSDATE, null) + ","
				+ getDbFunction(Constants.SYSDATE, null) + ",?,?,?,?,?)";
		Object[] Userargs = { vObj.getDashboardId(), intCurrentUserId, intCurrentUserId, "0",
				vObj.getRecordIndicatorNt(), intCurrentUserId, "N", "Y" };
		getJdbcTemplate().update(Usersql, Userargs);
		return 1;
	}

	private String generateDashboardID(Long maker) {
		String sql = "";
		if ("ORACLE".equalsIgnoreCase(databaseType)) {
			sql = " SELECT MAX (uniqueValue) FROM ( "
					+ " SELECT MAX (TO_NUMBER (SUBSTR (DASHBOARD_ID, INSTR (DASHBOARD_ID, '_') + 1, LENGTH (DASHBOARD_ID)))) AS uniqueValue "
					+ " FROM (select DASHBOARD_ID from VDD_RS_DASHBOARDS_WIP where MAKER = ? ) WIP "
					+ " UNION "
					+ " SELECT MAX (TO_NUMBER (SUBSTR (DASHBOARD_ID,INSTR (DASHBOARD_ID, '_') + 1, LENGTH (DASHBOARD_ID)))) AS uniqueValue "
					+ " FROM (select DASHBOARD_ID from VDD_RS_DASHBOARDS where MAKER = ? ) MAIN) SSBI_TEMP ";
		} else {
			sql = "SELECT MAX (uniqueValue) FROM ( "
					+ " SELECT MAX (convert (numeric, SUBSTRING (DASHBOARD_ID, CHARINDEX ('_', DASHBOARD_ID) + 1, LEN (DASHBOARD_ID)))) AS uniqueValue "
					+ " FROM (select DASHBOARD_ID from VDD_RS_DASHBOARDS_WIP where MAKER = ? ) WIP "
					+ "UNION "
					+ "SELECT MAX (convert (numeric,  SUBSTRING (DASHBOARD_ID, CHARINDEX ('_', DASHBOARD_ID) + 1, LEN (DASHBOARD_ID)))) AS uniqueValue "
					+ "FROM (select DASHBOARD_ID from VDD_RS_DASHBOARDS where MAKER = ? ) MAIN) SSBI_TEMP";
		}
		Object[] args= {maker, maker};
		Long uniqueValue = getJdbcTemplate().queryForObject(sql, args, Long.class);
		String dashboardId = new String(maker + "_" + ((uniqueValue != null) ? (uniqueValue + 1) : 1));
		return dashboardId;
	}

	public int doUpdateApprForDashboardInWIP(DashboardDesignVb vObj) throws RuntimeCustomException {

		setServiceDefaults();
		ExceptionCode exceptionDesignCode = CommonUtils.jsonToXml(vObj.getDashboardDesignJson());
		if (exceptionDesignCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			vObj.setDashboardDesign(exceptionDesignCode.getResponse() + "");
		}
		ExceptionCode exceptionFilterCode = CommonUtils.jsonToXml(vObj.getFilterContextJson());
		if (exceptionFilterCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			vObj.setFilterContext(exceptionFilterCode.getResponse() + "");
		}

		Object[] args = { vObj.getDashboardId() };
		
		/* DELETE ACTIVITY BEFORE UPDATE */
		getJdbcTemplate().update("DELETE FROM VDD_RS_DASHBOARDS_WIP WHERE DASHBOARD_ID=? ", args);
		getJdbcTemplate().update("DELETE FROM VDD_RS_WIDGETS_WIP WHERE DASHBOARD_ID=? ", args);

		String dashboardSql = "INSERT INTO VDD_RS_DASHBOARDS_WIP "
				+ " (DASHBOARD_ID,DASHBOARD_NAME, DASHBOARD_DESC,RSD_STATUS_NT, RSD_STATUS, "
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION, DASHBOARD_DESIGN, FILTER_CONTEXT) "
				+ " VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?)";

		Object[] argsDash = { vObj.getDashboardId(), vObj.getDashboardName(), vObj.getDashboardDesc(),
				vObj.getRsdStatusNt(), vObj.getRsdStatus(), vObj.getRecordIndicatorNt(), vObj.getRecordIndicator(),
				intCurrentUserId, intCurrentUserId, 0, };

		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int psIndex = 0;
				PreparedStatement ps = connection.prepareStatement(dashboardSql);

				for (int i = 1; i <= argsDash.length; i++) {
					ps.setObject(++psIndex, argsDash[i - 1]);
				}

				String clobData = ValidationUtil.isValid(vObj.getDashboardDesign()) ? vObj.getDashboardDesign() : "";
				ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

				clobData = ValidationUtil.isValid(vObj.getFilterContext()) ? vObj.getFilterContext() : "";
				ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

				return ps;
			}
		});
		String widgetSql = "INSERT INTO VDD_RS_WIDGETS_WIP "
				+ " (DASHBOARD_ID, SUB_DASHBOARD_ID,WIDGETS_ID,RSW_STATUS_NT, RSW_STATUS, "
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION, WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ",?,?)";

		for (WidgetDesignVb widgetVb : vObj.getChildren()) {
			ExceptionCode exceptionwidgetCode = CommonUtils.jsonToXml(widgetVb.getWidgetContextJson());
			if (exceptionwidgetCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				widgetVb.setWidgetContext(exceptionwidgetCode.getResponse() + "");
			}

			ExceptionCode exceptionfilterCode = CommonUtils.jsonToXml(widgetVb.getFilterContextJson());
			if (exceptionfilterCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				widgetVb.setFilterContext(exceptionfilterCode.getResponse() + "");
			}

			Object[] argswidget = { widgetVb.getDashboardId(), widgetVb.getSubDashboardId(), widgetVb.getWidgetId(),
					vObj.getRsdStatusNt(), vObj.getRsdStatus(), widgetVb.getRecordIndicatorNt(),
					widgetVb.getRecordIndicator(), intCurrentUserId, intCurrentUserId, 0 };

			getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = argswidget.length;
					PreparedStatement ps = connection.prepareStatement(widgetSql);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, argswidget[i - 1]);
					}
					String clobData = ValidationUtil.isValid(widgetVb.getWidgetContext()) ? widgetVb.getWidgetContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					String clobFilterData = ValidationUtil.isValid(widgetVb.getFilterContext())
							? widgetVb.getFilterContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobFilterData), clobFilterData.length());
					return ps;
				}
			});
		}
		return 1;
	}

	public int doUpdateApprForDashboardInMain(DashboardDesignVb vObj) throws RuntimeCustomException {

		String dashboardSql = "UPDATE VDD_RS_DASHBOARDS SET RSD_STATUS = ? where DASHBOARD_ID=? ";

		// DASHBOARD UPDATE
		Object[] argsDash = { vObj.getRsdStatus(), vObj.getDashboardId() };
		getJdbcTemplate().update(dashboardSql, argsDash);

		// WIDGET UPDATE
		String widgetSql = "UPDATE VDD_RS_WIDGETS SET RSW_STATUS=? where DASHBOARD_ID=?  ";
		getJdbcTemplate().update(widgetSql, argsDash);

		return 1;
	}

	protected List<DashboardDesignVb> selectRecord(DashboardDesignVb vObject, int intStatus, boolean appendStatus) {
		DashboardDesignVb dashboardVb = getQueryResults(vObject, intStatus, appendStatus);
		List<DashboardDesignVb> dashboardVbList = new ArrayList<DashboardDesignVb>();
		if (dashboardVb != null) {
			dashboardVbList.add(dashboardVb);
		}
		return dashboardVbList;
	}

	public List<DashboardDesignVb> getAllDashboardList() {
		setServiceDefaults();
		String sql = "SELECT DASHTEMP.*,(CASE WHEN (DFAULT_DASH.DEFAULT_DASH_ID !='NA' AND DFAULT_DASH.DEFAULT_DASH_ID IS NOT NULL) THEN 'true' ELSE 'false' END )FAVORITE_DASHBOARD   FROM( SELECT * FROM ( SELECT DASHBOARD.DASHBOARD_ID,"
				+ "  DASHBOARD.DASHBOARD_NAME,  DASHBOARD.DASHBOARD_DESC, ? VISION_ID,   DASHBOARD.RSD_STATUS,  DASHBOARD.MAKER,"
				+ "  (CASE WHEN (GROUPCHILD.DASHBOARD_PRIVATE_FLAG IS NULL ) THEN DASHBOARD.DASHBOARD_PRIVATE_FLAG ELSE  GROUPCHILD.DASHBOARD_PRIVATE_FLAG  END) DASHBOARD_PRIVATE_FLAG, "
				+ "  DASHBOARD.DASHBOARD_WRITE_FLAG"
				+ " FROM (SELECT DASH.DASHBOARD_ID,DASH.DASHBOARD_NAME,DASH.DASHBOARD_DESC,DASH.RSD_STATUS,DASH.MAKER,USRGROUP.DASHBOARD_PRIVATE_FLAG,USRGROUP.DASHBOARD_WRITE_FLAG"
				+ "  FROM VDD_RS_DASHBOARDS DASH,  VDD_DASHBOARDS_LOD_USER_GROUP USRGROUP"
				+ " WHERE DASH.DASHBOARD_ID = USRGROUP.DASHBOARD_ID AND (USRGROUP.USER_GROUP = ? AND USRGROUP.USER_PROFILE = ?)) DASHBOARD"
				+ " left JOIN VDD_DASHBOARDS_LOD_GROUP_CHILD GROUPCHILD ON DASHBOARD.DASHBOARD_ID = GROUPCHILD.DASHBOARD_ID"
				+ " and GROUPCHILD.VISION_ID = ? ) WHERE DASHBOARD_PRIVATE_FLAG !='R' "
				+ " and DASHBOARD_ID not in(select DASHBOARD_ID from VDD_DASHBOARDS_LOD_USER_LVL where VISION_ID = ? ) union SELECT * FROM ( SELECT DASHBOARD.DASHBOARD_ID,"
				+ "  DASHBOARD.DASHBOARD_NAME,  DASHBOARD.DASHBOARD_DESC, ? VISION_ID,   DASHBOARD.RSD_STATUS,   DASHBOARD.MAKER,"
				+ "  (CASE WHEN (GROUPCHILD.DASHBOARD_PRIVATE_FLAG IS NULL ) THEN DASHBOARD.DASHBOARD_PRIVATE_FLAG ELSE  GROUPCHILD.DASHBOARD_PRIVATE_FLAG  END) DASHBOARD_PRIVATE_FLAG, "
				+ "  DASHBOARD.DASHBOARD_WRITE_FLAG"
				+ " FROM (SELECT DASH.DASHBOARD_ID,DASH.DASHBOARD_NAME,DASH.DASHBOARD_DESC,DASH.RSD_STATUS,DASH.MAKER,USRGROUP.DASHBOARD_PRIVATE_FLAG,USRGROUP.DASHBOARD_WRITE_FLAG"
				+ "  FROM VDD_RS_DASHBOARDS_WIP DASH,  VDD_DASHBOARDS_LOD_USER_GROUP USRGROUP"
				+ " WHERE DASH.DASHBOARD_ID = USRGROUP.DASHBOARD_ID AND (USRGROUP.USER_GROUP = ? AND USRGROUP.USER_PROFILE = ?)) DASHBOARD "
				+ " left JOIN VDD_DASHBOARDS_LOD_GROUP_CHILD GROUPCHILD ON DASHBOARD.DASHBOARD_ID = GROUPCHILD.DASHBOARD_ID"
				+ " and GROUPCHILD.VISION_ID =  ?) "
				+ " where DASHBOARD_PRIVATE_FLAG  !='R' and DASHBOARD_WRITE_FLAG = 'Y' "
				+ " and DASHBOARD_ID not in(select DASHBOARD_ID from VDD_DASHBOARDS_LOD_USER_LVL where VISION_ID = ? ) UNION  SELECT T1.DASHBOARD_ID,    T1.DASHBOARD_NAME,"
				+ " T1.DASHBOARD_DESC, ? VISION_ID,  T1.RSD_STATUS, T1.MAKER,"
				+ " T2.DASHBOARD_PRIVATE_FLAG, T2.DASHBOARD_WRITE_FLAG  FROM VDD_RS_DASHBOARDS  T1, "
				+ "  VDD_DASHBOARDS_LOD_USER_LVL T2   where T1.DASHBOARD_ID = T2.DASHBOARD_ID  "
				+ "  AND T2.VISION_ID = ?  AND T2.DASHBOARD_PRIVATE_FLAG != 'R' "
				+ " UNION  SELECT T1.DASHBOARD_ID,    T1.DASHBOARD_NAME, T1.DASHBOARD_DESC, ?  VISION_ID,  T1.RSD_STATUS, T1.MAKER, T2.DASHBOARD_PRIVATE_FLAG,"
				+ " T2.DASHBOARD_WRITE_FLAG  FROM VDD_RS_DASHBOARDS_WIP  T1, "
				+ "  VDD_DASHBOARDS_LOD_USER_LVL T2   WHERE T1.DASHBOARD_ID = T2.DASHBOARD_ID  "
				+ "  AND T2.VISION_ID = ? and T2.DASHBOARD_WRITE_FLAG = 'Y'"
				+ " AND T2.DASHBOARD_PRIVATE_FLAG != 'R' ) DASHTEMP LEFT OUTER JOIN USER_GROUP_PROPERTIES_SSBI DFAULT_DASH "
				+ " ON DFAULT_DASH.VISION_ID = DASHTEMP.VISION_ID AND  DASHTEMP.DASHBOARD_ID = DFAULT_DASH.DEFAULT_DASH_ID"
				+ " ORDER BY CASE WHEN DASHTEMP.RSD_STATUS = '0' THEN 1 WHEN DASHTEMP.RSD_STATUS = '2' THEN 2 WHEN DASHTEMP.RSD_STATUS = '1' THEN 3 END, DASHTEMP.RSD_STATUS, DASHTEMP.DASHBOARD_NAME";
		
		Object[] args = { intCurrentUserId, userGroup, userProfile, intCurrentUserId, intCurrentUserId,
				intCurrentUserId, userGroup, userProfile, intCurrentUserId, intCurrentUserId, intCurrentUserId,
				intCurrentUserId, intCurrentUserId, intCurrentUserId };
		
		List<DashboardDesignVb> dashboardList = getJdbcTemplate().query(sql, new RowMapper<DashboardDesignVb>() {
			@Override
			public DashboardDesignVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				DashboardDesignVb dashboardVb = new DashboardDesignVb();
				dashboardVb.setDashboardId(rs.getString("DASHBOARD_ID"));
				dashboardVb.setDashboardName(rs.getString("DASHBOARD_NAME"));
				dashboardVb.setDashboardDesc(rs.getString("DASHBOARD_DESC"));
				dashboardVb.setRsdStatus(rs.getInt("RSD_STATUS"));
				dashboardVb.setMaker(rs.getLong("MAKER"));
				dashboardVb.setPrivateFlag(rs.getString("DASHBOARD_PRIVATE_FLAG"));
				dashboardVb.setWriteFlag(rs.getString("DASHBOARD_WRITE_FLAG"));
				dashboardVb.setFavoriteDashboard(Boolean.valueOf(rs.getString("FAVORITE_DASHBOARD")));
				return dashboardVb;
			}
		}, args);

		return dashboardList;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doPublishToMain(DashboardDesignVb dashboardVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setServiceDefaults();
			strCurrentOperation = "Publish";

			/* Get version number to be used */
			int versionNumber = getMaxVersionNumber(dashboardVb);
			versionNumber++;

			/* Move MAIN tables data to AD tables */
			moveMainDataToAD(dashboardVb, versionNumber);

			/* Delete data from MAIN tables */
			deleteDataFromTables(dashboardVb, true);

			/* Move WIP tables data to MAIN tables */
			moveWipDataToMain(dashboardVb);

			/* Delete data from WIP tables */
			deleteDataFromTables(dashboardVb, false);

		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	private void moveWipDataToMain(DashboardDesignVb dashboardVb) {
		ExceptionCode exceptionCode = new ExceptionCode();

		/* For DASHBOARD PARENT */
		String sql = "SELECT COUNT(1) FROM VDD_RS_DASHBOARDS_WIP WHERE DASHBOARD_ID=? ";

		Object[] args = { dashboardVb.getDashboardId() };

		int existanceCount = getJdbcTemplate().queryForObject(sql, args, Integer.class);
		if (existanceCount == 0) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("No records in work in progress table(VDD_RS_DASHBOARDS_WIP)");
			throw buildRuntimeCustomException(exceptionCode);
		}
		sql = "INSERT INTO VDD_RS_DASHBOARDS (DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN, "
				+ "FILTER_CONTEXT,RSD_STATUS_NT,RSD_STATUS,RECORD_INDICATOR_NT,"
				+ "RECORD_INDICATOR, MAKER, VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION) "
				+ "SELECT DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,FILTER_CONTEXT,RSD_STATUS_NT,"
				+ " 0 as RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,? as MAKER,? as VERIFIER,"
				+ "INTERNAL_STATUS,sysdate as DATE_LAST_MODIFIED,DATE_CREATION FROM VDD_RS_DASHBOARDS_WIP "
				+ " WHERE DASHBOARD_ID =?";

		args = new Object[] { intCurrentUserId, intCurrentUserId, dashboardVb.getDashboardId() };
		getJdbcTemplate().update(sql, args);

		/* For DASHBOARD CHILD(WIDGET) */
		sql = "SELECT COUNT(1) FROM VDD_RS_WIDGETS_WIP WHERE DASHBOARD_ID=?";

		args = new Object[] { dashboardVb.getDashboardId() };

		existanceCount = getJdbcTemplate().queryForObject(sql, args, Integer.class);
		if (existanceCount == 0) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("No records in work in progress table(VDD_RS_WIDGETS_WIP)");
			throw buildRuntimeCustomException(exceptionCode);
		}
		sql = "INSERT INTO VDD_RS_WIDGETS (DASHBOARD_ID,SUB_DASHBOARD_ID,WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,RSW_STATUS_NT,RSW_STATUS,RECORD_INDICATOR_NT,"
				+ "RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION) "
				+ "SELECT DASHBOARD_ID,SUB_DASHBOARD_ID,WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,RSW_STATUS_NT,0 RSW_STATUS,RECORD_INDICATOR_NT,"
				+ "RECORD_INDICATOR,? as MAKER,? as VERIFIER, INTERNAL_STATUS, "
				+ getDbFunction(Constants.SYSDATE, null)
				+ " as  DATE_LAST_MODIFIED, DATE_CREATION FROM VDD_RS_WIDGETS_WIP  WHERE DASHBOARD_ID =?";

		args = new Object[] { intCurrentUserId, intCurrentUserId, dashboardVb.getDashboardId() };
		getJdbcTemplate().update(sql, args);

	}

	private void moveMainDataToAD(DashboardDesignVb dashboardVb, int versionNumber) {

		/* For DASHBOARD PARENT */
		String sql = "INSERT INTO VDD_RS_DASHBOARDS_AD (VERSION_NO, DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,"
				+ "FILTER_CONTEXT,RSD_STATUS_NT,RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION) SELECT ? AS VERSION_NO, DASHBOARD_ID,"
				+ "DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,"
				+ "FILTER_CONTEXT,RSD_STATUS_NT,RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION "
				+ "FROM VDD_RS_DASHBOARDS WHERE DASHBOARD_ID=?";
		Object[] args = { versionNumber, dashboardVb.getDashboardId() };
		getJdbcTemplate().update(sql, args);

		/* For DASHBOARD CHILD(WIDGET) */
		sql = "INSERT INTO VDD_RS_WIDGETS_AD (VERSION_NO, DASHBOARD_ID,SUB_DASHBOARD_ID,WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,"
				+ "RSW_STATUS_NT,RSW_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION) SELECT ? AS VERSION_NO, DASHBOARD_ID,SUB_DASHBOARD_ID,"
				+ "WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,RSW_STATUS_NT,RSW_STATUS,RECORD_INDICATOR_NT,"
				+ " RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION"
				+ " FROM VDD_RS_WIDGETS WHERE DASHBOARD_ID=?";
		getJdbcTemplate().update(sql, args);

	}

	private void moveMainDataToWIP(DashboardDesignVb dashboardVb) {

		/* For DASHBOARD PARENT */
		String sql = "INSERT INTO VDD_RS_DASHBOARDS_WIP ( DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,"
				+ "FILTER_CONTEXT,RSD_STATUS_NT,RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION)  "
				+ "SELECT DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,"
				+ "FILTER_CONTEXT,RSD_STATUS_NT,2 RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION "
				+ "FROM VDD_RS_DASHBOARDS WHERE DASHBOARD_ID=?";
		Object[] args = { dashboardVb.getDashboardId() };
		getJdbcTemplate().update(sql, args);

		/* For DASHBOARD CHILD(WIDGET) */
		sql = "INSERT INTO VDD_RS_WIDGETS_WIP ( DASHBOARD_ID,SUB_DASHBOARD_ID,WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,"
				+ "RSW_STATUS_NT,RSW_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION)"
				+ " SELECT  DASHBOARD_ID,SUB_DASHBOARD_ID,"
				+ "WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,RSW_STATUS_NT,2 RSW_STATUS,RECORD_INDICATOR_NT,"
				+ " RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION"
				+ " FROM VDD_RS_WIDGETS WHERE DASHBOARD_ID=?";
		getJdbcTemplate().update(sql, args);

	}

	private int getMaxVersionNumber(DashboardDesignVb dashboardVb) {
		String sql = "SELECT CASE WHEN MAX(VERSION_NO) IS NULL THEN 0 ELSE MAX(VERSION_NO) END VERSION_NO FROM VDD_RS_DASHBOARDS_AD where DASHBOARD_ID = ?";
		Object args[] = { dashboardVb.getDashboardId() };
		return getJdbcTemplate().queryForObject(sql, args, Integer.class);
	}

	public void deleteDataFromTables(DashboardDesignVb dashboardVb, boolean mainTable) {
		Object[] args = { dashboardVb.getDashboardId() };
		if (mainTable) {
			/* For DASHBOARD PARENT */
			String sql = "DELETE FROM VDD_RS_DASHBOARDS where DASHBOARD_ID=?";

			getJdbcTemplate().update(sql, args);

			/* For DASHBOARD CHILD(WIDGET) */
			sql = "DELETE FROM VDD_RS_WIDGETS where DASHBOARD_ID=? ";
			getJdbcTemplate().update(sql, args);

		} else {
			/* For DASHBOARD PARENT */
			String sql = "DELETE FROM VDD_RS_DASHBOARDS_WIP WHERE DASHBOARD_ID=? ";
			getJdbcTemplate().update(sql, args);

			/* For DASHBOARD CHILD(WIDGET) */
			sql = "DELETE FROM VDD_RS_WIDGETS_WIP WHERE DASHBOARD_ID = ? ";
			getJdbcTemplate().update(sql, args);

		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertApprRecordforDashboardWIP(DashboardDesignVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<DashboardDesignVb> collTemp = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		try {
			/* Check if record already published */
			collTemp = selectRecord(vObject, 0, false);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() > 0) {
				int intStaticDeletionFlag = collTemp.get(0).getRsdStatus();

				/* If record already in [Published & Deleted] status, throw error */
				if (intStaticDeletionFlag == Constants.PASSIVATE) {
					exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
					throw buildRuntimeCustomException(exceptionCode);
				} else {
					// Change of logic Start //
					/* If record already published, set status as [Published & Work In Progress] */
					// vObject.setVcStatus(2);
					// Change of logic End //
					/* If record already published, throw error */
					exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			} else {
				/* If record not in main table, set status as [Work In Progress] */
				vObject.setRsdStatus(1);
			}
			/* Check if record exists in WIP table */
			collTemp = selectRecord(vObject, 1, false);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			/* If record exists in WIP table, throw error */
			if (collTemp.size() > 0) {
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			String sysDate = getSystemDate();
			vObject.setDateCreation(sysDate);
			vObject.setDateLastModified(sysDate);
			retVal = doInsertionApprforDashboardIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(strErrorDesc)) {
					retVal = 20;
				}
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}
//			retVal = doInsertionApprforVcCatalogAccessIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(strErrorDesc)) {
					retVal = 20;
				}
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}
			return exceptionCode;
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doSaveOperationsWIP(DashboardDesignVb vcMainVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setServiceDefaults();
			vcMainVb.setMaker(intCurrentUserId);
			vcMainVb.setVerifier(intCurrentUserId);
			strCurrentOperation = "add".equalsIgnoreCase(vcMainVb.getActionType()) ? "Add" : "Modify";
			/* Validate and Insert or Modify Catalog WIP */
			doValidateAndSaveCatalogMain(vcMainVb);
			/* Insert/modify hash variables */
//			 doInsertUpdateInVcHashVariableWIP(vcForCatalogWrapperVb);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	public ExceptionCode doValidateAndSaveCatalogMain(DashboardDesignVb vcMainVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = "add".equalsIgnoreCase(vcMainVb.getActionType()) ? "Add" : "Modify";
		List<DashboardDesignVb> dashboardWipList = selectRecord(vcMainVb, 1, false);
		List<DashboardDesignVb> dashboardMainList = selectRecord(vcMainVb, 0, false);
		if (ValidationUtil.isValidList(dashboardWipList)) {
			if ("add".equalsIgnoreCase(operation)) {
				exceptionCode = CommonUtils.getResultObject("Dashboard", Constants.DUPLICATE_KEY_INSERTION, operation,
						null);
				throw new RuntimeCustomException(exceptionCode);
			} else {
				if (0 == vcMainVb.getRsdStatus() && ValidationUtil.isValidList(dashboardMainList)) {
					exceptionCode = CommonUtils.getResultObject("Dashboard", Constants.WE_HAVE_ERROR_DESCRIPTION,
							operation, "Cannot create multiple instance for same Dashboard in pending Table.");
					throw new RuntimeCustomException(exceptionCode);
				}
				retVal = doUpdateApprForDashboardInWIP(vcMainVb);
				if (retVal == Constants.SUCCESSFUL_OPERATION)
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				else
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			}
			return exceptionCode;
		}

		if (ValidationUtil.isValidList(dashboardMainList)) {
			DashboardDesignVb catalogMainVb = dashboardMainList.get(0);
			if ("add".equalsIgnoreCase(operation)) {
				exceptionCode = CommonUtils.getResultObject("Dashboard",
						(catalogMainVb.getRsdStatus() == 9 ? Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE
								: Constants.DUPLICATE_KEY_INSERTION),
						operation, null);
				throw new RuntimeCustomException(exceptionCode);
			} else {
				if (catalogMainVb.getRsdStatus() == 9 && vcMainVb.getRsdStatus() == 9) {
					exceptionCode = CommonUtils.getResultObject("Dashboard", Constants.CANNOT_DELETE_AN_INACTIVE_RECORD,
							operation, null);
					throw new RuntimeCustomException(exceptionCode);
				} else {
					moveMainDataToWIP(vcMainVb);
					vcMainVb.setRsdStatus(2);
					retVal = doUpdateApprForDashboardInWIP(vcMainVb);
					if (retVal == Constants.SUCCESSFUL_OPERATION)
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					else
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				}
			}
			return exceptionCode;
		} else {
			if ("add".equalsIgnoreCase(operation)) {
				retVal = doInsertionApprforDashboardIntoWIP(vcMainVb);
				if (retVal == Constants.SUCCESSFUL_OPERATION)
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				else
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			} else {
				exceptionCode = CommonUtils.getResultObject("Catalog", Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD,
						operation, null);
				throw new RuntimeCustomException(exceptionCode);
			}
			return exceptionCode;
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doUpdateRecordDashboardIntoWIP(DashboardDesignVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		try {
			return doUpdateRecordForNonTransDashboardIntoWIP(vObject);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			// ex.printStackTrace();
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	private ExceptionCode doUpdateRecordForNonTransDashboardIntoWIP(DashboardDesignVb vObject) {
		List<DashboardDesignVb> collTempMain = null;
		List<DashboardDesignVb> collTempWIP = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* Check record existence in main table */
		collTempMain = selectRecord(vObject, 0, false);
		if (collTempMain == null) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		if (collTempMain.size() > 0) {
			/*
			 * Check to block modification of deleted record before changing it to [Work In
			 * Progress] status
			 */
			int intStaticDeletionFlag = collTempMain.get(0).getRsdStatus();
			if (intStaticDeletionFlag == Constants.PASSIVATE && vObject.getRsdStatus() == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				vObject.setRsdStatus(2);
				vObject.setDateCreation(collTempMain.get(0).getDateCreation());
			}
		}

		/* Check record existence in _WIP table */
		collTempWIP = selectRecord(vObject, 1, false);
		if (collTempWIP == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* If record not available in both MAIN and _WIP table, throw exception */
		if (collTempWIP.size() == 0 && collTempMain.size() == 0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/*
		 * Unless the status of record being modified is [Published & Work In Progress],
		 * the status of records in _WIP table must be [Work In Progress]
		 */
		if (!(2 == vObject.getRsdStatus())) {
			vObject.setRsdStatus(1);
		}

		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setMaker(getIntCurrentUserId());
		vObject.setVerifier(getIntCurrentUserId());
		if (collTempWIP.size() > 0) {
			retVal = doUpdateApprForDashboardInWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
//			retVal = doUpdateApprForVcCatalogAccessInPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			String sysDate = getSystemDate();
			vObject.setDateCreation(collTempMain.get(0).getDateCreation());
			vObject.setDateLastModified(sysDate);
			retVal = doInsertionApprforDashboardIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				vObject.setRsdStatus(2);
			}

//			retVal = doInsertionApprforVcCatalogAccessIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				vObject.setRsdStatus(2);
			}
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteRecordDashboardFromPend(DashboardDesignVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		try {
			return doDeleteRecordForNonTransDashboardFromWIP(vObject);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	protected ExceptionCode doDeleteRecordForNonTransDashboardFromWIP(DashboardDesignVb vObject)
			throws RuntimeCustomException {
		List<DashboardDesignVb> collTempMain = null;
		List<DashboardDesignVb> collTempWIP = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();

		DashboardDesignVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* Fetch record from MAIN table */
		collTempMain = selectRecord(vObject, 0, false);
		if (collTempMain == null) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* Fetch record from WIP table */
		collTempWIP = selectRecord(vObject, 1, false);
		if (collTempWIP == null) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* If record not in both MAIN and WIP table, throw exception */
		if (collTempMain.size() == 0 && collTempWIP.size() == 0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* If record exist in MAIN table */
		if (collTempMain.size() > 0) {
			/*
			 * If current status equal to status from MAIN table => attempt to delete record
			 * from MAIN table
			 */
			if (vObject.getRsdStatus() == collTempMain.get(0).getRsdStatus()) {
				/* If parallel record exist in WIP table, throw exception */
				if (collTempWIP.size() > 0) {
					exceptionCode = CommonUtils.getResultObject(serviceDesc, Constants.WE_HAVE_ERROR_DESCRIPTION,
							Constants.DELETE,
							"Cannot delete record from MAIN table when parallel record exist in WIP table");
					throw buildRuntimeCustomException(exceptionCode);
				} else if (vObject.getRsdStatus() == Constants.PASSIVATE) {
					exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
					throw buildRuntimeCustomException(exceptionCode);
				} else {
					/* Update MAIN table record with status [Published & Deleted] */
					vObjectlocal = collTempMain.get(0);
					vObjectlocal.setRsdStatus(9);
					retVal = doUpdateApprForDashboardInMain(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					} else {
						vObject.setRsdStatus(9);
					}
				}
			}
		}

		/*
		 * If record exist only in pend table, delete record from WIP table Also remove
		 * configurations from other tables based on the current DASHBOARD_ID
		 */
		if (collTempWIP.size() > 0) {
			retVal = deleteFromTablesWithDashboard(vObject.getDashboardId(), "VDD_RS_DASHBOARDS_WIP");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				strErrorDesc = "Unable to delete record from VDD_RS_DASHBOARDS_WIP";
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			retVal = deleteFromTablesWithDashboard(vObject.getDashboardId(), "VDD_RS_WIDGETS_WIP");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				strErrorDesc = "Unable to delete record from VDD_RS_WIDGETS_WIP";
				exceptionCode = getResultObject(20);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected int deleteFromTablesWithDashboard(String dashboardId, String tableName) {
		int result = 0;
		String query = "Delete From "+tableName+" Where DASHBOARD_ID = ?";
		Object[] args = { dashboardId };
		try {
			getJdbcTemplate().update(query, args);
			result = 1;
		} catch (Exception e) {
			// e.printStackTrace();
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	public int doInsertRecordForDashboardLOD(DashboardLODWrapperVb dsLODWrapperVb) {
		// use it for reference 
		setServiceDefaults();
		DashboardDesignVb mainObject = dsLODWrapperVb.getMainModel();
		String sql = "";
		String tableName = mainObject.getTableName();
		if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
			sql = "DELETE FROM " + tableName + " WHERE CATEGORY_ID = ? AND country = ?  AND le_book = ? ";
		} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
			sql = "DELETE FROM " + tableName + " WHERE CONNECTOR_ID = ? ";
		}
		// 
		Object[] args = null;
		
		if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
			args = new Object[] {  mainObject.getDashboardId(), mainObject.getCountry(),
					mainObject.getLeBook() };
		} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
			args = new Object[] {  mainObject.getDashboardId() };
		}
		
		try {
			getJdbcTemplate().update(sql, args);
		} catch (Exception e) {
		}
		try {
			if (ValidationUtil.isValidList(dsLODWrapperVb.getLodProfileList())) {
				if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
					sql = "INSERT INTO  " + tableName + " "
							+ " (COUNTRY,LE_BOOK,CATEGORY_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
							+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,USER_GROUP_AT,USER_GROUP,USER_PROFILE_AT,"
							+ " USER_PROFILE,WRITE_FLAG,VISION_ID) VALUES (?,?,?,?,?,"
							+ getDbFunction(Constants.SYSDATE, null) + ","
							+ getDbFunction(Constants.SYSDATE, null) + ",?,?,?,?,?,?,?,?)";

				} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
					sql = "INSERT INTO  " + tableName + " "
							+ " (CONNECTOR_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
							+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,USER_GROUP_AT,USER_GROUP,USER_PROFILE_AT,"
							+ " USER_PROFILE,WRITE_FLAG,VISION_ID) VALUES (?,?,?,"
							+ getDbFunction(Constants.SYSDATE, null) + ","
							+ getDbFunction(Constants.SYSDATE, null) + ",?,?,?,?,?,?,?,?)";
				}
				for (LevelOfDisplayVb vObject : dsLODWrapperVb.getLodProfileList()) {
					if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
						Object[] args1 = { mainObject.getCountry(), mainObject.getLeBook(), mainObject.getDashboardId(),
								mainObject.getMaker(), mainObject.getMaker(), "0", mainObject.getRecordIndicatorNt(), 1,
								vObject.getUserGroup(), 2, vObject.getUserProfile(), vObject.getWriteFlag(), 0 };
						getJdbcTemplate().update(sql, args1);

					} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
						Object[] args1 = { mainObject.getDashboardId(), mainObject.getMaker(), mainObject.getMaker(),
								"0", mainObject.getRecordIndicatorNt(), 1, vObject.getUserGroup(), 2,
								vObject.getUserProfile(), vObject.getWriteFlag(), 0 };
						getJdbcTemplate().update(sql, args1);
					}
				}
			}
			if (ValidationUtil.isValidList(dsLODWrapperVb.getLodUserList())) {
				if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
					sql = "INSERT INTO  " + tableName + " "
							+ " (COUNTRY,LE_BOOK,CATEGORY_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
							+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,USER_GROUP_AT,USER_GROUP,USER_PROFILE_AT,"
							+ " USER_PROFILE,WRITE_FLAG,VISION_ID) VALUES (?,?,?,?,?,"
							+ getDbFunction(Constants.SYSDATE, null) + ","
							+ getDbFunction(Constants.SYSDATE, null) + ",?,?,?,?,?,?,?,?)";

				} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
					sql = "INSERT INTO  " + tableName + " "
							+ " (CONNECTOR_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
							+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,USER_GROUP_AT,USER_GROUP,USER_PROFILE_AT,"
							+ " USER_PROFILE,WRITE_FLAG,VISION_ID) VALUES (?,?,?,"
							+ getDbFunction(Constants.SYSDATE, null) + ","
							+ getDbFunction(Constants.SYSDATE, null) + ",?,?,?,?,?,?,?,?)";
				}
				for (LevelOfDisplayUserVb vObject : dsLODWrapperVb.getLodUserList()) {
					if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
						Object[] param = { mainObject.getCountry(), mainObject.getLeBook(), mainObject.getDashboardId(),
								mainObject.getMaker(), mainObject.getMaker(), "0", mainObject.getRecordIndicatorNt(), 1,
								vObject.getUserGroup(), 2, vObject.getUserProfile(), vObject.getWriteFlag(),
								vObject.getVisionId() };
						getJdbcTemplate().update(sql, param);

					} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(mainObject.getTableName())) {
						Object[] param = { mainObject.getDashboardId(), mainObject.getMaker(), mainObject.getMaker(),
								"0", mainObject.getRecordIndicatorNt(), 1, vObject.getUserGroup(), 2,
								vObject.getUserProfile(), vObject.getWriteFlag(), vObject.getVisionId() };
						getJdbcTemplate().update(sql, param);
					}
				}
			}
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			throw e;
		}
	}

	private String getUserString(List<LevelOfDisplayUserVb> lodUserList) {
		String userStr = "";
		for (LevelOfDisplayUserVb userVbObj : lodUserList) {
			userStr += "'" + userVbObj.getVisionId() + "',";
		}
		if (ValidationUtil.isValidList(lodUserList)) {
			return userStr.substring(0, userStr.length() - 1);
		} else {
			return "''";
		}
	}

	private String getGroupProfileString(List<LevelOfDisplayVb> lodProfileList) {
		String groupStr = "";
		for (LevelOfDisplayVb lodVbObj : lodProfileList) {
			groupStr += "'" + lodVbObj.getUserGroup() + "_" + lodVbObj.getUserProfile() + "',";
		}

		if (ValidationUtil.isValidList(lodProfileList)) {
			return groupStr.substring(0, groupStr.length() - 1);
		} else {
			return "''";
		}
	}

	public int rejectDashboard(DashboardDesignVb dashboardVb) throws RuntimeCustomException {
		setServiceDefaults();
		Object[] args = { dashboardVb.getDashboardId(), intCurrentUserId };
		Integer count = getJdbcTemplate().queryForObject(
				" select count(*) from VDD_DASHBOARDS_LOD_USER_LVL where DASHBOARD_ID= ? and  VISION_ID=? ",
				Integer.class, args);
		if (count > 0) {
			getJdbcTemplate().update(
					"update  VDD_DASHBOARDS_LOD_USER_LVL  set DASHBOARD_PRIVATE_FLAG ='R'  where DASHBOARD_ID= ? and  VISION_ID=? ",
					args);
		} else {
			String privateQuery = "INSERT INTO VDD_DASHBOARDS_LOD_GROUP_CHILD "
					+ " (DASHBOARD_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
					+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,USER_GROUP,"
					+ " USER_PROFILE,VISION_ID,DASHBOARD_PRIVATE_FLAG,DASHBOARD_WRITE_FLAG) VALUES (?,?,?,"
					+ getDbFunction(Constants.SYSDATE, null) + "," + getDbFunction(Constants.SYSDATE, null)
					+ ",?,?,?,?,?,?,?)";
			Object[] Userargs = { dashboardVb.getDashboardId(), dashboardVb.getMaker(), dashboardVb.getVerifier(), "0",
					dashboardVb.getRecordIndicatorNt(), userGroup, userProfile, intCurrentUserId, "R",
					dashboardVb.getWriteFlag() };
			getJdbcTemplate().update(privateQuery, Userargs);
		}
		return 1;
	}

	public int acceptDashboard(DashboardDesignVb dashboardVb) throws RuntimeCustomException {
		setServiceDefaults();
		Object[] args = { dashboardVb.getDashboardId(), intCurrentUserId };
		Integer userCount = getJdbcTemplate().queryForObject(
				"select COUNT(*) from VDD_DASHBOARDS_LOD_USER_LVL  where DASHBOARD_ID = ? and VISION_ID = ? ",
				Integer.class, args);
		if (userCount > 0) {
			if (dashboardVb.getPrivateFlag().toUpperCase().contains("Y")) {
				// generating Dynamic DashboardID
				String dashboardId = generateDashboardID(intCurrentUserId);
				String privateQuery = "update  VDD_DASHBOARDS_LOD_USER_LVL  set DASHBOARD_PRIVATE_FLAG ='Y',DASHBOARD_WRITE_FLAG='Y'"
						+ "  where DASHBOARD_ID= ? and  VISION_ID=?";
				getJdbcTemplate().update(privateQuery, args);
				moveMainDataToWIPForPrivileges(dashboardVb, dashboardId);
			} else {
				String publicQuery = "update  VDD_DASHBOARDS_LOD_USER_LVL  set DASHBOARD_PRIVATE_FLAG ='N'  where DASHBOARD_ID= ? and  VISION_ID=?";
				getJdbcTemplate().update(publicQuery, args);
				dashboardVb.setRsdStatus(0);
			}
		} else {
			if (dashboardVb.getPrivateFlag().toUpperCase().contains("Y")) {
				// generating Dynamic DashboardID
				String dashboardId = generateDashboardID(intCurrentUserId);

				String sql = "UPDATE VDD_DASHBOARDS_LOD_GROUP_CHILD  SET DASHBOARD_PRIVATE_FLAG ='R' WHERE DASHBOARD_ID = ? AND VISION_ID =? ";
				getJdbcTemplate().update(sql, args);

				String privateQuery = "INSERT INTO VDD_DASHBOARDS_LOD_USER_LVL "
						+ " (DASHBOARD_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
						+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,"
						+ " VISION_ID,DASHBOARD_PRIVATE_FLAG,DASHBOARD_WRITE_FLAG) VALUES (?,?,?,"
						+ getDbFunction(Constants.SYSDATE, null) + "," + getDbFunction(Constants.SYSDATE, null)
						+ ",?,?,?,?,?)";

				Object[] userargs = { dashboardId, dashboardVb.getMaker(), dashboardVb.getVerifier(), "0",
						dashboardVb.getRecordIndicatorNt(), intCurrentUserId, "Y", "Y" };

				getJdbcTemplate().update(privateQuery, userargs);

				moveMainDataToWIPForPrivileges(dashboardVb, dashboardId);
			} else {
				String publicQuery = "INSERT INTO VDD_DASHBOARDS_LOD_GROUP_CHILD "
						+ " (DASHBOARD_ID,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION,"
						+ " RECORD_INDICATOR,RECORD_INDICATOR_NT,USER_GROUP,"
						+ " USER_PROFILE,VISION_ID,DASHBOARD_PRIVATE_FLAG,DASHBOARD_WRITE_FLAG) VALUES (?,?,?,"
						+ getDbFunction(Constants.SYSDATE, null) + "," + getDbFunction(Constants.SYSDATE, null)
						+ ",?,?,?,?,?,?,?)";

				Object[] userargs = { dashboardVb.getDashboardId(), dashboardVb.getMaker(), dashboardVb.getVerifier(),
						"0", dashboardVb.getRecordIndicatorNt(), userGroup, userProfile, intCurrentUserId, "N",
						dashboardVb.getWriteFlag() };

				getJdbcTemplate().update(publicQuery, userargs);
				dashboardVb.setRsdStatus(0);
			}
		}
		return 1;
	}

	private void moveMainDataToWIPForPrivileges(DashboardDesignVb dashboardVb, String dashboardId)
			throws RuntimeCustomException {

		/* For DASHBOARD PARENT */
		String sql = "INSERT INTO VDD_RS_DASHBOARDS_WIP ( DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,"
				+ "FILTER_CONTEXT,RSD_STATUS_NT,RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,DASHBOARD_PRIVATE_FLAG)  "
				+ "SELECT ? DASHBOARD_ID,DASHBOARD_NAME,DASHBOARD_DESC,DASHBOARD_DESIGN,"
				+ "FILTER_CONTEXT,RSD_STATUS_NT,1 RSD_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,? MAKER,"
				+ " ? VERIFIER,INTERNAL_STATUS, sysdate DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION,'Y' DASHBOARD_PRIVATE_FLAG "
				+ "FROM VDD_RS_DASHBOARDS WHERE DASHBOARD_ID=? and MAKER =?";
		Object[] args = { dashboardId, intCurrentUserId, intCurrentUserId, dashboardVb.getDashboardId(),
				dashboardVb.getMaker() };
		getJdbcTemplate().update(sql, args);

		/* For DASHBOARD CHILD(WIDGET) */
		sql = "INSERT INTO VDD_RS_WIDGETS_WIP ( DASHBOARD_ID,SUB_DASHBOARD_ID,WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,"
				+ "RSW_STATUS_NT,RSW_STATUS,RECORD_INDICATOR_NT,RECORD_INDICATOR,"
				+ "INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,MAKER,VERIFIER) SELECT ? DASHBOARD_ID,SUB_DASHBOARD_ID,"
				+ "WIDGETS_ID,WIDGET_CONTEXT,WIDGET_FILTER_CONTEXT,RSW_STATUS_NT,1 RSW_STATUS,RECORD_INDICATOR_NT,"
				+ " RECORD_INDICATOR,INTERNAL_STATUS," + getDbFunction(Constants.SYSDATE, null) + " DATE_LAST_MODIFIED,"
				+ getDbFunction(Constants.SYSDATE, null) + " DATE_CREATION, ? MAKER, ? VERIFIER "
				+ " FROM VDD_RS_WIDGETS WHERE DASHBOARD_ID=? and MAKER =?";
		getJdbcTemplate().update(sql, args);
		dashboardVb.setDashboardId(dashboardId);
		dashboardVb.setRsdStatus(1);

	}

	public DashboardLODWrapperVb getRecordForDashboardLOD(DashboardLODWrapperVb DashboardMain) {
		try {
			String sql = "";
			String tableName = DashboardMain.getMainModel().getTableName();
			
			if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(DashboardMain.getMainModel().getTableName())) {
				sql = " SELECT USER_GROUP, USER_PROFILE, WRITE_FLAG, VISION_ID  FROM " + tableName + " "
						+ " WHERE  UPPER (CATEGORY_ID) = UPPER (?)  UNION  SELECT T2.USER_GROUP, T2.USER_PROFILE, T1.WRITE_FLAG,"
						+ "T1.VISION_ID FROM "
						+ tableName + " T1, ETL_USER_VIEW T2 "
						+ " WHERE T1.USER_GROUP = T2.USER_GROUP and T1.USER_PROFILE = T2.USER_PROFILE AND T1.CATEGORY_ID = ? and T1.COUNTRY =? and T1.LE_BOOK =? ";

			} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(DashboardMain.getMainModel().getTableName())) {
				sql = " SELECT USER_GROUP, USER_PROFILE, WRITE_FLAG , VISION_ID  FROM " + tableName + " "
						+ " WHERE  UPPER (CONNECTOR_ID) = UPPER (?)  UNION  SELECT T2.USER_GROUP, T2.USER_PROFILE, "
						+ " T1.WRITE_FLAG, T1.VISION_ID  FROM "
						+ tableName + " T1, ETL_USER_VIEW T2 "
						+ " WHERE T1.USER_GROUP = T2.USER_GROUP and T1.USER_PROFILE = T2.USER_PROFILE  AND T1.CONNECTOR_ID = ? ";
			}
			 
			Object[] args = null;

			if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(DashboardMain.getMainModel().getTableName())) {
				args = new Object[] { DashboardMain.getMainModel().getDashboardId(),
						DashboardMain.getMainModel().getDashboardId(), DashboardMain.getMainModel().getCountry(),
						DashboardMain.getMainModel().getLeBook() };
			} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(DashboardMain.getMainModel().getTableName())) {
				args = new Object[] { DashboardMain.getMainModel().getDashboardId(),
						DashboardMain.getMainModel().getDashboardId() };
			}

			List<LevelOfDisplayVb> profileList = getJdbcTemplate().query(sql, new RowMapper<LevelOfDisplayVb>() {
				@Override
				public LevelOfDisplayVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					if (rs.getInt("VISION_ID") == 0) {
						return new LevelOfDisplayVb(rs.getString("USER_GROUP"), rs.getString("USER_PROFILE"),
								getUserList(rs.getString("USER_GROUP"), rs.getString("USER_PROFILE")),
								rs.getString("WRITE_FLAG"));
					} else {
						return new LevelOfDisplayVb(rs.getString("USER_GROUP"), rs.getString("USER_PROFILE"),
								getUserList(rs.getString("USER_GROUP"), rs.getString("USER_PROFILE")),
								rs.getString("WRITE_FLAG"));
					}
				}

				private List<LevelOfDisplayUserVb> getUserList(String userGroup, String userProfile) {
					String userListQuery = "";
					if ("ETL_FEED_CATEGORY_ACCESS".equalsIgnoreCase(DashboardMain.getMainModel().getTableName())) {
						userListQuery = " select T2.VISION_ID, T2.USER_NAME, T2.USER_LOGIN_ID, T2.USER_GROUP, T2.USER_PROFILE,T1.WRITE_FLAG "
								+ " from " + tableName + " T1,  ETL_USER_VIEW T2  "
								+ " where T1.VISION_ID = T2.VISION_ID   AND T1.CATEGORY_ID = ?  AND T2.USER_GROUP =?  AND T2.USER_PROFILE = ? ";
					} else if ("ETL_CONNECTOR_ACCESS".equalsIgnoreCase(DashboardMain.getMainModel().getTableName())) {
						userListQuery = " select T2.VISION_ID, T2.USER_NAME, T2.USER_LOGIN_ID, T2.USER_GROUP, T2.USER_PROFILE,T1.WRITE_FLAG "
								+ " from  " + tableName + " T1,  ETL_USER_VIEW T2  "
								+ " where T1.VISION_ID = T2.VISION_ID   AND T1.CONNECTOR_ID =?  AND T2.USER_GROUP = ?  AND T2.USER_PROFILE = ? ";
					}
					Object[] args =  { DashboardMain.getMainModel().getDashboardId(), userGroup, userProfile };

					return getJdbcTemplate().query(userListQuery, new RowMapper<LevelOfDisplayUserVb>() {
						@Override
						public LevelOfDisplayUserVb mapRow(ResultSet rs, int rowNum) throws SQLException {
							return new LevelOfDisplayUserVb(rs.getInt("VISION_ID"), rs.getString("USER_NAME"),
									rs.getString("USER_LOGIN_ID"), rs.getString("USER_GROUP"),
									rs.getString("USER_PROFILE"), rs.getString("WRITE_FLAG"));
						}
					}, args);
				}
			}, args);
			DashboardMain.setLodProfileList(ValidationUtil.isValidList(profileList) ? profileList : null);
			return DashboardMain;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}
	

	public DashboardLODWrapperVb getuserListExcludingUserGrpAndProf(DashboardLODWrapperVb dashboardMain) {
		String sql = " SELECT VISION_ID,USER_NAME,USER_LOGIN_ID,USER_GROUP,USER_PROFILE "
				+ " FROM ETL_USER_VIEW WHERE USER_GROUP = ? "
				+ " AND USER_PROFILE =?";
		
		Object [] args= {dashboardMain.getMainModel().getUserGroup(),dashboardMain.getMainModel().getUserProfile()};
		List<LevelOfDisplayUserVb> usersList = getJdbcTemplate().query(sql, new RowMapper<LevelOfDisplayUserVb>() {
			@Override
			public LevelOfDisplayUserVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				return new LevelOfDisplayUserVb(rs.getInt("VISION_ID"), rs.getString("USER_NAME"),
						rs.getString("USER_LOGIN_ID"), rs.getString("USER_GROUP"), rs.getString("USER_PROFILE"));
			}
		}, args);
		dashboardMain.setLodUserList(usersList);
		return dashboardMain;
	}

	public int updateFavoriteDashboard(DashboardDesignVb dashBoardVb) {
		setServiceDefaults();
		try {
			String sql = "UPDATE USER_GROUP_PROPERTIES_SSBI SET DEFAULT_DASH_ID =? where VISION_ID=?";
			Object [] args= {dashBoardVb.getDashboardId(),intCurrentUserId};
			getJdbcTemplate().update(sql ,args);
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception ex) {
			throw new RuntimeCustomException("Error While Update");
		}
	}

	public int insertFavoriteDashboard(DashboardDesignVb dashBoardVb) {
		/* For DASHBOARD DEFAULT SAVE */
		setServiceDefaults();
		try {
			String sql = " INSERT INTO  USER_GROUP_PROPERTIES_SSBI (VISION_ID,  DEFAULT_DASH_ID,  MAKER,  VERIFIER, "
					+ " INTERNAL_STATUS,  "
					+ " DATE_LAST_MODIFIED,  DATE_CREATION,  RECORD_INDICATOR_NT,  RECORD_INDICATOR ) "
					+ " VALUES (?, ?, ?, ?, ?, sysdate, sysdate, ?, ?) ";
			Object args[] = { intCurrentUserId, dashBoardVb.getDashboardId(), intCurrentUserId, intCurrentUserId,
					dashBoardVb.getInternalStatus(), dashBoardVb.getRecordIndicatorNt(),
					dashBoardVb.getRecordIndicator() };
			getJdbcTemplate().update(sql,args);
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception ex) {
			throw new RuntimeCustomException("Error While Insert");
		}
	}

}
