package com.vision.dao;

import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.json.JSONObject;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.DashboardDesignVb;
import com.vision.vb.ExportDashboardVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.VrdObjectPropVb;
import com.vision.vb.WidgetDesignVb;
import com.vision.vb.WidgetLODWrapperVb;
import com.vision.vb.WidgetWrapperVb;
import com.vision.vb.XmlJsonUploadVb;

@Component
public class WidgetDesignDao extends AbstractDao<WidgetDesignVb> {

	@Override
	protected void setServiceDefaults() {
		serviceName = "VC Configuration";
		serviceDesc = "VC Configuration";
		tableName = "VISION_CATALOG_WIP";
		childTableName = "VISION_CATALOG_WIP";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		userGroup = CustomContextHolder.getContext().getUserGroup();
		userProfile = CustomContextHolder.getContext().getUserProfile();
	}

	@SuppressWarnings("deprecation")
	public List<WidgetDesignVb> getQueryResults(WidgetDesignVb vObj) {
		setServiceDefaults();
		int intKeyFieldsCount = 3;
		String sql = " select MAINWID.WIDGET_CONTEXT,MAINWID.FILTER_CONTEXT,WIDGETSUBTABLE.* from (   SELECT  DISTINCT T1.WIDGET_ID, T1.DESCRIPTION, T1.QUERY_ID, "
				+ " T1.WIDGET_STATUS_NT, T1.WIDGET_STATUS, "
				+ " T1.RECORD_INDICATOR_NT, T1.RECORD_INDICATOR, T1.MAKER, T1.VERIFIER, T1.INTERNAL_STATUS,   "
				+ getDbFunction(Constants.DATEFUNC, null) + "(T1.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED,   "
				+ getDbFunction(Constants.DATEFUNC, null) + "(T1.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION  "
				+ " FROM VWC_MAIN_WIDGETS T1 LEFT JOIN VDD_RS_WIDGETS_LOD T2  ON T1.WIDGET_ID = T2.WIDGET_ID "
				+ " WHERE (T1.MAKER = ? OR (T2.USER_GROUP = ? AND T2.USER_PROFILE = ? ))  ) WIDGETSUBTABLE "
				+ " INNER JOIN VWC_MAIN_WIDGETS MAINWID  ON MAINWID.WIDGET_ID  = WIDGETSUBTABLE.WIDGET_ID ";

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = intCurrentUserId;
		objParams[1] = userGroup;
		objParams[2] = userProfile;
		
		if (ValidationUtil.isValid(vObj.getWidgetId())) {
			intKeyFieldsCount = 4;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = intCurrentUserId;
			objParams[1] = userGroup;
			objParams[2] = userProfile;
			objParams[3] = vObj.getWidgetId();
			sql += "AND UPPER(WIDGETSUBTABLE.WIDGET_ID) = UPPER(?) ";
		}
		sql += "ORDER BY WIDGETSUBTABLE.WIDGET_ID, WIDGETSUBTABLE.DESCRIPTION";
		try {
			return getJdbcTemplate().query(sql, objParams, new RowMapper<WidgetDesignVb>() {
				@Override
				public WidgetDesignVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					WidgetDesignVb widgetVb = new WidgetDesignVb();
					widgetVb.setWidgetId(rs.getString("WIDGET_ID"));
					widgetVb.setDescription(rs.getString("DESCRIPTION"));
					widgetVb.setQueryId(rs.getString("QUERY_ID"));
					if (ValidationUtil.isValid(vObj.getWidgetId())) {
						widgetVb.setWidgetContext(rs.getString("WIDGET_CONTEXT"));
						widgetVb.setWidgetContextJson(
								CommonUtils.XmlToJson(widgetVb.getWidgetContext()).getResponse() + "");
						widgetVb.setFilterContext(rs.getString("FILTER_CONTEXT"));
					}
					return widgetVb;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public int doInsertionAppr(WidgetDesignVb vObj) {

		setServiceDefaults();

		String sql = "INSERT INTO VWC_MAIN_WIDGETS "
				+ " (WIDGET_ID, DESCRIPTION, QUERY_ID, WIDGET_STATUS_NT, WIDGET_STATUS, "
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION, WIDGET_CONTEXT, FILTER_CONTEXT) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?)";
		try {
			Object[] args = { vObj.getWidgetId(), vObj.getDescription(), vObj.getQueryId(), vObj.getWidgetStatusNt(),
					vObj.getWidgetStatus(), vObj.getRecordIndicatorNt(), vObj.getRecordIndicator(), intCurrentUserId,
					intCurrentUserId, 0 };

			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(sql);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}

					String clobData = ValidationUtil.isValid(vObj.getWidgetContext()) ? vObj.getWidgetContext() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObj.getFilterContext()) ? vObj.getFilterContext() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public int doUpdateAppr(WidgetDesignVb vObj) {

		setServiceDefaults();
		String sql = "UPDATE VWC_MAIN_WIDGETS SET WIDGET_CONTEXT = ?, FILTER_CONTEXT = ?, DESCRIPTION = ?, QUERY_ID = ?, "
				+ " WIDGET_STATUS = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,  DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " WHERE WIDGET_ID = ? ";
		try {
			Object[] args = { vObj.getDescription(), vObj.getQueryId(), vObj.getWidgetStatus(),
					vObj.getRecordIndicator(), intCurrentUserId, intCurrentUserId, vObj.getWidgetId() };
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int psIndex = 0;
					PreparedStatement ps = connection.prepareStatement(sql);

					String clobData = ValidationUtil.isValid(vObj.getWidgetContext()) ? vObj.getWidgetContext() : "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObj.getFilterContext()) ? vObj.getFilterContext() : "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	@Override
	protected List<WidgetDesignVb> selectApprovedRecord(WidgetDesignVb vObject) {
		return getQueryResults(vObject);
	}

	public ExceptionCode executeSql(String sql, Object[] args) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			if (args == null)
				getJdbcTemplate().execute(sql);
			else
				getJdbcTemplate().update(sql, args);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCode;
	}

	public ExceptionCode formResponceJsonForGridWidget(WidgetWrapperVb vObject, List<XmlJsonUploadVb> headerNameList,
			List<XmlJsonUploadVb> columnNameList, String sql, String dataTable, String orderByStr) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			JSONObject headerObj = new JSONObject();
			headerObj.put("ROW1", headerNameList);
			List<String> rowStringList = new ArrayList<String>();
			Long totalRows = 0l;
			if (vObject.getMainModel().getTotalRows() == 0l)
				totalRows = getJdbcTemplate().queryForObject("SELECT  COUNT(*) FROM " + dataTable, Long.class);
			else
				totalRows = vObject.getMainModel().getTotalRows();

			vObject.getMainModel().setTotalRows(totalRows);
			sql = "SELECT * FROM (SELECT temp.*, ROWNUM num FROM (" + sql
					+ (ValidationUtil.isValid(orderByStr) ? (" ORDER BY " + orderByStr) : "") + ") temp where ROWNUM <="
					+ vObject.getMainModel().getLastIndex() + ") WHERE num > " + vObject.getMainModel().getStartIndex();
			
			getJdbcTemplate().query(sql, new ResultSetExtractor<Object>() {
				@Override
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					int rowIndex = 1;
					rowLoop: while (rs.next()) {
						List<XmlJsonUploadVb> cellValueList = new ArrayList<XmlJsonUploadVb>();
						for (XmlJsonUploadVb headerVb : columnNameList) {
							cellValueList.add(new XmlJsonUploadVb(rs.getString((headerVb.getData()).toUpperCase())));
						}
						JSONObject rowObj = new JSONObject();
						rowObj.put("Row" + rowIndex, cellValueList);
						rowStringList.add(rowObj.toString());
						if (rowIndex == 10000) {
							break rowLoop;
						}
						rowIndex++;
					}
					return null;
				}
			});
			Map<String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("HEADER", headerObj.toString());
			returnMap.put("BODY", rowStringList);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(returnMap);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public int doInsertRecordForWidgetAccess(WidgetLODWrapperVb widgetDesiignLodVb, boolean isMain) throws Exception {
		WidgetDesignVb mainObject = widgetDesiignLodVb.getMainModel();
		String tableName = "VDD_RS_Widgets_LOD";
		String sql = "DELETE FROM "+tableName+" WHERE WIDGET_ID = ? ";
		Object[] args = { mainObject.getWidgetId() };
		try {
			getJdbcTemplate().update(sql,args);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}

		try {
			if (ValidationUtil.isValidList(widgetDesiignLodVb.getLodProfileList())) {
				sql = " INSERT INTO " + tableName + " (WIDGET_ID,"
						+ " USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, WG_STATUS, RECORD_INDICATOR_NT, "
						+ " RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
						+ getDbFunction(Constants.TO_DATE, mainObject.getDateLastModified()) + " , "
						+ getDbFunction(Constants.TO_DATE, mainObject.getDateCreation()) + " )";
				for (LevelOfDisplayVb vObject : widgetDesiignLodVb.getLodProfileList()) {
					args = new Object[] { mainObject.getWidgetId(), vObject.getUserGroupAt(), vObject.getUserGroup(),
							vObject.getUserProfileAt(), vObject.getUserProfile(), "0",
							mainObject.getRecordIndicatorNt(), mainObject.getRecordIndicator(), mainObject.getMaker(),
							mainObject.getVerifier(), mainObject.getInternalStatus() };
					getJdbcTemplate().update(sql, args);
				}
			}
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw e;
		}
	}

	@SuppressWarnings("deprecation")
	public WidgetLODWrapperVb getRecordForWidgetLOD(WidgetLODWrapperVb widgetLobVb) {
		WidgetDesignVb mainObject = widgetLobVb.getMainModel();
		try {
			String sql = "SELECT USER_GROUP, USER_PROFILE FROM VDD_RS_WIDGETS_LOD "
					+ " WHERE UPPER(WIDGET_ID) = UPPER(?)  ORDER BY USER_GROUP, USER_PROFILE";
			Object[] args = { mainObject.getWidgetId() };
			List<LevelOfDisplayVb> profileList = getJdbcTemplate().query(sql, args, new RowMapper<LevelOfDisplayVb>() {
				@Override
				public LevelOfDisplayVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					return new LevelOfDisplayVb(rs.getString("USER_GROUP"), rs.getString("USER_PROFILE"), null);
				}
			});
			widgetLobVb.setLodProfileList(ValidationUtil.isValidList(profileList) ? profileList : null);
			return widgetLobVb;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public ExceptionCode ValidatingDashboardWidgetData(String WidgetId) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<DashboardDesignVb> mainList = WidgetLevelValidaton("VDD_RS_WIDGETS", "VDD_RS_DASHBOARDS", WidgetId);
		List<DashboardDesignVb> wipList = WidgetLevelValidaton("VDD_RS_WIDGETS_WIP", "VDD_RS_DASHBOARDS_WIP", WidgetId);
		mainList.addAll(wipList);
		if (mainList.size() > 0 || wipList.size() > 0) {
			exceptionCode.setErrorMsg("WIDGET DATA PRESENT IN DASHBOARD CANNOT DELETE");
			exceptionCode.setResponse(mainList);
			exceptionCode.setErrorCode(Constants.VALIDATION_ERRORS_FOUND);
		} else {
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}
		return exceptionCode;
	}

	public List<DashboardDesignVb> WidgetLevelValidaton(String WidgettableName, String dashboardTableName,
			String widgetID) {
		Object[] args = { WidgettableName, widgetID };
		return getJdbcTemplate().query(
				"SELECT DASHBOARD_ID,DASHBOARD_DESC FROM "+dashboardTableName
						+ " WHERE DASHBOARD_ID in ( select DASHBOARD_ID from ? WHERE WIDGETS_ID =? and RSW_STATUS !=9 ) ",
				new RowMapper<DashboardDesignVb>() {
					@Override
					public DashboardDesignVb mapRow(ResultSet rs, int rowNum) throws SQLException {
						DashboardDesignVb dashboardVb = new DashboardDesignVb();
						dashboardVb.setDashboardId(rs.getString("DASHBOARD_ID"));
						dashboardVb.setDashboardDesc(rs.getString("DASHBOARD_DESC"));
						return dashboardVb;
					}
				}, args);

	}

	public synchronized int getMaxVersionNumber(String widgetId) {
		String sql = "SELECT CASE WHEN MAX(VERSION_NO) IS NULL THEN 0 ELSE MAX(VERSION_NO) END VERSION_NO "
				+ " FROM VWC_MAIN_WIDGETS_AD WHERE WIDGET_ID = ?";
		Object args[] = { widgetId };
		return getJdbcTemplate().queryForObject(sql, args, Integer.class);
	}

	public void moveMainDataToAD(String widgetId) {
		Integer versionNo = getMaxVersionNumber(widgetId) + 1;
		Object[] args = { versionNo, widgetId };
		getJdbcTemplate().update(
				" INSERT INTO VWC_MAIN_WIDGETS_AD(WIDGET_ID,DESCRIPTION,QUERY_ID,WIDGET_CONTEXT,FILTER_CONTEXT,WIDGET_STATUS_NT,WIDGET_STATUS, "
						+ " RECORD_INDICATOR_NT,RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,VERSION_NO) "
						+ " select WIDGET_ID,DESCRIPTION,QUERY_ID,WIDGET_CONTEXT,FILTER_CONTEXT,WIDGET_STATUS_NT,WIDGET_STATUS,"
						+ " RECORD_INDICATOR_NT, RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS, "
						+ " DATE_LAST_MODIFIED,DATE_CREATION FROM VWC_MAIN_WIDGETS WHERE WIDGET_ID = ? ",
				args);
	}

	public String getChartXmlFormObjProperties(String chartType) {
		try {
			String sql = "select HTML_TAG_PROPERTY from VRD_OBJECT_PROPERTIES where "
					/* + "VRD_OBJECT_ID='Col3D' AND " */
					+ "UPPER(OBJ_TAG_ID)=UPPER(?)";
			Object[] args = { chartType };
			return getJdbcTemplate().queryForObject(sql, args, String.class);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public List<VrdObjectPropVb> findActiveColorPaletteFromObjProperties(String objTagId) throws DataAccessException {
		String sql = "SELECT * FROM VRD_OBJECT_PROPERTIES WHERE VRD_OBJECT_ID='Palette' AND OBJ_TAG_ID=? "
				+ " AND OBJ_TAG_STATUS=0 ORDER BY OBJ_TAG_ID";
		Object[] args = { objTagId };
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VrdObjectPropVb vObj = new VrdObjectPropVb();
				vObj.setVrdObjectId(rs.getString("VRD_OBJECT_ID"));
				vObj.setObjTagId(rs.getString("OBJ_TAG_ID"));
				if (ValidationUtil.isValid(rs.getString("HTML_TAG_PROPERTY")))
					vObj.setHtmlTagProperty(
							CommonUtils.getValueForXmlTag(rs.getString("HTML_TAG_PROPERTY"), "COLOR_CODE"));
				vObj.setObjTagStatus(rs.getInt("OBJ_TAG_STATUS"));
				vObj.setObjTagDesc(rs.getString("OBJ_TAG_DESC"));
				vObj.setObjTagIconLink(rs.getString("OBJ_TAG_ICON_LINK"));
				return vObj;
			}
		};
		return getJdbcTemplate().query(sql, args, mapper);
	}

	public int deleteColumnHeadersVcReport(ExportDashboardVb vObject) {
		String query = "DELETE FROM VC_COLUMN_HEADERS_STG WHERE REPORT_ID = ? AND SUB_REPORT_ID = ? And SESSION_ID like '"
				+ vObject.getSessionId() + "%'";
		Object args[] = { vObject.getReportId(), vObject.getSubReportId() };
		return jdbcTemplate.update(query, args);
	}

	public int doInsertionRsRptGrid(ExportDashboardVb vObject, String levelXml) {
		PreparedStatement ps = null;
		Connection con = null;
		String query = "Insert Into RS_RPT_GRID (REPORT_ID, SUB_REPORT_ID,SESSION_ID, VISION_ID,  "
				+ "LEVEL_ID, DATE_CREATED, XML_DATA) Values (?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", ?)";
		Object[] args = { vObject.getReportId(), vObject.getSubReportId(), vObject.getSessionId(), intCurrentUserId,
				vObject.getGridLevel() };
		int result = 0;
		int argumentLength = args.length;
		try {
			con = jdbcTemplate.getDataSource().getConnection();
			ps = con.prepareStatement(query);
			for (int i = 1; i <= argumentLength; i++) {
				ps.setObject(i, args[i - 1]);
			}
			String clobData = ValidationUtil.isValid(levelXml) ? levelXml : ""; // XML_DATA
			ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
			result = ps.executeUpdate();
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		} finally {
			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					// e.printStackTrace();
				}
			}
			JdbcUtils.closeStatement(ps);
			DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
		return result;
	}

	public ExceptionCode callProcedureForGrid(ExportDashboardVb vObjMain) {
		setServiceDefaults();
		ExceptionCode exceptionCode = new ExceptionCode();
		CallableStatement cs = null;
		String tableName = "";
		String status = "";
		String errorMessage = "";
		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			cs = connection.prepareCall("{call PR_RS_CATALOG_GRID_TRANS_NEW(?, ?, ?, ?, ?, ?, ?)}");
			cs.setString(1, vObjMain.getReportId());
			cs.setString(2, vObjMain.getSubReportId());
			cs.setString(3, vObjMain.getSessionId());
			cs.setString(4, vObjMain.getGridLevel());
			cs.registerOutParameter(5, java.sql.Types.VARCHAR); // output table name
			cs.registerOutParameter(6, java.sql.Types.VARCHAR); // Status
			cs.registerOutParameter(7, java.sql.Types.VARCHAR); // Error Message
			ResultSet rs = cs.executeQuery();
			tableName = cs.getString(5);
			status = cs.getString(6);
			errorMessage = cs.getString(7);
			rs.close();
			if ("0".equalsIgnoreCase(status)) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(tableName);
				return exceptionCode;
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg(
						"Problem in executing procedure[PR_RS_CATALOG_GRID_TRANS_NEW]. Cause [" + errorMessage + "]");
				return exceptionCode;
			}
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(
					"Problem in executing procedure[PR_RS_CATALOG_GRID_TRANS_NEW]. Cause [" + ex.getMessage() + "]");
			return exceptionCode;
		} finally {
			JdbcUtils.closeStatement(cs);
			DataSourceUtils.releaseConnection(connection, jdbcTemplate.getDataSource());
		}
	}

	public List<ColumnHeadersVb> getColumnHeadersForGrid(ExportDashboardVb vObj) {
		setServiceDefaults();
		int intKeyFieldsCount = 2;
		String query = new String(
				"SELECT REPORT_ID, SESSION_ID, LABEL_ROW_NUM, LABEL_COL_NUM, CAPTION, COLUMN_WIDTH, COL_TYPE, ROW_SPAN, COL_SPAN, NUMERIC_COLUMN_NO, DB_COLUMN "
						+ "FROM VC_COLUMN_HEADERS_STG WHERE REPORT_ID = ? AND SUB_REPORT_ID = ? AND SESSION_ID like '"
						+ vObj.getSessionId() + "%' ORDER BY 3,4");
		try {
			Object params[] = new Object[intKeyFieldsCount];
			params[0] = vObj.getReportId();
			params[1] = vObj.getSubReportId();

			RowMapper mapper = new RowMapper() {
				public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
					ColumnHeadersVb columnHeadersVb = new ColumnHeadersVb();
					columnHeadersVb.setReportId(rs.getString("REPORT_ID"));
					columnHeadersVb.setSessionId(rs.getString("SESSION_ID"));
					columnHeadersVb.setLabelRowNum(rs.getInt("LABEL_ROW_NUM"));
					columnHeadersVb.setLabelColNum(rs.getInt("LABEL_COL_NUM"));
					columnHeadersVb.setCaption(rs.getString("CAPTION"));
					// columnHeadersVb.setColumnWidth(rs.getLong("COLUMN_WIDTH"));
					columnHeadersVb.setColType(rs.getString("COL_TYPE"));
					columnHeadersVb.setRowSpanNum(rs.getInt("ROW_SPAN"));
					columnHeadersVb.setNumericColumnNo(rs.getInt("NUMERIC_COLUMN_NO"));
					columnHeadersVb.setColSpanNum(rs.getInt("COL_SPAN"));
					columnHeadersVb.setDbColumnName(rs.getString("DB_COLUMN"));
					return columnHeadersVb;
				}
			};

			return jdbcTemplate.query(query, params, mapper);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public ArrayList returnPivotXMLForDataAndHeader(String sourceTblName, String baseColHeaderStr,
			ExportDashboardVb vObjMain) {
		try {
			String sql = "SELECT * FROM "+sourceTblName+" ORDER BY ROW_ID, COL_ID";
			ResultSetExtractor<String> rse = new ResultSetExtractor<String>() {
				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					try {
						DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
						DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

						Document doc = docBuilder.newDocument();
						Element rootElement = doc.createElement("root");
						doc.appendChild(rootElement);
						Integer commonRowId = null;
						Element row = null;
						Element column;
						while (rs.next()) {
							if (commonRowId == null) {
								commonRowId = rs.getInt("ROW_ID");
								row = doc.createElement("Row" + commonRowId);
								row.setAttribute("ROW_ID", String.valueOf(commonRowId));
							}

							if (commonRowId == rs.getInt("ROW_ID")) {
								column = doc.createElement("Column" + rs.getInt("COL_ID"));
								column.setAttribute("COL_ID", rs.getString("COL_ID"));
								column.setAttribute("ROW_ID", rs.getString("ROW_ID"));
								column.setAttribute("ROW_SPAN", rs.getString("ROW_SPAN"));
								column.setAttribute("COL_SPAN", rs.getString("COL_SPAN"));
								column.setAttribute("CAPTION", rs.getString("CAPTION"));
								column.setAttribute("COL_TYPE", rs.getString("COL_TYPE"));
								column.setAttribute("FORMAT_TYPE", rs.getString("FORMAT_TYPE"));
								column.setAttribute("DRILL_THROUGH", rs.getString("DRILL_THROUGH"));
								column.setAttribute("COLUMN_COLOR", rs.getString("COLUMN_COLOR"));
								column.setAttribute("COLUMN_BGCOLOR", rs.getString("COLUMN_BGCOLOR"));
								column.setAttribute("COLUMN_FORMAT", rs.getString("COLUMN_FORMAT"));
								column.setAttribute("COLUMN_SIZE", rs.getString("COLUMN_SIZE"));
								column.setAttribute("COLUMN_DRILL_DOWN", rs.getString("COLUMN_DRILL_DOWN"));
								if (ValidationUtil.isValid(rs.getString("DATA_VALUE")))
									column.appendChild(doc.createTextNode(rs.getString("DATA_VALUE")));
								row.appendChild(column);
							} else {
								rootElement.appendChild(row);
								commonRowId = rs.getInt("ROW_ID");
								row = doc.createElement("Row" + commonRowId);
								row.setAttribute("ROW_ID", String.valueOf(commonRowId));
								column = doc.createElement("Column" + rs.getInt("COL_ID"));
								column.setAttribute("COL_ID", rs.getString("COL_ID"));
								column.setAttribute("ROW_ID", rs.getString("ROW_ID"));
								column.setAttribute("ROW_SPAN", rs.getString("ROW_SPAN"));
								column.setAttribute("COL_SPAN", rs.getString("COL_SPAN"));
								column.setAttribute("CAPTION", rs.getString("CAPTION"));
								column.setAttribute("COL_TYPE", rs.getString("COL_TYPE"));
								column.setAttribute("FORMAT_TYPE", rs.getString("FORMAT_TYPE"));
								column.setAttribute("DRILL_THROUGH", rs.getString("DRILL_THROUGH"));
								column.setAttribute("COLUMN_COLOR", rs.getString("COLUMN_COLOR"));
								column.setAttribute("COLUMN_BGCOLOR", rs.getString("COLUMN_BGCOLOR"));
								column.setAttribute("COLUMN_FORMAT", rs.getString("COLUMN_FORMAT"));
								column.setAttribute("COLUMN_SIZE", rs.getString("COLUMN_SIZE"));
								column.setAttribute("COLUMN_DRILL_DOWN", rs.getString("COLUMN_DRILL_DOWN"));
								if (ValidationUtil.isValid(rs.getString("DATA_VALUE")))
									column.appendChild(doc.createTextNode(rs.getString("DATA_VALUE")));
								row.appendChild(column);
							}
						}
						rootElement.appendChild(row);
						return CommonUtils.transformXmlDocToString(doc);
					} catch (Exception e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
						return null;
					}
				}
			};
			String dataXmlString = jdbcTemplate.query(sql, rse);

			String query = new String(
					"SELECT REPORT_ID, SESSION_ID, LABEL_ROW_NUM, LABEL_COL_NUM, CAPTION, COLUMN_WIDTH, COL_TYPE, ROW_SPAN, COL_SPAN, "
							+ "FORMAT_TYPE, NUMERIC_COLUMN_NO, DB_COLUMN "
							+ "FROM VC_COLUMN_HEADERS_STG WHERE REPORT_ID = ? AND SUB_REPORT_ID = ? AND SESSION_ID like '"
							+ vObjMain.getSessionId() + vObjMain.getLevelIndexCt() + "%' ORDER BY 3,4");
			Object params[] = new Object[2];
			params[0] = vObjMain.getReportId();
			params[1] = vObjMain.getSubReportId();

			ResultSetExtractor<String> rse1 = new ResultSetExtractor<String>() {

				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {

					try {
						DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
						DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

						Document doc = docBuilder.newDocument();
						Element rootElement = doc.createElement("root");
						doc.appendChild(rootElement);
						Integer commonRowId = null;
						Element row = null;
						Element column;
						while (rs.next()) {
							if (commonRowId == null) {
								commonRowId = rs.getInt("LABEL_ROW_NUM");
								row = doc.createElement("Row" + commonRowId);
								row.setAttribute("LABEL_ROW_NUM", String.valueOf(commonRowId));
							}

							if (commonRowId == rs.getInt("LABEL_ROW_NUM")) {
								column = doc.createElement("Column" + rs.getInt("LABEL_COL_NUM"));
								column.setAttribute("LABEL_COL_NUM", rs.getString("LABEL_COL_NUM"));
								column.setAttribute("LABEL_ROW_NUM", rs.getString("LABEL_ROW_NUM"));
								column.setAttribute("ROW_SPAN", rs.getString("ROW_SPAN"));
								column.setAttribute("COL_SPAN", rs.getString("COL_SPAN"));
								column.setAttribute("COL_TYPE", rs.getString("COL_TYPE"));
								column.setAttribute("FORMAT_TYPE", rs.getString("FORMAT_TYPE"));
								if (ValidationUtil.isValid(rs.getString("CAPTION")))
									column.appendChild(doc.createTextNode(rs.getString("CAPTION")));
								row.appendChild(column);
							} else {
								rootElement.appendChild(row);
								commonRowId = rs.getInt("LABEL_ROW_NUM");
								row = doc.createElement("Row" + commonRowId);
								row.setAttribute("LABEL_ROW_NUM", String.valueOf(commonRowId));
								column = doc.createElement("Column" + rs.getInt("LABEL_COL_NUM"));
								column.setAttribute("LABEL_COL_NUM", rs.getString("LABEL_COL_NUM"));
								column.setAttribute("LABEL_ROW_NUM", rs.getString("LABEL_ROW_NUM"));
								column.setAttribute("ROW_SPAN", rs.getString("ROW_SPAN"));
								column.setAttribute("COL_SPAN", rs.getString("COL_SPAN"));
								column.setAttribute("COL_TYPE", rs.getString("COL_TYPE"));
								column.setAttribute("FORMAT_TYPE", rs.getString("FORMAT_TYPE"));
								if (ValidationUtil.isValid(rs.getString("CAPTION")))
									column.appendChild(doc.createTextNode(rs.getString("CAPTION")));
								row.appendChild(column);
							}
						}
						rootElement.appendChild(row);
						return CommonUtils.transformXmlDocToString(doc);
					} catch (Exception e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
						return null;
					}
				}

			};

			String headerXmlString = jdbcTemplate.query(query, params, rse1);

			
			sql = "SELECT max(col_id) MAX_ROW_COL_COUNT FROM "+sourceTblName;
			String maxcolumnCount = jdbcTemplate.queryForObject(sql, String.class);

			sql = "SELECT max(row_id) MAX_ROW_COUNT FROM "+sourceTblName;
			Integer maxrowCount = jdbcTemplate.queryForObject(sql, Integer.class);

			sql = "SELECT Distinct(MAX(Label_ROW_NUM))  MAX_ROW_COUNT FROM  VC_COLUMN_HEADERS_STG WHERE REPORT_ID = ? "
					+ " AND SUB_REPORT_ID = ? AND SESSION_ID like '" + vObjMain.getSessionId() + vObjMain.getLevelIndexCt() + "%'";
			
			Object[] args = new Object[] {vObjMain.getReportId() , vObjMain.getSubReportId()};
			
			Integer maxHeaderRowCount = jdbcTemplate.queryForObject(sql, args, Integer.class);

			sql = "select SUM(CASE col_span  WHEN 0 THEN 1 ELSE col_span END) as SUM_COL_SPAN from VC_COLUMN_HEADERS_STG "
					+ " WHERE REPORT_ID = ? AND SUB_REPORT_ID = ? AND SESSION_ID like '" + vObjMain.getSessionId()
					+ vObjMain.getLevelIndexCt() + "%' and label_row_num=1 and col_type='T'";
			
			Integer sumColSpanCount = jdbcTemplate.queryForObject(sql, args, Integer.class);

			sql = "select SUM(Label_ROW_NUM)  MAX_ROW_COUNT from VC_COLUMN_HEADERS_STG WHERE REPORT_ID = ? "
					+ " AND SUB_REPORT_ID = ? AND SESSION_ID like '" + vObjMain.getSessionId()
					+ vObjMain.getLevelIndexCt() + "%' and label_row_num=1 and col_type='T'";
			
			Integer sumRowSpanCount = jdbcTemplate.queryForObject(sql, args, Integer.class);

			Integer totalRow = maxrowCount + maxHeaderRowCount;
			ArrayList<String> returnList = new ArrayList<String>(5);
			// System.out.println(headerXmlString);
			// System.out.println("******************");
			// System.out.println(dataXmlString);
			returnList.add(headerXmlString);
			returnList.add(dataXmlString);
			returnList.add(maxcolumnCount);
			returnList.add(totalRow.toString());
			returnList.add(sumColSpanCount.toString());
			returnList.add(sumRowSpanCount.toString());
			return returnList;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}

}
