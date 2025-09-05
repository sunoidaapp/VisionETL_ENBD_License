/**
 * Author : Deepak.s
 */
package com.vision.dao;

import java.awt.Color;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONObject;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.rowset.CachedRowSetImpl;
import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.DashboardFilterVb;
import com.vision.vb.DashboardTabVb;
import com.vision.vb.DashboardTilesVb;
import com.vision.vb.DashboardVb;
import com.vision.vb.VisionUsersVb;

@Component
public class DashboardsDao extends AbstractDao<DashboardVb>{
	@Autowired
	CommonDao commonDao;
	@Value("${app.clientName}")
	private String clientName;
	Connection con = null;
	
	public DashboardVb getDashboardDetail(DashboardVb dObj) {
		setServiceDefaults();
		DashboardVb dashboardVb = new DashboardVb();
		try {
			String query = " Select t1.Dashboard_Id,Dashboard_Name,Total_Tabs,Data_Source_Ref,Filter,Filter_Ref_Code,"
					+ "  "+ getDbFunction(Constants.NVL, null)+"((Select Min(t2.DS_Theme) from PRD_USER_Dashboard_Theme t2 "
							+ " where t2.dashboard_ID=t1.dashboard_ID and t2.vision_ID= ?),t1.DS_THEME) DS_THEME,"
					+ "  t1.DS_THEME_AT from PRD_DASHBOARDS t1 where Dashboard_ID = ? ";
			Object [] args= {CustomContextHolder.getContext().getVisionId(),dObj.getDashboardId()};
			List<DashboardVb> list = getJdbcTemplate().query(query, args, getDashboardDetMapper());
			return (list != null && list.size() > 0) ? list.get(0) : dashboardVb;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return dashboardVb;
		}
	}
	
	private RowMapper<DashboardVb> getDashboardDetMapper() {
		RowMapper<DashboardVb> mapper = new RowMapper<DashboardVb>() {
			public DashboardVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				DashboardVb vObject = new DashboardVb();
				vObject.setDashboardId(rs.getString("Dashboard_Id"));
				vObject.setDashboardName(rs.getString("Dashboard_Name"));
				vObject.setTotalTabs(rs.getString("Total_Tabs"));
				vObject.setDataSourceRef(rs.getString("Data_Source_Ref"));
				vObject.setFilterFlag(rs.getString("Filter"));
				vObject.setFilterRefCode(rs.getString("Filter_Ref_Code"));
				vObject.setDashboard_Theme(rs.getString("DS_THEME"));
				vObject.setDashboar_ThemeAT(rs.getInt("DS_THEME_AT"));
				return vObject;
			}
		};
		return mapper;
	}
	
	public List<DashboardTabVb> getDashboardTabDetails(DashboardVb dObj) {
		setServiceDefaults();
		List<DashboardTabVb> collTemp = null;
		try {
			String query = " Select Tab_ID,Tab_Name,Template_ID from PRD_DASHBOARD_TABS where Status = 0 "
					+ " and Dashboard_id= ? order by Tab_Sequence ";
			Object[] args = { dObj.getDashboardId() };
			collTemp = getJdbcTemplate().query(query, args, getDashboardTabMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	private RowMapper getDashboardTabMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				DashboardTabVb vObject = new DashboardTabVb();
				vObject.setTabId(rs.getString("Tab_ID"));
				vObject.setTabName(rs.getString("Tab_Name"));
				vObject.setTemplateId(rs.getString("Template_ID"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<DashboardTilesVb> getDashboardTileDetails(DashboardTabVb dObj) {
		setServiceDefaults();
		List<DashboardTilesVb> collTemp = null;
		try {
			String query = "Select Dashboard_ID,Tab_ID,Tile_ID,Tile_Caption,Sequence,Query_ID,TILE_TYPE,DRILL_DOWN_FLAG, "
					+ "CHART_TYPE,TILE_PROPERTY_XML, "
					+ "PLACE_HOLDER_COUNT,DOUBLE_WIDTH_FLAG,SUB_TILES,Transition_Effect,Theme from PRD_DASHBOARD_TILES "
					+ " where Dashboard_ID = ? "
					+ " and Tab_ID = ? Order by Sequence ";
			Object[] args = { dObj.getDashboardId(), dObj.getTabId() };
			collTemp = getJdbcTemplate().query(query, args, getDashboardTileMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	private RowMapper getDashboardTileMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				DashboardTilesVb vObject = new DashboardTilesVb();
				vObject.setDashboardId(rs.getString("Dashboard_ID"));
				vObject.setTabId(rs.getString("Tab_ID"));
				vObject.setTileId(rs.getString("Tile_ID"));
				vObject.setTileCaption(rs.getString("Tile_Caption"));
				vObject.setTileSequence(rs.getInt("Sequence"));
				vObject.setQueryId(rs.getString("Query_ID"));
				vObject.setTileType(rs.getString("TILE_TYPE"));
				vObject.setPropertyAttr(rs.getString("TILE_PROPERTY_XML"));
				vObject.setPlaceHolderCnt(rs.getInt("PLACE_HOLDER_COUNT"));
				vObject.setChartType(rs.getString("CHART_TYPE"));
				vObject.setDoubleWidthFlag(rs.getString("DOUBLE_WIDTH_FLAG"));
				if("Y".equalsIgnoreCase(rs.getString("DRILL_DOWN_FLAG"))) {
					vObject.setDrillDownFlag(true);
				}else {
					vObject.setDrillDownFlag(false);
				}
				vObject.setSubTiles(rs.getString("SUB_TILES"));
				vObject.setTransitionEffect(rs.getString("Transition_Effect"));
				vObject.setTheme(rs.getString("Theme"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<DashboardTilesVb> getTileDrillDownDetails(String dashboardId, String tabId, int parentSequence,
			String subSequence, Boolean subTileFlag) {
		setServiceDefaults();
		int intKeyFieldsCount = 3;
		List<DashboardTilesVb> collTemp = null;
		try {
			String query = "Select Dashboard_ID,Tab_ID,Tile_ID,Tile_Caption,Sequence,Query_ID,TILE_TYPE,DRILL_DOWN_FLAG,"
					+ " CHART_TYPE,TILE_PROPERTY_XML,PLACE_HOLDER_COUNT,DOUBLE_WIDTH_FLAG,'N' SUB_TILES,'' Transition_Effect, "
					+ "Theme from PRD_DASHBOARD_TILES_DD "
					+ " where Dashboard_ID = ? and Tab_ID = ? and Parent_Sequence = ? ";

			Object[] objParams = new Object[intKeyFieldsCount];
			objParams[0] = dashboardId;
			objParams[1] = tabId;
			objParams[2] = parentSequence;

			if (subTileFlag) {
				intKeyFieldsCount = 4;
				objParams = new Object[intKeyFieldsCount];
				objParams[0] = dashboardId;
				objParams[1] = tabId;
				objParams[2] = parentSequence;
				objParams[3] = subSequence;
				query = query + " and Sub_Sequence = ? ";
			}
			collTemp = getJdbcTemplate().query(query, objParams, getDashboardTileMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	
	public String getTilesReportData(DashboardTilesVb vObject) throws SQLException{
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"); 
		setServiceDefaults();
		Statement stmt = null;
		ResultSet rs = null;
		String resultData = "";
		DecimalFormat dfDec = new DecimalFormat("0.00");
		DecimalFormat dfNoDec = new DecimalFormat("0");
		try
		{
			String orginalQuery = getReportQuery(vObject.getQueryId());
			orginalQuery = replacePromptVariables(orginalQuery, vObject);
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					int colCount = metaData.getColumnCount();
					HashMap<String, String> columns = new HashMap<String, String>();
					while (rs.next()) {
						for (int cn = 1; cn <= colCount; cn++) {
							String columnName = metaData.getColumnName(cn);
							columns.put(columnName.toUpperCase(), rs.getString(columnName));
						}
					}
					return columns;
				}
			};
			HashMap<String, String> columns = (HashMap<String, String>) getJdbcTemplate().query(orginalQuery, mapper);
			String fieldProp = vObject.getPropertyAttr();
			fieldProp = ValidationUtil.isValid(fieldProp) ? fieldProp.replaceAll("\n", "").replaceAll("\r", "") : "";
			for (int ctr = 1; ctr <= vObject.getPlaceHolderCnt(); ctr++) {
				String placeHolder = CommonUtils.getValueForXmlTag(fieldProp, "PLACEHOLDER" + ctr);
				String sourceCol = CommonUtils.getValueForXmlTag(placeHolder, "SOURCECOL");
				String dataType = CommonUtils.getValueForXmlTag(placeHolder, "DATA_TYPE");
				String numberFormat = CommonUtils.getValueForXmlTag(placeHolder, "NUMBERFORMAT");
				String scaling = CommonUtils.getValueForXmlTag(placeHolder, "SCALING");
				if (!ValidationUtil.isValid(placeHolder) || !ValidationUtil.isValid(sourceCol)) {
					continue;
				}
				if (ValidationUtil.isValid(sourceCol)) {
					String fieldValue = columns.get(sourceCol);
					/* Double val = 0.00; */
					String prefix = "";
					if (ValidationUtil.isValid(scaling) && "Y".equalsIgnoreCase(scaling)
							&& ValidationUtil.isValid(fieldValue)) {
						Double dbValue = Math.abs(Double.parseDouble(fieldValue));
						if (dbValue > 1000000000) {
							dbValue = Double.parseDouble(fieldValue) / 1000000000;
							prefix = "B";
						} else if (dbValue > 1000000) {
							dbValue = Double.parseDouble(fieldValue) / 1000000;
							prefix = "M";
						} else if (dbValue > 10000) {
							dbValue = Double.parseDouble(fieldValue) / 1000;
							prefix = "K";
						}
						String afterDecimalVal = String.valueOf(dbValue);
						if (!afterDecimalVal.contains("E")) {
							afterDecimalVal = afterDecimalVal.substring(afterDecimalVal.indexOf(".") + 1);
							if (Double.parseDouble(afterDecimalVal) > 0)
								fieldValue = dfDec.format(dbValue) + prefix;
							else
								fieldValue = dfNoDec.format(dbValue) + prefix;
						} else {
							fieldValue = "0.00";
						}
					}
					if (ValidationUtil.isValid(fieldValue) && ValidationUtil.isValid(numberFormat)
							&& "Y".equalsIgnoreCase(numberFormat) && !ValidationUtil.isValid(prefix)) {
						DecimalFormat decimalFormat = new DecimalFormat("#,###.##");
						double tmpVal = Double.parseDouble(fieldValue);
						fieldValue = decimalFormat.format(tmpVal);
					}
					fieldProp = fieldProp.replaceAll(sourceCol, fieldValue);
				}
			}
			int PRETTY_PRINT_INDENT_FACTOR = 4;
			JSONObject xmlJSONObj = XML.toJSONObject(fieldProp);
			resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
			return resultData;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				System.out.println("Exception while executing Tile [" + vObject.getTileSequence() + "] ");
				ex.printStackTrace();
			}
			return null;
		}
	}
	
	public ArrayList getGridData(DashboardTilesVb vObject) throws SQLException{
		setServiceDefaults();
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList resultlst = new ArrayList();
		try
		{
			String orginalQuery = getReportQuery(vObject.getQueryId());
			orginalQuery = replacePromptVariables(orginalQuery, vObject);
			ArrayList datalst = new ArrayList();
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					int colCount = metaData.getColumnCount();
					HashMap<String, String> columns = new HashMap<String, String>();
					while (rs.next()) {
						columns = new HashMap<String, String>();
						for (int cn = 1; cn <= colCount; cn++) {
							String columnName = metaData.getColumnName(cn);
							columns.put(columnName.toUpperCase(), rs.getString(columnName));
						}
						datalst.add(columns);
					}
					return columns;
				}
			};
			HashMap<String, String> columns = (HashMap<String, String>) getJdbcTemplate().query(orginalQuery, mapper);
			String fieldProp = vObject.getPropertyAttr();
			fieldProp = ValidationUtil.isValid(fieldProp) ? fieldProp.replaceAll("\n", "").replaceAll("\r", "") : "";
			String columnValueDetails = CommonUtils.getValueForXmlTag(fieldProp, "COLUMNVALUE");
			String colValArray[] = {};
			if (ValidationUtil.isValid(columnValueDetails))
				colValArray = columnValueDetails.split(",");

			ArrayList finalDatalst = new ArrayList();
			ArrayList columnlst = new ArrayList();
			ArrayList columnFormatlst = new ArrayList();
			for (int ctr = 0; ctr < datalst.size(); ctr++) {
				LinkedHashMap<String, String> finalDataMap = new LinkedHashMap<String, String>();
				HashMap<String, String> dataMap = (HashMap<String, String>) datalst.get(ctr);
				for (int ct = 0; ct < colValArray.length; ct++) {
					String colArr[] = colValArray[ct].split("!@#");
					String colName = colArr[0];
					String colSrc = colArr[1];
					String colType = colArr[2];
					finalDataMap.put(colName, dataMap.get(colSrc));
					if (ctr == 0) {
						columnlst.add(colName);
						columnFormatlst.add(colType);
					}
				}
				finalDatalst.add(finalDataMap);
			}
			resultlst.add(columnlst);
			resultlst.add(finalDatalst);
			resultlst.add(columnFormatlst);
			return resultlst;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			resultlst.add(new ArrayList());
			resultlst.add(new ArrayList());
			resultlst.add(new ArrayList());
			return resultlst;
		}
	}

	public String getChartReportData(DashboardTilesVb vObject, Connection con) throws SQLException {
		setServiceDefaults();
		Statement stmt = null;
		List collTemp = new ArrayList();
		HashMap chartDataMap = new HashMap();
		ResultSet rs = null;
		try {
			con = getJdbcTemplate().getDataSource().getConnection();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String orginalQuery = getReportQuery(vObject.getQueryId());
			orginalQuery = replacePromptVariables(orginalQuery, vObject);
			if (ValidationUtil.isValid(orginalQuery)) {
				rs = stmt.executeQuery(orginalQuery);
			} else {
				return "";
			}
			CachedRowSetImpl rsChild = new CachedRowSetImpl();
			rsChild = new CachedRowSetImpl();
			rsChild.populate(rs);

			boolean multiY_NoSeries = false;
			boolean onlyX_NoSeries = false;
			boolean onlyY_onlyMeasure = false;
			boolean onlyY_WithSeries = false;
			String xAxisCol = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "X-AXIS");
			String yAxisCol = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "Y-AXIS");
			String zAxisCol = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "Z-AXIS");
			String seriesCol = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "SERIES");
			String measureProp = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "MEASURE_PROP");
			String chartType = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "ChartType");
			String isCustomColor = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "isCustomColor");
			String isRadiantColor = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "isRadiantColor");
			String enableColorPalette = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "EnableColorPalette");
			String userDefinedColorCode = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "ColorCode");
			String userDefinedPalette = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(), "ColorPalette");
			String genricChartProperties = CommonUtils.getValueForXmlTag(vObject.getPropertyAttr(),
					"GenericAttributes");

			if (ValidationUtil.isValid(xAxisCol) && !ValidationUtil.isValid(yAxisCol)
					&& !ValidationUtil.isValid(seriesCol))
				onlyX_NoSeries = true;
			else if (ValidationUtil.isValid(xAxisCol) && ValidationUtil.isValid(yAxisCol) && yAxisCol.indexOf(",") != -1
					&& !ValidationUtil.isValid(seriesCol))
				multiY_NoSeries = true;
			else if (!ValidationUtil.isValid(xAxisCol) && ValidationUtil.isValid(yAxisCol)
					&& yAxisCol.indexOf(",") != -1 && !ValidationUtil.isValid(seriesCol))
				onlyY_onlyMeasure = true;
			else if (!ValidationUtil.isValid(xAxisCol) && ValidationUtil.isValid(yAxisCol)
					&& yAxisCol.indexOf(",") != -1 && ValidationUtil.isValid(seriesCol))
				onlyY_WithSeries = true;

			String chartMataDataXml = getChartXmlFormObjProperties(vObject.getChartType());
			String chartXML = "";
			List<String> repeatTagList = new ArrayList<String>();
			Matcher matcherObj = Pattern.compile("\\<REPEATING_TAG\\>(.*?)\\<\\/REPEATING_TAG\\>", Pattern.DOTALL)
					.matcher(chartMataDataXml);
			while (matcherObj.find()) {
				String repeatTags = matcherObj.group(1).replaceAll("\n", "").replaceAll("\r", "")
						.replaceAll("\\s+", " ").trim();
				repeatTagList.add(repeatTags);
			}
			matcherObj = Pattern.compile("\\<chart(.*?)\\<\\/chart\\>", Pattern.DOTALL).matcher(chartMataDataXml);
			if (matcherObj.find()) {
				String tempChartXml = matcherObj.group(1);
				String postChartXml = tempChartXml.substring(tempChartXml.indexOf('>'), tempChartXml.length());
				tempChartXml = tempChartXml.substring(0, tempChartXml.indexOf('>'));
				if (ValidationUtil.isValid(genricChartProperties)) {
					Matcher attributeMatcherObj = Pattern.compile("</(.*?)>", Pattern.DOTALL)
							.matcher(genricChartProperties);
					while (attributeMatcherObj.find()) {
						if (tempChartXml.indexOf(" " + attributeMatcherObj.group(1)) == -1)
							if ("exportFileName".equalsIgnoreCase(attributeMatcherObj.group(1))) {
								String value = CommonUtils.getValueForXmlTag(genricChartProperties,
										attributeMatcherObj.group(1));
								if (!ValidationUtil.isValid(value)) {
									value = ValidationUtil
											.isValid(CommonUtils.getValueForXmlTag(genricChartProperties, "caption"))
													? CommonUtils.getValueForXmlTag(genricChartProperties, "caption")
													: ValidationUtil.isValid(CommonUtils
															.getValueForXmlTag(genricChartProperties, "subcaption"))
																	? CommonUtils.getValueForXmlTag(
																			genricChartProperties, "subcaption")
																	: "VisionCharts";
								}
								tempChartXml = tempChartXml + " exportFileName=\"" + value + "\"";
							} else
								tempChartXml = tempChartXml + " " + attributeMatcherObj.group(1) + "=\"" + CommonUtils
										.getValueForXmlTag(genricChartProperties, attributeMatcherObj.group(1)) + "\"";
					}
				}
				tempChartXml = tempChartXml + postChartXml;

				chartXML = "<chart" + tempChartXml + "</chart>";
			}
			String returnChartXml = chartXML;
			if (ValidationUtil.isValid(chartXML)) {
				for (String repeatTagMain : repeatTagList) {
					StringBuffer dataExistCheck = new StringBuffer();
					if (onlyX_NoSeries && repeatTagList.size() == 1) {
						returnChartXml = updateReturnXmlForSingleRepeatTag(repeatTagMain, chartXML, xAxisCol, yAxisCol,
								zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck);
					} else if (multiY_NoSeries && repeatTagList.size() == 2) {
						if (repeatTagMain.indexOf(",") == -1) {
							returnChartXml = updateReturnXmlForSingleRepeatTag(repeatTagMain, chartXML, xAxisCol,
									yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck);
						} else {
							returnChartXml = updateReturnXmlForMultipleRepeatTagMultiY_NoSeries(repeatTagMain, xAxisCol,
									yAxisCol, zAxisCol, seriesCol, chartXML, rs, dataExistCheck, rsChild,
									returnChartXml);
						}
					} else if (onlyY_onlyMeasure) {
						returnChartXml = updateReturnXmlForSingleRepeatTag_OnlyMeasure(repeatTagMain, chartXML,
								xAxisCol, yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck);
					} else if (onlyY_WithSeries) {
						if (repeatTagMain.indexOf(",") == -1) {
							returnChartXml = updateReturnXmlForSingleRepeatTag_OnlyMeasure(repeatTagMain, chartXML,
									xAxisCol, yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck);
						} else {
							returnChartXml = updateReturnXmlForMultipleRepeatTagMultiY(repeatTagMain, xAxisCol,
									yAxisCol, zAxisCol, seriesCol, chartXML, rs, dataExistCheck, rsChild,
									returnChartXml);
						}
					} else {
						if (repeatTagMain.indexOf(",") == -1) {
							returnChartXml = updateReturnXmlForSingleRepeatTag(repeatTagMain, chartXML, xAxisCol,
									yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck);
						} else {
							returnChartXml = updateReturnXmlForMultipleRepeatTag(repeatTagMain, xAxisCol, yAxisCol,
									zAxisCol, seriesCol, chartXML, rs, dataExistCheck, rsChild, returnChartXml);
						}
					}
				}
			}
			vObject.setChartType(CommonUtils.getValueForXmlTag(chartMataDataXml, "SWF_FILE_NAME"));
			if (ValidationUtil.isValid(returnChartXml)) {
				returnChartXml = returnChartXml.replaceAll("\n", "").replaceAll("\r", "").replaceAll("\\s+", " ");
			}
			List<String> colorAL = new ArrayList<String>();
			String systemPalette = "426FC0,59B697,F57E56,FFD700,DE7CB6,F8BB00,2B9C2B,ba40c3,b95c65,bac883,b96f13,bb5509,b9e5bb,bae653,ba36a1,ba5e94,bb425b,b965ef,ba8e7a,baddc8,b9048c,ba7242,b948b7,bafa01,ba989d,baf076,bb9a35,baca1b,b9abb3,baac4b,ba06bb,b9a090,bb69b6,babff7,b98b4b,bbde60,b9d10e,b90d17,ba7bcc,b9be61,b9789d,b92ae7,bb7c64,b9efde,b9ee47,ba8f12,b9215c,bab5d5,ba671f,b9b43e,baa228,b99706,b93e95,ba0524,ba494f,bb8687,b9bdc9,b86359,ba1a69,ba4be6,b94720,b9da99,bbade1,b98de2,ba0eae,b920c4,bb11dd,ba23f3,bb248b,ba0f46,b9966e,bae7ea,b982c0,b8a884,bad3a5,b902f4,bac18f,b9a91c,ba85ef,bab66d,ba2c7f,ba53d9,ba5471,bb3739,ba5dfc,b9c7eb,b96687,b9350a,bad43d,bbfc30,b97935,b93372,ba3f2d,ba2d17,baa3bf,bbcbb2,ba18d1,bafb98,b95143,ba8458,b9dc31,b9173a,b8edaf,ba70aa,b81f2d,bbf20e";
			if (ValidationUtil.isValid(userDefinedPalette)) {
				systemPalette = userDefinedPalette;
			}
			String color[] = systemPalette.split(",");
			colorAL = Arrays.asList(color[1].split(","));
			if (ValidationUtil.isValid(enableColorPalette) && "Y".equalsIgnoreCase(enableColorPalette)) {
				int colorReplaceIndex = 0;
				matcherObj = Pattern.compile("\\#COLOR\\_CODE\\#", Pattern.DOTALL).matcher(returnChartXml);
				while (matcherObj.find()) {
					colorReplaceIndex++;
				}
				String singleColor = "ba40c3";
				colorAL = returnColorListBasedOnColorCount(colorReplaceIndex, singleColor);
				colorReplaceIndex = 0;
				matcherObj = Pattern.compile("\\#COLOR\\_CODE\\#", Pattern.DOTALL).matcher(returnChartXml);
				while (matcherObj.find()) {
					try {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#", color[colorReplaceIndex]);
					} catch (Exception e) {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#", color[colorReplaceIndex]);
						colorReplaceIndex = 0;
					}
					colorReplaceIndex++;
				}
			} else {
				matcherObj = Pattern.compile("\\#COLOR\\_CODE\\#", Pattern.DOTALL).matcher(returnChartXml);
				while (matcherObj.find()) {
					try {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#",
								ValidationUtil.isValid(userDefinedColorCode) ? userDefinedColorCode : color[0]);
					} catch (Exception e) {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#",
								ValidationUtil.isValid(userDefinedColorCode) ? userDefinedColorCode : color[0]);
					}
				}
			}
			return returnChartXml;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		} finally {
			con.close();
		}
	}

	public String getChartXmlFormObjProperties(String chartType) {
		String sql = "select HTML_TAG_PROPERTY from PRD_CHART_PROPERTIES where "
				/* + "VRD_OBJECT_ID='Col3D' AND " */
				+ " UPPER(OBJ_TAG_ID)=UPPER(?)";
		Object [] args= {chartType};
		return getJdbcTemplate().queryForObject(sql,args, String.class);
	}

	public String getReportQuery(String reportId) {
		setServiceDefaults();
		String strQueryAppr = new String("Select Report_Query from PRD_Reports where Report_ID = ? ");
		Object[] args = { reportId };
		try {
			String i = getJdbcTemplate().queryForObject(strQueryAppr, args, String.class);
			return i;
		} catch (Exception ex) {
			return "";
		}
	}
	public String updateReturnXmlForSingleRepeatTag(String repeatTagMain, String chartXML, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String returnChartXml, ResultSet rs, StringBuffer dataExistCheck) throws SQLException{
		Matcher matcherObj = Pattern.compile("\\<"+repeatTagMain+"(.*?)\\<\\/"+repeatTagMain+"\\>",Pattern.DOTALL).matcher(chartXML);
		if(matcherObj.find()){
			String tagString = "<"+repeatTagMain+matcherObj.group(1)+"</"+repeatTagMain+">";
			ArrayList<String> patternStrAL = new ArrayList<String>();
			ArrayList<String> patternStrColNameAL = new ArrayList<String>();
			Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(tagString);
			
			while(valColMatcher.find()){
				patternStrAL.add(valColMatcher.group(1));
				patternStrColNameAL.add(getColumnName(valColMatcher.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol));
			}
			
			rs.beforeFirst();
			StringBuffer formedXmlSB = new StringBuffer();
	    	while(rs.next()){
	    		if(patternStrColNameAL.size()==1){
		    		if(dataExistCheck.indexOf(","+rs.getObject(patternStrColNameAL.get(0))+",")==-1){
		    			String value = String.valueOf(rs.getObject(patternStrColNameAL.get(0)));
		    			formedXmlSB.append(tagString.replaceAll("\\!\\@\\#"+patternStrAL.get(0)+"\\!\\@\\#", ValidationUtil.isValid(value)?value:""));
			    		dataExistCheck.append(","+rs.getObject(patternStrColNameAL.get(0))+",");
		    		}
	    		}else if(patternStrColNameAL.size()>1){
	    			int arrListIndex = 0;
	    			String tempTagString = tagString;
	    			for(String colName:patternStrColNameAL){
	    				String value ="";
	    				try{
	    					value = String.valueOf(rs.getObject(colName));
	    				}catch(Exception e){
	    					// e.printStackTrace();
							return null;
	    				}
	    				tempTagString = tempTagString.replaceAll("\\!\\@\\#"+patternStrAL.get(arrListIndex)+"\\!\\@\\#", ValidationUtil.isValid(value)?value:"");
	    				arrListIndex++;
	    			}
	    			formedXmlSB.append(tempTagString);
	    		}
	    	}
	    	/* Update to return XML */
	    	Matcher returnMatcher = Pattern.compile("^(.*?)\\<"+repeatTagMain+"(.*?)\\<\\/"+repeatTagMain+"\\>(.*?)$",Pattern.DOTALL).matcher(returnChartXml);
	    	if(returnMatcher.find()){
	    		returnChartXml = returnMatcher.group(1)+formedXmlSB+returnMatcher.group(3);
	    	}
		}
		return returnChartXml;
	}
	
	public String updateReturnXmlForSingleRepeatTag_OnlyMeasure(String repeatTagMain, String chartXML, String xAxisCol,
			String yAxisCol, String zAxisCol, String seriesCol, String returnChartXml, ResultSet rs,
			StringBuffer dataExistCheck) throws SQLException {
		Matcher matcherObj = Pattern
				.compile("\\<" + repeatTagMain + "(.*?)\\<\\/" + repeatTagMain + "\\>", Pattern.DOTALL)
				.matcher(chartXML);
		if (matcherObj.find()) {
			String tagString = "<" + repeatTagMain + matcherObj.group(1) + "</" + repeatTagMain + ">";
			String replaceTagString = "";
			Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#", Pattern.DOTALL).matcher(tagString);
			while (valColMatcher.find()) {
				replaceTagString = "Y_AXIS".equalsIgnoreCase(valColMatcher.group(1)) ? replaceTagString
						: valColMatcher.group(1);
			}
			rs.beforeFirst();
			StringBuffer formedXmlSB = new StringBuffer();
			while (rs.next()) {
				String columnArr[] = yAxisCol.split(",");
				for (String columnName : columnArr) {
					String value = "";
					try {
						value = String.valueOf(rs.getObject(columnName));
					} catch (Exception e) {
						// e.printStackTrace();
						return null;
					}
					value = tagString.replaceAll("\\!\\@\\#" + replaceTagString + "\\!\\@\\#", columnName)
							.replaceAll("\\!\\@\\#Y_AXIS\\!\\@\\#", ValidationUtil.isValid(value) ? value : "");
					if (formedXmlSB.indexOf(value) == -1)
						formedXmlSB.append(value);
				}
			}
			/* Update to return XML */
			Matcher returnMatcher = Pattern
					.compile("^(.*?)\\<" + repeatTagMain + "(.*?)\\<\\/" + repeatTagMain + "\\>(.*?)$", Pattern.DOTALL)
					.matcher(returnChartXml);
			if (returnMatcher.find()) {
				returnChartXml = returnMatcher.group(1) + formedXmlSB + returnMatcher.group(3);
			}
		}
		return returnChartXml;
	}

	public String getColumnName(String pattern, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol) {
		switch (pattern) {
		case "X_AXIS":
			return xAxisCol;
		case "Y_AXIS":
			return yAxisCol;
		case "Z-AXIS":
			return zAxisCol;
		case "SERIES_AXIS":
			return seriesCol;
		default:
			return ".";
		}
	}

	public String updateReturnXmlForMultipleRepeatTagMultiY_NoSeries(String repeatTagMain, String xAxisCol,
			String yAxisCol, String zAxisCol, String seriesCol, String chartXML, ResultSet rs,
			StringBuffer dataExistCheck, CachedRowSetImpl rsChild, String returnChartXml) throws SQLException {
		String[] repeatTagArr = repeatTagMain.split(",");
		if (repeatTagArr.length == 2) {
			Matcher matcherObj = Pattern
					.compile("\\<" + repeatTagArr[0] + "(.*?)\\<\\/" + repeatTagArr[0] + "\\>", Pattern.DOTALL)
					.matcher(chartXML);
			if (matcherObj.find()) {
				String fullTagString = "<" + repeatTagArr[0] + matcherObj.group(1) + "</" + repeatTagArr[0] + ">";
				String parentTagStr = "";
				String childTagString = "";
				String parentReplaceString = "";
				String childReplaceString = "";
				StringBuffer formedXmlSB = new StringBuffer();

				/* Form parent tag String */
				Matcher parentTagMatcher = Pattern.compile("\\<" + repeatTagArr[0] + "(.*?)\\>", Pattern.DOTALL)
						.matcher(fullTagString);
				if (parentTagMatcher.find()) {
					parentTagStr = "<" + repeatTagArr[0] + parentTagMatcher.group(1) + ">";
				}

				/* Get exact pattern from parent tag to be replaced with Y-Axis column name */
				String yAxisColArr[] = yAxisCol.split(",");
				Matcher replaceMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#", Pattern.DOTALL)
						.matcher(parentTagStr);
				if (replaceMatcher.find()) {
					parentReplaceString = replaceMatcher.group(1);
				}

				/* Form child tag String */
				Matcher matcherChildObj = Pattern
						.compile("\\<" + repeatTagArr[1] + "(.*?)\\<\\/" + repeatTagArr[1] + "\\>", Pattern.DOTALL)
						.matcher(fullTagString);
				if (matcherChildObj.find()) {
					childTagString = "<" + repeatTagArr[1] + matcherChildObj.group(1) + "</" + repeatTagArr[1] + ">";
				}

				replaceMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#", Pattern.DOTALL).matcher(childTagString);
				if (replaceMatcher.find()) {
					childReplaceString = replaceMatcher.group(1);
				}

				/* For every Y-Axis column Name from parent tag string [dataset] */
				for (String yCol : yAxisColArr) {
					formedXmlSB.append(parentTagStr.replaceAll("\\!\\@\\#" + parentReplaceString + "\\!\\@\\#", yCol));

					rs.beforeFirst();
					while (rs.next()) {
						formedXmlSB.append(childTagString.replaceAll("\\!\\@\\#" + childReplaceString + "\\!\\@\\#",
								String.valueOf(rs.getObject(yCol))));
					}
					formedXmlSB.append("</" + repeatTagArr[0] + ">");
				}

				/* Update return XML */
				Matcher returnMatcher = Pattern
						.compile("^(.*?)\\<" + repeatTagArr[0] + "(.*?)\\<\\/" + repeatTagArr[0] + "\\>(.*?)$",
								Pattern.DOTALL)
						.matcher(returnChartXml);
				if (returnMatcher.find()) {
					returnChartXml = returnMatcher.group(1) + formedXmlSB + returnMatcher.group(3);
				}
			}
		}
		return returnChartXml;
	}

	public String updateReturnXmlForMultipleRepeatTagMultiY(String repeatTagMain, String xAxisCol, String yAxisCol,
			String zAxisCol, String seriesCol, String chartXML, ResultSet rs, StringBuffer dataExistCheck,
			CachedRowSetImpl rsChild, String returnChartXml) throws SQLException {
		String[] repeatTagArr = repeatTagMain.split(",");
		if (repeatTagArr.length == 2) {
			Matcher matcherObj = Pattern
					.compile("\\<" + repeatTagArr[0] + "(.*?)\\<\\/" + repeatTagArr[0] + "\\>", Pattern.DOTALL)
					.matcher(chartXML);
			if (matcherObj.find()) {
				String fullTagString = "<" + repeatTagArr[0] + matcherObj.group(1) + "</" + repeatTagArr[0] + ">";
				String parentTagStr = "";
				String colNameForParent = "";
				Matcher parentTagMatcher = Pattern.compile("\\<" + repeatTagArr[0] + "(.*?)\\>", Pattern.DOTALL)
						.matcher(fullTagString);
				if (parentTagMatcher.find()) {
					parentTagStr = "<" + repeatTagArr[0] + parentTagMatcher.group(1) + ">";
				}
				Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#", Pattern.DOTALL)
						.matcher(parentTagStr);
				if (valColMatcher.find()) {
					colNameForParent = getColumnName(valColMatcher.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol);
				}
				rs.beforeFirst();
				StringBuffer formedXmlSB = new StringBuffer();
				while (rs.next()) {
					if (dataExistCheck.indexOf("," + rs.getObject(colNameForParent) + ",") == -1) {
						String value = String.valueOf(rs.getObject(colNameForParent));
						formedXmlSB.append(parentTagStr.replaceAll("\\!\\@\\#" + valColMatcher.group(1) + "\\!\\@\\#",
								ValidationUtil.isValid(value) ? value : ""));

						Matcher matcherChildObj = Pattern
								.compile("\\<" + repeatTagArr[1] + "(.*?)\\<\\/" + repeatTagArr[1] + "\\>",
										Pattern.DOTALL)
								.matcher(fullTagString);
						if (matcherChildObj.find()) {
							String childTagString = "<" + repeatTagArr[1] + matcherChildObj.group(1) + "</"
									+ repeatTagArr[1] + ">";

							ArrayList<String> patternStrAL = new ArrayList<String>();
							ArrayList<String> patternStrColNameAL = new ArrayList<String>();
							Matcher valColMatcherChild = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#", Pattern.DOTALL)
									.matcher(childTagString);

							while (valColMatcherChild.find()) {
								patternStrAL.add(valColMatcherChild.group(1));
								patternStrColNameAL.add(getColumnName(valColMatcherChild.group(1), xAxisCol, yAxisCol,
										zAxisCol, seriesCol));
							}
							for (String pattern : patternStrAL) {
								rsChild.beforeFirst();
								while (rsChild.next()) {
									if (String.valueOf(rsChild.getObject(colNameForParent))
											.equalsIgnoreCase(String.valueOf(rs.getObject(colNameForParent)))) {
										for (String colName : patternStrColNameAL.get(0).split(",")) {
											String valueChild = String.valueOf(rsChild.getObject(colName));
											formedXmlSB.append(
													childTagString.replaceAll("\\!\\@\\#" + pattern + "\\!\\@\\#",
															ValidationUtil.isValid(valueChild) ? valueChild : ""));
										}
									}
								}
							}
						}

						formedXmlSB.append("</" + repeatTagArr[0] + ">");
						dataExistCheck.append("," + rs.getObject(colNameForParent) + ",");
					}
				}
				/* Update return XML */
				Matcher returnMatcher = Pattern
						.compile("^(.*?)\\<" + repeatTagArr[0] + "(.*?)\\<\\/" + repeatTagArr[0] + "\\>(.*?)$",
								Pattern.DOTALL)
						.matcher(returnChartXml);
				if (returnMatcher.find()) {
					returnChartXml = returnMatcher.group(1) + formedXmlSB + returnMatcher.group(3);
				}
			}
		}
		return returnChartXml;
	}
	public String updateReturnXmlForMultipleRepeatTag(String repeatTagMain, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String chartXML, ResultSet rs, StringBuffer dataExistCheck, CachedRowSetImpl rsChild, String returnChartXml) throws SQLException{
		String[] repeatTagArr = repeatTagMain.split(",");
		if(repeatTagArr.length==2){
			Matcher matcherObj = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>",Pattern.DOTALL).matcher(chartXML);
			if(matcherObj.find()){
				String fullTagString = "<"+repeatTagArr[0]+matcherObj.group(1)+"</"+repeatTagArr[0]+">";
				String parentTagStr = "";
				String colNameForParent = "";
				Matcher parentTagMatcher = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\>",Pattern.DOTALL).matcher(fullTagString);
				if(parentTagMatcher.find()){
					parentTagStr = "<"+repeatTagArr[0]+parentTagMatcher.group(1)+">";
				}
				Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(parentTagStr);
				if(valColMatcher.find()){
					colNameForParent = getColumnName(valColMatcher.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol);
				}
				rs.beforeFirst();
				StringBuffer formedXmlSB = new StringBuffer();
				while(rs.next()){
		    		if(dataExistCheck.indexOf(","+rs.getObject(colNameForParent)+",")==-1){
		    			String value = String.valueOf(rs.getObject(colNameForParent));
		    			formedXmlSB.append(parentTagStr.replaceAll("\\!\\@\\#"+valColMatcher.group(1)+"\\!\\@\\#", ValidationUtil.isValid(value)?value:"" ));
		    			
		    			Matcher matcherChildObj = Pattern.compile("\\<"+repeatTagArr[1]+"(.*?)\\<\\/"+repeatTagArr[1]+"\\>",Pattern.DOTALL).matcher(fullTagString);
						if(matcherChildObj.find()){
							String childTagString = "<"+repeatTagArr[1]+matcherChildObj.group(1)+"</"+repeatTagArr[1]+">";
							
							ArrayList<String> patternStrAL = new ArrayList<String>();
							ArrayList<String> patternStrColNameAL = new ArrayList<String>();
							Matcher valColMatcherChild = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(childTagString);
							
							while(valColMatcherChild.find()){
								patternStrAL.add(valColMatcherChild.group(1));
								patternStrColNameAL.add(getColumnName(valColMatcherChild.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol));
							}
							
							rsChild.beforeFirst();
							while(rsChild.next()){
								String tempTagString = childTagString;
								if(String.valueOf(rsChild.getObject(colNameForParent)).equalsIgnoreCase(String.valueOf(rs.getObject(colNameForParent))) ){
									int arrListIndex = 0;
									for(String colName:patternStrColNameAL){
										String valueChild = String.valueOf(rsChild.getObject(colName));
					    				tempTagString = tempTagString.replaceAll("\\!\\@\\#"+patternStrAL.get(arrListIndex)+"\\!\\@\\#", ValidationUtil.isValid(valueChild)?valueChild:"");
					    				arrListIndex++;
					    			}
									formedXmlSB.append(tempTagString);
								}
					    	}
						}
						
						formedXmlSB.append("</"+repeatTagArr[0]+">");
			    		dataExistCheck.append(","+rs.getObject(colNameForParent)+",");
		    		}
		    	}
		    	/* Update return XML */
		    	Matcher returnMatcher = Pattern.compile("^(.*?)\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>(.*?)$",Pattern.DOTALL).matcher(returnChartXml);
		    	if(returnMatcher.find()){
		    		returnChartXml = returnMatcher.group(1)+formedXmlSB+returnMatcher.group(3);
		    	}
			}
		}
		return returnChartXml;
	}
	public LinkedHashMap<String,String> getDashboardFilterValue(String sourceQuery){
		ResultSetExtractor mapper = new ResultSetExtractor() {
			public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
				ResultSetMetaData metaData = rs.getMetaData();
				LinkedHashMap<String,String> filterMap = new LinkedHashMap<String,String>();
				while(rs.next()){
					filterMap.put(rs.getString(1),rs.getString(2));
				}
				return filterMap;
			}
		};
		return (LinkedHashMap<String,String>)getJdbcTemplate().query(sourceQuery, mapper);
	}
	public String getDashboardDefaultValue(String sourceQuery){
		ResultSetExtractor mapper = new ResultSetExtractor() {
			String defaultValue = "";
			public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
				ResultSetMetaData metaData = rs.getMetaData();
				while(rs.next()){
					defaultValue = rs.getString(1);
				}
				return defaultValue;
			}
		};
		return (String)getJdbcTemplate().query(sourceQuery, mapper);
	}

	public List<DashboardFilterVb> getDashboardFilterDetail(String filterRefCode) {
		setServiceDefaults();
		List<DashboardFilterVb> collTemp = null;
		try {
			String query = " Select Filter_Ref_code,filter_Xml,FILTER_COUNT from PRD_DASHBOARDS_FILTER "
					+ " where Status = 0 and Filter_Ref_Code = ? ";
			Object [] args= {filterRefCode};
			collTemp = getJdbcTemplate().query(query,args, getDashboardFilterMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	private RowMapper getDashboardFilterMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				DashboardFilterVb vObject = new DashboardFilterVb();
				vObject.setFilterRefCode(rs.getString("Filter_Ref_code"));
				vObject.setFilterRefXml(rs.getString("filter_Xml"));
				vObject.setFilterCnt(rs.getInt("FILTER_COUNT"));
				return vObject;
			}
		};
		return mapper;
	}

	public List<String> returnColorListBasedOnColorCount(int colorCount, String singleColor) {
		Color randomColor = null;
		List<String> colorList = new ArrayList<String>();
		final int maxLimit = 255;
		colorList.add(singleColor);
		int difference = calculateDifferenceCountBase(colorCount);
		int breakMark = difference;
		if ("ff0000".equalsIgnoreCase(singleColor)) {
			do {
				randomColor = new Color(maxLimit, breakMark, breakMark);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark += difference;
			} while (breakMark < maxLimit);
		} else if ("00ff00".equalsIgnoreCase(singleColor)) {
			do {
				randomColor = new Color(breakMark, maxLimit, breakMark);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark += difference;
			} while (breakMark < maxLimit);
		} else if ("0000ff".equalsIgnoreCase(singleColor)) {
			do {
				randomColor = new Color(breakMark, breakMark, maxLimit);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark += difference;
			} while (breakMark < maxLimit);
		} else {
			colorList = new ArrayList<String>();
			colorList.add("000000");
			do {
				randomColor = new Color(breakMark, breakMark, breakMark);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark += difference;
			} while (breakMark < maxLimit);
		}
		return colorList;
	}
	public int calculateDifferenceCountBase(int count){
		if(count<5)return 50; if(count<10)return 25; if(count<12)return 20; if(count<15)return 15;
		if(count<25)return 10; if(count<50)return 5; if(count<85)return 3; if(count<125)return 2;
		return  1;
	}
	public String replacePromptVariables(String reportQuery,DashboardTilesVb promptsVb) {
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_1#", promptsVb.getPromptValue1());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_2#", promptsVb.getPromptValue2());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_3#", promptsVb.getPromptValue3());
		if("FIDELITY".equalsIgnoreCase(clientName)) {
			String visionSbu = promptsVb.getPromptValue4();
			if(ValidationUtil.isValid(promptsVb.getPromptValue4()) && !"'ALL'".equalsIgnoreCase(promptsVb.getPromptValue4())) {
				visionSbu = commonDao.getVisionSbu(promptsVb.getPromptValue4());
			}
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_4#", visionSbu);
		}else {
			reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_4#",  promptsVb.getPromptValue4());
		}
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_5#", promptsVb.getPromptValue5());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_6#", promptsVb.getPromptValue6());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_7#", promptsVb.getPromptValue7());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_8#", promptsVb.getPromptValue8());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_9#", promptsVb.getPromptValue9());
		reportQuery = reportQuery.replaceAll("#PROMPT_VALUE_10#", promptsVb.getPromptValue10());
		reportQuery = reportQuery.replaceAll("#VISION_ID#", ""+CustomContextHolder.getContext().getVisionId());
		/*if(promptsVb.getReportId().contains("MPR") && ValidationUtil.isValid(promptsVb.getPromptValue2())) {
			reportQuery = reportQuery.replaceAll("#PYM#", getDateFormat(promptsVb.getPromptValue2(),"PYM"));
			reportQuery = reportQuery.replaceAll("#NYM#", getDateFormat(promptsVb.getPromptValue2(),"NYM"));
			reportQuery = reportQuery.replaceAll("#PM#", getDateFormat(promptsVb.getPromptValue2(),"PM"));
			reportQuery = reportQuery.replaceAll("#NM#", getDateFormat(promptsVb.getPromptValue2(),"NM"));
			reportQuery = reportQuery.replaceAll("#CM#", getDateFormat(promptsVb.getPromptValue2(),"CM"));
			reportQuery = reportQuery.replaceAll("#CY#", getDateFormat(promptsVb.getPromptValue2(),"CY"));
			reportQuery = reportQuery.replaceAll("#PY#", getDateFormat(promptsVb.getPromptValue2(),"PY"));
		}*/
		return reportQuery;
	}
	
	public List<DashboardTilesVb> getDashboardSubTileDetails(DashboardTilesVb dObj) {
		setServiceDefaults();
		List<DashboardTilesVb> collTemp = null;
		try {
			String query = "Select Dashboard_ID,Tab_ID,PARENT_SEQUENCE,SUB_SEQUENCE,SUB_TILE_ID,Tile_Caption,Query_ID,TILE_TYPE,DRILL_DOWN_FLAG, "
					+ "CHART_TYPE,TILE_PROPERTY_XML,PLACE_HOLDER_COUNT,DOUBLE_WIDTH_FLAG,Theme_AT,Theme from PRD_DASHBOARD_SUB_TILES "
					+ " where Dashboard_ID = ? and Tab_ID = ? and PARENT_SEQUENCE = ? Order by SUB_SEQUENCE ";

			Object[] args = { dObj.getDashboardId(), dObj.getTabId(), dObj.getTileSequence() };
			collTemp = getJdbcTemplate().query(query, args, getDashboardSubTileMapper());
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	private RowMapper getDashboardSubTileMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				DashboardTilesVb vObject = new DashboardTilesVb();
				vObject.setDashboardId(rs.getString("Dashboard_ID"));
				vObject.setTabId(rs.getString("Tab_ID"));
				vObject.setTileSequence(rs.getInt("PARENT_SEQUENCE"));
				vObject.setSubSequence(rs.getString("SUB_SEQUENCE"));
				vObject.setSubTileId(rs.getString("SUB_TILE_ID"));
				vObject.setTileCaption(rs.getString("Tile_Caption"));
				vObject.setQueryId(rs.getString("Query_ID"));
				vObject.setTileType(rs.getString("TILE_TYPE"));
				vObject.setPropertyAttr(rs.getString("TILE_PROPERTY_XML"));
				vObject.setPlaceHolderCnt(rs.getInt("PLACE_HOLDER_COUNT"));
				vObject.setChartType(rs.getString("CHART_TYPE"));
				vObject.setDoubleWidthFlag(rs.getString("DOUBLE_WIDTH_FLAG"));
				if("Y".equalsIgnoreCase(rs.getString("DRILL_DOWN_FLAG"))) {
					vObject.setDrillDownFlag(true);
				}else {
					vObject.setDrillDownFlag(false);
				}
				vObject.setThemeAT(rs.getInt("Theme_AT"));
				vObject.setTheme(rs.getString("Theme"));
				return vObject;
			}
		};
		return mapper;
	}
	//V1.02 Version New Features
	public List<AlphaSubTabVb> getDashboadList(String dashboardGroup) throws DataAccessException {
		VisionUsersVb visionUsersVb = CustomContextHolder.getContext();
		String sql = "Select Dashboard_ID,Dashboard_Name from PRD_DASHBOARDS t1,RA_PROFILE_DASHBOARDS t2 "
				+ " where User_group " + getDbFunction(Constants.PIPELINE, null) + " '-' "
				+ getDbFunction(Constants.PIPELINE, null) + " User_Profile = ? "
				+ " and t2.Home_Dashboard != t1.Dashboard_ID and t1.Dashboard_ID != 'NA' and Dashboard_Group = ? ";
		Object[] lParams = new Object[1];
		lParams[0] = visionUsersVb.getUserGroup() + "-" + visionUsersVb.getUserProfile();
		lParams[1] = dashboardGroup;
		return getJdbcTemplate().query(sql, lParams, getDashboardMapper());
	}
	
	protected RowMapper getDashboardMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AlphaSubTabVb alphaSubTabVb = new AlphaSubTabVb();
				alphaSubTabVb.setAlphaSubTab(rs.getString("Dashboard_ID"));
				alphaSubTabVb.setDescription(rs.getString("Dashboard_Name"));
				return alphaSubTabVb;
			}
		};
		return mapper;
	}
	public LinkedHashMap<String,String> getDashboardFilterTree(String sourceQuery){
		sourceQuery= "Select Bank_Group,Divison,Parent_SBU,Vision_SBU from Sbu_Temp order by Bank_Group,Divison,Parent_SBU,Vision_SBU";
		try {
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					LinkedHashMap<String,String> filterMap = new LinkedHashMap<String,String>();

					DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
		            DocumentBuilder documentBuilder;
					try {
						documentBuilder = documentFactory.newDocumentBuilder();
						Document document = documentBuilder.newDocument();
						Element root = document.createElement("TREE");
			            document.appendChild(root);
						while(rs.next()){
							Element employee = document.createElement(rs.getString(1));
							if(root.getElementsByTagName(rs.getString(1)) == null)
								root.appendChild(employee);
						}
						TransformerFactory transformerFactory = TransformerFactory.newInstance();
			            Transformer transformer = transformerFactory.newTransformer();
			            DOMSource domSource = new DOMSource(document);
			            StringWriter writer = new StringWriter();
			            StreamResult result = new StreamResult(writer);
			            transformer.transform(domSource, result);
			            // System.out.println("XML IN String format is: \n" + writer.toString());
			            
					} catch (ParserConfigurationException e) {
					} catch (TransformerConfigurationException e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
					} catch (TransformerException e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
					}
					return filterMap;
				}
			};
			return (LinkedHashMap<String,String>)getJdbcTemplate().query(sourceQuery, mapper);
		}catch (Exception e) {
			return new LinkedHashMap<>();
        }
		
	}
}