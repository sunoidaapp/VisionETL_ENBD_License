package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedDestinationVb;
import com.vision.vb.EtlFeedMainVb;

@Component
public class EtlFeedDestinationDao extends AbstractDao<EtlFeedDestinationVb> {

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String FeedTransformStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000,
			"TAppr.FEED_TRANSFORM_STATUS", "FEED_TRANSFORM_STATUS_DESC");
	String FeedTransformStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000,
			"TPend.FEED_TRANSFORM_STATUS", "FEED_TRANSFORM_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedDestinationVb vObject = new EtlFeedDestinationVb();
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
				if (rs.getString("DESTINATION_CONNECTOR_ID") != null) {
					vObject.setDestinationConnectorId(rs.getString("DESTINATION_CONNECTOR_ID"));
				} else {
					vObject.setDestinationConnectorId("");
				}
				if (rs.getString("DESTINATION_CONTEXT") != null) {
					vObject.setDestinationContext(rs.getString("DESTINATION_CONTEXT"));
				} else {
					vObject.setDestinationContext("");
				}
				vObject.setFeedTransformStatusNt(rs.getInt("FEED_TRANSFORM_STATUS_NT"));
				vObject.setFeedTransformStatus(rs.getInt("FEED_TRANSFORM_STATUS"));
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
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<EtlFeedDestinationVb> getQueryPopupResults(EtlFeedDestinationVb dObj) {
		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY" + ",TAppr.LE_BOOK" + ",TAppr.FEED_ID"
				+ ",TAppr.DESTINATION_CONNECTOR_ID" + ", "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.DESTINATION_CONTEXT") + " DESTINATION_CONTEXT"
				+ ",TAppr.FEED_TRANSFORM_STATUS_NT" + ",TAppr.FEED_TRANSFORM_STATUS" + ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR" + ",TAppr.MAKER" + ",TAppr.VERIFIER" + ",TAppr.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED" + ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION" + " from ETL_FEED_DESTINATION TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_FEED_DESTINATION_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY" + ",TPend.LE_BOOK" + ",TPend.FEED_ID"
				+ ",TPend.DESTINATION_CONNECTOR_ID" + ", "
				+ getDbFunction(Constants.TO_CHAR, "TPend.DESTINATION_CONTEXT") + " DESTINATION_CONTEXT"
				+ ",TPend.FEED_TRANSFORM_STATUS_NT" + ",TPend.FEED_TRANSFORM_STATUS" + ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR" + ",TPend.MAKER" + ",TPend.VERIFIER" + ",TPend.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED" + ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION" + " from ETL_FEED_DESTINATION_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY" + ",TUpl.LE_BOOK" + ",TUpl.FEED_ID"
				+ ",TUpl.DESTINATION_CONNECTOR_ID" + ", " + getDbFunction(Constants.TO_CHAR, "TUpl.DESTINATION_CONTEXT")
				+ " DESTINATION_CONTEXT" + ",TUpl.FEED_TRANSFORM_STATUS_NT" + ",TUpl.FEED_TRANSFORM_STATUS"
				+ ",TUpl.RECORD_INDICATOR_NT" + ",TUpl.RECORD_INDICATOR" + ",TUpl.MAKER" + ",TUpl.VERIFIER"
				+ ",TUpl.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TUpl.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION" + " from ETL_FEED_DESTINATION_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String(
				" Not Exists (Select 'X' From ETL_FEED_DESTINATION_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID) ");
		String strWhereNotExistsPendInUpl = new String(
				" Not Exists (Select 'X' From ETL_FEED_DESTINATION_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID) ");
		try {
			if (ValidationUtil.isValid(dObj.getCountry())) {
				params.addElement(dObj.getCountry());
				CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getLeBook())) {
				params.addElement(dObj.getLeBook());
				CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				params.addElement(dObj.getFeedId());
				CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
			}
			/*
			 * if (ValidationUtil.isValid(dObj.getDestinationConnectorId())){
			 * params.addElement(dObj.getDestinationConnectorId());
			 * CommonUtils.addToQuery("UPPER(TAppr.DESTINATION_CONNECTOR_ID) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.DESTINATION_CONNECTOR_ID) = ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.DESTINATION_CONNECTOR_ID) = ?",
			 * strBufUpl); } if (ValidationUtil.isValid(dObj.getDestinationContext())){
			 * params.addElement(dObj.getDestinationContext());
			 * CommonUtils.addToQuery("UPPER(TAppr.DESTINATION_CONTEXT) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.DESTINATION_CONTEXT) = ?",
			 * strBufPending); CommonUtils.addToQuery("UPPER(TUpl.DESTINATION_CONTEXT) = ?",
			 * strBufUpl); } if (ValidationUtil.isValid(dObj.getFeedTransformStatus())){
			 * params.addElement(dObj.getFeedTransformStatus());
			 * CommonUtils.addToQuery("TAppr.FEED_TRANSFORM_STATUS = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.FEED_TRANSFORM_STATUS = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.FEED_TRANSFORM_STATUS = ?", strBufUpl); } if
			 * (dObj.getRecordIndicator() != -1){ if (dObj.getRecordIndicator() > 3){
			 * params.addElement(new Integer(0));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufUpl); }else{
			 * params.addElement(new Integer(dObj.getRecordIndicator()));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufUpl); } }
			 */
			String orderBy = " Order By COUNTRY, LE_BOOK, FEED_ID ";
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

	public List<EtlFeedDestinationVb> getQueryResults(EtlFeedDestinationVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedDestinationVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY" + ",TAppr.LE_BOOK" + ",TAppr.FEED_ID"
				+ ",TAppr.DESTINATION_CONNECTOR_ID" + ",TAppr.DESTINATION_CONTEXT" + ",TAppr.FEED_TRANSFORM_STATUS_NT"
				+ ",TAppr.FEED_TRANSFORM_STATUS" + ",TAppr.RECORD_INDICATOR_NT" + ",TAppr.RECORD_INDICATOR"
				+ ",TAppr.MAKER" + ",TAppr.VERIFIER" + ",TAppr.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED" + ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_DESTINATION TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY" + ",TPend.LE_BOOK" + ",TPend.FEED_ID"
				+ ",TPend.DESTINATION_CONNECTOR_ID" + ",TPend.DESTINATION_CONTEXT" + ",TPend.FEED_TRANSFORM_STATUS_NT"
				+ ",TPend.FEED_TRANSFORM_STATUS" + ",TPend.RECORD_INDICATOR_NT" + ",TPend.RECORD_INDICATOR"
				+ ",TPend.MAKER" + ",TPend.VERIFIER" + ",TPend.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED" + ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_DESTINATION_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY" + ",TUpl.LE_BOOK" + ",TUpl.FEED_ID"
				+ ",TUpl.DESTINATION_CONNECTOR_ID" + ",TUpl.DESTINATION_CONTEXT" + ",TUpl.FEED_TRANSFORM_STATUS_NT"
				+ ",TUpl.FEED_TRANSFORM_STATUS" + ",TUpl.RECORD_INDICATOR_NT" + ",TUpl.RECORD_INDICATOR" + ",TUpl.MAKER"
				+ ",TUpl.VERIFIER" + ",TUpl.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TUpl.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_DESTINATION_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

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

	public List<EtlFeedDestinationVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedDestinationVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY" + ",TAppr.LE_BOOK" + ",TAppr.FEED_ID"
				+ ",TAppr.DESTINATION_CONNECTOR_ID" + ",TAppr.DESTINATION_CONTEXT" + ",TAppr.FEED_TRANSFORM_STATUS_NT"
				+ ",TAppr.FEED_TRANSFORM_STATUS" + ",TAppr.RECORD_INDICATOR_NT" + ",TAppr.RECORD_INDICATOR"
				+ ",TAppr.MAKER" + ",TAppr.VERIFIER" + ",TAppr.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED" + ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_DESTINATION TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY" + ",TPend.LE_BOOK" + ",TPend.FEED_ID"
				+ ",TPend.DESTINATION_CONNECTOR_ID" + ",TPend.DESTINATION_CONTEXT" + ",TPend.FEED_TRANSFORM_STATUS_NT"
				+ ",TPend.FEED_TRANSFORM_STATUS" + ",TPend.RECORD_INDICATOR_NT" + ",TPend.RECORD_INDICATOR"
				+ ",TPend.MAKER" + ",TPend.VERIFIER" + ",TPend.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED" + ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_DESTINATION_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY" + ",TUpl.LE_BOOK" + ",TUpl.FEED_ID"
				+ ",TUpl.DESTINATION_CONNECTOR_ID" + ",TUpl.DESTINATION_CONTEXT" + ",TUpl.FEED_TRANSFORM_STATUS_NT"
				+ ",TUpl.FEED_TRANSFORM_STATUS" + ",TUpl.RECORD_INDICATOR_NT" + ",TUpl.RECORD_INDICATOR" + ",TUpl.MAKER"
				+ ",TUpl.VERIFIER" + ",TUpl.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TUpl.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_DESTINATION_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
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

	@Override
	protected List<EtlFeedDestinationVb> selectApprovedRecord(EtlFeedDestinationVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedDestinationVb> doSelectPendingRecord(EtlFeedDestinationVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlFeedDestinationVb> doSelectUplRecord(EtlFeedDestinationVb vObject) {
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlFeedDestinationVb records) {
		return records.getFeedTransformStatus();
	}

	@Override
	protected void setStatus(EtlFeedDestinationVb vObject, int status) {
		vObject.setFeedTransformStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedDestinationVb vObject) {
		if (ValidationUtil.isValid(vObject.getDestinationContext())) {
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("<root><table>", ""));
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>", ""));
			vObject.setDestinationContext("<root><table>" + vObject.getDestinationContext() + "</table></root>");
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>",
					"</table><stagingTable>" + vObject.getStagingContext() + "</stagingTable></root>"));
		}
		String query = "Insert Into ETL_FEED_DESTINATION (COUNTRY, LE_BOOK, FEED_ID, DESTINATION_CONNECTOR_ID, "
				+ "FEED_TRANSFORM_STATUS_NT, FEED_TRANSFORM_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, DESTINATION_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getDestinationConnectorId(), vObject.getFeedTransformStatusNt(),
				vObject.getFeedTransformStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
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
					String clobData = ValidationUtil.isValid(vObject.getDestinationContext())
							? vObject.getDestinationContext()
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

	@Override
	protected int doInsertionPend(EtlFeedDestinationVb vObject) {
		if (ValidationUtil.isValid(vObject.getDestinationContext())) {
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("<root><table>", ""));
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>", ""));
			vObject.setDestinationContext("<root><table>" + vObject.getDestinationContext() + "</table></root>");
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>",
					"</table><stagingTable>" + vObject.getStagingContext() + "</stagingTable></root>"));
		}
		String query = "Insert Into ETL_FEED_DESTINATION_PEND (COUNTRY, LE_BOOK, FEED_ID, DESTINATION_CONNECTOR_ID, "
				+ "FEED_TRANSFORM_STATUS_NT, FEED_TRANSFORM_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, DESTINATION_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getDestinationConnectorId(), vObject.getFeedTransformStatusNt(),
				vObject.getFeedTransformStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
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
					String clobData = ValidationUtil.isValid(vObject.getDestinationContext())
							? vObject.getDestinationContext()
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

	@Override
	protected int doInsertionUpl(EtlFeedDestinationVb vObject) {
		if (ValidationUtil.isValid(vObject.getDestinationContext())) {
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("<root><table>", ""));
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>", ""));
			vObject.setDestinationContext("<root><table>" + vObject.getDestinationContext() + "</table></root>");
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>",
					"</table><stagingTable>" + vObject.getStagingContext() + "</stagingTable></root>"));
		}
		String query = "Insert Into ETL_FEED_DESTINATION_UPL (COUNTRY, LE_BOOK, FEED_ID, DESTINATION_CONNECTOR_ID, "
				+ "FEED_TRANSFORM_STATUS_NT, FEED_TRANSFORM_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, DESTINATION_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getDestinationConnectorId(), vObject.getFeedTransformStatusNt(),
				vObject.getFeedTransformStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
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
					String clobData = ValidationUtil.isValid(vObject.getDestinationContext())
							? vObject.getDestinationContext()
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

	@Override
	protected int doInsertionPendWithDc(EtlFeedDestinationVb vObject) {
		if (ValidationUtil.isValid(vObject.getDestinationContext())) {
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("<root><table>", ""));
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>", ""));
			vObject.setDestinationContext("<root><table>" + vObject.getDestinationContext() + "</table></root>");
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>",
					"</table><stagingTable>" + vObject.getStagingContext() + "</stagingTable></root>"));
		}
		String query = "Insert Into ETL_FEED_DESTINATION_PEND (COUNTRY, LE_BOOK, FEED_ID, DESTINATION_CONNECTOR_ID, "
				+ "FEED_TRANSFORM_STATUS_NT, FEED_TRANSFORM_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, DESTINATION_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " , ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getDestinationConnectorId(), vObject.getFeedTransformStatusNt(),
				vObject.getFeedTransformStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
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
					String clobData = ValidationUtil.isValid(vObject.getDestinationContext())
							? vObject.getDestinationContext()
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

	@Override
	protected int doUpdateAppr(EtlFeedDestinationVb vObject) {
		if (ValidationUtil.isValid(vObject.getDestinationContext())) {
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("<root><table>", ""));
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>", ""));
			vObject.setDestinationContext("<root><table>" + vObject.getDestinationContext() + "</table></root>");
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>",
					"</table><stagingTable>" + vObject.getStagingContext() + "</stagingTable></root>"));
		}
		String query = "Update ETL_FEED_DESTINATION Set DESTINATION_CONTEXT = ?, DESTINATION_CONNECTOR_ID = ?, FEED_TRANSFORM_STATUS_NT = ?, FEED_TRANSFORM_STATUS = ?,"
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getDestinationConnectorId(), vObject.getFeedTransformStatusNt(),
				vObject.getFeedTransformStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId() };

		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getDestinationContext())
							? vObject.getDestinationContext()
							: "";
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
		}
		return result;
	}

	@Override
	protected int doUpdatePend(EtlFeedDestinationVb vObject) {
		if (ValidationUtil.isValid(vObject.getDestinationContext())) {
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("<root><table>", ""));
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>", ""));
			vObject.setDestinationContext("<root><table>" + vObject.getDestinationContext() + "</table></root>");
			vObject.setDestinationContext(vObject.getDestinationContext().replaceAll("</table></root>",
					"</table><stagingTable>" + vObject.getStagingContext() + "</stagingTable></root>"));
		}
		String query = "Update ETL_FEED_DESTINATION_PEND Set DESTINATION_CONTEXT = ?, DESTINATION_CONNECTOR_ID = ?, FEED_TRANSFORM_STATUS_NT = ?, FEED_TRANSFORM_STATUS = ?,"
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getDestinationConnectorId(), vObject.getFeedTransformStatusNt(),
				vObject.getFeedTransformStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId() };

		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getDestinationContext())
							? vObject.getDestinationContext()
							: "";
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
		}
		return result;
	}

	@Override
	protected int doDeleteAppr(EtlFeedDestinationVb vObject) {
		String query = "Delete From ETL_FEED_DESTINATION Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedDestinationVb vObject) {
		String query = "Delete From ETL_FEED_DESTINATION_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deleteUplRecord(EtlFeedDestinationVb vObject) {
		String query = "Delete From ETL_FEED_DESTINATION_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_DESTINATION";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_DESTINATION_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_DESTINATION_PEND";
		}
		String query = "Delete From " + table + " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_DESTINATION_PRS";
		String query = "Delete From " + table
				+ "  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_FEED_DESTINATION";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_DESTINATION_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_DESTINATION_PEND";
		}
		String query = "update " + table
				+ " set FEED_TRANSFORM_STATUS = ? ,RECORD_INDICATOR =? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedDestinationVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getDestinationConnectorId()))
			strAudit.append(
					"DESTINATION_CONNECTOR_ID" + auditDelimiterColVal + vObject.getDestinationConnectorId().trim());
		else
			strAudit.append("DESTINATION_CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDestinationContext()))
			strAudit.append("DESTINATION_CONTEXT" + auditDelimiterColVal + vObject.getDestinationContext().trim());
		else
			strAudit.append("DESTINATION_CONTEXT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_TRANSFORM_STATUS_NT" + auditDelimiterColVal + vObject.getFeedTransformStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_TRANSFORM_STATUS" + auditDelimiterColVal + vObject.getFeedTransformStatus());
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

		return strAudit.toString();
	}

	@Override
	protected void setServiceDefaults() {
		serviceName = "EtlFeedDestination";
		serviceDesc = "ETL Feed Destination";
		tableName = "ETL_FEED_DESTINATION";
		childTableName = "ETL_FEED_DESTINATION";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}

	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_DESTINATION_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_DESTINATION_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_FEED_DESTINATION_PRS.SESSION_ID ) ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
