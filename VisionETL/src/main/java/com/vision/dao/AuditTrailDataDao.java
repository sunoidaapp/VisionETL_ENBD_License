package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AuditTrailDataVb;
import com.vision.vb.CommonVb;

@Component
public class AuditTrailDataDao extends AbstractDao<AuditTrailDataVb> {
	
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AuditTrailDataVb auditTrailDataVb = new AuditTrailDataVb();
				auditTrailDataVb.setReferenceNo(rs.getString("REFERENCE_NO"));
				auditTrailDataVb.setSubReferenceNo(rs.getString("SUB_REFERENCE_NO"));
				auditTrailDataVb.setTableName(rs.getString("TABLE_NAME"));
				auditTrailDataVb.setChildTableName(rs.getString("CHILD_TABLE_NAME"));
				auditTrailDataVb.setTableSequence(rs.getInt("TABLE_SEQUENCE"));
				auditTrailDataVb.setAuditMode(rs.getString("AUDIT_MODE"));
				auditTrailDataVb.setDateCreation(rs.getString("DATE_CREATION"));
				auditTrailDataVb.setAuditDataOld(rs.getString("AUDIT_DATA_OLD"));
				auditTrailDataVb.setAuditDataNew(rs.getString("AUDIT_DATA_NEW"));
				int auditOldDataOldLogic = rs.getInt("AUDIT_OLD_OLD_LOGIC_FLAG");
				int auditNewDataOldLogic = rs.getInt("AUDIT_NEW_OLD_LOGIC_FLAG");
				if(auditOldDataOldLogic == 0 && auditNewDataOldLogic == 0){
					auditTrailDataVb.setOldLogic(true);
				}
				auditTrailDataVb.setMaker(rs.getLong("MAKER"));
				if(rs.getString("IP_ADDRESS")!=null)
					auditTrailDataVb.setRemoteAddress(rs.getString("IP_ADDRESS"));
				return auditTrailDataVb;
			}
		};
		return mapper;
	}
	private RowMapper getRowMapperForReview(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AuditTrailDataVb auditTrailDataVb = new AuditTrailDataVb();
				auditTrailDataVb.setAuditMode(rs.getString("AUDIT_MODE"));
				auditTrailDataVb.setAuditDataOld(rs.getString("AUDIT_DATA_OLD"));
				auditTrailDataVb.setAuditDataNew(rs.getString("AUDIT_DATA_NEW"));
				auditTrailDataVb.setTablecolumns(rs.getString("TABLE_COLUMN"));
				return auditTrailDataVb;
			}
		};
		return mapper;
	}
	@Override
	protected void setServiceDefaults(){
		serviceName = "AuditTrail";
		serviceDesc = "Audit Trail";
		tableName = "AUDIT_TRAIL_DATA";
		childTableName = "AUDIT_TRAIL_DATA";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	public List<AuditTrailDataVb> getQueryResults(AuditTrailDataVb dObj, int intStatus) {

		Vector<Object> params = new Vector<Object>();
		setServiceDefaults();
		StringBuffer strBufApprove = new StringBuffer(
				"Select TAppr.REFERENCE_NO, TAppr.SUB_REFERENCE_NO, TAppr.TABLE_NAME, TAppr.CHILD_TABLE_NAME, "
						+ " TAppr.TABLE_SEQUENCE, TAppr.AUDIT_MODE, " + getDbFunction(Constants.DATEFUNC, null)
						+ " (TAppr.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION, "
						+ " TAppr.AUDIT_DATA_OLD, INSTR(TAppr.AUDIT_DATA_OLD, '^!#') AUDIT_OLD_OLD_LOGIC_FLAG, "
						+ " TAppr.AUDIT_DATA_NEW, INSTR(TAppr.AUDIT_DATA_NEW, '^!#') AUDIT_NEW_OLD_LOGIC_FLAG, "
						+ " TAppr.MAKER, TAppr.IP_ADDRESS From AUDIT_TRAIL_DATA TAppr");
		try {

			if (ValidationUtil.isValid(dObj.getTableName())) {
				params.addElement("%" + dObj.getTableName().toUpperCase() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.TABLE_NAME) like ?", strBufApprove);
			}
			if (!"-1".equalsIgnoreCase(dObj.getAuditMode())) {
				params.addElement(dObj.getAuditMode());
				CommonUtils.addToQuery("TAppr.AUDIT_MODE = ?", strBufApprove);
			}
			if (ValidationUtil.isValid(dObj.getChildTableName())) {
				params.addElement("%" + dObj.getChildTableName().toUpperCase() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.CHILD_TABLE_NAME) LIKE ?", strBufApprove);
			}
			if (ValidationUtil.isValid(dObj.getRemoteAddress())) {
				params.addElement("%" + dObj.getRemoteAddress().toUpperCase() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.IP_ADDRESS) LIKE ?", strBufApprove);
			}
			if (dObj.getMaker() != 0) {
				params.addElement(dObj.getMaker());
				CommonUtils.addToQuery("TAppr.MAKER = ?", strBufApprove);
			}
			if (ValidationUtil.isValid(dObj.getFromDate()) && ValidationUtil.isValid(dObj.getToDate())) {
				String startDate = dObj.getFromDate();
				String endDate = dObj.getToDate();
				if (startDate.trim().indexOf(" ") > -1) {
					// params.addElement(startDate);
				} else {
					// params.addElement(startDate + "00:00:00");
					startDate = startDate + "00:00:00";
				}
				if (endDate.trim().indexOf(" ") > -1) {
					// params.addElement(endDate);
				} else {
					// params.addElement(endDate+ "23:59:59");
					endDate = endDate + "23:59:59";
				}
				CommonUtils.addToQuery(" TAppr.DATE_CREATION BETWEEN " + getDbFunction(Constants.TO_DATE, startDate)
						+ " AND " + getDbFunction(Constants.TO_DATE, endDate) + " ", strBufApprove);
			}
			dObj.setVerificationRequired(false);
			String orderBy = " Order By TAppr.DATE_CREATION desc, TAppr.TABLE_NAME ";
			return getQueryPopupResults(dObj, new StringBuffer(), strBufApprove, "", orderBy, params);

		} catch (Exception ex) {
			return null;
		}
	}

	public List<AuditTrailDataVb> getQueryResultsForReview(AuditTrailDataVb dObj, int intStatus) {
		setServiceDefaults();
		List<AuditTrailDataVb> collTemp = null;
		Vector<Object> params = new Vector<Object>();
		StringBuffer strQueryAppr = new StringBuffer("select AD.AUDIT_DATA_OLD , AD.AUDIT_DATA_NEW , AD.AUDIT_MODE ");
		if (intStatus == 1)
			strQueryAppr.append(" ,TC.TABLE_COLUMN  from AUDIT_TRAIL_DATA AD, VISION_TABLE_COLUMNS TC "
					+ " Where  AD.REFERENCE_NO = ? AND AD.AUDIT_MODE= ?  and  AD.DATE_CREATION = ?" );
		else
			strQueryAppr.append(" , '' as TABLE_COLUMN from AUDIT_TRAIL_DATA AD "
					+ " Where  AD.REFERENCE_NO = ? AND AD.AUDIT_MODE= ?  and  AD.DATE_CREATION = ?" );
		try {
			params.add(dObj.getReferenceNo());
			params.add(dObj.getAuditMode());
			params.add(getDbFunction(Constants.TO_DATE, dObj.getDateCreation()));
			
			if (dObj.getTableName().equalsIgnoreCase(dObj.getChildTableName())) {
				params.add(dObj.getTableName());
				CommonUtils.addToQuery("AD.TABLE_NAME = ?", strQueryAppr);
				if (intStatus == 1) {
					CommonUtils.addToQuery(" TC.TABLE_NAME = AD.TABLE_NAME ", strQueryAppr);
				}
			} else {
				params.add(dObj.getTableName());
				params.add(dObj.getChildTableName());
				CommonUtils.addToQuery("AD.TABLE_NAME = ?", strQueryAppr);
				CommonUtils.addToQuery("AD.CHILD_TABLE_NAME = ?", strQueryAppr);
				if (intStatus == 1) {
					CommonUtils.addToQuery(" TC.TABLE_NAME = AD.CHILD_TABLE_NAME ", strQueryAppr);
				}
			}
			Object objParams[] = new Object[params.size()];
			for (int Ctr = 0; Ctr < params.size(); Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr);
			collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getRowMapperForReview());
			return collTemp;
		} catch (Exception ex) {
			return null;
		}
	}

	public String fetchMakerVerifierNames(long UserId) {
//		String resultValue = getJdbcTemplate().queryForObject("SELECT USER_NAME FROM VISION_USERS WHERE VISION_ID = '"+UserId+"' ",String.class);
		String sql = "SELECT USER_NAME FROM ETL_USER_VIEW WHERE VISION_ID = ? ";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setMakerName(rs.getString("USER_NAME"));
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = getJdbcTemplate().query(sql, mapper, new Object[] { UserId });
		if (commonVbs != null && !commonVbs.isEmpty()) {
			return commonVbs.get(0).getMakerName();
		}
		return "";
//		return (ValidationUtil.isValid(resultValue)?resultValue:"");
	}
}