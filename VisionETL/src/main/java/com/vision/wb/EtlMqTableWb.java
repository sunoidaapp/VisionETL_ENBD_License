package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.EtlMqColumnsDao;
import com.vision.dao.EtlMqTableDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlMqColumnsVb;
import com.vision.vb.EtlMqTableVb;
import com.vision.vb.ReviewResultVb;

@Component
public class EtlMqTableWb extends AbstractDynaWorkerBean<EtlMqTableVb> {

	@Autowired
	private EtlMqTableDao etlMqTableDao;
	@Autowired
	private EtlMqColumnsDao etlMqColumnsDao;

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(2008);
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2014); // column Datatype AT
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1103); // Date Format Nt
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1102); // Number Format Nt
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_MQ_TABLE");
			arrListLocal.add(collTemp);
			return arrListLocal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the Page load values.", ex);
			}
			return null;
		}
	}

	@Override
	protected List<ReviewResultVb> transformToReviewResults(List<EtlMqTableVb> approvedCollection,
			List<EtlMqTableVb> pendingCollection) {
		ArrayList collTemp = getPageLoadValues();
		ResourceBundle rsb = CommonUtils.getResourceManger();
		if (pendingCollection != null)
			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));
		if (approvedCollection != null)
			getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));
		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();

		ReviewResultVb lCountry = new ReviewResultVb(rsb.getString("country"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getCountryDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getCountryDesc(),
				(!pendingCollection.get(0).getCountry().equals(approvedCollection.get(0).getCountry())));
		lResult.add(lCountry);

		ReviewResultVb lLeBook = new ReviewResultVb(rsb.getString("leBook"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getLeBookDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLeBookDesc(),
				(!pendingCollection.get(0).getLeBook().equals(approvedCollection.get(0).getLeBook())));
		lResult.add(lLeBook);

		ReviewResultVb lConnectorId = new ReviewResultVb(rsb.getString("connectorId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectorId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectorId(),
				(!pendingCollection.get(0).getConnectorId().equals(approvedCollection.get(0).getConnectorId())));
		lResult.add(lConnectorId);

		ReviewResultVb lQueryId = new ReviewResultVb(rsb.getString("queryId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getQueryId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getQueryId(),
				(!pendingCollection.get(0).getQueryId().equals(approvedCollection.get(0).getQueryId())));
		lResult.add(lQueryId);

		ReviewResultVb lQueryDescription = new ReviewResultVb(rsb.getString("queryDescription"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getQueryDescription(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getQueryDescription(),
				(!pendingCollection.get(0).getQueryDescription()
						.equals(approvedCollection.get(0).getQueryDescription())));
		lResult.add(lQueryDescription);

		ReviewResultVb lQuery = new ReviewResultVb(rsb.getString("query"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getQuery(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getQuery(),
				(!pendingCollection.get(0).getQuery().equals(approvedCollection.get(0).getQuery())));
		lResult.add(lQuery);

		ReviewResultVb lValidatedFlag = new ReviewResultVb(rsb.getString("validatedFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getValidatedFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getValidatedFlag(),
				(!pendingCollection.get(0).getValidatedFlag().equals(approvedCollection.get(0).getValidatedFlag())));
		lResult.add(lValidatedFlag);

		ReviewResultVb lRecordIndicator = new ReviewResultVb(rsb.getString("recordIndicator"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getRecordIndicatorDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getRecordIndicatorDesc(),
				(!pendingCollection.get(0).getRecordIndicatorDesc()
						.equals(approvedCollection.get(0).getRecordIndicatorDesc())));
		lResult.add(lRecordIndicator);

		ReviewResultVb lMaker = new ReviewResultVb(rsb.getString("maker"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getMaker() == 0 ? "" : pendingCollection.get(0).getMakerName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getMaker() == 0 ? "" : approvedCollection.get(0).getMakerName(),
				(pendingCollection.get(0).getMaker() != approvedCollection.get(0).getMaker()));
		lResult.add(lMaker);

		ReviewResultVb lVerifier = new ReviewResultVb(rsb.getString("verifier"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getVerifier() == 0 ? "" : pendingCollection.get(0).getVerifierName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getVerifier() == 0 ? ""
								: approvedCollection.get(0).getVerifierName(),
				(pendingCollection.get(0).getVerifier() != approvedCollection.get(0).getVerifier()));
		lResult.add(lVerifier);

		ReviewResultVb lDateLastModified = new ReviewResultVb(rsb.getString("dateLastModified"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateLastModified(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateLastModified(),
				(!pendingCollection.get(0).getDateLastModified()
						.equals(approvedCollection.get(0).getDateLastModified())));
		lResult.add(lDateLastModified);

		ReviewResultVb lDateCreation = new ReviewResultVb(rsb.getString("dateCreation"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateCreation(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateCreation(),
				(!pendingCollection.get(0).getDateCreation().equals(approvedCollection.get(0).getDateCreation())));
		lResult.add(lDateCreation);
		return lResult;

	}

	public EtlMqTableDao getEtlMqTableDao() {
		return etlMqTableDao;
	}

	public void setEtlMqTableDao(EtlMqTableDao etlMqTableDao) {
		this.etlMqTableDao = etlMqTableDao;
	}

	@Override
	protected AbstractDao<EtlMqTableVb> getScreenDao() {
		return etlMqTableDao;
	}

	@Override
	protected void setAtNtValues(EtlMqTableVb vObject) {
		vObject.setRecordIndicatorNt(7);
		vObject.setQueryStatusNt(1);
		vObject.setQueryCategoryNt(2008);
		if (vObject.getEtlMqColumnsList() != null) {
			for (EtlMqColumnsVb etlMqColumnsVb : vObject.getEtlMqColumnsList()) {
				etlMqColumnsVb.setColumnDatatypeAt(2014);
				etlMqColumnsVb.setDateFormatNt(1103);
				etlMqColumnsVb.setNumberFormatNt(1102);
				etlMqColumnsVb.setMqColumnStatusNt(1);
				etlMqColumnsVb.setRecordIndicatorNt(7);
			}
		}
	}

	@Override
	protected void setVerifReqDeleteType(EtlMqTableVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_MQ_TABLE");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}

	protected List<ReviewResultVb> transformToReviewResultsChild(List<EtlMqColumnsVb> approvedCollection,
			List<EtlMqColumnsVb> pendingCollection) {
		ArrayList collTemp = getPageLoadValues();
		ResourceBundle rsb = CommonUtils.getResourceManger();
		if (pendingCollection != null)
			etlMqColumnsDao.fetchMakerVerifierNames(pendingCollection.get(0));
		if (approvedCollection != null)
			etlMqColumnsDao.fetchMakerVerifierNames(approvedCollection.get(0));
		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();

		ReviewResultVb lConnectorId = new ReviewResultVb(rsb.getString("connectorId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectorId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectorId());
		lResult.add(lConnectorId);

		ReviewResultVb lQueryId = new ReviewResultVb(rsb.getString("queryId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getQueryId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getQueryId());
		lResult.add(lQueryId);

		ReviewResultVb lColumnName = new ReviewResultVb(rsb.getString("columnName"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getColumnName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getColumnName());
		lResult.add(lColumnName);

		ReviewResultVb lColumnDatatype = new ReviewResultVb(rsb.getString("columnDatatype"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getColumnDatatype(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getColumnDatatype());
		lResult.add(lColumnDatatype);

		ReviewResultVb lDateFormat = new ReviewResultVb(rsb.getString("dateFormat"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateFormatNtDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateFormatNtDesc());
		lResult.add(lDateFormat);
		
		ReviewResultVb lNumberFormat = new ReviewResultVb(rsb.getString("numberFormat"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getNumberFormatNtDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getNumberFormatNtDesc());
		lResult.add(lNumberFormat);

		ReviewResultVb lPrimaryKeyFlag = new ReviewResultVb(rsb.getString("primaryKeyFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getPrimaryKeyFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getPrimaryKeyFlag());
		lResult.add(lPrimaryKeyFlag);

		ReviewResultVb lPartitionColumnFlag = new ReviewResultVb(rsb.getString("partitionColumnFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getPartitionColumnFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getPartitionColumnFlag());
		lResult.add(lPartitionColumnFlag);

		ReviewResultVb lRecordIndicator = new ReviewResultVb(rsb.getString("recordIndicator"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getRecordIndicatorDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getRecordIndicatorDesc());
		lResult.add(lRecordIndicator);
		ReviewResultVb lMaker = new ReviewResultVb(rsb.getString("maker"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getMaker() == 0 ? "" : pendingCollection.get(0).getMakerName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getMaker() == 0 ? "" : approvedCollection.get(0).getMakerName());
		lResult.add(lMaker);
		ReviewResultVb lVerifier = new ReviewResultVb(rsb.getString("verifier"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getVerifier() == 0 ? "" : pendingCollection.get(0).getVerifierName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getVerifier() == 0 ? ""
								: approvedCollection.get(0).getVerifierName());
		lResult.add(lVerifier);
		ReviewResultVb lDateLastModified = new ReviewResultVb(rsb.getString("dateLastModified"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateLastModified(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateLastModified());
		lResult.add(lDateLastModified);
		ReviewResultVb lDateCreation = new ReviewResultVb(rsb.getString("dateCreation"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateCreation(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateCreation());
		lResult.add(lDateCreation);
		return lResult;

	}
	@Override
	public ExceptionCode getQueryResultsSingle(EtlMqTableVb vObject){
		int intStatus = 1;
		setVerifReqDeleteType(vObject);
		ExceptionCode exceptionCode = null;
		List<EtlMqTableVb> collTemp = etlMqTableDao.getQueryResults(vObject,intStatus);
		if (collTemp.size() == 0){
			intStatus = 0;
			collTemp = etlMqTableDao.getQueryResults(vObject,intStatus);
		}
		if(collTemp.size() == 0){
			exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}else{
			if (!vObject.isVerificationRequired() || collTemp.get(0).getRecordIndicator() == 0) {
				List<EtlMqColumnsVb> collTempFinAdjDetails = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(),Constants.STATUS_ZERO);
				collTemp.get(0).setEtlMqColumnsList(collTempFinAdjDetails);
			}else {
				List<EtlMqColumnsVb> collTempFinAdjDetails = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(),Constants.STATUS_PENDING);
				collTemp.get(0).setEtlMqColumnsList(collTempFinAdjDetails);
			}
			doSetDesctiptionsAfterQuery(((ArrayList<EtlMqTableVb>)collTemp).get(0));
   		    exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			getScreenDao().fetchMakerVerifierNames(((ArrayList<EtlMqTableVb>)collTemp).get(0));
			((ArrayList<EtlMqTableVb>)collTemp).get(0).setVerificationRequired(vObject.isVerificationRequired());
			((ArrayList<EtlMqTableVb>)collTemp).get(0).setStaticDelete(vObject.isStaticDelete());
			exceptionCode.setOtherInfo(((ArrayList<EtlMqTableVb>)collTemp).get(0));
			return exceptionCode;
		}
	}
	
	@Override
	protected ExceptionCode doValidate(EtlMqTableVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("EtlMqTable", operation);
		if(!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation +" "+Constants.userRestrictionMsg);
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
}