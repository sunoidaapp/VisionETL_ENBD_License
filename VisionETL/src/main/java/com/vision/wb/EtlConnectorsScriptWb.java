package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.EtlConnectorScriptDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlConnectorsScriptVb;
import com.vision.vb.ReviewResultVb;

@Component
public class EtlConnectorsScriptWb extends AbstractDynaWorkerBean<EtlConnectorsScriptVb> {

	@Autowired
	private EtlConnectorScriptDao etlConnectorScriptDao;

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7);
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2122); // Script Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2123); // Execution Type
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_CONNECTOR_SCRIPTS");
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
	protected List<ReviewResultVb> transformToReviewResults(List<EtlConnectorsScriptVb> approvedCollection,
			List<EtlConnectorsScriptVb> pendingCollection) {
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

		ReviewResultVb lScriptId = new ReviewResultVb(rsb.getString("scriptId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getScriptId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getScriptId(),
				(!pendingCollection.get(0).getScriptId().equals(approvedCollection.get(0).getScriptId())));
		lResult.add(lScriptId);

		ReviewResultVb lScriptDescription = new ReviewResultVb(rsb.getString("scriptDescription"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getScriptDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getScriptDesc(),
				(!pendingCollection.get(0).getScriptDesc()
						.equals(approvedCollection.get(0).getScriptDesc())));
		lResult.add(lScriptDescription);

		ReviewResultVb lScript = new ReviewResultVb(rsb.getString("script"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getScript(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getScript(),
				(!pendingCollection.get(0).getScript().equals(approvedCollection.get(0).getScript())));
		lResult.add(lScript);
		
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

	public EtlConnectorScriptDao getEtlConnectorScriptDao() {
		return etlConnectorScriptDao;
	}

	public void setEtlConnectorScriptDao(EtlConnectorScriptDao etlConnectorScriptDao) {
		this.etlConnectorScriptDao = etlConnectorScriptDao;
	}

	@Override
	protected AbstractDao<EtlConnectorsScriptVb> getScreenDao() {
		return etlConnectorScriptDao;
	}

	@Override
	protected void setAtNtValues(EtlConnectorsScriptVb vObject) {
		vObject.setRecordIndicatorNt(7);
		vObject.setScriptStatusNt(1);
		vObject.setScriptTypeAt(2022);
		vObject.setExecutionTypeAt(2023);
	}

	@Override
	protected void setVerifReqDeleteType(EtlConnectorsScriptVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_CONNECTORS_SCRIPT");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}
	
	@Override
	protected ExceptionCode doValidate(EtlConnectorsScriptVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("EtlConnectorsScript", operation);
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