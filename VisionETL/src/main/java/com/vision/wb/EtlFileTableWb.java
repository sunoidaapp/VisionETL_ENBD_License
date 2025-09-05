package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.EtlFileTableDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlFileColumnsVb;
import com.vision.vb.EtlFileTableVb;
import com.vision.vb.ReviewResultVb;

@Component
public class EtlFileTableWb extends AbstractDynaWorkerBean<EtlFileTableVb> {

	@Autowired
	private EtlFileTableDao etlFileTableDao;

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7);
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_FILE_TABLE");
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
	protected List<ReviewResultVb> transformToReviewResults(List<EtlFileTableVb> approvedCollection,
			List<EtlFileTableVb> pendingCollection) {
		ArrayList collTemp = getPageLoadValues();
		ResourceBundle rsb = CommonUtils.getResourceManger();
		if (pendingCollection != null)
			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));
		if (approvedCollection != null)
			getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));
		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();

		ReviewResultVb lConnectorId = new ReviewResultVb(rsb.getString("connectorId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectorId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectorId(),
				(!pendingCollection.get(0).getConnectorId().equals(approvedCollection.get(0).getConnectorId())));
		lResult.add(lConnectorId);

		ReviewResultVb lViewName = new ReviewResultVb(rsb.getString("viewName"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getViewName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getViewName(),
				(!pendingCollection.get(0).getViewName().equals(approvedCollection.get(0).getViewName())));
		lResult.add(lViewName);

		ReviewResultVb lFileId = new ReviewResultVb(rsb.getString("fileId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getFileId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFileId(),
				(!pendingCollection.get(0).getFileId().equals(approvedCollection.get(0).getFileId())));
		lResult.add(lFileId);

		ReviewResultVb lSheetName = new ReviewResultVb(rsb.getString("sheetName"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getSheetName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getSheetName(),
				(!pendingCollection.get(0).getSheetName().equals(approvedCollection.get(0).getSheetName())));
		lResult.add(lSheetName);

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

	public EtlFileTableDao getEtlFileTableDao() {
		return etlFileTableDao;
	}

	public void setEtlFileTableDao(EtlFileTableDao etlFileTableDao) {
		this.etlFileTableDao = etlFileTableDao;
	}

	@Override
	protected AbstractDao<EtlFileTableVb> getScreenDao() {
		return etlFileTableDao;
	}

	@Override
	protected void setAtNtValues(EtlFileTableVb vObject) {
		vObject.setFileStatusNt(1);
		vObject.setRecordIndicatorNt(7);
		if (vObject.getFileColumnsList() != null && vObject.getFileColumnsList().size() > 0) {
			for (EtlFileColumnsVb columnsVb : vObject.getFileColumnsList()) {
				columnsVb.setColumnDatatypeAt(2104);
				columnsVb.setDateFormatNt(2105);
				columnsVb.setNumberFormatNt(2106);
				columnsVb.setFileColumnStatusNt(1);
				columnsVb.setRecordIndicatorNt(7);
			}
		}
	}

	@Override
	protected void setVerifReqDeleteType(EtlFileTableVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_FILE_TABLE");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}

	public ExceptionCode bulkApprove(List<EtlFileTableVb> vObjects, EtlFileTableVb queryPopObj) {
		ExceptionCode exceptionCode = null;
		DeepCopy<EtlFileTableVb> deepCopy = new DeepCopy<EtlFileTableVb>();
		List<EtlFileTableVb> clonedObjects = null;
		try {
			setVerifReqDeleteType(queryPopObj);
			doFormateData(vObjects);
			clonedObjects = deepCopy.copyCollection(vObjects);
			exceptionCode = getScreenDao().bulkApprove(vObjects, queryPopObj.isStaticDelete());
			ArrayList<EtlFileTableVb> tmpResult = (ArrayList<EtlFileTableVb>) etlFileTableDao
					.getQueryResultsByParent(queryPopObj.getConnectorId(), 0);
			exceptionCode.setResponse(tmpResult);
			exceptionCode.setOtherInfo(queryPopObj);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Bulk Approve Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(queryPopObj);
			exceptionCode.setResponse(clonedObjects);
			return exceptionCode;
		}
	}
	
	@Override
	protected ExceptionCode doValidate(EtlFileTableVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("EtlConnector", operation);
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