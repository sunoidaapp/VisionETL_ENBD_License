package com.vision.wb;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.EtlConnectorDao;
import com.vision.dao.EtlFileColumnsDao;
import com.vision.dao.EtlFileTableDao;
import com.vision.dao.EtlFileUploadAreaDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlFileColumnsVb;
import com.vision.vb.EtlFileTableVb;
import com.vision.vb.EtlFileUploadAreaVb;
import com.vision.vb.ReviewResultVb;

@Component
public class EtlFileUploadAreaWb extends AbstractDynaWorkerBean<EtlFileUploadAreaVb> {

	@Autowired
	private EtlFileUploadAreaDao etlFileUploadAreaDao;
	@Autowired
	private EtlConnectorWb etlConnectorWb;
	@Autowired
	private EtlConnectorDao etlConnectorDao;
	@Autowired
	private EtlFileTableDao etlFileTableDao;
	@Autowired
	private EtlFileColumnsDao etlFileColumnsDao;

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2116);
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2117);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1); // status
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7); // record indicator
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_FILE_UPLOAD_AREA");
			arrListLocal.add(collTemp);
			collTemp = etlConnectorDao.getDisplayTagList("VPNDIRECT");
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2014); // column Datatype AT
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1102); // Date Format AT
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1103); // Number Format AT
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
	protected List<ReviewResultVb> transformToReviewResults(List<EtlFileUploadAreaVb> approvedCollection,
			List<EtlFileUploadAreaVb> pendingCollection) {
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

		ReviewResultVb lFileId = new ReviewResultVb(rsb.getString("fileId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getFileId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFileId(),
				(!pendingCollection.get(0).getFileId().equals(approvedCollection.get(0).getFileId())));
		lResult.add(lFileId);

		ReviewResultVb lFileName = new ReviewResultVb(rsb.getString("fileName"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getFileName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFileName(),
				(!pendingCollection.get(0).getFileName().equals(approvedCollection.get(0).getFileName())));
		lResult.add(lFileName);
		ReviewResultVb lFileFormat = new ReviewResultVb(rsb.getString("fileFormat"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getFileFormat(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFileFormat(),
				(!pendingCollection.get(0).getFileFormat().equals(approvedCollection.get(0).getFileFormat())));
		lResult.add(lFileFormat);
		ReviewResultVb lFileType = new ReviewResultVb(rsb.getString("fileType"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getFileType(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFileType(),
				(!pendingCollection.get(0).getFileType().equals(approvedCollection.get(0).getFileType())));
		lResult.add(lFileType);

		ReviewResultVb lFtpDetailScript = new ReviewResultVb(rsb.getString("ftpDetailScript"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getFtpDetailScript(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFtpDetailScript(),
				(!pendingCollection.get(0).getFtpDetailScript()
						.equals(approvedCollection.get(0).getFtpDetailScript())));
		lResult.add(lFtpDetailScript);

		ReviewResultVb lFileContext = new ReviewResultVb(rsb.getString("fileContext"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getFileContext(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getFileContext(),
				(!pendingCollection.get(0).getFileContext().equals(approvedCollection.get(0).getFileContext())));
		lResult.add(lFileContext);

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

	public EtlFileUploadAreaDao getEtlFileUploadAreaDao() {
		return etlFileUploadAreaDao;
	}

	public void setEtlFileUploadAreaDao(EtlFileUploadAreaDao etlFileUploadAreaDao) {
		this.etlFileUploadAreaDao = etlFileUploadAreaDao;
	}

	@Override
	protected AbstractDao<EtlFileUploadAreaVb> getScreenDao() {
		return etlFileUploadAreaDao;
	}

	@Override
	protected void setAtNtValues(EtlFileUploadAreaVb vObject) {
		vObject.setFileFormatAt(2116);
		vObject.setFileTypeAt(2117);
		vObject.setRecordIndicatorNt(7);
		vObject.setFileStatusNt(1);
		if (vObject.getFileTableList() != null && vObject.getFileTableList().size() > 0) {
			for (EtlFileTableVb vObjectTable : vObject.getFileTableList()) {
				vObjectTable.setFileStatusNt(1);
				vObjectTable.setRecordIndicatorNt(7);
				if (vObjectTable.getFileColumnsList() != null && vObjectTable.getFileColumnsList().size() > 0) {
					for (EtlFileColumnsVb columnsVb : vObjectTable.getFileColumnsList()) {
						columnsVb.setColumnDatatypeAt(2104);
						columnsVb.setDateFormatNt(2105);
						columnsVb.setNumberFormatNt(2106);
						columnsVb.setFileColumnStatusNt(1);
						columnsVb.setRecordIndicatorNt(7);
					}
				}
			}
		}
	}

	@Override
	protected void setVerifReqDeleteType(EtlFileUploadAreaVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_FILE_UPLOAD_AREA");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}

	public List<EtlFileUploadAreaVb> dynamicScriptCreation(List<EtlFileUploadAreaVb> vObjects, String type) {
		List<EtlFileUploadAreaVb> etlFileUploadAreaVbs = new ArrayList<>();
		for (EtlFileUploadAreaVb etlFileUploadAreaVb : vObjects) {
			StringBuffer variableScript = new StringBuffer("");
			JSONObject extractVbData = new JSONObject(etlFileUploadAreaVb);
			JSONArray eachColData = (JSONArray) extractVbData.getJSONArray("dynamicScript");
			for (int i = 0; i < eachColData.length(); i++) {
				JSONObject ss = eachColData.getJSONObject(i);
				String ch = etlConnectorWb.fixJSONObject(ss);
				JSONObject extractData = new JSONObject(ch);
				String tag = extractData.getString("TAG");
				String encryption = extractData.getString("ENCRYPTION");
				String value = extractData.getString("VALUE");
				if (!ValidationUtil.isValid(type) && !"test".equalsIgnoreCase(type)) {
					value = extractData.getString("VALUE");
					String decodeUrl2 = null;
					try {
						decodeUrl2 = URLDecoder.decode(value, "UTF-8");
					} catch (UnsupportedEncodingException e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
					}
					value = decodeUrl2;
				}
				variableScript.append("{");
				if (ValidationUtil.isValid(encryption) && encryption.equalsIgnoreCase("Yes")) {
					variableScript.append(tag + ":#ENCRYPT$@!" + value + "#");
				} else {
					variableScript.append(tag + ":#CONSTANT$@!" + value + "#");
				}
				variableScript.append("}");
			}
			etlFileUploadAreaVb.setFtpDetailScript(String.valueOf(variableScript));
			etlFileUploadAreaVbs.add(etlFileUploadAreaVb);
		}
		return etlFileUploadAreaVbs;
	}

	public ExceptionCode getAllQueryPopupResult(EtlFileUploadAreaVb queryPopupObj) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setVerifReqDeleteType(queryPopupObj);
			doFormateDataForQuery(queryPopupObj);
			List<EtlFileUploadAreaVb> arrListResult = getScreenDao().getQueryPopupResults(queryPopupObj);
			List<EtlFileUploadAreaVb> responseList = new ArrayList<>();
			if (arrListResult != null && arrListResult.size() > 0) {
				for (EtlFileUploadAreaVb etlFileUploadAreaVb : arrListResult) {
					EtlFileTableVb dObj = new EtlFileTableVb();
					dObj.setConnectorId(etlFileUploadAreaVb.getConnectorId().toUpperCase());
					dObj.setFileId(etlFileUploadAreaVb.getFileId().toUpperCase());
					List<EtlFileTableVb> FileTableList = etlFileTableDao.getQueryPopupResults(dObj);
					List<EtlFileTableVb> responseListChild = new ArrayList<>();
					if (FileTableList != null && FileTableList.size() > 0) {
						for (EtlFileTableVb etlFileTableVb : FileTableList) {
							EtlFileColumnsVb dObjCols = new EtlFileColumnsVb();
							dObjCols.setConnectorId(etlFileTableVb.getConnectorId().toUpperCase());
							dObjCols.setViewName(etlFileTableVb.getViewName().toUpperCase());
							//List<EtlFileColumnsVb> fileColumnsList = etlFileColumnsDao.getQueryPopupResults(dObjCols);
							List<EtlFileColumnsVb> fileColumnsList = etlFileColumnsDao.getQueryResultsByParent(dObjCols.getConnectorId(), Constants.STATUS_PENDING, dObjCols.getViewName());
							if(fileColumnsList!= null && fileColumnsList.isEmpty()) {
								fileColumnsList = etlFileColumnsDao.getQueryResultsByParent(dObjCols.getConnectorId(), Constants.STATUS_ZERO, dObjCols.getViewName());
							}
							etlFileTableVb.setFileColumnsList(fileColumnsList);
							responseListChild.add(etlFileTableVb);
						}
					}
					etlFileUploadAreaVb.setFileTableList(responseListChild);
					responseList.add(etlFileUploadAreaVb);
				}
			}
			if (arrListResult != null && arrListResult.size() > 0) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(responseList);
			} else {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
			}
			exceptionCode.setOtherInfo(queryPopupObj);
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the getAllQueryPopupResult results.", ex);
			}
			return null;
		}
	}

	public ExceptionCode bulkApprove(List<EtlFileUploadAreaVb> vObjects, EtlFileUploadAreaVb queryPopObj) {
		ExceptionCode exceptionCode = null;
		DeepCopy<EtlFileUploadAreaVb> deepCopy = new DeepCopy<EtlFileUploadAreaVb>();
		List<EtlFileUploadAreaVb> clonedObjects = null;
		try {
			setVerifReqDeleteType(queryPopObj);
			doFormateData(vObjects);
			clonedObjects = deepCopy.copyCollection(vObjects);
			exceptionCode = getScreenDao().bulkApprove(vObjects, queryPopObj.isStaticDelete());
			ArrayList<EtlFileUploadAreaVb> tmpResult = (ArrayList<EtlFileUploadAreaVb>) etlFileUploadAreaDao
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
	protected ExceptionCode doValidate(EtlFileUploadAreaVb vObject){
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