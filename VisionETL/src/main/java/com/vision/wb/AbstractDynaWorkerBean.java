package com.vision.wb;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.DeepCopy;
import com.vision.vb.CommonVb;
import com.vision.vb.ReviewResultVb;

@Component
public abstract class AbstractDynaWorkerBean<E extends CommonVb> extends AbstractWorkerBean<E> {

	protected void setAtNtValues(List<E> vObjects){
		for(E vObject: vObjects){
			setAtNtValues(vObject);
		}
	}
	protected void doFormateData(List<E> vObject){}
	protected ExceptionCode doValidate(List<E> vObject) {
		ExceptionCode exceptionCode = null;
		return exceptionCode;
	}
	protected void doSetDesctiptionsAfterQuery(List<E> vObject) {}
	/**
	 * Inserts a new record to the approved or pending tables depending on Verification Required flag of the 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode insertRecord(ExceptionCode pRequestCode, List<E> vObjects){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObject = null;
		E vObject = null;
		try
		{
			setAtNtValues(vObjects);
			vObject = (E) pRequestCode.getOtherInfo();
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copyCollection(vObjects);
			doFormateData(vObjects);
			exceptionCode = doValidate(vObjects);
			if(exceptionCode != null && exceptionCode.getErrorMsg() != ""){
				return exceptionCode;
			}			
			if(!vObject.isVerificationRequired()){
				exceptionCode = getScreenDao().doInsertApprRecord(vObjects);
			}else{
				exceptionCode = getScreenDao().doInsertRecord(vObjects);
			}
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(vObjects);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Insert Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setResponse(clonedObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}
	}
	
	/**
	 * updates the existing record in the approved or pending tables depending on Verification Required flag of the 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode modifyRecord(ExceptionCode pRequestCode, List<E> vObjects){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObject = null;
		E vObject = null;
		try{
			setAtNtValues(vObjects);
			vObject = (E) pRequestCode.getOtherInfo();
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copyCollection(vObjects);
			doFormateData(vObjects);
			exceptionCode = doValidate(vObjects);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}
			if(!vObject.isVerificationRequired()){
				exceptionCode = getScreenDao().doUpdateApprRecord(vObjects);
			}else{
				exceptionCode = getScreenDao().doUpdateRecord(vObjects);
			}
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(vObjects);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Modify Exception " + rex.getCode().getErrorMsg());
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			}
			exceptionCode = rex.getCode();
			exceptionCode.setResponse(clonedObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}
	}
	
	/**
	 * Deletes the existing record in the approved or pending tables depending on Verification Required flag of the 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode deleteRecord(ExceptionCode pRequestCode, List<E> vObjects){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObject = null;
		E vObject = null;
		try{
			setAtNtValues(vObjects);
			vObject = (E) pRequestCode.getOtherInfo();
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copyCollection(vObjects);
			doFormateData(vObjects);
			exceptionCode = doValidate(vObjects);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}
			if(!vObject.isVerificationRequired()){
				exceptionCode = getScreenDao().doDeleteApprRecord(vObjects, vObject);
			}else{
				exceptionCode = getScreenDao().doDeleteRecord(vObjects, vObject);
			}
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(vObjects);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Delete Exception " + rex.getCode().getErrorMsg());
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(clonedObject);
			return exceptionCode;
		}
	}
	@Override
	public List<ReviewResultVb> reviewRecord(E vObject){
		try{
			List<E> approvedCollection = getScreenDao().getQueryResultsForReview(vObject,1);
			List<E> pendingCollection = getScreenDao().getQueryResultsForReview(vObject,0);
			return transformToReviewResults(approvedCollection,pendingCollection);
		}catch(Exception ex){
			return null;
		}
	}
	
	public ExceptionCode getQueryResults(E vObject) {
		int intStatus = 1;
		setVerifReqDeleteType(vObject);
		List<E> collTemp = getScreenDao().getQueryResults(vObject, intStatus);
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(collTemp);
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
	/**
	 * Bulk Approve to be called from other Business classes 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode bulkApprove(List<E> vObjects, E queryPopObj) {
		ExceptionCode exceptionCode = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObjects = null;
		try {
			setVerifReqDeleteType(queryPopObj);
			doFormateData(vObjects);
			clonedObjects = deepCopy.copyCollection(vObjects);
			exceptionCode = getScreenDao().bulkApprove(vObjects, queryPopObj.isStaticDelete());
			ArrayList<E> tmpResult = (ArrayList<E>) getScreenDao().getQueryResults(queryPopObj, 0);
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
	/**
	 * Bulk Approve to be called from other Business classes 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode bulkReject(List<E> vObjects, E queryPopObj) {
		ExceptionCode exceptionCode = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObjects = null;
		try {
			setVerifReqDeleteType(queryPopObj);
			clonedObjects = deepCopy.copyCollection(vObjects);
			exceptionCode = getScreenDao().doBulkReject(vObjects);
			ArrayList<E> tmpResult = (ArrayList<E>) getScreenDao().getQueryResults(queryPopObj, 0);
			exceptionCode.setResponse(tmpResult);
			exceptionCode.setOtherInfo(queryPopObj);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Bulk Reject Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(queryPopObj);
			exceptionCode.setResponse(clonedObjects);
			return exceptionCode;
		}
	}

	public ExceptionCode getQueryResultsSingle(E vObject) {
		int intStatus = 1;
		setVerifReqDeleteType(vObject);
		List<E> collTemp = getScreenDao().getQueryResults(vObject, intStatus);
		if (collTemp.size() == 0) {
			intStatus = 0;
			collTemp = getScreenDao().getQueryResults(vObject, intStatus);
		}
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(((ArrayList<E>) collTemp).get(0));
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			getScreenDao().fetchMakerVerifierNames(((ArrayList<E>) collTemp).get(0));
			((ArrayList<E>) collTemp).get(0).setVerificationRequired(vObject.isVerificationRequired());
			((ArrayList<E>) collTemp).get(0).setStaticDelete(vObject.isStaticDelete());
			exceptionCode.setOtherInfo(((ArrayList<E>) collTemp).get(0));
			return exceptionCode;
		}
	}

	public ExceptionCode reviewRecordNew(E vObject) {
		ExceptionCode exceptionCode = null;
		List<ReviewResultVb> list = null;
		try {
			List<E> approvedCollection = getScreenDao().getQueryResults(vObject, 1);
			List<E> pendingCollection = getScreenDao().getQueryResults(vObject, 0);
			list = transformToReviewResults(approvedCollection, pendingCollection);
			exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(list);
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setResponse(list);
			return exceptionCode;
		}
	}

	public ExceptionCode getQueryDetailResults(E vObject) {
		setVerifReqDeleteType(vObject);
		List<E> collTemp = getScreenDao().getQueryDetailResults(vObject);
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			doSetDesctiptionsAfterQuery(collTemp);
			doSetDesctiptionsAfterQuery(vObject);
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}
	
}
