package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.EtlFeedCategoryDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlFeedAlertMatrixVb;
import com.vision.vb.EtlFeedCategoryAlertConfigVb;
import com.vision.vb.EtlFeedCategoryDepedencyVb;
import com.vision.vb.EtlFeedCategoryVb;
import com.vision.vb.ReviewResultVb;

@Component
public class EtlFeedCategoryWb extends AbstractDynaWorkerBean<EtlFeedCategoryVb> {

	@Autowired
	private EtlFeedCategoryDao etlFeedCategoryDao;

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7);
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_FEED_CATEGORY");
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2130);
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
	protected List<ReviewResultVb> transformToReviewResults(List<EtlFeedCategoryVb> approvedCollection,
			List<EtlFeedCategoryVb> pendingCollection) {
		ArrayList collTemp = getPageLoadValues();
		ResourceBundle rsb = CommonUtils.getResourceManger();
		if (pendingCollection != null && pendingCollection.size() > 0)
			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));
		if (approvedCollection != null && approvedCollection.size() > 0)
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

		ReviewResultVb lCategoryId = new ReviewResultVb(rsb.getString("categoryId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getCategoryId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getCategoryId(),
				(!pendingCollection.get(0).getCategoryId()
						.equals(approvedCollection.get(0).getCategoryId())));
		lResult.add(lCategoryId);

		ReviewResultVb lCategoryDescription = new ReviewResultVb(rsb.getString("description"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getCategoryDescription(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getCategoryDescription(),
				(!pendingCollection.get(0).getCategoryDescription()
						.equals(approvedCollection.get(0).getCategoryDescription())));
		lResult.add(lCategoryDescription);


		ReviewResultVb lChannelTypeSMS = new ReviewResultVb(rsb.getString("channelTypeSMS"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getChannelTypeSMS(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getChannelTypeSMS(),
				(!pendingCollection.get(0).getChannelTypeSMS()
						.equals(approvedCollection.get(0).getChannelTypeSMS())));
		lResult.add(lChannelTypeSMS);


		ReviewResultVb lChannelTypeEMAIL = new ReviewResultVb(rsb.getString("channelTypeEmail"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getChannelTypeEmail(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getChannelTypeEmail(),
				(!pendingCollection.get(0).getChannelTypeEmail()
						.equals(approvedCollection.get(0).getChannelTypeEmail())));
		lResult.add(lChannelTypeEMAIL);

		
		String catdepPend = "";
		String catdepPendDesc = "";
		if (pendingCollection != null && pendingCollection.size() > 0) {
			List<EtlFeedCategoryDepedencyVb> lstpend =pendingCollection.get(0).getEtlCategoryDeplst();
			for (EtlFeedCategoryDepedencyVb etlFeedCategoryVb : lstpend) {
				if(!ValidationUtil.isValid(catdepPend)) {
					catdepPend = etlFeedCategoryVb.getDependentCategory();
					catdepPendDesc = etlFeedCategoryVb.getDependentCategoryDesc();
				}else {
					catdepPend = catdepPend +","+etlFeedCategoryVb.getDependentCategory();
					catdepPendDesc = catdepPendDesc +","+etlFeedCategoryVb.getDependentCategoryDesc();
				}
			}
		}
		String catdepApp = "";
		String catdepAppDesc = "";
		if (approvedCollection != null && approvedCollection.size() > 0) {
			List<EtlFeedCategoryDepedencyVb> lstpend =approvedCollection.get(0).getEtlCategoryDeplst();
			for (EtlFeedCategoryDepedencyVb etlFeedCategoryVb : lstpend) {
				if(!ValidationUtil.isValid(catdepApp)) {
					catdepApp = etlFeedCategoryVb.getDependentCategory();
					catdepAppDesc = etlFeedCategoryVb.getDependentCategoryDesc();
				}else {
					catdepApp = catdepApp +","+etlFeedCategoryVb.getDependentCategory();
					catdepAppDesc = catdepAppDesc +","+etlFeedCategoryVb.getDependentCategoryDesc();
				}
			}
		}
		ReviewResultVb lCategoryDependency = new ReviewResultVb(rsb.getString("categoryDependency"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: catdepPendDesc,
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: catdepAppDesc,
				(!catdepPendDesc
						.equals(catdepAppDesc)));
		lResult.add(lCategoryDependency);

		
		String catAlertPend = "";
		String catAlertPendDesc = "";
		if (pendingCollection != null && pendingCollection.size() > 0) {
			List<EtlFeedCategoryAlertConfigVb> lstpend =pendingCollection.get(0).getEtlCategoryAlertConfiglst();
			for (EtlFeedCategoryAlertConfigVb etlFeedCategoryVb : lstpend) {
				if(!ValidationUtil.isValid(catAlertPend)) {
					catAlertPend = etlFeedCategoryVb.getEventType();
					catAlertPendDesc = etlFeedCategoryVb.getEventTypeDesc();
				}else {
					catAlertPend = catAlertPend +","+etlFeedCategoryVb.getEventType();
					catAlertPendDesc = catAlertPendDesc +","+etlFeedCategoryVb.getEventTypeDesc();
				}
			}
		}
		String catAlertApp = "";
		String catAlertAppDesc = "";
		if (approvedCollection != null && approvedCollection.size() > 0) {
			List<EtlFeedCategoryAlertConfigVb> lstpend =approvedCollection.get(0).getEtlCategoryAlertConfiglst();
			for (EtlFeedCategoryAlertConfigVb etlFeedCategoryVb : lstpend) {
				if(!ValidationUtil.isValid(catAlertApp)) {
					catAlertApp = etlFeedCategoryVb.getEventType();
					catAlertAppDesc = etlFeedCategoryVb.getEventTypeDesc();
				}else {
					catAlertApp = catAlertApp +","+etlFeedCategoryVb.getEventType();
					catAlertAppDesc = catAlertAppDesc +","+etlFeedCategoryVb.getEventTypeDesc();
				}
			}
		}
		ReviewResultVb lCategoryAlert = new ReviewResultVb(rsb.getString("categoryAlertConfig"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: catAlertPendDesc,
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: catAlertAppDesc,
				(!catAlertPendDesc
						.equals(catAlertAppDesc)));
		lResult.add(lCategoryAlert);
		
		ReviewResultVb lCategoryStatus = new ReviewResultVb(rsb.getString("status"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getStatusDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getStatusDesc(),
				(!pendingCollection.get(0).getStatusDesc()
						.equals(approvedCollection.get(0).getStatusDesc())));
		lResult.add(lCategoryStatus);

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
				(!pendingCollection.get(0).getDateCreation()
						.equals(approvedCollection.get(0).getDateCreation())));
		lResult.add(lDateCreation);

		return lResult;
	}

	public EtlFeedCategoryDao getEtlFeedCategoryDao() {
		return etlFeedCategoryDao;
	}

	public void setEtlFeedCategoryDao(EtlFeedCategoryDao etlFeedCategoryDao) {
		this.etlFeedCategoryDao = etlFeedCategoryDao;
	}

	@Override
	protected AbstractDao<EtlFeedCategoryVb> getScreenDao() {
		return etlFeedCategoryDao;
	}

	@Override
	protected void setAtNtValues(EtlFeedCategoryVb vObject) {
		vObject.setCategoryStatusNt(1);
		vObject.setRecordIndicatorNt(7);
	}

	@Override
	protected void setVerifReqDeleteType(EtlFeedCategoryVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_FEED_CATEGORY");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}
	
	public List<EtlFeedCategoryVb> findDependencyCategory(EtlFeedCategoryVb vObject) {
		List<EtlFeedCategoryVb> dependencylst = null;
		try {
			String tableName = "ETL_FEED_CATEGORY";
			dependencylst = etlFeedCategoryDao.getCategoryListing(vObject, tableName);
			if (dependencylst != null && dependencylst.size() > 0) {
			} else {
				dependencylst = etlFeedCategoryDao.getCategoryListing(vObject, "ETL_FEED_CATEGORY_PEND");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return dependencylst;
	}

	public List<EtlFeedAlertMatrixVb> findAlertMatrix(EtlFeedAlertMatrixVb vObject) {
		List<EtlFeedAlertMatrixVb> matrixlst = null;
		try {
			matrixlst = etlFeedCategoryDao.getAlertMatrixListing(vObject);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return matrixlst;
	}
	
	@Override
	protected ExceptionCode doValidate(EtlFeedCategoryVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("ETLCategory", operation);
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