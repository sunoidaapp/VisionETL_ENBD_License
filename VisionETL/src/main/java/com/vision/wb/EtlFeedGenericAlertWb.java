package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import com.vision.dao.AbstractDao;
import com.vision.dao.EtlFeedGenericAlertDao;
import com.vision.dao.EtlFeedMainDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlFeedGenericAlertVb;
import com.vision.vb.ReviewResultVb;

@Controller
public class EtlFeedGenericAlertWb extends AbstractDynaWorkerBean<EtlFeedGenericAlertVb> {

	@Autowired
	private EtlFeedGenericAlertDao etlFeedGenericAlertDao;

	@Autowired
	private EtlFeedMainDao etlFeedMainDao;

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1);
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7);
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_FEED_GENERIC_ALERT");
			arrListLocal.add(collTemp);
			/*Map<String, String> feedCategory = getEtlFeedMainDao().getFeedCategory();
			arrListLocal.add(feedCategory);*/
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
	protected List<ReviewResultVb> transformToReviewResults(List<EtlFeedGenericAlertVb> approvedCollection,
			List<EtlFeedGenericAlertVb> pendingCollection) {
		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();
		ResourceBundle rsb = CommonUtils.getResourceManger();
		try {
		if (pendingCollection != null && pendingCollection.size() > 0)
			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));
		if (approvedCollection != null && approvedCollection.size() > 0)
			getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));

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

		ReviewResultVb lvl1AlertMailId = new ReviewResultVb(rsb.getString("lvl1AlertMailID"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getLvl1AlertMailId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLvl1AlertMailId(),
				(!pendingCollection.get(0).getLvl1AlertMailId()
						.equals(approvedCollection.get(0).getLvl1AlertMailId())));
		lResult.add(lvl1AlertMailId);


		ReviewResultVb lvl1AlertDescription = new ReviewResultVb(rsb.getString("lvl1AlertMailIDDescription"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getLvl1AlertDescription(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLvl1AlertDescription(),
				(!pendingCollection.get(0).getLvl1AlertDescription()
						.equals(approvedCollection.get(0).getLvl1AlertDescription())));
		lResult.add(lvl1AlertDescription);
		
		
		ReviewResultVb lvl2AlertMailId = new ReviewResultVb(rsb.getString("lvl2AlertMailID"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getLvl2AlertMailId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLvl2AlertMailId(),
				(!pendingCollection.get(0).getLvl2AlertMailId()
						.equals(approvedCollection.get(0).getLvl2AlertMailId())));
		lResult.add(lvl2AlertMailId);

		ReviewResultVb lvl2AlertDescription = new ReviewResultVb(rsb.getString("lvl2AlertMailIDDescription"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getLvl2AlertDescription(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLvl2AlertDescription(),
				(!pendingCollection.get(0).getLvl2AlertDescription()
						.equals(approvedCollection.get(0).getLvl2AlertDescription())));
		lResult.add(lvl2AlertDescription);
		

		ReviewResultVb lvl3AlertMailId = new ReviewResultVb(rsb.getString("lvl3AlertMailID"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getLvl3AlertMailId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLvl3AlertMailId(),
				(!pendingCollection.get(0).getLvl3AlertMailId()
						.equals(approvedCollection.get(0).getLvl3AlertMailId())));
		lResult.add(lvl3AlertMailId);


		ReviewResultVb lvl3AlertDescription = new ReviewResultVb(rsb.getString("lvl3AlertMailIDDescription"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getLvl3AlertDescription(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLvl3AlertDescription(),
				(!pendingCollection.get(0).getLvl3AlertDescription()
						.equals(approvedCollection.get(0).getLvl3AlertDescription())));
		lResult.add(lvl3AlertDescription);


		ReviewResultVb terminatedByFlag = new ReviewResultVb(rsb.getString("terminatedByFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getTerminatedByFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getTerminatedByFlag(),
				(!pendingCollection.get(0).getTerminatedByFlag()
						.equals(approvedCollection.get(0).getTerminatedByFlag())));
		lResult.add(terminatedByFlag);

		ReviewResultVb reinitiatedByFlag = new ReviewResultVb(rsb.getString("reinitiatedByFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getReinitiatedByFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getReinitiatedByFlag(),
				(!pendingCollection.get(0).getReinitiatedByFlag()
						.equals(approvedCollection.get(0).getReinitiatedByFlag())));
		lResult.add(reinitiatedByFlag);

		ReviewResultVb completionStatusFlag = new ReviewResultVb(rsb.getString("completionStatusFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getCompletionStatusFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getCompletionStatusFlag(),
				(!pendingCollection.get(0).getCompletionStatusFlag()
						.equals(approvedCollection.get(0).getCompletionStatusFlag())));
		lResult.add(completionStatusFlag);

		ReviewResultVb startTimeFlag = new ReviewResultVb(rsb.getString("startTimeFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getStartTimeFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getStartTimeFlag(),
				(!pendingCollection.get(0).getStartTimeFlag()
						.equals(approvedCollection.get(0).getStartTimeFlag())));
		lResult.add(startTimeFlag);

		ReviewResultVb endTimeFlag = new ReviewResultVb(rsb.getString("endTimeFlag"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getEndTimeFlag(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getEndTimeFlag(),
				(!pendingCollection.get(0).getEndTimeFlag()
						.equals(approvedCollection.get(0).getEndTimeFlag())));
		lResult.add(endTimeFlag);

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

		} catch (Exception e) {
		}
		return lResult;
	}
    
	public EtlFeedGenericAlertDao getEtlFeedGenericAlertDao() {
		return etlFeedGenericAlertDao;
	}

	public void setEtlFeedGenericAlertDao(EtlFeedGenericAlertDao etlFeedGenericAlertDao) {
		this.etlFeedGenericAlertDao = etlFeedGenericAlertDao;
	}

	@Override
	protected AbstractDao<EtlFeedGenericAlertVb> getScreenDao() {
		return etlFeedGenericAlertDao;
	}

	public EtlFeedMainDao getEtlFeedMainDao() {
		return etlFeedMainDao;
	}

	public void setEtlFeedMainDao(EtlFeedMainDao etlFeedMainDao) {
		this.etlFeedMainDao = etlFeedMainDao;
	}

	@Override
	protected void setAtNtValues(EtlFeedGenericAlertVb vObject) {
		vObject.setEtlAuditStatusNt(1);
		vObject.setRecordIndicatorNt(7);
	}

	@Override
	protected void setVerifReqDeleteType(EtlFeedGenericAlertVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_FEED_GENERIC_ALERT");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
	}
	
	@Override
	protected ExceptionCode doValidate(EtlFeedGenericAlertVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("EtlGenericAlert", operation);
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