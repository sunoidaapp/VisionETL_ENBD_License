package com.vision.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vision.exception.ExceptionCode;
import com.vision.exception.JSONExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.Constants;
import com.vision.vb.MagnifierResultVb;
import com.vision.wb.MagnifierWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("magnifierControls")
@Api(value="magnifierControls" , description="Magnifier Query Popups")

public class MagnifierController{
	@Autowired
	MagnifierWb magnifierWb;
	public static final String joinSymbol="+";
	public static String owner="";
	public static String getOwner() {
		return owner;
	}
	public static void setOwner(String owner) {
		MagnifierController.owner = owner;
	}
	/*-------------------------------------GET ALL RECORDS FROM CIF MASTER------------------------------------------*/
	@RequestMapping(path = "/magnifierQuery", method = RequestMethod.POST)
	@ApiOperation(value = "Get Magnifier Data",notes = "Pass Query Id to get Magnifier data",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> magnifierQuery(@RequestBody MagnifierResultVb vObject) {
		vObject.setVerificationRequired(false);
		vObject.setRecordIndicator(0);
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = magnifierWb.getAllQueryPopupResult(vObject);
			vObject.setQuery("");
			exceptionCode.setOtherInfo(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results", exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	/*public ModelAndView magnifierQuery(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWinow");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
				List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
				exceptionCode.setOtherInfo(magnifierResultVb);
				exceptionCode.setResponse(queryList);
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}*/
	
	/*public ModelAndView magnifierQuery1(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWindowLevels");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierQueryChargeId(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWindowChargeId");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}	
	public ModelAndView magnifierQueryMgtLines(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWindowMgtLines");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	 
	public ModelAndView magnifierQuery4(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
 			modelAndView = new ModelAndView("MagnifierWinow1");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			if(!magnifierResultVb.getQuery().contains("AO_NAME")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
			}
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierReportLine(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
 			modelAndView = new ModelAndView("MagWindowReportLine");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierLocal(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWinow");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	
	public ModelAndView magnifierQueryCurrency(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
 			modelAndView = new ModelAndView("MagWindowCurrency");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	
	public ModelAndView magnifierQueryBudget(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
 			modelAndView = new ModelAndView("MagnifierWindowBudget");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(magnifierResultVb.getQuery().contains("------")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
			}
			if(magnifierResultVb.getQuery().contains("LIKE ___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
			}
			if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
			}
			if(magnifierResultVb.getQuery().contains("___")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
			}
			if(magnifierResultVb.getQuery().contains("---")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
			}
			if(!magnifierResultVb.getQuery().contains("AO_NAME")){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
			}
			List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
			exceptionCode.setOtherInfo(magnifierResultVb);
			exceptionCode.setResponse(queryList);
			modelAndView.addObject("ExceptionCode", exceptionCode);
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierTableColumnsQuery(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWindowTableColumns");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("#OWNER#", owner));
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
				List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
				exceptionCode.setOtherInfo(magnifierResultVb);
				exceptionCode.setResponse(queryList);
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierQueryDyn(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWindowDynamic");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("#JOIN_SYMBOL#", joinSymbol));
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("#OWNER#", owner));
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
				List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
				exceptionCode.setOtherInfo(magnifierResultVb);
				exceptionCode.setResponse(queryList);
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierXBRLQuery(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWindowXBRL");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
				List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
				exceptionCode.setOtherInfo(magnifierResultVb);
				exceptionCode.setResponse(queryList);
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifieruploadFillingStakeholderMag(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("UploadFillingStakeholderMag");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				String whereConditions = magnifierResultVb.getQuery();
				if(ValidationUtil.isValid(magnifierResultVb.getVerifierName())){
					whereConditions = magnifierResultVb.getVerifierName();
				}else{
					magnifierResultVb.setVerifierName(whereConditions);
				}
				String seletQuery = "SELECT DISTINCT ADF.COUNTRY, ADF.LE_BOOK , T1.LEB_DESCRIPTION "+ 
                 " FROM ADF_PROCESS_CONTROL ADF , LE_BOOK T1 WHERE ADF.ACQUISITION_PROCESS_TYPE = 'XLS' AND T1.LE_BOOK = ADF.LE_BOOK AND T1.COUNTRY = ADF.COUNTRY ";
				if(ValidationUtil.isValid(CustomContextHolder.getContext().getUpdateRestrictionLeBook())){
					seletQuery = seletQuery+ "AND ADF.COUNTRY||'-'||ADF.LE_BOOK IN("+CustomContextHolder.getContext().getUpdateRestrictionLeBook().toUpperCase()+")";
				}
				seletQuery = seletQuery + whereConditions;
				magnifierResultVb.setQuery(seletQuery);
				
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
				List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
				exceptionCode.setOtherInfo(magnifierResultVb);
				exceptionCode.setResponse(queryList);
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}	
	public ModelAndView magnifierStackQuery(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("StakeholderMag");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				String whereConditions = magnifierResultVb.getQuery();
				String seletQuery = "SELECT COUNTRY, LE_BOOK, LEB_DESCRIPTION FROM LE_BOOK WHERE LEB_STATUS = 0  ";
				if(ValidationUtil.isValid(CustomContextHolder.getContext().getUpdateRestrictionLeBook())){
					seletQuery = seletQuery+ "AND COUNTRY||'-'||LE_BOOK IN("+CustomContextHolder.getContext().getUpdateRestrictionLeBook().toUpperCase()+")";
				}
				seletQuery = seletQuery + whereConditions;
				magnifierResultVb.setQuery(seletQuery);
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
					List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
					magnifierResultVb.setQuery(whereConditions);
					exceptionCode.setOtherInfo(magnifierResultVb);
					exceptionCode.setResponse(queryList);
					modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}
	public ModelAndView magnifierQueryDrillDownProp(HttpServletRequest request,HttpServletResponse response){
		ModelAndView modelAndView = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			modelAndView = new ModelAndView("MagnifierWinowDrillDownPop");
			MagnifierResultVb magnifierResultVb = new MagnifierResultVb();
			ServletRequestDataBinder binder = createBinder(request, magnifierResultVb);
			binder.bind(request);
			if(ValidationUtil.isValid(magnifierResultVb.getQuery())){
				if(magnifierResultVb.getQuery().contains("------")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("------", "+"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE UPPER(___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'%")); 
				}
				if(magnifierResultVb.getQuery().contains("LIKE ___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("LIKE ___", "LIKE '%"));
				}
				if(magnifierResultVb.getQuery().contains("LIKE") && magnifierResultVb.getQuery().contains("---!")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---!", "%'"));	
				}
				if(magnifierResultVb.getQuery().contains("___")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("___", "'"));
				}
				if(magnifierResultVb.getQuery().contains("---")){
					magnifierResultVb.setQuery(magnifierResultVb.getQuery().replaceAll("---", "'"));
				}
				magnifierResultVb.setQuery(magnifierResultVb.getQuery().toUpperCase());
				List<MagnifierResultVb> queryList = getMagnifierWb().getQueryPopupResults(magnifierResultVb);
				exceptionCode.setOtherInfo(magnifierResultVb);
				exceptionCode.setResponse(queryList);
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}else{
				exceptionCode.setErrorMsg("Resend");
				modelAndView.addObject("ExceptionCode", exceptionCode);
			}
		  return modelAndView;
		}catch (Exception e) {
			return modelAndView;
		}
	}*/
}