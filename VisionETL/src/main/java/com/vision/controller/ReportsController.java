package com.vision.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vision.authentication.CustomContextHolder;
import com.vision.dao.CommonDao;
import com.vision.dao.ReportsDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.JSONExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.PromptTreeVb;
import com.vision.vb.ReportFilterVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.ReportsWb;
import com.vision.wb.VisionUploadWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("reports")
@Api(value="reports" , description="Reports")
public class ReportsController {
	@Autowired
	ReportsWb reportsWb;
	@Autowired
	ReportsDao reportsDao;
	@Autowired
	VisionUploadWb visionUploadWb;
	@Autowired
	CommonDao commonDao;
	
	/*-------------------------------------GET REPORT FILTERS------------------------------------------*/
	@RequestMapping(path = "/getReportFilter", method = RequestMethod.POST)
	@ApiOperation(value = "Get Report Filter",notes = "Get Filter for both Reports and Dashboard",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getReportFilter(@RequestBody ReportsVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.reportFilterProcess(vObject.getFilterRefCode());
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*-------------------------------------GET CHILD DEPENDENT FILTER------------------------------------------*/
	@RequestMapping(path = "/getChildDependentFilter", method = RequestMethod.POST)
	@ApiOperation(value = "Get Dependency Filter Value",notes = "Get the value for Dependent filter",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getReportDependentFilter(@RequestBody ReportFilterVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.getFilterSourceValue(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*-------------------------------------GET REPORT CHILD DEPENDENT FILTER------------------------------------------*/
	@RequestMapping(path = "/getReportChildDependentFilter", method = RequestMethod.POST)
	@ApiOperation(value = "Get Report Prompt Dependency Filter Value",notes = "Get the value for Dependent filter",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDependentValueForReportPrompts(@RequestBody ReportFilterVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.getReportFilterSourceValue(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*------------------------------------FETCHING REPORTS RESULTS-------------------------------*/
	@RequestMapping(path = "/getReportData", method = RequestMethod.POST)
	@ApiOperation(value = "Get Report Data",notes = "Get the Report Data for the specified Report Id",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getSubReportResultData(@RequestBody ReportsVb vObject) throws SQLException {
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			ExceptionCode exceptionCode = reportsWb.getReportDetails(vObject);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					ReportsVb subReportsVb = (ReportsVb) exceptionCode.getResponse();
					exceptionCode =  reportsDao.getResultData(subReportsVb,false);
				}
			}
			exceptionCode.setOtherInfo(vObject);
		//	reportsDao.insertReportsAudit(vObject, exceptionCode.getErrorMsg(),"GENERATION");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	
	/*------------------------------------MDM REPORTS EXCEL EXPORT-------------------------------*/
	@RequestMapping(path = "/reportExcelExport", method = RequestMethod.POST)
	@ApiOperation(value = "Get Excel Report",notes = "Export Report Data to Excel",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> excelExportReport(@RequestBody ReportsVb vObject,HttpServletRequest request,HttpServletResponse response) {
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Download");
			VisionUsersVb visionUsersVb =CustomContextHolder.getContext();

			int currentUserId = visionUsersVb.getVisionId();
			ExceptionCode exceptionCode1 = reportsWb.exportToXls(vObject, currentUserId,"0");
			reportsDao.insertReportsAudit(vObject, exceptionCode1.getErrorMsg(),"XL EXPORT");
			if(exceptionCode1.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				request.setAttribute("fileExtension", "xlsx");
				request.setAttribute("fileName", ""+exceptionCode1.getOtherInfo()+".xlsx");
				request.setAttribute("filePath", exceptionCode1.getResponse());
				visionUploadWb.setExportXlsServlet(request, response);
				if(response.getStatus() == 404) {
					jsonExceptionCode =	new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Report is unable to Export.Contact System Admin!!", null);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
				}else{
					jsonExceptionCode =	new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
				}
			}else{
				jsonExceptionCode = new JSONExceptionCode(exceptionCode1.getErrorCode(),exceptionCode1.getErrorMsg(),exceptionCode1.getOtherInfo());
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
			}
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*------------------------------------ REPORTS PDF EXPORT-------------------------------*/
	@RequestMapping(path = "/reportPdfExport", method = RequestMethod.POST)
	@ApiOperation(value = "Get PDF Report",notes = "Export Report Data to PDF",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> pdfExportReport(@RequestBody ReportsVb vObject,HttpServletRequest request,HttpServletResponse response) {
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Download");
			VisionUsersVb visionUsersVb =CustomContextHolder.getContext();

			int currentUserId = visionUsersVb.getVisionId();
			ExceptionCode exceptionCode1 = reportsWb.exportToPdf(currentUserId,vObject);
			reportsDao.insertReportsAudit(vObject, exceptionCode1.getErrorMsg(),"PDF EXPORT");
			if(exceptionCode1.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				request.setAttribute("fileExtension", "pdf");
				request.setAttribute("fileName", ""+exceptionCode1.getOtherInfo()+".pdf");
				request.setAttribute("filePath", exceptionCode1.getResponse());
				visionUploadWb.setExportXlsServlet(request, response);
				if(response.getStatus() == 404) {
					jsonExceptionCode =	new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Report is unable to Export.Contact System Admin!!", null);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
				}else{
					jsonExceptionCode =	new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
				}
			}else{
				jsonExceptionCode = new JSONExceptionCode(exceptionCode1.getErrorCode(),exceptionCode1.getErrorMsg(),exceptionCode1.getOtherInfo());
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
			}
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	
	
	/*-------------------------------------GET TREE FILTER------------------------------------------*/
	@RequestMapping(path = "/getTreePromptData", method = RequestMethod.POST)
	@ApiOperation(value = "Get Tree Prompt Data",notes = "Get the value for Tree Prompt",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getTreePromptData(@RequestBody ReportFilterVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.getTreePromptData(vObject);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION && exceptionCode.getResponse()!= null) {
				List<PromptTreeVb> promptTreelst = (List) exceptionCode.getResponse();
				exceptionCode.setResponse(promptTreelst);
			}
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	
	/*-------------------------------------GET REPORT LIST BY GROUP------------------------------------------*/
	@RequestMapping(path = "/getWidgetMaster", method = RequestMethod.GET)
	@ApiOperation(value = "Get Widget List",notes = "Get Widget List",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getWidgetMasterList(){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.getWidgetList();
			exceptionCode.setRequest(commonDao.getAllBusinessDate()); 
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo(),exceptionCode.getRequest());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*-------------------------------------SAVE WIDGET------------------------------------------*/
	@RequestMapping(path = "/saveUserWidget", method = RequestMethod.POST)
	@ApiOperation(value = "Save User Widget",notes = "Save User Pref Widget",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> saveUserWidget(@RequestBody ReportsVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.saveWidget(vObject) ;
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = reportsWb.saveReportUserDef(vObject) ;
			}
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*-------------------------------------DELETE WIDGET------------------------------------------*/
	@RequestMapping(path = "/deleteUserWidget", method = RequestMethod.POST)
	@ApiOperation(value = "Delete User Widget",notes = "Delete User saved Widget",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteUserWidget(@RequestBody ReportsVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = reportsWb.deleteSavedWidget(vObject) ;
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = reportsWb.deleteSavedReportUserDef(vObject) ;
			}
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*------------------------------------ WIDGET EXPORT-------------------------------*/
	@RequestMapping(path = "/widgetExport", method = RequestMethod.POST)
	@ApiOperation(value = "Widget Export PDF",notes = "Widget Export Pdf",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> widgetPdfExport(@RequestParam("reportTitle") String reportTitle,@RequestParam("promptLabel") String promptLabel,
			@RequestParam("gridReportIds") String gridReportIds,@RequestParam("fileName") String fileName,@RequestParam("appTheme") String appTheme,
			HttpServletRequest request,HttpServletResponse response,@RequestParam("exportImage") String exportImageBase64) throws JsonMappingException, JsonProcessingException , JSONException {
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			ArrayList<ReportsVb> reportslst = new ArrayList();
			JSONArray array = new JSONArray(gridReportIds);
			for(int i=0; i < array.length(); i++) 
			{  
				JSONObject gridObject = array.getJSONObject(i);
				ReportsVb dObj = new ObjectMapper().readValue(gridObject.toString(), ReportsVb.class);
				reportslst.add(dObj);
			} 
			exceptionCode = reportsWb.exportWidgetToPdf(reportTitle,promptLabel,gridReportIds,fileName,reportslst,appTheme,exportImageBase64);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				request.setAttribute("fileExtension", "pdf");
				request.setAttribute("fileName", ""+exceptionCode.getOtherInfo()+".pdf");
				request.setAttribute("filePath", exceptionCode.getResponse());
				visionUploadWb.setExportXlsServlet(request, response);
				if(response.getStatus() == 404) {
					jsonExceptionCode =	new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Unable to Export.Contact System Admin!!", null);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
				}else{
					jsonExceptionCode =	new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
				}
			}else{
				jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(),exceptionCode.getOtherInfo());
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
			}
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}

	
}
