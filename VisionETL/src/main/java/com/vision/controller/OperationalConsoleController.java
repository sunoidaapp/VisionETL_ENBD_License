package com.vision.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vision.dao.CommonApiDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.JSONExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ConsoleDependencyFeedVb;
import com.vision.vb.OperationalConsoleVb;
import com.vision.wb.OperationalConsoleWb;
import com.vision.wb.VisionUploadWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("optConsole")
@Api(value = "Operational Console", description = "Operational Console")
public class OperationalConsoleController {
	@Autowired
	CommonApiDao commonApiDao;
	@Autowired
	OperationalConsoleWb operationalConsoleWb;
	@Autowired
	VisionUploadWb visionUploadWb;
	
	/*-------------------------------------Etl feed process control------------------------------------------*/
	@RequestMapping(path = "/pageLoadValues", method = RequestMethod.GET)
	@ApiOperation(value = "Page Load Values", notes = "Load AT/NT Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> pageOnLoad() {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			ArrayList arrayList = operationalConsoleWb.getPageLoadValues();
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Page Load Values", arrayList);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Singleton tabs------------------------------------------*/
	@RequestMapping(path = "/singletonTabs", method = RequestMethod.POST)
	@ApiOperation(value = "Singleton tab Values", notes = "Load Singleton tab Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> singletonTabs(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			//ExceptionCode exceptionCode = commonApiDao.getCommonResultDataFetch(vObject);
			ExceptionCode exceptionCode = operationalConsoleWb.getQuerySingletonTabResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Load Notification tab Values", exceptionCode);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Notification tab------------------------------------------*/
	@RequestMapping(path = "/notificationTab", method = RequestMethod.POST)
	@ApiOperation(value = "Notification tab Values", notes = "Load notification tab Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> notificationTab(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			ExceptionCode exceptionCode = operationalConsoleWb.getQueryFeedNotificationDetailResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Load Notification tab Values", exceptionCode);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------------------------------Dependency error tab------------------------------------------*/
	@RequestMapping(path = "/depError", method = RequestMethod.POST)
	@ApiOperation(value = "Dependency error list", notes ="Dependency error hierachy", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> depError(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			ExceptionCode exceptionCode = operationalConsoleWb.getQueryFeedDepErrorResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Dependency Error Hierachy List", exceptionCode);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------- ----- Operation console Grid 1  ------------------*/
	@RequestMapping(path = "/getOperationalConsoleGrid1", method = RequestMethod.POST)
	@ApiOperation(value = "Get All the details for category", notes = "Fetch all the existing records from the table",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getOperationalConsoleGrid1Data(@RequestBody OperationalConsoleVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = operationalConsoleWb.getAllQueryPopupResult(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results", exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	
	/*-------------------------------------Delete------------------------------------------*/
	@RequestMapping(path = "/deleteCategory", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Category", notes = "Delete Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteCategory(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			List<OperationalConsoleVb> processCtrlList = vObject.getChilds();
			exceptionCode.setOtherInfo(vObject);
			ExceptionCode exceptionCode1 = operationalConsoleWb.deleteRecord(exceptionCode, processCtrlList);
			exceptionCode1.setResponse(processCtrlList);
			exceptionCode1.setOtherInfo(vObject);
			if(exceptionCode1.getErrorCode() ==Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode1.getErrorMsg(), exceptionCode1.getOtherInfo());
			}else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode1.getErrorMsg(),exceptionCode1.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------------------------------REINITIATE------------------------------------------*/
	/*@RequestMapping(path = "/reinitiateCategory", method = RequestMethod.POST)
	@ApiOperation(value = "Reinitiate Category", notes = "Reinitiate Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reinitiateCategory(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("ReInitiate");
			List<OperationalConsoleVb> processCtrlList = vObject.getChilds();
			exceptionCode.setOtherInfo(vObject);
			ExceptionCode exceptionCode1 = operationalConsoleWb.reInitiate(exceptionCode, processCtrlList);
			exceptionCode1.setResponse(processCtrlList);
			exceptionCode1.setOtherInfo(vObject);
			if(exceptionCode1.getErrorCode() ==Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode1.getErrorMsg(), exceptionCode1.getOtherInfo());
			}else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode1.getErrorMsg(),exceptionCode1.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}*/
	
	/*-------------------------------------REINITIATE------------------------------------------*/
	@RequestMapping(path = "/reinitiateCategory", method = RequestMethod.POST)
	@ApiOperation(value = "Reinitiate Category", notes = "Reinitiate Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reinitiateCategory(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
//			OperationalConsoleVb categoryVb = vObject.getChilds().get(0);
			exceptionCode = operationalConsoleWb.reInitiateCategory(vObject.getChilds());
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode.getErrorMsg(), vObject);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), vObject);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Terminate------------------------------------------*/
	@RequestMapping(path = "/terminateCategory", method = RequestMethod.POST)
	@ApiOperation(value = "Terminate Category", notes = "Terminate Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> terminateCategory(@RequestBody OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setGrid("1");
			exceptionCode = operationalConsoleWb.terminate(vObject.getChilds());
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "ETL Feed Process Control - Terminate - Successful", vObject);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), vObject);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------------------------------ADD process control ------------------------------------------*/
	@RequestMapping(path = "/addProcessControlData", method = RequestMethod.POST)
	@ApiOperation(value = "Add Process Control Data", notes = "Add Process Control Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> add(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			exceptionCode = operationalConsoleWb.insertRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------------------------------Download------------------------------------------*/
	@RequestMapping(path = "/downloadCategoryLog", method = RequestMethod.POST)
	@ApiOperation(value = "Download Category log", notes = "Download Category log", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> downloadCategoryLog(@RequestBody OperationalConsoleVb vObject,
			HttpServletRequest request, HttpServletResponse response)  throws IOException  {
		ExceptionCode exceptionCode = new ExceptionCode();
		JSONExceptionCode jsonExceptionCode = null;
		try {
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = operationalConsoleWb.fileDownload(vObject.getChilds().get(0));
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				String logFileName = (String)exceptionCode.getRequest();
				if (!logFileName.contains(",")) {
					String fileExt[] = logFileName.split("\\.");
					request.setAttribute("fileExtension", fileExt[1]);
					request.setAttribute("fileName", fileExt[0]);
				} else {
					request.setAttribute("fileExtension", "zip");
					request.setAttribute("fileName", "logs");
				}
				request.setAttribute("filePath", exceptionCode.getResponse());
				visionUploadWb.setExportXlsServlet(request, response);
				if (response.getStatus() == 404) {
					jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "File not found", null);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
				} else {
					jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
				}
			} else {
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
			}
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	/*-----------------Operational console grid 2 -----------------------------------*/
	@RequestMapping(path = "/getOperationalConsoleGrid2", method = RequestMethod.POST)
	@ApiOperation(value = "Get All the details for feed based on category", notes = "Fetch all the existing records from the table",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getOperationalConsoleGrid2Data(@RequestBody OperationalConsoleVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = operationalConsoleWb.getOperationalConsoleGrid2Data(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), "Query Results", exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	@RequestMapping(path = "/reinitiateFeed", method = RequestMethod.POST)
	@ApiOperation(value = "Reinitiate Feed", notes = "Reinitiate Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reinitiateFeed(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			vObject.setGrid("2");
			List<OperationalConsoleVb> childList = vObject.getChilds();
			for(OperationalConsoleVb vb: childList) {
				vb.setDependencyFlag(vObject.getDependencyFlag());
			}
			exceptionCode = operationalConsoleWb.reInitiateFeed(childList);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode.getErrorMsg(), vObject);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), vObject);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/deleteFeed", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Feed", notes = "Delete Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteFeed(@RequestBody OperationalConsoleVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			vObject.setGrid("2");
			List<OperationalConsoleVb> processCtrlList = vObject.getChilds();
			exceptionCode.setOtherInfo(vObject);
			ExceptionCode exceptionCode1 = operationalConsoleWb.deleteRecord(exceptionCode, processCtrlList);
			exceptionCode1.setResponse(processCtrlList);
			exceptionCode1.setOtherInfo(vObject);
			if(exceptionCode1.getErrorCode() ==Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode1.getErrorMsg(), exceptionCode1.getOtherInfo());
			}else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode1.getErrorMsg(),exceptionCode1.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------------------------------Download------------------------------------------*/
	@RequestMapping(path = "/downloadFeedLog", method = RequestMethod.POST)
	@ApiOperation(value = "Download Feed log", notes = "Download Feed log", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> downloadFeedLog(@RequestBody OperationalConsoleVb vObject,
			HttpServletRequest request, HttpServletResponse response)  throws IOException  {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setGrid("2");
			vObject.setActionType("Download");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = operationalConsoleWb.fileDownload(vObject.getChilds().get(0));
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				String logFileName = (String)exceptionCode.getRequest();
				if (!logFileName.contains(",")) {
					String fileExt[] = logFileName.split("\\.");
					request.setAttribute("fileExtension", fileExt[1]);
					request.setAttribute("fileName", fileExt[0]);
				} else {
					request.setAttribute("fileExtension", "zip");
					request.setAttribute("fileName", "logs");
				}
				request.setAttribute("filePath", exceptionCode.getResponse());
				visionUploadWb.setExportXlsServlet(request, response);
				if (response.getStatus() == 404) {
					jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "File not found", null);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
				} else {
					jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
					return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
				}
			} else {
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
			}
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	/*-------------------------------------Terminate------------------------------------------*/
	/*@RequestMapping(path = "/terminateFeed", method = RequestMethod.POST)
	@ApiOperation(value = "Terminate Feed", notes = "Terminate Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> terminateFeed(@RequestBody OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setGrid("2");
			exceptionCode = operationalConsoleWb.terminate(vObject);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"ETL Feed Process Control - Terminate - Successful", vObject);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						vObject);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}*/
	@RequestMapping(path = "/getFeeds", method = RequestMethod.POST)
	@ApiOperation(value = "Listing Feeds based on category", notes = "Returns list of feeds based on category except recieved feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getFeeds(@RequestBody OperationalConsoleVb vObj) {
		JSONExceptionCode jsonExceptionCode = null;
		List<OperationalConsoleVb> dependencylst = null;
		try {
			if(ValidationUtil.isValid(vObj.getFeedCategory())) {
				dependencylst = operationalConsoleWb.findFeed(vObj);
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "",dependencylst);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Not Valid",dependencylst);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK); 
		}
	}
	
	@RequestMapping(path = "/getFullDependencyHierarchy", method = RequestMethod.POST)
	@ApiOperation(value = "Get Full Dependency Hierachy", notes = "Get Full Dependency Hierachy", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getFullDependencyHierarchy(@RequestBody ConsoleDependencyFeedVb vObject) {//String feedID, String sessionID
		JSONExceptionCode jsonExceptionCode = null;
		try {
			ExceptionCode exceptionCode = operationalConsoleWb.getFullDependencyHierarchy(vObject);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			}else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/getImmediateParentDependency", method = RequestMethod.POST)
	@ApiOperation(value = "Get Immediate Parent Dependency", notes = "Get Immediate Parent Dependency", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDependencyDetails(@RequestBody ConsoleDependencyFeedVb vObject) {//String feedID, String sessionID
		JSONExceptionCode jsonExceptionCode = null;
		try {
			ExceptionCode exceptionCode = operationalConsoleWb.getImmediateParentDependency(vObject);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION){
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/getAllChildDataForReinitiate", method = RequestMethod.POST)
	@ApiOperation(value = "Get All the details for feed based on category", notes = "Fetch all the existing records from the table",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllChildDataForReinitiate(@RequestBody OperationalConsoleVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = operationalConsoleWb.getAllChildDataForReinitiate(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), "Query Results",
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/getProcessDate", method = RequestMethod.POST)
	@ApiOperation(value = "Get Vision Business day for Adhoc", notes = "Get Vision Business day for Adhoc",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getVisionBusinessDay(@RequestBody OperationalConsoleVb vObject){
		JSONExceptionCode jsonExceptionCode = null;
		try {
			ArrayList arrayList = operationalConsoleWb.getVisionBusinessDay(vObject);
			if(arrayList != null && arrayList.size() > 0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Get Vision Business Day", arrayList);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Get Vision Business Day - Error", arrayList, vObject);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
}