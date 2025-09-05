package com.vision.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import com.vision.vb.EtlFeedGenericAlertVb;
import com.vision.vb.MenuVb;
import com.vision.wb.EtlFeedGenericAlertWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("etlFeedGenericAlert")
@Api(value = "etlFeedGenericAlert", description = "Etl Feed Generic Alert")
public class EtlFeedGenericAlertController {
	@Autowired
	EtlFeedGenericAlertWb etlFeedGenericAlertWb;

	/*-------------------------------------Etl Feed Generic Alert SCREEN PAGE LOAD------------------------------------------*/
	@RequestMapping(path = "/pageLoadValues", method = RequestMethod.GET)
	@ApiOperation(value = "Page Load Values", notes = "Load AT/NT Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> pageOnLoad() {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			MenuVb menuVb = new MenuVb();
			menuVb.setActionType("Clear");
			ArrayList arrayList = etlFeedGenericAlertWb.getPageLoadValues();
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Page Load Values", arrayList);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*------------------------------------Etl Feed Generic Alert - FETCH HEADER RECORDS-------------------------------*/
	@RequestMapping(path = "/getAllQueryResults", method = RequestMethod.POST)
	@ApiOperation(value = "Get All Feed Generic Alert", notes = "Fetch all the existing records from the table", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllQueryResults(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedGenericAlertWb.getAllQueryPopupResult(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results",
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryDetails", method = RequestMethod.POST)
	@ApiOperation(value = "Get All Feed Generic Alert", notes = "Feed Generic Alert Details", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> queryDetails(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedGenericAlertWb.getQueryResultsSingle(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------ADD Etl Feed Generic Alert------------------------------------------*/
	@RequestMapping(path = "/add", method = RequestMethod.POST)
	@ApiOperation(value = "Add Etl Feed Generic Alert", notes = "Add Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> add(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			exceptionCode = etlFeedGenericAlertWb.insertRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------MODIFY Etl Feed Generic Alert------------------------------------------*/
	@RequestMapping(path = "/modify", method = RequestMethod.POST)
	@ApiOperation(value = "Modify Etl Feed Generic Alert", notes = "Modify Etl Feed Generic Alert Values", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> modify(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Modify");
			exceptionCode = etlFeedGenericAlertWb.modifyRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------DELETE Etl Feed Generic Alert------------------------------------------*/
	@RequestMapping(path = "/delete", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Etl Feed Generic Alert", notes = "Delete existing Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> delete(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedGenericAlertWb.deleteRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Reject Etl Feed Generic Alert------------------------------------------*/
	@RequestMapping(path = "/reject", method = RequestMethod.POST)
	@ApiOperation(value = "Reject Etl Feed Generic Alert", notes = "Reject existing Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reject(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedGenericAlertWb.reject(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/approve", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Feed Generic Alert", notes = "Approve existing Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> approve(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedGenericAlertWb.approve(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/bulkApprove", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Feed Generic Alert", notes = "Approve existing Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkApprove(@RequestBody List<EtlFeedGenericAlertVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedGenericAlertVb vObject = new EtlFeedGenericAlertVb();
			vObject.setActionType("Approve"); 
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedGenericAlertWb.bulkApprove(vObjects, vObject);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("Approve", "Bulk Approve");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/bulkReject", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Feed Generic Alert", notes = "Approve existing Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkReject(@RequestBody List<EtlFeedGenericAlertVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedGenericAlertVb vObject = new EtlFeedGenericAlertVb();
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedGenericAlertWb.bulkReject(vObjects, vObject);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("Reject", "Bulk Reject");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/review", method = RequestMethod.POST)
	@ApiOperation(value = "Get Headers", notes = "Fetch all the existing records from the table", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> review(@RequestBody EtlFeedGenericAlertVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedGenericAlertWb.reviewRecordNew(vObject);
			String errorMesss = exceptionCode.getErrorMsg().replaceAll("Query ", "Review");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMesss,
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	@RequestMapping(path = "/bulkDelete", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Etl Feed Generic Alert", notes = "Delete Etl Feed Generic Alert", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkDelete(@RequestBody List<EtlFeedGenericAlertVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedGenericAlertVb vObject = new EtlFeedGenericAlertVb();
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedGenericAlertWb.deleteRecord(exceptionCode, vObjects);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("- DeleteObjectsove -", "- Bulk Delete -");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

}
