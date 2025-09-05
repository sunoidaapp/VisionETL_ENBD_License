package com.vision.controller;

import java.util.ArrayList;
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
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedAlertMatrixVb;
import com.vision.vb.EtlFeedCategoryVb;
import com.vision.vb.MenuVb;
import com.vision.wb.EtlFeedCategoryWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("etlFeedCategory")
@Api(value = "etlFeedCategory", description = "Etl Feed Category")
public class EtlFeedCategoryController {
	@Autowired
	EtlFeedCategoryWb etlFeedCategoryWb;

	/*-------------------------------------Etl Feed Category SCREEN PAGE LOAD------------------------------------------*/
	@RequestMapping(path = "/pageLoadValues", method = RequestMethod.GET)
	@ApiOperation(value = "Page Load Values", notes = "Load AT/NT Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> pageOnLoad() {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			MenuVb menuVb = new MenuVb();
			menuVb.setActionType("Clear");
			ArrayList arrayList = etlFeedCategoryWb.getPageLoadValues();
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Page Load Values", arrayList);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*------------------------------------Etl Feed Category - FETCH HEADER RECORDS-------------------------------*/
	@RequestMapping(path = "/getAllQueryResults", method = RequestMethod.POST)
	@ApiOperation(value = "Get All Feed Category", notes = "Fetch all the existing records from the table", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllQueryResults(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedCategoryWb.getAllQueryPopupResult(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results",
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryDetails", method = RequestMethod.POST)
	@ApiOperation(value = "Get All Feed Category", notes = "Feed Category Details", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> queryDetails(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedCategoryWb.getQueryResultsSingle(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------ADD Etl Feed Category------------------------------------------*/
	@RequestMapping(path = "/add", method = RequestMethod.POST)
	@ApiOperation(value = "Add Etl Feed Category", notes = "Add Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> add(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			exceptionCode = etlFeedCategoryWb.insertRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------MODIFY Etl Feed Category------------------------------------------*/
	@RequestMapping(path = "/modify", method = RequestMethod.POST)
	@ApiOperation(value = "Modify Etl Feed Category", notes = "Modify Etl Feed Category Values", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> modify(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Modify");
			exceptionCode = etlFeedCategoryWb.modifyRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------DELETE Etl Feed Category------------------------------------------*/
	@RequestMapping(path = "/delete", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Etl Feed Category", notes = "Delete existing Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> delete(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedCategoryWb.deleteRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Reject Etl Feed Category------------------------------------------*/
	@RequestMapping(path = "/reject", method = RequestMethod.POST)
	@ApiOperation(value = "Reject Etl Feed Category", notes = "Reject existing Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reject(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedCategoryWb.reject(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/approve", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Feed Category", notes = "Approve existing Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> approve(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedCategoryWb.approve(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/bulkApprove", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Feed Category", notes = "Approve existing Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkApprove(@RequestBody List<EtlFeedCategoryVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
			vObject.setActionType("Approve"); 
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedCategoryWb.bulkApprove(vObjects, vObject);
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
	@ApiOperation(value = "Reject Etl Feed Category", notes = "Reject existing Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkReject(@RequestBody List<EtlFeedCategoryVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedCategoryWb.bulkReject(vObjects, vObject);
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
	@ApiOperation(value = "Review Etl Feed Category", notes = "Review Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> review(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedCategoryWb.reviewRecordNew(vObject);
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
	@ApiOperation(value = "Delete Etl Feed Category", notes = "Delete Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkDelete(@RequestBody List<EtlFeedCategoryVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedCategoryWb.deleteRecord(exceptionCode, vObjects);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("- DeleteObjectsove -", "- Bulk Delete -");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	@RequestMapping(path = "/getDependencyCategory", method = RequestMethod.POST)
	@ApiOperation(value = "Listing category except self category", notes = "Returns list of category except", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDependencyCategory(@RequestBody EtlFeedCategoryVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		List<EtlFeedCategoryVb> dependencylst = null;
		try {
			dependencylst = etlFeedCategoryWb.findDependencyCategory(vObject);
			if(ValidationUtil.isValid(dependencylst) && dependencylst.size()>0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "",dependencylst);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No Records Found",dependencylst);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK); 
		}
	}
	
	@RequestMapping(path = "/getAlertMatrix", method = RequestMethod.POST)
	@ApiOperation(value = "Listing Matrix based on country lebook", notes = "Returns list of alert Matrix based on country lebook", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAlertMatrix(@RequestBody EtlFeedAlertMatrixVb vObj) {
		JSONExceptionCode jsonExceptionCode = null;
		List<EtlFeedAlertMatrixVb> matrixlst = null;
		try {
			matrixlst = etlFeedCategoryWb.findAlertMatrix(vObj);
			if(ValidationUtil.isValid(matrixlst) && matrixlst.size()>0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "",matrixlst);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No Records Found",matrixlst);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK); 
		}
	}
}
