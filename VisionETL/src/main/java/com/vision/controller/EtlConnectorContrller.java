package com.vision.controller;

/*@RestController
@RequestMapping("etlConnector1")
@Api(value = "etlConnector", description = "Etl Connector")*/
public class EtlConnectorContrller{/*
	@Autowired
	EtlConnectorWb etlConnectorWb;
	-------------------------------------Etl Connector SCREEN PAGE LOAD------------------------------------------
	@RequestMapping(path = "/pageLoadValues", method = RequestMethod.GET)
	@ApiOperation(value = "Page Load Values - Load AT/NT Values on screen load", notes = "Load AT/NT Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> pageOnLoad() {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			MenuVb menuVb = new MenuVb();
			menuVb.setActionType("Clear");
			ArrayList arrayList = etlConnectorWb.getPageLoadValues();
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Page Load Values", arrayList);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	------------------------------------Etl Connector - FETCH HEADER RECORDS-------------------------------
	@RequestMapping(path = "/getAllQueryResults", method = RequestMethod.POST)
	@ApiOperation(value = "Fetch all the existing records from the table",notes = "Fetch all the existing records from the table",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllQueryResults(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlConnectorWb.getAllQueryPopupResult(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results", exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	@RequestMapping(path = "/getQueryDetails", method = RequestMethod.POST)
	@ApiOperation(value = "Get query results for a single Record",notes = "Get query results for a single Record",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> queryDetails(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			vObject.setActionType("Query");
			ExceptionCode  exceptionCode= etlConnectorWb.getQueryResultsSingle(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}	
	}
	-------------------------------------ADD Etl Connector------------------------------------------
	@RequestMapping(path = "/addEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Add Etl Connector", notes = "Add Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> add(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			exceptionCode = etlConnectorWb.insertRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	-------------------------------------MODIFY Etl Connector------------------------------------------
	@RequestMapping(path = "/modifyEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Modify Etl Connector", notes = "Modify Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> modify(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Modify");
			exceptionCode = etlConnectorWb.modifyRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	-------------------------------------DELETE Etl Connector------------------------------------------
	@RequestMapping(path = "/deleteEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Etl Connector", notes = "Delete existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> delete(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.deleteRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	-------------------------------------Reject Etl Connector------------------------------------------
	@RequestMapping(path = "/rejectEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Reject Etl Connector", notes = "Reject existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reject(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.reject(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/approveEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Connector", notes = "Approve existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> approve(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.approve(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/bulkApproveEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Connector", notes = "Approve existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkApprove(@RequestBody List<EtlConnectorVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb vObject = new EtlConnectorVb();
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.bulkApprove(vObjects, vObject);
			String errorMessage= exceptionCode.getErrorMsg().replaceAll("- Approve -", "- Bulk Approve -");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	@RequestMapping(path = "/bulkRejectEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Reject Etl Connector", notes = "Reject existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkReject(@RequestBody List<EtlConnectorVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb vObject = new EtlConnectorVb();
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.bulkReject(vObjects, vObject);
			String errorMessage= exceptionCode.getErrorMsg().replaceAll("- Reject -", "- Bulk Reject -");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}	
	
	@RequestMapping(path = "/reviewEtlConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Review Etl Connector", notes = "Review Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> review(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlConnectorWb.reviewRecordNew(vObject);
			String errorMesss = exceptionCode.getErrorMsg().replaceAll("Query ", "Review");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMesss,
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}	

*/}
