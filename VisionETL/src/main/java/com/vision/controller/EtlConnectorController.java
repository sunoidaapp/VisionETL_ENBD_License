package com.vision.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.QueryParam;

import org.apache.commons.io.FilenameUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.vision.download.ExportXlsServlet;
import com.vision.exception.ExceptionCode;
import com.vision.exception.JSONExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlConnectorVb;
import com.vision.vb.EtlFileColumnsVb;
import com.vision.vb.EtlFileUploadAreaVb;
import com.vision.vb.MenuVb;
import com.vision.wb.EtlConnectorWb;
import com.vision.wb.VisionUploadWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("etlConnector")
@Api(value = "etlConnector", description = "Etl Connector")
public class EtlConnectorController {
	@Autowired
	EtlConnectorWb etlConnectorWb;

	@Autowired
	VisionUploadWb visionUploadWb;

	@Autowired
	ExportXlsServlet exportXlsServlet;

	/*-------------------------------------Etl Connector SCREEN PAGE LOAD------------------------------------------*/
	@RequestMapping(path = "/pageLoadValues", method = RequestMethod.GET)
	@ApiOperation(value = "Page Load Values", notes = "Load AT/NT Values on screen load", response = ResponseEntity.class)
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

	/*------------------------------------Etl Connector - FETCH HEADER RECORDS-------------------------------*/
	@RequestMapping(path = "/getAllQueryResults", method = RequestMethod.POST)
	@ApiOperation(value = "Get All profile Data", notes = "Fetch all the existing records from the table", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllQueryResults(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlConnectorWb.getAllQueryPopupResult(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results",
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryDetails", method = RequestMethod.POST)
	@ApiOperation(value = "Get All ADF Schules", notes = "ADF Schules Details", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> queryDetails(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlConnectorWb.getQueryResultsSingle(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------ADD Etl Connector------------------------------------------*/
	@RequestMapping(path = "/add", method = RequestMethod.POST)
	@ApiOperation(value = "Add Etl Connector", notes = "Add Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> add(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			String macroVarScript = etlConnectorWb.dynamicScriptCreation(vObject);
			vObject.setConnectorScripts(macroVarScript);
			exceptionCode = etlConnectorWb.insertRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------MODIFY Etl Connector------------------------------------------*/
	@RequestMapping(path = "/modify", method = RequestMethod.POST)
	@ApiOperation(value = "Modify Etl Connector", notes = "Modify Etl Connector Values", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> modify(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Modify");
			String macroVarScript = etlConnectorWb.dynamicScriptCreation(vObject);
			vObject.setConnectorScripts(macroVarScript);
			exceptionCode = etlConnectorWb.modifyRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------DELETE Etl Connector------------------------------------------*/
	@RequestMapping(path = "/delete", method = RequestMethod.POST)
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

	/*-------------------------------------Reject Etl Connector------------------------------------------*/
	@RequestMapping(path = "/reject", method = RequestMethod.POST)
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

	@RequestMapping(path = "/approve", method = RequestMethod.POST)
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

	@RequestMapping(path = "/bulkApprove", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Connector", notes = "Approve existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkApprove(@RequestBody List<EtlConnectorVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb vObject = new EtlConnectorVb();
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.bulkApprove(vObjects, vObject);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("Approve", "Bulk Approve");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/bulkReject", method = RequestMethod.POST)
	@ApiOperation(value = "Approve Etl Connector", notes = "Approve existing Etl Connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkReject(@RequestBody List<EtlConnectorVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb vObject = new EtlConnectorVb();
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.bulkReject(vObjects, vObject);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("Reject", "Bulk Reject");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/review", method = RequestMethod.POST)
	@ApiOperation(value = "Get Headers", notes = "Fetch all the existing records from the table", response = ResponseEntity.class)
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

	@RequestMapping(path = "/getTagListDbCon", method = RequestMethod.POST)
	@ApiOperation(value = "Get Tag list", notes = "Returns a tag list from Macrovar_tagging", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getTagListDbCon(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			List<EtlConnectorVb> dbScriptPopList = etlConnectorWb.getDisplayTagList(vObject.getMacroVarType());
			if (dbScriptPopList.size() > 0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"Display specific macro tag list", dbScriptPopList, vObject);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No Results Found", null);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/test", method = RequestMethod.POST)
	@ApiOperation(value = "Test Data Connector", notes = "Test a data connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> testConnector(@RequestBody EtlConnectorVb vObject) throws ParseException {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = null;
		try {
			String macroVarScript = etlConnectorWb.dynamicScriptCreation(vObject);
			if("SSH".equalsIgnoreCase(vObject.getConnectorType())) {
				 exceptionCode = CommonUtils.getSSHConnection(macroVarScript);
			}else {
				 exceptionCode = CommonUtils.getConnection(macroVarScript);
			}
			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());
			} else {
				exceptionCode = etlConnectorWb.validateDBLink(exceptionCode, macroVarScript);
				if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
							exceptionCode.getOtherInfo());
				} else {
					jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),
							"ETL Connector - Test Connection - Successful", exceptionCode.getOtherInfo());
				}
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} 
	}

	@RequestMapping(path = "/bulkDelete", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Etl Connector", notes = "Delete Etl Feed Category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkDelete(@RequestBody List<EtlConnectorVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb vObject = new EtlConnectorVb();
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlConnectorWb.deleteRecord(exceptionCode, vObjects);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("DeleteObjectsove", "Bulk Delete");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/uploadStaticFile", method = RequestMethod.POST)
	@ApiOperation(value = "Uploading Static files to Server", notes = "Upload Static files to  Server", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> uploadStaticFile(@RequestParam("file") MultipartFile[] files , @RequestParam("connectorId") String connectorId, @RequestParam("fileId") String fileId) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb etlConnectorVb = new EtlConnectorVb();
			etlConnectorVb.setActionType("Upload");
			//exceptionCode = etlConnectorWb.doValidate(etlConnectorVb);
			/*if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = visionUploadWb.uploadStaticFileForETLConnector(files, connectorId, fileId);
			} else {
				return new ResponseEntity<JSONExceptionCode>(
						new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(), null),
						HttpStatus.OK);
			}*/
			exceptionCode = visionUploadWb.uploadStaticFileForETLConnector(files, connectorId, fileId);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "File Upload Success",exceptionCode.getResponse(),
						exceptionCode.getOtherInfo());
			} else {
				jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
						exceptionCode.getResponse());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}
	@RequestMapping(path = "/columnMetaData", method = RequestMethod.POST)
	@ApiOperation(value = "ColumnMetaData of uploaded file", notes = "ColumnMetaData of uploaded file", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> columnMetaData(@RequestParam("connectorId") String connectorId, @RequestParam("fileId") String fileId, @RequestParam("sheetName") String sheetName, @RequestParam("fileName") String fileName, @RequestParam("viewName") String viewName,
			@RequestParam String fileColumns,@RequestParam String dynamicScript,@RequestParam String context) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		List<EtlFileColumnsVb> columnsList =new ArrayList<EtlFileColumnsVb>();
		try {
			if(!"E".equalsIgnoreCase(fileColumns)) {
				JSONArray array = new JSONArray(fileColumns);  
				for(int i=0; i < array.length(); i++){  
					EtlFileColumnsVb vb = new EtlFileColumnsVb();
					JSONObject object = array.getJSONObject(i);  
					vb.setColumnId(object.getInt("columnId")); 
					vb.setColumnName(object.getString("columnName"));  
					vb.setColumnDatatype(object.getString("columnDataType"));
					columnsList.add(vb);
				}  
			}  

			exceptionCode = visionUploadWb.columnMetaDataOfUploadedFile(connectorId, fileId,sheetName,fileName,viewName, columnsList,dynamicScript,context);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "File Upload Success",exceptionCode.getResponse(),
						exceptionCode.getOtherInfo());
			} else {
				jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
						exceptionCode.getResponse());

			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}
	@RequestMapping(path = "/uploadDynamicFile", method = RequestMethod.POST)
	@ApiOperation(value = "Uploading Dynamic files to Server", notes = "Upload Dynamic files to  Server", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> uploadDynamicFile(@RequestBody EtlFileUploadAreaVb vbObj) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlConnectorVb etlConnectorVb = new EtlConnectorVb();
			etlConnectorVb.setActionType("Upload");
			//exceptionCode = etlConnectorWb.doValidate(etlConnectorVb);
			/*if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = visionUploadWb.uploadDynamicFileForETLConnector(vbObj);
			} else {
				return new ResponseEntity<JSONExceptionCode>(
						new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(), null),
						HttpStatus.OK);
			}*/
			exceptionCode = visionUploadWb.uploadDynamicFileForETLConnector(vbObj);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "File Upload Success",exceptionCode.getResponse(),
						exceptionCode.getOtherInfo());
			} else {
				if (!ValidationUtil.isValid(exceptionCode.getErrorMsg()))
					exceptionCode.setErrorMsg("Failed to upload File");
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());

			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}
	@RequestMapping(path = "/getSheetName", method = RequestMethod.POST)
	@ApiOperation(value = "Get Sheet Names for excel upload", notes = "Get Sheet Names for excel upload", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getSheetName(@RequestBody EtlFileUploadAreaVb vbObj)
			throws ServletException, IOException {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = visionUploadWb.getSheetNames(vbObj);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Get Sheet Name Success", exceptionCode.getResponse(),
						exceptionCode.getOtherInfo());
			} else {
				if (!ValidationUtil.isValid(exceptionCode.getErrorMsg()))
					exceptionCode.setErrorMsg("Failed to upload File");
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());

			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch (RuntimeCustomException rex) {
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}
	
	@RequestMapping(path = "/downloadFilesFromFtp", method = RequestMethod.GET)
	@ApiOperation(value = "Download files from server", notes = "Download files from server", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> downloadFilesFromFtp(@QueryParam("fileName") String fileName,
			@QueryParam("fileExtension") String fileExtension, HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			ExceptionCode exceptionCode = etlConnectorWb.fileDownload(fileName, fileExtension);
			if (exceptionCode.getErrorCode() == 1) {
				request.setAttribute("fileName", fileName);
				request.setAttribute("fileExtension", fileExtension);
				request.setAttribute("filePath", exceptionCode.getResponse());
				exportXlsServlet.doPost(request, response);
				if (response.getStatus() == 404) {
					return new ResponseEntity<JSONExceptionCode>(
							new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "File not found", null),
							HttpStatus.EXPECTATION_FAILED);
				}
				return new ResponseEntity<JSONExceptionCode>(
						new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", exceptionCode.getResponse()),
						HttpStatus.OK);
			} else {
				return new ResponseEntity<JSONExceptionCode>(
						new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Failure", null),
						HttpStatus.EXPECTATION_FAILED);
			}
		} catch (RuntimeCustomException rex) {
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}

	@RequestMapping(path = "/getConnectorScript", method = RequestMethod.POST)
	@ApiOperation(value = "Get Connector Scripts", notes = "Get Connector Scripts", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getSpecificConnectorScript(@RequestBody EtlConnectorVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			List<EtlConnectorVb> queryList = etlConnectorWb.getSpecificConnectorByHashList(vObject);
			if (queryList.size() == 0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No data found", vObject);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "List Connector Script",
						queryList, vObject);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/validateFileExtension", method = RequestMethod.POST)
	@ApiOperation(value = "Validate File Extension", notes = "Validate File Extension", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> validateFileExtension(@QueryParam("fileName") String fileName,
			@QueryParam("connectorDir") String connectorDir, HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			if (ValidationUtil.isValid(fileName) && ValidationUtil.isValid(connectorDir)) {
				String fileExtension = FilenameUtils.getExtension(fileName).toLowerCase();
				if (etlConnectorWb.validateFileExtension(fileExtension, fileName, connectorDir)) {
					jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
				} else {
					jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Unable to read file",
							null);
				}
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.FILE_UPLOAD_REMOTE_ERROR, "Parameters are missing",
						null);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}
	
	@RequestMapping(path = "/deleteFileFromServer", method = RequestMethod.POST)
	@ApiOperation(value = "Delete Files From Server", notes = "Delete the Files which are not saved From Server", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteFileFromServer(@RequestBody String[] fileNames) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = visionUploadWb.deleteFilesFromServer(fileNames);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());
			} else {
				if (!ValidationUtil.isValid(exceptionCode.getErrorMsg()))
					exceptionCode.setErrorMsg("Failed to Delete File");
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			return new ResponseEntity<JSONExceptionCode>(
					new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null),
					HttpStatus.EXPECTATION_FAILED);
		}
	}

}
