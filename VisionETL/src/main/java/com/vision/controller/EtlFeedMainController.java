
package com.vision.controller;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.xml.sax.SAXException;

import com.vision.exception.ExceptionCode;
import com.vision.exception.JSONExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ETLForQueryReportFieldsWrapperVb;
import com.vision.vb.EtlConnectorVb;
import com.vision.vb.EtlExtractionSummaryFieldsVb;
import com.vision.vb.ETLFeedColumnsVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.ETLFeedTablesVb;
import com.vision.vb.EtlForQueryReportVb;
import com.vision.vb.MenuVb;
import com.vision.wb.EtlFeedMainWb;
import com.vision.wb.VisionUploadWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("etlFeedMain")
@Api(value = "Etl Feed Main", description = "Etl Feed Main")
public class EtlFeedMainController {
	
	@Autowired
	EtlFeedMainWb etlFeedMainWb;

	@Autowired
	VisionUploadWb visionUploadWb;

	/*-------------------------------------Etl Feed Main SCREEN PAGE LOAD------------------------------------------*/
	@RequestMapping(path = "/pageLoadValues", method = RequestMethod.GET)
	@ApiOperation(value = "Load Required Values on screen load", notes = "Load Required Values on screen load", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> pageOnLoad() {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			MenuVb menuVb = new MenuVb();
			menuVb.setActionType("Clear");
			ArrayList arrayList = etlFeedMainWb.getPageLoadValues();
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Page Load Values", arrayList);
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*------------------------------------Etl Feed Main - FETCH HEADER RECORDS-------------------------------*/
	@RequestMapping(path = "/getAllQueryResults", method = RequestMethod.POST)
	@ApiOperation(value = "Fetch the existing records", notes = "Fetch the existing records", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllQueryResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getAllQueryPopupResult(vObject);
			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query Results",
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getEtlFeedData", method = RequestMethod.POST)
	@ApiOperation(value = "Get Feed Details", notes = "Get Feed Details", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQueryFeedResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQueryFeedDetailResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getEtlSourceData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Source Data", notes = "Get ETL Source Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQuerySourceResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQuerySourceResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getEtlSourceTableData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Source Table Data", notes = "Get ETL Source Table Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQuerySourceTableResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQuerySourceTableResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getEtlSourceTableColData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Source Table Column Data", notes = "Get ETL Source Table Column Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQuerySourceTableColResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQuerySourceTableColResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getEtlRelationData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Source Relation Data", notes = "Get ETL Source Relation Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQuerySourceRelationResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQuerySourceRelationResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryMaterializedData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Materilized Tab Data", notes = "Get ETL Materilized Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQueryMaterializedResults(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQueryMaterializedViewSetupResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryTransformData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Transformation Tab Data", notes = "Get ETL Transformation Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQueryTransformData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQueryTransformSetupResults(vObject);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Query",
						exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						null, exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryTargetLoadData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Loader Tab Data", notes = "Get ETL Loader Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQueryTargetLoadData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQueryTargetLoadResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getQueryScheduleData", method = RequestMethod.POST)
	@ApiOperation(value = "Get ETL Schedule Tab Data", notes = "Get ETL Schedule Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQueryScheduleLoadData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQueryScheduleResults(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------CONNECTOR LIST SERVICE-------------------------------------------*/
	@RequestMapping(path = "/getAllValidConnectors", method = RequestMethod.POST)
	@ApiOperation(value = "Get All Valid Connectors", notes = "Get All Valid Connectors", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getAllValidConnectors(@RequestBody EtlConnectorVb vObj) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObj.setActionType("Query");
			List list = etlFeedMainWb.getEtlFeedMainDao().getAllConnections(vObj);
			if (list.size() > 0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "ETL Connector Listing",
						list);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No Results Found", null);
			}

			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Defining Connector Specific Tables SERVICE-------------------------------------------*/
	@RequestMapping(path = "/getConnectorTables", method = RequestMethod.POST)
	@ApiOperation(value = "Listing Connector Tables", notes = "Returns list of data connector tables", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getConnectorTables(@RequestBody EtlConnectorVb etlConnectorVb) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			etlConnectorVb.setActionType("Query");
			List<ETLFeedTablesVb> treeVbList = null;
			if (ValidationUtil.isValid(etlConnectorVb.getConnectorScripts())) {
				if (etlConnectorVb.getDynamicScript() != null && etlConnectorVb.getDynamicScript().length > 0) {
					treeVbList = new ArrayList<ETLFeedTablesVb>(etlConnectorVb.getDynamicScript().length);
					for (Object obj : etlConnectorVb.getDynamicScript()) {
						HashMap<Object, Object> sam = (HashMap<Object, Object>) obj;
						ETLFeedTablesVb treeVb = new ETLFeedTablesVb();
						for (Map.Entry entry : sam.entrySet()) {
							String key = String.valueOf(entry.getKey());
							try {
								Method setter = ETLFeedTablesVb.class.getMethod(
										"set" + key.substring(0, 1).toUpperCase() + key.substring(1, key.length()),
										String.class);
								setter.invoke(treeVb, String.valueOf(entry.getValue()));
							} catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException
									| InvocationTargetException e) {
							}
						}
						treeVbList.add(treeVb);
					}
				}
			}
			if ("S".equalsIgnoreCase(etlConnectorVb.getConnectorType())) {// database
				exceptionCode = etlFeedMainWb.formConnectorTables(etlConnectorVb.getConnectorId(),
						etlConnectorVb.getConnectorScripts(), treeVbList);
			} else if ("SS".equalsIgnoreCase(etlConnectorVb.getConnectorType())) { // file
				exceptionCode = etlFeedMainWb.formConnectorFileTables(etlConnectorVb.getConnectorId(),
						etlConnectorVb.getConnectorScripts(), treeVbList);
			}
			if (exceptionCode != null && exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"Defining tables for " + etlConnectorVb.getConnectorId() + "", exceptionCode.getOtherInfo());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException | SecurityException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getConnectorTableCols", method = RequestMethod.POST)
	@ApiOperation(value = "Listing Connector table columns", notes = "Returns list of data connector table columns", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getConnectorTableCols(@RequestBody EtlConnectorVb etlConnectorVb) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			etlConnectorVb.setActionType("Query");
			if ("S".equalsIgnoreCase(etlConnectorVb.getConnectorType())) {
				// String dbScript = vcConfigMainWb.getDbScript(dsConnectorVb.getMacroVar());
				exceptionCode = etlFeedMainWb.formConnectorTableSpecificCols(etlConnectorVb.getConnectorId(),
						etlConnectorVb.getConnectorScripts(), etlConnectorVb.getTableName(),
						etlConnectorVb.getTableType());
			} else if ("SS".equalsIgnoreCase(etlConnectorVb.getConnectorType())) {
				exceptionCode = etlFeedMainWb.formConnectorFileTableSpecificCols(etlConnectorVb.getConnectorId(),
						etlConnectorVb.getConnectorScripts(), etlConnectorVb.getTableName(),
						etlConnectorVb.getTableType());

			}
			if (exceptionCode != null && exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				List<ETLFeedColumnsVb> treeStructuredCols = (List<ETLFeedColumnsVb>) exceptionCode.getOtherInfo();
				if (treeStructuredCols.size() > 0) {
					jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
							"Defining tables [" + etlConnectorVb.getTableName() + "] , table type ["
									+ etlConnectorVb.getTableType() + "] and columns for "
									+ etlConnectorVb.getConnectorId() + "",
							exceptionCode.getOtherInfo());
				} else {
					jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No Records Found",
							exceptionCode.getOtherInfo());
				}
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------MODIFY Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/add", method = RequestMethod.POST)
	@ApiOperation(value = "Add a Etl Feed", notes = "Add a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> add(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			exceptionCode = etlFeedMainWb.insertRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------MODIFY Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/modifyEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Modify a Etl Feed", notes = "Modify a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> modify(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Modify");
			exceptionCode = etlFeedMainWb.modifyRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------DELETE Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/delete", method = RequestMethod.POST)
	@ApiOperation(value = "Delete a Etl Feed", notes = "Delete a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> delete(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedMainWb.deleteRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------Reject Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/rejectEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Reject a Etl Feed", notes = "Reject a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> reject(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedMainWb.reject(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/publishEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Publish a Etl Feed", notes = "Publish a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> publish(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Publish");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedMainWb.publishRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/approveEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Approve a Etl Feed", notes = "Approve a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> approve(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedMainWb.approve(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/bulkApproveEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Approve a Etl Feed", notes = "Approve a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkApprove(@RequestBody List<EtlFeedMainVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedMainVb vObject = new EtlFeedMainVb();
			vObject.setActionType("Approve");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedMainWb.bulkApprove(vObjects, vObject);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("- Approve -", "- Bulk Approve -");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/bulkRejectEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Reject a Etl Feed", notes = "Reject a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> bulkReject(@RequestBody List<EtlFeedMainVb> vObjects) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlFeedMainVb vObject = new EtlFeedMainVb();
			vObject.setActionType("Reject");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode = etlFeedMainWb.bulkReject(vObjects, vObject);
			String errorMessage = exceptionCode.getErrorMsg().replaceAll("- Reject -", "- Bulk Reject -");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/reviewEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Review a Etl Feed", notes = "Review a Etl Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> review(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Review");
			ExceptionCode exceptionCode = etlFeedMainWb.reviewRecordNew(vObject);
			String errorMesss = exceptionCode.getErrorMsg().replaceAll("Query ", "Review");
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMesss,
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/returnJoinCondition", method = RequestMethod.POST)
	@ApiOperation(value = "Return Join Condition", notes = "Return Join Condition", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> returnJoinCondition(
			@RequestBody ETLForQueryReportFieldsWrapperVb etlForQueryReportFieldsWrapperVb) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			EtlForQueryReportVb etlForQueryReportVb = etlForQueryReportFieldsWrapperVb.getMainModel();
			boolean relationExists = false;
			if (ValidationUtil.isValid(etlForQueryReportVb.getCountry())
					&& ValidationUtil.isValid(etlForQueryReportVb.getLeBook())
					&& ValidationUtil.isValid(etlForQueryReportVb.getFeedId())) {
				relationExists = etlFeedMainWb.getEtlFeedMainDao().CheckRelationFeedExists(etlForQueryReportVb);
			}
			if (etlForQueryReportVb.isChecked() || (relationExists && !etlForQueryReportVb.isChecked())) {

				Set<String> uniqueTableIdSet = new HashSet<String>();
				for (EtlExtractionSummaryFieldsVb rCReportFieldsVb : etlForQueryReportFieldsWrapperVb
						.getReportFields()) {
					if (ValidationUtil.isValid(rCReportFieldsVb.getTableId())) {
						uniqueTableIdSet.add(rCReportFieldsVb.getTableId());
					}
				}

				// Integer joinType=
				// designAnalysisWb.designAnalysisDao.getJoinTypeInCatalog(vcForQueryReportVb.getCatalogId());
				/* Query needed data from Etl_FEED_Relations Table */
				Integer joinType = 1;
				String country = etlForQueryReportFieldsWrapperVb.getMainModel().getCountry();
				String leBook = etlForQueryReportFieldsWrapperVb.getMainModel().getLeBook();
				String feedId = etlForQueryReportFieldsWrapperVb.getMainModel().getFeedId();

				int approveOrPend = 0;
				if (etlForQueryReportFieldsWrapperVb.getMainModel().getFeedStatus() == Constants.WORK_IN_PROGRESS) {
					approveOrPend = 9999;
				} else if (etlForQueryReportFieldsWrapperVb.getMainModel().getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
					approveOrPend = 1;
				} else if (etlForQueryReportFieldsWrapperVb.getMainModel().getFeedStatus() == Constants.PUBLISHED) {
					approveOrPend = 0;
				}
				
				ArrayList getDataAL = etlFeedMainWb.getEtlFeedMainDao().getData(country, leBook, feedId, joinType, approveOrPend);
				etlForQueryReportVb.setBaseTableId(etlFeedMainWb.returnBaseTableId(country, leBook, feedId));
				exceptionCode = etlFeedMainWb.returnJoinCondition(etlForQueryReportVb,
						uniqueTableIdSet.stream().collect(Collectors.toList()), joinType, getDataAL);

				if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
					jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION,
							exceptionCode.getErrorMsg(), exceptionCode.getOtherInfo());
				} else {
					jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
							"DynamicJoinFormation and TableId's", exceptionCode.getResponse());
				}
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"DynamicJoinFormation and TableId's", exceptionCode.getResponse());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/generateDynamicDate", method = RequestMethod.POST)
	@ApiOperation(value = "Generate dynamic date with parameters", notes = "Generate dynamic date with parameters", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> generateDynamicDate(@RequestBody EtlExtractionSummaryFieldsVb vObjectMain)
			throws DataAccessException, SAXException {
		JSONExceptionCode jsonExceptionCode = null;
		String dynamciDate1 = null;
		String dynamciDate2 = null;
		try {
			if (ValidationUtil.isValid(vObjectMain.getDynamicStartFlag())
					&& vObjectMain.getDynamicStartFlag().equalsIgnoreCase("y")) {
				Integer dso = 0;
				if (ValidationUtil.isValid(vObjectMain.getDynamicStartOperator())) {
					dso = Integer.parseInt(vObjectMain.getDynamicStartOperator());
				}
				dynamciDate1 = etlFeedMainWb.generateDynamicDate(vObjectMain.getDynamicStartDate(), dso,
						vObjectMain.getDynamicDateFormat(), vObjectMain.getJavaFormatDesc());
			}
			if (ValidationUtil.isValid(vObjectMain.getDynamicEndFlag())
					&& vObjectMain.getDynamicEndFlag().equalsIgnoreCase("y")) {
				Integer deo = 0;
				if (ValidationUtil.isValid(vObjectMain.getDynamicEndOperator())) {
					deo = Integer.parseInt(vObjectMain.getDynamicEndOperator());
				}

				dynamciDate2 = etlFeedMainWb.generateDynamicDate(vObjectMain.getDynamicEndDate(), deo,
						vObjectMain.getDynamicDateFormat(), vObjectMain.getJavaFormatDesc());
			}
			vObjectMain.setValue1(dynamciDate1);
			vObjectMain.setValue2(dynamciDate2);
			if (dynamciDate1 != null || dynamciDate2 != null) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "", vObjectMain);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Not a Valid Date ",
						vObjectMain);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getCategories", method = RequestMethod.POST)
	@ApiOperation(value = "Listing category", notes = "Listing category", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getCategories(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		List<EtlFeedMainVb> dependencylst = null;
		try {
			etlFeedMainVb.setActionType("Query");
			dependencylst = etlFeedMainWb.getCategoryListing(etlFeedMainVb);
			if (ValidationUtil.isValid(dependencylst) && dependencylst.size() > 0) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "", dependencylst);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "No Records Found",
						dependencylst);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getDependencyFeeds", method = RequestMethod.POST)
	@ApiOperation(value = "Get Dependency Feeds", notes = "Get Dependency Feeds", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDependencyFeeds(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		List<EtlFeedMainVb> dependencylst = null;
		try {
			etlFeedMainVb.setActionType("Query");
			if (ValidationUtil.isValid(etlFeedMainVb.getFeedCategory())
					&& ValidationUtil.isValid(etlFeedMainVb.getFeedId())) {
				dependencylst = etlFeedMainWb.findDependencyFeed(etlFeedMainVb);
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "", dependencylst);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Not Valid", dependencylst);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getInjectionConfigCount", method = RequestMethod.POST)
	@ApiOperation(value = "Get Injection Config Count", notes = "Get Injection Config Count", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getInjectionConfigCountBasedOnFeed(
			@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		boolean feedExists = false;
		try {
			etlFeedMainVb.setActionType("Query");
			if (ValidationUtil.isValid(etlFeedMainVb.getCountry()) && ValidationUtil.isValid(etlFeedMainVb.getLeBook())
					&& ValidationUtil.isValid(etlFeedMainVb.getFeedId())) {
				feedExists = etlFeedMainWb.getEtlFeedMainDao().CheckFeedExists(etlFeedMainVb);
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "", feedExists);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Not Valid", feedExists);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getConnectorReadinessScript", method = RequestMethod.POST)
	@ApiOperation(value = "Get Readiness script based on connector", notes = "Get Readiness script based on connector", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getConnectorReadinessScript(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		String script = "";
		try {
			etlFeedMainVb.setActionType("Query");
			if (ValidationUtil.isValid(etlFeedMainVb.getConnectorId())) {
				script = etlFeedMainWb.getEtlFeedMainDao().getConnectorReadinessScript(etlFeedMainVb.getConnectorId());
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "", script);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Not Valid", script);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getTransformationMasterDetails", method = RequestMethod.GET)
	@ApiOperation(value = "Listing Transformation master data", notes = "Returns list of all transformation master details", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getTransformationMasterDetails() {
		ExceptionCode exceptionCode = new ExceptionCode();
		JSONExceptionCode jsonExceptionCode = null;
		try {
			exceptionCode = etlFeedMainWb.getTransformationMasterDetails();
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"Fetching Transformation Master Details Success", exceptionCode.getResponse());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						null, exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*------------------------------------- Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/loadTransformationDataView", method = RequestMethod.POST)
	@ApiOperation(value = "Load Transformation Data View", notes = "Load Transformation Data View and transformation session", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> loadTransformationDataView(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = etlFeedMainWb.saveTransformationSession(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*------------------------------------- Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/retrieveTransformationDataView", method = RequestMethod.POST)
	@ApiOperation(value = "Retrieve Transformation Data View", notes = "Retrieve Transformation Data View based on session", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> retrieveTransformationDataView(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Query");
			exceptionCode = etlFeedMainWb.retrieveTransformationDataView(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------ADD Etl Feed Main------------------------------------------*/
	@RequestMapping(path = "/saveEtlFeedMain", method = RequestMethod.POST)
	@ApiOperation(value = "Save Etl Feed Main", notes = "Add Etl Feed Main", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> saveEtlFeedMain(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Add");
			exceptionCode = etlFeedMainWb.saveFeedDataIntoUplRecord(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	/*-------------------------------------cHECK CONNECTOR------------------------------------------*/
	@RequestMapping(path = "/checkConnector", method = RequestMethod.POST)
	@ApiOperation(value = "Check connector based on Feed", notes = "Check connector based on Feed", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> checkConnector(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		String isValid = "";
		try {
			etlFeedMainVb.setActionType("Query");
			if (ValidationUtil.isValid(etlFeedMainVb.getFeedId())
					&& ValidationUtil.isValid(etlFeedMainVb.getFeedType())) {
				isValid = etlFeedMainWb.getEtlFeedMainDao().checkConnector(etlFeedMainVb);
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "", isValid);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, "Not Valid", isValid);
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getSampleDataRuleDetails", method = RequestMethod.POST)
	@ApiOperation(value = "Get Sample Rule Details", notes = "Get Sample Rule Details", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getSampleDataRuleDetails(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			etlFeedMainVb.setActionType("Query");
			exceptionCode = etlFeedMainWb.getSampleDataRuleDetails(etlFeedMainVb);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"Getting Sample Data Rule Details ", exceptionCode.getResponse());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						null, exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/createSparkFileData", method = RequestMethod.POST)
	@ApiOperation(value = "Create Sample Spark File Data", notes = "Create Sample Spark File Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> createSparkFileData(@RequestParam("file") MultipartFile file,
			@RequestParam("feedId") String feedId, @RequestParam("country") String country,
			@RequestParam("leBook") String leBook) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = visionUploadWb.createSparkFileData(file, feedId, country, leBook);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"Creating Sample Spark File Data", exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						null, exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/deleteTransformationSession", method = RequestMethod.POST)
	@ApiOperation(value = "Delete a Transformation Session", notes = "Delete a Transformation Session", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteTransformationSession(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = visionUploadWb.deleteTransformationSession(etlFeedMainVb, false);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION,
						"Deleting Transformation Session - Successful", exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(),
						null, exceptionCode.getOtherInfo());
			}
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/getTransformedDataSample", method = RequestMethod.POST)
	@ApiOperation(value = "Get Transformed Data Sample", notes = "Get Transformed Data Sample", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getTransformedDataSample(@RequestBody EtlFeedMainVb vObject,
			@RequestParam("nodeId") String nodeId, HttpServletRequest request, HttpServletResponse response) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Query");
			exceptionCode = etlFeedMainWb.getSampleTransformedDataByReadingOP_File(vObject, nodeId);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				response.addHeader("Content-Type", "application/txt");
				response.setContentType("application/txt");
				byte[] bytes = (byte[]) exceptionCode.getResponse();
				InputStream is = new ByteArrayInputStream(bytes);
				OutputStream out = response.getOutputStream();
//				FileUtil.copyStream(is, out);
				final int MAX = 4096;
				byte[] buf = new byte[MAX];
				for (int bytesRead = is.read(buf, 0, MAX); bytesRead != -1; bytesRead = is.read(buf, 0, MAX)) {
					out.write(buf, 0, bytesRead);
				}
				out.flush();
				out.close();
				is.close();
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null);
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
			}
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
		}
	}
	
	@RequestMapping(path = "/getQueryTransformedLoadData", method = RequestMethod.POST)
	@ApiOperation(value = "Get Loader Data For Transformation", notes = "Get Loader Data For Transformation", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getQueryTransformedLoadData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getQueryTransformedLoadData(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/doFullTransformationRunOnSampleData", method = RequestMethod.POST)
	@ApiOperation(value = "Do full run for transformation on sample data", notes = "Do full run for transformation on sample data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> doFullTransformationRunOnSampleData(@RequestBody EtlFeedMainVb vObject, 
			HttpServletRequest request, HttpServletResponse response) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Query");
			exceptionCode = etlFeedMainWb.doFullTransformationRunOnSampleData(vObject);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				response.addHeader("Content-Type", "application/txt");
				response.setContentType("application/txt");
				byte[] bytes = (byte[]) exceptionCode.getResponse();
				InputStream is = new ByteArrayInputStream(bytes);
				OutputStream out = response.getOutputStream();
//				FileUtil.copyStream(is, out);
				final int MAX = 4096;
				byte[] buf = new byte[MAX];
				for (int bytesRead = is.read(buf, 0, MAX); bytesRead != -1; bytesRead = is.read(buf, 0, MAX)) {
					out.write(buf, 0, bytesRead);
				}
				out.flush();
				out.close();
				is.close();
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, "Success", response);
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null);
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
			}
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
		}
	}

	@RequestMapping(path = "/getPayloadDataForFullRun", method = RequestMethod.POST)
	@ApiOperation(value = "Get Payload For Data Viewer", notes = "Get Payload For Data Viewer", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getPayloadDataForFullRun(@RequestParam("status") int status, @RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Query");
			exceptionCode = etlFeedMainWb.getPayloadDataForFullRun(vObject, status);
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, exceptionCode.getErrorMsg(), exceptionCode.getResponse(), exceptionCode.getOtherInfo());
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
			} else {
				jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, exceptionCode.getErrorMsg(), null);
				return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
			}
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/deleteAllSourceTabData", method = RequestMethod.POST)
	@ApiOperation(value = "Delete all Source Tab Data", notes = "Delete all Source Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteAllSourceTabData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode = etlFeedMainWb.deleteAllSourceTabData(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/deleteAllMaterializedTabData", method = RequestMethod.POST)
	@ApiOperation(value = "Delete all Materialized Tab Data", notes = "Delete all Materialized Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteAllMaterializedTabData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode = etlFeedMainWb.deleteAllMaterializedTabData(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/deleteAllTransformationTabData", method = RequestMethod.POST)
	@ApiOperation(value = "Delete all Transformation Tab Data", notes = "Delete all Transformation Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteAllTransformationTabData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode = etlFeedMainWb.deleteAllTransformationTabData(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}

	@RequestMapping(path = "/deleteAllLoaderTabData", method = RequestMethod.POST)
	@ApiOperation(value = "Delete all Loader Tab Data", notes = "Delete all Loader Tab Data", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> deleteAllLoaderTabData(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			vObject.setActionType("Delete");
			exceptionCode = etlFeedMainWb.deleteAllLoaderTabData(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
	@RequestMapping(path = "/getEtlSourceTableList", method = RequestMethod.POST)
	@ApiOperation(value = "Get Etl Source Table List", notes = "Get Etl Source Table List", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getEtlSourceTableList(@RequestBody EtlFeedMainVb vObject) {
		JSONExceptionCode jsonExceptionCode = null;
		try {
			vObject.setActionType("Query");
			ExceptionCode exceptionCode = etlFeedMainWb.getEtlSourceTableList(vObject);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse(), exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (RuntimeCustomException rex) {
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	

	@RequestMapping(path = "/generateFeedId", method = RequestMethod.POST)
	@ApiOperation(value = "Generate Feed Id", notes = "Generate Feed Id", response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> generateFeedId(@RequestBody EtlFeedMainVb etlFeedMainVb) {
		JSONExceptionCode jsonExceptionCode = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			exceptionCode = etlFeedMainWb.generateFeedId(etlFeedMainVb);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),
					exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		} catch (Exception e) {
			// e.printStackTrace();
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, e.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}
	}
	
}
