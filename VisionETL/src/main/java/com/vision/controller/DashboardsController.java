package com.vision.controller;

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
import com.vision.vb.DashboardTilesVb;
import com.vision.vb.DashboardVb;
import com.vision.wb.DashboardsWb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("dashboards")
@Api(value="Dashboards Controller" , description="Dashboards Controller")
public class DashboardsController {
	@Autowired
	DashboardsWb dashboardsWb;
	
	/*------------------------------------- DASHOARD ON LOAD------------------------------------------*/
	@RequestMapping(path = "/getDashboardOnLoad", method = RequestMethod.POST)
	@ApiOperation(value = "Load Dashboard Detail",notes = "Dashboard Detail and Tab detail",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDashboardDetail(@RequestBody DashboardVb dObj){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			dObj.setActionType("Clear");
			exceptionCode = dashboardsWb.getDashboardDetail(dObj);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*------------------------------------- DASHOARD TAB DATA------------------------------------------*/
	@RequestMapping(path = "/getDashboardResultData", method = RequestMethod.POST)
	@ApiOperation(value = "Dashboard Result Data",notes = "Get Tab data by passing Tab Id and Dashboard Id and Tile Id",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDashboardTabData(@RequestBody DashboardTilesVb dObj){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = dashboardsWb.getTileResultData(dObj);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*------------------------------------- DASHOARD TAB DRILL DOWN DATA------------------------------------------*/
	@RequestMapping(path = "/getDashboardDrillDownData", method = RequestMethod.POST)
	@ApiOperation(value = "Load Dashboard Detail",notes = "Get Tab data by passing Tab Id and Dashboard Id",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDashboardDrillDownData(@RequestBody List<DashboardTilesVb> drillDownlst){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = dashboardsWb.getDrillDownData(drillDownlst);
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
	/*-------------------------------------GET DASHBOARD LIST BY GROUP------------------------------------------*/
	@RequestMapping(path = "/getDashboardList", method = RequestMethod.GET)
	@ApiOperation(value = "Get Dasshboard List",notes = "Get List of Dashboard on Group wise",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> getDashboardList(){
		JSONExceptionCode jsonExceptionCode  = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			exceptionCode = dashboardsWb.getDashboardList();
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(),exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.METHOD_FAILURE);
		}	
	}
}
