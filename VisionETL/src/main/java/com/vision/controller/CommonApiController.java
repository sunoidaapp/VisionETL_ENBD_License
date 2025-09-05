package com.vision.controller;

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
import com.vision.vb.CommonApiModel;
import com.vision.vb.VisionUsersVb;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("commonCompApi")
@Api(value = "Common Api", description = "Common Api")
public class CommonApiController {

	@Autowired
	CommonApiDao commonApiDao;
	
	@RequestMapping(path = "/commonDataApi", method = RequestMethod.POST)
	@ApiOperation(value = "Common Data Api",notes = "Common Data Api",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> commonDataApi(@RequestBody CommonApiModel vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataFetch(vObject) ;
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
		}
	}
	@RequestMapping(path = "/saveAppTheme", method = RequestMethod.POST)
	@ApiOperation(value = "Save App Theme",notes = "Save App Theme",response = ResponseEntity.class)
	public ResponseEntity<JSONExceptionCode> saveAppTheme(@RequestBody VisionUsersVb vObject){
		JSONExceptionCode jsonExceptionCode  = null;
		try{
			ExceptionCode exceptionCode = commonApiDao.insertAppTheme(vObject) ;
			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(), exceptionCode.getResponse());
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);
		}catch(RuntimeCustomException rex){
			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), "");
			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.EXPECTATION_FAILED);
		}
	}
}
