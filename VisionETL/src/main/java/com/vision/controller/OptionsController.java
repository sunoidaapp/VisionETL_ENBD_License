package com.vision.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OptionsController {
	 @RequestMapping(method = RequestMethod.OPTIONS)
	    public ResponseEntity handle() {
	        return new ResponseEntity(HttpStatus.OK);
	    }
}
