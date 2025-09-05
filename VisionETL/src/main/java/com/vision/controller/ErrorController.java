package com.vision.controller;

import java.util.Map;

import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;

@RestController
public class ErrorController implements org.springframework.boot.web.servlet.error.ErrorController {

    private final ErrorAttributes errorAttributes;

    public ErrorController(ErrorAttributes errorAttributes) {
        this.errorAttributes = errorAttributes;
    }

    @RequestMapping("/error")
    public ResponseEntity<Map<String, Object>> handleError(WebRequest webRequest) {
        Map<String, Object> errorAttributesMap = errorAttributes.getErrorAttributes(webRequest, ErrorAttributeOptions.of(ErrorAttributeOptions.Include.MESSAGE));

        // Extract the relevant error details from the errorAttributesMap
        int statusCode = (int) errorAttributesMap.get("status");
        String errorMessage = (String) errorAttributesMap.get("message");

        // Create a custom error response
        String response = "An error occurred. Status: " + statusCode + ", Message: " + errorMessage;

        // Return the custom error response with the appropriate status code
        return ResponseEntity.status(HttpStatus.valueOf(statusCode)).body(errorAttributesMap);
    }
}



