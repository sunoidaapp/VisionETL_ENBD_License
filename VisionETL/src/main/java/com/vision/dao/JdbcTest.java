package com.vision.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JdbcTest {

	@Autowired
	JdbcTemplate jdbcTemplate;
	
	
	public String getSampleValue() {
		String query = " select ERROR_CODE from ERROR_CODES where rownum = 1 ";
		return jdbcTemplate.queryForObject(query, String.class);
	}
	
}
