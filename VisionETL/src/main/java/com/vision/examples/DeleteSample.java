package com.vision.examples;

import java.sql.*;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class DeleteSample {

	public static void main(String[] args) {
		String jdbcUrl = "jdbc:sqlserver://10.16.1.38;instance=VISIONBISQL2019;port=52866;DatabaseName=Vision_ETL;encrypt=false;trustServerCertificate=false";
		String username = "Vision";
		String password = "Vision@123";

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			DriverManagerDataSource dataSource = new DriverManagerDataSource(jdbcUrl, username, password);

			JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

			/*String sqlQuery = "delete FROM PRD_Suspecious_Token_Audit where R_SESSION_ID = ? ";

			String rSessId = "2011_714002_1694168537158";

			int rowsDeleted = jdbcTemplate.update(sqlQuery, new Object[] { rSessId });
			if (rowsDeleted > 0) {
				// System.out.println(rowsDeleted + " row(s) deleted successfully.");
			} else {
				// System.out.println("No rows were deleted.");
			}*/
			
			String sqlQuery = " DROP TABLE ? PURGE ";
			String tableName = "Prd_Refresh_Token_Count_temp_table";
			int returnval = jdbcTemplate.update(sqlQuery, new Object[] {tableName});
			// System.out.println(returnval);
			
			
			
		} catch (Exception e) {
			System.err.println("SQL Exception: " + e.getMessage());
			// e.printStackTrace();
		}

	}

}
