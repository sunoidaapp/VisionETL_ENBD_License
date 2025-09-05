package com.vision.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class SparkAPICaller {

	public static void main(String[] args) {
		

		String appId = "app-20230823164627-0002";
		String sparkMasterUrl = "http://10.16.1.101:8082";
		String appKillEndpoint = "/api/v1/applications/" + appId + "/kill";
		/*try {
			URL url = new URL(sparkMasterUrl + appKillEndpoint);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");

			int responseCode = connection.getResponseCode();
			// System.out.println("Response Code: " + responseCode);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String inputLine;
			StringBuilder response = new StringBuilder();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			// System.out.println("Response: " + response.toString());
		} catch (Exception e) {
			// e.printStackTrace();
		}*/
		
		 String apiEndpoint = "http://10.16.1.101:8082/app/kill/";
		 String payload = "{\"id\":\"app-20230823164627-0002\", \"terminate\": true}";
		
		try {
			URL url = new URL(apiEndpoint);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = payload.getBytes("utf-8");
				os.write(input, 0, input.length);
			}
			int responseCode = conn.getResponseCode();
			// System.out.println("Response Code: " + responseCode);
			try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"))) {
				StringBuilder response = new StringBuilder();
				String responseLine = null;
				while ((responseLine = br.readLine()) != null) {
					response.append(responseLine.trim());
				}
				// System.out.println("Response: " + response.toString());
			}
			conn.disconnect(); 
		} catch (Exception e) {
			// e.printStackTrace();
		}
		
		
		
	}
}
