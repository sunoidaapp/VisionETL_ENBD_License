package com.vision.examples;

import java.io.*;
import java.util.stream.Collectors;

public class SparkProcessKiller {
	public static void main(String[] args) {
		String sparkProcessName = "ETLExtractionEng_Transformation.jar";
		String folderName = "your_folder_name";

		try {
			String psCommand = "ps -ef | grep spark | grep -v grep | grep -i '" + sparkProcessName + "' | grep -i '"
					+ folderName + "' | awk '{print $2}'";

			ProcessBuilder psProcessBuilder = new ProcessBuilder("/bin/bash", "-c", psCommand);
			Process psProcess = psProcessBuilder.start();
			InputStream psInputStream = psProcess.getInputStream();

			String pid = new BufferedReader(new InputStreamReader(psInputStream)).lines()
					.collect(Collectors.joining("\n"));

			if (!pid.isEmpty()) {
				String killCommand = "kill -9 " + pid;
				ProcessBuilder killProcessBuilder = new ProcessBuilder("/bin/bash", "-c", killCommand);
				Process killProcess = killProcessBuilder.start();
				int exitCode = killProcess.waitFor();
				if (exitCode == 0) {
					System.out.println("Successfully killed the Spark process with PID: " + pid);
				} else {
					System.out.println("Failed to kill the Spark process with PID: " + pid);
				}
			} else {
				System.out.println("No matching Spark process found.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
