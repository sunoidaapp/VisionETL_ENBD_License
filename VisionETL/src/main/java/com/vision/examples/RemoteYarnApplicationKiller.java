package com.vision.examples;

import com.jcraft.jsch.*;

public class RemoteYarnApplicationKiller {

	public static void main(String[] args) {
		String username = "vision";
		String hostname = "10.16.1.101";
		String applicationId = "app-20230823164627-0002";

	//	 String command = "yarn application -kill " + applicationId;

		 //	 String command = "bash -l -c 'yarn application -kill " + applicationId + "'";

		
		 /*	try {
			JSch jsch = new JSch();
			Session session = jsch.getSession(username, hostname, 22);
			session.setPassword("vision123");
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			ChannelExec channel = (ChannelExec) session.openChannel("exec");
			channel.setCommand(command);
			channel.connect();
			int exitStatus = channel.getExitStatus();
			if (exitStatus == 0) {
				// System.out.println("Application terminated successfully.");
			} else {
				System.err.println("Failed to terminate application. Exit code: " + exitStatus);
			}

			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			// e.printStackTrace();
		} */
		 
		
		String command = "bash -c \"yarn application -kill " + applicationId + "\"";

		try {
			Process process = Runtime.getRuntime().exec(command);
			int exitCode = process.waitFor();
			if (exitCode == 0) {
				// System.out.println("Application terminated successfully.");
			} else {
				System.err.println("Failed to terminate application. Exit code: " + exitCode);
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}
}
