package com.vision.examples;

import java.io.IOException;

public class YarnApplicationKiller {

    public static void main(String[] args) {
        String applicationId = "app-20230823164627-0002"; 

        String command = "cmd.exe /c yarn application -kill " + applicationId;

        try {
            Process process = new ProcessBuilder(command).start();
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
