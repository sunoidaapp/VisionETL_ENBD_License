package com.vision.examples;

import com.jcraft.jsch.*;
import java.io.*;

public class SparkApplicationKiller {
    public static void main(String[] args) {
        String remoteHost = "10.16.1.101";
        String remoteUser = "vision";
        String remotePassword = "vision123";
        String applicationId = "app-20230425124640-0001";

        try {
            // Establish an SSH session to the remote host
            JSch jsch = new JSch();
            Session session = jsch.getSession(remoteUser, remoteHost, 22);
            session.setPassword(remotePassword);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            // Build the command to get the PID of the Spark application
            String pidCommand = "ps -ef | grep -i 'spark.*" + applicationId + "' | grep -v grep | awk '{print $2}'";

            // Execute the PID command on the remote host
            Channel pidChannel = session.openChannel("exec");
            ((ChannelExec) pidChannel).setCommand(pidCommand);
            pidChannel.setInputStream(null);
            ((ChannelExec) pidChannel).setErrStream(System.err);
            InputStream pidStream = pidChannel.getInputStream();
            pidChannel.connect();

            // Read the PID from the channel's input stream
            byte[] pidBytes = new byte[1024];
            int pidLength = pidStream.read(pidBytes);
            String pid = new String(pidBytes, 0, pidLength).trim();

            // Build the kill command to execute on the remote host
            String killCommand = "kill -9 " + pid;

            // Execute the kill command on the remote host
            Channel killChannel = session.openChannel("exec");
            ((ChannelExec) killChannel).setCommand(killCommand);
            killChannel.setInputStream(null);
            ((ChannelExec) killChannel).setErrStream(System.err);
            InputStream killStream = killChannel.getInputStream();
            killChannel.connect();

            // Wait for the command to complete
            byte[] tmp = new byte[1024];
            while (true) {
                while (killStream.available() > 0) {
                    int i = killStream.read(tmp, 0, 1024);
                    if (i < 0) break;
                    // System.out.print(new String(tmp, 0, i));
                }
                if (killChannel.isClosed()) {
                    if (killStream.available() > 0) continue;
                    // System.out.println("Exit status: " + killChannel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }

            // Close the SSH session
            pidChannel.disconnect();
            killChannel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
