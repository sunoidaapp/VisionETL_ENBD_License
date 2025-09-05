package com.vision.examples;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;

public class FtpSample {
	
	public static void main(String[] args) {
//		ExceptionCode exceptionCode  = new ExceptionCode();
	    try{
	    	String dirName = "DD_DIR";
	    	String fileName = "sample";
	    	String fileExtension = "txt";
			File lFile = new File("E:/Alert/sample.txt");
			if(lFile.exists()){
				lFile.delete();
			}
			lFile.createNewFile();
			try {
			    FileWriter myWriter = new FileWriter("E:/Alert/sample.txt");
			    myWriter.write("content");
			    myWriter.close();
		    } catch (IOException e) {
		    	// e.printStackTrace();
		    }
	        byte[] data = Files.readAllBytes(Paths.get("E:/Alert/sample.txt"));

			if("Y".equalsIgnoreCase("Y")){
	    		JSch jsch = new JSch();  
				Session session = jsch.getSession("vision", "10.16.1.101", 22); 
				session.setPassword("vision123");
			        session.setConfig("StrictHostKeyChecking", "no");
			        session.connect();
				Channel channel = session.openChannel("sftp"); 
				channel.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channel;
		        sftpChannel.cd("/home/vision/app");
		        String currentDirectory=sftpChannel.pwd();
		        SftpATTRS attrs=null;
		        try {
		            attrs = sftpChannel.stat(currentDirectory+"/"+dirName);
		        } catch (Exception e) {
		        }

		        if (attrs != null) {
		        } else {
		            sftpChannel.mkdir(dirName);
		        }
		        sftpChannel.cd("/home/vision/app"+"/"+dirName+"/");
			    InputStream in = new ByteArrayInputStream(data);
				sftpChannel.put(in,fileName+"."+fileExtension);
				sftpChannel.exit();
				do {Thread.sleep(1000); } while(!channel.isEOF()); 
				session.disconnect();
	    	}

	    }catch(FileNotFoundException e){
	    	// e.printStackTrace();
	    }catch(IOException e){
	    	// e.printStackTrace();
	    }catch(Exception e){
	    	// e.printStackTrace();
	    }

	}

}
