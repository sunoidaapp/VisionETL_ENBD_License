package com.vision.wb;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.parser.UnixFTPEntryParser;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.multipart.MultipartFile;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;
import com.vision.authentication.CustomContextHolder;
import com.vision.dao.AbstractDao;
import com.vision.dao.EtlFeedMainDao;
import com.vision.dao.VisionUploadDao;
import com.vision.download.ExportXlsServlet;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.AESEncryptionDecryption;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.ValidationUtil;
import com.vision.vb.DSConnectorVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedTranNodeVb;
import com.vision.vb.EtlFileColumnsVb;
import com.vision.vb.EtlFileUploadAreaVb;
import com.vision.vb.FileInfoVb;
import com.vision.vb.VisionUploadVb;

@Component
public class VisionUploadWb extends AbstractDynaWorkerBean<VisionUploadVb> implements ApplicationContextAware {

	@Autowired
	private VisionUploadDao visionUploadDao;

	@Autowired
	private EtlFeedMainDao etlFeedMainDao;

	@Autowired
	private ExportXlsServlet exportXlsServlet;

	private static String uploadScriptDir;

	@Value("${feed.upload.scriptDir}")
	public void setUploadScriptDir(String uploadScriptDir) {
		VisionUploadWb.uploadScriptDir = uploadScriptDir;
	}

	@Value("${ftp.sampleFeedDataSeparator}")
	private String sampleFeedDataSeparator;

	@Value("${ftp.sampleFeedDataDir}")
	private String sampleFeedDataDir;

	@Value("${ftp.connectorTempFeedDir}")
	private String connectorTempFeedDir;

	@Value("${ftp.connectorFeedDir}")
	private String connectorFeedDir;

	@Value("${ftp.port}")
	private String port;

	@Value("${ftp.hostName}")
	private String hostName;

	@Value("${ftp.userName}")
	private String userName;

	@Value("${ftp.password}")
	private String password;

	@Value("${ftp.xlUploadhostName}")
	private String xlUploadhostName;

	@Value("${ftp.xlUploaduserName}")
	private String xlUploaduserName;

	@Value("${ftp.xlUploadpassword}")
	private String xlUploadpassword;

	private String knownHostsFileName;

	private String cbDir;
	private int blockSize;
	private String uploadDir;
	private String uploadDirExl;
	private String downloadDir;
	private String buildLogsDir;
	private String timezoneId;
	private String dateFormate;
	private String serverType;
	private String scriptDir;
	private int fileType;
	private char prompt;
	
	@Value("${feed.upload.securedFtp}")
	private String securedFtp = "N";
	
	private String fileExtension;
	private String connectorUploadDir;
	private String connectorADUploadDir;
	private int uploadFileChkIntervel = 30;
	private static final String SERVICE_NAME = "Vision Upload";
	private static final int DIR_TYPE_DOWNLOAD = 1;
	private WebApplicationContext webApplicationContext = null;

	@Value("${ftp.transSampleOperationDir}")
	private String transSampleOperationDir;

	@Value("${feed.upload.localFilePath}")
	private String localFilePath;

	@Value("${ftp.transSampleFileName}")
	private String transFileName;

	@Value("${ftp.transSampleFileExtension}")
	private String transFileExtension;

	public VisionUploadDao getVisionUploadDao() {
		return visionUploadDao;
	}

	public void setVisionUploadDao(VisionUploadDao visionUploadDao) {
		this.visionUploadDao = visionUploadDao;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getConnectorUploadDir() {
		return connectorUploadDir;
	}

	public void setConnectorUploadDir(String connectorUploadDir) {
		this.connectorUploadDir = connectorUploadDir;
	}

	public String getConnectorADUploadDir() {
		return connectorADUploadDir;
	}

	public void setConnectorADUploadDir(String connectorADUploadDir) {
		this.connectorADUploadDir = connectorADUploadDir;
	}

	public String getUploadDir() {
		return uploadDir;
	}

	public void setUploadDir(String uploadDir) {
		this.uploadDir = uploadDir;
	}

	public String getDownloadDir() {
		return downloadDir;
	}

	public void setDownloadDir(String downloadDir) {
		this.downloadDir = downloadDir;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public String getTimezoneId() {
		return timezoneId;
	}

	public void setTimezoneId(String timezoneId) {
		this.timezoneId = timezoneId;
	}

	public String getDateFormate() {
		return dateFormate;
	}

	public void setDateFormate(String dateFormate) {
		this.dateFormate = dateFormate;
	}

	public String getServerType() {
		return serverType;
	}

	public void setServerType(String serverType) {
		this.serverType = serverType;
	}

	public String getBuildLogsDir() {
		return buildLogsDir;
	}

	public void setBuildLogsDir(String buildLogsDir) {
		this.buildLogsDir = buildLogsDir;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.webApplicationContext = (WebApplicationContext) applicationContext;
	}

	public String getScriptDir() {
		return scriptDir;
	}

	public void setScriptDir(String scriptDir) {
		this.scriptDir = scriptDir;
	}

	public WebApplicationContext getWebApplicationContext() {
		return webApplicationContext;
	}

	public void setWebApplicationContext(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}

	public String getKnownHostsFileName() {
		return knownHostsFileName;
	}

	public void setKnownHostsFileName(String knownHostsFileName) {
		this.knownHostsFileName = knownHostsFileName;
	}

	public char getPrompt() {
		return prompt;
	}

	public void setPrompt(char prompt) {
		this.prompt = prompt;
	}

	public int getFileType() {
		return fileType;
	}

	public void setFileType(int fileType) {
		this.fileType = fileType;
	}

	public String getFileExtension() {
		return fileExtension;
	}

	public void setFileExtension(String fileExtension) {
		this.fileExtension = fileExtension;
	}

	public String getXlUploadhostName() {
		return xlUploadhostName;
	}

	public void setXlUploadhostName(String xlUploadhostName) {
		this.xlUploadhostName = xlUploadhostName;
	}

	public String getXlUploaduserName() {
		return xlUploaduserName;
	}

	public void setXlUploaduserName(String xlUploaduserName) {
		this.xlUploaduserName = xlUploaduserName;
	}

	public String getXlUploadpassword() {
		return xlUploadpassword;
	}

	public void setXlUploadpassword(String xlUploadpassword) {
		this.xlUploadpassword = xlUploadpassword;
	}

	public String getUploadDirExl() {
		return uploadDirExl;
	}

	public void setUploadDirExl(String uploadDirExl) {
		this.uploadDirExl = uploadDirExl;
	}
	
	

	public class MyUserInfo implements UserInfo {
		public String getPassword() {
			return password;
		}

		public boolean promptYesNo(String str) {
			return false;
		}

		public String getPassphrase() {
			return null;
		}

		public boolean promptPassphrase(String message) {
			return true;
		}

		public boolean promptPassword(String message) {
			return false;
		}

		public void showMessage(String message) {
			return;
		}
	}

	public ExceptionCode doUpload(String fileName, byte[] data, String fileExtension) {
		ExceptionCode exceptionCode = null;
		try {
			setUploadDownloadDirFromDB();
			fileName = fileName.toUpperCase(); // Changes done by Deepak for Bank One
			if (fileName.contains(".XLSX") || fileName.contains(".XLS")) {
				fileExtension = "xlsx";
				setUserName(getXlUploaduserName());
				setPassword(getXlUploadpassword());
				setHostName(getXlUploadhostName());
				setUploadDir(getUploadDirExl());
			}
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				jsch.setKnownHosts(getKnownHostsFileName());
				Session session = jsch.getSession(getUserName(), getHostName());
				{ // "interactive" version // can selectively update specified known_hosts file
					// need to implement UserInfo interface
					// MyUserInfo is a swing implementation provided in
					// examples/Sftp.java in the JSch dist
					UserInfo ui = new MyUserInfo();
					session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
					session.setPassword(getPassword());
				}
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				Channel channel = session.openChannel("sftp");
				channel.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channel;
				fileName = fileName.substring(0, fileName.lastIndexOf('.')) + "." + fileExtension + "";
				sftpChannel.cd(uploadDir);
				InputStream in = new ByteArrayInputStream(data);
				sftpChannel.put(in, fileName);
				sftpChannel.exit();
				channel = session.openChannel("shell");
				OutputStream inputstream_for_the_channel = channel.getOutputStream();
				PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
				channel.connect();
				commander.println("cd " + scriptDir);
				StringBuilder cmd = new StringBuilder();
				if (!"xlsx".equalsIgnoreCase(fileExtension)) {
					if ('/' != uploadDir.charAt(uploadDir.length() - 1)) {
						cmd.append("./remSpecialChar.sh " + uploadDir + "/" + fileName);
					} else {
						cmd.append("./remSpecialChar.sh " + uploadDir + fileName);
					}
				}
				commander.println(cmd);
				commander.println("exit");
				commander.close();
				do {
					Thread.sleep(1000);
				} while (!channel.isEOF());
				session.disconnect();
			} else {
				FTPClient ftpClient = getConnection();
				ftpClient.connect(getHostName());
				boolean response = ftpClient.login(getUserName(), getPassword());
				if (!response) {
					ftpClient.disconnect();
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR,
							"Upload", "");
					exceptionCode.setResponse(fileName);
					throw new RuntimeCustomException(exceptionCode);
				}
				ftpClient.setFileType(fileType);
				response = ftpClient.changeWorkingDirectory(uploadDir);
				if (!response) {
					ftpClient.disconnect();
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR,
							"Upload", "");
					exceptionCode.setResponse(fileName);
					throw new RuntimeCustomException(exceptionCode);
				}
				fileName = fileName.toUpperCase();
				fileName = fileName.substring(0, fileName.lastIndexOf('.')) + "_"
						+ CustomContextHolder.getContext().getVisionId() + "." + fileExtension + "";
				InputStream in = new ByteArrayInputStream(data);
				ftpClient.storeFile(fileName, in);
				in.close(); // close the io streams
				ftpClient.disconnect();
			}
		} catch (FileNotFoundException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		}
		exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
		exceptionCode.setOtherInfo(fileName);
		return exceptionCode;
	}

	public ExceptionCode getQueryResults(VisionUploadVb vObject) {
		setVerifReqDeleteType(vObject);
		List<VisionUploadVb> collTemp = getScreenDao().getQueryResults(vObject, 1);
		doSetDesctiptionsAfterQuery(collTemp);
		ExceptionCode exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Query",
				"");
		exceptionCode.setOtherInfo(vObject);
		exceptionCode.setResponse(collTemp);
		return exceptionCode;
	}

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(10);
			arrListLocal.add(collTemp);
			return arrListLocal;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
				logger.error("Exception in getting the Page load values.", e);
			}
			return null;
		}
	}

	public ExceptionCode listFilesFromFtpServer(int dirType, String orderBy) {
		ExceptionCode exceptionCode = null;
		TelnetConnection telnetConnection = null;
		try {
			setUploadDownloadDirFromDB();
			if ("Y".equalsIgnoreCase(securedFtp)) {
				return listFilesFromSFtpServer(dirType, orderBy);
			}
			List<FileInfoVb> lFileList = new ArrayList<FileInfoVb>();
			telnetConnection = new TelnetConnection(hostName, userName, password, prompt);
			// telnetConnection.connect(getServerType());
			telnetConnection.connect();
			if (dirType == DIR_TYPE_DOWNLOAD)
				telnetConnection.sendCommand("cd " + downloadDir);
			else
				telnetConnection.sendCommand("cd " + buildLogsDir);
			String responseStr = telnetConnection.sendCommand("ls -ltc ");
			String[] fileEntryArray = responseStr.split("\r\n");
			FTPClientConfig conf = new FTPClientConfig(getServerType());
			conf.setServerTimeZoneId(getTimezoneId());
			UnixFTPEntryParser unixFtpEntryParser = new UnixFTPEntryParser(conf);
			List<String> fileEntryList = unixFtpEntryParser
					.preParse(new LinkedList<String>(Arrays.asList(fileEntryArray)));
			List<FTPFile> lfiles = new ArrayList<FTPFile>(fileEntryList.size());
			for (String fileEntry : fileEntryList) {
				FTPFile ftpFile = unixFtpEntryParser.parseFTPEntry(fileEntry);
				if (ftpFile != null)
					lfiles.add(ftpFile);
			}
			for (FTPFile fileName : lfiles) {
				if (fileName.getName().endsWith(".zip") || fileName.getName().endsWith(".tar"))
					continue;
				FileInfoVb fileInfoVb = new FileInfoVb();
				fileInfoVb.setName(fileName.getName());
				fileInfoVb.setSize(formatFileSize(fileName.getSize()));
				/*
				 * if(dirType == 1){ fileInfoVb.setDate(formatDate(fileName));
				 * lFileList.add(fileInfoVb); }
				 */
				if (dirType == 1 && (fileInfoVb.getName().startsWith("VISION_UPLOAD")
						&& StringUtils.countOccurrencesOf(fileInfoVb.getName(), "_") > 0)) {
					Calendar cal = new GregorianCalendar();
					int month = cal.get(Calendar.MONTH) + 1;
					int year = cal.get(Calendar.YEAR);
					int day = cal.get(Calendar.DAY_OF_MONTH);
					fileInfoVb.setDate(CommonUtils.getFixedLength(String.valueOf(day), "0", 2) + "-"
							+ CommonUtils.getFixedLength(String.valueOf(month), "0", 2) + "-" + year);
					lFileList.add(fileInfoVb);
				} else if (dirType == 2 && (fileInfoVb.getName().startsWith("BUILDCRON")
						&& StringUtils.countOccurrencesOf(fileInfoVb.getName(), "-") <= 0)) {
					Calendar cal = new GregorianCalendar();
					int month = cal.get(Calendar.MONTH) + 1;
					int year = cal.get(Calendar.YEAR);
					int day = cal.get(Calendar.DAY_OF_MONTH);
					fileInfoVb.setDate(CommonUtils.getFixedLength(String.valueOf(day), "0", 2) + "-"
							+ CommonUtils.getFixedLength(String.valueOf(month), "0", 2) + "-" + year);
					lFileList.add(fileInfoVb);
				} else if ((dirType == 2 || dirType == 1)
						&& (StringUtils.countOccurrencesOf(fileInfoVb.getName(), "-") > 0)) {
					fileInfoVb.setDate(formatDate1(fileName));
					lFileList.add(fileInfoVb);
				}
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Download", "");
//	    	exceptionCode.setResponse(createParentChildRelations(createData(),orderBy,dirType));
			exceptionCode.setResponse(createParentChildRelations(lFileList, orderBy, dirType));
		} catch (FileNotFoundException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download",
					"");
			throw new RuntimeCustomException(exceptionCode);
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download",
					"");
			throw new RuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download","");
			throw new RuntimeCustomException(exceptionCode);
		} finally {
			if (telnetConnection != null && telnetConnection.isConnected()) {
				telnetConnection.disconnect();
				telnetConnection = null;
			}
		}
		return exceptionCode;
	}

	public ExceptionCode listFilesFromSFtpServer(int dirType, String orderBy) {
		ExceptionCode exceptionCode = null;
		try {
			setUploadDownloadDirFromDB();
			JSch jsch = new JSch();
			// jsch.setKnownHosts( getKnownHostsFileName() );
			Session session = jsch.getSession(getUserName(), getHostName());
			{ // "interactive" version // can selectively update specified known_hosts file
				// need to implement UserInfo interface
				// MyUserInfo is a swing implementation provided in
				// examples/Sftp.java in the JSch dist
				UserInfo ui = new MyUserInfo();
				session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
				session.setPassword(getPassword());
			}
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			if (dirType == DIR_TYPE_DOWNLOAD) {
				sftpChannel.cd(downloadDir); // downloadDir
			} else {
				sftpChannel.cd(buildLogsDir);
			}
			Vector<ChannelSftp.LsEntry> vtc = sftpChannel.ls("*.*");
			sftpChannel.disconnect();
			session.disconnect();
			List<FileInfoVb> lFileList = new ArrayList<FileInfoVb>();
			FTPClientConfig conf = new FTPClientConfig(getServerType());
			conf.setServerTimeZoneId(getTimezoneId());
			UnixFTPEntryParser unixFtpEntryParser = new UnixFTPEntryParser(conf);
			List<FTPFile> lfiles = new ArrayList<FTPFile>(vtc.size());
			for (ChannelSftp.LsEntry lsEntry : vtc) {
				FTPFile file = unixFtpEntryParser.parseFTPEntry(lsEntry.getLongname());
				if (file != null)
					lfiles.add(file);
			}
			for (FTPFile fileName : lfiles) {
				if (fileName.getName().endsWith(".zip") || fileName.getName().endsWith(".tar"))
					continue;
				FileInfoVb fileInfoVb = new FileInfoVb();
				fileInfoVb.setName(fileName.getName());
				fileInfoVb.setSize(formatFileSize(fileName.getSize()));
				/*
				 * if(dirType == 1){ fileInfoVb.setDate(formatDate(fileName));
				 * lFileList.add(fileInfoVb); }
				 */
				if (dirType == 1 && (fileInfoVb.getName().startsWith("VISION_UPLOAD")
						&& StringUtils.countOccurrencesOf(fileInfoVb.getName(), "_") > 0)) {
					Calendar cal = new GregorianCalendar();
					int month = cal.get(Calendar.MONTH) + 1;
					int year = cal.get(Calendar.YEAR);
					int day = cal.get(Calendar.DAY_OF_MONTH);
					fileInfoVb.setDate(CommonUtils.getFixedLength(String.valueOf(day), "0", 2) + "-"
							+ CommonUtils.getFixedLength(String.valueOf(month), "0", 2) + "-" + year);
					lFileList.add(fileInfoVb);
				} else if (dirType == 2 && (fileInfoVb.getName().startsWith("BUILDCRON")
						&& StringUtils.countOccurrencesOf(fileInfoVb.getName(), "-") <= 0)) {
					Calendar cal = new GregorianCalendar();
					int month = cal.get(Calendar.MONTH) + 1;
					int year = cal.get(Calendar.YEAR);
					int day = cal.get(Calendar.DAY_OF_MONTH);
					fileInfoVb.setDate(CommonUtils.getFixedLength(String.valueOf(day), "0", 2) + "-"
							+ CommonUtils.getFixedLength(String.valueOf(month), "0", 2) + "-" + year);
					lFileList.add(fileInfoVb);
				} else if ((dirType == 2 || dirType == 1)
						&& (StringUtils.countOccurrencesOf(fileInfoVb.getName(), "-") > 0)) {
					fileInfoVb.setDate(formatDate1(fileName));
					lFileList.add(fileInfoVb);
				}
			}
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Download");
			// exceptionCode = CommonUtils.getResultObject(SERVICE_NAME,
			// Constants.SUCCESSFUL_OPERATION, "Download", "");
			exceptionCode.setResponse(createParentChildRelations(lFileList, orderBy, dirType));
		} catch (FileNotFoundException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download",
					"");
			throw new RuntimeCustomException(exceptionCode);
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download",
					"");
			throw new RuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download",
					"");
			throw new RuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	private String formatFileSize(long numSize) {
		String strReturn;
		BigDecimal lSize = BigDecimal.valueOf((numSize)).divide(BigDecimal.valueOf(1024), 1, BigDecimal.ROUND_HALF_UP);
		if (lSize.floatValue() <= 0) {
			lSize = BigDecimal.valueOf(1);
		}
		strReturn = lSize + " KB";
		if (lSize.floatValue() > 1024) {
			lSize = lSize.divide(BigDecimal.valueOf(1024), 1, BigDecimal.ROUND_HALF_UP);
			strReturn = lSize + " MB";
			if (lSize.floatValue() > 1024) {
				lSize = lSize.divide(BigDecimal.valueOf(1024), 1, BigDecimal.ROUND_HALF_UP);
				strReturn = lSize + " GB";
			}
		}
		return strReturn;
	}

	private String formatDate1(FTPFile fileName) {
		String fileName1 = fileName.getName();
		String year = fileName1.substring(fileName1.length() - 14, fileName1.length() - 10);
		String month = fileName1.substring(fileName1.length() - 9, fileName1.length() - 7);
		String day = fileName1.substring(fileName1.length() - 6, fileName1.length() - 4);
		return CommonUtils.getFixedLength(day, "0", 2) + "-" + CommonUtils.getFixedLength(month, "0", 2) + "-" + year;
	}

	private FTPClient getConnection() throws IOException {
		FTPClient ftpClient = new FTPClient();
		FTPClientConfig conf = new FTPClientConfig("UNIX");
		conf.setServerTimeZoneId("Asia/Dubai");
		ftpClient.configure(conf);
		return ftpClient;
	}

	public void setUploadDownloadDirFromDB() {
		String uploadLogFilePathFromDB = getVisionVariableValue("VISION_XLUPL_LOG_FILE_PATH");
		if (uploadLogFilePathFromDB != null && !uploadLogFilePathFromDB.isEmpty()) {
			downloadDir = uploadLogFilePathFromDB;
		}
		String uploadDataFilePathFromDB = getVisionVariableValue("VISION_XLUPL_DATA_FILE_PATH");
		if (uploadDataFilePathFromDB != null && !uploadDataFilePathFromDB.isEmpty()) {
			uploadDir = uploadDataFilePathFromDB;
		}
		String buildLogsFilePathFromDB = getVisionVariableValue("BUILDCRON_LOG_FILE_PATH");
		if (buildLogsFilePathFromDB != null && !buildLogsFilePathFromDB.isEmpty()) {
			buildLogsDir = buildLogsFilePathFromDB;
		}
		String scriptPathFromDB = getVisionVariableValue("BUILDCRON_SCRIPTS_PATH");
		if (scriptPathFromDB != null && !scriptPathFromDB.isEmpty()) {
			scriptDir = scriptPathFromDB;
		}
		String uploadFileChkIntervelFromDB = getVisionVariableValue("VISION_XLUPL_FILE_CHK_INTVL");
		if (uploadFileChkIntervelFromDB != null && !uploadFileChkIntervelFromDB.isEmpty()) {
			if (ValidationUtil.isValidId(uploadFileChkIntervelFromDB))
				uploadFileChkIntervel = Integer.valueOf(uploadFileChkIntervelFromDB);
		}
		String uploadDataFilePathFromCB = getVisionVariableValue("VISION_XLCB_DATA_FILE_PATH");
		if (uploadDataFilePathFromCB != null && !uploadDataFilePathFromCB.isEmpty()) {
			cbDir = uploadDataFilePathFromCB;
		}
		String excelUploadPathDB = getVisionVariableValue("VISION_XLUPL_PATH");
		if (excelUploadPathDB != null && !excelUploadPathDB.isEmpty()) {
			uploadDirExl = excelUploadPathDB;
		}
		String connectorUploadPathDB = getVisionVariableValue("VISION_CONNECTOR_DATA_FILE_PATH");
//		connectorUploadPathDB="C:\\connectorsData\\";
		if (connectorUploadPathDB != null && !connectorUploadPathDB.isEmpty()) {
			connectorUploadDir = connectorUploadPathDB;
		}
		String connectorADUploadPathDB = getVisionVariableValue("VISION_CONNECTOR_AD_DATA_FILE_PATH");
//		connectorADUploadPathDB="C:\\connectorsDataAD\\";
		if (connectorADUploadPathDB != null && !connectorADUploadPathDB.isEmpty()) {
			connectorADUploadDir = connectorADUploadPathDB;
		}
	}

	public ExceptionCode insertRecord(ExceptionCode pRequestCode, List<VisionUploadVb> vObjects) {
		ExceptionCode exceptionCode = null;
		DeepCopy<VisionUploadVb> deepCopy = new DeepCopy<VisionUploadVb>();
		List<VisionUploadVb> clonedObject = null;
		VisionUploadVb vObject = null;
		try {
			setAtNtValues(vObjects);
			vObject = (VisionUploadVb) pRequestCode.getOtherInfo();
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copyCollection(vObjects);
			doFormateData(vObjects);
			// exceptionCode = doValidate(vObject, vObjects);
			if (exceptionCode != null && exceptionCode.getErrorMsg() != "") {
				return exceptionCode;
			}
			exceptionCode = getScreenDao().doInsertRecord(vObjects);
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(vObjects);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				rex.printStackTrace();
				logger.error("Insert Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setResponse(clonedObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}
	}

	private BufferedInputStream downloadFilesFromFTP(String pFileNames, int dirType) {
		BufferedInputStream bufferedInputStream = null;
		TelnetConnection telnetConnection = null;
		String[] fileNames = pFileNames.split(" @- ");
		setUploadDownloadDirFromDB();
		FTPClient ftpClient = null;
		try {
			ftpClient = getConnection();
			ftpClient.connect(getHostName());
			boolean response = ftpClient.login(getUserName(), getPassword());
			if (!response) {
				ftpClient.disconnect();
				throw new FTPConnectionClosedException("Unable to connect to Remote Computer");
			}
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			if (dirType == DIR_TYPE_DOWNLOAD)
				response = ftpClient.changeWorkingDirectory(downloadDir);
			else
				response = ftpClient.changeWorkingDirectory(buildLogsDir);
			if (!response) {
				ftpClient.disconnect();
				throw new FTPConnectionClosedException("Unable to connect to Remote Computer");
			}
			if (fileNames.length == 1) {
				bufferedInputStream = new BufferedInputStream(ftpClient.retrieveFileStream(fileNames[0]));
			} else if (fileNames.length > 1) {
				telnetConnection = new TelnetConnection(hostName, userName, password, prompt);
				telnetConnection.connect();
				if (dirType == DIR_TYPE_DOWNLOAD)
					telnetConnection.sendCommand("cd " + downloadDir);
				else
					telnetConnection.sendCommand("cd " + buildLogsDir);
				// for(int i=0;i<fileNames.length;i++){
				for (int i = 0; i < fileNames.length; i++) {
					telnetConnection.sendCommand("echo " + fileNames[i] + " >> example.txt");
				}
				telnetConnection.sendCommand("tar cvf logs.tar `cat example.txt`");
				telnetConnection.sendCommand("rm example.txt");
				ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
				if (dirType == DIR_TYPE_DOWNLOAD)
					response = ftpClient.changeWorkingDirectory(downloadDir);
				else
					response = ftpClient.changeWorkingDirectory(buildLogsDir);
				if (!response) {
					ftpClient.disconnect();
					throw new FTPConnectionClosedException();
				}
				bufferedInputStream = new BufferedInputStream(ftpClient.retrieveFileStream("logs.tar"));
			} else {
				throw new FileNotFoundException("File not found on the Server.");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		} finally {
			if (ftpClient != null && ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
				ftpClient = null;
			}
			if (telnetConnection != null && telnetConnection.isConnected()) {
				telnetConnection.disconnect();
			}
		}
		return bufferedInputStream;
	}

	public BufferedInputStream downloadFilesFromSFTP(String pFileNames, int dirType) {
		BufferedInputStream bufferedInputStream = null;
		String[] fileNames = pFileNames.split(" @- ");
		setUploadDownloadDirFromDB();
		Session session = null;
		try {
			JSch jsch = new JSch();
			// jsch.setKnownHosts(getKnownHostsFileName());
			session = jsch.getSession(getUserName(), getHostName());
			{ // "interactive" version // can selectively update specified known_hosts file
				// need to implement UserInfo interface
				// MyUserInfo is a swing implementation provided in
				// examples/Sftp.java in the JSch dist
				UserInfo ui = new MyUserInfo();
				session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
				session.setPassword(getPassword());
			}
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			if (dirType == DIR_TYPE_DOWNLOAD)
				sftpChannel.cd(downloadDir);
			else
				sftpChannel.cd(buildLogsDir);
			if (fileNames.length == 1) {
				try {
					InputStream ins = sftpChannel.get(fileNames[0]);
					bufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				} catch (SftpException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
			} else if (fileNames.length > 1) {
				channel = session.openChannel("shell");
				OutputStream inputstream_for_the_channel = channel.getOutputStream();
				PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
				channel.connect();
				if (dirType == DIR_TYPE_DOWNLOAD)
					commander.println("cd " + downloadDir + "\n");
				else
					commander.println("cd " + buildLogsDir + "\n");
				// for(int i=0;i<fileNames.length;i++){
				for (int i = 0; i < fileNames.length; i++) {
					commander.println("echo " + fileNames[i] + " >> example.txt" + "\n");
				}
				commander.println("tar cvf logs.tar `cat example.txt`" + "\n");
				commander.println("rm example.txt");
				commander.println("exit");
				commander.close();
				do {
					Thread.sleep(1000);
				} while (!channel.isEOF());
				try {
					InputStream ins = sftpChannel.get("logs.tar");
					bufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				} catch (SftpException ex) {
				}
			} else {
				throw new FileNotFoundException("File not found on the Server.");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		} finally {
			if (session != null && session.isConnected()) {
				session.disconnect();
			}
		}
		return bufferedInputStream;
	}

	protected ExceptionCode doValidate(VisionUploadVb pObject, List<VisionUploadVb> vObjects) {
		ExceptionCode exceptionCode = null;
		long currentUser = CustomContextHolder.getContext().getVisionId();
		FTPClient ftpClient;
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				return doValidateSFtp(pObject, vObjects);
			}
			ftpClient = getConnection();
			ftpClient.connect(getHostName());
			boolean response = ftpClient.login(getUserName(), getPassword());
			if (!response) {
				ftpClient.disconnect();
				exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Add",
						"");
				throw new RuntimeCustomException(exceptionCode);
			}
			ftpClient.setFileType(fileType);
			response = ftpClient.changeWorkingDirectory(uploadDir);
			if (!response) {
				ftpClient.disconnect();
				exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Add",
						"");
				throw new RuntimeCustomException(exceptionCode);
			}
			long maxIntervel = 0;
			for (VisionUploadVb lVUploadVb : vObjects) {
				if (lVUploadVb.isChecked()) {
					setVerifReqDeleteType(lVUploadVb);
					int countOfUplTables = getVisionUploadDao().getCountOfUploadTables(lVUploadVb);
					lVUploadVb.setMaker(currentUser);
					if (countOfUplTables <= 0) {
						String strErrorDesc = "No sufficient privileges to upload for the Table["
								+ lVUploadVb.getTableName() + "]";
						exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.WE_HAVE_ERROR_DESCRIPTION,
								"Insert", strErrorDesc);
						throw new RuntimeCustomException(exceptionCode);
					}
					FTPFile file = isFileExists(lVUploadVb.getFileName().toUpperCase() + "_" + currentUser + ".txt",
							ftpClient);
					if (file == null) {
						String strErrorDesc = lVUploadVb.getFileName()
								+ ".txt does not exists. Please upload the file first.";
						exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.WE_HAVE_ERROR_DESCRIPTION,
								"Insert", strErrorDesc);
						throw new RuntimeCustomException(exceptionCode);
					}
					maxIntervel = Math.max(
							(((System.currentTimeMillis() / 1000) - file.getTimestamp().getTimeInMillis()) / 60),
							maxIntervel);
				}
			}
			if (maxIntervel >= uploadFileChkIntervel && pObject.getCheckUploadInterval() == false) {
				String strErrorDesc = "Upload file(s) is more than " + uploadFileChkIntervel
						+ " mins old. Do you want to continue with upload?";
				exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.WE_HAVE_ERROR_DESCRIPTION, "Insert",
						strErrorDesc);
				throw new RuntimeCustomException(exceptionCode);
			}
			ftpClient.disconnect();
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Add", "");
			throw new RuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	protected ExceptionCode doValidateSFtp(VisionUploadVb pObject, List<VisionUploadVb> vObjects) {
		ExceptionCode exceptionCode = null;
		long currentUser = CustomContextHolder.getContext().getVisionId();
		try {
			JSch jsch = new JSch();
			jsch.setKnownHosts(getKnownHostsFileName());
			Session session = jsch.getSession(getUserName(), getHostName());
			{ // "interactive" version // can selectively update specified known_hosts file
				// need to implement UserInfo interface
				// MyUserInfo is a swing implementation provided in
				// examples/Sftp.java in the JSch dist
				UserInfo ui = new MyUserInfo();
				session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
				session.setPassword(getPassword());
			}
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			sftpChannel.cd(uploadDir);
			long maxIntervel = 0;
			for (VisionUploadVb lVUploadVb : vObjects) {
				if (lVUploadVb.isChecked()) {
					setVerifReqDeleteType(lVUploadVb);
					int countOfUplTables = getVisionUploadDao().getCountOfUploadTables(lVUploadVb);
					lVUploadVb.setMaker(currentUser);
					if (countOfUplTables <= 0) {
						String strErrorDesc = "No sufficient privileges to upload for the Table["
								+ lVUploadVb.getTableName() + "]";
						exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.WE_HAVE_ERROR_DESCRIPTION,
								"Insert", strErrorDesc);
						throw new RuntimeCustomException(exceptionCode);
					}
					ChannelSftp.LsEntry entry = isFileExists(
							lVUploadVb.getFileName().toUpperCase() + "_" + currentUser + ".txt", sftpChannel);
					if (entry == null) {
						String strErrorDesc = lVUploadVb.getFileName()
								+ ".txt does not exists. Please upload the file first.";
						exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.WE_HAVE_ERROR_DESCRIPTION,
								"Insert", strErrorDesc);
						throw new RuntimeCustomException(exceptionCode);
					}
					maxIntervel = Math.max((((System.currentTimeMillis() / 1000) - entry.getAttrs().getMTime()) / 60),
							maxIntervel);
				}
			}
			if (maxIntervel >= uploadFileChkIntervel && pObject.getCheckUploadInterval() == false) {
				String strErrorDesc = "Upload file(s) is more than " + uploadFileChkIntervel
						+ " mins old. Do you want to continue with upload?";
				exceptionCode = new ExceptionCode();
				exceptionCode.setErrorSevr("W");
				exceptionCode.setErrorCode(Constants.WE_HAVE_ERROR_DESCRIPTION);
				exceptionCode.setErrorMsg(strErrorDesc);
				throw new RuntimeCustomException(exceptionCode);
			}
			sftpChannel.disconnect();
			session.disconnect();
		} catch (SftpException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Add", "");
			throw new RuntimeCustomException(exceptionCode);
		} catch (JSchException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Add", "");
			throw new RuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	private List<FileInfoVb> createParentChildRelations(List<FileInfoVb> legalTreeList, String orderBy, int dirType)
			throws IOException {
		List<FileInfoVb> lResult = new ArrayList<FileInfoVb>(0);
		Set set = new HashSet<FileInfoVb>();
		// Top Roots are added.
		for (FileInfoVb fileVb : legalTreeList) {
			if ("Date".equalsIgnoreCase(orderBy)) {
				// String date = fileVb.getName().substring(fileVb.getName().length()-14,
				// fileVb.getName().length()-4);
				if (fileVb.getDate() != null && fileVb.getDate() != "") {
					set.add(fileVb.getDate());
				}
			} else {
				String fileNeme = "";
				if (dirType == 2) {
					int cout = StringUtils.countOccurrencesOf(fileVb.getName(), "_");
					if (cout == 0) {
						int cout1 = StringUtils.countOccurrencesOf(fileVb.getName(), "-");
						if (cout1 == 0)
							fileNeme = fileVb.getName().substring(0, fileVb.getName().length() - 4);
						else
							fileNeme = fileVb.getName().substring(0, fileVb.getName().length() - 15);
					} else if (cout > 1)
						fileNeme = fileVb.getName().substring(0, fileVb.getName().length() - 21);
				} else {
					int cout = StringUtils.countOccurrencesOf(fileVb.getName(), "_");
					if (cout == 1) {
						fileNeme = fileVb.getName().substring(0, fileVb.getName().length() - 4);
					} else if (cout > 1) {
						fileNeme = fileVb.getName().substring(0, fileVb.getName().length() - 20);
					}
				}
				if (fileNeme != null && fileNeme.length() > 0) {
					set.add(fileNeme);
				}
			}
		}
		for (Iterator iterator = set.iterator(); iterator.hasNext();) {
			String date = (String) iterator.next();
			FileInfoVb fileVb = new FileInfoVb();
			fileVb.setDescription(date);
			lResult.add(fileVb);
		}
		// For each top node add all child's and to that child's add sub child's
		// recursively.
		for (FileInfoVb legalVb : lResult) {
			addChilds(legalVb, legalTreeList, orderBy, dirType);
		}
		if ("Date".equalsIgnoreCase(orderBy)) {
			final SimpleDateFormat dtF = new SimpleDateFormat("dd-MM-yyyy");
			// set the empty lists to null. this is required for UI to display the leaf
			// nodes properly.
			Collections.sort(lResult, new Comparator<FileInfoVb>() {
				public int compare(FileInfoVb m1, FileInfoVb m2) {
					try {
						return dtF.parse(m1.getDescription()).compareTo(dtF.parse(m2.getDescription()));
					} catch (ParseException e) {
						return 0;
					}
				}
			});
			Collections.reverse(lResult);
		} else {
			Collections.sort(lResult, new Comparator<FileInfoVb>() {
				public int compare(FileInfoVb m1, FileInfoVb m2) {
					return m1.getDescription().compareTo(m2.getDescription());
				}
			});
		}
		return lResult;
	}

	public void addChilds(FileInfoVb vObject, List<FileInfoVb> legalTreeListCopy, String orderBy, int dirType) {
		for (FileInfoVb fileTreeVb : legalTreeListCopy) {
			if ("Date".equalsIgnoreCase(orderBy)) {
				// String date =
				// fileTreeVb.getName().substring(fileTreeVb.getName().length()-14,
				// fileTreeVb.getName().length()-4);
				if (vObject.getDescription().equalsIgnoreCase(fileTreeVb.getDate())) {
					if (vObject.getChildren() == null) {
						vObject.setChildren(new ArrayList<FileInfoVb>(0));
					}
					fileTreeVb.setDescription(fileTreeVb.getName());
					vObject.getChildren().add(fileTreeVb);
				}
			} else {
				String fileNeme = "";
				if (dirType == 2) {
					int cout = StringUtils.countOccurrencesOf(fileTreeVb.getName(), "_");
					if (cout == 0) {
						int cout1 = StringUtils.countOccurrencesOf(fileTreeVb.getName(), "-");
						if (cout1 == 0)
							fileNeme = fileTreeVb.getName().substring(0, fileTreeVb.getName().length() - 4);
						else
							fileNeme = fileTreeVb.getName().substring(0, fileTreeVb.getName().length() - 15);
					} else if (cout > 1)
						fileNeme = fileTreeVb.getName().substring(0, fileTreeVb.getName().length() - 21);
				} else {
					int cout = StringUtils.countOccurrencesOf(fileTreeVb.getName(), "_");
					if (cout == 1) {
						fileNeme = fileTreeVb.getName().substring(0, fileTreeVb.getName().length() - 4);
					} else if (cout > 1) {
						fileNeme = fileTreeVb.getName().substring(0, fileTreeVb.getName().length() - 20);
					}
				}
				if (vObject.getDescription().equalsIgnoreCase(fileNeme)) {
					if (vObject.getChildren() == null) {
						vObject.setChildren(new ArrayList<FileInfoVb>(0));
					}
					fileTreeVb.setDescription(fileTreeVb.getName());
					vObject.getChildren().add(fileTreeVb);
				}
			}
		}
	}

	public BufferedInputStream getLogFile(FileInfoVb fileInfoVb) {
		FTPClient ftpClient = null;
		BufferedInputStream input = null;
		try {
			setUploadDownloadDirFromDB();
			if ("Y".equalsIgnoreCase(securedFtp)) {
				return getLogFileFromSFTP(fileInfoVb);
			}
			ftpClient = getConnection();
			ftpClient.connect(getHostName());
			boolean response = ftpClient.login(getUserName(), getPassword());
			if (!response) {
				// logger.error("Unable to login to the FTP Server.");
				return null;
			}
			response = ftpClient.changeWorkingDirectory(buildLogsDir);
			if (!response) {
				// logger.error("Unable to login to the FTP Server.");
				return null;
			}
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			input = new BufferedInputStream(ftpClient.retrieveFileStream(fileInfoVb.getName()));
			if (input == null) {
				input = new BufferedInputStream(ftpClient.retrieveFileStream("BUILDCRON.log"));
				fileInfoVb.setName("BUILDCRON.log");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Unable to login to the FTP Server.");
				e.printStackTrace();
			}
			return null;
		} finally {
			if (ftpClient != null)
				try {
					ftpClient.disconnect();
				} catch (IOException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
		}
		return input;
	}

	public BufferedInputStream getLogFileFromSFTP(FileInfoVb fileInfoVb) {
		BufferedInputStream input = null;
		Session session = null;
		try {
			setUploadDownloadDirFromDB();
			JSch jsch = new JSch();
			jsch.setKnownHosts(getKnownHostsFileName());
			session = jsch.getSession(getUserName(), getHostName());
			{ // "interactive" version // can selectively update specified known_hosts file
				// need to implement UserInfo interface
				// MyUserInfo is a swing implementation provided in
				// examples/Sftp.java in the JSch dist
				UserInfo ui = new MyUserInfo();
				session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
				session.setPassword(getPassword());
			}
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			sftpChannel.cd(buildLogsDir);
			try {
				InputStream ins = sftpChannel.get(fileInfoVb.getName());
				input = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
			} catch (SftpException ex) {
				try {
					if (input == null) {
						InputStream ins = sftpChannel.get("BUILDCRON.log");
						input = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
						fileInfoVb.setName("BUILDCRON.log");
					}
				} catch (SftpException ex1) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						ex1.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			logger.error("Unable to login to the FTP Server.");
			return null;
		} finally {
			if (session != null)
				session.disconnect();
		}
		return input;
	}

	public BufferedInputStream getLogFilesFromSFTP(FileInfoVb fileInfoVb, String command) {
		BufferedInputStream input = null;
		Session session = null;
		Channel shellChannel = null;
		try {
			setUploadDownloadDirFromDB();
			JSch jsch = new JSch();
			jsch.setKnownHosts(getKnownHostsFileName());
			session = jsch.getSession(getUserName(), getHostName());
			{ // "interactive" version // can selectively update specified known_hosts file
				// need to implement UserInfo interface
				// MyUserInfo is a swing implementation provided in
				// examples/Sftp.java in the JSch dist
				UserInfo ui = new MyUserInfo();
				session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
				session.setPassword(getPassword());
			}
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();

			shellChannel = session.openChannel("shell");
			OutputStream inputstream_for_the_channel = shellChannel.getOutputStream();
			PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
			shellChannel.connect();
			commander.println("cd " + buildLogsDir);
			commander.println("tar -cvf " + fileInfoVb.getName() + " " + command);
			commander.println("exit");
			commander.close();
			do {
				Thread.sleep(1000);
			} while (!shellChannel.isEOF());
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			sftpChannel.cd(buildLogsDir);
			try {
				InputStream ins = sftpChannel.get(fileInfoVb.getName());
				input = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
			} catch (SftpException exp) {
				try {
					InputStream ins = sftpChannel.get("BUILDCRON.log");
					input = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
					fileInfoVb.setName("BUILDCRON.log");
				} catch (SftpException ex) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						ex.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			logger.error("Unable to login to the FTP Server.");
			return null;
		} finally {
			if (session != null)
				session.disconnect();
		}
		return input;
	}

	public BufferedInputStream getLogFiles(FileInfoVb fileInfoVb, String command) {
		FTPClient ftpClient = null;
		TelnetConnection telnetConnection = null;
		BufferedInputStream input = null;
		try {
			setUploadDownloadDirFromDB();
			if ("Y".equalsIgnoreCase(securedFtp)) {
				return getLogFilesFromSFTP(fileInfoVb, command);
			}
			telnetConnection = new TelnetConnection(hostName, userName, password, prompt);
			telnetConnection.connect();
			telnetConnection.sendCommand("cd " + buildLogsDir);
			telnetConnection.sendCommand("tar -cvf " + fileInfoVb.getName() + " " + command);
			ftpClient = getConnection();
			ftpClient.connect(getHostName());
			boolean response = ftpClient.login(getUserName(), getPassword());
			if (!response) {
				// logger.error("Unable to login to the FTP Server.");
				return null;
			}
			response = ftpClient.changeWorkingDirectory(buildLogsDir);
			if (!response) {
				// logger.error("Unable to login to the FTP Server.");
				return null;
			}
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			input = new BufferedInputStream(ftpClient.retrieveFileStream(fileInfoVb.getName()));
			if (input == null) {
				input = new BufferedInputStream(ftpClient.retrieveFileStream("BUILDCRON.log"));
				fileInfoVb.setName("BUILDCRON.log");
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		} finally {
			if (ftpClient != null)
				try {
					ftpClient.disconnect();
				} catch (IOException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
			if (telnetConnection != null && telnetConnection.isConnected()) {
				telnetConnection.disconnect();
			}
		}
		return input;
	}

	private FTPFile isFileExists(String strFileName, FTPClient ftpClient) throws IOException {
		FTPFile[] tmp = ftpClient.listFiles(strFileName);
		if (tmp != null && tmp.length > 0)
			return tmp[0];
		return null;
	}

	private ChannelSftp.LsEntry isFileExists(String strFileName, ChannelSftp ftpClient) throws SftpException {
		try {
			Vector<ChannelSftp.LsEntry> lvec = ftpClient.ls(strFileName);
			return (lvec != null && lvec.size() > 0) ? lvec.get(0) : null;
		} catch (SftpException e) {
		}
		return null;
	}

	@Override
	protected AbstractDao<VisionUploadVb> getScreenDao() {
		return visionUploadDao;
	}

	@Override
	protected void setAtNtValues(VisionUploadVb object) {
		object.setUploadStatusNt(10);
	}

	@Override
	protected void setVerifReqDeleteType(VisionUploadVb object) {
		object.setVerificationRequired(false);
		object.setStaticDelete(false);
	}

	public ExceptionCode doTemplateUpload(String fileName, byte[] data) {
		ExceptionCode exceptionCode = null;
		File lfile = null;
		FileChannel source = null;
		FileChannel destination = null;
		try {
			setUploadDownloadDirFromDB();
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				jsch.setKnownHosts(getKnownHostsFileName());
				Session session = jsch.getSession(getUserName(), getHostName());
				{ // "interactive" version // can selectively update specified known_hosts file
					// need to implement UserInfo interface
					// MyUserInfo is a swing implementation provided in
					// examples/Sftp.java in the JSch dist
					UserInfo ui = new MyUserInfo();
					session.setUserInfo(ui); // OR non-interactive version. Relies in host key being in known-hosts file
					session.setPassword(getPassword());
				}
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				Channel channel = session.openChannel("sftp");
				channel.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channel;
				fileName = fileName.toUpperCase();
				sftpChannel.cd(cbDir);
				InputStream in = new ByteArrayInputStream(data);
				sftpChannel.put(in, fileName);
				sftpChannel.exit();
				channel = session.openChannel("shell");
				OutputStream inputstream_for_the_channel = channel.getOutputStream();
				PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
				channel.connect();
				commander.println("cd " + scriptDir);
				StringBuilder cmd = new StringBuilder();
				commander.println(cmd);
				commander.println("exit");
				commander.close();
				do {
					Thread.sleep(1000);
				} while (!channel.isEOF());
				session.disconnect();
			} else {
				FTPClient ftpClient = getConnection();
				ftpClient.connect(getHostName());
				boolean response = ftpClient.login(getUserName(), getPassword());
				if (!response) {
					ftpClient.disconnect();
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR,
							"Upload", "");
					;
					exceptionCode.setResponse(fileName);
					throw new RuntimeCustomException(exceptionCode);
				}
				ftpClient.setFileType(fileType);
				response = ftpClient.changeWorkingDirectory(cbDir);
				if (!response) {
					ftpClient.disconnect();
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR,
							"Upload", "");
					;
					exceptionCode.setResponse(fileName);
					throw new RuntimeCustomException(exceptionCode);
				}
				fileName = fileName.toUpperCase();
				fileName = fileName.substring(0, fileName.lastIndexOf('.')) + "_"
						+ CustomContextHolder.getContext().getVisionId() + "." + getFileExtension() + "";
				InputStream in = new ByteArrayInputStream(data);
				ftpClient.storeFile(fileName, in);
				in.close(); // close the io streams
				ftpClient.disconnect();
			}
		} catch (FileNotFoundException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		}
		exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
		exceptionCode.setOtherInfo(fileName);
		return exceptionCode;
	}

	public long getDateTimeInMS(String date, String formate) {
		SimpleDateFormat lFormat = new SimpleDateFormat(formate);
		try {
			Date lDate = lFormat.parse(date);
			return lDate.getTime();
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return System.currentTimeMillis();
		}
	}

	public ExceptionCode uploadMultipartFile(MultipartFile[] files, DSConnectorVb lsData, String macroVar,
			String curDate) throws IOException {
		ExceptionCode exceptionCode = null;
		FileInfoVb fileInfoVb = new FileInfoVb();
		try {
			for (MultipartFile uploadedFile : files) {
				String extension = FilenameUtils.getExtension(uploadedFile.getOriginalFilename());
				fileInfoVb.setName(uploadedFile.getOriginalFilename().toUpperCase());
				fileInfoVb.setData(uploadedFile.getBytes());
				fileInfoVb.setExtension(extension);
				fileInfoVb.setDate(curDate);
				exceptionCode = doDataConnectorUpload(fileInfoVb, lsData, macroVar);
			}
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				rex.printStackTrace();
				logger.error("Error in upload" + rex.getCode().getErrorMsg());
				logger.error(((fileInfoVb == null) ? "vObject is Null" : fileInfoVb.toString()));
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileInfoVb);
			throw new RuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	public ExceptionCode moveConnectorFileToAD(DSConnectorVb lsData) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setUploadDownloadDirFromDB();
			if (lsData != null && ValidationUtil.isValid(lsData.getMacroVarScript())) {/* Move File to AD */
				String upfileName = CommonUtils.getValue(lsData.getMacroVarScript(), "NAME");
				String extension = CommonUtils.getValue(lsData.getMacroVarScript(), "EXTENSION");
				long lCurrentDate = getDateTimeInMS(lsData.getDateLastModified(), "dd-M-yyyy hh:mm:ss");
				String fileName = upfileName + "_" + lsData.getMaker() + "_" + lsData.getMacroVar() + "_" + lCurrentDate
						+ "." + extension;
				File lFile = new File(getConnectorUploadDir() + fileName);
				if (lFile.exists()) {
					if (lFile.renameTo(new File(getConnectorADUploadDir() + fileName))) {
						lFile.delete();
					} else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Problem during backup process");
						throw new RuntimeCustomException(exceptionCode);
					}
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}

	public ExceptionCode doDataConnectorUpload(FileInfoVb fileInfoVb, DSConnectorVb lsData, String macroVar) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String fileName = fileInfoVb.getName();
		try {
			setUploadDownloadDirFromDB();
			/* Move File to AD */
			moveConnectorFileToAD(lsData);
			long lCurrentDate = getDateTimeInMS(fileInfoVb.getDate(), "dd-M-yyyy hh:mm:ss");
			fileName = fileInfoVb.getName().substring(0, fileInfoVb.getName().lastIndexOf('.')) + "_"
					+ CustomContextHolder.getContext().getVisionId() + "_" + macroVar + "_" + lCurrentDate + "."
					+ fileInfoVb.getExtension();
			File lFile = new File(getConnectorUploadDir() + fileName);
			if (lFile.exists()) {
				if (lFile.renameTo(new File(getConnectorADUploadDir() + fileName))) {
					lFile.delete();
				} else {
					// System.out.println("Problem during backup process");
				}
			}
			lFile.createNewFile();
			FileOutputStream fileOuputStream = null;
			fileOuputStream = new FileOutputStream(lFile);
			fileOuputStream.write(fileInfoVb.getData());
			fileOuputStream.flush();
			fileOuputStream.close();
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
			exceptionCode.setOtherInfo(fileInfoVb);
			return exceptionCode;
		} catch (FileNotFoundException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		}
	}

	public ExceptionCode doDataConnectorUploadOld(FileInfoVb fileInfoVb, List<DSConnectorVb> lsData, String macroVar) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String fileName = fileInfoVb.getName();
		String fileExtension = fileInfoVb.getExtension();
		try {
			setUploadDownloadDirFromDB();
			fileName = fileName.toUpperCase(); // Changes done by Deepak for Bank One
			if (fileName.contains(".XLSX") || fileName.contains(".XLS")) {
				fileExtension = "xlsx";
				setUserName(getXlUploaduserName());
				setPassword(getXlUploadpassword());
				setHostName(getXlUploadhostName());
				setUploadDir(getUploadDirExl());
			}

			if ("Y".equalsIgnoreCase(securedFtp)) {
				fileName = fileName.toUpperCase();
				JSch jsch = new JSch();
				// jsch.setKnownHosts(getKnownHostsFileName());
				Session session = jsch.getSession(getUserName(), getHostName());
				{
					UserInfo ui = new MyUserInfo();
					session.setUserInfo(ui);
					session.setPassword(getPassword());
				}
				java.util.Properties config = new java.util.Properties();
				config.put("StrictHostKeyChecking", "no");
				session.setConfig(config);
				session.connect();

				Channel channel = session.openChannel("sftp");
				channel = session.openChannel("shell");
				OutputStream inputstream_for_the_channel = channel.getOutputStream();
				PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
				channel.connect();
				commander.println("cd " + connectorUploadDir);
				StringBuilder cmd = new StringBuilder();

				/* Move file to AD */
				if (lsData.size() > 0) {
					String upfileName = CommonUtils.getValue(lsData.get(0).getMacroVarScript(), "NAME");
					String extension = CommonUtils.getValue(lsData.get(0).getMacroVarScript(), "EXTENSION");
					long lCurrentDate = getDateTimeInMS(lsData.get(0).getDateLastModified(), "dd-M-yyyy hh:mm:ss");
					String sourceFile = upfileName.toUpperCase() + "_" + lsData.get(0).getMaker() + "_"
							+ lsData.get(0).getMacroVar() + "_" + lCurrentDate + "." + extension;

					if ('/' != connectorUploadDir.charAt(connectorUploadDir.length() - 1)) {
						cmd.append(" mv " + sourceFile + " " + connectorADUploadDir + "/" + sourceFile);
					} else {
						cmd.append(" mv " + sourceFile + " " + connectorADUploadDir + "/" + sourceFile);

					}
				}

				commander.println(cmd);
				commander.println("exit");
				commander.close();

				do {
					Thread.sleep(500);
				} while (!channel.isEOF());

				long lCurrentDate = getDateTimeInMS(fileInfoVb.getDate(), "dd-M-yyyy hh:mm:ss");
				fileName = fileName.substring(0, fileName.lastIndexOf('.')) + "_"
						+ CustomContextHolder.getContext().getVisionId() + "_" + macroVar + "_" + lCurrentDate + "."
						+ fileExtension + "";
				channel = session.openChannel("sftp");
				channel.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channel;
				sftpChannel.cd(connectorUploadDir);
				InputStream in = new ByteArrayInputStream(fileInfoVb.getData());
				sftpChannel.put(in, fileName);
				sftpChannel.exit();

				channel = session.openChannel("shell");
				inputstream_for_the_channel = channel.getOutputStream();
				commander = new PrintStream(inputstream_for_the_channel, true);
				channel.connect();
				commander.println("cd " + scriptDir);
				cmd = new StringBuilder();
				if ('/' != connectorUploadDir.charAt(connectorUploadDir.length() - 1)) {
					cmd.append("./remSpecialChar.sh " + connectorUploadDir + "/" + fileName);
				} else {
					cmd.append("./remSpecialChar.sh " + connectorUploadDir + fileName);
				}
				commander.println(cmd);
				commander.println("exit");
				commander.close();
				do {
					Thread.sleep(1000);
				} while (!channel.isEOF());
				session.disconnect();
			} else {
				FTPClient ftpClient = getConnection();
				ftpClient.connect(getHostName());
				boolean response = ftpClient.login(getUserName(), getPassword());
				if (!response) {
					ftpClient.disconnect();
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR,
							"Upload", "");
					exceptionCode.setResponse(fileName);
					throw new RuntimeCustomException(exceptionCode);
				}
				ftpClient.setFileType(fileType);
				response = ftpClient.changeWorkingDirectory(connectorUploadDir);
				if (!response) {
					ftpClient.disconnect();
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR,
							"Upload", "");
					exceptionCode.setResponse(fileName);
					throw new RuntimeCustomException(exceptionCode);
				}
				fileName = fileName.toUpperCase();
				fileName = fileName.substring(0, fileName.lastIndexOf('.')) + "_"
						+ CustomContextHolder.getContext().getVisionId() + "." + fileExtension + "";
				InputStream in = new ByteArrayInputStream(fileInfoVb.getData());
				ftpClient.storeFile(fileName, in);
				in.close(); // close the io streams
				ftpClient.disconnect();
			}
		} catch (FileNotFoundException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (IOException e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(fileInfoVb);
		return exceptionCode;
	}

	public ExceptionCode listFilesFromConnectors(String macroVar) {
		ExceptionCode exceptionCode = null;
		try {
			setUploadDownloadDirFromDB();
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

			File[] files = new File(connectorADUploadDir).listFiles();
			List<FileInfoVb> collTemp = new ArrayList<FileInfoVb>();
			if (ValidationUtil.isValid(files)) {
				for (File file : files) {
					String fileName = file.getName();
					if (fileName.indexOf(
							"_" + CustomContextHolder.getContext().getVisionId() + "_" + macroVar + "_") != -1) {
						FileInfoVb fileInfoVb = new FileInfoVb();
						fileInfoVb.setName(file.getName());
						fileInfoVb.setSize(formatFileSize(file.length()));
						fileInfoVb.setDate(sdf.format(file.lastModified()));
						fileInfoVb.setExtension(fileName.substring(fileName.lastIndexOf(".") + 1));
						fileInfoVb.setGroupBy("connectorADUploadDir");
						collTemp.add(fileInfoVb);
					}
				}
			}

			File[] filesData = new File(connectorUploadDir).listFiles();
			if (ValidationUtil.isValid(filesData)) {
				for (File file : filesData) {
					String fileName1 = file.getName();
					if (fileName1.indexOf(
							"_" + CustomContextHolder.getContext().getVisionId() + "_" + macroVar + "_") != -1) {
						FileInfoVb fileInfoVb = new FileInfoVb();
						fileInfoVb.setName(file.getName());
						fileInfoVb.setSize(formatFileSize(file.length()));
						fileInfoVb.setDate(sdf.format(file.lastModified()));
						fileInfoVb.setExtension(fileName1.substring(fileName1.lastIndexOf(".") + 1));
						fileInfoVb.setGroupBy("connectorUploadDir");
						collTemp.add(fileInfoVb);
					}
				}
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION,
					"List Files Success", "");
			exceptionCode.setResponse(createParentChildRelations(collTemp, "Date", 1));
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Download",
					"");
			throw new RuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	public ExceptionCode fileDownloadFromConnector(FileInfoVb fileInfoVb, String connectorDir) {
		ExceptionCode exceptionCode = null;
		File file = null;
		FileOutputStream fos = null;
		BufferedInputStream bufferedInputStream = null;
		String searchDir = "";
		try {
			String fileName = fileInfoVb.getName();
			setUploadDownloadDirFromDB();
			String filePath = System.getProperty("java.io.tmpdir");
			file = new File(filePath + fileName);
			fos = new FileOutputStream(file);
			if (connectorDir.equalsIgnoreCase("connectorUploadDir")) {
				searchDir = connectorUploadDir + fileName;
			} else {
				searchDir = connectorADUploadDir + fileName;

			}
			InputStream ins = new FileInputStream(searchDir);
			bufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));

			int bit = 4096;
			while ((bit) >= 0) {
				bit = bufferedInputStream.read();
				fos.write(bit);
			}
			bufferedInputStream.close();
			fos.close();
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Sucess", "");
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return exceptionCode;
	}

	public HttpServletResponse setExportXlsServlet(HttpServletRequest request, HttpServletResponse response) {
		try {
			exportXlsServlet.doPost(request, response);
			return response;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e.getMessage());
		}
	}

	public ExceptionCode xmlToJson(MultipartFile[] files) throws IOException {
		ExceptionCode exceptionCode = null;
		try {
			for (MultipartFile uploadedFile : files) {
				String content = new String(uploadedFile.getBytes(), "UTF-8");
				exceptionCode = CommonUtils.XmlToJson(content);
			}
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				rex.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject("xmlToJson", Constants.WE_HAVE_ERROR_DESCRIPTION,
					"XML To JSON Conversion", rex.getMessage());
			exceptionCode.setResponse("");
			return exceptionCode;
		}
		return exceptionCode;
	}

	public ExceptionCode doDeleteDataConnectorUpload(FileInfoVb fileInfoVb, DSConnectorVb lsData, String macroVar) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String fileName = fileInfoVb.getName();
		try {
			setUploadDownloadDirFromDB();
			if (lsData != null && ValidationUtil.isValid(lsData.getMacroVarScript())) {/* Move File to AD */
				String upfileName = CommonUtils.getValue(lsData.getMacroVarScript(), "NAME");
				String extension = CommonUtils.getValue(lsData.getMacroVarScript(), "EXTENSION");
				long lCurrentDate = getDateTimeInMS(lsData.getDateLastModified(), "dd-M-yyyy hh:mm:ss");
				fileName = upfileName + "_" + lsData.getMaker() + "_" + lsData.getMacroVar() + "_" + lCurrentDate + "."
						+ extension;
				File lFile = new File(getConnectorUploadDir() + fileName);
				if (lFile.exists()) {
					if (lFile.renameTo(new File(getConnectorADUploadDir() + fileName))) {
						lFile.delete();
					} else {
						// System.out.println("Problem during backup process");
					}
				}
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
			exceptionCode.setOtherInfo(fileInfoVb);
			return exceptionCode;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.FILE_UPLOAD_REMOTE_ERROR, "Upload", "");
			exceptionCode.setResponse(fileName);
			throw new RuntimeCustomException(exceptionCode);
		}
	}

	public ExceptionCode uploadStaticFileForETLConnector(MultipartFile[] files, String connectorId, String fileId) {
		Session session = null;
		Channel channelSFTP = null;
		ChannelSftp sftpChannel = null;
		String uploadeddataFileName = "";
		String exactUploadeddataFileName = "";
		ExceptionCode exceptionCode = new ExceptionCode();
		FileInfoVb fileInfoVb = new FileInfoVb();
		List<String> SheetNames = new ArrayList<>();
		try {
			String extension = "";
			for (MultipartFile uploadedFile : files) {
				extension = FilenameUtils.getExtension(uploadedFile.getOriginalFilename());
				fileInfoVb.setName(uploadedFile.getOriginalFilename().toUpperCase());
				fileInfoVb.setData(uploadedFile.getBytes());
				fileInfoVb.setExtension(extension);
			}
			
			String fileN = fileInfoVb.getName().toUpperCase();

			exactUploadeddataFileName = fileN.substring(0, fileN.lastIndexOf('.')) + "." + fileInfoVb.getExtension();
			uploadeddataFileName = fileN.substring(0, fileN.lastIndexOf('.')) + "_" + connectorId + "_" + fileId
					+ "." + fileInfoVb.getExtension();
			InputStream in = new ByteArrayInputStream(fileInfoVb.getData());
			BufferedInputStream bis = null;
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				// SFTP Session
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;
				sftpChannel.cd(connectorFeedDir);
				sftpChannel.put(in, uploadeddataFileName);
				bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(in)));
			} else {
				Path baseDirectory = Paths.get(connectorFeedDir);
				Path filePath = baseDirectory.resolve(uploadeddataFileName);
				Files.write(filePath, fileInfoVb.getData());
			    bis = new BufferedInputStream(new ByteArrayInputStream(fileInfoVb.getData()));
			}
			if (!ValidationUtil.isValid(bis)) {
				exceptionCode.setErrorMsg("Problem in fetching File Detail - File [ " + uploadeddataFileName + "]");
				return exceptionCode;
			}
			if (extension.equalsIgnoreCase("xlsx")) {
				XSSFWorkbook workbook = new XSSFWorkbook(bis);
				for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
					SheetNames.add(workbook.getSheetName(i));
				}
			} else if (extension.equalsIgnoreCase("xls")) {
				Workbook wb = WorkbookFactory.create(bis);
				for (int i = 0; i < wb.getNumberOfSheets(); i++) {
					SheetNames.add(wb.getSheetName(i));
				}
			}
			bis.close();
			in.close();
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
			exceptionCode.setOtherInfo(exactUploadeddataFileName);
			exceptionCode.setResponse(SheetNames);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setResponse(connectorId);
		} finally {
			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				try {
					if (sftpChannel != null && sftpChannel.isConnected()) {
						sftpChannel.cd(connectorFeedDir);
						sftpChannel.rm(uploadeddataFileName);
					}
				} catch (SftpException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
			}
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
				sftpChannel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		return exceptionCode;
	}

	public ExceptionCode columnMetaDataOfUploadedFile(String connectorId, String fileId, String sheetName,
			String upFileName, String viewName, List<EtlFileColumnsVb> fileColumns, String dynamicScript,
			String context) {
		Session session = null;
		Channel channelSFTP = null; 
		ExceptionCode exceptionCode = new ExceptionCode();
		List<EtlFileColumnsVb> columnsMetaDataList = new ArrayList<EtlFileColumnsVb>();
		ChannelSftp sftpChannel = null; // Local SFTP
		ChannelSftp sftpChannelSource = null;
		PrintStream commander = null;
		String fileExtension = upFileName.substring(upFileName.lastIndexOf(".") + 1);
		upFileName = upFileName.substring(0, upFileName.lastIndexOf('.')) + "_" + connectorId + "_" + fileId + "."
				+ fileExtension + "";
		String dataFileName = "";
		String inputdataFileName = "";
		String ouputdataFileName = "";
		String uploadeddataFileName = "";
		OutputStream inputstream_for_the_channel = null;
		List<EtlFileColumnsVb> columnList = new ArrayList<EtlFileColumnsVb>();
		Channel channel = null;
		String sourceDynamicFileLocation = "";
		String sourcePort = null;
		String sourceHostName = null;
		String sourceUserName = null;
		String sourcePassword = null;
		boolean sourceSecuredFtp = false;
		try {
			if (!"E".equalsIgnoreCase(dynamicScript)) {
				EtlFileUploadAreaVb vb = new EtlFileUploadAreaVb();
				vb.setConnectorId(connectorId);
				vb.setFileId(fileId);
				JSONArray array = new JSONArray(dynamicScript);
				for (int i = 0; i < array.length(); i++) {
					JSONObject object = array.getJSONObject(i);
					String tag = object.getString("tag");
					String encryption = object.getString("encryption");
					String value = object.getString("value");
					if (ValidationUtil.isValid(encryption) && encryption.equalsIgnoreCase("Yes")) {
						// Decode UTF-8 encoded value - Start
						try {
							value = URLDecoder.decode(value, "UTF-8");
						} catch (UnsupportedEncodingException e) {
							if ("Y".equalsIgnoreCase(enableDebug)) {
								e.printStackTrace();
							}
						}
						// Decode UTF-8 encoded value - End
						value = AESEncryptionDecryption.decryptPKCS7(value);
					}
					if (tag.equalsIgnoreCase("SERVER_IP")) {
						sourceHostName = value;
					} else if (tag.equalsIgnoreCase("SERVER_PORT")) {
						sourcePort = value;
					} else if (tag.equalsIgnoreCase("SERVER_USER")) {
						sourceUserName = value;
					} else if (tag.equalsIgnoreCase("SERVER_PWD")) {
						sourcePassword = value;
					} else if (tag.equalsIgnoreCase("FILE_LOCATION")) {
						sourceDynamicFileLocation = value;
					} else if (tag.equalsIgnoreCase("FTP")) {
						if("sftp".equalsIgnoreCase(value)) {
							sourceSecuredFtp = true;
						}
					}
				}
			}
			if(sourceSecuredFtp) {
				JSch jsch = new JSch();
				session = jsch.getSession(sourceUserName, sourceHostName, Integer.parseInt(sourcePort));
				session.setPassword(sourcePassword);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				// SFTP Session
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannelSource = (ChannelSftp) channelSFTP;
			}
			if (!ValidationUtil.isValid(hostName) || !ValidationUtil.isValid(port) || !ValidationUtil.isValid(userName)
					|| !ValidationUtil.isValid(password) || !ValidationUtil.isValid(connectorFeedDir)) {
				exceptionCode.setErrorMsg("Problem in fetching File Detail from ETL_File_Upload_area - Connector_ID [ "
						+ connectorId + "] View_Name [" + viewName + "]");
				return exceptionCode;
			}
			/*if ("Y".equalsIgnoreCase(securedFtp)) {*/
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				// SFTP Session
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;
				
				channel = session.openChannel("shell");
				inputstream_for_the_channel = channel.getOutputStream();
				commander = new PrintStream(inputstream_for_the_channel, true);

				String sessionId = String.valueOf(System.currentTimeMillis());
				
				uploadeddataFileName = upFileName.replaceAll(connectorId + "_" + fileId,
						connectorId + "_" + fileId + "_" + sessionId);
				// uploadeddataFileName -- user uploaded file
				channel.connect();
				// sftpChannel.connect();
				sftpChannel.pwd();
				InputStream ins = null;
				BufferedInputStream bis = null;
				String filePattern = null;
				if (!"E".equalsIgnoreCase(dynamicScript)) {
					String fileName = sourceDynamicFileLocation
							.substring(sourceDynamicFileLocation.lastIndexOf("/") + 1);
					String fileLocation = sourceDynamicFileLocation.substring(0,
							sourceDynamicFileLocation.lastIndexOf("/") + 1);
					if (fileName.contains("*")) {
						filePattern = fileName.replace("*", ".*?");
					}
					sftpChannelSource.cd(fileLocation);
					if (ValidationUtil.isValid(filePattern)) {
						Vector<ChannelSftp.LsEntry> files = (Vector<ChannelSftp.LsEntry>) sftpChannelSource.ls(".");
				        files.sort(Comparator.comparing(ChannelSftp.LsEntry::getAttrs, Comparator.comparingInt(SftpATTRS::getMTime)).reversed());
						for (ChannelSftp.LsEntry file : files) {
							String filename = file.getFilename();
							if (filename.matches(filePattern)) {
								Pattern pattern = Pattern.compile(fileName.replace("*", "(.*?)"));
								Matcher matcher = pattern.matcher(filename);
								if (matcher.find()) {
									String matchedText = matcher.group(1);
								}
								ins = sftpChannelSource.get(filename);
								//temporary code
								filename = filename.substring(0, filename.lastIndexOf('.')) + "_" + connectorId + "_"
										+ fileId + "_" + sessionId + "." + fileExtension + "";
								if(!filename.equalsIgnoreCase(uploadeddataFileName)) {
									uploadeddataFileName = filename;
								}
								//temporary code
								break;
							}
						}
					} else {
						ins = sftpChannelSource.get(sourceDynamicFileLocation);
					}
				} else {
					ins = sftpChannel.get(connectorFeedDir + "/" + upFileName);
				}
				bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				sftpChannel.pwd();
				sftpChannel.cd(connectorTempFeedDir);
				sftpChannel.put(bis, uploadeddataFileName);
				bis.close();
				ins.close();

				// dataFileName -- user uploaded file with session id
				dataFileName = uploadeddataFileName;

				// inputdataFileName -- user uploaded file and path
				inputdataFileName = "input" + connectorId + "_" + sessionId + ".JSON";

				// ouputdataFileName -- spark giving output
				ouputdataFileName = "output" + "_" + connectorId + "_" + sessionId;

				/* temp dir - output file upload */
				String jsonReponse = "";
				jsonReponse = "{" + "\"xlsTreatEmptyValuesAsNulls\" : " + "\"true\"," + " \"xlsInferSchema\" : "
						+ "\"true\"," + " \"xlsTimestampFormat\" : " + "\"dd-mm-yyyy HH:mm:ss\"," + " \"xlsHeader\" : "
						+ "\"true\"," + " \"inputFormatType\" : " + "\"" + fileExtension + "\","
						+ " \"sourceFilepath\" : " + "\"" + connectorTempFeedDir + "\"," + " \"sourceFileName\" : "
						+ "\"" + dataFileName + "\"," + " \"outputSchemaFileName\" : " + "\"" + ouputdataFileName
						+ "\"," + " \"outputSchemaFilePath\" : " + "\"" + connectorTempFeedDir + "\"";
				if (ValidationUtil.isValid(fileExtension)
						&& (fileExtension.equalsIgnoreCase("XML") || fileExtension.equalsIgnoreCase("xml"))) {
					JSONObject object = new JSONObject(context);
					jsonReponse = jsonReponse + ",\"xlsDataAddress\" : " + "\"" + sheetName + "\"," + "\"delimiter\" : "
							+ "\"\"," + "\"rootTag\" : " + "\"" + object.getString("rootTag") + "\"," + " \"rowTag\" : "
							+ "\"" + object.getString("rowTag") + "\"";
				} else if (ValidationUtil.isValid(fileExtension) && (fileExtension.equalsIgnoreCase("xlsx")
						|| fileExtension.equalsIgnoreCase("xls") || fileExtension.equalsIgnoreCase("csv"))) {
					jsonReponse = jsonReponse + ",\"xlsDataAddress\" : " + "\"" + sheetName + "\"," + "\"delimiter\" : "
							+ "\"\"," + "\"rootTag\" : " + "\"\"," + "\"rowTag\" : " + "\"\"";
					if (fileExtension.equalsIgnoreCase("csv")) {
						JSONObject object = new JSONObject(context);
						jsonReponse = jsonReponse + ",\"xlsDataAddress\" : " + "\"" + sheetName + "\","
								+ "\"delimiter\" : " + "\"" + object.getString("delimiter") + "\"," + "\"rootTag\" : "
								+ "\"\"," + "\"rowTag\" : " + "\"\"";
					}
				} else {
					jsonReponse = jsonReponse + ",\"xlsDataAddress\" : " + "\"" + sheetName + "\"," + "\"delimiter\" : "
							+ "\"\"," + "\"rootTag\" : " + "\"\"," + "\"rowTag\" : " + "\"\"";

				}
				jsonReponse = jsonReponse + "}";

				/* start - upoad file */
				sftpChannel.cd(connectorTempFeedDir);
				
				
				File lFile1 = new File(connectorTempFeedDir + inputdataFileName);
				if (lFile1.exists()) {
					lFile1.delete();
				}
				InputStream in = new ByteArrayInputStream(jsonReponse.getBytes());
				sftpChannel.put(in, inputdataFileName);

				/* temp dir - output file upload */

				/* Below code will execute command and wait for result */
				int retVal = execUnxComAndGetResult(
						"./getXmlSchema.sh " + connectorTempFeedDir + " " + inputdataFileName + "", channel, session,
						commander, in);
				if (retVal == 0) {
					sftpChannel.cd(connectorTempFeedDir);
					InputStream stream = null;
					try {
						stream = sftpChannel.get(connectorTempFeedDir + ouputdataFileName + ".log");
					} catch (Exception e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
						// stream = sftpChannel.get(connectorTempFeedDir+"Error.log");
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg(
								"Output [" + ouputdataFileName + "].log not exists in [" + connectorTempFeedDir + "]");
						return exceptionCode;
					}
					Map<String, String> dataTypeMap = etlFeedMainDao.getDataTypeMap("EXCEL");
					if (dataTypeMap.size() == 0) {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg(
								"Current select TAG_NAME, DISPLAY_NAME from MACROVAR_TAGGING t where MACROVAR_NAME = 'DATA_TYPE' AND UPPER(MACROVAR_TYPE) = UPPER('EXCEL') is not maintained");
						return exceptionCode;
					}
					try {
						BufferedReader br = new BufferedReader(new InputStreamReader(stream));
						StringJoiner line = new StringJoiner(" ");
						line.add(br.readLine());
						if (ValidationUtil.isValid(line)) {
							columnsMetaDataList = generateSchema(line.toString().trim(), dataTypeMap);
						}

						if (columnsMetaDataList.size() > 0 && fileColumns.size() > 0) {
							List<String> l1 = new ArrayList<String>();
							for (EtlFileColumnsVb li : fileColumns) {
								l1.add(li.getColumnName() + "@-@" + li.getColumnDatatype());
							}
							List<String> l2 = new ArrayList<String>();
							for (EtlFileColumnsVb li : columnsMetaDataList) {
								l2.add(li.getColumnName() + "@-@" + li.getColumnDatatype());
							}

							Set<String> set = new HashSet<String>();
							set.addAll(l1);
							set.addAll(l2);
							List<String> list = new ArrayList<String>(set);

							List<String> uLit = list;
							for (String y : uLit) {
								String[] entValue = y.toString().split("@-@");
								EtlFileColumnsVb cVb = new EtlFileColumnsVb();
								cVb.setColumnName(entValue[0]);
								cVb.setColumnDatatype(entValue[1]);
								if (l1.contains(y) && l2.contains(y)) {
									cVb.setWriteFlag("E");// existing column
								} else if (!l1.contains(y) && l2.contains(y)) {
									cVb.setWriteFlag("A");/// new column from api
								} else if (l1.contains(y) && !l2.contains(y)) {
									cVb.setWriteFlag("D");// deleted from existing
								}
								/*
								 * for(int k=0;k<fileColumns.size();k++) {
								 * if(y.equalsIgnoreCase(fileColumns.get(k).getColumnName())) {
								 * cVb.setColumnId(fileColumns.get(k).getColumnId()); break; }else { for(int
								 * k1=0;k1<columnsMetaDataList.size();k1++) {
								 * if(y.equalsIgnoreCase(columnsMetaDataList.get(k1).getColumnName())) {
								 * cVb.setColumnId(columnsMetaDataList.get(k1).getColumnId()); break; } } } }
								 * if(!ValidationUtil.isValid(cVb.getColumnId())){ for(int
								 * k1=0;k1<columnsMetaDataList.size();k1++) {
								 * if(y.equalsIgnoreCase(columnsMetaDataList.get(k1).getColumnName())) {
								 * cVb.setColumnId(columnsMetaDataList.get(k1).getColumnId()); break; } } }
								 */
								// i++;
								columnList.add(cVb);
							}
							int colId = 0;
							for (int k = 0; k < columnList.size(); k++) {
								colId = k + 1;
								columnList.get(k).setColumnId(colId);
							}
						}

					} catch (IOException io) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							io.printStackTrace();
						}
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.ERRONEOUS_OPERATION,
								"Upload",
								"Exception occurred during reading file from SFTP server due to " + io.getMessage());
						return exceptionCode;
					} catch (Exception e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.ERRONEOUS_OPERATION,
								"Upload",
								"Exception occurred during reading file from SFTP server due to " + e.getMessage());
						return exceptionCode;
					}
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.ERRONEOUS_OPERATION, "Upload",
							"Erro while executing shell script");
					return exceptionCode;
				}
			/*}*/
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
			if (columnList.size() > 0) {
				exceptionCode.setResponse(columnList);
			} else {
				/*
				 * if(columnsMetaDataList.size()==0) { EtlFileColumnsVb newColumnVb = new
				 * EtlFileColumnsVb(); newColumnVb.setColumnName("Country");
				 * newColumnVb.setColumnId(1); newColumnVb.setColumnDatatype("Y");
				 * columnsMetaDataList.add(newColumnVb); }
				 */
				exceptionCode.setResponse(columnsMetaDataList);
			}

		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setResponse(connectorId);
		} finally {
			try {
				channel = session.openChannel("sftp");
				channel.connect();
				sftpChannel = (ChannelSftp) channel;
				System.out.println("finally connectorTempFeedDir "+connectorTempFeedDir);
				/*try {
					sftpChannel.cd(connectorTempFeedDir);
					sftpChannel.rm(inputdataFileName);
					sftpChannel.rm(uploadeddataFileName);
					sftpChannel.rm(ouputdataFileName + ".log");
				} catch (SftpException e) {
					e.printStackTrace();
				}*/
			} catch (JSchException e1) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e1.printStackTrace();
				}
			}
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				channel = null;
			}
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
				sftpChannel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		return exceptionCode;
	}

	public <String> List<String> union(List<String> list1, List<String> list2) {
		Set<String> set = new HashSet<String>();
		set.addAll(list1);
		set.addAll(list2);
		return new ArrayList<String>(set);
	}

	public <String> List<String> intersection(List<String> list1, List<String> list2) {
		List<String> list = new ArrayList<String>();
		for (String t : list1) {
			if (list2.contains(t)) {
				list.add(t);
			}
		}
		return list;
	}

	private List<EtlFileColumnsVb> generateSchema(String str, Map<String, String> dataTypeMap) {
		List<EtlFileColumnsVb> columnsMetaDataList = new ArrayList<EtlFileColumnsVb>();
		Pattern pattern = Pattern.compile("StructField\\((.*?)\\,(.*?)Type\\,(.*?)\\)", Pattern.DOTALL);
		Matcher matcher = pattern.matcher(str);
		int i = 1;
		while (matcher.find()) {
			EtlFileColumnsVb newColumnVb = new EtlFileColumnsVb();
			newColumnVb.setColumnName(matcher.group(1).toUpperCase());
			if (dataTypeMap != null && !dataTypeMap.isEmpty()
					&& ValidationUtil.isValid(dataTypeMap.get(matcher.group(2).toUpperCase()))) {
				newColumnVb.setColumnDatatype(dataTypeMap.get(matcher.group(2).toUpperCase()));
			} else {
				newColumnVb.setColumnDatatype("Y");
			}
			newColumnVb.setColumnId(i);
			columnsMetaDataList.add(newColumnVb);
			i++;
		}
		return columnsMetaDataList;
	}

	public static int execUnxComAndGetResult(String command, Channel channel, Session session, PrintStream commander,
			InputStream in) {
		int returnVal = -1;
		try {
			commander.println("cd " + uploadScriptDir);
			System.out.println("command "+command);
			StringBuilder cmd = new StringBuilder();
			cmd.append(command);
			commander.println(cmd);
			commander.println("exit");
			commander.close();
			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
				}
				if (channel.isClosed()) {
					returnVal = channel.getExitStatus();
					break;
				}
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return returnVal;
		}
		return returnVal;
	}

	public String fixJSONObject(JSONObject obj) {
		String jsonString = obj.toString();
		for (int i = 0; i < obj.names().length(); i++) {
			try {
				jsonString = jsonString.replace(obj.names().getString(i), obj.names().getString(i).toUpperCase());
			} catch (JSONException e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
			}
		}
		return jsonString;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode uploadDynamicFileForETLConnector(EtlFileUploadAreaVb etlFileUploadAreaVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		ChannelSftp sftpChannel = null;
//		Channel channelSFTP = null;
		Session session = null;
		String filePath = "";
		List<String> SheetNames = new ArrayList<>();
		String exactUploadeddataFileName = "";
		String uploadeddataFileName = "";
		try {
			JSONObject extractVbData = new JSONObject(etlFileUploadAreaVb);
			JSONArray eachColData = (JSONArray) extractVbData.getJSONArray("dynamicScript");
			for (int i = 0; i < eachColData.length(); i++) {
				JSONObject ss = eachColData.getJSONObject(i);
				String ch = fixJSONObject(ss);
				JSONObject extractData = new JSONObject(ch);
				String tag = extractData.getString("TAG");
				String encryption = extractData.getString("ENCRYPTION");
				String value = extractData.getString("VALUE");
				if (ValidationUtil.isValid(encryption) && encryption.equalsIgnoreCase("Yes")) {
					try {
						value = URLDecoder.decode(value, "UTF-8");
					} catch (UnsupportedEncodingException e) {
						if ("Y".equalsIgnoreCase(enableDebug)) {
							e.printStackTrace();
						}
					}
					value = AESEncryptionDecryption.decryptPKCS7(value);
				}
				if (tag.equalsIgnoreCase("SERVER_IP")) {
					etlFileUploadAreaVb.setHostName(value);
				} else if (tag.equalsIgnoreCase("SERVER_PORT")) {
					etlFileUploadAreaVb.setPort(Integer.parseInt(value));
				} else if (tag.equalsIgnoreCase("SERVER_USER")) {
					etlFileUploadAreaVb.setUserName(value);
				} else if (tag.equalsIgnoreCase("SERVER_PWD")) {
					etlFileUploadAreaVb.setPassword(value);
				} else if (tag.equalsIgnoreCase("SERVER_TYPE")) {
					etlFileUploadAreaVb.setServerType(value);
				} else if (tag.equalsIgnoreCase("FILE_LOCATION")) {
					etlFileUploadAreaVb.setFileLocation(value);
				} else if (tag.equalsIgnoreCase("FTP")) {
					etlFileUploadAreaVb.setConnectionType(value);
				}
			}
			String connectorId = etlFileUploadAreaVb.getConnectorId();
			String fileId = etlFileUploadAreaVb.getFileId();

			filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			String fileName = etlFileUploadAreaVb.getFileLocation()
					.substring(etlFileUploadAreaVb.getFileLocation().lastIndexOf("/") + 1);
			if(!ValidationUtil.isValid(fileName)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("No filename mentioned in the location");
			}
			String fileLocation = etlFileUploadAreaVb.getFileLocation().substring(0,
					etlFileUploadAreaVb.getFileLocation().lastIndexOf("/") + 1);
			
			String filePattern = null;
			if (fileName.contains("*")) {
				filePattern = fileName.replace("*", ".*?");
			}
			
			String fileExtension = fileName.substring(fileName.lastIndexOf(".") + 1);
			
			exactUploadeddataFileName = fileName.substring(0, fileName.lastIndexOf('.')) + "." + fileExtension + "";
			uploadeddataFileName = fileName.substring(0, fileName.lastIndexOf('.')) + "_" + connectorId + "_" + fileId
					+ "." + fileExtension + "";
			exactUploadeddataFileName = fileName.substring(0, fileName.lastIndexOf('.')) + "." + fileExtension + "";
			BufferedInputStream bis = null;
			InputStream ins = null;

			/*if ("Y".equalsIgnoreCase(securedFtp)) {*/
				JSch jsch = new JSch();
				session = jsch.getSession(etlFileUploadAreaVb.getUserName(), etlFileUploadAreaVb.getHostName(),
						etlFileUploadAreaVb.getPort());
				session.setPassword(etlFileUploadAreaVb.getPassword());
				session.setConfig("StrictHostKeyChecking", "no");
				try {
					session.connect();
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setErrorMsg("Problem in gaining connection for this connection details");
					return exceptionCode;
				}
				/*Channel channel = session.openChannel("sftp");
				channel.connect();
				sftpChannel = (ChannelSftp) channel;*/
				sftpChannel = (ChannelSftp) session.openChannel("sftp");
				sftpChannel.connect();
				sftpChannel.cd(fileLocation);
				try {
					if(ValidationUtil.isValid(filePattern)) {
						Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(".");
						files.sort(Comparator.comparing(ChannelSftp.LsEntry::getAttrs, Comparator.comparingInt(SftpATTRS::getMTime)).reversed());
						for (ChannelSftp.LsEntry file : files) {
							String filename = file.getFilename();
							if (filename.matches(filePattern)) {
								Pattern pattern = Pattern.compile(fileName.replace("*", "(.*?)"));
								Matcher matcher = pattern.matcher(filename);
								if (matcher.find()) {
									String matchedText = matcher.group(1);
								}
								String localFilePath = filePath + "/" + filename;
								OutputStream outputStream = new FileOutputStream(localFilePath);
								sftpChannel.get(filename, outputStream);
								outputStream.close();
								ins = sftpChannel.get(filename);
								bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
								break;
							}
						}
					} else {
						ins = sftpChannel.get(fileName);
						bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
						File lFile = new File(filePath + uploadeddataFileName);
						if (lFile.exists()) {
							lFile.delete();
						}
					}
				} catch (SftpException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
				sftpChannel.exit();
				/*	} else {
				if (ValidationUtil.isValid(filePattern)) {
					 try {
			                Path latestFile = findLatestFile(fileLocation, filePattern);
			                if (latestFile != null) {
			                    System.out.println("Latest file: " + latestFile.getFileName());
			                    String localFilePath = fileLocation + "/" + latestFile.getFileName();
			                    // Read the file content (example: print to console)
			                    try (InputStream insN = Files.newInputStream(latestFile);
			                    	BufferedInputStream bisN = new BufferedInputStream(insN)) {
			                        byte[] buffer = new byte[1024];
			                        int bytesRead;
			                        while ((bytesRead = bisN.read(buffer)) != -1) {
			                            System.out.write(buffer, 0, bytesRead);
			                        }
			                    }
			                } else {
			                    System.out.println("No matching files found.");
			                }
			            } catch (IOException e) {
			                e.printStackTrace();
			            }
				} else {
					Path baseDirectory = Paths.get(fileLocation);
					Path filePathTarget = baseDirectory.resolve(fileName);
					ins = new BufferedInputStream(Files.newInputStream(filePathTarget));
					bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				}
			}*/
			
			if (!ValidationUtil.isValid(bis)) {
				exceptionCode.setErrorMsg("Problem in fetching File Detail - File [ " + fileName + "]");
				return exceptionCode;
			}
			if (fileExtension.equalsIgnoreCase("xlsx")) {
				XSSFWorkbook workbook = new XSSFWorkbook(bis);
				for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
					SheetNames.add(workbook.getSheetName(i));
				}
			} else if (fileExtension.equalsIgnoreCase("xls")) {
				Workbook wb = WorkbookFactory.create(bis);
				for (int i = 0; i < wb.getNumberOfSheets(); i++) {
					SheetNames.add(wb.getSheetName(i));
				}
			}
			ins.close();
			bis.close();
			
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
			exceptionCode.setOtherInfo(exactUploadeddataFileName);
			exceptionCode.setResponse(SheetNames);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
		} finally {
			File lFile = new File(filePath + uploadeddataFileName);
			if (lFile.exists()) {
				lFile.delete();
			}
			/*if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}*/
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
				sftpChannel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		return exceptionCode;
	}

	public ExceptionCode deleteFilesFromServer(String[] fileNames) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			for (String fileName : fileNames) {
				File lFile = new File(filePath + fileName);
				if (lFile.exists()) {
					lFile.delete();
				}
			}
			exceptionCode.setErrorMsg("File Delete Success");
			exceptionCode.setOtherInfo(fileNames);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setOtherInfo(fileNames);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
	}

	public ExceptionCode getSheetNames(EtlFileUploadAreaVb etlFileUploadAreaVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Session session = null;
		Channel channelSFTP = null;
		List<String> SheetNames = new ArrayList<>();
		String filePath = "";
		String uploadeddataFileName = "";
		try {
			Object[] dynamicScript = etlFileUploadAreaVb.getDynamicScript();
			String dynamicConnection = "";

			if (dynamicScript.length > 0) {
				JSONArray array = new JSONArray(dynamicScript);
				for (int i = 0; i < array.length(); i++) {
					JSONObject object = array.getJSONObject(i);
					String tag = object.getString("tag");
					if (tag.equalsIgnoreCase("SERVER_IP")) {
						hostName = object.getString("value");
					} else if (tag.equalsIgnoreCase("SERVER_PORT")) {
						port = object.getString("value");
					} else if (tag.equalsIgnoreCase("SERVER_USER")) {
						userName = object.getString("value");
					} else if (tag.equalsIgnoreCase("SERVER_PWD")) {
						password = object.getString("value");
					} else if (tag.equalsIgnoreCase("FILE_LOCATION")) {
						dynamicConnection = object.getString("value");
					}
				}
			}
			if (!ValidationUtil.isValid(hostName) || !ValidationUtil.isValid(port) || !ValidationUtil.isValid(userName)
					|| !ValidationUtil.isValid(password) || !ValidationUtil.isValid(connectorFeedDir)) {
				exceptionCode.setErrorMsg("Problem in fetching File Detail from ETL_File_Upload_area");
				return exceptionCode;
			}

			JSch jsch = new JSch();
			session = jsch.getSession(userName, hostName, Integer.parseInt(port));
			session.setPassword(password);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			// SFTP Session
			channelSFTP = session.openChannel("sftp");
			channelSFTP.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channelSFTP;

			BufferedInputStream bis = null;
			InputStream ins = null;
			if (dynamicScript.length > 0) {
				try {
					ins = sftpChannel.get(dynamicConnection);
					bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				} catch (SftpException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
			} else {
				String fileN = etlFileUploadAreaVb.getFileName();
				String fileExtension = etlFileUploadAreaVb.getFileName()
						.substring(etlFileUploadAreaVb.getFileName().lastIndexOf(".") + 1);
				// uploadeddataFileName -- user uploaded file
				uploadeddataFileName = fileN.substring(0, fileN.lastIndexOf('.')) + "_"
						+ etlFileUploadAreaVb.getConnectorId() + "_" + etlFileUploadAreaVb.getFileId() + "."
						+ fileExtension + "";
				try {
					sftpChannel.cd(connectorFeedDir);
					ins = sftpChannel.get(uploadeddataFileName);
					bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				} catch (SftpException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
			}
			String fileExtension = uploadeddataFileName.substring(uploadeddataFileName.lastIndexOf(".") + 1);

			if (!ValidationUtil.isValid(bis)) {
				exceptionCode.setErrorMsg("Problem in fetching File Detail");
				return exceptionCode;
			}
			if (fileExtension.equalsIgnoreCase("xlsx")) {
				XSSFWorkbook workbook = new XSSFWorkbook(bis);
				for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
					SheetNames.add(workbook.getSheetName(i));
				}
			} else if (fileExtension.equalsIgnoreCase("xls")) {
				Workbook wb = WorkbookFactory.create(bis);
				for (int i = 0; i < wb.getNumberOfSheets(); i++) {
					SheetNames.add(wb.getSheetName(i));
				}
			}
			ins.close();
			bis.close();

			exceptionCode.setOtherInfo(etlFileUploadAreaVb.getFileName());
			exceptionCode.setResponse(SheetNames);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		} finally {
			File lFile = new File(filePath + uploadeddataFileName);
			if (lFile.exists()) {
				lFile.delete();
			}
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
	}

	public ExceptionCode loadTransformationDataView(EtlFeedMainVb vObject) {
		Session session = null;
		Channel channelSFTP = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		ChannelSftp sftpChannel = null;
		String inputdataFileName = "";
		String uploadeddataFileName = "";
		Channel channel = null;
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				// SFTP Session
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;

				channel = session.openChannel("shell");
				try {
					if (ValidationUtil.isValid(sampleFeedDataDir)) {
						sftpChannel.cd(sampleFeedDataDir);
					}
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Directory [" + sampleFeedDataDir + "] not exists");
					return exceptionCode;
				}
				long currentUser = CustomContextHolder.getContext().getVisionId();
				uploadeddataFileName = vObject.getSessionId() + "_" + vObject.getFeedId();

				sftpChannel.mkdir(uploadeddataFileName);
				sftpChannel.cd(uploadeddataFileName);
				inputdataFileName = "data.txt";

				String jsonReponse = vObject.getSampleData();
//	    	jsonReponse="[ "  
//	    	    	+"{'header':'name,email'},  "
//	    	    	+"{'row1':'Bob,bob32@gmail.com'}, "
//	    	    	+"{'row2':'Rekha,rekha@gmail.com'}, "
//	    	    	+"{'row3':'Test,test@gmail.com'} "
//	    	    	+"]  ";

				/* start - upoad file */
				File lFile1 = new File(uploadeddataFileName + inputdataFileName);
				if (lFile1.exists()) {
					lFile1.delete();
				}
				List<String> dataHeader = new ArrayList<String>();
				List<List<String>> dataValue = new ArrayList<List<String>>();
				JSONArray dataArray = new JSONArray(jsonReponse);
				for (int i = 0; i < dataArray.length(); i++) {
					List<String> dataRowValue = new ArrayList<String>();
					JSONObject object = dataArray.getJSONObject(i);
					Iterator<String> keys = object.keys();
					while (keys.hasNext()) {
						String key = keys.next();
						String keyVal = object.get(key).toString();
						if (i == 0) {
							dataHeader.add(keyVal);
						} else {
							dataRowValue.add(keyVal);
						}
					}
					if (dataRowValue.size() > 0) {
						dataValue.add(dataRowValue);
					}
				}
				OutputStream fileOutStream = sftpChannel.put(inputdataFileName);
				PrintWriter printWriter = new PrintWriter(fileOutStream, true);
				String[] array = dataHeader.toArray(new String[0]);
				for (int j = 0; j < array.length; j++) {
					String[] header = array[j].split(",");
					for (int i = 0; i < header.length; i++) {
						if (i == header.length - 1) {
							printWriter.write(header[i]);
						} else {
							printWriter.write(header[i] + sampleFeedDataSeparator);
						}
					}
				}
				for (int i = 0; i < dataValue.size(); i++) {
					String[] array1 = dataValue.get(i).toArray(new String[0]);
					printWriter.write(System.getProperty("line.separator"));
					String[] data = array1[0].split(",");
					for (int k = 0; k < data.length; k++) {
						if (k == data.length - 1) {
							printWriter.write(data[k]);
						} else {
							printWriter.write(data[k] + sampleFeedDataSeparator);
						}
					}
				}

				printWriter.close();

			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("File Upload success");
			exceptionCode.setOtherInfo(vObject);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setOtherInfo(vObject);
		} finally {
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				channel = null;
			}
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
				sftpChannel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		return exceptionCode;
	}

	public ExceptionCode retrieveTransformationDataView(EtlFeedMainVb vObject) {
		Session session = null;
		Channel channelSFTP = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		ChannelSftp sftpChannel = null;
		String uploadeddataFileName = "";
		Channel channel = null;
		PrintStream commander = null;
		OutputStream inputstream_for_the_channel = null;
		String jsonReponse = "";
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				// SFTP Session
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;

				channel = session.openChannel("shell");
				inputstream_for_the_channel = channel.getOutputStream();
				commander = new PrintStream(inputstream_for_the_channel, true);

				try {
					if (ValidationUtil.isValid(sampleFeedDataDir)) {
						sftpChannel.cd(sampleFeedDataDir);
					}
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Directory [" + sampleFeedDataDir + "] not exists");
					return exceptionCode;
				}
				long currentUser = CustomContextHolder.getContext().getVisionId();
				uploadeddataFileName = vObject.getSessionId() + "_" + vObject.getFeedId() + "_" + currentUser;
				try {
					if (ValidationUtil.isValid(uploadeddataFileName)) {
						sftpChannel.cd(uploadeddataFileName);
					}
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Directory [" + uploadeddataFileName + "] not exists");
					return exceptionCode;
				}
				channel.connect();
				sftpChannel.pwd();
				InputStream ins = null;
				ins = sftpChannel.get("data.txt");
				byte[] bytes = IOUtils.toByteArray(ins);
				ByteArrayInputStream input = new ByteArrayInputStream(bytes);
				BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				BufferedReader br = new BufferedReader(new InputStreamReader(input));
				String newLine;
				List<String> lines = new ArrayList<String>();
				int k = 0;
				while ((newLine = br.readLine()) != null) {
					if (k == 0) {
						jsonReponse = "[" + "{'header':'" + newLine + "'},";
					} else {
						jsonReponse = jsonReponse + "{'row" + k + "':'" + newLine + "'},";
					}
					lines.add(newLine);
					k++;
				}
				StringBuffer sb = new StringBuffer(jsonReponse);
				// invoking the method
				sb.deleteCharAt(sb.length() - 1);

				jsonReponse = sb.toString() + "]  ";
				br.close();
				bis.close();
				ins.close();
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Retrieve success");
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
			exceptionCode.setErrorMsg(ex.getMessage());
			vObject.setSampleData(jsonReponse);
			exceptionCode.setOtherInfo(vObject);
		} finally {
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				channel = null;
			}
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
				sftpChannel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		vObject.setSampleData(jsonReponse);
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	public ExceptionCode createSparkFileData(MultipartFile file, String feedId, String country, String leBook) {
		Session session = null;
		Channel channelSFTP = null;
		ChannelSftp sftpChannel = null;
		String uploadeddataFileName = "";
		String folderName = "";
		ExceptionCode exceptionCode = new ExceptionCode();
		FileInfoVb fileInfoVb = new FileInfoVb();
		InputStream in = null;
		String sessionId = String.valueOf(System.currentTimeMillis());
		try {
			folderName = feedId + "_" + sessionId;
			fileInfoVb.setName(file.getOriginalFilename().toUpperCase());
			fileInfoVb.setData(file.getBytes());
			String extension = "";
			extension = FilenameUtils.getExtension(file.getOriginalFilename());
			fileInfoVb.setExtension(extension);
			uploadeddataFileName = "read.csv";
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				// session.setConfig("kex", "diffie-hellman-group1-sha1");
				session.connect();
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;
				sftpChannel.cd(transSampleOperationDir);
				sftpChannel.mkdir(folderName);
				sftpChannel.cd(folderName);
				in = new ByteArrayInputStream(fileInfoVb.getData());
				sftpChannel.put(in, uploadeddataFileName);
			} else {
				Path baseDirectory = Paths.get(transSampleOperationDir);
				Path targetDirectory = baseDirectory.resolve(folderName);
				if (!Files.exists(targetDirectory)) {
					// Create the directory if it doesn't exist
					Set<PosixFilePermission> permissions = new HashSet<>();
					permissions.add(PosixFilePermission.OWNER_READ);
					permissions.add(PosixFilePermission.OWNER_WRITE);
					permissions.add(PosixFilePermission.OWNER_EXECUTE);
				//	Files.createDirectories(targetDirectory, createFileAttribute(permissions));
					Files.createDirectories(targetDirectory);
				}
				Path filePath = targetDirectory.resolve(uploadeddataFileName);
				Files.write(filePath, fileInfoVb.getData());
			}
			EtlFeedMainVb vObject = new EtlFeedMainVb();
			vObject.setCountry(country);
			vObject.setLeBook(leBook);
			vObject.setSessionId(sessionId);
			vObject.setFeedId(feedId);
			etlFeedMainDao.doInsertTransformationMasterRecord(vObject);
			exceptionCode = CommonUtils.getResultObject(SERVICE_NAME, Constants.SUCCESSFUL_OPERATION, "Upload", "");
			exceptionCode.setResponse(uploadeddataFileName);
			exceptionCode.setOtherInfo(sessionId);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
			exceptionCode.setErrorMsg(ex.getMessage());
		} finally {
			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				try {
					if (sftpChannel != null && sftpChannel.isConnected()) {
						sftpChannel.cd(transSampleOperationDir);
						sftpChannel.cd(folderName);
						sftpChannel.rm(uploadeddataFileName);
					}
				} catch (SftpException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
			}

			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}

			}
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
				sftpChannel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		return exceptionCode;
	}

	private FileAttribute<Set<PosixFilePermission>> createFileAttribute(Set<PosixFilePermission> permissions) {
	    return PosixFilePermissions.asFileAttribute(permissions);
	}
	
	public ExceptionCode deleteTransformationSession(EtlFeedMainVb vObject, boolean schedulerFlag) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Session session = null;
		Channel channelSFTP = null;
		ChannelSftp sftpChannel = null;
		String folderName = "";
		try {
			folderName = vObject.getFeedId() + "_" + vObject.getSessionId();
			String sparkHome = System.getenv("SPARK_HOME");

			if (!ValidationUtil.isValid(sparkHome))
				sparkHome = "/home/vision/app/spark/spark";

			String appID = null;
			String appIdPath = transSampleOperationDir + "/" + folderName + "/AppID.txt";
			if ("Y".equalsIgnoreCase(securedFtp)) {
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;
				sftpChannel.cd(transSampleOperationDir);
				if (CommonUtils.isFileExist(sftpChannel, appIdPath)) {
					appID = readTxtFromFile(sftpChannel, appIdPath);
				}
				recursiveFolderDelete(sftpChannel, transSampleOperationDir + "/" + folderName);
				if(ValidationUtil.isValid(appID)) {
					recursiveFolderDelete(sftpChannel, sparkHome+"/work/"+appID);
				}
			} else {
				if (CommonUtils.isFileExist(appIdPath)) {
					appID = readTxtFromFile(appIdPath);
				}
				recursiveFolderDelete(transSampleOperationDir + "/" + folderName);
				if (ValidationUtil.isValid(appID)) {
					recursiveFolderDelete(sparkHome+"/work/"+appID);
				}
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			int retVal = etlFeedMainDao.deleteEtlTransformationSession(vObject, schedulerFlag);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Folder Name - " + folderName + ". Cause -" + e.getMessage());
		} finally {
			if (channelSFTP != null && channelSFTP.isConnected()) {
				channelSFTP.disconnect();
				channelSFTP = null;
			}
			if (sftpChannel != null && sftpChannel.isConnected()) {
				sftpChannel.exit();
				sftpChannel.disconnect();
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		exceptionCode.setOtherInfo(vObject);
		return exceptionCode;
	}

	private static void recursiveFolderDelete(ChannelSftp channelSftp, String path) throws SftpException {
		Collection<ChannelSftp.LsEntry> fileAndFolderList = channelSftp.ls(path);
		for (ChannelSftp.LsEntry item : fileAndFolderList) {
			if (!item.getAttrs().isDir()) {
				channelSftp.rm(path + "/" + item.getFilename());
			} else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
				try {
					channelSftp.rmdir(path + "/" + item.getFilename());
				} catch (Exception e) { 
					recursiveFolderDelete(channelSftp, path + "/" + item.getFilename());
				}
			}
		}
		channelSftp.rmdir(path); // delete the parent directory after empty
	}
	
	public static void recursiveFolderDelete(String path) {
		try {
			Stream<Path> fileAndFolderList = Files.list(Paths.get(path));
			fileAndFolderList.forEach(filePath -> {
				try {
					if (Files.isDirectory(filePath)) {
						recursiveFolderDelete(filePath.toString());
					} else {
						Files.delete(filePath);
					}
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			});
			Files.delete(Paths.get(path));
			fileAndFolderList.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ExceptionCode moveInputJson_TriggerSpark_ReadDataFile(String feedId, String sessionId,
			EtlFeedTranNodeVb selectedNodeVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Session session = null;
		Channel channelSFTP = null;
		Channel channel = null;
		String folderName = feedId + "_" + sessionId;
		String statusFileName = "status.txt";
		String errorFileName = "error.log";
		String appIdTxt = "AppID.txt";
		String localFileName = feedId + "_" + sessionId;
		File lFile = new File(localFilePath + localFileName + "." + transFileExtension);
		InputStream in = null;
		ChannelSftp sftpChannel = null;
		String selectedNodeId = selectedNodeVb.getNodeId();
		String sparkHome = System.getenv("SPARK_HOME");

		if (!ValidationUtil.isValid(sparkHome))
			sparkHome = "/home/vision/app/spark/spark";
		
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				// Create Session
				JSch jsch = new JSch();
				session = jsch.getSession(userName, hostName, Integer.parseInt(port));
				session.setPassword(password);
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();

				// Get sftp channel
				channelSFTP = session.openChannel("sftp");
				channelSFTP.connect();
				sftpChannel = (ChannelSftp) channelSFTP;

				// Use sftp and move input.json file to Engine
				in = new FileInputStream(lFile);
				sftpChannel.cd(transSampleOperationDir);
				sftpChannel.cd(folderName + "/");
				sftpChannel.put(in, transFileName + "." + transFileExtension);
				if (CommonUtils.isFileExist(sftpChannel,
						transSampleOperationDir + "/" + folderName + "/" + statusFileName)) {
					sftpChannel.rm(transSampleOperationDir + "/" + folderName + "/" + statusFileName);
				}

				// Get shell channel and execute sh file
				channel = session.openChannel("shell");
				OutputStream outputStream = channel.getOutputStream();
				channel.connect();

				StringBuilder cmd = new StringBuilder();
				PrintStream commander = new PrintStream(outputStream, true);
				commander.println("cd " + uploadScriptDir);
				cmd.append("nohup ./runTransformationSpark.sh " + transSampleOperationDir + "/" + folderName + "/ > "
						+ transSampleOperationDir + "/" + folderName + "/" + folderName + ".txt &");
				commander.println(cmd);
				Thread.sleep(1000);
				commander.println("exit");
				commander.close();

				// Find status file
				sftpChannel.cd(transSampleOperationDir);
				String completionStatus = "E";
				boolean isFileExist = false;
				// wait till spark creates status file inside the folder
				do {
					isFileExist = CommonUtils.isFileExist(sftpChannel,
							transSampleOperationDir + "/" + folderName + "/" + statusFileName);
					Thread.sleep(1000);
				} while (!isFileExist);

				completionStatus = readTxtFromFile(sftpChannel,
						transSampleOperationDir + "/" + folderName + "/" + statusFileName);

				if ("S".equalsIgnoreCase(completionStatus)) {
					InputStream stream = sftpChannel
							.get(transSampleOperationDir + "/" + folderName + "/" + selectedNodeId + ".csv");
					byte[] bytes = IOUtils.toByteArray(stream);
					exceptionCode.setResponse(bytes);
					exceptionCode.setOtherInfo(selectedNodeId + ".csv");
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				} else {
					isFileExist = CommonUtils.isFileExist(sftpChannel,
							transSampleOperationDir + "/" + folderName + "/" + errorFileName);
					if (isFileExist) {
						InputStream stream = sftpChannel
								.get(transSampleOperationDir + "/" + folderName + "/" + errorFileName);
						BufferedReader br = new BufferedReader(new InputStreamReader(stream));
						String line;
						if ((line = br.readLine()) != null) {
						}
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg(line);
						throw new RuntimeCustomException(exceptionCode);
					} else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Error file not found - Unknown error");
						throw new RuntimeCustomException(exceptionCode);
					}
				}
				
			} else {
				in = new FileInputStream(lFile);
				Path baseDirectory = Paths.get(transSampleOperationDir);
				Path targetDirectory = baseDirectory.resolve(folderName);
				Path filePath = targetDirectory.resolve(transFileName + "." + transFileExtension);
				byte[] data = IOUtils.toByteArray(in);
				Files.write(filePath, data);
				if (CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + statusFileName)) {
					Path statusFilePath = Paths.get(transSampleOperationDir + "/" + folderName + "/" + statusFileName);
					Files.delete(statusFilePath);
				}
				String scriptPath = null;
				scriptPath =  uploadScriptDir+ "/runTransformationSpark.sh ";
				String command = "nohup "+ scriptPath + transSampleOperationDir + "/" + folderName + "/ > "
						+ transSampleOperationDir + "/" + folderName + "/" + folderName + ".txt &";
				logger.error(command);
				boolean isFileExist = false;
				Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
				int exitCode = 1;
				exitCode = process.waitFor();
				Thread.sleep(1000);
				do {
					isFileExist = CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + folderName + ".txt");
					Thread.sleep(1000);
				} while (!isFileExist);
				if (exitCode == 0 && isFileExist) {
					exceptionCode.setErrorMsg("Command executed successfully.");
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("Warning. Problem maybe with command execution - [" + command + "].");
					return exceptionCode;
				}
				
				String completionStatus = "E";
				isFileExist = false;
				do {
					isFileExist = CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + statusFileName);
					Thread.sleep(1000);
				} while (!isFileExist);
				
				completionStatus = readTxtFromFile(transSampleOperationDir + "/" + folderName + "/" + statusFileName);
				InputStream inputStream = null;
				if ("S".equalsIgnoreCase(completionStatus)) {
					try {
						inputStream = new FileInputStream(transSampleOperationDir + "/" + folderName + "/" + selectedNodeId + ".csv");
						byte[] bytes = IOUtils.toByteArray(inputStream);
						exceptionCode.setResponse(bytes);
						exceptionCode.setOtherInfo(selectedNodeId + ".csv");
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					isFileExist = CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + errorFileName);
					if (isFileExist) {
						inputStream = new FileInputStream(transSampleOperationDir + "/" + folderName + "/" + errorFileName); 
						BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
						String line;
						if ((line = br.readLine()) != null) {
						}
						br.close();
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg(line);
						throw new RuntimeCustomException(exceptionCode);
					} else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Error file not found - Unknown error");
						throw new RuntimeCustomException(exceptionCode);
					}
				}
				
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e);
		} finally {
			// Delete App-id here
			try {
				if ("Y".equalsIgnoreCase(securedFtp)) {
					if (CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + appIdTxt)) {
						String appID = readTxtFromFile(sftpChannel,	transSampleOperationDir + "/" + folderName + "/" + appIdTxt);
						if (ValidationUtil.isValid(appID)) {
							if (ValidationUtil.isValid(appID)) {
								recursiveFolderDelete(sftpChannel, sparkHome + "/work/" + appID);
							}
						}
					}
				} else {
					if (CommonUtils.isFileExist(transSampleOperationDir + "/" + folderName + "/" + appIdTxt)) {
						String appID = readTxtFromFile(transSampleOperationDir + "/" + folderName + "/" + appIdTxt);
						if (ValidationUtil.isValid(appID)) {
							if (ValidationUtil.isValid(appID)) {
								recursiveFolderDelete(sparkHome + "/work/" + appID);
							}
						}
					}
				}
				sftpChannel.exit();
				do {
					Thread.sleep(1000);
				} while (!channelSFTP.isEOF());
			} catch (Exception e) {
			}
			try {
				if (channel != null)
					channel.disconnect();
			} catch (Exception e2) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e2.printStackTrace();
				}
			}
			try {
				if (channelSFTP != null)
					channelSFTP.disconnect();
			} catch (Exception e2) {
			}
			try {
				if (session != null)
					session.disconnect();
			} catch (Exception e2) {
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
			}
			if (lFile.exists()) {
				lFile.delete();
			}
		}
		return exceptionCode;
	}

	public void createInputFileInLocal(String jsonReponse, String fileName) {
		OutputStreamWriter output = null;
		try {
			File lFile = new File(localFilePath + fileName + "." + transFileExtension);
			if (lFile.exists()) {
				lFile.delete();
			}
			lFile.createNewFile();
			FileOutputStream file = new FileOutputStream(localFilePath + fileName + "." + transFileExtension);
			output = new OutputStreamWriter(file, StandardCharsets.UTF_8);
			output.write(jsonReponse);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e);
		} finally {
			try {
				if (output != null) {
					output.close();
				}
			} catch (Exception e2) {
			}
		}
	}

	private String readTxtFromFile(ChannelSftp sftpChannel, String fileName) throws SftpException, Exception {
		InputStream ins = sftpChannel.get(fileName);
		StringBuilder textBuilder = new StringBuilder();
		try (Reader reader = new BufferedReader(
				new InputStreamReader(ins, Charset.forName(StandardCharsets.UTF_8.name())))) {
			int c = 0;
			while ((c = reader.read()) != -1) {
				textBuilder.append((char) c);
			}
		}
		return textBuilder.toString().trim();
	}
	
	@Override
	protected ExceptionCode doValidate(VisionUploadVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("", operation);
		if(!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation +" "+Constants.userRestrictionMsg);
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
	
	private String readTxtFromFile(String fileName) throws IOException {
		StringBuilder textBuilder = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
			String line;
			while ((line = reader.readLine()) != null) {
				textBuilder.append(line).append(System.lineSeparator());
			}
		}
		return textBuilder.toString().trim();
	}
	
	private static Path findLatestFile(String directoryPath, String filePattern) throws IOException {
		List<Path> matchingFiles = new ArrayList<>();
		Pattern pattern = Pattern.compile(filePattern.replace("*", ".*"));

		Files.walkFileTree(Paths.get(directoryPath), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				String filename = file.getFileName().toString();
				if (pattern.matcher(filename).matches()) {
					matchingFiles.add(file);
				}
				return FileVisitResult.CONTINUE;
			}
		});
		return matchingFiles.stream().max(Comparator.comparingLong(file -> file.toFile().lastModified())).orElse(null);
	}
	
}
