package com.vision.wb;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.vision.authentication.CustomContextHolder;
import com.vision.dao.AbstractDao;
import com.vision.dao.CommonDao;
import com.vision.dao.EtlFeedMainDao;
import com.vision.dao.OperationalConsoleDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonApiModel;
import com.vision.vb.ConsoleDependencyFeedVb;
import com.vision.vb.DependencyCategoryVb;
import com.vision.vb.DependencyFeedVb;
import com.vision.vb.ETLAlertVb;
import com.vision.vb.ETLFeedProcessControlVb;
import com.vision.vb.OperationalConsoleVb;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component
public class OperationalConsoleWb extends AbstractDynaWorkerBean<OperationalConsoleVb> {

	@Autowired
	EtlFeedMainDao etlFeedMainDao;
	@Autowired
	OperationalConsoleDao operationalConsoleDao;
	@Autowired
	CommonDao commonDao;

	private static String hostName;
	private static String userName;
	private static String passWord;
	private static String uploadDir;
	private static String port;
	private static String securedFtp;
	private static String scriptDir;
	final String HIPHEN = "-";

	@Value("${feed.upload.hostName}")
	public void setHostName(String hostName) {
		OperationalConsoleWb.hostName = hostName;
	}

	@Value("${feed.upload.userName}")
	public void setUserName(String userName) {
		OperationalConsoleWb.userName = userName;
	}

	@Value("${feed.upload.passWord}")
	public void setPassWord(String passWord) {
		OperationalConsoleWb.passWord = passWord;
	}

	@Value("${feed.upload.uploadDir}")
	public void setUploadDir(String uploadDir) {
		OperationalConsoleWb.uploadDir = uploadDir;
	}

	@Value("${feed.upload.securedFtp}")
	public void setSecuredFtp(String securedFtp) {
		OperationalConsoleWb.securedFtp = securedFtp;
	}

	@Value("${feed.upload.port}")
	public void setPort(String port) {
		OperationalConsoleWb.port = port;
	}

	@Value("${feed.upload.scriptDir}")
	public void setScriptDir(String scriptDir) {
		OperationalConsoleWb.scriptDir = scriptDir;
	}

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			Map<String, String> feedCategory = operationalConsoleDao.getFeedCategory();
			arrListLocal.add(feedCategory); // 0
			Map<String, String> feedId = operationalConsoleDao.getFeedIDBasedonCategory();
			arrListLocal.add(feedId); // 1
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2112); // current process
			arrListLocal.add(collTemp); // 2
			// String timeZoneOfETL = operationalConsoleDao.lstTimeZone();
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2102); // Feed Type
			arrListLocal.add(collTemp); //3
			return arrListLocal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the Page load values.", ex);
			}
			return null;
		}
	}

	public ExceptionCode deleteRecord(ExceptionCode pRequestCode, List<OperationalConsoleVb> vObjects) {
		ExceptionCode exceptionCode = null;
		DeepCopy<OperationalConsoleVb> deepCopy = new DeepCopy<OperationalConsoleVb>();
		List<OperationalConsoleVb> clonedObject = null;
		OperationalConsoleVb vObject = null;
		try {
			setAtNtValues(vObjects);
			vObject = (OperationalConsoleVb) pRequestCode.getOtherInfo();
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copyCollection(vObjects);
			doFormateData(vObjects);
			exceptionCode = doValidate(vObjects);
			if (exceptionCode != null && exceptionCode.getErrorMsg() != "") {
				return exceptionCode;
			}
			exceptionCode = operationalConsoleDao.doDeleteApprRecord(vObjects, vObject);
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(vObjects);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			}
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(clonedObject);
			return exceptionCode;
		}
	}

	// public ExceptionCode reInitiateCategory(OperationalConsoleVb categoryVb){
	public ExceptionCode reInitiateCategory(List<OperationalConsoleVb> categoryVbs) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String reInitiateAllowed = commonDao.findVisionVariableValue("ETL_RE-INITIATE_FLAG");
			if ("N".equalsIgnoreCase(reInitiateAllowed)) {
				exceptionCode.setErrorMsg("Re-Initiate not allowed");
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			exceptionCode = operationalConsoleDao.reInitiateCategory(categoryVbs);
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Reinitiate Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setErrorMsg(rex.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
	}

	public ExceptionCode reInitiateFeed(List<OperationalConsoleVb> feedVbList) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<OperationalConsoleVb> feedVbFromChildCategoryList = new ArrayList<OperationalConsoleVb>();
			String reInitiateAllowed = commonDao.findVisionVariableValue("ETL_RE-INITIATE_FLAG");
			if ("N".equalsIgnoreCase(reInitiateAllowed)) {
				exceptionCode.setErrorMsg("Re-Initiate not allowed");
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			Map<String, OperationalConsoleVb> reinitiateSessionId_FeedIdKeyMap = new HashMap<String, OperationalConsoleVb>();
			Set<String> allProcessedParentCategoryIDsSet = new HashSet();
			Set<String> allProcessedParentFeedIDsSet = new HashSet();
			
			List<OperationalConsoleVb> feedLvlPRS_TableStatusList = operationalConsoleDao.getAllFeedLvlStatusFromPRS_CtrlTable();
			if (feedLvlPRS_TableStatusList != null && feedLvlPRS_TableStatusList.size() > 0) {
				List<DependencyCategoryVb> catDepConfigList = operationalConsoleDao.getAllCatLvlDependencyMaintenance();
				if (catDepConfigList != null && catDepConfigList.size() > 0) {
					/*Reinitiate click happens at the Category lvl. So the 'arrListResult' will have feeds corresponding to single category.
					Hence, we just get the CategoryID of the first obj of the list.*/
					String categoryId = feedVbList.get(0).getFeedCategory();
					/*Get all direct & indirect dependencies for this categoryId with help of recursive method*/
					Set<String> childCategoryList = getAllCatDependenciesChildren(categoryId, catDepConfigList, allProcessedParentCategoryIDsSet);
					if(childCategoryList!=null && childCategoryList.size()>0) {
						//Get 'grouped completion status' of all categories from the ETL_FEED_PROCESS_CONTROL table
						//'Grouped completion status' - indicates whether all instances of a particular category have a status of 'S' (success) or not.
						//Nested loop to identify if any of the parent_categories of the current category_id is not having status 'S' in completion_status column
						for(String childCatId: childCategoryList) {
							for(OperationalConsoleVb feedLvlStatusVb: feedLvlPRS_TableStatusList) {
								if(childCatId.equalsIgnoreCase(feedLvlStatusVb.getFeedCategory()) 
										&& !"S".equalsIgnoreCase(feedLvlStatusVb.getCompletionStatus())) {
									String sessionId_FeedIdComboKey = feedLvlStatusVb.getSessionId()+Constants.UNDERSCORE+feedLvlStatusVb.getFeedId();
									if (reinitiateSessionId_FeedIdKeyMap.get(sessionId_FeedIdComboKey) == null) {
										reinitiateSessionId_FeedIdKeyMap.put(sessionId_FeedIdComboKey, feedLvlStatusVb);
//										allProcessedParentFeedIDsSet.add(feedLvlStatusVb.getFeedId());
										feedVbFromChildCategoryList.add(feedLvlStatusVb);
									}
								}
							}
						}
					}
				}
				
				//Get all Feed Lvl Dependency Maintenance from ETL_FEED_DEPENDENCIES table
				List<DependencyFeedVb> feedDepConfigList = operationalConsoleDao.getAllFeedLvlDependencyMaintenance();
				if (feedDepConfigList != null && feedDepConfigList.size() > 0) {
					//Unlike category_lvl, we need to check for every feed. Hence, we loop the 'arrListResult'
					identifyFeedChildren(feedVbList, feedDepConfigList, feedLvlPRS_TableStatusList, reinitiateSessionId_FeedIdKeyMap, allProcessedParentFeedIDsSet);
					identifyFeedChildren(feedVbFromChildCategoryList, feedDepConfigList, feedLvlPRS_TableStatusList, reinitiateSessionId_FeedIdKeyMap, allProcessedParentFeedIDsSet);
				}
			}
			List<OperationalConsoleVb> valuesList = new ArrayList<>(reinitiateSessionId_FeedIdKeyMap.values());
			
			feedVbList.addAll(valuesList);
			exceptionCode = operationalConsoleDao.reInitiateFeedLvl(feedVbList);
			
			/*
			for (OperationalConsoleVb operationalConsoleVb : feedVbList) {
//				exceptionCode = operationalConsoleDao.reInitiateFeedLvl(operationalConsoleVb);
			}*/
			return exceptionCode;
		} catch (RuntimeCustomException rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				logger.error("Reinitiate Exception " + rex.getCode().getErrorMsg());
			}
			exceptionCode = rex.getCode();
			exceptionCode.setErrorMsg(rex.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
	}
	
	
	
	public void identifyFeedChildren(List<OperationalConsoleVb> feedVbList, List<DependencyFeedVb> feedDepConfigList,
			List<OperationalConsoleVb> feedLvlPRS_TableStatusList, Map<String, OperationalConsoleVb> reinitiateSessionId_FeedIdKeyMap,
			Set<String> allProcessedParentFeedIDsSet) {
		for (OperationalConsoleVb feedVb : feedVbList) {
			String feedId = feedVb.getFeedId();
			/*Get all direct & indirect dependencies for this feedId with help of recursive method*/
			Set<String> childFeedList = getAllFeedDependenciesChildren(feedId, feedDepConfigList, allProcessedParentFeedIDsSet);
			if (childFeedList != null && childFeedList.size() > 0) {
				//Get 'grouped completion status' of all feeds from the ETL_FEED_PROCESS_CONTROL table
				//'Grouped completion status' - indicates whether all instances of a particular feed have a status of 'S' (success) or not.
				//Nested loop to identify if any of the parent_feeds of the current feedId is not having status 'S' in completion_status column
				for(String childFeedId: childFeedList) {
					for(OperationalConsoleVb feedLvlStatusVb: feedLvlPRS_TableStatusList) {
						if(childFeedId.equalsIgnoreCase(feedLvlStatusVb.getFeedId()) 
								&& !"S".equalsIgnoreCase(feedLvlStatusVb.getCompletionStatus())) {
							String sessionId_FeedIdComboKey = feedLvlStatusVb.getSessionId()+Constants.UNDERSCORE+feedLvlStatusVb.getFeedId();
							if (reinitiateSessionId_FeedIdKeyMap.get(sessionId_FeedIdComboKey) == null) {
								reinitiateSessionId_FeedIdKeyMap.put(sessionId_FeedIdComboKey, feedLvlStatusVb);
							}
						}
					}
				}
			}
		}
	}
	
	
	

	/*public ExceptionCode terminate(OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = null;
		TelnetConnection telnetConnection = null;
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				return terminateSecured(vObject);
			} else if ("N".equalsIgnoreCase(securedFtp)){
				return terminateSecuredWithoutSftp(vObject);
			}
		} catch (Exception rex) {
			// ex.printStackTrace();
			exceptionCode = CommonUtils.getResultObject("ETL Feed Process Control", Constants.ERRONEOUS_OPERATION,
					"Terminate", "");
			return exceptionCode;
		} finally {
			if (telnetConnection != null && telnetConnection.isConnected()) {
				telnetConnection.disconnect();
				telnetConnection = null;
			}
		}
		return getQueryResults(vObject);
	}*/
	
	public ExceptionCode terminate(List<OperationalConsoleVb> vObjects) {
		ExceptionCode exceptionCode = new ExceptionCode();
		TelnetConnection telnetConnection = null;
		try {
			if ("Y".equalsIgnoreCase(securedFtp)) {
				for (OperationalConsoleVb operationalConsoleVb : vObjects) {
					exceptionCode = terminateSecured(operationalConsoleVb);
				}
			} else if ("N".equalsIgnoreCase(securedFtp)){
				for (OperationalConsoleVb operationalConsoleVb : vObjects) {
					exceptionCode = terminateSecuredWithoutSftp(operationalConsoleVb);
				}
			}
		} catch (Exception rex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				rex.printStackTrace();
			}
			exceptionCode = CommonUtils.getResultObject("ETL Feed Process Control", Constants.ERRONEOUS_OPERATION,
					"Terminate", "");
			return exceptionCode;
		} finally {
			if (telnetConnection != null && telnetConnection.isConnected()) {
				telnetConnection.disconnect();
				telnetConnection = null;
			}
		}
		return exceptionCode;
	}

	@SuppressWarnings("unchecked")
	private ExceptionCode terminateSecured(OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Session session = null;
		Channel channel = null;
		ChannelSftp sftpChannel = null;
		try {
			List<OperationalConsoleVb> lProcessCtrlList = new ArrayList<OperationalConsoleVb>();
			if ("2".equalsIgnoreCase(vObject.getGrid())) {
				lProcessCtrlList = vObject.getChilds();
			} else {
				lProcessCtrlList = operationalConsoleDao.getQueryResults(vObject, 0);
			}

			for (OperationalConsoleVb feedVb : lProcessCtrlList) {
				String folderName = feedVb.getFeedId().toUpperCase() + "-" + feedVb.getFeedCategory().toUpperCase()
						+ "-" + feedVb.getSessionId() + "-" + feedVb.getVersionNumber() + "-"
						+ feedVb.getIterationCount();
				String pid = null;

				if ((!"S".equalsIgnoreCase(feedVb.getCompletionStatus())) && (!"E".equalsIgnoreCase(feedVb.getCompletionStatus()))) {
					if ("I".equalsIgnoreCase(feedVb.getCompletionStatus())) {
						JSch jsch = new JSch();
						session = jsch.getSession(userName, hostName, Integer.parseInt(port));
						session.setPassword(passWord);
						java.util.Properties config = new java.util.Properties();
						config.put("StrictHostKeyChecking", "no");
						session.setConfig(config);
						session.connect();
						channel = session.openChannel("sftp");
						channel.connect();
						sftpChannel = (ChannelSftp) channel;
						InputStream ins = null;

						if (CommonUtils.isFileExist(sftpChannel, uploadDir + "/" + folderName + "/input.JSON")) {
							if ("DB_LINK".equalsIgnoreCase(feedVb.getFeedType())) {
								if (CommonUtils.isFileExist(sftpChannel, uploadDir + "/" + folderName + "/PID.txt")) {
									ins = sftpChannel.get(uploadDir + "/" + folderName + "/PID.txt");
									pid = IOUtils.toString(ins);
								}
							} else {
								Channel pidChannel = session.openChannel("exec");
								((ChannelExec) pidChannel).setCommand(
										"ps -ef | grep spark | grep -v grep | grep -i 'ETLExtractionEng_Transformation.jar' | grep -i '"
												+ folderName + "' | awk '{print $2}'");

								pidChannel.setInputStream(null);
								((ChannelExec) pidChannel).setErrStream(System.err);
								InputStream pidStream = pidChannel.getInputStream();
								pidChannel.connect();
								pid = IOUtils.toString(pidStream);
								if (pid != null) {
									pid = pid.trim().replaceAll("\n", "");
								}
							}
						}
						if (ValidationUtil.isValid(pid)) {
							String killCommand = "kill -9 " + pid;

							Channel killChannel = session.openChannel("exec");
							((ChannelExec) killChannel).setCommand(killCommand);
							killChannel.setInputStream(null);
							((ChannelExec) killChannel).setErrStream(System.err);
							InputStream killStream = killChannel.getInputStream();
							killChannel.connect();

							byte[] tmp = new byte[1024];
							while (true) {
								while (killStream.available() > 0) {
									int i = killStream.read(tmp, 0, 1024);
									if (i < 0)
										break;
								}
								if (killChannel.isClosed()) {
									if (killStream.available() > 0)
										continue;
									break;
								}
								try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
								}
							}
							killChannel.disconnect();
						}
						String errorMsg = null;
						if (ValidationUtil.isValid(pid)) {
							if (!"DB_LINK".equalsIgnoreCase(feedVb.getFeedType())) {
								Channel pidChannel = session.openChannel("exec");
								((ChannelExec) pidChannel).setCommand(
										"ps -ef | grep spark | grep -v grep | grep -i 'ETLExtractionEng_Transformation.jar' | grep -i '"
												+ folderName + "' | awk '{print $2}'");
								pidChannel.setInputStream(null);
								((ChannelExec) pidChannel).setErrStream(System.err);
								InputStream pidStream = pidChannel.getInputStream();
								pidChannel.connect();
								pid = IOUtils.toString(pidStream);
								if (ValidationUtil.isValid(pid) || StringUtils.hasText(pid)) {
									errorMsg = "Unable to terminate";
								}
							}
						}
						if (ValidationUtil.isValid(errorMsg) && "Unable to terminate".equalsIgnoreCase(errorMsg)) {
							throw new RuntimeCustomException("Unable to terminate");
						}
						// Update into Process Control Table

						/*
						 * String currentProcess =
						 * getEquivalentStatusForCurrentProcess(feedVb.getCurrentProcess());
						 * feedVb.setCurrentProcess(currentProcess);
						 */
					}
					feedVb.setCurrentProcess("TERMINATED");
					feedVb.setTerminatedBy(CustomContextHolder.getContext().getVisionId());
					feedVb.setCompletionStatus("E");
					operationalConsoleDao.updateRecordForTerminationStatus(feedVb);
				}
				if (sftpChannel != null) {
					sftpChannel.exit();
				}
				ETLFeedProcessControlVb processControlVb = new ETLFeedProcessControlVb();
				processControlVb.setCountry(feedVb.getCountry());
				processControlVb.setLeBook(feedVb.getLeBook());
				processControlVb.setFeedId(feedVb.getFeedId());
				processControlVb.setFeedCategory(feedVb.getFeedCategory());
				processControlVb.setSessionId(feedVb.getSessionId());
				processControlVb.setEventType("TERMINATE");

				exceptionCode = operationalConsoleDao.getAlertDetailsOfFeed(processControlVb);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					if ("Y".equalsIgnoreCase(processControlVb.getAlertFlag())
							&& ValidationUtil.isValid(exceptionCode.getResponse()))
						processControlVb.setAlertVbMap((Map<String, ETLAlertVb>) exceptionCode.getResponse());
				}
				if (processControlVb.getAlertVbMap() != null && processControlVb.getAlertVbMap().size() > 0) {
					if (ValidationUtil.isValid(processControlVb.getEventType())
							&& processControlVb.getAlertVbMap().get(processControlVb.getEventType()) != null) {
						operationalConsoleDao.performAlertPush(processControlVb, processControlVb.getEventType());
					}
				}
			}

		} catch (Exception rex) {
			exceptionCode = CommonUtils.getResultObject("ETL Feed Process Control", Constants.ERRONEOUS_OPERATION,
					"Terminate", rex.getMessage());
			return exceptionCode;
		} finally {
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				channel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
	
	private ExceptionCode terminateSecuredWithoutSftp(OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<OperationalConsoleVb> lProcessCtrlList = new ArrayList<OperationalConsoleVb>();
			if ("2".equalsIgnoreCase(vObject.getGrid())) {
				lProcessCtrlList = vObject.getChilds();
			} else {
				lProcessCtrlList = operationalConsoleDao.getQueryResults(vObject, 0);
			}
			for (OperationalConsoleVb feedVb : lProcessCtrlList) {
				String folderName = feedVb.getFeedId().toUpperCase() + "-" + feedVb.getFeedCategory().toUpperCase()
						+ "-" + feedVb.getSessionId() + "-" + feedVb.getVersionNumber() + "-"
						+ feedVb.getIterationCount();
				
				String pid = "";
				String psCommand = "ps -ef | grep spark | grep -v grep | grep -i 'ETLExtractionEng_Transformation.jar' | grep -i '"
						+ folderName + "' | awk '{print $2}'";
				
				if ((!"S".equalsIgnoreCase(feedVb.getCompletionStatus())) && (!"E".equalsIgnoreCase(feedVb.getCompletionStatus()))) {
					if ("I".equalsIgnoreCase(feedVb.getCompletionStatus())) {
						if (CommonUtils.isFileExist(uploadDir + "/" + folderName + "/input.JSON")) {
							if ("DB_LINK".equalsIgnoreCase(feedVb.getFeedType())) {
								if (CommonUtils.isFileExist(uploadDir + "/" + folderName + "/PID.txt")) {
									pid = readTxtFromFile(uploadDir + "/" + folderName + "/PID.txt");
								}
							} else {
								ProcessBuilder psProcessBuilder = new ProcessBuilder("/bin/bash", "-c", psCommand);
								Process psProcess = psProcessBuilder.start();
								InputStream psInputStream = psProcess.getInputStream();
								pid = new BufferedReader(new InputStreamReader(psInputStream)).lines()
										.collect(Collectors.joining("\n"));
							}
							pid = pid.trim();
						}

						if (ValidationUtil.isValid(pid) && !pid.isEmpty()) {
							String killCommand = "kill -9 " + pid;
							ProcessBuilder killProcessBuilder = new ProcessBuilder("/bin/bash", "-c", killCommand);
							Process killProcess = killProcessBuilder.start();
							int exitCode = killProcess.waitFor();
							if (exitCode == 0) {
								logger.info("Successfully killed the Spark process with PID: " + pid);
							} else {
								logger.info("Failed to kill the Spark process with PID: " + pid);
							}
						}
						String errorMsg = null;
						if (ValidationUtil.isValid(pid)) {
							if (!"DB_LINK".equalsIgnoreCase(feedVb.getFeedType())) {
								logger.error(psCommand);
								ProcessBuilder psProcessBuilder = new ProcessBuilder("/bin/bash", "-c", psCommand);
								Process psProcess = psProcessBuilder.start();
								InputStream psInputStream = psProcess.getInputStream();
								pid = new BufferedReader(new InputStreamReader(psInputStream)).lines()
										.collect(Collectors.joining("\n"));
								pid = pid.trim();
								if (ValidationUtil.isValid(pid) || StringUtils.hasText(pid)) {
									// throw new RuntimeCustomException("Unable to terminate");
									errorMsg = "Unable to terminate";
								}
							}

						}

						if (ValidationUtil.isValid(errorMsg) && "Unable to terminate".equalsIgnoreCase(errorMsg)) {
							throw new RuntimeCustomException("Unable to terminate");
						}
					}

					// Update into Process Control Table

					/*
					 * String currentProcess =
					 * getEquivalentStatusForCurrentProcess(feedVb.getCurrentProcess());
					 * feedVb.setCurrentProcess(currentProcess);
					 */
					feedVb.setCurrentProcess("TERMINATED");
					feedVb.setTerminatedBy(CustomContextHolder.getContext().getVisionId());
					feedVb.setCompletionStatus("E");
					operationalConsoleDao.updateRecordForTerminationStatus(feedVb);
				}
				
				ETLFeedProcessControlVb processControlVb = new ETLFeedProcessControlVb();
				processControlVb.setCountry(feedVb.getCountry());
				processControlVb.setLeBook(feedVb.getLeBook());
				processControlVb.setFeedId(feedVb.getFeedId());
				processControlVb.setFeedCategory(feedVb.getFeedCategory());
				processControlVb.setSessionId(feedVb.getSessionId());
				processControlVb.setEventType("TERMINATE");
				
				exceptionCode = operationalConsoleDao.getAlertDetailsOfFeed(processControlVb);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					if ("Y".equalsIgnoreCase(processControlVb.getAlertFlag())
							&& ValidationUtil.isValid(exceptionCode.getResponse()))
						processControlVb.setAlertVbMap((Map<String, ETLAlertVb>) exceptionCode.getResponse());
				}
				if (processControlVb.getAlertVbMap() != null && processControlVb.getAlertVbMap().size() > 0) {
					if (ValidationUtil.isValid(processControlVb.getEventType())
							&& processControlVb.getAlertVbMap().get(processControlVb.getEventType()) != null) {
						operationalConsoleDao.performAlertPush(processControlVb, processControlVb.getEventType());
					}
				}
			}
		} catch (Exception e) {
			exceptionCode = CommonUtils.getResultObject("ETL Feed Process Control", Constants.ERRONEOUS_OPERATION,
					"Terminate", e.getMessage());
			return exceptionCode;
		} 
		return exceptionCode;
	}
	

	private String getEquivalentStatusForCurrentProcess(String currentProcess) {

		switch (currentProcess) {
		case "SUBMITTED":
			currentProcess = "TERMINATED";
			break;

		case "DEPCHECK":
		case "DEPHOLD":
			currentProcess = "DEPERR";
			break;

		case "EXTIP":
			currentProcess = "EXTERR";
			break;

		case "HKIP":
			currentProcess = "HKERR";
			break;

		case "LOADIP":
			currentProcess = "LOADERR";
			break;

		case "POSTEXTIP":
			currentProcess = "POSTEXTERR";
			break;

		case "PREEXTIP":
			currentProcess = "PREEXTERR";
			break;

		case "PREREQIP":
			currentProcess = "PREREQERR";
			break;

		case "READCHECK":
		case "READHOLD":
			currentProcess = "READERR";
			break;

		case "SELFHOLD":
			currentProcess = "SELFERR";
			break;

		case "TRANSIP":
			currentProcess = "TRANSERR";
			break;

		default:
			currentProcess = "TERMINATED";
			break;
		}
		return currentProcess;

	}

	/*private ExceptionCode terminateSecuredCategoryOld(ExceptionCode pExceptionCode,
			List<OperationalConsoleVb> processCtrl) {
		ExceptionCode exceptionCode = new ExceptionCode();
		OperationalConsoleVb vObj = (OperationalConsoleVb) pExceptionCode.getOtherInfo();
		Session session = null;
		Channel channel = null;
		HashMap<String, String> mapDir = new HashMap<String, String>();
		try {
			List<OperationalConsoleVb> lProcessCtrlList = new ArrayList<OperationalConsoleVb>(processCtrl);
			if ("2".equalsIgnoreCase(vObj.getGrid())) {
				int j = 0;
				String[] dirNameLst = new String[lProcessCtrlList.size()];
				for (OperationalConsoleVb lOperationalConsoleVb : lProcessCtrlList) {
					dirNameLst[j] = lOperationalConsoleVb.getCountry().toUpperCase() + "-"
							+ lOperationalConsoleVb.getLeBook().toUpperCase() + "@-@"
							+ lOperationalConsoleVb.getFeedId().toUpperCase() + "-"
							+ lOperationalConsoleVb.getFeedCategory().toUpperCase() + "-"
							+ lOperationalConsoleVb.getSessionId() + "-" + lOperationalConsoleVb.getIterationCount();
					mapDir.put(lOperationalConsoleVb.getFeedCategory(), dirNameLst[j]);
					j++;
				}
			} else {
				for (OperationalConsoleVb lOperationalConsoleVb : lProcessCtrlList) {
					List<OperationalConsoleVb> feedLst = operationalConsoleDao.getQueryResults(lOperationalConsoleVb,
							0, true);
					String[] dirNameLst = new String[feedLst.size()];
					int j = 0;
					for (OperationalConsoleVb lOperationalConsoleVb1 : feedLst) {
						dirNameLst[j] = lOperationalConsoleVb1.getCountry().toUpperCase() + "-"
								+ lOperationalConsoleVb1.getLeBook().toUpperCase() + "@-@"
								+ lOperationalConsoleVb1.getFeedId().toUpperCase() + "-"
								+ lOperationalConsoleVb1.getFeedCategory().toUpperCase() + "-"
								+ lOperationalConsoleVb1.getSessionId() + "-"
								+ lOperationalConsoleVb1.getIterationCount();
						mapDir.put(lOperationalConsoleVb1.getFeedId() + lOperationalConsoleVb1.getFeedCategory()
								+ lOperationalConsoleVb1.getSessionId(), dirNameLst[j]);
						j++;
					}
				}
			}
			JSch jsch = new JSch();
			session = jsch.getSession(userName, hostName, Integer.parseInt(port));
			session.setPassword(passWord);
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			BufferedInputStream bis = null;
			InputStream ins = null;
			try {
				for (Map.Entry<String, String> entry : mapDir.entrySet()) {
					String[] entValue = entry.getValue().toString().split("@-@");
					String[] clValue = entValue[0].split("-");

					ins = sftpChannel.get(uploadDir + "/" + entValue[1] + "/PID.txt");
					bis = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
					if (ValidationUtil.isValid(bis)) {
						Runtime.getRuntime().exec("taskkill /F /PID " + bis.toString() + "");
					}
					 Get alert details - Start 
					String[] getData = entValue[1].split("-");
					ETLFeedProcessControlVb processControlVb = new ETLFeedProcessControlVb();
					processControlVb.setCountry(clValue[0]);
					processControlVb.setLeBook(clValue[1]);
					processControlVb.setFeedId(getData[0]);
					processControlVb.setFeedCategory(getData[1]);
					processControlVb.setSessionId(getData[2]);
					processControlVb.setEventType("TERMINATE");
					exceptionCode = operationalConsoleDao.getAlertDetailsOfFeed(processControlVb);
					if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
						if ("Y".equalsIgnoreCase(processControlVb.getAlertFlag())
								&& ValidationUtil.isValid(exceptionCode.getResponse()))
							processControlVb.setAlertVbMap((Map<String, ETLAlertVb>) exceptionCode.getResponse());
					}
					if (processControlVb.getAlertVbMap() != null && processControlVb.getAlertVbMap().size() > 0) {
						if (ValidationUtil.isValid(processControlVb.getEventType())
								&& processControlVb.getAlertVbMap().get(processControlVb.getEventType()) != null) {
							operationalConsoleDao.performAlertPush(processControlVb, processControlVb.getEventType());
						}
					}
					 Get alert details - End 
				}
			} catch (SftpException e) {
				// e.printStackTrace();
				exceptionCode = CommonUtils.getResultObject("ETL Feed Process Control", Constants.ERRONEOUS_OPERATION,
						"Terminate", e.getMessage());
				exceptionCode.setResponse(processCtrl);
				exceptionCode.setOtherInfo(vObj);
				return exceptionCode;
			}

			sftpChannel.exit();
		} catch (Exception rex) {
			// ex.printStackTrace();
			exceptionCode = CommonUtils.getResultObject("ETL Feed Process Control", Constants.ERRONEOUS_OPERATION,
					"Terminate", rex.getMessage());
			exceptionCode.setResponse(processCtrl);
			exceptionCode.setOtherInfo(vObj);
			return exceptionCode;
		} finally {
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				channel = null;
			}
			if (session != null && session.isConnected()) {
				session.disconnect();
				session = null;
			}
		}
		return getQueryResults(vObj);
	}*/

	public static int execUnxComAndGetResult(String command, Channel channel, Session session) {
		int returnVal = -1;
		try {
//	        JSch jsch = new JSch();
//	        Session session = jsch.getSession(userName, hostName, Integer.parseInt(port));
//	        session.setPassword(passWord);
//	        session.setConfig("StrictHostKeyChecking", "no");
//	        session.connect();
//	        Channel channel = session.openChannel("shell");
//			channel.connect();
			OutputStream inputstream_for_the_channel = channel.getOutputStream();
			PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
			InputStream in = channel.getInputStream();
			commander.println("cd " + scriptDir);
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
				try {
					Thread.sleep(1);
				} catch (Exception e) {
					// e.printStackTrace();
					return returnVal;
				}
			}
			// channel.disconnect();
			// session.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			return returnVal;
		}
		return returnVal;
	}

	public ExceptionCode fileDownload(OperationalConsoleVb vObj) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String[] arrDirNames = new String[1];
		String fName = vObj.getCountry() + "" + vObj.getLeBook() + "" + vObj.getFeedCategory().toUpperCase() + ""
				+ vObj.getSessionId();
		List<OperationalConsoleVb> auditDetailslst = new ArrayList<OperationalConsoleVb>();
		BufferedInputStream in = null;
		FileOutputStream fos = null;
		vObj.setActionType("Download");
		exceptionCode = doValidate(vObj);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			return exceptionCode;
		}
		try {
			if ("2".equalsIgnoreCase(vObj.getGrid())) {
				arrDirNames[0] = vObj.getFeedId().toUpperCase() + "-" + vObj.getFeedCategory().toUpperCase() + "-"
						+ vObj.getSessionId() + "-" + vObj.getVersionNumber() + "-" + vObj.getIterationCount();
				auditDetailslst = operationalConsoleDao.getAuditDetailsBasedOnFeed(vObj.getFeedCategory().toUpperCase(),
						vObj.getFeedId().toUpperCase(), vObj.getSessionId(), vObj.getVersionNumber(),
						vObj.getIterationCount());
			} else {
				List<OperationalConsoleVb> categoryBasedFeedLst = operationalConsoleDao.getQueryResults(vObj, 0);
				arrDirNames = new String[categoryBasedFeedLst.size()];
				int k = 0;
				for (OperationalConsoleVb vobject : categoryBasedFeedLst) {
					arrDirNames[k] = vobject.getFeedId().toUpperCase() + "-" + vobject.getFeedCategory().toUpperCase()
							+ "-" + vobject.getSessionId() + "-" + vobject.getVersionNumber() + "-"
							+ vobject.getIterationCount();
					auditDetailslst = operationalConsoleDao.getAuditDetailsBasedOnFeed(
							vObj.getFeedCategory().toUpperCase(), vobject.getFeedId().toUpperCase(),
							vObj.getSessionId(), vobject.getVersionNumber(), vobject.getIterationCount());
					k++;
				}
			}
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			File file = null;
			if ("Y".equalsIgnoreCase(securedFtp)) {
				exceptionCode = downloadFilesFromSFTP(arrDirNames, fName, auditDetailslst);
			} else {
				exceptionCode = downloadFilesWithoutSFTP(arrDirNames, fName, auditDetailslst);
			}
			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			} else {
				in = (BufferedInputStream) exceptionCode.getResponse();
				file = new File(filePath + fName + ".zip");
				fos = new FileOutputStream(file);
				int bit = 4096;
				while ((bit) >= 0) {
					bit = in.read();
					fos.write(bit);
				}
			}
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(1);
			exceptionCode.setErrorMsg("Sucess");
			exceptionCode.setRequest(fName + ".zip");
			exceptionCode.setResponse(filePath);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		} finally {
			try {
				in.close();
			} catch (IOException e) {
			}
			try {
				fos.close();
			} catch (IOException e) {
			}
		}
		return exceptionCode;
	}

	public ExceptionCode downloadFilesFromSFTP(String[] arrDirNames, String fName,
			List<OperationalConsoleVb> auditDetailslst) {
		ExceptionCode exceptionCode = new ExceptionCode();
		BufferedInputStream bufferedInputStream = null;
		Session session = null;
		ChannelSftp sftpChannel = null;
		OutputStream inputstream_for_the_channel = null;
		Channel channel = null;
		auditDetailslst.get(0).setActionType("Download");
		exceptionCode = doValidate(auditDetailslst.get(0));
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			return exceptionCode;
		}
		try {
			JSch jsch = new JSch();
			session = jsch.getSession(userName, hostName, Integer.parseInt(port));
			session.setPassword(passWord);
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();

			channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			channel = session.openChannel("shell");
			inputstream_for_the_channel = channel.getOutputStream();
			PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
			channel.connect();
			uploadDir = uploadDir.trim();
			try {
				sftpChannel.cd(uploadDir);
				sftpChannel.mkdir(fName);
				sftpChannel.cd(uploadDir + "/" + fName + "/");
			} catch (Exception e) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg(
						"Path not found - [" + uploadDir + "/" + fName + "/] and [" + sftpChannel.pwd() + "]");
				return exceptionCode;
			}
			String inputdataFileName = "audit.txt";
			/*
			 * File lFile1 = new File(uploadDir+"/"+fName+"/"); if (lFile1.exists()) {
			 * lFile1.delete(); }
			 */
			sftpChannel.cd(uploadDir + "/" + fName + "/");
			for (int k = 0; k < auditDetailslst.size(); k++) {
				String desc = auditDetailslst.get(k).getAuditDesc() + "\n";
				InputStream in = new ByteArrayInputStream(desc.getBytes());
				sftpChannel.put(in, inputdataFileName, sftpChannel.APPEND);
			}
			sftpChannel.pwd();
			sftpChannel.cd(uploadDir);
			for (int i = 0; i < arrDirNames.length; i++) {
				commander.println("cd " + uploadDir + "\n");
				commander.println("cp -R " + arrDirNames[i] + " " + fName + " \n");
				commander.println("cd " + uploadDir + "/" + fName + "/" + arrDirNames[i] + "\n");
				commander.println("rm -f input.JSON\n");
				commander.println("rm -f AppID.txt\n");
				commander.println("rm -f PID.txt\n");
				commander.println("cd " + uploadDir + "\n");
			}
			commander.println("tar cvf " + uploadDir + "/" + fName + ".tar " + fName + "\n");
			commander.println("exit");
			commander.close();
			do {
				Thread.sleep(1000);
			} while (!channel.isEOF());
			try {
				InputStream stream = sftpChannel.get(uploadDir + "/" + fName + ".tar");
				bufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(stream)));
			} catch (SftpException ex) {
			}
			exceptionCode.setResponse(bufferedInputStream);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		} finally {
			try {
				sftpChannel.cd(uploadDir + "/");
				sftpChannel.rm(fName + ".tar");
				sftpChannel.pwd();
				sftpChannel.cd(uploadDir + "/" + fName);
				recursiveFolderDelete(sftpChannel, sftpChannel.pwd());
			} catch (SftpException e1) {
				e1.printStackTrace();
			}
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				channel = null;
			}
			sftpChannel.exit();
			if (sftpChannel != null && sftpChannel.isConnected()) {
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

	public ExceptionCode downloadFilesWithoutSFTP(String[] arrDirNames, String fName,
			List<OperationalConsoleVb> auditDetailslst) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String inputdataFileName = "audit.txt";
		File zipFile = new File(uploadDir, fName + ".zip");
	//	File zipFileFolder = new File(uploadDir, fName);
		auditDetailslst.get(0).setActionType("Download");
		exceptionCode = doValidate(auditDetailslst.get(0));
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			return exceptionCode;
		}
		try {
			FileOutputStream fos = new FileOutputStream(zipFile);
			ZipOutputStream zos = new ZipOutputStream(fos);
			String auditContent = "";
			
			for (OperationalConsoleVb auditDetail : auditDetailslst) {
				auditContent += auditDetail.getAuditDesc() + "\n";
			}
			
			ByteArrayInputStream auditInputStream = new ByteArrayInputStream(auditContent.getBytes());
			String relativePath = fName + File.separator + inputdataFileName;

			try {
				BufferedInputStream bufferedInputStream = new BufferedInputStream(auditInputStream);
				zos.putNextEntry(new ZipEntry(relativePath));
				byte[] buffer = new byte[1024];
				int bytesRead;
				while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
					zos.write(buffer, 0, bytesRead);
				}
				zos.closeEntry();
			} catch (Exception e) {
			}
			
			for (String dirName : arrDirNames) {
				File dir = new File(uploadDir, dirName);
				if (dir.exists()) {

					File targetDir = new File(uploadDir, fName+ File.separator + dirName);
					if (!targetDir.exists() && !targetDir.mkdirs()) {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						return exceptionCode;
					}
					File[] filesToRemove = { new File(targetDir, "input.JSON"), new File(targetDir, "AppID.txt"),
							new File(targetDir, "PID.txt") };

					copyDirectoryContents(dir, targetDir);

					for (File fileToRemove : filesToRemove) {
						if (fileToRemove.exists() && !fileToRemove.delete()) {
						}
					}
					Path directoryPath = Paths.get(targetDir.toURI());
					if (!Files.exists(directoryPath)) {
					    Files.createDirectories(directoryPath);
					}
					Files.walk(directoryPath).forEach(filePath -> {
						if (Files.isRegularFile(filePath)) {
							String entryName = fName + File.separator + dirName + File.separator
									+ directoryPath.relativize(filePath).toString();
							try {
								ZipEntry zipEntry = new ZipEntry(entryName);
								zos.putNextEntry(zipEntry);
								BufferedInputStream bufferedInputStream = new BufferedInputStream(
										Files.newInputStream(filePath));
								byte[] buffer = new byte[1024];
								int bytesRead;
								while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
									zos.write(buffer, 0, bytesRead);
								}
								zos.closeEntry();
							} catch (IOException e) {
							}
						}
					});
				}
			}
			zos.close(); // Close the ZIP output stream after all directories have been processed
		    fos.close();
			// Create a BufferedInputStream for the ZIP file.
			BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(zipFile));
			exceptionCode.setResponse(bufferedInputStream);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		} finally {
			try {
	       //     recursiveFolderDelete(zipFileFolder);
				recursiveFolderDelete(uploadDir + "/" + fName);
	            int maxRetries = 3;
	            int retryCount = 0;

	            while (retryCount < maxRetries) {
	                if (zipFile.delete()) {
	                    // Deletion succeeded
	                    break;
	                } else {
	                    // Deletion failed, wait and then retry
	                    Thread.sleep(1000); // Wait for 1 second before retrying
	                    retryCount++;
	                }
	            }
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		}
		return exceptionCode;
	}
	private void copyDirectoryContents(File sourceDir, File targetDir) {
	    if (sourceDir.isDirectory()) {
	        if (!targetDir.exists()) {
	            targetDir.mkdirs();
	        }
	        String[] files = sourceDir.list();
	        if (files != null) {
	            for (String file : files) {
	                File srcFile = new File(sourceDir, file);
	                File destFile = new File(targetDir, file);
	                copyDirectoryContents(srcFile, destFile);
	            }
	        }
	    } else {
	        try (FileInputStream fis = new FileInputStream(sourceDir);
	             FileOutputStream fos = new FileOutputStream(targetDir)) {
	        	byte[] buffer = new byte[1024];
	            int length;
	            while ((length = fis.read(buffer)) > 0) {
	                fos.write(buffer, 0, length);
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	}

	/*private void recursiveFolderDelete(File folder) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    recursiveFolderDelete(file);
                }
            }
        }
        folder.delete();
    }*/
	
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
	
	/*public void copyDirectory(Path sourceDir, Path targetDir) throws IOException {
        Files.walkFileTree(sourceDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path targetDirPath = targetDir.resolve(sourceDir.relativize(dir));
                if (!Files.exists(targetDirPath)) {
                    Files.createDirectory(targetDirPath);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path targetFilePath = targetDir.resolve(sourceDir.relativize(file));
                Files.copy(file, targetFilePath, StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }*/

	private FileAttribute<Set<PosixFilePermission>> createFileAttribute(Set<PosixFilePermission> permissions) {
		return PosixFilePermissions.asFileAttribute(permissions);
	}

	@SuppressWarnings("unchecked")
	private static void recursiveFolderDelete(ChannelSftp channelSftp, String path) throws SftpException {
		// List source directory structure.
		Collection<ChannelSftp.LsEntry> fileAndFolderList = channelSftp.ls(path);

		// Iterate objects in the list to get file/folder names.
		for (ChannelSftp.LsEntry item : fileAndFolderList) {
			if (!item.getAttrs().isDir()) {
				channelSftp.rm(path + "/" + item.getFilename()); // Remove file.
			} else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) { // If it is a subdir.
				try {
					// removing sub directory.
					channelSftp.rmdir(path + "/" + item.getFilename());
				} catch (Exception e) { // If subdir is not empty and error occurs.
					// Do lsFolderRemove on this subdir to enter it and clear its contents.
					recursiveFolderDelete(channelSftp, path + "/" + item.getFilename());
				}
			}
		}
		channelSftp.rmdir(path); // delete the parent directory after empty
	}

	public ExceptionCode getOperationalConsoleGrid2Data(OperationalConsoleVb queryPopupObj) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setVerifReqDeleteType(queryPopupObj);
			doFormateDataForQuery(queryPopupObj);
			List<OperationalConsoleVb> arrListResult = operationalConsoleDao.getOperationalConsoleGrid2Data(queryPopupObj);
			if (arrListResult != null && arrListResult.size() > 0) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(arrListResult);
			} else {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
			}
			exceptionCode.setOtherInfo(queryPopupObj);
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the getAllQueryPopupResultGrid2 results.", ex);
			}
			return null;
		}
	}

	@Override
	protected AbstractDao<OperationalConsoleVb> getScreenDao() {
		return operationalConsoleDao;

	}

	@Override
	protected void setAtNtValues(OperationalConsoleVb vObject) {

	}

	@Override
	protected void setVerifReqDeleteType(OperationalConsoleVb vObject) {
		/* Commented by Prakashika on 15_December_2023
		 * 
		 *  ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) commonDao.findVerificationRequiredAndStaticDelete("ETL_FEED_PROCESS_CONTROL");*/
		// vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		// vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());
		vObject.setStaticDelete(false);
		vObject.setVerificationRequired(false);
	}

	public ExceptionCode getQuerySingletonTabResults(OperationalConsoleVb vObject) {
		ArrayList collTemp = operationalConsoleDao.getSingletonTabResults(vObject);
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}

	public ExceptionCode getQueryFeedNotificationDetailResults(OperationalConsoleVb vObject) {
		List<OperationalConsoleVb> collTemp = operationalConsoleDao.getNotificationTabResults(vObject);
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}

	public ExceptionCode getQueryFeedDepErrorResults(OperationalConsoleVb vObject) {
		List<OperationalConsoleVb> collTemp = operationalConsoleDao.getDepErrorResults(vObject);
		if (collTemp.size() == 0) {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} else {
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setResponse(collTemp);
			return exceptionCode;
		}
	}

	public List<OperationalConsoleVb> findFeed(OperationalConsoleVb vObject) {
		List<OperationalConsoleVb> dependencylst = null;
		try {
			dependencylst = operationalConsoleDao.findFeed(vObject);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return dependencylst;
	}

	@SuppressWarnings("unchecked")
	public ExceptionCode getImmediateParentDependency(ConsoleDependencyFeedVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String dependencyFlag = operationalConsoleDao.getDEPENDENCY_CHECK_FLAG(vObject);
		vObject.setDependencyFlag(dependencyFlag);
		//vObject.setDependencyFlag(ValidationUtil.isValid(dependencyFlag) ? dependencyFlag : "D");
		if("I".equalsIgnoreCase(vObject.getDependencyFlag())){
			return exceptionCode;
		} else {
			exceptionCode = operationalConsoleDao.getDependencyDetailsSingleLvl(vObject, new LinkedHashMap<String, ConsoleDependencyFeedVb>());
		}
		if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			if (exceptionCode.getResponse() != null) {
				Map<String, ConsoleDependencyFeedVb> currentFeedDetailsMap = (Map<String, ConsoleDependencyFeedVb>) exceptionCode.getResponse();
				List<ConsoleDependencyFeedVb> returnList = new ArrayList<ConsoleDependencyFeedVb>(currentFeedDetailsMap.values());
				if (returnList != null && returnList.size() > 0) {
					ConsoleDependencyFeedVb currentFeed = returnList.get(0);
					if (currentFeed.getParentFeedList() != null && currentFeed.getParentFeedList().size() > 0) {
						returnList.addAll(currentFeed.getParentFeedList());
						exceptionCode.setResponse(returnList);
					}
				}
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}

	public ExceptionCode getFullDependencyHierarchy(ConsoleDependencyFeedVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			LinkedHashMap<Integer, List<String>> levelBasedDependencyMap = new LinkedHashMap<Integer, List<String>>();
			LinkedHashMap<String, ConsoleDependencyFeedVb> dependencyFeedDetailsMap = new LinkedHashMap<String, ConsoleDependencyFeedVb>();

			LinkedHashMap<String, Integer> feedToLevelMap = new LinkedHashMap<String, Integer>();
			String rootNodeKey = vObject.getFeedId() + HIPHEN + vObject.getSessionId();
			levelBasedDependencyMap.put(0, new ArrayList<>(Arrays.asList(new String[] { rootNodeKey })));
			exceptionCode = getDependencyDetailsLoop(vObject, dependencyFeedDetailsMap, feedToLevelMap, 1);

			/*
			 * feedToLevelMap - This Map contains the greatest level number for every
			 * Feed_ID-Session_ID combination Loop this Map and form another map where Level
			 * is the Key and values are List of Nodes under the LevelIndex ->
			 * levelBasedDependencyMap
			 */
			for (Map.Entry<String, Integer> entry : feedToLevelMap.entrySet()) {
				String nodeKeyStr = entry.getKey();
				if (!rootNodeKey.equalsIgnoreCase(nodeKeyStr)) {
					int levelIndex = entry.getValue();
					if (levelBasedDependencyMap.get(levelIndex) == null) {
						String[] nodeKey = { nodeKeyStr };
						levelBasedDependencyMap.put(levelIndex, new ArrayList<>(Arrays.asList(nodeKey)));
					} else {
						List<String> nodeKeyList = levelBasedDependencyMap.get(levelIndex);
						nodeKeyList.add(nodeKeyStr);
						levelBasedDependencyMap.put(levelIndex, nodeKeyList);
					}
				}
			}

			/*
			 * Merging dependencyFeedDetailsMap with Node-Level information from
			 * levelBasedDependencyMap
			 */
			for (Entry<Integer, List<String>> entry : levelBasedDependencyMap.entrySet()) {
				int levelIndex = entry.getKey();
				List<String> nodeKeyLst = entry.getValue();
				for (String nodeKey : nodeKeyLst) {
					ConsoleDependencyFeedVb depFeedVb = dependencyFeedDetailsMap.get(nodeKey);
					depFeedVb.setLevel(String.valueOf(levelIndex));
					dependencyFeedDetailsMap.put(nodeKey, depFeedVb);
				}
			}

			/*
			 * Print output for validation
			 * 
			 * for (Map.Entry<Integer, List<String>> entry :
			 * levelBasedDependencyMap.entrySet()) { //
			 * System.out.println("levelIndex:"+entry.getKey()); //
			 * System.out.println(Arrays.toString((entry.getValue()).toArray())); //
			 * System.out.println("*************************"); //
			 * System.out.println("*************************"); //
			 * System.out.println("*************************"); }
			 */
			exceptionCode.setResponse(new ArrayList<ConsoleDependencyFeedVb>(dependencyFeedDetailsMap.values()));
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode getDependencyDetailsLoop(ConsoleDependencyFeedVb vObject,
			LinkedHashMap<String, ConsoleDependencyFeedVb> dependencyFeedDetailsMap,
			LinkedHashMap<String, Integer> feedToLevelMap, int levelIndex) {
		// Get immediate parent of current vObject(Current Feed)
		ExceptionCode exceptionCode = operationalConsoleDao.getDependencyDetailsSingleLvl(vObject,
				dependencyFeedDetailsMap);
		if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			if (exceptionCode.getResponse() != null) {
				LinkedHashMap<String, ConsoleDependencyFeedVb> currentFeedDetailsMap = (LinkedHashMap<String, ConsoleDependencyFeedVb>) exceptionCode
						.getResponse();
				for (Map.Entry<String, ConsoleDependencyFeedVb> entry : currentFeedDetailsMap.entrySet()) {
					ConsoleDependencyFeedVb depFeedVb = entry.getValue();
					List<ConsoleDependencyFeedVb> parentFeedVbList = depFeedVb.getParentFeedList();
					// Loop parent feeds of current Feed
					for (ConsoleDependencyFeedVb parentFeedVb : parentFeedVbList) {

						String nodeKey = parentFeedVb.getFeedId() + HIPHEN + parentFeedVb.getSessionId();
						/* Add level index to the current Parent Feed - start */
						if (feedToLevelMap.get(nodeKey) == null) {
							feedToLevelMap.put(nodeKey, levelIndex);
						} else {
							int previousLvl = feedToLevelMap.get(nodeKey);
							if (previousLvl < levelIndex)
								feedToLevelMap.put(nodeKey, levelIndex);
						}
						/* Add level index to the current Parent Feed - end */

						// If the parent details of current parent feed not available, then get the
						// details and add them to dependencyFeedDetailsMap Map
						if (dependencyFeedDetailsMap.get(nodeKey) == null) {
							getDependencyDetailsLoop(parentFeedVb, dependencyFeedDetailsMap, feedToLevelMap,
									levelIndex + 1);
						}

					}
				}

			} else {
				return exceptionCode;
			}
		} else {
			throw new RuntimeCustomException(exceptionCode);
		}

		return exceptionCode;
	}

	/*
	 * @SuppressWarnings("unused") private LinkedHashMap<Integer, List<String>>
	 * sortDependencyMapByLevel(LinkedHashMap<String, ConsoleDependencyFeedVb>
	 * dependencyFeedDetailsMap, ConsoleDependencyFeedVb vObject) { String[] nodeKey
	 * = {vObject.getFeedId()+HIPHEN+vObject.getSessionId()}; LinkedHashMap<Integer,
	 * Map<String, ConsoleDependencyFeedVb>> lvlSortedDependencyMap
	 * lvlSortedDependencyMap.put(0, Arrays.asList(nodeKey));
	 * 
	 * return lvlSortedDependencyMap; }
	 * 
	 * private void addChildernOfCurrentFeedVbToNextLevel(ConsoleDependencyFeedVb
	 * currentFeed, LinkedHashMap<Integer, Map<String, ConsoleDependencyFeedVb>>
	 * lvlSortedDependencyMap) {
	 * 
	 * }
	 */

	@Override
	protected ExceptionCode doValidate(OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
		String srtRestrion = getCommonDao().getRestrictionsByUsers("PilotConsole", operation);
		if (!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation + " " + Constants.userRestrictionMsg);
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
	
	public ExceptionCode getAllChildDataForReinitiate(OperationalConsoleVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setVerifReqDeleteType(vObject);
			doFormateDataForQuery(vObject);
			List<OperationalConsoleVb> arrListResult = operationalConsoleDao.getAllChildDataForReinitiate(vObject);
			if (arrListResult != null && arrListResult.size() > 0) {
				//Is CATEGORY lvl parent dependency avaiable with other than 'S' as COMPLETION_STATUS in PROCESS_CONTROL_TABLE
				boolean isCatLvlParentAvailable = false;
				//Get all Category Lvl Dependency Maintenance from ETL_FEED_CATEGORY_DEPENDENCIES table
				List<DependencyCategoryVb> catDepConfigList = operationalConsoleDao.getAllCatLvlDependencyMaintenance();
				if (catDepConfigList != null && catDepConfigList.size() > 0) {
					/*Reinitiate click happens at the Category lvl. So the 'arrListResult' will have feeds corresponding to single category.
					Hence, we just get the CategoryID of the first obj of the list.*/
					String categoryId = arrListResult.get(0).getFeedCategory();
					/*Get all direct & indirect dependencies for this categoryId with help of recursive method*/
					Set<String> parentCategoryList = getAllCatDependenciesParent(categoryId, catDepConfigList);
					if(parentCategoryList!=null && parentCategoryList.size()>0) {
						//Get 'grouped completion status' of all categories from the ETL_FEED_PROCESS_CONTROL table
						//'Grouped completion status' - indicates whether all instances of a particular category have a status of 'S' (success) or not.
						List<DependencyCategoryVb> catLvlGrpStatusList = operationalConsoleDao.getCategoryGroupedStatus();
						if (catLvlGrpStatusList != null && catLvlGrpStatusList.size() > 0) {
							//Nested loop to identify if any of the parent_categories of the current category_id is not having status 'S' in completion_status column
							parentCatLoop:for(String parentCatId: parentCategoryList) {
								for(DependencyCategoryVb catLvlStatusVb: catLvlGrpStatusList) {
									if(parentCatId.equalsIgnoreCase(catLvlStatusVb.getCategoryId()) 
											&& !"Success".equalsIgnoreCase(catLvlStatusVb.getGroupedCategoryStatus())) {
										isCatLvlParentAvailable = true;
										break parentCatLoop;
									}
								}
							}
						}
					}
				}
				if(!isCatLvlParentAvailable) {
					//Get all Feed Lvl Dependency Maintenance from ETL_FEED_DEPENDENCIES table
					List<DependencyFeedVb> feedDepConfigList = operationalConsoleDao.getAllFeedLvlDependencyMaintenance();
					if (feedDepConfigList != null && feedDepConfigList.size() > 0) {
						//Get 'grouped completion status' of all feeds from the ETL_FEED_PROCESS_CONTROL table
						//'Grouped completion status' - indicates whether all instances of a particular feed have a status of 'S' (success) or not.
						List<DependencyFeedVb> feedLvlGrpStatusList = operationalConsoleDao.getFeedGroupedStatus();
						if (feedLvlGrpStatusList != null && feedLvlGrpStatusList.size() > 0) {
							//Unlike category_lvl, we need to check for every feed. Hence, we loop the 'arrListResult'
							for (OperationalConsoleVb feedVb : arrListResult) {
								String feedId = feedVb.getFeedId();
								//To avoid deadlock
								Set<String> alreadyProcessedFeedIds = new HashSet<>();
								
								/*Get all direct & indirect dependencies for this feedId with help of recursive method*/
								Set<String> parentFeedList = getAllFeedDependenciesParent(feedId, feedDepConfigList, alreadyProcessedFeedIds);
								if (parentFeedList != null && parentFeedList.size() > 0) {
									//Nested loop to identify if any of the parent_feeds of the current feedId is not having status 'S' in completion_status column
									parentFeedLoop:for(String parentFeedId: parentFeedList) {
										for(DependencyFeedVb feedLvlStatusVb: feedLvlGrpStatusList) {
											if(parentFeedId.equalsIgnoreCase(feedLvlStatusVb.getFeedId()) 
													&& !"Success".equalsIgnoreCase(feedLvlStatusVb.getGroupedStatus())) {
												//isCatLvlParentAvailable = true;
												feedVb.setIsParentFlag("N");
												break parentFeedLoop;
											}
										}
									}
								}
							}
						}
					}
				} else {
					arrListResult.forEach(childVb -> childVb.setIsParentFlag("N"));
				}
			}
			
			if (arrListResult != null && arrListResult.size() > 0) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(arrListResult);
			} else {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
			}
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the getAllChildDataForReinitiate results.", ex);
			}
			return null;
		}
	}
	
	public Set<String> getAllCatDependenciesParent(String categoryId, List<DependencyCategoryVb> catDepConfigList) {
        Set<String> dependencies = new HashSet<>();
        getAllCatDependenciesParentRecursive(categoryId, dependencies, catDepConfigList);
        return dependencies;
    }

    // Helper method to recursively find dependencies
	// Method to identify all the dependent parent corresponding to the category
    private void getAllCatDependenciesParentRecursive(String categoryId, Set<String> dependencies, List<DependencyCategoryVb> catDepConfigList) {
    	catDepConfigList.stream()
                .filter(dependency -> dependency.getCategoryId().equalsIgnoreCase(categoryId))
                .forEach(dependency -> {
                    String parentCategoryId = dependency.getDependentCategoryId();
                    if(!parentCategoryId.equalsIgnoreCase(categoryId)) {
                    	dependencies.add(parentCategoryId);
                        getAllCatDependenciesParentRecursive(parentCategoryId, dependencies, catDepConfigList);
                    }
                });
    }
    
    public Set<String> getAllFeedDependenciesParent(String feedId, List<DependencyFeedVb> feedDepConfigList, Set<String> alreadyProcessedFeedIds) {
        Set<String> dependencies = new HashSet<>();
        getAllFeedDependenciesParentRecursive(feedId, dependencies, feedDepConfigList, alreadyProcessedFeedIds);
        return dependencies;
    }

    // Method to identify all the dependent parent corresponding to the feed
    // Helper method to recursively find dependencies
    private void getAllFeedDependenciesParentRecursive(String feedId, Set<String> dependencies, 
    		List<DependencyFeedVb> feedDepConfigList, Set<String> alreadyProcessedFeedIds) {
    	if(!alreadyProcessedFeedIds.contains(feedId)) {
    		alreadyProcessedFeedIds.add(feedId);
    		feedDepConfigList.stream()
            .filter(dependency -> dependency.getFeedId().equals(feedId))
            .forEach(dependency -> {
                String parentFeedId = dependency.getDependentFeedId();
                if(!parentFeedId.equalsIgnoreCase(feedId)) {
                	dependencies.add(parentFeedId);
                    getAllFeedDependenciesParentRecursive(parentFeedId, dependencies, feedDepConfigList, alreadyProcessedFeedIds);
                }
            });
    	}
    }
    
    
    public Set<String> getAllCatDependenciesChildren(String parentCategoryId, List<DependencyCategoryVb> catDepConfigList, Set<String> allProcessedParentCategoryIDsSet) {
        Set<String> dependencies = new HashSet<>();
        getAllCatDependenciesChildrenRecursive(parentCategoryId, dependencies, catDepConfigList, allProcessedParentCategoryIDsSet);
        return dependencies;
    }

    // Helper method to recursively find child dependencies
	// Method to identify all the dependent children corresponding to the category_id passed which is parent_category
    private void getAllCatDependenciesChildrenRecursive(String parentCategoryId, Set<String> dependencies, 
    		List<DependencyCategoryVb> catDepConfigList, Set<String> allProcessedParentCategoryIDsSet) {
    	if(!allProcessedParentCategoryIDsSet.contains(parentCategoryId)) {
    		allProcessedParentCategoryIDsSet.add(parentCategoryId);
    		catDepConfigList.stream()
            .filter(dependency -> dependency.getDependentCategoryId().equals(parentCategoryId))
            .forEach(dependency -> {
                String childCategoryId = dependency.getCategoryId();
                dependencies.add(childCategoryId);
                getAllCatDependenciesChildrenRecursive(childCategoryId, dependencies, catDepConfigList, allProcessedParentCategoryIDsSet);
            });
    	}
    }
    
    public Set<String> getAllFeedDependenciesChildren(String parentFeedId, List<DependencyFeedVb> feedDepConfigList, Set<String> allProcessedParentFeedIDsSet) {
        Set<String> dependencies = new HashSet<>();
        getAllFeedDependenciesChildrenRecursive(parentFeedId, dependencies, feedDepConfigList, allProcessedParentFeedIDsSet);
        return dependencies;
    }

    // Method to identify all the dependent children corresponding to the feed which will be parent_feed_ID
    // Helper method to recursively find dependencies
    private void getAllFeedDependenciesChildrenRecursive(String parentFeedId, Set<String> dependencies, 
    		List<DependencyFeedVb> feedDepConfigList, Set<String> allProcessedParentFeedIDsSet) {
    	if(!allProcessedParentFeedIDsSet.contains(parentFeedId)) {
    		allProcessedParentFeedIDsSet.add(parentFeedId);
    		feedDepConfigList.stream()
            .filter(dependency -> dependency.getDependentFeedId().equals(parentFeedId))
            .forEach(dependency -> {
                String childFeedId = dependency.getFeedId();
                dependencies.add(childFeedId);
                getAllFeedDependenciesChildrenRecursive(childFeedId, dependencies, feedDepConfigList, allProcessedParentFeedIDsSet);
            });
    	}
    }

	public ArrayList getVisionBusinessDay(OperationalConsoleVb vObject) {
		try {
			/*String countryLebook = vObject.getCountry() + "-" + vObject.getLeBook();
			String businessDate = commonDao.getVisionBusinessDate(countryLebook);
			arrListLocal.add(businessDate);
			Map<String, String> busDtTz = operationalConsoleDao.getBusinessDateTimeZone();
			String timeZone = busDtTz.get(countryLebook).split("\\|!#")[1];
			arrListLocal.add(timeZone); */
			CommonApiModel vb = new CommonApiModel();
			vb.setQueryId("ETL_VBD_TIMEZONE");
			vb.setParam1(vObject.getCountry());
			vb.setParam2(vObject.getLeBook());
			vb.setParam3(vObject.getFeedCategory());
			ArrayList arrListLocal = commonDao.getCommonResultDataQuery(vb);
			return arrListLocal;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
				logger.error("Exception in getting the VisionBusinessDay results.", e);
			}
			return null;
		}
	}

}