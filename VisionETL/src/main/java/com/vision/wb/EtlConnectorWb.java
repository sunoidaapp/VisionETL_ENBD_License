package com.vision.wb;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.poi.util.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;
import com.vision.dao.AbstractDao;
import com.vision.dao.EtlConnectorDao;
import com.vision.dao.EtlFeedMainDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.EtlConnectorVb;
import com.vision.vb.EtlMqColumnsVb;
import com.vision.vb.ReviewResultVb;

@Component
public class EtlConnectorWb extends AbstractDynaWorkerBean<EtlConnectorVb> {

	@Autowired
	private EtlConnectorDao etlConnectorDao;
	
	@Autowired
	private EtlFeedMainDao etlFeedMainDao;

	@Value("${ftp.hostName}")
	private String hostName;

	@Value("${ftp.userName}")
	private String userName;

	@Value("${ftp.password}")
	private String password;

	private String serverType;
	private String timezoneId;

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

	public ArrayList getPageLoadValues() {
		List collTemp = null;
		ArrayList<Object> arrListLocal = new ArrayList<Object>();
		try {
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(7); // Record Indicator
			arrListLocal.add(collTemp);
			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1); // Status
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2100); // Connector Type
			arrListLocal.add(collTemp);
			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(2101);// End Point Type
			arrListLocal.add(collTemp);
			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete("ETL_CONNECTOR");
			arrListLocal.add(collTemp);
			return arrListLocal;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
				logger.error("Exception in getting the Page load values.", ex);
			}
			return null;
		}
	}

	@Override
	protected List<ReviewResultVb> transformToReviewResults(List<EtlConnectorVb> approvedCollection,
			List<EtlConnectorVb> pendingCollection) {

		ResourceBundle rsb = CommonUtils.getResourceManger();
		if (pendingCollection != null)
			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));
		if (approvedCollection != null)
			getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));
		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();

		ReviewResultVb lCountry = new ReviewResultVb(rsb.getString("country"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getCountryDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getCountryDesc(),
				(!pendingCollection.get(0).getCountry().equals(approvedCollection.get(0).getCountry())));
		lResult.add(lCountry);

		ReviewResultVb lLeBook = new ReviewResultVb(rsb.getString("leBook"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? "" : pendingCollection.get(0).getLeBookDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getLeBookDesc(),
				(!pendingCollection.get(0).getLeBook().equals(approvedCollection.get(0).getLeBook())));
		lResult.add(lLeBook);

		ReviewResultVb lConnectorType = new ReviewResultVb(rsb.getString("connectorType"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectorTypeDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectorTypeDesc(),
				(!pendingCollection.get(0).getConnectorTypeDesc().equals(approvedCollection.get(0).getConnectorTypeDesc())));
		lResult.add(lConnectorType);

		ReviewResultVb lConnectorId = new ReviewResultVb(rsb.getString("connectorId"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectorId(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectorId(),
				(!pendingCollection.get(0).getConnectorId().equals(approvedCollection.get(0).getConnectorId())));
		lResult.add(lConnectorId);

		ReviewResultVb lConnectionName = new ReviewResultVb(rsb.getString("connectionName"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectionName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectionName(),
				(!pendingCollection.get(0).getConnectionName().equals(approvedCollection.get(0).getConnectionName())));
		lResult.add(lConnectionName);

		ReviewResultVb lConnectorScripts = new ReviewResultVb(rsb.getString("connectorScripts"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getConnectorScripts(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getConnectorScripts(),
				(!pendingCollection.get(0).getConnectorScripts()
						.equals(approvedCollection.get(0).getConnectorScripts())));
		lResult.add(lConnectorScripts);

		ReviewResultVb lEndpointType = new ReviewResultVb(rsb.getString("endpointType"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getEndpointTypeDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getEndpointTypeDesc(),
				(!pendingCollection.get(0).getEndpointTypeDesc().equals(approvedCollection.get(0).getEndpointTypeDesc())));
		lResult.add(lEndpointType);

		ReviewResultVb lConnectionStauts = new ReviewResultVb(rsb.getString("connectionStauts"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getStatusDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getStatusDesc(),
				(!pendingCollection.get(0).getStatusDesc().equals(approvedCollection.get(0).getStatusDesc())));
		lResult.add(lConnectionStauts);

		ReviewResultVb lRecordIndicator = new ReviewResultVb(rsb.getString("recordIndicator"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getRecordIndicatorDesc(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getRecordIndicatorDesc(),
				(!pendingCollection.get(0).getRecordIndicatorDesc()
						.equals(approvedCollection.get(0).getRecordIndicatorDesc())));
		lResult.add(lRecordIndicator);

		ReviewResultVb lMaker = new ReviewResultVb(rsb.getString("maker"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getMaker() == 0 ? "" : pendingCollection.get(0).getMakerName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getMaker() == 0 ? "" : approvedCollection.get(0).getMakerName(),
				(pendingCollection.get(0).getMaker() != approvedCollection.get(0).getMaker()));
		lResult.add(lMaker);

		ReviewResultVb lVerifier = new ReviewResultVb(rsb.getString("verifier"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getVerifier() == 0 ? "" : pendingCollection.get(0).getVerifierName(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getVerifier() == 0 ? ""
								: approvedCollection.get(0).getVerifierName(),
				(pendingCollection.get(0).getVerifier() != approvedCollection.get(0).getVerifier()));
		lResult.add(lVerifier);

		ReviewResultVb lDateLastModified = new ReviewResultVb(rsb.getString("dateLastModified"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateLastModified(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateLastModified(),
				(!pendingCollection.get(0).getDateLastModified()
						.equals(approvedCollection.get(0).getDateLastModified())));
		lResult.add(lDateLastModified);

		ReviewResultVb lDateCreation = new ReviewResultVb(rsb.getString("dateCreation"),
				(pendingCollection == null || pendingCollection.isEmpty()) ? ""
						: pendingCollection.get(0).getDateCreation(),
				(approvedCollection == null || approvedCollection.isEmpty()) ? ""
						: approvedCollection.get(0).getDateCreation(),
				(!pendingCollection.get(0).getDateCreation().equals(approvedCollection.get(0).getDateCreation())));
		lResult.add(lDateCreation);
		return lResult;
	}

	@Override
	protected AbstractDao<EtlConnectorVb> getScreenDao() {
		return etlConnectorDao;
	}

	@Override
	protected void setAtNtValues(EtlConnectorVb vObject) {
		vObject.setConnectorTypeAt(2100);
		vObject.setEndpointTypeAt(2101);
		vObject.setConnectionStatusNt(1);
		vObject.setRecordIndicatorNt(7);
	}

	@Override
	protected void setVerifReqDeleteType(EtlConnectorVb vObject) {
		ArrayList<CommonVb> lCommVbList = (ArrayList<CommonVb>) getCommonDao()
				.findVerificationRequiredAndStaticDelete("ETL_CONNECTOR");
		vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());
		vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());

	}

	public EtlConnectorDao getEtlConnectorDao() {
		return etlConnectorDao;
	}

	public void setEtlConnectorDao(EtlConnectorDao etlConnectorDao) {
		this.etlConnectorDao = etlConnectorDao;
	}

	public List<EtlConnectorVb> getDisplayTagList(String macroVarType) {
		try {
			return etlConnectorDao.getDisplayTagList(macroVarType);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e.getMessage());
		}
	}

	public String dynamicScriptCreation(EtlConnectorVb etlConnectorVb) {
		String macroVarType = etlConnectorVb.getMacroVarType();
		if (macroVarType.equalsIgnoreCase("ORACLE") || macroVarType.equalsIgnoreCase("MYSQL")
				|| macroVarType.equalsIgnoreCase("MSSQL") || macroVarType.equalsIgnoreCase("SYBASE")
				|| macroVarType.equalsIgnoreCase("POSTGRESSQL")) {
			etlConnectorVb.setConnectorType("S");
		} else if (macroVarType.equalsIgnoreCase("SSH")) {
			etlConnectorVb.setConnectorType("SSH");
		}
		if (etlConnectorVb.getConnectorType().equalsIgnoreCase("S")
				|| etlConnectorVb.getConnectorType().equalsIgnoreCase("SSH")) {
			StringBuffer variableScript = new StringBuffer(
					"{DATABASE_TYPE:#CONSTANT$@!" + etlConnectorVb.getMacroVarType() + "#}");
			JSONObject extractVbData = new JSONObject(etlConnectorVb);
			JSONArray eachColData = (JSONArray) extractVbData.getJSONArray("dynamicScript");
			for (int i = 0; i < eachColData.length(); i++) {
				JSONObject ss = eachColData.getJSONObject(i);
				String ch = fixJSONObject(ss);
				JSONObject extractData = new JSONObject(ch);
				String tag = extractData.getString("TAG");
				String encryption = extractData.getString("ENCRYPTION");
				String value = extractData.getString("VALUE");
				// Decode UTF-8 encoded value - Start
				try {
					value = URLDecoder.decode(value, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
				}
				// Decode UTF-8 encoded value - End
				variableScript.append("{");
				if (ValidationUtil.isValid(encryption) && encryption.equalsIgnoreCase("Yes")) {
					variableScript.append(tag + ":#ENCRYPT$@!" + value + "#");
				} else {
					variableScript.append(tag + ":#CONSTANT$@!" + value + "#");
				}
				variableScript.append("}");
			}
			return String.valueOf(variableScript);
		} /*else if (etlConnectorVb.getConnectorType().equalsIgnoreCase("SS") ) {
			for (EtlFileUploadAreaVb vb :etlConnectorVb.getChildren()) {
				if ("D".contentEquals(vb.getFileType())) {
					JSONObject extractVbData = new JSONObject(etlConnectorVb);
					JSONArray eachColData = (JSONArray) extractVbData.getJSONArray("dynamicScript");
					for (int i = 0; i < eachColData.length(); i++) {
						JSONObject ss = eachColData.getJSONObject(i);
						String ch = fixJSONObject(ss);
						JSONObject extractData = new JSONObject(ch);
						String tag = extractData.getString("TAG");
						String encryption = extractData.getString("ENCRYPTION");
						String value = extractData.getString("VALUE");
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
						variableScript.append("{");
						if (ValidationUtil.isValid(encryption) && encryption.equalsIgnoreCase("Yes")) {
							variableScript.append(tag + ":#ENCRYPT$@!" + value + "#");
						} else {
							variableScript.append(tag + ":#CONSTANT$@!" + value + "#");
						}
						variableScript.append("}");
					}
					 String.valueOf(variableScript);
				}
			}
		}*/ else {
			return null;
		}
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

	public EtlConnectorVb getSpecificConnector(EtlConnectorVb vObj) {
		List<EtlConnectorVb> collTemp = null;
		int intStatus = Constants.STATUS_PENDING;
		try {
			collTemp = etlConnectorDao.getQueryResults(vObj, intStatus);
			if (collTemp.size() == 0 && collTemp.isEmpty()) {
				intStatus = Constants.STATUS_ZERO;
				collTemp = etlConnectorDao.getQueryResults(vObj, intStatus);
			}
			return collTemp.get(0);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	public ExceptionCode fileDownload(String fileNames, String fileExtension) {
		ExceptionCode exceptionCode = null;
		try {
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			BufferedInputStream in = null;
			File file = null;
			FileOutputStream fos = null;
			String fName = "";
			int number = 0;
			// fileNames = fileNames.substring(0,fileNames.length()-4);
			String[] arrFileNames = fileNames.split(",");
			if (arrFileNames.length == 1) {
				fName = arrFileNames[0] + ".txt";
				file = new File(filePath + fName);
				fos = new FileOutputStream(file);
				in = downloadFilesFromSFTP(fileNames);
				number = 1;
			} else {
				throw new FileNotFoundException("File not found on the Server.");
			}
			int bit = 4096;
			while ((bit) >= 0) {
				bit = in.read();
				fos.write(bit);
			}
			in.close();
			fos.close();
			exceptionCode = CommonUtils.getResultObject("", 1, "", "");
			exceptionCode.setRequest(fName);
//			exceptionCode.setResponse(number);
			exceptionCode.setResponse(filePath);

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
		}
		return exceptionCode;
	}

	private FTPClient getConnection() throws IOException {
		FTPClient ftpClient = new FTPClient();
		FTPClientConfig conf = new FTPClientConfig(getServerType());
		conf.setServerTimeZoneId(getTimezoneId());
		ftpClient.configure(conf);
		return ftpClient;
	}

	public BufferedInputStream downloadFilesFromSFTP(String pFileNames) {
		BufferedInputStream bufferedInputStream = null;
		String[] fileNames = pFileNames.split(",");
		// setUploadDownloadDirFromDB();
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
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);

			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			String filePAth = "/root/Downloads/" + fileNames[0] + ".txt";
			if (fileNames.length == 1) {
				try {
					InputStream ins = sftpChannel.get(filePAth);
					bufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(IOUtils.toByteArray(ins)));
				} catch (SftpException e) {
					// e.printStackTrace();
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

	public String getServerType() {
		return serverType;
	}

	public void setServerType(String serverType) {
		this.serverType = serverType;
	}

	public String getTimezoneId() {
		return timezoneId;
	}

	public void setTimezoneId(String timezoneId) {
		this.timezoneId = timezoneId;
	}

	public List<EtlConnectorVb> getSpecificConnectorByHashList(EtlConnectorVb vObj) {
		List<EtlConnectorVb> dbScriptPopList = null;
		String etlConnectorScript = null;
		try {
			int intStatus = 1;
			etlConnectorScript = etlConnectorDao.getConnectorScripts(vObj, intStatus);
			if (!ValidationUtil.isValid(etlConnectorScript)) {
				intStatus = 0;
				etlConnectorScript = etlConnectorDao.getConnectorScripts(vObj, intStatus);
			}
			if (ValidationUtil.isValid(etlConnectorScript)) {
				if (vObj != null) {
					if ("FILE".equalsIgnoreCase(vObj.getScriptType())) {
						dbScriptPopList = new ArrayList<EtlConnectorVb>();
						vObj.setFileName(CommonUtils.getValueWithoutEncrypt(etlConnectorScript, "NAME"));
						vObj.setExtension(CommonUtils.getValueWithoutEncrypt(etlConnectorScript, "EXTENSION"));
						vObj.setDelimiter(CommonUtils.getValueWithoutEncrypt(etlConnectorScript, "DELIMITER"));
						dbScriptPopList.add(vObj);
					} else {
						vObj.setMacroVarType(CommonUtils.getValueWithoutEncrypt(etlConnectorScript, "DATABASE_TYPE"));
						if (!ValidationUtil.isValid(vObj.getMacroVarType())) {
							throw new RuntimeCustomException("Data not maintained properly");
						} else {
							dbScriptPopList = getDisplayTagList(vObj.getMacroVarType());
							for (EtlConnectorVb data : dbScriptPopList) {
								data.setTagValue(CommonUtils.getValueWithoutEncrypt(etlConnectorScript, data.getTagName()));
								data.setMacroVarType(vObj.getMacroVarType());
								data.setMacroVar(vObj.getMacroVar());
								data.setDescription(vObj.getDescription());
							}
						}
					}
				}
			}

			return dbScriptPopList;
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			throw new RuntimeCustomException(e.getMessage());
		}
	}

	public boolean validateFileExtension(String fileExtension, String fileName, String connectorDir) {
		if (fileExtension.equalsIgnoreCase(".xls") || fileExtension.equalsIgnoreCase(".xlsx")
				|| fileExtension.equalsIgnoreCase(".json") || fileExtension.equalsIgnoreCase(".xml")
				|| fileExtension.equalsIgnoreCase(".csv") || fileExtension.equalsIgnoreCase(".avro")
				|| fileExtension.equalsIgnoreCase(".parquet")) {
			return true;
		} else {
			return false;
		}

	}

	public String getConnectorScripts(EtlConnectorVb vObj) throws DataAccessException {
		String etlConnectorScript = null;
		try {
			int intStatus = 1;
			etlConnectorScript = etlConnectorDao.getConnectorScripts(vObj, intStatus);
			if (!ValidationUtil.isValid(etlConnectorScript)) {
				intStatus = 0;
				etlConnectorScript = etlConnectorDao.getConnectorScripts(vObj, intStatus);
			}
			return etlConnectorScript;
		} catch (Exception e) {
			// e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public ExceptionCode validateSqlQuery(EtlConnectorVb vObj, String etlConnectorScript,
			String[] hashArr, String[] hashValArr) {
	//	logger.error("sqlMainQuery1:"+etlConnectorScript);

		ExceptionCode exceptionCode = new ExceptionCode();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		String level = "";
		exceptionCode = CommonUtils.getConnection(etlConnectorScript);
		if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			con = (Connection) exceptionCode.getResponse();
		} else {
			return exceptionCode;
		}
		String dbSetParam1 = CommonUtils.getValue(etlConnectorScript, "DB_SET_PARAM1");
		String dbSetParam2 = CommonUtils.getValue(etlConnectorScript, "DB_SET_PARAM2");
		String dbSetParam3 = CommonUtils.getValue(etlConnectorScript, "DB_SET_PARAM3");
		String dataBaseType = CommonUtils.getValue(etlConnectorScript, "DATABASE_TYPE");

		String stgQuery = "";
		String sessionId = String.valueOf(System.currentTimeMillis());
		String stgTableName1 = "TVC_" + sessionId + "_STG_1";
		String stgTableName2 = "TVC_" + sessionId + "_STG_2";
		String stgTableName3 = "TVC_" + sessionId + "_STG_3";
		String sqlMainQuery = "";
		try {
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			if (ValidationUtil.isValid(dbSetParam1)) {
				level = "DB Param 1";
				stmt.executeUpdate(dbSetParam1);
			}
			if (ValidationUtil.isValid(dbSetParam2)) {
				level = "DB Param 2";
				stmt.executeUpdate(dbSetParam2);
			}
			if (ValidationUtil.isValid(dbSetParam3)) {
				level = "DB Param 3";
				stmt.executeUpdate(dbSetParam3);
			}
			Pattern pattern = Pattern.compile("#(.*?)#");
			Matcher matcher = null;
			if (ValidationUtil.isValid(vObj.getStgQuery1())) {
				stgQuery = vObj.getStgQuery1();
				matcher = pattern.matcher(stgQuery);
				while (matcher.find()) {
					if ("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName1);
					if ("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName2);
					if ("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName3);
				}
				level = "Staging 1";
				stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
				stmt.executeUpdate(stgQuery);
			}
			if (ValidationUtil.isValid(vObj.getStgQuery2())) {
				stgQuery = vObj.getStgQuery2();
				matcher = pattern.matcher(stgQuery);
				while (matcher.find()) {
					if ("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName1);
					if ("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName2);
					if ("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName3);
				}
				level = "Staging 2";
				stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
				stmt.executeUpdate(stgQuery);
			}
			if (ValidationUtil.isValid(vObj.getStgQuery3())) {
				stgQuery = vObj.getStgQuery3();
				matcher = pattern.matcher(stgQuery);
				while (matcher.find()) {
					if ("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName1);
					if ("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName2);
					if ("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName3);
				}
				level = "Staging 3";
				stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
				stmt.executeUpdate(stgQuery);
			}
			sqlMainQuery = vObj.getSqlQuery();
			matcher = pattern.matcher(sqlMainQuery);
			
			sqlMainQuery =sqlMainQuery.replaceAll("#PROCESS_DATE#", "01-01-1900 00:00:00");
			sqlMainQuery =sqlMainQuery.replaceAll("#LAST_EXTRACTION_DATE#", "01-01-1900 00:00:00");
			
			boolean validSql = sqlMainQuery.toUpperCase().startsWith("SELECT") || sqlMainQuery.toUpperCase().startsWith("WITH");
			
			if(!validSql) {
				exceptionCode.setErrorMsg("The provided sql statement is not Valid select query");
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			} 
			sqlMainQuery = "select * FROM ("+sqlMainQuery.trim()+") TMP_ALIAS WHERE 1=0";
			while (matcher.find()) {
				if ("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
					sqlMainQuery = sqlMainQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName1);
				if ("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
					sqlMainQuery = sqlMainQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName2);
				if ("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
					sqlMainQuery = sqlMainQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName3);
				else
					sqlMainQuery = sqlMainQuery.replaceAll("#" + matcher.group(1) + "#",
							"#" + String.valueOf(matcher.group(1)).toUpperCase().replaceAll("\\s", "\\_") + "#");
			}
			level = "Main Query";


			sqlMainQuery = sqlMainQuery.replaceAll("#PROCESS_DATE#", "01-01-1900 00:00:00");
			sqlMainQuery = sqlMainQuery.replaceAll("#LAST_EXTRACTION_DATE#", "01-01-1900 00:00:00");
			
			LinkedHashMap<String, String> before2AfterChangeWordMap = new LinkedHashMap<String, String>();
			LinkedHashMap<String, String> before2AfterChangeIndexMap = new LinkedHashMap<String, String>();
			int index = 0;
			if (hashArr != null && hashValArr != null && hashArr.length == hashValArr.length) {
				for (String variable : hashArr) {
					int varGuessIndex = sqlMainQuery.indexOf("#" + variable + "#");
					while (varGuessIndex >= 0) {
						String preString = sqlMainQuery.substring(0, varGuessIndex);
						String wholeWordBefChange = "";
						String wholeWordAfterChange = "";
						int startIndexOfMainSQL = 0;
						startIndexOfMainSQL = preString.indexOf(" ") != -1 ? preString.lastIndexOf(" ") : -1;
						int endIndexOfMainSQL = 0;
						endIndexOfMainSQL = sqlMainQuery.indexOf(" ", varGuessIndex + 1);

						/* Get whole word */
						wholeWordBefChange = (startIndexOfMainSQL != -1 && endIndexOfMainSQL != -1)
								? sqlMainQuery.substring(startIndexOfMainSQL, endIndexOfMainSQL)
								: (startIndexOfMainSQL == -1 && endIndexOfMainSQL != -1)
										? sqlMainQuery.substring(0, endIndexOfMainSQL)
										: sqlMainQuery.substring(startIndexOfMainSQL, sqlMainQuery.length());
						wholeWordBefChange = wholeWordBefChange.trim();
						wholeWordAfterChange = wholeWordBefChange
								.replaceFirst("#" + hashArr[index] + "#", hashValArr[index]).trim();

						String storingIndex = "";
						if (before2AfterChangeIndexMap.get(wholeWordAfterChange) != null)
							storingIndex = before2AfterChangeIndexMap.get(wholeWordAfterChange);
						storingIndex = storingIndex + varGuessIndex + ",";
						before2AfterChangeIndexMap.put(wholeWordAfterChange.toUpperCase(), storingIndex);
						before2AfterChangeWordMap.put(wholeWordAfterChange.toUpperCase(), wholeWordBefChange);

						/* change the value in main query */
						if (startIndexOfMainSQL != -1 && endIndexOfMainSQL != -1)
							sqlMainQuery = sqlMainQuery.substring(0, startIndexOfMainSQL) + " " + wholeWordAfterChange
									+ " " + sqlMainQuery.substring(endIndexOfMainSQL, sqlMainQuery.length());
						else if (startIndexOfMainSQL == -1)
							sqlMainQuery = wholeWordAfterChange + " "
									+ sqlMainQuery.substring(endIndexOfMainSQL, sqlMainQuery.length());
						else if (endIndexOfMainSQL == -1)
							sqlMainQuery = sqlMainQuery.substring(0, startIndexOfMainSQL) + " " + wholeWordAfterChange;

						varGuessIndex = sqlMainQuery.indexOf("#" + variable + "#", varGuessIndex + 1);
					}
					index++;
				}
			}
			sqlMainQuery = CommonUtils.replaceHashTag(sqlMainQuery, hashArr, hashValArr);
		//	logger.error("sqlMainQuery:"+sqlMainQuery);

			rs = stmt.executeQuery(sqlMainQuery);

			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			/*
			 * LinkedHashMap<String, Integer> columnsMetaData = new LinkedHashMap<String,
			 * Integer>(); for (int i = 1; i <= columnCount; i++) { String columnName =
			 * before2AfterChangeWordMap.get(metaData.getColumnName(i).toUpperCase());
			 * if(columnName!=null) columnsMetaData.put(columnName, i); else
			 * columnsMetaData.put(metaData.getColumnName(i), i); }
			 */
			Map<String, String> dataTypeMap = etlFeedMainDao.getDataTypeMap(dataBaseType);
			if(dataTypeMap.size()==0){
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Current select TAG_NAME, DISPLAY_NAME from MACROVAR_TAGGING t where MACROVAR_NAME = 'DATA_TYPE' AND UPPER(MACROVAR_TYPE) = UPPER('"+dataBaseType+"') is not maintained");
				return exceptionCode;
			}
	            
			List<EtlMqColumnsVb> oldColList = null;

			Map<String, EtlMqColumnsVb> mapObj = new HashMap<String, EtlMqColumnsVb>();

			List<EtlMqColumnsVb> columnsMetaDataList = new ArrayList<>();

			oldColList = vObj.getEtlMqColumnsList();

			if (oldColList != null && oldColList.size() > 0) {
				for (EtlMqColumnsVb etlMqColumnsVb : oldColList) {
					mapObj.put(etlMqColumnsVb.getColumnName().toUpperCase(), etlMqColumnsVb);
				}
			}
			for (int i = 1; i <= columnCount; i++) {
				String colName = metaData.getColumnName(i).toUpperCase();
				String colType = metaData.getColumnTypeName(i);

				if (mapObj.get(colName) != null) {
					EtlMqColumnsVb vObject = mapObj.get(colName);
					vObject.setColumnId(i);
					columnsMetaDataList.add(vObject);
				} else {
					EtlMqColumnsVb newColumnVb = new EtlMqColumnsVb();
					newColumnVb.setColumnName(colName);
					newColumnVb.setColumnId(i);
					if(dataTypeMap!=null && !dataTypeMap.isEmpty() && ValidationUtil.isValid(dataTypeMap.get(colType))){
						newColumnVb.setColumnDatatype(dataTypeMap.get(colType));
					}else{
						newColumnVb.setColumnDatatype("Y");
					}
					columnsMetaDataList.add(newColumnVb);
				}
			}
			
			/*for (int i = 1; i <= columnCount; i++) {
				String columnName = before2AfterChangeWordMap.get(metaData.getColumnName(i).toUpperCase());
				EtlMqColumnsVb etlMqColumnsVb = new EtlMqColumnsVb();
				if (columnName != null) {
					etlMqColumnsVb.setColumnName(columnName);
					etlMqColumnsVb.setColumnId(i);
				} else {
					etlMqColumnsVb.setColumnName(metaData.getColumnName(i));
					etlMqColumnsVb.setColumnId(i);
				}
				columnsMetaDataList.add(etlMqColumnsVb);
			}*/
			
			exceptionCode.setRequest(columnsMetaDataList);

			if (ValidationUtil.isValid(vObj.getPostQuery())) {
				stgQuery = vObj.getPostQuery();
				matcher = pattern.matcher(stgQuery);
				while (matcher.find()) {
					if ("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName1);
					if ("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName2);
					if ("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#" + matcher.group(1) + "#", stgTableName3);
				}
				level = "Post Query";
				try {
					stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
					stmt.executeUpdate(stgQuery);
				} catch (Exception e) {
					if ("Y".equalsIgnoreCase(enableDebug)) {
						e.printStackTrace();
					}
					exceptionCode.setOtherInfo(vObj);
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Warning - Post Query Execution Failed - Cause:" + e.getMessage());
					return exceptionCode;
				}
			}

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(
					"Validation failed at level - " + level + " - Cause:" + e.getMessage() + "[" + sqlMainQuery + "]");
			exceptionCode.setOtherInfo(vObj);
			return exceptionCode;
		} finally {
			try {
				if (ValidationUtil.isValid(stgTableName1))
					stmt.executeUpdate("DROP TABLE " + stgTableName1 + " PURGE");
			} catch (Exception e) {
			}
			try {
				if (ValidationUtil.isValid(stgTableName2))
					stmt.executeUpdate("DROP TABLE " + stgTableName2 + " PURGE");
			} catch (Exception e) {
			}
			try {
				if (ValidationUtil.isValid(stgTableName3))
					stmt.executeUpdate("DROP TABLE " + stgTableName3 + " PURGE");
			} catch (Exception e) {
			}
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (Exception e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;

	}

	public ExceptionCode validateDBLink(ExceptionCode ec, String macroVarScript) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection con = (Connection) ec.getResponse();
		try {
			String dbLinkName = CommonUtils.getValue(macroVarScript, "DB_LINK_NAME");
			if (ValidationUtil.isValid(dbLinkName)) {
				String dbType = CommonUtils.getValue(macroVarScript, "DATABASE_TYPE");
				String dbLinkValidationSql = etlConnectorDao.getDbLinkValidationQuery(dbType);
				if (ValidationUtil.isValid(dbLinkValidationSql)) {
					dbLinkValidationSql = dbLinkValidationSql.replaceAll("#DB_LINK_NAME#", dbLinkName);
					PreparedStatement pstmt = con.prepareStatement(dbLinkValidationSql);
					pstmt.execute();
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg(
							"Physical connection is Successful. But unable to Validate 'DB-Link Name' as the maintenance is not proper. Contanct system-admin");
					return exceptionCode;
				}
			}
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			} 
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(
					"Physical connection is Successful. But invalid 'DB-Link Name' - Cause [" + e.getMessage() + "]");
			return exceptionCode;
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
	@Override
	public ExceptionCode doValidate(EtlConnectorVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = vObject.getActionType();
	//	String srtRestrion = getCommonDao().getRestrictionsByUsers("etlConnector", operation);
		String srtRestrion = getCommonDao().getRestrictionsByUsers("EtlConnector", operation);
		if(!"Y".equalsIgnoreCase(srtRestrion)) {
			exceptionCode = new ExceptionCode();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(operation +" "+Constants.userRestrictionMsg);
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
}
