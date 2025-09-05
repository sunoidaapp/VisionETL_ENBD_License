package com.vision.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.stereotype.Controller;

import com.vision.util.CommonUtils;
import com.vision.util.ValidationUtil;

public class VisionJavaFilesCreations {

	public static void main(String[] args) {  
		try{
			//step1 load the driver class  
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");  

			//step2 create  the connection object  
			//Connection con=DriverManager.getConnection("jdbc:oracle:thin:@10.16.1.106:1521:VISDB","VISION_ETL","Vision123");
			
			String url = "jdbc:sqlserver://10.16.1.38;instance=VISIONBISQL2019;port=52866;DatabaseName=Vision_ETL;encrypt=false;trustServerCertificate=false";
			
			Connection con=DriverManager.getConnection(url,"Vision","Vision@123");
			  
			//step3 create the statement object  
			// System.out.println("1");
//			Statement stmt=con.createStatement();
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			// System.out.println("2");
			String tableName = "PRD_BEARER_TOKEN";
			
			String filePath = "D:\\JavaFiles\\";
			
			String queyr = "SELECT UPPER(RTRIM(COLUMN_NAME)) COLUMN_NAME, NULLABLE,CASE WHEN DATA_PRECISION IS NULL THEN DATA_LENGTH ELSE DATA_PRECISION END PRECISION, \r\n"
					+ "							 DATA_TYPE, DATA_SCALE, DATA_DEFAULT FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"+tableName+"' \r\n"
					+ "                             ORDER BY COLUMN_ID";
			String indexQuey = "Select COLUMN_NAME from USER_IND_COLUMNS Where TABLE_NAME = '"+tableName+"'\r\n"
					+ "					 And INDEX_NAME = (Select RTrim(INDEX_NAME) INDEX_NAME From USER_INDEXES Where TABLE_NAME = '"+tableName+"' \r\n"
					+ "                     And (INDEX_NAME Like '%_PK' OR INDEX_NAME Like '%_IDX'))";
			ResultSet rs=stmt.executeQuery(queyr);
			String fileName = ValidationUtil.toTitleCase(tableName).replaceAll("_", "");
			filePath = filePath+fileName+"\\";
			File dir = new File(filePath);
			if (!dir.exists()) {
				dir.mkdir();
			}
			// System.out.println("4");
			VisionJavaFilesCreations visionJavaFilesCreations = new VisionJavaFilesCreations();
			visionJavaFilesCreations.createVbObject(tableName, rs, filePath);
			rs.beforeFirst();
			visionJavaFilesCreations.createMessageProperties(tableName, rs, filePath);
			rs.beforeFirst();
			visionJavaFilesCreations.createWbObject(tableName, rs, filePath);
			rs.beforeFirst();
			visionJavaFilesCreations.createControllerObject(tableName, rs, filePath);
			rs.beforeFirst();
			Statement stmt1 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			ResultSet rsIndex=stmt1.executeQuery(indexQuey);
			visionJavaFilesCreations.createDaofil(tableName, tableName, rs, rsIndex, filePath, true, true);
			//step5 close the connection object  
			
			rs.close();
			rsIndex.close();
			con.close();  
			  
		}catch(Exception e){ 
			// System.out.println(e);
		}  
	}
	
	public void createMessageProperties(String fileName, ResultSet rs, String filePath){
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+".prop";
		try {
//			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File(filePath+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				if(!("'RECORD_INDICATOR_NT', '0RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					String strLabel =  ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", " ");
					StringBuffer stringBuffer = new StringBuffer();
					String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					stringBuffer.append(""+javaSetValue+" = "+strLabel);
//					// System.out.println(stringBuffer);
					outHeader.println(stringBuffer.toString());
				}
			}
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("Message Properties file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}

	public void createWbObject(String fileName, ResultSet rs, String filePath){
		String actFile = fileName;
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"Wb.java";
		try {
			File fileHeader = new File(filePath+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			outHeader.print("package com.vision.wb;\n\n");
			
			outHeader.print("import java.util.ArrayList;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import java.util.List;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import java.util.ResourceBundle;");
			outHeader.print(System.lineSeparator());
			outHeader.print("");
			outHeader.print(System.lineSeparator());
			outHeader.print("import org.apache.log4j.Logger;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import org.springframework.beans.factory.annotation.Autowired;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import org.springframework.stereotype.Controller;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import com.vision.dao.AbstractDao;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import com.vision.dao."+fileName+"Dao;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import com.vision.util.CommonUtils;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import com.vision.vb.CommonVb;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import com.vision.vb."+fileName+"Vb;");
			outHeader.print(System.lineSeparator());
			outHeader.print("import com.vision.vb.ReviewResultVb;");
			outHeader.print(System.lineSeparator());
			outHeader.print("@Controller");
			outHeader.print(System.lineSeparator());
			outHeader.print("public class "+fileName+"Wb extends AbstractDynaWorkerBean<"+fileName+"Vb>{\n\n");
			
			StringBuffer stringBuffer = new StringBuffer();
			String daoFileName = (fileName.substring(0, 1)).toLowerCase() + fileName.substring(1,fileName.length());
			
			stringBuffer.append("	@Autowired ");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	private "+fileName+"Dao "+daoFileName+"Dao;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public static Logger logger = Logger.getLogger("+fileName+"Wb.class);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ArrayList getPageLoadValues(){");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		List collTemp = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ArrayList<Object> arrListLocal = new ArrayList<Object>();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try{");
			stringBuffer.append(System.lineSeparator());
			rs.beforeFirst();			
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String defaultValue = rs.getString("DATA_DEFAULT");
				if(!ValidationUtil.isValid(defaultValue)) {
					defaultValue = "";
				}
				if(dbColumnName.toUpperCase().endsWith("_AT")) {
					stringBuffer.append("			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab("+defaultValue.trim()+");");
					stringBuffer.append(System.lineSeparator());	
					stringBuffer.append("			arrListLocal.add(collTemp);");
					stringBuffer.append(System.lineSeparator());	
				}else if(dbColumnName.toUpperCase().endsWith("_NT")){
					stringBuffer.append("			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab("+defaultValue.trim()+");");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("			arrListLocal.add(collTemp);");
					stringBuffer.append(System.lineSeparator());	
				}
			}
			
			
			stringBuffer.append("			collTemp = getCommonDao().findVerificationRequiredAndStaticDelete(\""+actFile+"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			arrListLocal.add(collTemp);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return arrListLocal;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}catch(Exception ex){");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			// ex.printStackTrace();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			logger.error(\"Exception in getting the Page load values.\", ex);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			
			outHeader.println(stringBuffer.toString());
			stringBuffer.append(System.lineSeparator());
			
			
			stringBuffer = new StringBuffer();
			stringBuffer.append("	@Override");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	protected List<ReviewResultVb> transformToReviewResults(List<"+fileName+"Vb> approvedCollection, List<"+fileName+"Vb> pendingCollection) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ArrayList collTemp = getPageLoadValues();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ResourceBundle rsb = CommonUtils.getResourceManger();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		if(pendingCollection != null)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		if(approvedCollection != null)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();");
			stringBuffer.append(System.lineSeparator());
			outHeader.println(stringBuffer.toString());
			stringBuffer.append(System.lineSeparator());
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				if(!("'RECORD_INDICATOR_NT', 'RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					stringBuffer = new StringBuffer();
					String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					if(!dbColumnName.toUpperCase().endsWith("_AT") || !dbColumnName.toUpperCase().endsWith("_NT")) {
					stringBuffer.append("				ReviewResultVb l"+javaObject+" = new ReviewResultVb(rsb.getString(\""+javaSetValue+"\"),");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("					(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).get"+javaObject+"(),");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("					(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).get"+javaObject+"());");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("				lResult.add(l"+javaObject+");");
					stringBuffer.append(System.lineSeparator());
					
					}
					outHeader.println(stringBuffer.toString());
				}
			}
			rs.beforeFirst();
			outHeader.println("				ReviewResultVb lRecordIndicator = new ReviewResultVb(rsb.getString(\"recordIndicator\"),");
			outHeader.println("						(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getRecordIndicatorDesc(),");
			outHeader.println("						(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getRecordIndicatorDesc());");
			outHeader.println("					lResult.add(lRecordIndicator);");
			outHeader.println("				ReviewResultVb lMaker = new ReviewResultVb(rsb.getString(\"maker\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getMaker() == 0?\"\":pendingCollection.get(0).getMakerName(),");
			outHeader.println("						(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getMaker() == 0?\"\":approvedCollection.get(0).getMakerName());");
			outHeader.println("				lResult.add(lMaker);");
			outHeader.println("				ReviewResultVb lVerifier = new ReviewResultVb(rsb.getString(\"verifier\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getVerifier() == 0?\"\":pendingCollection.get(0).getVerifierName(),");
			outHeader.println("						(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getVerifier() == 0?\"\":approvedCollection.get(0).getVerifierName());");
			outHeader.println("				lResult.add(lVerifier);");
			outHeader.println("				ReviewResultVb lDateLastModified = new ReviewResultVb(rsb.getString(\"dateLastModified\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getDateLastModified(),");
			outHeader.println("					(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getDateLastModified());");
			outHeader.println("				lResult.add(lDateLastModified);");
			outHeader.println("				ReviewResultVb lDateCreation = new ReviewResultVb(rsb.getString(\"dateCreation\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getDateCreation(),");
			outHeader.println("					(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getDateCreation());");
			outHeader.println("				lResult.add(lDateCreation);");
			outHeader.println("				return lResult; ");
			
			outHeader.print("				\n\n	}");
	
			outHeader.println("			public "+fileName+"Dao get"+fileName+"Dao() {");
			outHeader.println("				return "+daoFileName+"Dao;");
			outHeader.println("			}");
			outHeader.println("			");
			outHeader.println("			public void set"+fileName+"Dao("+fileName+"Dao "+daoFileName+"Dao) {");
			outHeader.println("				this."+daoFileName+"Dao = "+daoFileName+"Dao;");
			outHeader.println("			}");
			
			outHeader.println("			@Override");
			outHeader.println("			protected AbstractDao<"+fileName+"Vb> getScreenDao() {");
			outHeader.println("				return "+daoFileName+"Dao;");
			outHeader.println("			}");
			
			
			outHeader.println("			@Override");
			outHeader.println("			protected void setAtNtValues("+fileName+"Vb vObject) {");
			outHeader.println("				set AT NT Values here");
			outHeader.println("			}");
			
			outHeader.println("			@Override");
			outHeader.println("			protected void setVerifReqDeleteType("+fileName+"Vb vObject) {");
			outHeader.println("				ArrayList<CommonVb> lCommVbList =(ArrayList<CommonVb>) getCommonDao().findVerificationRequiredAndStaticDelete(\""+actFile+"\");");
			outHeader.println("				vObject.setStaticDelete(lCommVbList.get(0).isStaticDelete());");
			outHeader.println("				vObject.setVerificationRequired(lCommVbList.get(0).isVerificationRequired());");
			outHeader.println("			}");
			
			outHeader.print("\n\n}");
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("WB file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}

	public void createVbObject(String fileName, ResultSet rs, String filePath){
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"Vb.java";
		try {
			File fileHeader = new File(filePath+headerFileName);
			if (!fileHeader.exists()) {
				    fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			outHeader.print("package com.vision.vb;\n\n");
			outHeader.print("public class "+fileName+"Vb extends CommonVb{\n\n");
			

			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String columnType = rs.getString("DATA_TYPE");
				if(!("'RECORD_INDICATOR_NT', '0RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					StringBuffer stringBuffer = new StringBuffer();
					String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					if("NUMBER".equalsIgnoreCase(columnType))
						stringBuffer.append("	private int "+javaSetValue+" = 0 ;");
					else
						stringBuffer.append("	private String "+javaSetValue+" = "+"\"\";");
					outHeader.println(stringBuffer.toString());
				}
			}
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String columnType = rs.getString("DATA_TYPE");
				if(!("'RECORD_INDICATOR_NT', 'RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					String javaSetValue = "";
					javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					StringBuffer stringBuffer = new StringBuffer();
					
					String type = "String";
					if("NUMBER".equalsIgnoreCase(columnType)){
						type = "int";
					}
					stringBuffer = new StringBuffer();
					stringBuffer.append("	public void set"+javaObject+"("+type+" "+javaSetValue+") {\n");
					stringBuffer.append("		this."+javaSetValue+" = "+javaSetValue+"; \n");;
					stringBuffer.append("	}\n");
					outHeader.println(stringBuffer.toString());
					stringBuffer = new StringBuffer();
					stringBuffer.append("	public "+type+" get"+javaObject+"() {\n");
					stringBuffer.append("		return "+javaSetValue+"; \n");;
					stringBuffer.append("	}");
					outHeader.println(stringBuffer.toString());
//					// System.out.println(stringBuffer);
				}
			}
			outHeader.print("\n\n}");
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("VB file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}
	public void createDaofil(String fileName, String tableName, ResultSet rs, ResultSet rsIndex, String filePath, boolean isUpl, boolean isSmarSearch){
		String actualFileName = fileName;
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"Dao.java";
		try {
			File fileHeader = new File(filePath+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			
			outHeader.println("package com.vision.dao;");

			outHeader.print("import java.sql.ResultSet;\n");
			outHeader.print("import java.sql.SQLException;\n");
			outHeader.print("import org.springframework.stereotype.Component;\n");
			outHeader.print("import java.util.List;\n");
			outHeader.print("import java.util.Vector;\n");

			outHeader.print("import org.springframework.jdbc.core.RowMapper;\n");

			outHeader.print("import com.vision.authentication.CustomContextHolder;\n");
			outHeader.print("import com.vision.util.CommonUtils;\n");
			outHeader.print("import com.vision.util.Constants;\n");
			outHeader.print("import com.vision.util.ValidationUtil;\n");
			outHeader.print("import com.vision.vb."+fileName+"Vb;\n\n");
			outHeader.print("@Component\n");
			outHeader.print("public class "+fileName+"Dao extends AbstractDao<"+fileName+"Vb> {\n\n");
				
			outHeader.println("/*******Mapper Start**********/");
			StringBuffer stringBufferMapper = new StringBuffer();
			stringBufferMapper.append("\t@Override\n");
			stringBufferMapper.append("\tprotected RowMapper getMapper(){\n");
			stringBufferMapper.append("\t\tRowMapper mapper = new RowMapper() {\n");
			stringBufferMapper.append("\t\t\tpublic Object mapRow(ResultSet rs, int rowNum) throws SQLException {\n");
			stringBufferMapper.append("\t\t\t\t"+fileName+"Vb vObject = new "+fileName+"Vb();\n");
			StringBuffer display = new StringBuffer();

			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String dbColumnType = rs.getString("DATA_TYPE");
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if(dbColumnName.toUpperCase().endsWith("_AT")) {
					outHeader.println("");
					String dbColumnNameTemp = dbColumnName.substring(0, dbColumnName.indexOf("_AT"));
					outHeader.println("\tString "+javaObject+"ApprDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"AT\", ?, \"TAppr."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
					outHeader.println("\tString "+javaObject+"PendDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"AT\", ?, \"TPend."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
				}else if(dbColumnName.toUpperCase().endsWith("_NT")){
					outHeader.println("");
					String dbColumnNameTemp = dbColumnName.substring(0, dbColumnName.indexOf("_NT"));
					outHeader.println("\tString "+javaObject+"ApprDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"NT\", ?, \"TAppr."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
					outHeader.println("\tString "+javaObject+"PendDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"NT\", ?, \"TPend."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
				}
				if(dbColumnName.equalsIgnoreCase("COUNTRY")) {
					outHeader.println("\tString "+javaObject+"ApprDesc = \"(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr."+dbColumnName+") "+dbColumnName+"_DESC\";");
					outHeader.println("\tString "+javaObject+"PendDesc = \"(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend."+dbColumnName+") "+dbColumnName+"_DESC\";");
				}
				if(dbColumnName.equalsIgnoreCase("LE_BOOK")) {
					outHeader.println("\tString "+javaObject+"ApprDesc = \"(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr."+dbColumnName+" AND COUNTRY = TAppr.COUNTRY ) "+dbColumnName+"_DESC\";");
					outHeader.println("\tString "+javaObject+"TPendDesc = \"(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend."+dbColumnName+" AND COUNTRY = TPend.COUNTRY ) "+dbColumnName+"_DESC\";");
				}
				
				if("NUMBER".equalsIgnoreCase(dbColumnType)) {
					display = new StringBuffer();
					display.append("\t\t\t\tvObject.set"+javaObject+"(rs.getInt(\""+dbColumnName+"\"));\n");
				}else {
					display = new StringBuffer("\t\t\t\tif(rs.getString(\""+dbColumnName+"\")!= null){ \n");
					display.append("\t\t\t\t\tvObject.set"+javaObject+"(rs.getString(\""+dbColumnName+"\"));\n");
					display.append("\t\t\t\t}else{\n");
					display.append("\t\t\t\t\tvObject.set"+javaObject+"(\"\");\n");
					display.append("\t\t\t\t}\n");
				}
				stringBufferMapper.append(display);
			}
			stringBufferMapper.append("\t\t\t\treturn vObject;\n");
			stringBufferMapper.append("\t\t\t}\n");
			stringBufferMapper.append("\t\t};\n");
			stringBufferMapper.append("\t\treturn mapper;\n");
			stringBufferMapper.append("\t}\n");	
			outHeader.println(stringBufferMapper.toString());
			outHeader.println("/*******Mapper End**********/");
			
			if(1== 1){
			
			StringBuffer dbApprColumns = new StringBuffer();
			StringBuffer dbUplColumns = new StringBuffer();
			
			StringBuffer dbPendColumns = new StringBuffer();
			StringBuffer whereIndexColumns = new StringBuffer();
			StringBuffer indexColumns = new StringBuffer();
			StringBuffer whereNotIndexColumns = new StringBuffer();
			rs.beforeFirst();
			int indexCount = 0;
			while (rsIndex.next()) {
				String indexColumn = rsIndex.getString("COLUMN_NAME");
				whereNotIndexColumns.append("TAppr."+indexColumn+" = TPend."+indexColumn);
				whereIndexColumns.append(""+indexColumn+" = ? ");
				indexColumns.append(""+indexColumn+",");
				if(rsIndex.isLast() == false){
					whereNotIndexColumns.append(" AND ");
					whereIndexColumns.append(" AND ");
				}
				indexCount++;
			}
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				if(!("'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					dbApprColumns.append("TAppr."+dbColumnName+"\"\n\t\t\t");
					if(isUpl)
						dbUplColumns.append("TUpl."+dbColumnName+"\"\n\t\t\t");
					dbPendColumns.append("TPend."+dbColumnName+"\"\n\t\t\t");
				}else{
					dbApprColumns.append(" To_Char(TAppr."+dbColumnName+", 'DD-MM-YYYY HH24:MI:SS') "+dbColumnName+"\"\n\t\t\t");
					if(isUpl)						
						dbUplColumns.append(" To_Char(TUpl."+dbColumnName+", 'DD-MM-YYYY HH24:MI:SS') "+dbColumnName+"\"\n\t\t\t");
					dbPendColumns.append(" To_Char(TPend."+dbColumnName+", 'DD-MM-YYYY HH24:MI:SS') "+dbColumnName+"\"\n\t\t\t");
				}
				if(rs.isLast() == false){
					dbApprColumns.append("+ \",");
					dbPendColumns.append("+ \",");
					if(isUpl)
						dbUplColumns.append("+ \",");
				}
				
			}
			
			outHeader.println("\tpublic List<"+fileName+"Vb> getQueryPopupResults("+fileName+"Vb dObj){");
			outHeader.println("");
			outHeader.println("\t\tVector<Object> params = new Vector<Object>();");
			outHeader.println("\t\tStringBuffer strBufApprove = new StringBuffer(\"Select "+dbApprColumns+"\" from "+tableName+" TAppr \");");
			outHeader.println("\t\tString strWhereNotExists = new String( \" Not Exists (Select 'X' From "+tableName+"_PEND TPend WHERE "+whereNotIndexColumns+" )\");");
			outHeader.println("\t\tStringBuffer strBufPending = new StringBuffer(\"Select "+dbPendColumns+"\" from "+tableName+"_PEND TPend \");");
			if(isUpl)
				outHeader.println("\t\tStringBuffer strBufUpl = new StringBuffer(\"Select "+dbUplColumns+"\" from "+tableName+"_UPL TUpl \");");
			
			if(isUpl) {
				outHeader.println("\t\tString strWhereNotExistsApprInUpl = new String( \" Not Exists (Select 'X' From "+tableName+"_UPL TUpl WHERE "+whereNotIndexColumns.toString().replaceAll("TPend.", "TUpl.")+") \");");
				outHeader.println("\t\tString strWhereNotExistsPendInUpl = new String( \" Not Exists (Select 'X' From "+tableName+"_UPL TUpl WHERE "+whereNotIndexColumns.toString().replaceAll("TAppr.", "TUpl.")+") \");");
			}
			outHeader.println("\t\ttry");
			outHeader.println("\t\t\t{");
			rs.beforeFirst();
			if(isSmarSearch){
				outHeader.println("							if (dObj.getSmartSearchOpt() != null && dObj.getSmartSearchOpt().size() > 0) {");
				outHeader.println("								int count = 1;");
				outHeader.println("								for (SmartSearchVb data: dObj.getSmartSearchOpt()){");
				outHeader.println("									if(count == dObj.getSmartSearchOpt().size()) {");
				outHeader.println("										data.setJoinType(\"\");");
				outHeader.println("									} else {");
				outHeader.println("										if(!ValidationUtil.isValid(data.getJoinType()) && !(\"AND\".equalsIgnoreCase(data.getJoinType()) || \"OR\".equalsIgnoreCase(data.getJoinType()))) {");
				outHeader.println("											data.setJoinType(\"AND\");");
				outHeader.println("										}");
				outHeader.println("									}");
				outHeader.println("									String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());");
				outHeader.println("									switch (data.getObject()) {");
			}
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String columnType = rs.getString("DATA_TYPE");
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
				if(!("MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED,DATE_CREATION".contains(dbColumnName))){
					if(!dbColumnName.contains("_AT"))
					if(!dbColumnName.contains("_NT")){
						if(!isSmarSearch){
							if(!"RECORD_INDICATOR".contentEquals(dbColumnName)){
								if("NUMBER".equalsIgnoreCase(columnType)){
									outHeader.println("\t\t\t\tif (ValidationUtil.isValid(dObj.get"+javaObject+"())){");
									outHeader.println("\t\t\t\t\t\tparams.addElement(dObj.get"+javaObject+"());");
									outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TAppr."+dbColumnName+" = ?\", strBufApprove);");
									outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TPend."+dbColumnName+" = ?\", strBufPending);");
									if(isUpl)
										outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TUpl."+dbColumnName+" = ?\", strBufUpl);");
									outHeader.println("\t\t\t\t}");
								}else {
									outHeader.println("\t\t\t\tif (ValidationUtil.isValid(dObj.get"+javaObject+"())){");
									outHeader.println("\t\t\t\t\t\tparams.addElement(dObj.get"+javaObject+"());");
									outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"UPPER(TAppr."+dbColumnName+") = ?\", strBufApprove);");
									outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"UPPER(TPend."+dbColumnName+") = ?\", strBufPending);");
									if(isUpl)
										outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"UPPER(TUpl."+dbColumnName+") = ?\", strBufUpl);");
									outHeader.println("\t\t\t\t}");
								}
							}else{
								outHeader.println("\t\t\t\tif (dObj.getRecordIndicator() != -1){");
								outHeader.println("\t\t\t\t\tif (dObj.getRecordIndicator() > 3){");
								outHeader.println("\t\t\t\t\t\tparams.addElement(new Integer(0));");
								outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TAppr.RECORD_INDICATOR > ?\", strBufApprove);");
								outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TPend.RECORD_INDICATOR > ?\", strBufPending);");
								if(isUpl)
									outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TUpl.RECORD_INDICATOR > ?\", strBufUpl);");
								outHeader.println("\t\t\t\t\t}else{");
								outHeader.println("\t\t\t\t\t\t params.addElement(new Integer(dObj.getRecordIndicator()));");
								outHeader.println("\t\t\t\t\t\t CommonUtils.addToQuery(\"TAppr.RECORD_INDICATOR = ?\", strBufApprove);");
								outHeader.println("\t\t\t\t\t\t CommonUtils.addToQuery(\"TPend.RECORD_INDICATOR = ?\", strBufPending);");
								if(isUpl)
									outHeader.println("\t\t\t\t\t\t CommonUtils.addToQuery(\"TUpl.RECORD_INDICATOR = ?\", strBufUpl);");
								outHeader.println("\t\t\t\t\t}");
								outHeader.println("\t\t\t\t}");
							}
						}else {
							outHeader.println("									case \""+javaSetValue+"\":");
							outHeader.println("										CommonUtils.addToQuerySearch(\" upper(TAppr."+dbColumnName+") \"+ val, strBufApprove, data.getJoinType());");
							outHeader.println("										CommonUtils.addToQuerySearch(\" upper(TPend."+dbColumnName+") \"+ val, strBufPending, data.getJoinType());");
							if(isUpl)
								outHeader.println("										CommonUtils.addToQuerySearch(\" upper(TUpl."+dbColumnName+") \"+ val, strBufUpl, data.getJoinType());");
							outHeader.println("										break;");
							outHeader.println("	");
						}
					}
				}
			}
			if(isSmarSearch){
				outHeader.println("										default:");
				outHeader.println("									}");
				outHeader.println("									count++;");
				outHeader.println("								}");
				outHeader.println("							}");
			}
			outHeader.println("\t\t\tString orderBy=\" Order By "+whereIndexColumns+" \";");
			if(!isUpl) {
				outHeader.println("\t\t\treturn getQueryPopupResults(dObj,strBufPending, strBufApprove, strWhereNotExists, orderBy, params);");
			}else {
				outHeader.println("CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);");
				outHeader.println("CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);");
				outHeader.println("StringBuffer lPageQuery = new StringBuffer();");
				outHeader.println("if(dObj.isVerificationRequired())");
				outHeader.println("	lPageQuery.append(strBufPending.toString() + \" Union \" + strBufUpl.toString());");
				outHeader.println("else");
				outHeader.println("	lPageQuery.append(strBufApprove.toString() + \" Union \" + strBufUpl.toString());");
				outHeader.println("return getQueryPopupResultsPgn(dObj,lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,getMapper());");
			}
			outHeader.println("");
			outHeader.println("\t\t\t}catch(Exception ex){");
			outHeader.println("\t\t\t\t// ex.printStackTrace();");
			outHeader.println("\t\t\t\tlogger.error(((strBufApprove==null)? \"strBufApprove is Null\":strBufApprove.toString()));");
			outHeader.println("\t\t\t\tlogger.error(\"UNION\");");
			outHeader.println("\t\t\t\tlogger.error(((strBufPending==null)? \"strBufPending is Null\":strBufPending.toString()));");
			outHeader.println("");
			outHeader.println("\t\t\t\tif (params != null)");
			outHeader.println("\t\t\t\t\tfor(int i=0 ; i< params.size(); i++)");
			outHeader.println("\t\t\t\t\t\tlogger.error(\"objParams[\" + i + \"]\" + params.get(i).toString());");
			outHeader.println("\t\t\treturn null;");
			outHeader.println("");
			outHeader.println("\t\t\t}");
			outHeader.println("\t}");
			
			outHeader.println("");
			outHeader.println("");
			outHeader.println("");
				
			outHeader.println("\tpublic List<"+fileName+"Vb> getQueryResults("+fileName+"Vb dObj, int intStatus){");
			outHeader.println("");
			outHeader.println("\t\tsetServiceDefaults();");
			outHeader.println("");
					outHeader.println("\t\tList<"+fileName+"Vb> collTemp = null;");
					outHeader.println("");
					outHeader.println("\t\tfinal int intKeyFieldsCount = "+indexCount+";");

					outHeader.println("\t\tString strQueryAppr = new String(\"Select "+dbApprColumns+"\" From "+tableName+" TAppr WHERE "+whereIndexColumns+" \");");
					outHeader.println("\t\tString strQueryPend = new String(\"Select "+dbPendColumns+"\" From "+tableName+"_PEND TPend WHERE "+whereIndexColumns+"  \");");
					if(isUpl)	
						outHeader.println("\t\tString strQueryUpl = new String(\"Select "+dbUplColumns+"\" From "+tableName+"_UPL TPend WHERE "+whereIndexColumns+"  \");");
					outHeader.println("");
					outHeader.println("\t\tObject objParams[] = new Object[intKeyFieldsCount];");
//					outHeader.println("\t\tobjParams[0] = dObj.get");
					int i = 0;
					rsIndex.beforeFirst();
					while (rsIndex.next()) {
						String dbColumnName = rsIndex.getString("COLUMN_NAME");
						String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
						outHeader.println("\t\tobjParams["+i+"] = dObj.get"+javaObject+"();");
					i++;
					}
					
					outHeader.println("");
					outHeader.println("\t\ttry{");
					outHeader.println("\t\t\tif(!dObj.isVerificationRequired()){intStatus =0;}");
					outHeader.println("\t\t\tif(intStatus == 0){");
					outHeader.println("\t\t\t\tlogger.info(\"Executing approved query\");");
							outHeader.println("\t\t\t\tcollTemp = getJdbcTemplate().query(strQueryAppr.toString(),objParams,getMapper());");
						outHeader.println("\t\t\t}else{");
							outHeader.println("\t\t\t\tlogger.info(\"Executing pending query\");");
							outHeader.println("\t\t\t\tcollTemp = getJdbcTemplate().query(strQueryPend.toString(),objParams,getMapper());");
						outHeader.println("\t\t\t}");
						outHeader.println("\t\t\treturn collTemp;");
					outHeader.println("\t\t}catch(Exception ex){");
						outHeader.println("\t\t\t\t// ex.printStackTrace();");
						outHeader.println("\t\t\t\tlogger.error(\"Error: getQueryResults Exception :   \");");
						outHeader.println("\t\t\t\tif(intStatus == 0)");
							outHeader.println("\t\t\t\t\tlogger.error(((strQueryAppr == null) ? \"strQueryAppr is Null\" : strQueryAppr.toString()));");
						outHeader.println("\t\t\t\telse");
							outHeader.println("\t\t\t\t\tlogger.error(((strQueryPend == null) ? \"strQueryPend is Null\" : strQueryPend.toString()));");
					outHeader.println("");
						outHeader.println("\t\t\t\tif (objParams != null)");
							outHeader.println("\t\t\t\tfor(int i=0 ; i< objParams.length; i++)");
								outHeader.println("\t\t\t\t\tlogger.error(\"objParams[\" + i + \"]\" + objParams[i].toString());");
						outHeader.println("\t\t\t\treturn null;");
					outHeader.println("\t\t\t\t}");
				outHeader.println("\t}");
			
			outHeader.println("	@Override");
			outHeader.println("	protected List<"+fileName+"Vb> selectApprovedRecord("+fileName+"Vb vObject){");
			outHeader.println("		return getQueryResults(vObject, Constants.STATUS_ZERO);");
			outHeader.println("	}");
			outHeader.println("");
			outHeader.println("");
			outHeader.println("	@Override");
			outHeader.println("	protected List<"+fileName+"Vb> doSelectPendingRecord("+fileName+"Vb vObject){");
			outHeader.println("		return getQueryResults(vObject, Constants.STATUS_PENDING);");
			outHeader.println("	}");
			outHeader.println("");
			outHeader.println("");
			if(isUpl) {
				outHeader.println("	@Override");
				outHeader.println("	protected List<"+fileName+"Vb> doSelectUplRecord("+fileName+"Vb vObject){");
				outHeader.println("		return getQueryResults(vObject, 9999);");
				outHeader.println("	}");
				outHeader.println("");
				outHeader.println("");
			}
			outHeader.println("	@Override");
			outHeader.println("	protected int getStatus("+fileName+"Vb records){return records.getErrorStatus();}");
			outHeader.println("");
			outHeader.println("");
			outHeader.println("	@Override");
			outHeader.println("	protected void setStatus("+fileName+"Vb vObject,int status){vObject.setErrorStatus(status);}");
			outHeader.println("");
			outHeader.println("");
			outHeader.println("	@Override");
			outHeader.println("	protected int doInsertionAppr("+fileName+"Vb vObject){");
			StringBuffer insertStringBuf = new StringBuffer();
			StringBuffer insertColumns = new StringBuffer();
			StringBuffer insertQuestingMarks = new StringBuffer();
			StringBuffer insertGetValues = new StringBuffer();
			
			StringBuffer insertIndexGetValues = new StringBuffer();
			
			insertStringBuf.append("\t\tString query =\t\"Insert Into "+tableName+" (");
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				insertColumns.append(dbColumnName);
//				insertColumns.append(System.lineSeparator());
				if(("'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					insertQuestingMarks.append("SysDate");
				}else{
					insertQuestingMarks.append("?");
				}
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if((!"'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName)))
					insertGetValues.append("vObject.get"+javaObject+"()");
				if(rs.isLast() == false){
					insertColumns.append(", ");
					insertQuestingMarks.append(", ");
					insertGetValues.append(", ");
				}
			}
			insertStringBuf.append(insertColumns);
			insertStringBuf.append(")\"+ \n");
			insertStringBuf.append("\t\t \"Values (");
			insertStringBuf.append(insertQuestingMarks);
			insertStringBuf.append(")\"; \n");
			
			insertStringBuf.append("\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString().replace(", , ", " "));
			insertStringBuf.append("};\n");
			outHeader.println(insertStringBuf);
			outHeader.println("\t\t return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			outHeader.println("");
			outHeader.println("");
			outHeader.println("	@Override");
			outHeader.println("	protected int doInsertionPend("+fileName+"Vb vObject){");
			insertStringBuf = new StringBuffer();
			insertColumns = new StringBuffer();
			insertQuestingMarks = new StringBuffer();
			insertGetValues = new StringBuffer();
			insertStringBuf.append("\t\tString query =\t\"Insert Into "+tableName+"_PEND (");
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				insertColumns.append(dbColumnName);
//				insertColumns.append(System.lineSeparator());
				if(("'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					insertQuestingMarks.append("SysDate");
				}else{
					insertQuestingMarks.append("?");
				}
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if((!"'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName)))
					insertGetValues.append("vObject.get"+javaObject+"()");
				if(rs.isLast() == false){
					insertColumns.append(", ");
					insertQuestingMarks.append(", ");
					insertGetValues.append(", ");
				}
			}
			insertStringBuf.append(insertColumns);
			insertStringBuf.append(")\"+ \n");
			insertStringBuf.append("\t\t \"Values (");
			insertStringBuf.append(insertQuestingMarks);
			insertStringBuf.append(")\"; \n");
			
			insertStringBuf.append("\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString().replace(", , ", " "));
			insertStringBuf.append("};\n");
			outHeader.println(insertStringBuf);
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			
			outHeader.println("");
			outHeader.println("");
			outHeader.println("	@Override");
			outHeader.println("	protected int doInsertionPendWithDc("+fileName+"Vb vObject){");
			insertStringBuf = new StringBuffer();
			insertColumns = new StringBuffer();
			insertQuestingMarks = new StringBuffer();
			insertGetValues = new StringBuffer();
			insertStringBuf.append("\t\tString query =\t\"Insert Into "+tableName+"_PEND (");
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				insertColumns.append(dbColumnName);
//				insertColumns.append(System.lineSeparator());
				if(("DATE_LAST_MODIFIED, DATE_CREATION".contains(dbColumnName))){
					if(("DATE_CREATION".contains(dbColumnName))){
						insertQuestingMarks.append("To_Date(?, 'DD-MM-YYYY HH24:MI:SS')");
					}else{
						insertQuestingMarks.append("SysDate");
					}
				}else{
					insertQuestingMarks.append("?");
				}
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if((!"DATE_LAST_MODIFIED".contains(dbColumnName)))
					insertGetValues.append("vObject.get"+javaObject+"()");
				if(rs.isLast() == false){
					insertColumns.append(", ");
					insertQuestingMarks.append(", ");
					insertGetValues.append(", ");
				}
			}
			insertStringBuf.append(insertColumns);
			insertStringBuf.append(")\"+ \n");
			insertStringBuf.append("\t\t \"Values (");
			insertStringBuf.append(insertQuestingMarks);
			insertStringBuf.append(")\"; \n");
			
			insertStringBuf.append("\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString().replace(", , ", ", "));
			insertStringBuf.append("};\n");
			outHeader.println(insertStringBuf);
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			
			outHeader.println("");
			outHeader.println("");
			insertStringBuf = new StringBuffer();
			insertGetValues = new StringBuffer();
			insertStringBuf.append("\t@Override\n");
			insertStringBuf.append("\tprotected int doUpdateAppr("+fileName+"Vb vObject){\n");
			insertStringBuf.append("\tString query = \"Update "+tableName+" Set ");
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if(!indexColumns.toString().contains(dbColumnName)){
					if(("DATE_LAST_MODIFIED, DATE_CREATION".contains(dbColumnName))){
						if(("DATE_LAST_MODIFIED".contains(dbColumnName))){
							insertStringBuf.append(dbColumnName+" = SysDate");	
						}
						if(rs.isLast() == false){
							insertStringBuf.append(", ");
							insertGetValues.append(", ");
						}
					}else {
						insertStringBuf.append(dbColumnName+" = ?");
						insertGetValues.append("vObject.get"+javaObject+"() ");
						if(rs.isLast() == false){
							insertStringBuf.append(", ");
							insertGetValues.append(", ");
						}
					}
				}else {
					insertIndexGetValues.append("vObject.get"+javaObject+"() ");
					if(rs.isLast() == false)
						insertIndexGetValues.append(", ");
				}
			}
//			insertStringBuf = new StringBuffer(insertStringBuf.toString().substring(0,insertStringBuf.toString().length()-1));
			insertStringBuf.append(" WHERE "+whereIndexColumns);
			insertStringBuf.append("\";\n\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString());
			insertStringBuf.append(insertIndexGetValues.toString());
			insertStringBuf.append("};\n");
			outHeader.println(insertStringBuf);
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			
			outHeader.println("");
			outHeader.println("");
			insertStringBuf = new StringBuffer();
			insertGetValues = new StringBuffer();
			insertStringBuf.append("\t@Override\n");
			insertStringBuf.append("\tprotected int doUpdatePend("+fileName+"Vb vObject){\n");
			insertStringBuf.append("\tString query = \"Update "+tableName+"_PEND Set ");
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if(!indexColumns.toString().contains(dbColumnName)){
					if(("DATE_LAST_MODIFIED, DATE_CREATION".contains(dbColumnName))){
						if(("DATE_LAST_MODIFIED".contains(dbColumnName))){
							insertStringBuf.append(dbColumnName+" = SysDate");	
						}
						if(rs.isLast() == false){
							insertStringBuf.append(", ");
							insertGetValues.append(", ");
						}
					}else {
						insertStringBuf.append(dbColumnName+" = ?");
						insertGetValues.append("vObject.get"+javaObject+"() ");
						if(rs.isLast() == false){
							insertStringBuf.append(", ");
							insertGetValues.append(", ");
						}
					}
				}else {
					insertIndexGetValues.append("vObject.get"+javaObject+"() ");
					if(rs.isLast() == false)
						insertIndexGetValues.append(", ");
				}
			}
//			insertStringBuf = new StringBuffer(insertStringBuf.toString().substring(0,insertStringBuf.toString().length()-1));
			insertStringBuf.append(" WHERE "+whereIndexColumns);
			insertStringBuf.append("\";\n\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString());
			insertStringBuf.append("};\n");
			outHeader.println(insertStringBuf);
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");

			
			outHeader.println("");
			outHeader.println("");
			rsIndex.beforeFirst();
			StringBuffer deleteWhereValues = new StringBuffer();
			while (rsIndex.next()) {
				String dbColumnName = rsIndex.getString("COLUMN_NAME");
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					deleteWhereValues.append("vObject.get"+javaObject+"()");
					if(rsIndex.isLast() == false) {
						deleteWhereValues.append(", ");
					}
					i++;
			}
			
			outHeader.println("\t@Override");
			outHeader.println("\tprotected int doDeleteAppr("+fileName+"Vb vObject){");
			outHeader.println("\t\tString query = \"Delete From "+tableName+" Where "+whereIndexColumns+" \";");
			outHeader.println("		Object[] args = { "+deleteWhereValues+" };");
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			
			outHeader.println("");
			outHeader.println("");
			outHeader.println("\t@Override");
			outHeader.println("\tprotected int deletePendingRecord("+fileName+"Vb vObject){");
			outHeader.println("\t\tString query = \"Delete From "+tableName+"_PEND Where "+whereIndexColumns+" \";");
			outHeader.println("		Object[] args = { "+deleteWhereValues+" };");
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			outHeader.println("");
			outHeader.println("");
			if(isUpl) {
				outHeader.println("\t@Override");
				outHeader.println("\tprotected int deleteUplRecord("+fileName+"Vb vObject){");
				outHeader.println("\t\tString query = \"Delete From "+tableName+"_UPL Where "+whereIndexColumns+" \";");
				outHeader.println("		Object[] args = { "+deleteWhereValues+" };");
				outHeader.println("		return getJdbcTemplate().update(query,args);");
				outHeader.println("	}");
				outHeader.println("");
				outHeader.println("");
			}
			StringBuffer stringBufferAuditTrail = new StringBuffer();
			outHeader.println("\t@Override");
			outHeader.println("\tprotected String getAuditString("+fileName+"Vb vObject){");
			outHeader.println("\t\tfinal String auditDelimiter = vObject.getAuditDelimiter();");
			outHeader.println("\t\tfinal String auditDelimiterColVal = vObject.getAuditDelimiterColVal();");
			outHeader.println("\t\tStringBuffer strAudit = new StringBuffer(\"\");");
			rs.beforeFirst();
			while (rs.next()) {
				String dbColumnName = rs.getString("COLUMN_NAME");
				String columnType = rs.getString("DATA_TYPE");
				insertStringBuf.append(dbColumnName+" = ?,");
//				insertStringBuf.append(System.lineSeparator());
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				String trim = ".trim()";
				if("NUMBER".equalsIgnoreCase(columnType)){
					trim = "";
					stringBufferAuditTrail.append("\t\t\t\tstrAudit.append(\""+dbColumnName+"\"+auditDelimiterColVal+vObject.get"+javaObject+"()"+trim+");\n");
					stringBufferAuditTrail.append("\t\t\tstrAudit.append(auditDelimiter);\n");
					stringBufferAuditTrail.append("\n");
				}else{
					stringBufferAuditTrail.append("\t\t\tif(ValidationUtil.isValid(vObject.get"+javaObject+"()))\n");
					stringBufferAuditTrail.append("\t\t\t\tstrAudit.append(\""+dbColumnName+"\"+auditDelimiterColVal+vObject.get"+javaObject+"()"+trim+");\n");
					stringBufferAuditTrail.append("\t\t\telse\n");
					stringBufferAuditTrail.append("\t\t\t\tstrAudit.append(\""+dbColumnName+"\"+auditDelimiterColVal+\"NULL\");\n");
					stringBufferAuditTrail.append("\t\t\tstrAudit.append(auditDelimiter);\n");
					stringBufferAuditTrail.append("\n");
				}
			}
			stringBufferAuditTrail.append("\t\treturn strAudit.toString();\n");
			stringBufferAuditTrail.append("\t\t}\n");
			outHeader.println(stringBufferAuditTrail);
				
		}
			String fileName1 = ValidationUtil.toTitleCase(actualFileName).replaceAll("_", " ");
			outHeader.println("	@Override");
			outHeader.println("	protected void setServiceDefaults(){");
			outHeader.println("		serviceName = \""+fileName+"\";");
			outHeader.println("		serviceDesc = \""+fileName1+"\";");
			outHeader.println("		tableName = \""+tableName+"\";");
			outHeader.println("		childTableName = \""+tableName+"\";");
			outHeader.println("		intCurrentUserId = CustomContextHolder.getContext().getVisionId();");
			outHeader.println("		");
			outHeader.println("	}");
			
			outHeader.println("\n\n}");
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("Dao file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}
	
	
	public void createControllerObject(String fileName, ResultSet rs, String filePath){
		String actulaFile = fileName;
		String screenTitle = ValidationUtil.toTitleCase(fileName).replaceAll("_", " ");
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		// System.out.println(actulaFile);
		// System.out.println(fileName);
		String wbFileName = (fileName.substring(0, 1)).toLowerCase() + fileName.substring(1,fileName.length());
		// System.out.println(screenTitle);
		String headerFileName = fileName+"Controller.java";
		try {

			File fileHeader = new File(filePath+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			StringBuffer stringBuffer = new StringBuffer();
			stringBuffer.append("package com.vision.controller;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import java.util.ArrayList;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import java.util.Date;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import java.util.List;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.beans.factory.annotation.Autowired;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.http.HttpStatus;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.http.ResponseEntity;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.web.bind.annotation.RequestBody;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.web.bind.annotation.RequestMapping;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.web.bind.annotation.RequestMethod;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import org.springframework.web.bind.annotation.RestController;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.exception.ExceptionCode;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.exception.JSONExceptionCode;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.exception.RuntimeCustomException;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.util.Constants;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.vb."+fileName+"Vb;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.vb.MenuVb;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import com.vision.wb."+fileName+"Wb;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import io.swagger.annotations.Api;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("import io.swagger.annotations.ApiOperation;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("@RestController");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("@RequestMapping(\""+wbFileName+"\")");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("@Api(value = \""+wbFileName+"\", description = \""+screenTitle+"\")");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("public class "+fileName+"Controller{");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@Autowired");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	"+fileName+"Wb "+wbFileName+"Wb;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	/*-------------------------------------"+screenTitle+" SCREEN PAGE LOAD------------------------------------------*/");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/pageLoadValues\", method = RequestMethod.GET)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Page Load Values\", notes = \"Load AT/NT Values on screen load\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> pageOnLoad() {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			MenuVb menuVb = new MenuVb();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			menuVb.setActionType(\"Clear\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			ArrayList arrayList = "+wbFileName+"Wb.getPageLoadValues();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, \"Page Load Values\", arrayList);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	/*------------------------------------"+screenTitle+" - FETCH HEADER RECORDS-------------------------------*/");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/getAllQueryResults\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Get All profile Data\",notes = \"Fetch all the existing records from the table\",response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> getAllQueryResults(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode  = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try{");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Query\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			// System.out.println(\"Query Start : \"+(new Date()).toString());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			ExceptionCode exceptionCode = "+wbFileName+"Wb.getAllQueryPopupResult(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			// System.out.println(\"Query End : \"+(new Date()).toString());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.SUCCESSFUL_OPERATION, \"Query Results\", exceptionCode.getResponse(),exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}catch(RuntimeCustomException rex){");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/getQueryDetails\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Get All ADF Schules\",notes = \"ADF Schules Details\",response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> queryDetails(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode  = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try{");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Query\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			ExceptionCode  exceptionCode= "+wbFileName+"Wb.getQueryResultsSingle(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(), exceptionCode.getResponse(),exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}catch(RuntimeCustomException rex){");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	/*-------------------------------------ADD "+screenTitle+"------------------------------------------*/");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/add"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Add "+screenTitle+"\", notes = \"Add "+screenTitle+"\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> add(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Add\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.insertRecord(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	/*-------------------------------------MODIFY "+screenTitle+"------------------------------------------*/");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/modify"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Modify "+screenTitle+"\", notes = \"Modify "+screenTitle+" Values\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> modify(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Modify\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.modifyRecord(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	/*-------------------------------------DELETE "+screenTitle+"------------------------------------------*/");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/delete"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Delete "+screenTitle+"\", notes = \"Delete existing "+screenTitle+"\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> delete(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Delete\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode.setOtherInfo(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.deleteRecord(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	/*-------------------------------------Reject "+screenTitle+"------------------------------------------*/");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/reject"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Reject "+screenTitle+"\", notes = \"Reject existing "+screenTitle+"\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> reject(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Reject\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode.setOtherInfo(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.reject(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/approve"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Approve "+screenTitle+"\", notes = \"Approve existing "+screenTitle+"\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> approve(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Approve\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode.setOtherInfo(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.approve(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/bulkApprove"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Approve "+screenTitle+"\", notes = \"Approve existing "+screenTitle+"\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> bulkApprove(@RequestBody List<"+fileName+"Vb> vObjects) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			"+fileName+"Vb vObject = new "+fileName+"Vb();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Approve\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode.setOtherInfo(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.bulkApprove(vObjects, vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			String errorMessage= exceptionCode.getErrorMsg().replaceAll(\"- Approve -\", \"- Bulk Approve -\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/bulkReject"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Approve "+screenTitle+"\", notes = \"Approve existing "+screenTitle+"\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> bulkReject(@RequestBody List<"+fileName+"Vb> vObjects) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		ExceptionCode exceptionCode = new ExceptionCode();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			"+fileName+"Vb vObject = new "+fileName+"Vb();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Reject\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode.setOtherInfo(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			exceptionCode = "+wbFileName+"Wb.bulkReject(vObjects, vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			String errorMessage= exceptionCode.getErrorMsg().replaceAll(\"- Reject -\", \"- Bulk Reject -\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMessage,");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@RequestMapping(path = \"/review"+fileName+"\", method = RequestMethod.POST)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@ApiOperation(value = \"Get Headers\", notes = \"Fetch all the existing records from the table\", response = ResponseEntity.class)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	public ResponseEntity<JSONExceptionCode> review(@RequestBody "+fileName+"Vb vObject) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		JSONExceptionCode jsonExceptionCode = null;");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		try {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			vObject.setActionType(\"Query\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			ExceptionCode exceptionCode = "+wbFileName+"Wb.reviewRecordNew(vObject);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			String errorMesss = exceptionCode.getErrorMsg().replaceAll(\"Query \", \"Review\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(exceptionCode.getErrorCode(), errorMesss,");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("					exceptionCode.getResponse(), exceptionCode.getOtherInfo());");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		} catch (RuntimeCustomException rex) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			jsonExceptionCode = new JSONExceptionCode(Constants.ERRONEOUS_OPERATION, rex.getMessage(), \"\");");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			return new ResponseEntity<JSONExceptionCode>(jsonExceptionCode, HttpStatus.OK);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		}");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	}	");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("}");
			stringBuffer.append(System.lineSeparator());
			outHeader.print(stringBuffer);
			outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("Controller file Created Successfully !!");
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	



}
