package com.vision.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.vision.util.ValidationUtil;

public class SpringMapperGeneration {

	public static void main(String args[]){  
		try{  
			//step1 load the driver class  
			Class.forName("oracle.jdbc.driver.OracleDriver");  
			  
			//step2 create  the connection object  
			Connection con=DriverManager.getConnection("jdbc:oracle:thin:@10.16.1.106:1521:VISDB","VISION_ETL","Vision123");
			  
			//step3 create the statement object  
			// System.out.println("1");
			Statement stmt=con.createStatement();
			// System.out.println("2");
			//String strArg = args[0];
			//step4 execute query
			String dbLink = "@finacle_prd_ng";
			String previouse ="";
			String tableName = "ETL_CONNECTOR"; 
			ResultSet rs=stmt.executeQuery("select * from "+tableName+" where rownum<2 ");
//			ResultSet rs=stmt.executeQuery(Query);
			// System.out.println("4");
			ResultSetMetaData metadata = rs.getMetaData();
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				int type = metadata.getColumnType(i);
				String typeName = metadata.getColumnTypeName(i);
				int size = metadata.getColumnDisplaySize(i);
				
				// System.out.println("dbColumnName : "+dbColumnName+" type : "+type+" typeName : "+typeName+" size : "+size);
			}
			SpringMapperGeneration generation = new SpringMapperGeneration();
			generation.createMessageProperties(tableName, rs);
			generation.createVbObject(tableName, rs);
			generation.createWbObject(tableName, rs);
			generation.createDaofil(tableName, tableName, rs, false);
			generation.createControllerObject(tableName, rs);
			//step5 close the connection object  
			con.close();  
			  
		}catch(Exception e){ 
			// System.out.println(e);
		}  
	}
	public void createAngularObject(String fileName, ResultSet rs){
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"Angular.txt";
		try {
			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File("C:\\JavaFiles\\"+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			outHeader.print("headers: [");
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				if(!("'RECORD_INDICATOR_NT', '0RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					StringBuffer stringBuffer = new StringBuffer();
					String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
//					stringBuffer.append("	private String "+javaSetValue+" = "+"\"\";");
					stringBuffer.append("{"
							+ "            name: "+javaSetValue+","
							+ "            referField: '"+javaSetValue+"',"
							+ "            style: '',"
							+ "            filter: false,"
							+ "            needIndicator: true"
							+ "        }");
					
//					// System.out.println(stringBuffer);
					outHeader.println(stringBuffer.toString());
				}
			}
			outHeader.print("\n]");
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("VB file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}
	public void createVbObject(String fileName, ResultSet rs){
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"Vb.java";
		try {
			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File("C:\\JavaFiles\\"+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			outHeader.print("package com.vision.vb;\n\n");
			outHeader.print("public class "+fileName+"Vb extends CommonVb{\n\n");
			

			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				if(!("'RECORD_INDICATOR_NT', '0RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					StringBuffer stringBuffer = new StringBuffer();
					String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					if("NUMBER".equalsIgnoreCase(metadata.getColumnTypeName(i)))
						stringBuffer.append("	private int "+javaSetValue+" = 0 ;");
					else
						stringBuffer.append("	private String "+javaSetValue+" = "+"\"\";");
//					// System.out.println(stringBuffer);
					outHeader.println(stringBuffer.toString());
				}
			}
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				if(!("'RECORD_INDICATOR_NT', 'RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					String javaSetValue = "";
					javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					StringBuffer stringBuffer = new StringBuffer();
					
					String type = "String";
					if("NUMBER".equalsIgnoreCase(metadata.getColumnTypeName(i))){
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
	
	public void createDaofil(String fileName, String tableName, ResultSet rs, boolean onyMapper){
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"Dao.java";
		try {
			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File("C:\\JavaFiles\\"+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			
			outHeader.println("package com.vision.dao;");

			outHeader.print("import java.sql.ResultSet;\n");
			outHeader.print("import java.sql.SQLException;\n");
			outHeader.print("import java.util.List;\n");
			outHeader.print("import java.util.Vector;\n");

			outHeader.print("import org.springframework.jdbc.core.RowMapper;\n");

			outHeader.print("import com.vision.authentication.CustomContextHolder;\n");
			outHeader.print("import com.vision.util.CommonUtils;\n");
			outHeader.print("import com.vision.util.Constants;\n");
			outHeader.print("import com.vision.util.ValidationUtil;\n");
			outHeader.print("import com.vision.vb."+fileName+"Vb;\n\n");
			outHeader.print("@Component\n\n");
			outHeader.print("public class "+fileName+"Dao extends AbstractDao<"+fileName+"Vb> {\n\n");
				
			outHeader.println("/*******Mapper Start**********/");
			StringBuffer stringBufferMapper = new StringBuffer();
			stringBufferMapper.append("\t@Override\n");
			stringBufferMapper.append("\tprotected RowMapper getMapper(){\n");
			stringBufferMapper.append("\t\tRowMapper mapper = new RowMapper() {\n");
			stringBufferMapper.append("\t\t\tpublic Object mapRow(ResultSet rs, int rowNum) throws SQLException {\n");
			stringBufferMapper.append("\t\t\t\t"+fileName+"Vb vObject = new "+fileName+"Vb();\n");
			StringBuffer display = new StringBuffer();
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if(dbColumnName.toUpperCase().endsWith("_AT")) {
					outHeader.println("");
					String dbColumnNameTemp = dbColumnName.substring(0, dbColumnName.indexOf("_AT"));
					outHeader.println("String "+javaObject+"ApprDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"AT\", ?, \"TAppr."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
					outHeader.println("String "+javaObject+"PendDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"AT\", ?, \"TPend."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
				}else if(dbColumnName.toUpperCase().endsWith("_NT")){
					outHeader.println("");
					String dbColumnNameTemp = dbColumnName.substring(0, dbColumnName.indexOf("_NT"));
					outHeader.println("String "+javaObject+"ApprDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"NT\", ?, \"TAppr."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
					outHeader.println("String "+javaObject+"PendDesc = ValidationUtil.numAlphaTabDescritpionQuery(\"NT\", ?, \"TPend."+dbColumnNameTemp+"\", \""+dbColumnNameTemp+"_DESC\");");
				}
				
				if("NUMBER".equalsIgnoreCase(metadata.getColumnTypeName(i))) {
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
			StringBuffer dbPendColumns = new StringBuffer();
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				if(!("'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					dbApprColumns.append("TAppr."+dbColumnName+"");
//					dbApprColumns.append(System.lineSeparator());
					dbPendColumns.append("TPend."+dbColumnName);
//					dbPendColumns.append(System.lineSeparator());
				}else{
					dbApprColumns.append(" To_Char(TAppr."+dbColumnName+", 'DD-MM-YYYY HH24:MI:SS') "+dbColumnName);
//					dbApprColumns.append(System.lineSeparator());
					dbPendColumns.append(" To_Char(Tpend."+dbColumnName+", 'DD-MM-YYYY HH24:MI:SS') "+dbColumnName);
//					dbPendColumns.append(System.lineSeparator());
				}
				if(metadata.getColumnCount() > (i)){
					dbApprColumns.append(",");
					dbPendColumns.append(",");
				}
			}
			
			outHeader.println("\tpublic List<"+fileName+"Vb> getQueryPopupResults("+fileName+"Vb dObj){");
			outHeader.println("");
			outHeader.println("\t\tVector<Object> params = new Vector<Object>();");
			outHeader.println("\t\tStringBuffer strBufApprove = new StringBuffer(\"Select "+dbApprColumns+" from "+tableName+" TAppr \");");
			outHeader.println("\t\tString strWhereNotExists = new String( \" Not Exists (Select 'X' From "+tableName+"_PEND TPend Where Key Fields)\");");
			outHeader.println("\t\tStringBuffer strBufPending = new StringBuffer(\"Select "+dbPendColumns+" from "+tableName+"_PEND TPend \");");
			outHeader.println("\t\ttry");
			outHeader.println("\t\t\t{");
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				if(!("MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED,DATE_CREATION".contains(dbColumnName))){
					if(!dbColumnName.contains("_AT"))
					if(!dbColumnName.contains("_NT")){
						if(!"RECORD_INDICATOR".contentEquals(dbColumnName)){
							outHeader.println("\t\t\t\tif (ValidationUtil.isValid(dObj.get"+javaObject+"())){");
							outHeader.println("\t\t\t\t\t\tparams.addElement(\"%\" + dObj.get"+javaObject+"().toUpperCase() + \"%\");");
							outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"UPPER(TAppr."+dbColumnName+") LIKE ?\", strBufApprove);");
							outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"UPPER(TPend."+dbColumnName+") LIKE ?\", strBufApprove);");
							outHeader.println("\t\t\t\t}");
						}else{
							outHeader.println("\t\t\t\tif (dObj.getRecordIndicator() != -1){");
							outHeader.println("\t\t\t\t\tif (dObj.getRecordIndicator() > 3){");
							outHeader.println("\t\t\t\t\t\tparams.addElement(new Integer(0));");
							outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TAppr.RECORD_INDICATOR > ?\", strBufApprove);");
							outHeader.println("\t\t\t\t\t\tCommonUtils.addToQuery(\"TPend.RECORD_INDICATOR > ?\", strBufPending);");
							outHeader.println("\t\t\t\t\t}else{");
							outHeader.println("\t\t\t\t\t\t params.addElement(new Integer(dObj.getRecordIndicator()));");
							outHeader.println("\t\t\t\t\t\t CommonUtils.addToQuery(\"TAppr.RECORD_INDICATOR = ?\", strBufApprove);");
							outHeader.println("\t\t\t\t\t\t CommonUtils.addToQuery(\"TPend.RECORD_INDICATOR = ?\", strBufPending);");
							outHeader.println("\t\t\t\t\t}");
							outHeader.println("\t\t\t\t}");
						}
					}
				}
			}
			outHeader.println("\t\t\tString orderBy=\" Order By Key Fields \";");
			outHeader.println("\t\t\treturn getQueryPopupResults(dObj,strBufPending, strBufApprove, strWhereNotExists, orderBy, params);");
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
					outHeader.println("\t\tList<"+fileName+"Vb> collTemp = null;");
					outHeader.println("");
					outHeader.println("\t\tfinal int intKeyFieldsCount = \"Key field Count ;\"");

					outHeader.println("\t\tString strQueryAppr = new String(\"Select "+dbApprColumns+" From "+tableName+" TAppr where  Write Where Condition Files\");");
					outHeader.println("\t\tString strQueryPend = new String(\"Select "+dbPendColumns+" From "+tableName+"_PEND TPend where  Write Where Condition Files\");");
					outHeader.println("");
					outHeader.println("\t\tObject objParams[] = new Object[intKeyFieldsCount];");
					outHeader.println("\t\tobjParams[0] = dObj.get");
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
			insertStringBuf.append("\t\tString query =\t\"Insert Into "+tableName+" (");
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
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
				if(metadata.getColumnCount() > (i)){
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
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
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
				if(metadata.getColumnCount() > (i)){
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
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
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
				if(metadata.getColumnCount() > (i)){
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
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				insertStringBuf.append(dbColumnName+" = ?,");
//				insertStringBuf.append(System.lineSeparator());
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				insertGetValues.append("vObject.get"+javaObject+"(),");
			}
//			insertStringBuf = new StringBuffer(insertStringBuf.toString().substring(0,insertStringBuf.toString().length()-1));
			insertStringBuf.append("\";\n\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString());
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
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				insertStringBuf.append(dbColumnName+" = ?,");
//				insertStringBuf.append(System.lineSeparator());
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				insertGetValues.append("vObject.get"+javaObject+"(),");
			}
//			insertStringBuf = new StringBuffer(insertStringBuf.toString().substring(0,insertStringBuf.toString().length()-1));
			insertStringBuf.append("\";\n\t\tObject[] args = {");
			insertStringBuf.append(insertGetValues.toString());
			insertStringBuf.append("};\n");
			outHeader.println(insertStringBuf);
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");

			
			outHeader.println("");
			outHeader.println("");
			outHeader.println("\t@Override");
			outHeader.println("\tprotected int doDeleteAppr("+fileName+"Vb vObject){");
			outHeader.println("\t\tString query = \"Delete From "+tableName+" Where need to \";");
			outHeader.println("		Object[] args = {Write key fields here };");
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			
			outHeader.println("");
			outHeader.println("");
			outHeader.println("\t@Override");
			outHeader.println("\tprotected int deletePendingRecord("+fileName+"Vb vObject){");
			outHeader.println("\t\tString query = \"Delete From "+tableName+"_PEND Where need to \";");
			outHeader.println("		Object[] args = {Write key fields here };");
			outHeader.println("		return getJdbcTemplate().update(query,args);");
			outHeader.println("	}");
			outHeader.println("");
			outHeader.println("");
			
			StringBuffer stringBufferAuditTrail = new StringBuffer();
			outHeader.println("\t@Override");
			outHeader.println("\tprotected String getAuditString("+fileName+"Vb vObject){");
			outHeader.println("\t\tfinal String auditDelimiter = vObject.getAuditDelimiter();");
			outHeader.println("\t\tfinal String auditDelimiterColVal = vObject.getAuditDelimiterColVal();");
			outHeader.println("\t\tStringBuffer strAudit = new StringBuffer(\"\");");
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				insertStringBuf.append(dbColumnName+" = ?,");
//				insertStringBuf.append(System.lineSeparator());
				String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
				stringBufferAuditTrail.append("\t\t\tif(ValidationUtil.isValid(vObject.get"+javaObject+"()))\n");
				stringBufferAuditTrail.append("\t\t\t\tstrAudit.append(\""+metadata.getColumnLabel(i)+"\"+auditDelimiterColVal+vObject.get"+javaObject+"().trim());\n");
				stringBufferAuditTrail.append("\t\t\telse\n");
				stringBufferAuditTrail.append("\t\t\t\tstrAudit.append(\""+metadata.getColumnLabel(i)+"\"+auditDelimiterColVal+\"NULL\");\n");
				stringBufferAuditTrail.append("\t\t\tstrAudit.append(auditDelimiter);\n");
				stringBufferAuditTrail.append("\n");
			}
			stringBufferAuditTrail.append("\t\treturn strAudit.toString();\n");
			stringBufferAuditTrail.append("\t\t}\n");
			outHeader.println(stringBufferAuditTrail);
				
		}
			
			outHeader.println("\n\n}");
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("Dao file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}
	public void createMessageProperties(String fileName, ResultSet rs){
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+".prop";
		try {
			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File("C:\\JavaFiles\\"+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
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
	
	public void createWbObject(String fileName, ResultSet rs){
		String actFile = fileName;
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		String headerFileName = fileName+"wb.java";
		try {
			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File("C:\\JavaFiles\\"+headerFileName);
			if (!fileHeader.exists()) {
					fileHeader.createNewFile();
			}
			FileWriter fwHeader = new FileWriter(fileHeader.getAbsoluteFile());
			PrintWriter outHeader = new PrintWriter(fwHeader);
			outHeader.print("package com.vision.wb;\n\n");
			outHeader.print("public class "+fileName+"Wb extends extends AbstractDynaWorkerBean<"+fileName+"Vb>{\n\n");
			
			StringBuffer stringBuffer = new StringBuffer();
			
			stringBuffer.append("	@Autowired ");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	private "+fileName+"Dao "+fileName+"Dao;");
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
			stringBuffer.append("			collTemp = getNumSubTabDao().findActiveNumSubTabsByNumTab(1);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			arrListLocal.add(collTemp);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			collTemp = getAlphaSubTabDao().findActiveAlphaSubTabsByAlphaTab(73);");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("			arrListLocal.add(collTemp);");
			stringBuffer.append(System.lineSeparator());
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
			stringBuffer.append("@Override");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("protected List<ReviewResultVb> transformToReviewResults(List<"+fileName+"Vb> approvedCollection, List<"+fileName+"Vb> pendingCollection) {");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	ArrayList collTemp = getPageLoadValues();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	ResourceBundle rsb = CommonUtils.getResourceManger();");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	if(pendingCollection != null)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		getScreenDao().fetchMakerVerifierNames(pendingCollection.get(0));");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	if(approvedCollection != null)");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("		getScreenDao().fetchMakerVerifierNames(approvedCollection.get(0));");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	ArrayList<ReviewResultVb> lResult = new ArrayList<ReviewResultVb>();");
			stringBuffer.append(System.lineSeparator());
			outHeader.println(stringBuffer.toString());
			stringBuffer.append(System.lineSeparator());
			for(int i=1;i<= metadata.getColumnCount(); i++){
				String dbColumnName = metadata.getColumnLabel(i);
				
				if(!("'RECORD_INDICATOR_NT', '0RECORD_INDICATOR', 'MAKER', 'VERIFIER', 'INTERNAL_STATUS', 'DATE_LAST_MODIFIED','DATE_CREATION'".contains(dbColumnName))){
					String javaObject = ValidationUtil.toTitleCase(dbColumnName).replaceAll("_", "");
					stringBuffer = new StringBuffer();
					String javaSetValue = (javaObject.substring(0, 1)).toLowerCase() + javaObject.substring(1,javaObject.length()); 
					if(!dbColumnName.toUpperCase().endsWith("_AT") || !dbColumnName.toUpperCase().endsWith("_NT")) {
					stringBuffer.append("	ReviewResultVb l"+javaObject+" = new ReviewResultVb(rsb.getString(\""+javaSetValue+"\"),");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("		(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).get"+javaObject+"(),");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("		(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).get"+javaObject+"());");
					stringBuffer.append(System.lineSeparator());
					stringBuffer.append("	lResult.add(l"+javaObject+");");
					stringBuffer.append(System.lineSeparator());
					
					}
					outHeader.println(stringBuffer.toString());
				}
			}
			outHeader.println("	ReviewResultVb lRecordIndicator = new ReviewResultVb(rsb.getString(\"recordIndicator\"),");
			outHeader.println("			(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getRecordIndicatorDesc(),");
			outHeader.println("			(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getRecordIndicatorDesc());");
			outHeader.println("		lResult.add(lRecordIndicator);");
			outHeader.println("	ReviewResultVb lMaker = new ReviewResultVb(rsb.getString(\"maker\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getMaker() == 0?\"\":pendingCollection.get(0).getMakerName(),");
			outHeader.println("			(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getMaker() == 0?\"\":approvedCollection.get(0).getMakerName());");
			outHeader.println("	lResult.add(lMaker);");
			outHeader.println("	ReviewResultVb lVerifier = new ReviewResultVb(rsb.getString(\"verifier\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getVerifier() == 0?\"\":pendingCollection.get(0).getVerifierName(),");
			outHeader.println("			(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getVerifier() == 0?\"\":approvedCollection.get(0).getVerifierName());");
			outHeader.println("	lResult.add(lVerifier);");
			outHeader.println("	ReviewResultVb lDateLastModified = new ReviewResultVb(rsb.getString(\"dateLastModified\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getDateLastModified(),");
			outHeader.println("		(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getDateLastModified());");
			outHeader.println("	lResult.add(lDateLastModified);");
			outHeader.println("	ReviewResultVb lDateCreation = new ReviewResultVb(rsb.getString(\"dateCreation\"),(pendingCollection == null || pendingCollection.isEmpty())?\"\":pendingCollection.get(0).getDateCreation(),");
			outHeader.println("		(approvedCollection == null || approvedCollection.isEmpty())?\"\":approvedCollection.get(0).getDateCreation());");
			outHeader.println("	lResult.add(lDateCreation);");

			outHeader.print("\n\n}");
			outHeader.print("\n\n}");
		    outHeader.flush();
		    outHeader.close();
			fwHeader.close();
			// System.out.println("WB file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}
	
	public void createControllerObject(String fileName, ResultSet rs){
		String actulaFile = fileName;
		String screenTitle = ValidationUtil.toTitleCase(fileName).replaceAll("_", " ");
		fileName = ValidationUtil.toTitleCase(fileName).replaceAll("_", "");
		// System.out.println(actulaFile);
		// System.out.println(fileName);
		// System.out.println(screenTitle);
		String headerFileName = fileName+"Contrller.java";
		try {

			ResultSetMetaData metadata = rs.getMetaData();
			File fileHeader = new File("C:\\JavaFiles\\"+headerFileName);
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
			stringBuffer.append("@RequestMapping(\"glCodes\")");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("@Api(value = \"glCodes\", description = \""+screenTitle+"\")");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("public class "+fileName+"Controller{");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	@Autowired");
			stringBuffer.append(System.lineSeparator());
			stringBuffer.append("	"+fileName+"Wb glCodesWb;");
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
			stringBuffer.append("			ArrayList arrayList = glCodesWb.getPageLoadValues();");
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
			stringBuffer.append("			ExceptionCode exceptionCode = glCodesWb.getAllQueryPopupResult(vObject);");
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
			stringBuffer.append("			ExceptionCode  exceptionCode= glCodesWb.getQueryResultsSingle(vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.insertRecord(vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.modifyRecord(vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.deleteRecord(vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.reject(vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.approve(vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.bulkApprove(vObjects, vObject);");
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
			stringBuffer.append("			exceptionCode = glCodesWb.bulkReject(vObjects, vObject);");
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
			stringBuffer.append("			ExceptionCode exceptionCode = glCodesWb.reviewRecordNew(vObject);");
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
			// System.out.println("WB file Created Successfully !!");
		} catch (IOException | SQLException e) {
			// e.printStackTrace();
		}
	}

}  