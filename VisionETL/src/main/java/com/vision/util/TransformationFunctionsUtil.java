package com.vision.util;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.vision.vb.EtlFeedTranColumnVb;

public class TransformationFunctionsUtil {

	private final String TICK = "`";
	private final String COMMA = ",";
	private final String QUOTE = "'";
	private final String SPACE = " ";
	private final String DOT = ".";
	private final String OPEN_BRACKET = "(";
	private final String CLOSE_BRACKET = ")";
	private final String SQ_OPEN_BRACKET = "[";
	private final String SQ_CLOSE_BRACKET = "]";
	private final String FROM = "FROM";
	private final String PLUS = "+";
	private final String MINUS = "-";
	private final String MULTIPLY = "*";
	private final String MODULO = "%";
	private final String DIVISION = "/";
	private final String AND = " AND ";
	private final String ON = " ON ";
	private final String WHERE = " WHERE ";
	private final String UNDERSCORE = "_";
	private final String AS = " AS ";

	private static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
	
	public static void main(String[] args) {
		try {
			TransformationFunctionsUtil classObj = new TransformationFunctionsUtil();
			Method mth = classObj.getClass().getDeclaredMethod("concatination", new Class[] { Map.class });

			// System.out.println(mth.invoke(classObj, "CONCAT_COL", new String[] { "COUNTRY", "_", "LE_BOOK" }));

		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

	public String concatination(Map<String, List<EtlFeedTranColumnVb>> ioMap) {

		/* Sample Sql :  SELECT concat('Spark', 'SQL');
			Output : SparkSQL */
		StringBuffer sqlQuery = new StringBuffer("concat");
		StringBuffer arguments = new StringBuffer();// slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				arguments.append(SPACE + QUOTE + vb.getColumnName() + QUOTE + SPACE + COMMA);
			} else {
				arguments.append(SPACE + TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + SPACE + COMMA);
			}
		}
		arguments.deleteCharAt(arguments.length() - 1);
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE +  TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + arguments + CLOSE_BRACKET + SPACE + aliasName);
		return String.valueOf(sqlQuery);
	}

	public String concatinationDelimited(Map<String, List<EtlFeedTranColumnVb>> ioMap) {

		/*
		 * Sample Sql: SELECT concat_ws(' ', 'Spark', 'SQL');
		 * Output: Spark SQL
		 */

		StringBuffer sqlQuery = new StringBuffer("concat_ws");
		StringBuffer arguments = new StringBuffer();// slab 1
		String delimiterStr = "";// slab 2

		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					arguments.append(QUOTE + vb.getColumnName() + QUOTE + COMMA);
				} else {
					arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK  + COMMA);
				}
			} else {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					delimiterStr = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					delimiterStr = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}

		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName()) ? ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + delimiterStr + COMMA + (arguments.substring(0, arguments.length() - 1))
				+ CLOSE_BRACKET + aliasName);
		return String.valueOf(sqlQuery);
	}

	public String split(Map<String, List<EtlFeedTranColumnVb>> ioMap) {

		/*
		 * Sample Sql: Concat Delimited : Split(node893667799.ConcatOp,'-')[0]
		 * Country,Split(node893667799.ConcatOp,'-')[1] Le_Book
		 * 
		 * Sample Sql: Concat : Split(node654824672.ConcatOp,'-')[0]
		 * Country,Split(node654824672.ConcatOp,'-')[1] Le_Book
		 * 
		 * Sample Sql: SELECT split('oneAtwoBthreeC', '[ABC]');
		 * Output :  ["one","two","three",""]
		 * 
		 * Sample Sql: SELECT split('oneAtwoBthreeC', '[ABC]', -1);
		 * Output : ["one","two","three",""]
		 * 
		 * Sample Sql: SELECT split('oneAtwoBthreeC', '[ABC]', 2);
		 * Output : ["one","twoBthreeC"]
		 * 
		 */

		StringBuffer sqlQuery = new StringBuffer("");
		StringBuffer splitSyntax = new StringBuffer("Split");
		String arguments = "";// slab1
		String delimiterStr = "";// slab2

		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					arguments = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					arguments = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			} else {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					delimiterStr = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					delimiterStr = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		splitSyntax.append(OPEN_BRACKET + arguments + COMMA + delimiterStr + CLOSE_BRACKET);

		int index = 0;
		for (EtlFeedTranColumnVb outputVb : ioMap.get("output")) {
			String aliasName =  SPACE + (ValidationUtil.isValid(outputVb.getAliasName()) ? outputVb.getAliasName():outputVb.getColumnName()) ;
			aliasName = aliasName + UNDERSCORE +  outputVb.getColumnId();
			sqlQuery.append(splitSyntax + SQ_OPEN_BRACKET + index + SQ_CLOSE_BRACKET + SPACE + aliasName + COMMA);
			index++;
		}
		return sqlQuery.substring(0, sqlQuery.length() - 1);
	}

	public String upper(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		/* 
		 * Sample Sql: SELECT upper('SparkSql');
		 * Output : SPARKSQL
		 * 
		 */
		StringBuffer sqlQuery = new StringBuffer("upper");
		StringBuffer upperCaseStr = new StringBuffer(); // Slab 1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				upperCaseStr.append(QUOTE + vb.getColumnName() + QUOTE);
			} else {
				upperCaseStr.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK);
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + upperCaseStr + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String lower(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		/* 
		 * Sample Sql: SELECT lower('SparkSql');
		 * Output : sparksql
		 * 
		 * */
		StringBuffer sqlQuery = new StringBuffer("lower");
		StringBuffer upperCaseStr = new StringBuffer(); // Slab 1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				upperCaseStr.append(QUOTE + vb.getColumnName() + QUOTE);
			} else {
				upperCaseStr.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK);
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + upperCaseStr + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String length(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		/* 
		 * Sample Sql: SELECT length('Spark SQL ');
		 * Output : 10
		 * 
		 * Sample Sql: SELECT CHAR_LENGTH('Spark SQL ');
		 * Output : 10
		 * 
		 * Sample Sql: SELECT CHARACTER_LENGTH('Spark SQL ');
		 * Output : 10
		 * 
		 * */
		StringBuffer sqlQuery = new StringBuffer("length");
		StringBuffer lengthStr = new StringBuffer();
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				lengthStr.append(QUOTE + vb.getColumnName() + QUOTE);
			} else {
				lengthStr.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK);
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + lengthStr + CLOSE_BRACKET + aliasName);
		return sqlQuery.toString();
	}

	public String substring(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		/* 
		 * Sample Sql: SELECT substring('Spark SQL', 5);
		 * Output : k SQL
		 * 
		 * Sample Sql: SELECT substring('Spark SQL', -3);
		 * Output : SQL
		 * 
		 * Sample Sql: SELECT substring('Spark SQL', 5, 1);
		 * Output : k
		 * 
		 * Sample Sql: SELECT substring('Spark SQL' FROM 5);
		 * Output : k SQL
		 * 
		 * Sample Sql: SELECT substring('Spark SQL' FROM -3);
		 * Output : SQL
		 * 
		 * Sample Sql: SELECT substring('Spark SQL' FROM 5 FOR 1);
		 * Output : k
		 * 
		 * */
		StringBuffer sqlQuery = new StringBuffer("substring");
		String arguments = "";// slab1
		String index = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("N".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					arguments = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + COMMA;
				} else {
					arguments = vb.getColumnName() + COMMA;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					index = vb.getColumnName();
				} else {
					index = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + arguments + index + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String substringIndex(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		
		/* Sample Sql: SELECT substring_index('www.apache.org', '.', 2);
	   	   Output : www.apache   */
	 
		StringBuffer sqlQuery = new StringBuffer("substring_index");
		String arguments = "";// slab1
		String delimiter = ""; // slab2
		String occurance = ""; // slab3
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("N".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					arguments = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + COMMA;
				} else {
					arguments = vb.getColumnName() + COMMA;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					delimiter = QUOTE + vb.getColumnName() + QUOTE + COMMA;
				} else {
					delimiter = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + COMMA;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					occurance = vb.getColumnName();
				} else {
					occurance = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + arguments + delimiter + occurance + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String addition(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		
		/* Sample Sql: SELECT 1 + 2;
	   	   Output : 3  */
		
		StringBuffer sqlQuery = new StringBuffer();
		StringBuffer arguments = new StringBuffer(); // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				arguments.append(vb.getColumnName() + PLUS);
			} else {
				arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + PLUS);
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(arguments.deleteCharAt(arguments.length() - 1) + aliasName);
		return sqlQuery.toString();
	}

	public String subtraction(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		
		/* Sample Sql: SELECT 2 - 1;
	   	   Output : 1  */
		
		StringBuffer sqlQuery = new StringBuffer();
		StringBuffer arguments = new StringBuffer(); // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				arguments.append(vb.getColumnName() + MINUS);
			} else {
				arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + MINUS);
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(arguments.deleteCharAt(arguments.length() - 1) + aliasName);
		return sqlQuery.toString();
	}

	public String multiply(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		
		/*  Sample Sql: SELECT 2 * 3;
		Output :  6  */
		
		StringBuffer sqlQuery = new StringBuffer();
		StringBuffer arguments = new StringBuffer(); // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				arguments.append(vb.getColumnName() + MULTIPLY);
			} else {
				arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + MULTIPLY);
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(arguments.deleteCharAt(arguments.length() - 1) + aliasName);
		return sqlQuery.toString();
	}

	public String moduloDivision(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		
		/*Sample Sql: SELECT 2 % 1.8; 
		Output : 0.2
	
		Sample Sql: SELECT MOD(2, 1.8); 
		Output : 0.2*/
		
		StringBuffer sqlQuery = new StringBuffer();
		StringBuffer arguments = new StringBuffer(); // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				arguments.append(vb.getColumnName() + MODULO);
			} else {
				arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + MODULO);
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(arguments.deleteCharAt(arguments.length() - 1) + aliasName);
		return sqlQuery.toString();
	}

	public String division(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		
		StringBuffer sqlQuery = new StringBuffer();
		StringBuffer arguments = new StringBuffer(); // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				arguments.append(vb.getColumnName() + DIVISION);
			} else {
				arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + DIVISION);
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(arguments.deleteCharAt(arguments.length() - 1) + aliasName);
		return sqlQuery.toString();
	}

	/*public String target(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer();
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			sqlQuery.append( TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + COMMA);
		}
		sqlQuery.deleteCharAt(sqlQuery.length() - 1);
		return sqlQuery.toString();
	}*/
	
	public String target(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer();
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			sqlQuery.append( TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK );
			String aliasName = (ValidationUtil.isValid(vb.getAliasName())?vb.getAliasName():vb.getColumnName()) ;
			aliasName = TICK + aliasName + UNDERSCORE + vb.getColumnId() + TICK;
			sqlQuery.append(AS).append(aliasName).append(COMMA);
		}
		sqlQuery.deleteCharAt(sqlQuery.length() - 1);
		return sqlQuery.toString();
	}

	public String truncateDateTime(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		String formatModel = "";// slab 1
		String timeStamp = "";// slab 2
		StringBuffer sqlQuery = new StringBuffer("date_trunc");

		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				formatModel = QUOTE + vb.getColumnName() + QUOTE;
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					timeStamp = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					timeStamp = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + formatModel + COMMA + timeStamp + CLOSE_BRACKET + aliasName);
		return String.valueOf(sqlQuery);
	}

	public String toDate(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("to_date");
		String date = ""; // slab1
		String format = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					date = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					date = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId()) && ValidationUtil.isValid(vb.getColumnName())) {
				format = QUOTE + vb.getColumnName() + QUOTE;
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(format)) {
			sqlQuery.append(OPEN_BRACKET + date + COMMA + format + CLOSE_BRACKET + SPACE + aliasName);
		} else {
			sqlQuery.append(OPEN_BRACKET + date + CLOSE_BRACKET + SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String initcap(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("initcap");
		String str = ""; // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					str = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					str = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + str + CLOSE_BRACKET + aliasName);
		return sqlQuery.toString();
	}

	public String instr(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("instr");
		String argument = "";// slab1
		String occurrence = "";// slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					argument = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					argument = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					occurrence = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					occurrence = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + argument + COMMA + occurrence + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String ltrim(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("ltrim");
		String argument = "";// slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					argument = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					argument = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + argument + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String rtrim(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("rtrim");
		String argument = "";// slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					argument = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					argument = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + argument + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String trim(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		/*
		 * Sample: 1. SELECT trim('SSparkSQLS') -> SELECT trim('<value>') 2. SELECT
		 * trim(TRAILING 'SL' FROM 'SSparkSQLS') -> SELECT trim(<option> '<trimString>'
		 * FROM '<value>')
		 */

		StringBuffer sqlQuery = new StringBuffer("trim");
		String value = "";// slab1
		String trimString = "";// slab2
		String option = "";// slab3
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					value = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					value = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					trimString = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					trimString = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				option = vb.getColumnName();
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(value) && !ValidationUtil.isValid(trimString) && !ValidationUtil.isValid(option)) {
			sqlQuery.append(OPEN_BRACKET + value + CLOSE_BRACKET + SPACE + aliasName);
		} else if (ValidationUtil.isValid(value) && !ValidationUtil.isValid(trimString)
				&& ValidationUtil.isValid(option)) {
			sqlQuery.append(OPEN_BRACKET + option + SPACE + FROM + SPACE + value + CLOSE_BRACKET + SPACE + aliasName);
		} else if (ValidationUtil.isValid(value) && ValidationUtil.isValid(trimString)
				&& !ValidationUtil.isValid(option)) {
			sqlQuery.append(OPEN_BRACKET + "BOTH" + SPACE + trimString + SPACE + FROM + SPACE + value + CLOSE_BRACKET
					+ SPACE + aliasName);
		} else if (ValidationUtil.isValid(value) && ValidationUtil.isValid(trimString)
				&& ValidationUtil.isValid(option)) {
			sqlQuery.append(OPEN_BRACKET + option + SPACE + trimString + SPACE + FROM + SPACE + value + CLOSE_BRACKET
					+ SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String lpad(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("lpad");
		String value = "";// slab1
		String length = "";// slab2
		String padString = "";// slab3
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					value = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					value = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					length = vb.getColumnName();
				} else {
					length = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				padString = QUOTE + vb.getColumnName() + QUOTE;
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(value) && ValidationUtil.isValid(length) && !ValidationUtil.isValid(padString)) {
			sqlQuery.append(OPEN_BRACKET + value + COMMA + length + CLOSE_BRACKET + SPACE + aliasName);
		} else if (ValidationUtil.isValid(value) && ValidationUtil.isValid(length)
				&& ValidationUtil.isValid(padString)) {
			sqlQuery.append(
					OPEN_BRACKET + value + COMMA + length + COMMA + padString + CLOSE_BRACKET + SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String rpad(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("rpad");
		String value = "";// slab1
		String length = "";// slab2
		String padString = "";// slab3
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					value = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					value = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					length = vb.getColumnName();
				} else {
					length = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				padString = QUOTE + vb.getColumnName() + QUOTE;
			}
		}
		String aliasName = ioMap.get("output").get(0).getColumnName() + SPACE + (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(value) && ValidationUtil.isValid(length) && !ValidationUtil.isValid(padString)) {
			sqlQuery.append(OPEN_BRACKET + value + COMMA + length + CLOSE_BRACKET + SPACE + aliasName);
		} else if (ValidationUtil.isValid(value) && ValidationUtil.isValid(length)
				&& ValidationUtil.isValid(padString)) {
			sqlQuery.append(
					OPEN_BRACKET + value + COMMA + length + COMMA + padString + CLOSE_BRACKET + SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String formatString(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("format_string");
		String string = ""; // slab1
		StringBuffer arguments = new StringBuffer(); // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					string = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					string = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					arguments.append(vb.getColumnName() + COMMA);
				} else {
					arguments.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + COMMA);
				}
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + string + COMMA + arguments.deleteCharAt(arguments.length() - 1) + CLOSE_BRACKET
				+ SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String locate(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("locate");
		String subString = "";// slab1
		String string = "";// slab2
		String position = "";// slab3 (optional)
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					subString = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					subString = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					string = vb.getColumnName();
				} else {
					string = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				position = vb.getColumnName();
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(subString) && ValidationUtil.isValid(string) && !ValidationUtil.isValid(position)) {
			sqlQuery.append(OPEN_BRACKET + subString + COMMA + string + CLOSE_BRACKET + SPACE + aliasName);
		} else if (ValidationUtil.isValid(subString) && ValidationUtil.isValid(string)
				&& ValidationUtil.isValid(position)) {
			sqlQuery.append(
					OPEN_BRACKET + subString + COMMA + string + COMMA + position + CLOSE_BRACKET + SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String regexpExtract(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("regexp_extract");
		String string = ""; // slab1
		String regularExpression = ""; // slab2
		String groupIndex = "";// slab3
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					string = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					string = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					regularExpression = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					regularExpression = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					groupIndex = vb.getColumnName();
				} else {
					groupIndex = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(string) && ValidationUtil.isValid(regularExpression)
				&& !ValidationUtil.isValid(groupIndex)) {
			sqlQuery.append(OPEN_BRACKET + string + COMMA + regularExpression + COMMA + "1" + CLOSE_BRACKET + SPACE
					+ aliasName);
		} else if (ValidationUtil.isValid(string) && ValidationUtil.isValid(regularExpression)
				&& ValidationUtil.isValid(groupIndex)) {
			sqlQuery.append(OPEN_BRACKET + string + COMMA + regularExpression + COMMA + groupIndex + CLOSE_BRACKET
					+ SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String regexpReplace(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("regexp_replace");
		String string = ""; // slab1
		String regularExpression = ""; // slab2
		String replaceString = ""; // slab3
		String position = ""; // slab4
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					string = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					string = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					regularExpression = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					regularExpression = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("3".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					replaceString = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					replaceString = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("4".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					position = vb.getColumnName();
				} else {
					position = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (ValidationUtil.isValid(string) && ValidationUtil.isValid(regularExpression)
				&& ValidationUtil.isValid(replaceString) && !ValidationUtil.isValid(position)) {
			sqlQuery.append(OPEN_BRACKET + string + COMMA + regularExpression + COMMA + replaceString + CLOSE_BRACKET
					+ SPACE + aliasName);
		} else if (ValidationUtil.isValid(string) && ValidationUtil.isValid(regularExpression)
				&& ValidationUtil.isValid(replaceString) && ValidationUtil.isValid(position)) {
			sqlQuery.append(OPEN_BRACKET + string + COMMA + regularExpression + COMMA + replaceString + COMMA + position
					+ CLOSE_BRACKET + SPACE + aliasName);
		}
		return String.valueOf(sqlQuery);
	}

	public String currentDate(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("current_date");
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + CLOSE_BRACKET + SPACE + aliasName);
		return String.valueOf(sqlQuery);
	}

	public String dateFormat(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("date_format");
		String date = ""; // slab1
		String format = "";// slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					date = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					date = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					format = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					format = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + date + COMMA + format + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String addMonths(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("add_months");
		String date = ""; // slab1
		String month = "";// slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					date = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					date = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					month = vb.getColumnName();
				} else {
					month = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + date + COMMA + month + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String dateAdd(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("date_add");
		String date = ""; // slab1
		String noOfDays = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					date = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					date = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					noOfDays = vb.getColumnName();
				} else {
					noOfDays = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + date + COMMA + noOfDays + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String dateDiff(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("datediff");
		String startDate = ""; // slab1
		String endDate = ""; // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if (vb.getColumnSortOrder() == 1) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					startDate = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					startDate = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if (vb.getColumnSortOrder() == 2) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					endDate = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					endDate = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + endDate + COMMA + startDate + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String monthsBetween(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("months_between");
		String timestamp1 = "", timestamp2 = ""; // slab1
		String roundOff = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if (vb.getColumnSortOrder() == 1) {
					if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
						timestamp1 = QUOTE + vb.getColumnName() + QUOTE;
					} else {
						timestamp1 = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
					}
				}
				if (vb.getColumnSortOrder() == 2) {
					if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
						timestamp2 = QUOTE + vb.getColumnName() + QUOTE;
					} else {
						timestamp2 = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
					}
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					roundOff = vb.getColumnName();
				} else {
					roundOff = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (!ValidationUtil.isValid(roundOff)) {
			sqlQuery.append(OPEN_BRACKET + timestamp1 + COMMA + timestamp2 + CLOSE_BRACKET + SPACE + aliasName);
		} else {
			sqlQuery.append(OPEN_BRACKET + timestamp1 + COMMA + timestamp2 + COMMA + roundOff + CLOSE_BRACKET + SPACE
					+ aliasName);
		}
		return sqlQuery.toString();
	}

	public String nextDay(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("next_day");
		String date = ""; // slab1
		String day = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					date = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					date = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					day = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					day = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE +  TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + date + COMMA + day + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String lastDay(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("last_day");
		String date = ""; // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					date = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					date = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + date + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}

	public String fromUnixtime(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("from_unixtime");
		String unixTime = ""; // slab1
		String format = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					unixTime = vb.getColumnName();
				} else {
					unixTime = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					format = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					format = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName =  (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (!ValidationUtil.isValid(format)) {
			sqlQuery.append(OPEN_BRACKET + unixTime + CLOSE_BRACKET + SPACE + aliasName);
		} else {
			sqlQuery.append(OPEN_BRACKET + unixTime + COMMA + format + CLOSE_BRACKET + SPACE + aliasName);
		}
		return sqlQuery.toString();
	}

	public String unixTimestamp(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("unix_timestamp");
		String timeStamp = ""; // slab1
		String dateFormat = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					timeStamp = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					timeStamp = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					dateFormat = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					dateFormat = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		if (!ValidationUtil.isValid(timeStamp)) {
			sqlQuery.append(OPEN_BRACKET + CLOSE_BRACKET + SPACE + aliasName);
		} else {
			sqlQuery.append(OPEN_BRACKET + timeStamp + COMMA + dateFormat + CLOSE_BRACKET + SPACE + aliasName);
		}
		return sqlQuery.toString();
	}
	
	public String[] aggregation(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		String[] returnArr = new String[2];  
		 // returnArr[0] -- select 
		//  returnArr[1] -- group by  
		StringBuffer measure = new StringBuffer();
		StringBuffer dimention = new StringBuffer();
		StringBuffer selectBuf = new StringBuffer();
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					if ("Y".equalsIgnoreCase(vb.getOutputFlag())) {
						selectBuf.append(QUOTE + vb.getColumnName() + QUOTE + COMMA);
					}
					dimention.append(QUOTE + vb.getColumnName() + QUOTE + COMMA);
				} else {
					if ("Y".equalsIgnoreCase(vb.getOutputFlag())) {
						selectBuf.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + SPACE + TICK + vb.getAliasName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
					}
					dimention.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + COMMA);
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				measure
				.append(SPACE + returnAggFunction(vb.getAggFunction()))
				.append(OPEN_BRACKET + TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + CLOSE_BRACKET)
				.append(SPACE + TICK + vb.getOutputColumnName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			}
		}
		if(measure.length()>0) {
			measure.deleteCharAt(measure.length() - 1);
		}

		if(ValidationUtil.isValid(String.valueOf(selectBuf)) && ValidationUtil.isValid(String.valueOf(measure))) {
			returnArr[0] = String.valueOf(selectBuf) + measure;
			returnArr[1] = "GROUP BY " + dimention.deleteCharAt(dimention.length() - 1);
		} else if(ValidationUtil.isValid(String.valueOf(selectBuf))){
			returnArr[0] = selectBuf.substring(0, (selectBuf.length()-1));
			returnArr[1] = "GROUP BY " + dimention.deleteCharAt(dimention.length() - 1);
		} else {
			returnArr[0] = String.valueOf(measure);
			returnArr[1] = "";
		}
		return returnArr;
	}
	
	public String returnAggFunction(String function) {
		String returnVal = "";
		switch (function.toUpperCase()) {
		case "MAX":
			returnVal = "MAX";
			break;
		case "MIN":
			returnVal = "MIN";
			break;
		case "SUM":
			returnVal = "SUM";
			break;
		case "VARIANCE":
			returnVal = "VARIANCE";
			break;
		case "FIRST":
			returnVal = "FIRST";
			break;
		case "LAST":
			returnVal = "LAST";
			break;
		case "COUNT":
			returnVal = "COUNT";
			break;
		case "AVG":
			returnVal = "AVG";
			break;
		case "MEAN":
			returnVal = "MEAN";
			break;
		case "GROUPING":
			returnVal = "GROUPING";
			break;
		default:
			break;
		}
		return returnVal;
	}

	public String join(Map<String, List<EtlFeedTranColumnVb>> ioMap, String joinType) {
		ioMap.get("input").sort(Comparator.comparing(EtlFeedTranColumnVb::getColumnSortOrder));
		String masterTableNodeID = ((ioMap.get("input")).stream().filter(vb -> ("1".equalsIgnoreCase(vb.getSlabId())))
				.findAny().map(v -> v).orElse(null)).getParentNodeId();
		String detailTableNodeID = ((ioMap.get("input")).stream().filter(vb -> ("2".equalsIgnoreCase(vb.getSlabId())))
				.findAny().map(v -> v).orElse(null)).getParentNodeId();
		StringBuffer fromClause = new StringBuffer(FROM);
		StringBuffer selectColumns = new StringBuffer("SELECT ");
		StringBuffer joinCondition = new StringBuffer("");
		List<EtlFeedTranColumnVb> ip_ColumnVbList = ioMap.get("input");
		
		//Column list of Details Slab - useful to filter linked column vb detail
		List<EtlFeedTranColumnVb> detailsSlabColumnList = ip_ColumnVbList.stream().filter(v -> "2".equalsIgnoreCase(v.getSlabId())).collect(Collectors.toList());
		
		for (EtlFeedTranColumnVb vb : ip_ColumnVbList) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if(ValidationUtil.isValid(vb.getLinkedColumn())) {
					/* Get linked column Vb detail */
					EtlFeedTranColumnVb linkedColumnVb = getLinkedColumnDetailWithLinkedColumnName(detailsSlabColumnList, vb.getLinkedColumn());
					joinCondition
					.append(TICK + masterTableNodeID + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK)
					.append(returnJoinOperator(vb.getJoinOperator().toUpperCase()))
					.append(TICK + detailTableNodeID + TICK + DOT + TICK + linkedColumnVb.getColumnName() + UNDERSCORE + linkedColumnVb.getParentColumnId() + TICK)
					.append(AND);
				}
			}
			if ("Y".equalsIgnoreCase(vb.getOutputFlag())) {
				selectColumns.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + SPACE + TICK + vb.getAliasName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			}
		}
		fromClause
		.append(SPACE + TICK + masterTableNodeID + TICK + SPACE + TICK + masterTableNodeID + TICK + SPACE)
		.append(joinType)
		.append(SPACE + TICK + detailTableNodeID + TICK + SPACE + TICK + detailTableNodeID + TICK + SPACE)
		.append(ON);
		return selectColumns.substring(0, selectColumns.length() - 1) + SPACE + fromClause + SPACE + OPEN_BRACKET
				+ joinCondition.substring(0, (joinCondition.length() - 5)) + CLOSE_BRACKET;
	}
	
	private EtlFeedTranColumnVb getLinkedColumnDetailWithLinkedColumnName (List<EtlFeedTranColumnVb> columnList, String linkedColumnAliasName) {
		return columnList.stream().filter(vb -> linkedColumnAliasName.equalsIgnoreCase(vb.getAliasName())).findFirst().map(v -> v).orElse(null);
	}
	
	public String returnJoinOperator(String operator) {
		String output = "";
		try {
			switch (operator) {
			case "EQUAL":
				output = " = ";
				break;

			case "NOTEQUAL":
				output = " != ";
				break;

			case "LESSTHAN":
				output = " < ";
				break;

			case "GREATERTHAN":
				output = " > ";
				break;

			case "LESSTHANEQUALTO":
				output = " <= ";
				break;

			case "GREATERTHANEQUALTO":
				output = " >= ";
				break;

			/*case "IN":
				output = " IN( " + detailTable + DOT + linkedCol + CLOSE_BRACKET;
				break;

			case "NOTIN":
				output = " NOT IN (" + detailTable + DOT + linkedCol + CLOSE_BRACKET;
				break;

			case "BETWEEN":
				String arrVal[] = linkedCol.split(",");
				output = " BETWEEN (" + detailTable + DOT + arrVal[0] + ")  AND (" + detailTable + DOT + arrVal[1] + ") ";
				break;*/

			default:
				break;
			}
			return output;
		} catch (Exception e) {
			// e.printStackTrace();
			return null;
		}
	}
	
	public String[] filter(Map<String, List<EtlFeedTranColumnVb>> ioMap, boolean isFilterCustomFlag) {
		// returnArr[0] -- select 
		//  returnArr[1] -- where  
		ioMap.get("input").sort(Comparator.comparing(EtlFeedTranColumnVb::getColumnSortOrder));
		StringBuffer selectColumns = new StringBuffer("");
		StringBuffer whereCondition = new StringBuffer();
		String[] returnArr = new String[2];  
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getOutputFlag())) {
				selectColumns.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + SPACE + TICK + vb.getAliasName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			}
			if(ValidationUtil.isValid(vb.getFilterCriteria())) {
				whereCondition.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK + SPACE)
				.append(returnFilterCondition(vb.getFilterCriteria(), vb.getFilterValue1(), vb.getFilterValue2())
						+ SPACE + AND);
			}
		}
		if (isFilterCustomFlag == false) {
			returnArr[0] = selectColumns.substring(0, selectColumns.length() - 1);
			if(ValidationUtil.isValid(String.valueOf(whereCondition))) {
				returnArr[1] = WHERE + whereCondition.substring(0, (whereCondition.length() - 5));
			}
		} else {
			returnArr[0] = selectColumns.substring(0, selectColumns.length() - 1);
			returnArr[1] = "";
		}
		return returnArr;
	}
	
	
	
	public String returnFilterCondition(String filterCriteria, String filterValue1, String filterValue2) {
		String output = "";
		try {
			switch (filterCriteria) {
			case "EQUAL":
				output = " = " + QUOTE + filterValue1 + QUOTE;
				break;

			case "NOTEQUAL":
				output = " != " + QUOTE + filterValue1 + QUOTE;
				break;

			case "LESSTHAN":
				output = " < " + QUOTE + filterValue1 + QUOTE;
				break;

			case "GREATERTHAN":
				output = " > " + QUOTE + filterValue1 + QUOTE;
				break;

			case "LESSTHANEQUALTO":
				output = " <= " + QUOTE + filterValue1 + QUOTE;
				break;

			case "GREATERTHANEQUALTO":
				output = " >= " + QUOTE + filterValue1 + QUOTE;
				break;

			case "IN":
				filterValue1 = filterValue1.replaceAll(COMMA, QUOTE + COMMA + QUOTE);
				output = " IN ('" + filterValue1 + QUOTE + CLOSE_BRACKET;
				break;

			case "NOTIN":
				filterValue1 = filterValue1.replaceAll(COMMA, QUOTE + COMMA + QUOTE);
				output = " NOT IN ('" + filterValue1 + QUOTE + CLOSE_BRACKET;
				break;

			case "BETWEEN":
				output = " BETWEEN ('" + filterValue1 + "')  AND ('" + filterValue2 + "') ";
				break;

			default:
				break;
			}
			return output;
		} catch (Exception e) {
			// e.printStackTrace();
			return null;
		}
	}

	public String selectQualifier(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer selectColumns = new StringBuffer("");
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("Y".equalsIgnoreCase(vb.getOutputFlag())) {
				selectColumns.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE
						+ vb.getParentColumnId() + TICK + SPACE + vb.getAliasName() + UNDERSCORE + vb.getColumnId()
						+ COMMA);
			}
		}
		return selectColumns.substring(0, selectColumns.length() - 1);
	}
	
	public String formatNumber(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("format_number");
		String number = ""; // slab1
		String format = ""; // slab2
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					number = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					number = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					if (vb.getColumnName().contains("#")) {
						format = QUOTE + vb.getColumnName() + QUOTE;
					} else {
						format = vb.getColumnName();
					}
				} else {
					format = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName =  SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + number + COMMA + format + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}
	public String isNull(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("isnull");
		String expression = ""; // slab1
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					expression = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					expression = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + expression + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
	}
	
	public String expressionText(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("");
		for (EtlFeedTranColumnVb vb : ioMap.get("output")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				sqlQuery.append(vb.getExpressionText() + SPACE + TICK + vb.getColumnName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			} else {
				sqlQuery.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK);
				sqlQuery.append(AS);
				sqlQuery.append(TICK + vb.getAliasName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			}
		}
		return sqlQuery.substring(0, sqlQuery.length() - 1);
	}
	
	public String caseCondition(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("");
		for (EtlFeedTranColumnVb vb : ioMap.get("output")) {
			if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
				sqlQuery.append(vb.getExpressionText() + SPACE + TICK + vb.getColumnName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			} else {
				sqlQuery.append(TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK);
				sqlQuery.append(AS);
				sqlQuery.append(TICK + vb.getAliasName() + UNDERSCORE + vb.getColumnId() + TICK + COMMA);
			}
		}
		return sqlQuery.substring(0, sqlQuery.length() - 1);
	}
	
	public String ifnull(Map<String, List<EtlFeedTranColumnVb>> ioMap) {
		StringBuffer sqlQuery = new StringBuffer("ifnull");
		String expression = ""; // slab1
		String replaceValue = "";
		for (EtlFeedTranColumnVb vb : ioMap.get("input")) {
			if ("1".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					expression = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					expression = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
			if ("2".equalsIgnoreCase(vb.getSlabId())) {
				if ("Y".equalsIgnoreCase(vb.getDerivedColumnFlag())) {
					replaceValue = QUOTE + vb.getColumnName() + QUOTE;
				} else {
					replaceValue = TICK + vb.getParentNodeId() + TICK + DOT + TICK + vb.getColumnName() + UNDERSCORE + vb.getParentColumnId() + TICK;
				}
			}
		}
		String aliasName = (ValidationUtil.isValid(ioMap.get("output").get(0).getAliasName())?ioMap.get("output").get(0).getAliasName():ioMap.get("output").get(0).getColumnName()) ;
		aliasName = SPACE + TICK + aliasName + UNDERSCORE + ioMap.get("output").get(0).getColumnId() + TICK;
		sqlQuery.append(OPEN_BRACKET + expression + COMMA + replaceValue + CLOSE_BRACKET + SPACE + aliasName);
		return sqlQuery.toString();
		
	}
}
