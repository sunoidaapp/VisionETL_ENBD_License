package com.vision.wb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vision.exception.RuntimeCustomException;
import com.vision.util.Node;
import com.vision.util.ValidationUtil;
import com.vision.vb.ETLFeedTablesVb;


public class EtlDynamicJoinNew  {
	
	public static Logger logger = LoggerFactory.getLogger(EtlDynamicJoinNew.class);
	
	public static void main(String[] args) {
		String arr[] = {"0", "4"};
//		String arr[] = {"0", "1", "2", "3"};
		List<String> sample = new ArrayList<String>(Arrays.asList(arr));
		List<Integer> intTblIdList = sample.stream().mapToInt(e -> Integer.parseInt(e)).boxed().collect(Collectors.toList());
		Collections.sort(intTblIdList);
		sample = intTblIdList.stream().map(v -> String.valueOf(v)).collect(Collectors.toList());
		sample.stream().forEach(System.out::println);
	}
	
	static String captureErrorMsg="";
	static HashMap<String,List> linkedTblsForIndividualTblHM = null;
	static HashMap<String,List<String>> relationHM = null;
//	static HashMap<String,String> aliasNameForTableIDHM = null;
//	static HashMap<String,String> tableNameForTableIDHM = null;
	static HashMap<String,ETLFeedTablesVb> tableDetailsForTableIDHM = null;
	static HashSet<String> linkKeysHhSet = null;
	static List<String> tableIdList = new ArrayList<String>();
	static HashSet<String> doneTableIdsSet = null;
	static List<String> unDoneTableIdsList = null;
	static String catalogId = "";
	static Integer baseTable = null;
	static HashMap<Integer,List> directLinkLvlHM = null;
	static StringBuffer returnJoinSB = null;
	static StringBuffer returnFromSB = null;
	static List<String> chkList = null;
	static final String hyphen = "-";
	static final String openBraces = "(";
	static final String closeBraces = ")";
	static final String space = " ";
	static final String on = space+"ON"+space;
	static final String comma = ", ";
	
	private static void initializeVariables(){
		linkedTblsForIndividualTblHM = null;
		relationHM = null;
//		aliasNameForTableIDHM = null;
//		tableNameForTableIDHM = null;
		tableDetailsForTableIDHM = null;
		linkKeysHhSet = null;
		tableIdList = new ArrayList<String>();
		doneTableIdsSet = new HashSet<String>();
		unDoneTableIdsList = null;
		catalogId = "";
		baseTable = null;
		directLinkLvlHM = new HashMap<Integer,List>();
		returnJoinSB = new StringBuffer();
		returnFromSB = new StringBuffer();
		chkList = new ArrayList<String>();
	}
	
	protected Connection returnConnection() throws Exception {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			return DriverManager.getConnection("jdbc:oracle:thin:@10.16.1.101:1521:VISION", "VISIONBI", "vision123");
		} catch (Exception e) {
			throw e;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static synchronized HashMap<String, Object> formDynamicJoinString(String catalogIdReq, Integer joinSyntaxType, List<String> tableIdArrReq, ArrayList relationMetaData, Integer baseTableId){
		HashMap<String, Object> returnMap = new HashMap<String, Object>();
		try {
		initializeVariables();
		tableIdList = tableIdArrReq;
		unDoneTableIdsList = new ArrayList<String>(tableIdArrReq);
		catalogId = catalogIdReq;
		linkedTblsForIndividualTblHM = (HashMap<String,List>) relationMetaData.get(0);
		relationHM = (HashMap<String,List<String>>) relationMetaData.get(1);
//		aliasNameForTableIDHM = (HashMap<String,String>) relationMetaData.get(2);
//		tableNameForTableIDHM = (HashMap<String,String>) relationMetaData.get(3);
		tableDetailsForTableIDHM = (HashMap<String,ETLFeedTablesVb>) relationMetaData.get(2);
		baseTable = baseTableId;
		
//		if (tableIdList.size() == 1) {
			if (baseTable != null) {
				HashSet<String> tempTableIdList = new HashSet<String>();
				tempTableIdList.add(String.valueOf(baseTable));
				tempTableIdList.addAll(tableIdList);
				tableIdList = new ArrayList<String>();
				tableIdList.addAll(tempTableIdList);
				unDoneTableIdsList = new ArrayList<String>(tableIdList);
			}
//		}
			
		if(tableIdList.size()>1) {
			
			checkDirectLinks();
			
			if(checkIfDirectLinkLvlsAreUnrelated() || tableIdList.size() == unDoneTableIdsList.size() || unDoneTableIdsList.size() >0) {
				try {
					if(directLinkLvlHM.size()>1) {
						findExtendedRelationsBetweenDirectLinkLvls();
					}
					if(unDoneTableIdsList.size()>0) {
						List<String> tempTableIdList = new ArrayList<String>(tableIdList);;
						tempTableIdList.removeAll(unDoneTableIdsList);
						doneTableIdsSet.addAll(tempTableIdList);
						if(doneTableIdsSet.size()==0)
							findExtendedRelationsForUndoneTablesWithoutDoneTables();
						else
							findExtendedRelationsForUndoneTables();
					}
					linkKeysHhSet.stream().forEach(System.out::println);
					
					if(joinSyntaxType == 1) {
						formJoinStringForANSII();
					} else {
						formJoinStringForStandardJoin();
					}
				} catch (Exception e) {
					// e.printStackTrace();
					captureErrorMsg =e.getMessage();
					throw new RuntimeCustomException(e.getMessage());
				}
			} else if(unDoneTableIdsList.size() == 0) {
				if(joinSyntaxType == 1) {
					formJoinStringForANSII();
				} else {
					formJoinStringForStandardJoin();
				}
			}
			HashSet<String> returnTables = new HashSet<String>();
			if(linkKeysHhSet.size()>0) {
				for(String key : linkKeysHhSet) {
					returnTables.add(key.split("-")[0]);
					returnTables.add(key.split("-")[1]);
				}
			}
			formFromStringForStandardJoin(returnTables);
			returnMap.put("LINKS",linkKeysHhSet);
			returnMap.put("TABLES",returnTables);
		} else if (tableIdList.size() == 1) {
			formFromStringForSingleTable(tableIdList.get(0));
			if(joinSyntaxType != 1)
				returnJoinSB = new StringBuffer();
		}
		
		if(joinSyntaxType == 1) {
			returnMap.put("FROM",returnJoinSB);
			returnMap.put("WHERE","");
			returnMap.put("ERROR",captureErrorMsg);
		} else {
			returnMap.put("FROM",returnFromSB);
			returnMap.put("WHERE",returnJoinSB);
			returnMap.put("ERROR",captureErrorMsg);
			
		}
		}catch (Exception e) {
			// TODO: handle exception
		}finally {
			returnMap.put("ERROR",captureErrorMsg);
		}
		return returnMap;
	}
	
	private static void formFromStringForSingleTable(String tableId) {
		returnFromSB.append(chkAndReturnKeyForFromTblId(tableDetailsForTableIDHM.get(tableId))+space+tableDetailsForTableIDHM.get(tableId).getTableAliasName());
		returnJoinSB.append(chkAndReturnKeyForFromTblId(tableDetailsForTableIDHM.get(tableId))+space+tableDetailsForTableIDHM.get(tableId).getTableAliasName());
	}
	
	private static void formFromStringForStandardJoin(HashSet<String> returnTables) {
		for(String tableId : returnTables) {
			returnFromSB.append(chkAndReturnKeyForFromTblId(tableDetailsForTableIDHM.get(tableId))+space+tableDetailsForTableIDHM.get(tableId).getTableAliasName()+comma);
		}
		if(ValidationUtil.isValid(String.valueOf(returnFromSB)))
			returnFromSB = new StringBuffer(returnFromSB.substring(0, (returnFromSB.length()-2)));
	}
	
	private static void formJoinStringForStandardJoin() {
		if(linkKeysHhSet.size()>0) {
			for(String key : linkKeysHhSet) {
				if(!ValidationUtil.isValid(String.valueOf(returnJoinSB))) {
					returnJoinSB.append(relationHM.get(key).get(1));
				} else {
					if(ValidationUtil.isValid(relationHM.get(key).get(1)))
						returnJoinSB.append(" AND "+relationHM.get(key).get(1));
				}
			}
		}
	}
	
	private static void formJoinStringForANSII() {
		List<String> linkKeysList = linkKeysHhSet.stream().collect(Collectors.toList());
		Collections.sort(linkKeysList);
		
		linkKeysList.stream().forEach(System.out::println);
		String currentKey = linkKeysList.get(0);
		
		appendStringForRelationANSII(currentKey, false);
	}
	
	
	private static void appendStringForRelationANSII(String key, boolean joinSwapRequired){
		
		chkList.add(hyphen+key+hyphen);
		
		String joinType = relationHM.get(key).get(0);
		
		if(joinSwapRequired) {
			if("2".equalsIgnoreCase(joinType))
				joinType = "3";
			if("3".equalsIgnoreCase(joinType))
				joinType = "2";
		}
		
		String joinText = relationHM.get(key).get(1);
		
		String fromTblId = key.substring(0, key.indexOf("-"));
		String fromTblName = chkAndReturnKeyForFromTblId(tableDetailsForTableIDHM.get(fromTblId));
		String fromTblAlias = tableDetailsForTableIDHM.get(fromTblId).getTableAliasName();
		
		String toTblId = key.substring(key.indexOf("-")+1, key.length());
		String toTblName = chkAndReturnKeyForFromTblId(tableDetailsForTableIDHM.get(toTblId));
		String toTblAlias = tableDetailsForTableIDHM.get(toTblId).getTableAliasName();
		
		StringBuffer tempStrBuf = new StringBuffer();
		if(!ValidationUtil.isValid(String.valueOf(returnJoinSB))) {
			tempStrBuf.append(fromTblName+space+fromTblAlias);
			tempStrBuf.append(space+returnJoinStringANSII(joinType)+space);
			tempStrBuf.append(toTblName+space+toTblAlias);
			if(!"4".equalsIgnoreCase(joinType))
				tempStrBuf.append(on+openBraces+joinText+closeBraces);
			returnJoinSB = tempStrBuf;
		} else {
			tempStrBuf.append(openBraces);
			tempStrBuf.append(String.valueOf(returnJoinSB));
			tempStrBuf.append(closeBraces);
			tempStrBuf.append(space+returnJoinStringANSII(joinType)+space);
			tempStrBuf.append(toTblName+space+toTblAlias);
			if(!"4".equalsIgnoreCase(joinType))
				tempStrBuf.append(on+openBraces+joinText+closeBraces);
			returnJoinSB = tempStrBuf;
		}
		
		String chkKey = chkAndReturnKeyForFromTblId(fromTblId);
		if(ValidationUtil.isValid(chkKey)) {
			appendStringForRelationANSII(chkKey, false);
		}
		chkKey = chkAndReturnKeyForFromTblIdAsToTbl(fromTblId);
		if(ValidationUtil.isValid(chkKey)) {
			appendStringForRelationANSII(chkKey, true);
		}
		chkKey = chkAndReturnKeyForToTblId(toTblId);
		if(ValidationUtil.isValid(chkKey)) {
			appendStringForRelationANSII(chkKey, false);
		}
		chkKey = chkAndReturnKeyForToTblIdAsToTbl(toTblId);
		if(ValidationUtil.isValid(chkKey)) {
			appendStringForRelationANSII(chkKey, true);
		}
	}
	
	public static String getDBType(String country, String leBook, String connectorId, String tableId) {
		String script="";
		try {
			String sql ="Select T1.SOURCE_CONNECTOR_ID, T1.TABLE_ID , T1.COUNTRY FROM ETL_FEED_TABLES_UPL T1 ,"+
				 " ETL_CONNECTOR T2 WHERE T1.SOURCE_CONNECTOR_ID= T2.CONNECTOR_ID " + 
				 " AND  T1.TABLE_ID='"+tableId+"' AND T1.SOURCE_CONNECTOR_ID='"+connectorId+"' and T1.COUNTRY='"+country+"' AND T1.LE_BOOK='"+leBook+"'";
			//script = getJdbcTemplate().queryForObject(sql, String.class);
		}catch(Exception e){
			// e.printStackTrace();
		}
		return script;
	}
	

	private static String chkAndReturnKeyForFromTblId(ETLFeedTablesVb treeVb) {
		//String script = getDBType(treeVb.getCountry(), treeVb.getLeBook(), treeVb.getSourceConnectorId(), treeVb.getTableId());
		return treeVb.getTableName();
		
/*
		if("F".equalsIgnoreCase(treeVb.getTableType())) {
			return "{{F_("+treeVb.getTableName()+"_"+treeVb.getTableId()+")}}";
		} else if("T".equalsIgnoreCase(treeVb.getTableType())) {
			if(CommonUtils.DEFAULT_DB.equalsIgnoreCase(treeVb.getDatabaseConnectivityDetails())) {
				return treeVb.getTableName();
			} else {
				return "{{DB_("+treeVb.getTableName()+"_"+treeVb.getTableId()+")}}";
			}
		} else if("M".equalsIgnoreCase(treeVb.getTableType())) {
			return "{{MQ_("+treeVb.getTableName()+"_"+treeVb.getTableId()+")}}";
		} else {
			throw new RuntimeCustomException("Problem in source type maintenance of the table "+treeVb.getTableAliasName()+" ID["+treeVb.getTableId()+"]");
		}*/
		/*if("FILE".equalsIgnoreCase(treeVb.getDatabaseType())) {
			return "{{F_("+treeVb.getTableName()+"_"+treeVb.getTableId()+")}}";
		} else if("MACROVAR".equalsIgnoreCase(treeVb.getDatabaseType())) {
			if(CommonUtils.DEFAULT_DB.equalsIgnoreCase(treeVb.getDatabaseConnectivityDetails())) {
				return treeVb.getTableName();
			} else {
				return "{{DB_("+treeVb.getTableName()+"_"+treeVb.getTableId()+")}}";
			}
		} else if("M_QUERY".equalsIgnoreCase(treeVb.getDatabaseType())) {
			return "{{MQ_("+treeVb.getTableName()+"_"+treeVb.getTableId()+")}}";
		} else {
			throw new RuntimeCustomException("Problem in source type maintenance of the table "+treeVb.getTableAliasName()+" ID["+treeVb.getTableId()+"]");
		}*/
	}
	private static String chkAndReturnKeyForFromTblId(String fromTblId) {
		return linkKeysHhSet.stream().filter(
				key -> (((String) key).startsWith(fromTblId + hyphen) && !chkList.contains(hyphen + key + hyphen)))
				.collect(Collectors.collectingAndThen(Collectors.toList(), keyList -> {
					return keyList.size()>0?keyList.get(0):null;
				}));
	}
	
	private static String chkAndReturnKeyForFromTblIdAsToTbl(String fromTblId) {
		return linkKeysHhSet.stream().filter(
				key -> (((String) key).endsWith(hyphen + fromTblId) && !chkList.contains(hyphen + key + hyphen)))
				.collect(Collectors.collectingAndThen(Collectors.toList(), keyList -> {
					return keyList.size()>0?keyList.get(0):null;
				}));
	}
	
	private static String chkAndReturnKeyForToTblId(String toTblId) {
		return linkKeysHhSet.stream().filter(
				key -> (((String) key).startsWith(toTblId + hyphen) && !chkList.contains(hyphen + key + hyphen)))
				.collect(Collectors.collectingAndThen(Collectors.toList(), keyList -> {
					return keyList.size()>0?keyList.get(0):null;
				}));
	}
	
	private static String chkAndReturnKeyForToTblIdAsToTbl(String toTblId) {
		return linkKeysHhSet.stream()
				.filter(key -> (((String) key).endsWith(hyphen + toTblId) && !chkList.contains(hyphen + key + hyphen)))
				.collect(Collectors.collectingAndThen(Collectors.toList(), keyList -> {
					return keyList.size()>0?keyList.get(0):null;
				}));
	}
	
	private static String returnJoinStringANSII(String joinType){
		switch(joinType) {
		case "1" :
			joinType = "INNER JOIN";
			break;
		case "2" :
			joinType = "LEFT OUTER JOIN";
			break;
		case "3" :
			joinType = "RIGHT OUTER JOIN";
			break;
		case "4" :
			joinType = "CROSS JOIN";
			break;
		default :
			joinType = "INNER JOIN";
			break;
		}
		return joinType;
	}
	
	private static void findExtendedRelationsForUndoneTables() throws RuntimeCustomException, Exception {
		List<String> unDoneTblMainList = new ArrayList<String>(unDoneTableIdsList);
		for(String tbl1:unDoneTblMainList) {
			List<Node<String>> nodeLvlList = new ArrayList<Node<String>>();
			Node<String> targetNode = null;
			for(String tbl2:doneTableIdsSet) {
				targetNode = formLink(tbl1, tbl2);
				if(targetNode!=null) {
					nodeLvlList.add(targetNode);
				}else {
					captureErrorMsg ="Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName()+"";
					throw new RuntimeCustomException("Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName());
				}
			}
			if(nodeLvlList.size() == 1) {
				targetNode = nodeLvlList.get(0);
			} else if(nodeLvlList.size()>1) {
				targetNode = compareAndReturnOptimumTargetNode(nodeLvlList);
			}
			
			addRelationToLinkKeyList(targetNode);
		}
	}
	
	private static void findExtendedRelationsForUndoneTablesWithoutDoneTables() throws RuntimeCustomException, Exception {
		List<String> unDoneTblMainList = new ArrayList<String>(unDoneTableIdsList);
		for(String tbl1:unDoneTblMainList) {
			List<Node<String>> nodeLvlList = new ArrayList<Node<String>>();
			Node<String> targetNode = null;
			for(String tbl2:unDoneTableIdsList) {
				if(!tbl1.equalsIgnoreCase(tbl2)) {
					targetNode = formLink(tbl1, tbl2);
					if(targetNode!=null)
						nodeLvlList.add(targetNode);
					else
						captureErrorMsg ="Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName()+"";
						throw new RuntimeCustomException("Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName());
				}
			}
			if(nodeLvlList.size() == 1) {
				targetNode = nodeLvlList.get(0);
			} else if(nodeLvlList.size()>1) {
				targetNode = compareAndReturnOptimumTargetNode(nodeLvlList);
			}
			
			addRelationToLinkKeyList(targetNode);
		}
	}
	
	private static void findExtendedRelationsBetweenDirectLinkLvls() throws RuntimeCustomException, Exception {
		for(Entry<Integer, List> entry:directLinkLvlHM.entrySet()) {
			HashSet<String> mainSet = (HashSet<String>) entry.getValue().get(1);
			Node<String> returnNode = null;
			if(!(boolean) entry.getValue().get(0)) {
				List<Node<String>> linkLvlList = new ArrayList<Node<String>>();
				for(Entry<Integer, List> innerEntry : directLinkLvlHM.entrySet()) {
					returnNode = null;
					if(innerEntry.getKey()!=entry.getKey() && (boolean) innerEntry.getValue().get(0)){
						List<Node<String>> nodeLvlList = new ArrayList<Node<String>>();
						HashSet<String> subSet = (HashSet<String>) innerEntry.getValue().get(1);
						for(int mainIndex = 0;mainIndex<mainSet.size();mainIndex++) {
							String tbl1 = (String) (mainSet.toArray())[mainIndex];
							for(int subIndex = 0;subIndex<subSet.size();subIndex++) {
								String tbl2 = (String) (subSet.toArray())[subIndex];
								Node<String> targetNode = formLink(tbl1, tbl2);
								if(targetNode!=null)
									nodeLvlList.add(targetNode);
								else
									captureErrorMsg ="Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName()+"";
									throw new RuntimeCustomException("Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName());
							}
						}
						if(nodeLvlList.size() == 1) {
							returnNode = nodeLvlList.get(0);
						} else if(nodeLvlList.size()>1) {
							returnNode = compareAndReturnOptimumTargetNode(nodeLvlList);
						}
					}
					if(returnNode!=null)
						linkLvlList.add(returnNode);
				}
				if(linkLvlList.size() == 1) {
					returnNode = linkLvlList.get(0);
				} else if(linkLvlList.size()>1) {
					returnNode = compareAndReturnOptimumTargetNode(linkLvlList);
				}
				
				List valueList = directLinkLvlHM.get(entry.getKey());
				valueList.set(0,true);
				directLinkLvlHM.put(entry.getKey(), valueList);
				addRelationToLinkKeyList(returnNode);
			}
		}
	}
	
	private static void addRelationToLinkKeyList (Node<String> node) {
		if(node!=null) {
			Node<String> currentNode = node;
			int length = 0;
			while(currentNode.getParent()!=null) {
				if(relationHM.get(currentNode.getData()+"-"+currentNode.getParent().getData())!=null)
					linkKeysHhSet.add(String.valueOf(currentNode.getData()+"-"+currentNode.getParent().getData()));
				else
					linkKeysHhSet.add(String.valueOf(currentNode.getParent().getData()+"-"+currentNode.getData()));
				unDoneTableIdsList.remove(currentNode.getData());
				unDoneTableIdsList.remove(currentNode.getParent().getData());
				doneTableIdsSet.add(currentNode.getData());
				doneTableIdsSet.add(currentNode.getParent().getData());
				currentNode = currentNode.getParent();
				length++;
			}
		}
	}
	
	private static Node<String> formLink (String tbl1, String tbl2) {
		Node<String> returnNode = null;
		Node<String> root = new Node<String>(tbl1);
		addChildForOneNode(root);
		List<Node<String>> childNodeList = root.getChildren();
		if(childNodeList==null)
			return null;
		chkLoop:for(Node<String> each:childNodeList){
			if(each.getData().equalsIgnoreCase(tbl2)){
				returnNode = each;
				break chkLoop;
			}
		}
		if(returnNode==null) {
			List<Node<String>> targetNodeList = new ArrayList<Node<String>>();
			for(Node<String> each:childNodeList){
				returnNode = formLinkWithChildren(childNodeList, tbl2);
				if(returnNode!=null) {
					targetNodeList.add(returnNode);
				} else {
					captureErrorMsg="Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName()+"";
					throw new RuntimeCustomException("Invalid Relation Configuration between tables "+tableDetailsForTableIDHM.get(tbl1).getTableAliasName() + " and "+tableDetailsForTableIDHM.get(tbl2).getTableAliasName());
				}
			}
			if(targetNodeList.size()==1) {
				returnNode = targetNodeList.get(0);
			} else if(targetNodeList.size()>1) {
				returnNode = compareAndReturnOptimumTargetNode(targetNodeList);
			}
		}
		return returnNode;
	}
	
	private static Node<String> compareAndReturnOptimumTargetNode(List<Node<String>> targetNodeList) {
		Node<String> returnNode = null;
		int length = 9999;
		for(Node<String> currentNode : targetNodeList) {
			int result = findNodeLength(currentNode);
			if(result<length) {
				length = result;
				returnNode = currentNode;
			}
		}
		return returnNode;
	}
	
	private static int findNodeLength(Node<String> node) {
		Node<String> currentNode = node;
		int length = 0;
		while(currentNode.getParent()!=null) {
			currentNode = currentNode.getParent();
			length++;
		}
		return length;
	}
	
	private static Node<String> formLinkWithChildren(List<Node<String>> childNodeList, String destTblId) {
		if(childNodeList==null || childNodeList.size() == 0) {
			return null;
		}
		List<Node<String>> localChildNodeList = new ArrayList<Node<String>>();
		HashSet<Integer> setToAvoidDuplicates = new HashSet<Integer>();
		Node<String> returnNode = null;
		addChildForNodeList(childNodeList);
		chkLoop:for(Node<String> eachParent:childNodeList){
			List<Node<String>> childNode = eachParent.getChildren();
			for(Node<String> eachChild:childNode){
				if(eachChild.getData().equalsIgnoreCase(destTblId)){
					returnNode = eachChild;
					break chkLoop;
				}
				if(setToAvoidDuplicates.add(Integer.parseInt(eachChild.getData())))
					localChildNodeList.add(eachChild);
			}
		}
		if(returnNode==null) {
			returnNode = formLinkWithChildren(localChildNodeList, destTblId);
		}
		return returnNode;
	}
	
	private static void addChildForOneNode(Node<String> node){
		String tableId = node.getData();
		List<Integer> parentNodeList = formParentNodeList(node);
		List linkedTblList = linkedTblsForIndividualTblHM.get(tableId);
		List<Node<String>> childList = new ArrayList<Node<String>>();
		if(linkedTblList!=null && linkedTblList.size()>0) {
			for(Object tabId:linkedTblList){
				if(!checkIfParentNodeRepeats(parentNodeList, String.valueOf(tabId)))
					childList.add(new Node<String>(String.valueOf(tabId)));
			}
			node.addChildren(childList);
		}

	}
	
	private static void addChildForNodeList(List<Node<String>> nodeList){
		Iterator<Node<String>> nodeItr = nodeList.iterator();
		while(nodeItr.hasNext()){
			Node<String> each = nodeItr.next();
			List<Integer> parentNodeList = formParentNodeList(each);
			String tableId = each.getData();
			List linkedTblList = linkedTblsForIndividualTblHM.get(tableId);
			List<Node<String>> childList = new ArrayList<Node<String>>();
			for(Object tabId:linkedTblList){
				if(!checkIfParentNodeRepeats(parentNodeList, String.valueOf(tabId)))
					childList.add(new Node<String>(String.valueOf(tabId)));
			}
			each.addChildren(childList);
		}
	}
	
	private static List<Integer> formParentNodeList(Node<String> node) {
		if(node.getParent() != null) {
			List<Integer> parentNodeList = new ArrayList<Integer>();
			Node<String> currentNode = node.getParent();
			while(currentNode!=null) {
				parentNodeList.add(Integer.parseInt(currentNode.getData()));
				currentNode = currentNode.getParent();
			}
			return parentNodeList;
		} else {
			return null;
		}
	}
	
	private static boolean checkIfParentNodeRepeats(List<Integer> parentNodeList, String tableId) {
		if(parentNodeList!=null) {
			return parentNodeList.stream().filter(tblId -> tblId == Integer.parseInt(tableId))
					.collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
						return ValidationUtil.isValidList(list) ? true : false;
					}));
		} else {
			return false;
		}
	}
	
	private static boolean checkIfDirectLinkLvlsAreUnrelated() {
		return (directLinkLvlHM.entrySet().stream().filter(e -> !(boolean) e.getValue().get(0))).count()>0?true:false;
	}
	
	private static String chkRelation (String tbl1, String tbl2) {
		boolean result = false;
		String key = "";
		if (relationHM.get(tbl1 + "-" + tbl2) != null) {
			result = true;
			key = tbl1+"-"+tbl2;
		} else if (relationHM.get(tbl2 + "-" + tbl1) != null) {
			result = true;
			key = tbl2+"-"+tbl1;
		} else {
			result = false;
		}
		return result?key:null;
	}
	
	private static void checkDirectLinks() {
		linkKeysHhSet = new LinkedHashSet<String>();
		String key = "";
		for(int iIndex=0;iIndex<tableIdList.size();iIndex++){
			String val1 = tableIdList.get(iIndex);
			HashSet<String> doneTableHhSet = new HashSet<String>();
			for(int jIndex=0;jIndex<unDoneTableIdsList.size();jIndex++){
				String val2 = unDoneTableIdsList.get(jIndex);
				if(!val1.equalsIgnoreCase(val2)){
					key = chkRelation(val1, val2);
					if(key!=null){
						linkKeysHhSet.add(key);
						unDoneTableIdsList.remove(val1);
						unDoneTableIdsList.remove(val2);
						doneTableHhSet.add(val1);
						doneTableHhSet.add(val2);
					}
				}
			}
			if(doneTableHhSet.size()>0) {
				List val = new ArrayList();
				val.add(directLinkLvlHM.size()==0?true:false);//Linked to other direct Link Set?
				val.add(doneTableHhSet);
				directLinkLvlHM.put(iIndex, val);
			}
		}
		if(directLinkLvlHM.size()>1) {
			for(Entry<Integer, List> entry:directLinkLvlHM.entrySet()) {
				key = "";
				HashSet<String> mainSet = (HashSet<String>) entry.getValue().get(1);
				subEntry_DirectLinkLvl:for(Entry<Integer, List> innerEntry:directLinkLvlHM.entrySet()) {
					if(entry.getKey()!=innerEntry.getKey()) {
						if(false == (boolean) innerEntry.getValue().get(0)) {
							HashSet<String> subSet = (HashSet<String>) innerEntry.getValue().get(1);
							for(int mainIndex = 0;mainIndex<mainSet.size();mainIndex++) {
								String tbl1 = (String) (mainSet.toArray())[mainIndex];
								for(int subIndex = 0;subIndex<subSet.size();subIndex++) {
									String tbl2 = (String) (subSet.toArray())[subIndex];
									key = chkRelation(tbl1, tbl2);
									if(key!=null){
										linkKeysHhSet.add(key);
										List innerValueLst = innerEntry.getValue();
										innerValueLst.set(0, true);
										directLinkLvlHM.put(innerEntry.getKey(), innerValueLst);
										List valueLst = entry.getValue();
										valueLst.set(0, true);
										directLinkLvlHM.put(entry.getKey(), valueLst);
										break subEntry_DirectLinkLvl;
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
}
