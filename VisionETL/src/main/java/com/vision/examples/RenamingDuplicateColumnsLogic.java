package com.vision.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class RenamingDuplicateColumnsLogic {
	
	
	public static void main(String[] args) {
		
		RenamingDuplicateColumnsLogic classObj = new RenamingDuplicateColumnsLogic();
		/* Column Details - Node1*/
		List<ColumnVb> columnsLst = new ArrayList<ColumnVb>();
		columnsLst.add(new ColumnVb("COUNTRY", "COUNTRY"));
		columnsLst.add(new ColumnVb("LE_BOOK", "LE_BOOK"));
		NodeVb node1 = new NodeVb(columnsLst);
		
		/* Column Details - Node2*/
		List<ColumnVb> columnsLst2 = new ArrayList<ColumnVb>();
		columnsLst2.add(new ColumnVb("COUNTRY", "COUNTRY"));
		columnsLst2.add(new ColumnVb("LE_BOOK", "LE_BOOK"));
		columnsLst2.add(new ColumnVb("COUNTRY", "COUNTRY_1"));
		columnsLst2.add(new ColumnVb("COUNTRY", "COUNTRY_2"));
		columnsLst2.add(new ColumnVb("COUNTRY_1", "COUNTRY_1_1"));
		NodeVb node2 = new NodeVb(columnsLst2);
		
		/* Column Details - Node3*/
		List<ColumnVb> columnsLst3 = new ArrayList<ColumnVb>();
		columnsLst3.add(new ColumnVb("COUNTRY_1", "COUNTRY_1"));
		columnsLst3.add(new ColumnVb("LE_BOOK", "LE_BOOK"));
		NodeVb node3 = new NodeVb(columnsLst3);
		
		/* Add "COUNTRY" of N1 to N2 */
		ColumnVb N1C1_Vb = (node1.getColumns()).stream().filter(vb -> "COUNTRY".equalsIgnoreCase(vb.getName())).findFirst().map(v -> v).orElse(null);
		classObj.addColumnToNode(N1C1_Vb, node2);
		
		for(ColumnVb cVb:node2.getColumns()) {
			// System.out.println("Name:"+cVb.getName()+" Alias:"+cVb.getAlias());
		}
	}

	private void addColumnToNode(ColumnVb columnVb, NodeVb nodeVb) {
		/* Update columnVb with AliasName as ColumnName */
		columnVb.setName(columnVb.getAlias());
		
		String newAliasName = columnVb.getAlias();
		List<ColumnVb> existingColumns = nodeVb.getColumns();
		boolean isColNamePresent = isNewAliasPresentAsColumnName(columnVb, existingColumns);
		boolean isColAliasPresent = isNewAliasPresentAsAliasName(columnVb, existingColumns);
		if(isColNamePresent || isColAliasPresent) {
			newAliasName = newAliasName+"_1";
			newAliasName = validateTheNewAliasName(newAliasName, existingColumns);
			columnVb.setAlias(newAliasName);
		} 
		existingColumns.add(columnVb);
	}
	
	private boolean isNewAliasPresentAsColumnName(ColumnVb columnVb, List<ColumnVb> existingColumns) {
		boolean isPresent = false;
		existingChk:for(ColumnVb cVb:existingColumns) {
			if(cVb.getName().equalsIgnoreCase(columnVb.getAlias())) {
				isPresent = true;
				break existingChk;
			}
		}
		return isPresent;
	}
	
	private boolean isNewAliasPresentAsAliasName(ColumnVb columnVb, List<ColumnVb> existingColumns) {
		boolean isPresent = false;
		existingChk:for(ColumnVb cVb:existingColumns) {
			if(cVb.getAlias().equalsIgnoreCase(columnVb.getAlias())) {
				isPresent = true;
				break existingChk;
			}
		}
		return isPresent;
	}
	
	private String validateTheNewAliasName(String newAliasName, List<ColumnVb> existingColumns) {
		String validAliasName = newAliasName;
		boolean isPresent = false;
		existingChk:for(ColumnVb cVb:existingColumns) {
			if(cVb.getAlias().equalsIgnoreCase(newAliasName)) {
				isPresent = true;
				break existingChk;
			}
		}
		
		if(isPresent) {
			String[] aliasArr = newAliasName.split("_");
			int count = Integer.parseInt(aliasArr[1]);
			validAliasName = aliasArr[0]+"_"+(count+1);
			validAliasName = validateTheNewAliasName(validAliasName, existingColumns);
		}
		
		return validAliasName;
	}
	
}


class ColumnVb {
	private String name = "";
	private String alias = "";
	
	public ColumnVb(String name, String alias) {
		this.name = name;
		this.alias = alias;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
}

class NodeVb {
	private List<ColumnVb> columns = null;

	public NodeVb(List<ColumnVb> columns) {
		this.columns = columns;
	}
	
	public List<ColumnVb> getColumns() {
		return columns;
	}
	public void setColumns(List<ColumnVb> columns) {
		this.columns = columns;
	}
}