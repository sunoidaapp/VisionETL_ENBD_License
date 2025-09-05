package com.vision.vb;

public class RelationMapVb extends CommonVb{
	
	private static final long serialVersionUID = -5022264255321929212L;

	private String fColumn = "";
	private String tColumn = "";

	public RelationMapVb() {}
	
	public RelationMapVb(String fColumn, String tColumn) {
		this.fColumn = fColumn;
		this.tColumn = tColumn;
	}

	public String getfColumn() {
		return fColumn;
	}

	public void setfColumn(String fColumn) {
		this.fColumn = fColumn;
	}

	public String gettColumn() {
		return tColumn;
	}

	public void settColumn(String tColumn) {
		this.tColumn = tColumn;
	}
	
}
