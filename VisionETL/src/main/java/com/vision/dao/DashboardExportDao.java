package com.vision.dao;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import com.vision.vb.ExportWidgetVb;
import com.vision.vb.WidgetDesignVb;

@Component
public class DashboardExportDao extends AbstractDao<WidgetDesignVb>{
	@SuppressWarnings("unchecked")
	public void wrideDataForGidWithQuery(ExportWidgetVb exportWidgetVb, XSSFWorkbook wb, String query) {
		
		Sheet widgetSheet = wb.getSheet(exportWidgetVb.getChartName());
		Sheet dataSheet = wb.getSheet("Main Sheet");
		
		getJdbcTemplate().query(query, new ResultSetExtractor() {

			@Override
			public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnCount = rsmd.getColumnCount();
				
				/*int startCol = exportWidgetVb.getX();
				int startRow = exportWidgetVb.getY();
				int endCol = exportWidgetVb.getX()+exportWidgetVb.getCols();
				int endRow = exportWidgetVb.getY()+exportWidgetVb.getRows();
				int noOfRowsByDesign = exportWidgetVb.getRows()-2;
				int noOfColsByDesign = exportWidgetVb.getCols();
				int noOfColsToChkDesign = (columnCount<noOfColsByDesign)?columnCount:noOfColsByDesign;
				RowSetFactory factory = RowSetProvider.newFactory();
		    	CachedRowSet rsChild = factory.createCachedRowSet();
		    	rsChild.populate(rs);
		    	int rowCountForDesign = 0;
		    	int rowIndexForDesign = startRow+2;
		    	while(rsChild.next() && rowCountForDesign<noOfRowsByDesign) {
		    		Row row = dataSheet.getRow(rowIndexForDesign);
		    		if(row == null) {
		    			row = dataSheet.createRow(rowIndexForDesign);
		    		}
		    		int colIndexForDesign = startCol;
		    		for(int colCount = 0;colCount<noOfColsToChkDesign;colCount++) {
		    			CellUtil.createCell(row, colIndexForDesign, rsChild.getString(colCount+1));
		    			colIndexForDesign++;
		    		}
		    		rowCountForDesign++;
		    		rowIndexForDesign++;
		    	}
				
				int rowIndex = 1;
				rsChild.beforeFirst();
				while(rsChild.next()) {
					Row row = widgetSheet.createRow(rowIndex);
					for(int colIndex = 0;colIndex<columnCount;colIndex++) {
						CellUtil.createCell(row, colIndex, rsChild.getString(colIndex+1));
					}
					rowIndex++;
				}
				try{rsChild.close();}catch(Exception e) {}*/
				
				int rowIndex = 1;
				while(rs.next()) {
					Row row = widgetSheet.createRow(rowIndex);
					for(int colIndex = 0;colIndex<columnCount;colIndex++) {
						CellUtil.createCell(row, colIndex, rs.getString(colIndex+1));
					}
					rowIndex++;
				}
				return null;
			}
		});
	}
}
