package gov.va.aes.vear.dataloader.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by I831828 on 6/17/2018.
 */
public class TableAndColumnMappingInfo implements Serializable {

    private String tableName;
    private Map<String, DatabaseColumn> columnMappings;
    private Map<String, DatabaseColumn> pkColumnMappings;

    public TableAndColumnMappingInfo() {
	this.columnMappings = new HashMap<>();
	this.pkColumnMappings = new HashMap<>();
    }

    public String getTableName() {
	return tableName;
    }

    public void setTableName(String tableName) {
	this.tableName = tableName;
    }

    public Map<String, DatabaseColumn> getColumnMappings() {
	return columnMappings;
    }

    public void setColumnMappings(Map<String, DatabaseColumn> columnMappings) {
	this.columnMappings = columnMappings;
    }

    public Map<String, DatabaseColumn> getPkColumnMappings() {
	return pkColumnMappings;
    }

    public void setPkColumnMappings(Map<String, DatabaseColumn> pkColumnMappings) {
	this.pkColumnMappings = pkColumnMappings;
    }

    public void addColumnMapping(final String csvColumnName, String dbColName, String dbColType, Long pickListTableId,
	    boolean excelColumnDataCleanup) {
	this.columnMappings.put(csvColumnName,
		new DatabaseColumn(dbColName, dbColType, pickListTableId, excelColumnDataCleanup));
    }

    public void addPkColumnMapping(final String csvColumnName, String dbColName, String dbColType) {
	this.pkColumnMappings.put(csvColumnName, new DatabaseColumn(dbColName, dbColType, null, false));
    }

}
