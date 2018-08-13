package gov.va.aes.vear.dataloader.model;

public class DatabaseColumn {
    private String dbColName;
    private String dbColType;
    private Long pickListTableId;
    private boolean excelColumnDataCleanup;
    private int columnSize;
    private String mappedTableName;
    private String mappedKeyColumn;
    private String mappedValueColumn;
    private String MappedFilter;

    public DatabaseColumn(String dbColName, String dbColType, Long pickListTableId, boolean excelColumnDataCleanup,
	    int columnSize, String mappedTableName, String mappedKeyColumn, String mappedValueColumn,
	    String mappedFilter) {
	this.dbColName = dbColName;
	this.dbColType = dbColType;
	this.pickListTableId = pickListTableId;
	this.excelColumnDataCleanup = excelColumnDataCleanup;
	this.columnSize = columnSize;
	this.mappedTableName = mappedTableName;
	this.mappedKeyColumn = mappedKeyColumn;
	this.mappedValueColumn = mappedValueColumn;
	this.MappedFilter = mappedFilter;
    }

    public String getMappedTableName() {
	return mappedTableName;
    }

    public void setMappedTableName(String mappedTableName) {
	this.mappedTableName = mappedTableName;
    }

    public String getMappedKeyColumn() {
	return mappedKeyColumn;
    }

    public void setMappedKeyColumn(String mappedKeyColumn) {
	this.mappedKeyColumn = mappedKeyColumn;
    }

    public String getMappedValueColumn() {
	return mappedValueColumn;
    }

    public void setMappedValueColumn(String mappedValueColumn) {
	this.mappedValueColumn = mappedValueColumn;
    }

    public String getMappedFilter() {
	return MappedFilter;
    }

    public void setMappedFilter(String mappedFilter) {
	MappedFilter = mappedFilter;
    }

    public boolean isExcelColumnDataCleanup() {
	return excelColumnDataCleanup;
    }

    public void setExcelColumnDataCleanup(boolean excelColumnDataCleanup) {
	this.excelColumnDataCleanup = excelColumnDataCleanup;
    }

    public String getDbColName() {
	return dbColName;
    }

    public void setDbColName(String dbColName) {
	this.dbColName = dbColName;
    }

    public String getDbColType() {
	return dbColType;
    }

    public void setDbColType(String dbColType) {
	this.dbColType = dbColType;
    }

    public Long getPickListTableId() {
	return pickListTableId;
    }

    public void setPickListTableId(Long pickListTableId) {
	this.pickListTableId = pickListTableId;
    }

    public int getColumnSize() {
	return columnSize;
    }

    public void setColumnSize(int columnSize) {
	this.columnSize = columnSize;
    }

}
