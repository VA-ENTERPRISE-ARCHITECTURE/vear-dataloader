package gov.va.aes.vear.dataloader.model;

public class DatabaseColumn {
    private String dbColName;
    private String dbColType;
    private Long pickListTableId;
    private boolean excelColumnDataCleanup;
    private int columnSize;

    public DatabaseColumn(String dbColName, String dbColType, Long pickListTableId, boolean excelColumnDataCleanup,
	    int columnSize) {
	this.dbColName = dbColName;
	this.dbColType = dbColType;
	this.pickListTableId = pickListTableId;
	this.excelColumnDataCleanup = excelColumnDataCleanup;
	this.columnSize = columnSize;
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
