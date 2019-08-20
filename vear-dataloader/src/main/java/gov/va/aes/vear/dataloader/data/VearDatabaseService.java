package gov.va.aes.vear.dataloader.data;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class VearDatabaseService {

    private static final Logger LOG = Logger.getLogger(VearDatabaseService.class.getName());

    @Autowired
    public JdbcTemplate jdbcTemplate;

    public boolean isVASIDBAccessible() {
	int i = 0;
	try {
	    i = jdbcTemplate.queryForObject("SELECT 1 FROM DUAL", Integer.class);
	} catch (Exception e) {
	    LOG.log(Level.SEVERE, "Not able to establish connection to VASI Database.");
	    return false;
	}

	if (i == 1) {
	    LOG.log(Level.INFO, "Successfully connected to VASI Database.");
	    return true;
	}
	return false;
    }

    public void processDbRecordInsert(Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	List<Object> insertParams = new ArrayList<>();
	String sql = "insert into  " + tableAndColumnMappingInfo.getTableName();
	String colNamesSQL = null;
	String valuesSQLStr = null;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (colNamesSQL == null) {
		colNamesSQL = " ( " + mapping.getValue().getDbColName();
	    } else {
		colNamesSQL = colNamesSQL + ", " + mapping.getValue().getDbColName();
	    }
	    String value = "NULL";
	    Object columnValue = excelRecord.get(mapping.getKey());
	    if (columnValue != null) {
		value = "?";

		if (mapping.getValue().getColumnSize() > 0) {// size truncation
		    if (columnValue != null && columnValue instanceof String
			    && ((String) columnValue).getBytes().length > mapping.getValue().getColumnSize()) {
			int originalSize = ((String) columnValue).getBytes().length;
			int originalLength = ((String) columnValue).length();

			columnValue = ((String) columnValue).substring(0, mapping.getValue().getColumnSize() - 5);
			LOG.log(Level.FINE,
				"Truncating text greater than " + mapping.getValue().getColumnSize()
					+ " bytes. Column Name:" + mapping.getKey() + " Original: " + originalLength
					+ "(" + originalSize + ") Truncated: " + ((String) columnValue).length() + "("
					+ ((String) columnValue).getBytes().length + ")");

		    }
		}

		insertParams.add(columnValue);
	    }

	    if (valuesSQLStr == null) {
		valuesSQLStr = " values (" + value;
	    } else {
		valuesSQLStr = valuesSQLStr + " , " + value;
	    }

	}
	colNamesSQL = colNamesSQL + ")";
	valuesSQLStr = valuesSQLStr + ")";
	sql = sql + colNamesSQL + valuesSQLStr;
	// PK loop
	LOG.log(Level.FINE, "DB INSERT SQL :" + sql);
	LOG.log(Level.FINE, "DB INSERT PARAMS :" + insertParams.toString());
	// Insert Records
	jdbcTemplate.update(sql, insertParams.toArray(new Object[insertParams.size()]));
    }

    public void processDbRecordUpdate(Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	List<Object> updateParams = new ArrayList<>();
	String sql = "update " + tableAndColumnMappingInfo.getTableName() + " set ";
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (!tableAndColumnMappingInfo.getPkColumnMappings().containsKey(mapping.getKey())) {
		Object columnValue = excelRecord.get(mapping.getKey());
		if (mapping.getValue().getColumnSize() > 0) {// size truncation
		    if (columnValue != null && columnValue instanceof String
			    && ((String) columnValue).getBytes().length > mapping.getValue().getColumnSize()) {
			int originalSize = ((String) columnValue).getBytes().length;
			int originalLength = ((String) columnValue).length();

			columnValue = ((String) columnValue).substring(0, mapping.getValue().getColumnSize() - 5);
			LOG.log(Level.FINE,
				"Truncating text greater than " + mapping.getValue().getColumnSize()
					+ " bytes. Column Name:" + mapping.getKey() + " Original: " + originalLength
					+ "(" + originalSize + ") Truncated: " + ((String) columnValue).length() + "("
					+ ((String) columnValue).getBytes().length + ")");

		    }
		}

		sql = sql.concat(mapping.getValue().getDbColName()).concat(" = ").concat("?").concat(",");
		updateParams.add(columnValue);

	    }
	}
	sql = sql.substring(0, sql.length() - 1);
	String whereSQL = null;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings().entrySet()) {
	    if (whereSQL == null) {
		whereSQL = " where " + mapping.getValue().getDbColName() + " =  ?";
		updateParams.add(excelRecord.get(mapping.getKey()));
	    } else {
		whereSQL = whereSQL + " and " + mapping.getValue().getDbColName() + " =  ?";
		updateParams.add(excelRecord.get(mapping.getKey()));
	    }

	}
	sql = sql + whereSQL;

	LOG.log(Level.FINE, "DB UPDATE SQL :" + sql);
	LOG.log(Level.FINE, "DB UPDATE PARAMS :" + updateParams.toString());

	// update records
	jdbcTemplate.update(sql, updateParams.toArray(new Object[updateParams.size()]));
    }

    public String getDbRecordUpdateSQL(TableAndColumnMappingInfo tableAndColumnMappingInfo) {

	String sql = "update " + tableAndColumnMappingInfo.getTableName() + " set ";
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (!tableAndColumnMappingInfo.getPkColumnMappings().containsKey(mapping.getKey())) {

		sql = sql.concat(mapping.getValue().getDbColName()).concat(" = ").concat("?").concat(",");

	    }
	}
	sql = sql.substring(0, sql.length() - 1);
	String whereSQL = null;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings().entrySet()) {
	    if (whereSQL == null) {
		whereSQL = " where " + mapping.getValue().getDbColName() + " =  ?";

	    } else {
		whereSQL = whereSQL + " and " + mapping.getValue().getDbColName() + " =  ?";

	    }

	}
	sql = sql + whereSQL;

	LOG.log(Level.FINE, "DB UPDATE SQL :" + sql);

	return sql;

    }

    public List<Object> getDbRecordUpdateParams(Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	List<Object> updateParams = new ArrayList<>();

	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (!tableAndColumnMappingInfo.getPkColumnMappings().containsKey(mapping.getKey())) {
		Object columnValue = excelRecord.get(mapping.getKey());
		if (mapping.getValue().getColumnSize() > 0) {// size truncation
		    if (columnValue != null && columnValue instanceof String
			    && ((String) columnValue).getBytes().length > mapping.getValue().getColumnSize()) {
			int originalSize = ((String) columnValue).getBytes().length;
			int originalLength = ((String) columnValue).length();

			columnValue = ((String) columnValue).substring(0, mapping.getValue().getColumnSize() - 5);
			LOG.log(Level.FINE,
				"Truncating text greater than " + mapping.getValue().getColumnSize()
					+ " bytes. Column Name:" + mapping.getKey() + " Original: " + originalLength
					+ "(" + originalSize + ") Truncated: " + ((String) columnValue).length() + "("
					+ ((String) columnValue).getBytes().length + ")");

		    }
		}

		updateParams.add(columnValue);

	    }
	}

	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings().entrySet()) {

	    updateParams.add(excelRecord.get(mapping.getKey()));

	}

	LOG.log(Level.FINE, "DB UPDATE PARAMS :" + updateParams.toString());

	return updateParams;

    }

    public void insertOrUpdateBatch(String sql, List<List<Object>> paramsList) {

	// for (List<Object> params : paramsList) {
	// try {
	// jdbcTemplate.update(sql, params.toArray(new Object[params.size()]));
	// } catch (DataAccessException e) {
	// LOG.log(Level.INFO, "params=" + params);
	// }
	// }

	jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

	    @Override
	    public void setValues(PreparedStatement ps, int i) throws SQLException {
		List<Object> params = paramsList.get(i);
		for (int j = 0; j < params.size(); j++) {
		    StatementCreatorUtils.setParameterValue(ps, j + 1, SqlTypeValue.TYPE_UNKNOWN, params.get(j));
		}
	    }

	    @Override
	    public int getBatchSize() {

		return paramsList.size();
	    }
	});
    }

    public String getDbRecordInsertSQL(TableAndColumnMappingInfo tableAndColumnMappingInfo) {

	String sql = "insert into  " + tableAndColumnMappingInfo.getTableName();
	String colNamesSQL = null;
	String valuesSQLStr = null;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (colNamesSQL == null) {
		colNamesSQL = " ( " + mapping.getValue().getDbColName();
	    } else {
		colNamesSQL = colNamesSQL + ", " + mapping.getValue().getDbColName();
	    }

	    if (valuesSQLStr == null) {
		valuesSQLStr = " values ( ?";
	    } else {
		valuesSQLStr = valuesSQLStr + " , ?";
	    }

	}
	colNamesSQL = colNamesSQL + ")";
	valuesSQLStr = valuesSQLStr + ")";
	sql = sql + colNamesSQL + valuesSQLStr;
	LOG.log(Level.FINE, "DB INSERT SQL :" + sql);
	return sql;
    }

    public List<Object> getDbRecordInsertParams(Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	List<Object> insertParams = new ArrayList<>();

	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {

	    Object columnValue = excelRecord.get(mapping.getKey());
	    if (mapping.getValue().getColumnSize() > 0) {// size truncation
		if (columnValue != null && columnValue instanceof String
			&& ((String) columnValue).getBytes().length > mapping.getValue().getColumnSize()) {
		    int originalSize = ((String) columnValue).getBytes().length;
		    int originalLength = ((String) columnValue).length();

		    columnValue = ((String) columnValue).substring(0, mapping.getValue().getColumnSize() - 5);
		    LOG.log(Level.FINE,
			    "Truncating text greater than " + mapping.getValue().getColumnSize()
				    + " bytes. Column Name:" + mapping.getKey() + " Original: " + originalLength + "("
				    + originalSize + ") Truncated: " + ((String) columnValue).length() + "("
				    + ((String) columnValue).getBytes().length + ")");

		}
	    }
	    insertParams.add(columnValue);

	}

	LOG.log(Level.FINE, "DB INSERT PARAMS :" + insertParams.toString());
	return insertParams;
    }

    public Map<String, Object> getDBRecord(Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	Map<String, Object> dbRecord = null;

	String colNamesSQL = null;
	List<Object> selectWhereParams = new ArrayList<>();

	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (colNamesSQL == null) {
		colNamesSQL = mapping.getValue().getDbColName();
	    } else {
		colNamesSQL = colNamesSQL + ", " + mapping.getValue().getDbColName();
	    }

	}
	String whereSQL = null;
	boolean foundAllPKValues = true;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings().entrySet()) {
	    Object param = excelRecord.get(mapping.getKey());
	    String columnName = mapping.getValue().getDbColName();
	    if (param == null) {
		foundAllPKValues = false;
	    }
	    if (whereSQL == null) {
		whereSQL = " where " + columnName + " =  ?";
		selectWhereParams.add(param);
	    } else {
		whereSQL = " and " + columnName + " =  ?";
		selectWhereParams.add(param);
	    }

	}
	if (!foundAllPKValues)
	    return null; // Cannot get DB record when Primary Key value is missing

	String sql = "select  " + colNamesSQL + " from " + tableAndColumnMappingInfo.getTableName() + whereSQL;

	LOG.log(Level.FINE, "DB Select SQL :" + sql);
	LOG.log(Level.FINE, "DB Select SQL :" + selectWhereParams);

	try {
	    dbRecord = jdbcTemplate.queryForMap(sql, selectWhereParams.toArray(new Object[selectWhereParams.size()]));

	} catch (EmptyResultDataAccessException e) {
	    // ignore this and return null;
	}
	return dbRecord;
    }

    public List<Map<String, Object>> getAllDBRecords(TableAndColumnMappingInfo tableAndColumnMappingInfo)
	    throws IOException {

	// TODO this method does not handle multiple tables
	List<Map<String, Object>> dbRecords = null;

	String colNamesSQL = null;
	String tableName = null;

	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (colNamesSQL == null) {
		colNamesSQL = mapping.getValue().getDbColName();
	    } else {
		colNamesSQL = colNamesSQL + ", " + mapping.getValue().getDbColName();
	    }
	    tableName = tableAndColumnMappingInfo.getTableName();
	}

	String sql = "select  " + colNamesSQL + " from " + tableName;// + " where parent_file = 3200117";
	LOG.log(Level.FINE, "DB Select SQL for all dbRecords: " + sql);

	try {
	    dbRecords = jdbcTemplate.queryForList(sql);
	    LOG.log(Level.FINE, "dbRecords size: " + dbRecords.size());
	} catch (Throwable e) {
	    LOG.log(Level.SEVERE, "Unable to read records from VEAR Database.", e);
	}
	return dbRecords;
    }

}
