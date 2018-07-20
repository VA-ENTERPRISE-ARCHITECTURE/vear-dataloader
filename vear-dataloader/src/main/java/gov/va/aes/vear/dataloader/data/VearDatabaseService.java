package gov.va.aes.vear.dataloader.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.main.VearDataLoader;
import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class VearDatabaseService {

    @Autowired
    public JdbcTemplate jdbcTemplate;

    public void processDbRecordInsert(VearDataLoader vearDataLoader, Map<String, Object> excelRecord,
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
	    if (excelRecord.get(mapping.getKey()) != null) {
		value = "?";
		insertParams.add(excelRecord.get(mapping.getKey()));
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
	System.out.println("DB INSERT SQL :" + sql);
	System.out.println("DB INSERT PARAMS :" + insertParams.toString());
	// jdbcTemplate.update(sql, insertParams);
    }

    public void processDbRecordUpdate(VearDataLoader vearDataLoader, Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	List<Object> updateParams = new ArrayList<>();
	String sql = "update " + tableAndColumnMappingInfo.getTableName() + " set ";
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    if (!tableAndColumnMappingInfo.getPkColumnMappings().containsKey(mapping.getKey())) {
		if (excelRecord.get(mapping.getKey()) != null) {
		    sql = sql.concat(mapping.getValue().getDbColName()).concat(" = ").concat("?").concat(",");
		    updateParams.add(excelRecord.get(mapping.getKey()));
		}
	    }
	}
	sql = sql.substring(0, sql.length() - 1);
	String whereSQL = null;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings().entrySet()) {
	    if (whereSQL == null) {
		whereSQL = " where " + mapping.getValue().getDbColName() + " =  ?";
		updateParams.add(excelRecord.get(mapping.getKey()));
	    } else {
		whereSQL = " and " + mapping.getValue().getDbColName() + " =  ?";
		updateParams.add(excelRecord.get(mapping.getKey()));
	    }

	}
	sql = sql + whereSQL;
	// jdbcTemplate.update(sql, updateParams);

	System.out.println("DB UPDATE SQL :" + sql);
	System.out.println("DB UPDATE PARAMS :" + updateParams.toString());
    }

    public Map<String, Object> getDBRecord(VearDataLoader vearDataLoader, Map<String, Object> excelRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	Map<String, Object> dbRecord = null;
	// TODO Auto-generated method stub

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
	    if (param != null) {
		System.out.println(
			"Column : " + columnName + " Value: " + param + " Type: " + param.getClass().getName());
	    } else {
		System.out.println("Column : " + columnName + " Value: " + param);
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

	System.out.println("DB Select SQL :" + sql);
	System.out.println("DB Select SQL :" + selectWhereParams);

	// Use SQL and Params to execute Query using template.qureyForObject method
	dbRecord = jdbcTemplate.queryForMap(sql, selectWhereParams.toArray(new Object[selectWhereParams.size()]));

	return dbRecord;
    }

}
