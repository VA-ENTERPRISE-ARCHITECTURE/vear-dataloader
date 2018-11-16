package gov.va.aes.vear.dataloader.main;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.PrimaryKeyMapping;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class CompareRecords {

    @Autowired
    PrimaryKeyMapping primaryKeyMapping;

    Boolean compareDbNullEqualsExcelBlank;

    private static final Logger LOG = Logger.getLogger(CompareRecords.class.getName());

    public boolean checkAttributesChanged(Map<String, Object> excelRecord, Map<String, Object> dbRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	boolean checkAttributesChanged = false;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    DatabaseColumn dbColumn = mapping.getValue();
	    if (tableAndColumnMappingInfo.getPkColumnMappings().containsKey(dbColumn.getDbColName()))
		continue;
	    Object columnValue = excelRecord.get(mapping.getKey());

	    if (dbColumn.getColumnSize() > 0) {
		if (columnValue != null && columnValue instanceof String
			&& ((String) columnValue).getBytes().length > dbColumn.getColumnSize()) {
		    columnValue = ((String) columnValue).substring(0, dbColumn.getColumnSize() - 5);
		}
	    }

	    checkAttributesChanged = !compareObject(columnValue, dbRecord.get(dbColumn.getDbColName()));
	    if (checkAttributesChanged) {

		String pkValueStr = primaryKeyMapping.getPKValueAsString(excelRecord,
			Arrays.asList(tableAndColumnMappingInfo));
		LOG.log(Level.FINE,
			"Compare NOT Matching data for column " + dbColumn.getDbColName() + ", for Excel record ["
				+ pkValueStr + "] value: " + columnValue + " - Db Record value: "
				+ dbRecord.get(dbColumn.getDbColName()));
		break;
	    }
	}

	return checkAttributesChanged;
    }

    private boolean compareObject(Object obj1, Object obj2) {
	boolean compareFlag = false;
	;
	if (obj1 instanceof String && obj2 instanceof String)
	    return compare((String) obj1, (String) obj2);
	compareFlag = (!hasValue(obj1, compareDbNullEqualsExcelBlank) ? obj2 == null
		: (obj1.equals(obj2) || (obj2 != null && compareObject(obj1.toString(), obj2.toString()))));
	return compareFlag;
    }

    private boolean compare(String str1, String str2) {
	boolean compareFlag = false;
	str1 = cleanUp(str1);
	str2 = cleanUp(str2);
	compareFlag = (str1 == null ? str2 == null : str1.equals(str2));

	// compareFlag = (!hasValue(str1, GlobalValues.compareDbNullEqualsExcelBlank) ?
	// str2 == null : str1.equals(str2));
	return compareFlag;
    }

    private String cleanUp(String str1) {
	if (str1 == null || str1.trim().isEmpty()) {
	    return "";
	}
	return str1;
    }

    private boolean hasValue(Object obj, boolean ignoreBlankString) {
	if (ignoreBlankString && obj instanceof String && ((String) obj).equals("")) {
	    return false;
	}
	return obj != null;
    }

}
