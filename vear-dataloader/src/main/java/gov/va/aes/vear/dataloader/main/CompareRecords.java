package gov.va.aes.vear.dataloader.main;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class CompareRecords {

    private static final Logger LOG = Logger.getLogger(CompareRecords.class.getName());

    public boolean checkAttributesChanged(Map<String, Object> excelRecord, Map<String, Object> dbRecord,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo) {
	boolean checkAttributesChanged = false;
	for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
	    Object columnValue = excelRecord.get(mapping.getKey());
	    if (mapping.getValue().getColumnSize() > 0) {
		if (columnValue != null && columnValue instanceof String
			&& ((String) columnValue).getBytes().length > mapping.getValue().getColumnSize()) {
		    columnValue = ((String) columnValue).substring(0, mapping.getValue().getColumnSize() - 5);
		}
	    }

	    checkAttributesChanged = !compareObject(columnValue, dbRecord.get(mapping.getValue().getDbColName()));
	    if (checkAttributesChanged) {
		LOG.log(Level.INFO, "Compare NOT Matching data, Excel record value: " + columnValue
			+ " - Db Record value: " + dbRecord.get(mapping.getValue().getDbColName()));
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
	compareFlag = (obj1 == null ? obj2 == null
		: (obj1.equals(obj2) || (obj2 != null && compareObject(obj1.toString(), obj2.toString()))));
	return compareFlag;
    }

    private boolean compare(String str1, String str2) {
	boolean compareFlag = false;
	str1 = cleanUp(str1);
	str2 = cleanUp(str2);
	compareFlag = (str1 == null ? str2 == null : str1.equals(str2));
	return compareFlag;
    }

    private String cleanUp(String str1) {
	if (str1 == null || str1.trim().isEmpty()) {
	    return "";
	}
	return str1;
    }

}
