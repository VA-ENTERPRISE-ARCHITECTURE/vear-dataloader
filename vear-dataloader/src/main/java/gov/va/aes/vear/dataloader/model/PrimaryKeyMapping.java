package gov.va.aes.vear.dataloader.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;

@Component
public class PrimaryKeyMapping {

    private static final Logger LOG = Logger.getLogger(PrimaryKeyMapping.class.getName());

    public String getPKValueAsString(Map<String, Object> excelRecord,
	    Collection<TableAndColumnMappingInfo> tableMappingInfo) {
	List<Object> pkValuesList = new ArrayList<>();
	for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {

	    for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings()
		    .entrySet()) {

		pkValuesList.add(excelRecord.get(mapping.getKey()));
	    }

	}

	return pkValuesList.toString();
    }

    public String getDBPKValueAsString(Map<String, Object> dbRecord,
	    Collection<TableAndColumnMappingInfo> tableMappingInfo) {
	List<Object> pkValuesList = new ArrayList<>();
	for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {

	    for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getPkColumnMappings()
		    .entrySet()) {

		pkValuesList.add(dbRecord.get(mapping.getValue().getDbColName()));
	    }

	}

	return pkValuesList.toString();
    }

}
