package gov.va.aes.vear.dataloader.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;

@Component
public class CompileDbRecordsNotFound {

    private static final Logger LOG = Logger.getLogger(CompileDbRecordsNotFound.class.getName());

    public List<Map<String, Object>> compileDbRecordsForDeletion(HashMap<String, Map<String, Object>> excelRecordsMap,
	    HashMap<String, Map<String, Object>> dbRecordsMap) throws FileNotFoundException, IOException {

	try {

	    return complielist(excelRecordsMap, dbRecordsMap);

	} catch (Exception e) {
	    LOG.log(Level.SEVERE, e.getMessage());
	    return null;
	}

    }

    private List<Map<String, Object>> complielist(HashMap<String, Map<String, Object>> excelRecordsMap,
	    HashMap<String, Map<String, Object>> dbRecordsMap) {
	List<Map<String, Object>> dbRecordsNotFound = new ArrayList<>();

	for (String dbRecordKey : dbRecordsMap.keySet()) {

	    if (!excelRecordsMap.containsKey(dbRecordKey)) {
		dbRecordsNotFound.add(dbRecordsMap.get(dbRecordKey));
	    }

	}
	// iterate dbRecordsNotFound
	LOG.log(Level.FINE, "db Records not Found size: " + dbRecordsNotFound.size());
	return dbRecordsNotFound;
    }

}
