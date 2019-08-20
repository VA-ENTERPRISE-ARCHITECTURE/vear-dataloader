package gov.va.aes.vear.dataloader.utils;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;

public final class PrintUtils {

    @Autowired

    private static final Logger LOG = Logger.getLogger(PrintUtils.class.getName());

    private PrintUtils() {
    }

    public static void printSummaryReport(String projectName, Boolean avoidVearDbInserts, int TotalInputRecordsCount,
	    int recordsUpdateCount, int recordsInsertCount, int recordsMatchCount,
	    List<Map<String, Object>> recordsUpdated, List<Map<String, Object>> diffRecords,
	    List<Map<String, Object>> recordsInserted, List<Map<String, Object>> recordsFailingUpdate,
	    List<Map<String, Object>> recordsFailingInsert, List<Map<String, Object>> dbRecordsNotFound,
	    List<Map<String, Object>> recordsInvalid) {
	LOG.log(Level.INFO, "");
	LOG.log(Level.INFO, "Project Name: " + projectName);
	LOG.log(Level.INFO, "==========");
	LOG.log(Level.INFO, "Total Records read from VEAR ETL Input files = " + TotalInputRecordsCount);
	LOG.log(Level.INFO, "");
	// LOG.log(Level.INFO, "Input Errors");
	// LOG.log(Level.INFO, "============");
	// if (ERROR_MESSAGES.size() == 0) {
	// LOG.log(Level.INFO, "No Errors found in VEAR ETL input");
	// } else {
	// for (String errMessage : ERROR_MESSAGES) {
	// LOG.log(Level.INFO, errMessage);
	// }
	// }
	LOG.log(Level.INFO, "");
	LOG.log(Level.INFO, "VEAR ETL Output");
	LOG.log(Level.INFO, "==========");
	if (!avoidVearDbInserts) {
	    LOG.log(Level.INFO, "Processed " + recordsInserted.size() + " Insert records without errors");
	} else {
	    LOG.log(Level.INFO, "Inserts are NOT processed");
	}
	LOG.log(Level.INFO, "Processed " + recordsUpdated.size() + " Update records without errors");
	LOG.log(Level.INFO, "ETL Records in Sync with VEAR = " + recordsMatchCount);

	if (recordsFailingUpdate.size() > 0 || recordsFailingInsert.size() > 0 || recordsInvalid.size() > 0) {
	    if (recordsFailingUpdate.size() > 0) {
		LOG.log(Level.INFO, "Failed to Update " + recordsFailingUpdate.size() + " records");
	    }
	    if (recordsFailingInsert.size() > 0) {
		LOG.log(Level.INFO, "Failed to Insert " + recordsFailingInsert.size() + " records");
	    }
	    if (recordsInvalid.size() > 0) {
		LOG.log(Level.INFO, "Records that are invalid in Source File " + recordsInvalid.size() + " records");
	    }
	}
	LOG.log(Level.INFO, "VEAR DB Records Flagged for Deletion = " + dbRecordsNotFound.size());
	LOG.log(Level.INFO, "");
	LOG.log(Level.INFO, "");
    }

}
