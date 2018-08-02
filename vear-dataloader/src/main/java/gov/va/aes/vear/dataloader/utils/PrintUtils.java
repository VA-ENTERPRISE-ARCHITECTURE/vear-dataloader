package gov.va.aes.vear.dataloader.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class PrintUtils {
    private static final Logger LOG = Logger.getLogger(PrintUtils.class.getName());

    private PrintUtils() {
    }

    public static void printSummaryReport() {
	LOG.log(Level.INFO, "");
	LOG.log(Level.INFO, "Project Name: " + GlobalValues.projectName);
	LOG.log(Level.INFO, "==========");
	LOG.log(Level.INFO, "Total Records read from VEAR ETL Input files = " + GlobalValues.TotalInputRecordsCount);
	LOG.log(Level.INFO, "");
	LOG.log(Level.INFO, "Input Errors");
	LOG.log(Level.INFO, "============");
	if (GlobalValues.ERROR_MESSAGES.size() == 0) {
	    LOG.log(Level.INFO, "No Errors found in VEAR ETL input");
	} else {
	    for (String errMessage : GlobalValues.ERROR_MESSAGES) {
		LOG.log(Level.INFO, errMessage);
	    }
	}
	LOG.log(Level.INFO, "");
	LOG.log(Level.INFO, "VEAR ETL Output");
	LOG.log(Level.INFO, "==========");
	LOG.log(Level.INFO, "Processed " + GlobalValues.recordsInserted.size() + " Insert records without errors");
	LOG.log(Level.INFO, "Processed " + GlobalValues.recordsUpdated.size() + " Update records without errors");
	LOG.log(Level.INFO, "ETL Records in Sync with VEAR = " + GlobalValues.recordsMatchCount);

	if (GlobalValues.recordsFailingUpdate.size() > 0 || GlobalValues.recordsFailingInsert.size() > 0) {
	    if (GlobalValues.recordsFailingUpdate.size() > 0) {
		LOG.log(Level.INFO, "Failed to Update " + GlobalValues.recordsFailingUpdate.size() + " records");
	    }
	    if (GlobalValues.recordsFailingInsert.size() > 0) {
		LOG.log(Level.INFO, "Failed to Insert " + GlobalValues.recordsFailingInsert.size() + " records");
	    }
	}
	LOG.log(Level.INFO, "VEAR DB Records Flagged for Deletion = " + GlobalValues.dbRecordsNotFound.size());

    }

}
