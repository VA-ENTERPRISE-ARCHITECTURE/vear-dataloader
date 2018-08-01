package gov.va.aes.vear.dataloader.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import gov.va.aes.vear.dataloader.data.VearDatabaseService;
import gov.va.aes.vear.dataloader.model.PrimaryKeyMapping;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;
import gov.va.aes.vear.dataloader.utils.GlobalValues;
import gov.va.aes.vear.dataloader.utils.PrintUtils;

@Component
public class VearDataLoader {

    private static final Logger LOG = Logger.getLogger(VearDataLoader.class.getName());

    @Autowired
    public ExcelDataReader excelDataReader;
    @Autowired
    DataMappingExcelReader dataMappingExcelReader;
    @Autowired
    VearDatabaseService vearDatabaseService;
    @Autowired
    CompareRecords compareRecords;
    @Autowired
    PrimaryKeyMapping primaryKeyMapping;
    @Autowired
    CompileDbRecordsNotFound compileDbRecordsNotFound;

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void process() {
	// Make Sure proper directory structure in place.
	if (Files.exists(Paths.get(GlobalValues.INPUT_FILE_PATH))) {
	    try {

		Collection<TableAndColumnMappingInfo> tableMappingInfo = dataMappingExcelReader
			.readMappingFile(GlobalValues.MAPPING_FILE_PATH);
		String[] fileNamesToBeProcessed = collectFileNames();

		processDataFiles(fileNamesToBeProcessed, tableMappingInfo);

	    } catch (Exception e) {
		LOG.log(Level.SEVERE, "VearDataLoader Process failed with Exception: " + e.getMessage());
		e.printStackTrace();
	    } finally {
		throw new RuntimeException("Throwing Exception for Rollingback.");
	    }
	} else {
	    System.out.println(
		    "ERROR: Sorry... I was told to find the ETL Input files a folder named with Input_Files. I could not find the directory "
			    + GlobalValues.INPUT_FILE_PATH);
	    System.out.println("Aborting the VEAR ETL process .....");
	}
    }

    private String[] collectFileNames() throws FileNotFoundException {
	List<String> files = new ArrayList<>();
	try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(GlobalValues.INPUT_FILE_PATH),
		"*.{xlsx,xls}")) {
	    for (Path p : stream) {
		files.add(p.toString());
	    }
	} catch (IOException ex) {
	    LOG.log(Level.SEVERE, ex.getMessage());
	}
	if (files.size() == 0) {
	    throw new FileNotFoundException("ERROR: Sorry ... I could not find the VEAR ETL Input file in the folder "
		    + GlobalValues.INPUT_FILE_PATH + " . I was told to look for a excel file "
		    + ". Aborting VEAR ETL process ....");
	} else {
	    return files.toArray(new String[files.size()]);

	}

    }

    public void processDataFiles(final String[] dataFiles, final Collection<TableAndColumnMappingInfo> tableMappingInfo)
	    throws FileNotFoundException, IOException {

	try {

	    List<Map<String, Object>> excelRecords = excelDataReader.readExcelData(dataFiles, tableMappingInfo);
	    // creating excelRecordsMap from the excelRecords list
	    HashMap<String, Map<String, Object>> excelRecordsMap = new HashMap<String, Map<String, Object>>();
	    for (Map<String, Object> excelRecord : excelRecords) {

		String pkValueStr = primaryKeyMapping.getPKValueAsString(excelRecord, tableMappingInfo);
		excelRecordsMap.put(pkValueStr, excelRecord);

	    }
	    GlobalValues.TotalInputRecordsCount = excelRecords.size();

	    for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) { // Processing excel records
											   // for each table
		List<Map<String, Object>> dbRecords = vearDatabaseService.getAllDBRecords(tableAndColumnMappingInfo);
		// creating dbRecordsMap from the dbRecords list
		HashMap<String, Map<String, Object>> dbRecordsMap = new HashMap<String, Map<String, Object>>();
		for (Map<String, Object> dbRecord : dbRecords) {
		    String pkValueStr = primaryKeyMapping.getDBPKValueAsString(dbRecord, tableMappingInfo);
		    dbRecordsMap.put(pkValueStr, dbRecord);
		}

		for (Map<String, Object> excelRecord : excelRecords) {

		    Map<String, Object> dbRecord = dbRecordsMap
			    .get(primaryKeyMapping.getPKValueAsString(excelRecord, tableMappingInfo));

		    if (dbRecord != null) {
			if (compareRecords.checkAttributesChanged(excelRecord, dbRecord, tableAndColumnMappingInfo)) {
			    LOG.log(Level.FINE, "Changes found  Excel Record: " + excelRecord.toString() + " DB Record:"
				    + dbRecord.toString());

			    try {

				vearDatabaseService.processDbRecordUpdate(this, excelRecord, tableAndColumnMappingInfo);
				GlobalValues.recordsUpdated.add(excelRecord);
				GlobalValues.recordsUpdateCount++;
			    } catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to update record", e);
				GlobalValues.recordsFailingUpdate.add(excelRecord);
			    }
			} else {
			    LOG.log(Level.FINE, "Skipping Record as no changes found: " + excelRecord.toString());

			}
		    } else { // No record in DB Insert New record.
			try {
			    vearDatabaseService.processDbRecordInsert(this, excelRecord, tableAndColumnMappingInfo);
			    GlobalValues.recordsInserted.add(excelRecord);
			    GlobalValues.recordsInsertCount++;
			} catch (Exception e) {
			    LOG.log(Level.SEVERE, "Failed to insert record", e);
			    GlobalValues.recordsFailingInsert.add(excelRecord);
			}
		    }

		}

		// compile VEAR Records Not Found in Input ETL files
		GlobalValues.dbRecordsNotFound = compileDbRecordsNotFound.compileDbRecordsForDeletion(excelRecordsMap,
			dbRecordsMap);
		PrintUtils.printSummaryReport(tableAndColumnMappingInfo);
	    }
	} catch (Exception e) {
	    LOG.log(Level.SEVERE, e.getMessage());
	}

    }

}
