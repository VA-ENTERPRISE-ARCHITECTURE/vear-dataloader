package gov.va.aes.vear.dataloader.main;

import java.io.File;
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

import gov.va.aes.vear.dataloader.configuration.ProjectConfig;
import gov.va.aes.vear.dataloader.data.VearDatabaseService;
import gov.va.aes.vear.dataloader.model.PrimaryKeyMapping;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;
import gov.va.aes.vear.dataloader.utils.PrintUtils;
import gov.va.aes.vear.dataloader.utils.RecordWriter;

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
    @Autowired
    RecordWriter recordWriter;
    @Autowired
    Map<String, ProjectConfig> projectConfigMap;

    @Autowired
    String dirsToBeProcessed;

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void process() {
	try {
	    if (dirsToBeProcessed != null && !"".equals(dirsToBeProcessed)) {
		String[] dirs = dirsToBeProcessed.split(",");

		for (String dirToBeProcessed : dirs) {

		    if (projectConfigMap.get(dirToBeProcessed).compareDbNullEqualsExcelBlank != null) {
			compareRecords.compareDbNullEqualsExcelBlank = projectConfigMap
				.get(dirToBeProcessed).compareDbNullEqualsExcelBlank;
		    } else {
			compareRecords.compareDbNullEqualsExcelBlank = projectConfigMap
				.get(".").compareDbNullEqualsExcelBlank;
		    }
		    if (projectConfigMap.get(dirToBeProcessed).inputDateFormat != null) {
			excelDataReader.inputDateFormat = projectConfigMap.get(dirToBeProcessed).inputDateFormat;
		    } else {
			excelDataReader.inputDateFormat = projectConfigMap.get(".").inputDateFormat;
		    }
		    excelDataReader.reset();
		    LOG.log(Level.INFO, "Processing ETL for Directory: " + dirToBeProcessed);
		    procesDirectory(dirToBeProcessed);
		}

	    } else {

		String dirToBeProcessed = ".";
		compareRecords.compareDbNullEqualsExcelBlank = projectConfigMap.get(".").compareDbNullEqualsExcelBlank;
		excelDataReader.inputDateFormat = projectConfigMap.get(".").inputDateFormat;
		procesDirectory(dirToBeProcessed);
	    }
	} finally {
	    // throw new RuntimeException("Throwing Exception for Rollingback.");
	}
    }

    public void procesDirectory(String dirToBeProcessed) {
	String mappingFilePath = dirToBeProcessed + File.separator + "Vear_ETL_Mapping.xlsx";
	String inputFilePath = dirToBeProcessed + File.separator + "Input_Files" + File.separator;

	// Make Sure proper directory structure in place.
	if (Files.exists(Paths.get(inputFilePath))) {
	    try {

		Collection<TableAndColumnMappingInfo> tableMappingInfo = extractMappingInfo(mappingFilePath);
		String[] fileNamesToBeProcessed = collectFileNames(inputFilePath);
		String projectName = projectConfigMap.get(".").projectName;
		if (!dirToBeProcessed.equals(".")) {
		    projectName = projectName + "-" + dirToBeProcessed;
		}
		processDataFiles(fileNamesToBeProcessed, tableMappingInfo, projectName,
			projectConfigMap.get(dirToBeProcessed).avoidVearDbInserts);

	    } catch (Throwable e) {
		LOG.log(Level.SEVERE, "VearDataLoader Process failed with Exception: ", e);
		e.printStackTrace();
	    } finally {
		// throw new RuntimeException("Throwing Exception for Rollingback.");
	    }
	} else {
	    LOG.log(Level.SEVERE,
		    "ERROR: Sorry... I was told to find the ETL Input files a folder named with Input_Files. I could not find the directory ",
		    inputFilePath);
	    LOG.log(Level.SEVERE, "Aborting the VEAR ETL process .....");
	}
    }

    public Collection<TableAndColumnMappingInfo> extractMappingInfo(String mappingFilePath) throws Exception {
	Collection<TableAndColumnMappingInfo> tableMappingInfo = dataMappingExcelReader
		.readMappingFile(mappingFilePath);
	return tableMappingInfo;
    }

    private String[] collectFileNames(String inputFilePath) throws FileNotFoundException {
	List<String> files = new ArrayList<>();
	try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(inputFilePath),
		"*.{xlsx,xls,DAT,csv}")) {
	    for (Path p : stream) {
		files.add(p.toString());
	    }
	} catch (IOException ex) {
	    LOG.log(Level.SEVERE, "Collect File Names Failed with Exception ", ex);
	}
	if (files.size() == 0) {
	    throw new FileNotFoundException("ERROR: Sorry ... I could not find the VEAR ETL Input file in the folder "
		    + inputFilePath + " . I was told to look for a excel file " + ". Aborting VEAR ETL process ....");
	} else {
	    return files.toArray(new String[files.size()]);

	}

    }

    public void processDataFiles(final String[] dataFiles, final Collection<TableAndColumnMappingInfo> tableMappingInfo,
	    String projectName, Boolean avoidVearDbInserts) throws FileNotFoundException, IOException {
	int TotalInputRecordsCount = 0;
	int recordsUpdateCount = 0;
	int recordsInsertCount = 0;
	int recordsMatchCount = 0;
	List<Map<String, Object>> recordsUpdated = new ArrayList<>();
	List<Map<String, Object>> diffRecords = new ArrayList<>();
	List<Map<String, Object>> recordsInserted = new ArrayList<>();
	List<Map<String, Object>> recordsFailingUpdate = new ArrayList<>();
	List<Map<String, Object>> recordsFailingInsert = new ArrayList<>();
	List<Map<String, Object>> dbRecordsNotFound = new ArrayList<>();
	long insertBatchSize = 1000;
	long updateBatchSize = 1000;
	try {

	    List<Map<String, Object>> excelRecords = excelDataReader.readDataFiles(dataFiles, tableMappingInfo);
	    TotalInputRecordsCount = excelRecords.size();
	    LOG.log(Level.INFO, "Finished reading excel records. Total : " + TotalInputRecordsCount + " records");
	    // creating excelRecordsMap from the excelRecords list
	    HashMap<String, Map<String, Object>> excelRecordsMap = new HashMap<String, Map<String, Object>>();
	    for (Map<String, Object> excelRecord : excelRecords) {

		String pkValueStr = primaryKeyMapping.getPKValueAsString(excelRecord, tableMappingInfo);
		excelRecordsMap.put(pkValueStr, excelRecord);

	    }

	    for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) { // Processing excel records
											   // for each table
		LOG.log(Level.INFO, "Begin reading all db records.");
		List<Map<String, Object>> dbRecords = vearDatabaseService.getAllDBRecords(tableAndColumnMappingInfo);
		LOG.log(Level.INFO, "Finished reading all db records. Total : " + dbRecords.size() + " records");
		// creating dbRecordsMap from the dbRecords list
		HashMap<String, Map<String, Object>> dbRecordsMap = new HashMap<String, Map<String, Object>>();
		for (Map<String, Object> dbRecord : dbRecords) {
		    String pkValueStr = primaryKeyMapping.getDBPKValueAsString(dbRecord, tableMappingInfo);
		    dbRecordsMap.put(pkValueStr, dbRecord);
		}

		String updateSQL = vearDatabaseService.getDbRecordUpdateSQL(tableAndColumnMappingInfo);
		List<List<Object>> updateParamsList = new ArrayList<>();
		String insertSQL = vearDatabaseService.getDbRecordInsertSQL(tableAndColumnMappingInfo);
		List<List<Object>> insertParamsList = new ArrayList<>();
		List<Map<String, Object>> recordsInsertedInBatch = new ArrayList<>();
		List<Map<String, Object>> recordsUpdatedInBatch = new ArrayList<>();

		for (Map<String, Object> excelRecord : excelRecords) {

		    Map<String, Object> dbRecord = dbRecordsMap
			    .get(primaryKeyMapping.getPKValueAsString(excelRecord, tableMappingInfo));

		    if (dbRecord != null) {
			if (compareRecords.checkAttributesChanged(excelRecord, dbRecord, tableAndColumnMappingInfo)) {
			    LOG.log(Level.FINE, "Changes found  Excel Record: " + excelRecord.toString() + " DB Record:"
				    + dbRecord.toString());

			    List<Object> params = vearDatabaseService.getDbRecordUpdateParams(excelRecord,
				    tableAndColumnMappingInfo);
			    recordsUpdatedInBatch.add(dbRecord);
			    diffRecords.add(excelRecord);
			    diffRecords.add(dbRecord);

			    recordsUpdateCount++;
			    updateParamsList.add(params);

			    if (updateParamsList.size() >= updateBatchSize) {
				LOG.log(Level.INFO,
					"Starting batch Update of batch : " + (recordsUpdateCount / updateBatchSize));
				try {
				    vearDatabaseService.insertOrUpdateBatch(updateSQL, updateParamsList);
				    recordsUpdated.addAll(recordsUpdatedInBatch);
				} catch (Exception e) {
				    LOG.log(Level.SEVERE, "Failed to update records for batch: "
					    + (recordsUpdateCount / updateBatchSize), e);
				    recordsFailingUpdate.addAll(recordsUpdatedInBatch);
				} finally {
				    updateParamsList.clear();
				    recordsUpdatedInBatch.clear();
				}

			    }
			    // vearDatabaseService.processDbRecordUpdate( excelRecord,
			    // tableAndColumnMappingInfo);

			} else {
			    LOG.log(Level.FINE, "Skipping Record as no changes found: " + excelRecord.toString());
			    recordsMatchCount++;

			}
		    } else { // No record in DB Insert New record.
			recordsInsertedInBatch.add(excelRecord);

			if (!avoidVearDbInserts) {
			    recordsInsertCount++;
			    List<Object> params = vearDatabaseService.getDbRecordInsertParams(excelRecord,
				    tableAndColumnMappingInfo);
			    insertParamsList.add(params);

			    if (insertParamsList.size() >= insertBatchSize) {
				LOG.log(Level.INFO,
					"Starting batch Insert of batch : " + (recordsInsertCount / insertBatchSize));
				try {
				    vearDatabaseService.insertOrUpdateBatch(insertSQL, insertParamsList);
				    recordsInserted.addAll(recordsInsertedInBatch);
				} catch (Exception e) {
				    LOG.log(Level.SEVERE, "Failed to insert records for batch: "
					    + (recordsInsertCount / insertBatchSize), e);
				    recordsFailingInsert.addAll(recordsInsertedInBatch);
				} finally {
				    insertParamsList.clear();
				    recordsInsertedInBatch.clear();
				}
			    }
			    // vearDatabaseService.processDbRecordInsert(excelRecord,
			    // tableAndColumnMappingInfo);

			}

		    }

		}

		if (updateParamsList.size() > 0) {

		    LOG.log(Level.INFO, "Starting Last batch Update of Records : " + updateParamsList.size());
		    try {
			vearDatabaseService.insertOrUpdateBatch(updateSQL, updateParamsList);
			recordsUpdated.addAll(recordsUpdatedInBatch);
		    } catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to update records for last batch: ", e);
			recordsFailingUpdate.addAll(recordsUpdatedInBatch);
		    } finally {
			updateParamsList.clear();
			recordsUpdatedInBatch.clear();
		    }

		}
		if (insertParamsList.size() > 0) {

		    LOG.log(Level.INFO, "Starting Last batch Insert of Records : " + insertParamsList.size());
		    try {
			vearDatabaseService.insertOrUpdateBatch(insertSQL, insertParamsList);
			recordsInserted.addAll(recordsInsertedInBatch);
		    } catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to insert records for last batch: ", e);
			recordsFailingInsert.addAll(recordsInsertedInBatch);
		    } finally {
			insertParamsList.clear();
			recordsInsertedInBatch.clear();
		    }
		}
		recordWriter.writeCSVOutput(diffRecords, tableAndColumnMappingInfo, projectName + "/ETL_UPDATES_",
			"INPUT_Record", "VEAR_Record", false, true);
		recordWriter.writeCSVOutput(recordsInserted, tableAndColumnMappingInfo, projectName + "/ETL_INSERTS_",
			null, null, false, false);

		// compile VEAR Records Not Found in Input ETL files
		dbRecordsNotFound = compileDbRecordsNotFound.compileDbRecordsForDeletion(excelRecordsMap, dbRecordsMap);
		recordWriter.writeCSVOutput(dbRecordsNotFound, tableAndColumnMappingInfo,
			projectName + "/RECORDS_FLAGGED_FOR_DELETION_", null, null, true, true);
		PrintUtils.printSummaryReport(projectName, avoidVearDbInserts, TotalInputRecordsCount,
			recordsUpdateCount, recordsInsertCount, recordsMatchCount, recordsUpdated, diffRecords,
			recordsInserted, recordsFailingUpdate, recordsFailingInsert, dbRecordsNotFound);
	    }
	} catch (Exception e) {
	    LOG.log(Level.SEVERE, "VEAR DATA LOADER Failed for " + projectName, e);
	}

    }

}
