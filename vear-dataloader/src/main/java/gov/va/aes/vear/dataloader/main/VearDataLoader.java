package gov.va.aes.vear.dataloader.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import gov.va.aes.vear.dataloader.configuration.GlobalValues;
import gov.va.aes.vear.dataloader.data.VearDatabaseService;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

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

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void process(String directroryPath, String filenamePattern) {
	try {
	    Collection<TableAndColumnMappingInfo> tableMappingInfo = dataMappingExcelReader
		    .readMappingFile(directroryPath + "\\Vear_Dataloader_Mapping.xlsx");
	    String[] fileNamesToBeProcessed = collectFieNames(directroryPath, filenamePattern);
	    for (String filename : fileNamesToBeProcessed) {
		LOG.log(Level.INFO, "Processing File : {0}", filename);
		processDataFile(filename, tableMappingInfo);
	    }
	} catch (Exception e) {
	    LOG.log(Level.SEVERE, "VearDataLoader Process failed with Exception: " + e.getMessage());
	    e.printStackTrace();
	} finally {
	    throw new RuntimeException("Throwing Exception for Rollingback.");
	}
    }

    private String[] collectFieNames(String directroryPath, String inputFilePattern) throws FileNotFoundException {
	List<String> files = new ArrayList<>();
	try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(directroryPath),
		inputFilePattern + ".{xlsx,xls}")) {
	    for (Path p : stream) {
		files.add(p.toString());
	    }
	} catch (IOException ex) {
	    LOG.log(Level.SEVERE, ex.getMessage());
	}
	if (files.size() == 0) {
	    throw new FileNotFoundException("ERROR: Sorry ... I could not find the CMDB Input file in the folder "
		    + GlobalValues.FILE_PATH
		    + " . I was told to look for a excel file with a name starting 'ECMDB_VASI_ETL'. Aborting ETL process ....");
	} else {
	    return files.toArray(new String[files.size()]);

	}

    }

    public void processDataFile(final String dataFile, final Collection<TableAndColumnMappingInfo> tableMappingInfo)
	    throws FileNotFoundException, IOException {

	try {

	    List<Map<String, Object>> excelRecords = excelDataReader.readExcelData(dataFile, tableMappingInfo);
	    List<Map<String, Object>> recordsFilingUpdate = new ArrayList<>();
	    List<Map<String, Object>> recordsFilingInsert = new ArrayList<>();
	    for (Map<String, Object> excelRecord : excelRecords) {

		for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {

		    Map<String, Object> dbRecord = vearDatabaseService.getDBRecord(this, excelRecord,
			    tableAndColumnMappingInfo);

		    if (dbRecord != null) {
			if (compareRecords.checkAttributesChanged(excelRecord, dbRecord, tableAndColumnMappingInfo)) {
			    LOG.log(Level.FINE, "Changes found  Excel Record: " + excelRecord.toString() + " DB Record:"
				    + dbRecord.toString());

			    try {

				vearDatabaseService.processDbRecordUpdate(this, excelRecord, tableAndColumnMappingInfo);
			    } catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to update record", e);
				recordsFilingUpdate.add(excelRecord);
			    }
			} else {
			    LOG.log(Level.FINE, "Skipping Record as no changes found: " + excelRecord.toString());

			}
		    } else { // No record in DB Insert New record.
			try {
			    vearDatabaseService.processDbRecordInsert(this, excelRecord, tableAndColumnMappingInfo);
			} catch (Exception e) {
			    LOG.log(Level.SEVERE, "Failed to insert record", e);
			    recordsFilingInsert.add(excelRecord);
			}
		    }
		}

	    }
	    if (recordsFilingUpdate.size() > 0 || recordsFilingInsert.size() > 0) {
		if (recordsFilingUpdate.size() > 0) {
		    LOG.log(Level.INFO, "Failed to Update {0} records", recordsFilingUpdate.size());
		}
		if (recordsFilingInsert.size() > 0) {
		    LOG.log(Level.INFO, "Failed to Insert {0} records", recordsFilingInsert.size());
		}
	    } else {
		LOG.log(Level.INFO, "All records processed without errors");
	    }

	} catch (ValidateException e) {
	    // TODO Auto-generated catch block
	    LOG.log(Level.SEVERE, e.getMessage());
	    LOG.log(Level.WARNING, "Validation failed File will be skipped");
	}

    }

}
