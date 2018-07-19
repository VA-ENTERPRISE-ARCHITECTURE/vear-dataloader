package gov.va.aes.vear.dataloader.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.data.VearDatabaseService;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class VearDataLoader {

    @Autowired
    public ExcelDataReader excelDataReader;
    @Autowired
    DataMappingExcelReader dataMappingExcelReader;
    @Autowired
    public VearDatabaseService vearDatabaseService;

    public void process() {
	try {
	    Collection<TableAndColumnMappingInfo> tableMappingInfo = dataMappingExcelReader
		    .readMappingFile("mapping.xlsx");
	    processDataFile(
		    "C:\\va_ea_dev\\workspace\\va_aes\\vear-riskvision-etl-processor\\src\\main\\resources\\data.xlsx",
		    tableMappingInfo);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void processDataFile(final String dataFile, final Collection<TableAndColumnMappingInfo> tableMappingInfo)
	    throws FileNotFoundException, IOException {

	try {
	    List<Map<String, Object>> excelRecords = excelDataReader.readExcelData(dataFile, tableMappingInfo);

	    cleanupExcelRecords(excelRecords, tableMappingInfo);

	    for (Map<String, Object> excelRecord : excelRecords) {

		for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {

		    Map<String, Object> dbRecord = vearDatabaseService.getDBRecord(this, excelRecord,
			    tableAndColumnMappingInfo);

		    if (dbRecord != null) { // record exists so update recrod in DB
			vearDatabaseService.processDbRecordUpdate(this, excelRecord, tableAndColumnMappingInfo);
		    } else { // No record in DB Insert New record.
			vearDatabaseService.processDbRecordInsert(this, excelRecord, tableAndColumnMappingInfo);
		    }
		}

	    }
	} catch (Exception e) {
	    throw e;

	}

    }

    private void cleanupExcelRecords(List<Map<String, Object>> excelRecords,
	    Collection<TableAndColumnMappingInfo> tableMappingInfo) {
	// TODO cleanup column data
	for (Map<String, Object> excelRecord : excelRecords) {

	    for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {

		// iterate columns where 'ExcelColumnDataCleanUp' column boolean value is 'Yes';
		// perform cleanup html tags using Jsoup
	    }

	}
    }

}
