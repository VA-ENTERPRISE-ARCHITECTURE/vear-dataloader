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
		    .readMappingFile("C:\\Development\\docs\\projects\\RV\\RV_mapping.xlsx");
	    processDataFile(
		    "C:\\Development\\docs\\projects\\RV\\EO System Inventory for VASI Reporting - May 2018.xlsx",
		    tableMappingInfo);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void processDataFile(final String dataFile, final Collection<TableAndColumnMappingInfo> tableMappingInfo)
	    throws FileNotFoundException, IOException {

	List<Map<String, Object>> excelRecords = excelDataReader.readExcelData(dataFile, tableMappingInfo);

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

    }

}
