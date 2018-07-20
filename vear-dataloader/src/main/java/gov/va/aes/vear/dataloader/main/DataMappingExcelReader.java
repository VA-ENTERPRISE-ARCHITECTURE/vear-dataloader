package gov.va.aes.vear.dataloader.main;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class DataMappingExcelReader {

    public Collection<TableAndColumnMappingInfo> readMappingFile(final String fileName) throws Exception {
	Map<String, TableAndColumnMappingInfo> result = new HashMap<>();
	try {
	    InputStream inputStream = new FileInputStream(fileName);
	    Workbook workbook = new XSSFWorkbook(inputStream);
	    Sheet firstSheet = workbook.getSheetAt(0);
	    Iterator<Row> iterator = firstSheet.iterator();
	    while (iterator.hasNext()) {
		Row nextRow = iterator.next();
		if (nextRow.getRowNum() == 0) {
		    continue; // just skip the header row
		}
		Iterator<Cell> cellIterator = nextRow.cellIterator();
		String excelColumnaName = cellIterator.next().getStringCellValue();
		String tablename = cellIterator.next().getStringCellValue();
		String tableColName = cellIterator.next().getStringCellValue();
		String tableColDataType = cellIterator.next().getStringCellValue();
		Boolean isPkCol = cellIterator.next().getBooleanCellValue();
		double pickListTableIdCellValue = cellIterator.next().getNumericCellValue();
		Long pickListTableId = pickListTableIdCellValue == 0 ? null
			: Long.valueOf((long) pickListTableIdCellValue);

		Boolean isExcelColumnDataCleanup = cellIterator.next().getBooleanCellValue();
		TableAndColumnMappingInfo tableAndColumnMappingInfo = result.get(tablename);
		if (tableAndColumnMappingInfo == null) {
		    tableAndColumnMappingInfo = new TableAndColumnMappingInfo();
		    tableAndColumnMappingInfo.setTableName(tablename);
		}
		tableAndColumnMappingInfo.addColumnMapping(excelColumnaName, tableColName, tableColDataType,
			pickListTableId, isExcelColumnDataCleanup);
		if (isPkCol != null && Boolean.valueOf(isPkCol)) {
		    tableAndColumnMappingInfo.addPkColumnMapping(excelColumnaName, tableColName, tableColDataType);
		}
		result.put(tablename, tableAndColumnMappingInfo);
	    }

	    workbook.close();
	    inputStream.close();
	} catch (Exception e) {
	    throw e;
	}
	return result.values();
    }
}
