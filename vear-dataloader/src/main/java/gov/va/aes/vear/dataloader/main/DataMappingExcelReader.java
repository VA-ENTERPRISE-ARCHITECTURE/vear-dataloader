package gov.va.aes.vear.dataloader.main;

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
	    InputStream inputStream = DataMappingExcelReader.class.getClassLoader().getResourceAsStream(fileName);
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
		String pickListTableIdCellValue = cellIterator.next().getStringCellValue();
		Long pickListTableId = pickListTableIdCellValue != null ? Long.valueOf(pickListTableIdCellValue) : null;
		TableAndColumnMappingInfo tableAndColumnMappingInfo = result.get(tablename);
		if (tableAndColumnMappingInfo == null) {
		    tableAndColumnMappingInfo = new TableAndColumnMappingInfo();
		    tableAndColumnMappingInfo.setTableName(tablename);
		}
		tableAndColumnMappingInfo.addColumnMapping(excelColumnaName, tableColName, tableColDataType,
			pickListTableId);
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
