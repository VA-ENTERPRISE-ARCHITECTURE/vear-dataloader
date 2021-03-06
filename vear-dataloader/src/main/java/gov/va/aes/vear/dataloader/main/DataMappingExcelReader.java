package gov.va.aes.vear.dataloader.main;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
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
	    FileInputStream inputStream = new FileInputStream(new File(fileName));
	    Workbook workbook = new XSSFWorkbook(inputStream);
	    Sheet firstSheet = workbook.getSheetAt(0);
	    Iterator<Row> iterator = firstSheet.iterator();
	    DataFormatter df = new DataFormatter();
	    while (iterator.hasNext()) {
		Row nextRow = iterator.next();
		if (nextRow.getRowNum() == 0) {
		    continue; // just skip the header row
		}
		Iterator<Cell> cellIterator = nextRow.cellIterator();
		String excelColumnNumber = df.formatCellValue(cellIterator.next()).trim();
		String tablename = cellIterator.next().getStringCellValue().trim();
		String tableColName = cellIterator.next().getStringCellValue().trim();
		String tableColDataType = cellIterator.next().getStringCellValue().trim();
		Boolean isPkCol = cellIterator.next().getBooleanCellValue();
		double pickListTableIdCellValue = cellIterator.next().getNumericCellValue();
		Long pickListTableId = pickListTableIdCellValue == 0 ? null
			: Long.valueOf((long) pickListTableIdCellValue);

		Boolean isExcelColumnDataCleanup = cellIterator.next().getBooleanCellValue();
		double columnSizeCellValue = cellIterator.next().getNumericCellValue();
		int columnSize = columnSizeCellValue == 0 ? 0 : Integer.valueOf((int) columnSizeCellValue);
		String mappedTableName = null;
		String mappedKeyColumn = null;
		String mappedValueColumn = null;
		String mappedFilter = null;
		if (tableColDataType.equals("MAPPED")) {
		    mappedTableName = cellIterator.next().getStringCellValue();
		    mappedTableName = mappedTableName != null ? mappedTableName.trim() : null;
		    mappedKeyColumn = cellIterator.next().getStringCellValue();
		    mappedValueColumn = cellIterator.next().getStringCellValue();
		    mappedFilter = cellIterator.next().getStringCellValue();
		    mappedFilter = mappedFilter != null ? mappedFilter.trim() : null;
		}
		TableAndColumnMappingInfo tableAndColumnMappingInfo = result.get(tablename);
		if (tableAndColumnMappingInfo == null) {
		    tableAndColumnMappingInfo = new TableAndColumnMappingInfo();
		    tableAndColumnMappingInfo.setTableName(tablename);
		}
		tableAndColumnMappingInfo.addColumnMapping(excelColumnNumber, tableColName, tableColDataType,
			pickListTableId, isExcelColumnDataCleanup, columnSize, mappedTableName, mappedKeyColumn,
			mappedValueColumn, mappedFilter);
		if (isPkCol != null && Boolean.valueOf(isPkCol)) {
		    tableAndColumnMappingInfo.addPkColumnMapping(excelColumnNumber, tableColName, tableColDataType);
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
