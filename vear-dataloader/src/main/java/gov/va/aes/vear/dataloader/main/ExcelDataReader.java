package gov.va.aes.vear.dataloader.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.util.CellReference;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.data.PickListDao;
import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class ExcelDataReader {

    @Autowired
    PickListDao pickListDao;

    private Map<Long, Map<String, Object>> pickListDataReverseMap = new HashMap<>();

    public List<Map<String, Object>> readExcelData(final String dataFile,
	    final Collection<TableAndColumnMappingInfo> tableMappingInfo) throws FileNotFoundException, IOException {
	FileInputStream inputStream = new FileInputStream(new File(dataFile));
	Workbook workbook = new XSSFWorkbook(inputStream);
	Sheet firstSheet = workbook.getSheetAt(0);
	Iterator<Row> iterator = firstSheet.iterator();
	List<Map<String, Object>> excelRecords = new ArrayList<>();
	Map<String, DatabaseColumn> excelColNamesMap = new HashMap<>();
	for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {
	    for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
		excelColNamesMap.put(mapping.getKey(), mapping.getValue());
	    }

	}
	while (iterator.hasNext()) {
	    Row nextRow = iterator.next();
	    if (nextRow.getRowNum() == 0) {
		continue; // just skip the header row
	    }
	    Iterator<Cell> cellIterator = nextRow.cellIterator();
	    Map<String, Object> excelRecord = new HashMap<>();
	    while (cellIterator.hasNext()) {
		Cell cell = cellIterator.next();
		String headerName = getCellName(cell, firstSheet);
		// String cellValue = getCellValueAsString(cell);
		if (excelColNamesMap.containsKey(headerName)) {

		    DatabaseColumn dbColumn = excelColNamesMap.get(headerName);
		    System.out.println("Reading Cell: " + headerName + " - type: " + dbColumn.getDbColType());
		    Object valueObj = getValueAsObject(cell, dbColumn);

		    if (dbColumn.isExcelColumnDataCleanup()) {
			String cleanedUpValue = cleanupValue((String) valueObj);
			excelRecord.put(headerName, cleanedUpValue);
		    } else {
			excelRecord.put(headerName, valueObj);
		    }

		}
	    }
	    excelRecords.add(excelRecord);
	}
	workbook.close();
	inputStream.close();
	return excelRecords;
    }

    private String cleanupValue(String valueObj) {
	// TODO Auto-generated method stub
	return valueObj != null ? Jsoup.parse(valueObj).text() : null;
    }

    private Object getValueAsObject(Cell cell, DatabaseColumn dbColumn) {
	// TODO Convert String to Object based on db Data type;
	DataFormatter df = new DataFormatter();
	SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
	if (dbColumn.getDbColType().equals("TEXT")) {
	    return getCellValueAsString(cell);
	} else if (dbColumn.getDbColType().equals("NUMBER")) {
	    return new BigDecimal(df.formatCellValue(cell));
	} else if (dbColumn.getDbColType().equals("DATE")) {
	    if (cell.getCellType() == XSSFCell.CELL_TYPE_STRING) {
		String dateStr = getCellValueAsString(cell);
		try {
		    return new Timestamp(new SimpleDateFormat("dd/MM/yyyy").parse(dateStr).getTime());
		} catch (ParseException e) {
		    return null;
		}
	    } else {
		return new Timestamp(cell.getDateCellValue().getTime());
	    }

	} else if (dbColumn.getDbColType().equals("PICKLIST")) {
	    return getPickListDataKey(getCellValueAsString(cell), dbColumn.getPickListTableId());
	}
	throw new RuntimeException(" Unsupported DB Column Type in Mapping");
    }

    private Object getPickListDataKey(String cellValueAsString, Long pickListTableId) {
	// TODO Auto-generated method stub
	Map<String, Object> pickListDataReveseMap = pickListDataReverseMap.get(pickListTableId);
	if (pickListDataReveseMap == null) {
	    pickListDataReveseMap = populatePickListData(pickListTableId);
	}
	return pickListDataReveseMap.get(cellValueAsString);

    }

    private Map<String, Object> populatePickListData(Long pickListTableId) {
	// TODO Auto-generated method stub
	List<Map<String, Object>> pickListData = pickListDao.getPickListData(pickListTableId);
	Map<String, Object> reverseMap = new HashMap<>();
	for (Map<String, Object> record : pickListData) {
	    reverseMap.put((String) record.get("DESCRIPTION"), record.get("OPTION_ID"));
	}
	pickListDataReverseMap.put(pickListTableId, reverseMap);
	return reverseMap;

    }

    private String getCellName(Cell cell1, Sheet sheet) {
	CellReference cr = new CellReference(CellReference.convertNumToColString(cell1.getColumnIndex()) + "0");
	Row row = sheet.getRow(0);
	Cell cell = row.getCell(cell1.getColumnIndex());
	return cell.getStringCellValue();
    }

    private String getCellValueAsString(Cell cell) {
	String strCellValue = null;
	if (cell != null) {
	    switch (cell.getCellType()) {
	    case Cell.CELL_TYPE_STRING:
		strCellValue = cell.toString();
		break;
	    case Cell.CELL_TYPE_NUMERIC:
		if (DateUtil.isCellDateFormatted(cell)) {
		    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		    strCellValue = dateFormat.format(cell.getDateCellValue());
		} else {
		    Double value = cell.getNumericCellValue();
		    Long longValue = value.longValue();
		    strCellValue = new String(longValue.toString());
		}
		break;
	    case Cell.CELL_TYPE_BOOLEAN:
		strCellValue = new String(new Boolean(cell.getBooleanCellValue()).toString());
		break;
	    case Cell.CELL_TYPE_BLANK:
		strCellValue = "";
		break;
	    }
	}
	return strCellValue;
    }

}
