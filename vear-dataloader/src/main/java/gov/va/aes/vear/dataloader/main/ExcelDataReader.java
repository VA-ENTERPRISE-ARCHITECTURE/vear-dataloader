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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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

import gov.va.aes.vear.dataloader.data.MappedSqlDao;
import gov.va.aes.vear.dataloader.data.PickListDao;
import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class ExcelDataReader {

    private static final Logger LOG = Logger.getLogger(ExcelDataReader.class.getName());

    @Autowired
    PickListDao pickListDao;
    @Autowired
    MappedSqlDao mappedSqlDao;

    private Map<Long, Map<String, Object>> pickListDataReverseMap = new HashMap<>();
    private Map<String, Map<String, Object>> mappedSqlDataReverseMap = new HashMap<>();

    public List<Map<String, Object>> readExcelData(final String[] dataFiles,
	    final Collection<TableAndColumnMappingInfo> tableMappingInfo)
	    throws FileNotFoundException, IOException, ValidateException {
	List<Map<String, Object>> excelRecords = new ArrayList<>();
	for (String dataFile : dataFiles) {
	    LOG.log(Level.INFO, "Reading Excel File : {0}", dataFile);
	    FileInputStream inputStream = new FileInputStream(new File(dataFile));
	    Workbook workbook = new XSSFWorkbook(inputStream);
	    Sheet firstSheet = workbook.getSheetAt(0);

	    Iterator<Row> iterator = firstSheet.iterator();

	    Map<String, DatabaseColumn> excelColNamesMap = new HashMap<>();
	    for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {
		for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings()
			.entrySet()) {
		    excelColNamesMap.put(mapping.getKey(), mapping.getValue());
		}
	    }

	    while (iterator.hasNext()) {
		Row nextRow = iterator.next();
		if (nextRow.getRowNum() == 0) {
		    validateDataFileColumnsExists(excelColNamesMap, nextRow);
		    continue; // just skip the header row
		}
		Iterator<Cell> cellIterator = nextRow.cellIterator();
		Map<String, Object> excelRecord = new HashMap<>();
		while (cellIterator.hasNext()) {
		    Cell cell = cellIterator.next();
		    String headerName = getCellName(cell, firstSheet).trim();
		    // String cellValue = getCellValueAsString(cell);
		    if (excelColNamesMap.containsKey(headerName)) {

			DatabaseColumn dbColumn = excelColNamesMap.get(headerName);
			LOG.log(Level.FINE, "Reading Cell: " + headerName + " - type: " + dbColumn.getDbColType());
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
	}
	return excelRecords;
    }

    // collect headerNames to verify all columns are present
    private void validateDataFileColumnsExists(Map<String, DatabaseColumn> excelColNamesMap, Row nextRow)
	    throws ValidateException {
	List<String> headerNamesList = new ArrayList<>();
	Iterator<Cell> cellIterator = nextRow.cellIterator();
	while (cellIterator.hasNext()) {
	    Cell cell = cellIterator.next();

	    headerNamesList.add(String.valueOf(cell.getColumnIndex()));
	}
	Set<String> headerNamesSet = excelColNamesMap.keySet();

	for (String mappingHeaderName : headerNamesSet) {
	    if (!headerNamesList.contains(mappingHeaderName)) {
		throw new ValidateException("Invalid Input Data File. Header Name \"" + mappingHeaderName
			+ "\" does not exist in Excel file.");

	    }
	}

    }

    private String cleanupValue(String valueObj) {
	return valueObj != null ? Jsoup.parse(valueObj).text() : null;
    }

    /**
     * 
     * @param cell
     *            : cell value from input data file
     * @param dbColumn
     *            : DatabaseColumn object
     * @return : returns an object based on the cell value and column type defined
     *         in mapping file
     */
    private Object getValueAsObject(Cell cell, DatabaseColumn dbColumn) {
	DataFormatter df = new DataFormatter();
	try {
	    if (dbColumn.getDbColType().equals("TEXT")) {
		return getCellValueAsString(cell);
	    } else if (dbColumn.getDbColType().equals("NUMBER")) {
		return new BigDecimal(df.formatCellValue(cell));
	    } else if (dbColumn.getDbColType().equals("DATE")) {
		if (cell.getCellType() == XSSFCell.CELL_TYPE_STRING) {
		    String dateStr = getCellValueAsString(cell);
		    try {
			return new Timestamp(new SimpleDateFormat("MM/dd/yyyy").parse(dateStr).getTime());
		    } catch (ParseException e) {
			return null;
		    }
		} else {
		    return new Timestamp(cell.getDateCellValue().getTime());
		}

	    } else if (dbColumn.getDbColType().equals("PICKLIST")) {
		return getPickListDataKey(getCellValueAsString(cell), dbColumn.getPickListTableId());
	    } else if (dbColumn.getDbColType().equals("BOOLEAN")) {
		String boolStr = getCellValueAsString(cell);
		if (boolStr.equals("TRUE")) {
		    return new Integer(1);
		} else {
		    return new Integer(0);
		}
	    } else if (dbColumn.getDbColType().equals("MAPPED")) {
		return getSqlMappedDataKey(getCellValueAsString(cell), dbColumn.getMappedSql());
	    }
	} catch (NullPointerException e) {
	    return null;
	}
	throw new RuntimeException(" Unsupported DB Column Type in Mapping");
    }

    private Object getSqlMappedDataKey(String cellValueAsString, String mappedSql) {
	// TODO Auto-generated method stub mappedSqlDao
	Map<String, Object> mappedSqlDataRevsMap = mappedSqlDataReverseMap.get(mappedSql);
	if (mappedSqlDataRevsMap == null) {
	    mappedSqlDataRevsMap = populateMappedSqlData(mappedSql);
	}
	return mappedSqlDataRevsMap.get(cellValueAsString);
    }

    private Map<String, Object> populateMappedSqlData(String mappedSql) {
	List<Map<String, Object>> mappedSqlData = mappedSqlDao.getMappedSqlData(mappedSql);
	Map<String, Object> reverseMap = new HashMap<>();
	String keyColumnName = null, valueColumnName = null;

	for (Map<String, Object> record : mappedSqlData) {
	    if (keyColumnName == null || valueColumnName == null) {
		for (String recordKey : record.keySet()) {
		    if (recordKey.endsWith("_ID")) {
			keyColumnName = recordKey;

		    } else {
			valueColumnName = recordKey;
		    }
		}
	    }
	    LOG.log(Level.INFO, "mappedSqlData, " + valueColumnName + ": " + record.get(valueColumnName) + " - "
		    + keyColumnName + ": " + record.get(keyColumnName));
	    reverseMap.put((String) record.get(valueColumnName), record.get(keyColumnName));
	}
	mappedSqlDataReverseMap.put(mappedSql, reverseMap);
	return reverseMap;

    }

    private Object getPickListDataKey(String cellValueAsString, Long pickListTableId) {
	Map<String, Object> pickListReverseDataMap = pickListDataReverseMap.get(pickListTableId);
	if (pickListReverseDataMap == null) {
	    pickListReverseDataMap = populatePickListData(pickListTableId);
	}
	return pickListReverseDataMap.get(cellValueAsString);

    }

    private Map<String, Object> populatePickListData(Long pickListTableId) {
	List<Map<String, Object>> pickListData = pickListDao.getPickListData(pickListTableId);
	Map<String, Object> reverseMap = new HashMap<>();
	for (Map<String, Object> record : pickListData) {
	    LOG.log(Level.FINE, "pickListData, DESCRIPTION: " + record.get("DESCRIPTION") + " - OPTION_ID: "
		    + record.get("OPTION_ID"));
	    reverseMap.put((String) record.get("DESCRIPTION"), record.get("OPTION_ID"));
	}
	pickListDataReverseMap.put(pickListTableId, reverseMap);
	return reverseMap;

    }

    private String getCellName(Cell cell1, Sheet sheet) {
	return String.valueOf(cell1.getColumnIndex());

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
