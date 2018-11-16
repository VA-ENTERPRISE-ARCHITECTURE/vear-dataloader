package gov.va.aes.vear.dataloader.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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

    String inputDateFormat;

    private Map<Long, Map<String, Object>> pickListDataReverseMap = new HashMap<>();
    private Map<DatabaseColumn, Map<String, Object>> mappedSqlDataReverseMap = new HashMap<>();

    public void reset() {
	pickListDataReverseMap = new HashMap<>();
	mappedSqlDataReverseMap = new HashMap<>();
    }

    public List<Map<String, Object>> readDataFiles(final String[] dataFiles,
	    final Collection<TableAndColumnMappingInfo> tableMappingInfo)
	    throws FileNotFoundException, IOException, ValidateException {
	List<Map<String, Object>> excelRecords = new ArrayList<>();
	for (String dataFile : dataFiles) {

	    if (dataFile.endsWith(".xlsx") || dataFile.endsWith(".xls")) {
		readExelDataFile(tableMappingInfo, excelRecords, dataFile);
	    } else if (dataFile.endsWith(".DAT")) {
		readCSVDataFile(tableMappingInfo, excelRecords, dataFile, '|');
	    } else if (dataFile.endsWith(".csv")) {
		readCSVDataFile(tableMappingInfo, excelRecords, dataFile, ',');
	    }
	}
	return excelRecords;
    }

    private void readExelDataFile(final Collection<TableAndColumnMappingInfo> tableMappingInfo,
	    List<Map<String, Object>> excelRecords, String dataFile)
	    throws FileNotFoundException, IOException, ValidateException {
	LOG.log(Level.INFO, "Reading Excel File : {0}", dataFile);
	FileInputStream inputStream = new FileInputStream(new File(dataFile));
	Workbook workbook = new XSSFWorkbook(inputStream);
	Sheet firstSheet = workbook.getSheetAt(0);

	Iterator<Row> iterator = firstSheet.iterator();

	Map<String, DatabaseColumn> excelColNamesMap = new HashMap<>();
	for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {
	    for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
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
		if (excelColNamesMap.containsKey(headerName)) {

		    DatabaseColumn dbColumn = excelColNamesMap.get(headerName);
		    // LOG.log(Level.FINE, "Reading Cell: " + headerName + " - type: " +
		    // dbColumn.getDbColType());
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

    private void readCSVDataFile(final Collection<TableAndColumnMappingInfo> tableMappingInfo,
	    List<Map<String, Object>> excelRecords, String dataFile, char delimiter)
	    throws FileNotFoundException, IOException, ValidateException {
	LOG.log(Level.INFO, "Reading Excel File : {0}", dataFile);

	Reader reader = Files.newBufferedReader(Paths.get((new File(dataFile)).getAbsolutePath()),
		Charset.forName("ISO-8859-1"));

	CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(delimiter));

	Map<String, DatabaseColumn> excelColNamesMap = new HashMap<>();
	for (TableAndColumnMappingInfo tableAndColumnMappingInfo : tableMappingInfo) {
	    for (Map.Entry<String, DatabaseColumn> mapping : tableAndColumnMappingInfo.getColumnMappings().entrySet()) {
		excelColNamesMap.put(mapping.getKey(), mapping.getValue());
	    }
	}

	long csvlineNumber = 1;
	boolean firstLine = true;

	try {
	    for (CSVRecord csvRecord : csvParser) {

		if (firstLine) {
		    validateDataFileColumnsExists(excelColNamesMap, csvRecord);
		    firstLine = false;
		    continue; // just skip the header row
		}

		Map<String, Object> excelRecord = new HashMap<>();
		for (int i = 0; i < csvRecord.size(); i++) {
		    String iStr = String.valueOf(i);
		    if (excelColNamesMap.containsKey(iStr)) {
			DatabaseColumn dbColumn = excelColNamesMap.get(iStr);
			String cell = csvRecord.get(i);
			Object valueObj = getValueAsObject(cell, dbColumn);

			if (dbColumn.isExcelColumnDataCleanup()) {
			    String cleanedUpValue = cleanupValue((String) valueObj);
			    excelRecord.put(iStr, cleanedUpValue);
			} else {
			    excelRecord.put(iStr, valueObj);
			}
		    }
		}

		excelRecords.add(excelRecord);
		csvlineNumber++;

	    }
	} catch (Exception e) {
	    LOG.log(Level.INFO, "Last Processed CSV Record : " + csvlineNumber);
	    LOG.log(Level.SEVERE, "CSV Parsing Failed : ", e);
	    throw e;
	} finally {
	    csvParser.close();
	}

    }

    private void validateDataFileColumnsExists(Map<String, DatabaseColumn> excelColNamesMap, CSVRecord csvRecord)
	    throws ValidateException {
	List<String> headerNamesList = new ArrayList<>();

	for (int i = 0; i < csvRecord.size(); i++) {

	    headerNamesList.add(String.valueOf(i));
	}
	Set<String> headerNamesSet = excelColNamesMap.keySet();

	for (String mappingHeaderName : headerNamesSet) {
	    if (!headerNamesList.contains(mappingHeaderName)) {
		throw new ValidateException("Invalid Input Data File. Header Name \"" + mappingHeaderName
			+ "\" does not exist in Excel file.");

	    }
	}

    }

    // collect headerColumns to verify all columns are present
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

			return new Timestamp(new SimpleDateFormat(inputDateFormat).parse(dateStr).getTime());
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
		return getSqlMappedDataKey(getCellValueAsString(cell), dbColumn);
	    }
	} catch (NullPointerException e) {

	    throw new RuntimeException("getValueAsObject Error, Unsupported DB Column Type in Mapping: ", e);
	    // return null;
	}
	throw new RuntimeException(" Unsupported DB Column Type in Mapping");
    }

    private Object getValueAsObject(String cell, DatabaseColumn dbColumn) {

	try {
	    if (dbColumn.getDbColType().equals("TEXT")) {
		return cell;
	    } else if (dbColumn.getDbColType().equals("NUMBER")) {

		return cell == null || "".equals(cell) ? null : new BigDecimal(cell);
	    } else if (dbColumn.getDbColType().equals("DATE")) {

		if (cell == null)
		    return null;
		try {
		    return new Timestamp(new SimpleDateFormat(inputDateFormat).parse(cell).getTime());
		} catch (ParseException e) {
		    return null;
		}

	    } else if (dbColumn.getDbColType().equals("PICKLIST")) {
		return getPickListDataKey(cell, dbColumn.getPickListTableId());
	    } else if (dbColumn.getDbColType().equals("BOOLEAN")) {
		String boolStr = cell;
		if ("TRUE".equals(boolStr)) {
		    return new Integer(1);
		} else {
		    return new Integer(0);
		}
	    } else if (dbColumn.getDbColType().equals("MAPPED")) {
		return getSqlMappedDataKey(cell, dbColumn);
	    }
	} catch (NullPointerException e) {

	    throw new RuntimeException("getValueAsObject Error, Unsupported DB Column Type in Mapping: ", e);
	    // return null;
	}
	throw new RuntimeException(" Unsupported DB Column Type in Mapping");
    }

    private Object getSqlMappedDataKey(String cellValueAsString, DatabaseColumn dbColumn) {
	Map<String, Object> mappedSqlDataRevsMap = mappedSqlDataReverseMap.get(dbColumn);
	if (mappedSqlDataRevsMap == null) {
	    mappedSqlDataRevsMap = populateMappedSqlData(dbColumn);
	}
	return mappedSqlDataRevsMap.get(cellValueAsString);
    }

    private Map<String, Object> populateMappedSqlData(DatabaseColumn dbColumn) {
	List<Map<String, Object>> mappedSqlData = mappedSqlDao.getMappedSqlData(dbColumn);
	Map<String, Object> reverseMap = new HashMap<>();
	String keyColumnName = dbColumn.getMappedKeyColumn(), valueColumnName = dbColumn.getMappedValueColumn();

	for (Map<String, Object> record : mappedSqlData) {

	    LOG.log(Level.FINE, "mappedSqlData, " + valueColumnName + ": " + record.get(valueColumnName) + " - "
		    + keyColumnName + ": " + record.get(keyColumnName));
	    reverseMap.put(record.get(valueColumnName).toString(), record.get(keyColumnName));
	}
	mappedSqlDataReverseMap.put(dbColumn, reverseMap);
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
		    SimpleDateFormat dateFormat = new SimpleDateFormat(inputDateFormat);
		    strCellValue = dateFormat.format(cell.getDateCellValue());
		} else {
		    strCellValue = cell.toString();
		    /*
		     * Double value = cell.getNumericCellValue(); Long longValue =
		     * value.longValue(); strCellValue = new String(value.toString());
		     */
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
