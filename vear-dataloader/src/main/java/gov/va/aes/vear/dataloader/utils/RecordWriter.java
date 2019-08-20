package gov.va.aes.vear.dataloader.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.model.DatabaseColumn;
import gov.va.aes.vear.dataloader.model.TableAndColumnMappingInfo;

@Component
public class RecordWriter {

    private static final Logger LOG = Logger.getLogger(RecordWriter.class.getName());

    public static String FILE_PATH = "." + File.separator + "OUTPUT_"
	    + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + File.separator;

    public <T extends Enum<T>> T[] toEnums(String[] arr, Class<T> type) {
	@SuppressWarnings("unchecked")
	T[] result = (T[]) Array.newInstance(type, arr.length);
	for (int i = 0; i < arr.length; i++)
	    result[i] = Enum.valueOf(type, arr[i]);
	return result;
    }

    public void writeCSVOutput(List<Map<String, Object>> recordsList,
	    TableAndColumnMappingInfo tableAndColumnMappingInfo, String fileNamePrefix, String oddRecordMarker,
	    String evenRecordMarker, boolean oddRowKeysAreColNums, boolean evenRowKeysAreColNums) throws IOException {

	// String sheetName = "RECORDS_FLAGGED_FOR_DELETION";// name of sheet

	Map<String, DatabaseColumn> columnMappings = tableAndColumnMappingInfo.getColumnMappings();
	List<String> columnNumList = new ArrayList<>();
	List<String> columnNameList = new ArrayList<>();
	columnNumList.addAll(columnMappings.keySet());
	columnNumList.sort(new Comparator<String>() {

	    public int compare(String o1, String o2) {
		return new BigDecimal(o1).compareTo(new BigDecimal(o2));
	    }

	});

	List<String> headerList = new ArrayList<>();

	int startColumIndex = 0;
	if (evenRecordMarker != null && oddRecordMarker != null) {
	    headerList.add("RECORD_FROM");
	    startColumIndex = 1;
	}

	for (int i = startColumIndex; i < columnNumList.size() + startColumIndex; i++) {
	    DatabaseColumn dbColumn = columnMappings.get(columnNumList.get(i - startColumIndex));
	    headerList.add(dbColumn.getDbColName());
	    columnNameList.add(dbColumn.getDbColName());
	}
	File file = new File(getOutFileName(fileNamePrefix, "csv"));
	file.getParentFile().mkdirs();

	BufferedWriter writer = Files.newBufferedWriter(Paths.get(file.getAbsolutePath()));

	CSVPrinter csvPrinter = new CSVPrinter(writer,
		CSVFormat.DEFAULT.withHeader(headerList.toArray(new String[headerList.size()])));

	// Write Data
	Map<String, Object> outRecord = null;
	Iterator<Map<String, Object>> outIterator = recordsList.iterator();
	int rowNumb = 1;

	while (outIterator.hasNext()) {
	    outRecord = outIterator.next();

	    String recordMarker = "";
	    if (startColumIndex == 1) {
		if ((rowNumb % 2) == 1) {
		    recordMarker = oddRecordMarker;
		} else {
		    recordMarker = evenRecordMarker;
		}

	    }
	    LOG.log(Level.FINE, "Writing [" + recordMarker + "] record " + outRecord);
	    if (outRecord != null) {
		List<Object> rowValuesList = new ArrayList<>();

		if (startColumIndex == 1) {
		    if ((rowNumb % 2) == 1) {
			rowValuesList.add(oddRecordMarker);
		    } else {
			rowValuesList.add(evenRecordMarker);
		    }
		}

		for (int i = startColumIndex; i < columnNameList.size() + startColumIndex; i++) {

		    String columnKey = null;
		    if ((rowNumb % 2) == 0) {
			if (evenRowKeysAreColNums) {
			    columnKey = columnNameList.get(i - startColumIndex);
			} else {
			    columnKey = columnNumList.get(i - startColumIndex);// String.valueOf(i - startColumIndex);
			}

		    } else {
			if (oddRowKeysAreColNums) {
			    columnKey = columnNameList.get(i - startColumIndex);
			} else {
			    columnKey = columnNumList.get(i - startColumIndex);
			    ;
			}
		    }
		    LOG.log(Level.FINE, "startColumIndex =" + startColumIndex + ", columnKey=" + columnKey
			    + ", outRecord.get(columnKey) = " + outRecord.get(columnKey));
		    if (outRecord.get(columnKey) != null) {
			rowValuesList.add(outRecord.get(columnKey).toString());
		    } else {
			rowValuesList.add("");
		    }
		}
		// write the records to workbook.
		csvPrinter.printRecord(rowValuesList.toArray(new Object[rowValuesList.size()]));

	    }
	    rowNumb++;
	}

	// close the writer.
	csvPrinter.close();
	writer.close();
    }

    public void writeOutput(List<Map<String, Object>> recordsList, TableAndColumnMappingInfo tableAndColumnMappingInfo,
	    String fileNamePrefix, String sheetName, String evenRecordMarker, String oddRecordMarker,
	    boolean evenKeyIsColumnNum, boolean oddKeyIsColumnNum) throws IOException {

	// String sheetName = "RECORDS_FLAGGED_FOR_DELETION";// name of sheet

	XSSFWorkbook wb = new XSSFWorkbook();
	XSSFSheet sheet = wb.createSheet(sheetName);

	// Add Header Row
	XSSFRow header = sheet.createRow(0);

	Map<String, DatabaseColumn> columnMappings = tableAndColumnMappingInfo.getColumnMappings();
	List<String> columnNumList = new ArrayList<>();
	List<String> columnNameList = new ArrayList<>();
	columnNumList.addAll(columnMappings.keySet());
	columnNumList.sort(new Comparator<String>() {

	    public int compare(String o1, String o2) {
		return new BigDecimal(o1).compareTo(new BigDecimal(o2));
	    }

	});
	int startColumIndex = 0;
	if (evenRecordMarker != null && oddRecordMarker != null) {
	    header.createCell(0).setCellValue("RECORD_FROM");
	    startColumIndex = 1;
	}

	for (int i = startColumIndex; i < columnNumList.size() + startColumIndex; i++) {
	    DatabaseColumn dbColumn = columnMappings.get(columnNumList.get(i - startColumIndex));
	    header.createCell(i).setCellValue(dbColumn.getDbColName());
	    columnNameList.add(dbColumn.getDbColName());
	}

	applyStyles(header, getHeaderStyle(wb), columnNumList.size());

	// Write Data
	Map<String, Object> outRecord = null;
	Iterator<Map<String, Object>> outIterator = recordsList.iterator();
	int rowNumb = 1;

	while (outIterator.hasNext()) {
	    outRecord = outIterator.next();
	    String recordMarker = "";
	    if (startColumIndex == 1) {
		if ((rowNumb % 2) == 1) {
		    recordMarker = oddRecordMarker;
		} else {
		    recordMarker = evenRecordMarker;
		}
		LOG.log(Level.FINE, "Writing [" + recordMarker + "] record " + outRecord);
	    }

	    if (outRecord != null) {
		XSSFRow row = sheet.createRow(rowNumb++);

		if (startColumIndex == 1) {
		    if ((rowNumb % 2) == 1) {
			row.createCell(0).setCellValue(oddRecordMarker);
		    } else {
			row.createCell(0).setCellValue(evenRecordMarker);
		    }
		}

		for (int i = startColumIndex; i < columnNameList.size() + startColumIndex; i++) {

		    String columnKey = null;
		    if ((rowNumb % 2) == 1) {
			if (oddKeyIsColumnNum) {
			    columnKey = columnNameList.get(i - startColumIndex);
			} else {
			    columnKey = String.valueOf(i - startColumIndex);
			}

		    } else {
			if (evenKeyIsColumnNum) {
			    columnKey = columnNameList.get(i - startColumIndex);
			} else {
			    columnKey = String.valueOf(i - startColumIndex);
			}
		    }
		    if (outRecord.get(columnKey) != null) {
			row.createCell(i).setCellValue(outRecord.get(columnKey).toString());
		    }
		}

	    }
	}

	// write this workbook to an Output stream.
	File file = new File(getOutFileName(fileNamePrefix, "xlsx"));
	file.getParentFile().mkdirs();
	FileOutputStream fileOut = new FileOutputStream(file);

	wb.write(fileOut);
	fileOut.flush();
	fileOut.close();
    }

    private CellStyle getHeaderStyle(XSSFWorkbook wb) {
	XSSFFont font = wb.createFont();
	font.setFontHeightInPoints((short) 12);
	font.setFontName("Calibri");
	font.setColor(IndexedColors.BLACK.getIndex());
	font.setBold(false);
	font.setItalic(false);

	CellStyle style = wb.createCellStyle();
	style.setFont(font);

	return style;

    }

    private CellStyle getSubHeaderStyle(XSSFWorkbook wb) {
	XSSFFont font = wb.createFont();
	font.setFontHeightInPoints((short) 12);
	font.setFontName("Calibri");
	font.setColor(IndexedColors.BLUE.getIndex());
	font.setBold(true);
	font.setItalic(false);

	CellStyle style = wb.createCellStyle();
	style.setFont(font);

	return style;
    }

    private void applyStyles(XSSFRow row, CellStyle style, int numCells) {
	for (int i = 0; i < numCells; i++) {
	    row.getCell(i).setCellStyle(style);
	}

    }

    private String getOutFileName(String fileNamePrefix, String suffix) {
	DateTimeFormatter timeStampPattern = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	String dateStr = timeStampPattern.format(java.time.LocalDateTime.now());

	return FILE_PATH + fileNamePrefix + dateStr + "." + suffix;
    }
}
