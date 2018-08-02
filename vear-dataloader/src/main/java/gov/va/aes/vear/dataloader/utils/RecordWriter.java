package gov.va.aes.vear.dataloader.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

    public void writeOutput(List<Map<String, Object>> record, TableAndColumnMappingInfo tableAndColumnMappingInfo)
	    throws IOException {

	String sheetName = "RECORDS_FLAGGED_FOR_DELETION";// name of sheet

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

	for (int i = 0; i < columnNumList.size(); i++) {
	    DatabaseColumn dbColumn = columnMappings.get(columnNumList.get(i));
	    header.createCell(i).setCellValue(dbColumn.getDbColName());
	    columnNameList.add(dbColumn.getDbColName());
	}

	applyStyles(header, getHeaderStyle(wb), columnNumList.size());

	// Write Data
	Map<String, Object> outRecord = null;
	Iterator<Map<String, Object>> outIterator = record.iterator();
	int rowNumb = 1;

	while (outIterator.hasNext()) {
	    outRecord = outIterator.next();
	    if (outRecord != null) {
		XSSFRow row = sheet.createRow(rowNumb++);

		for (int i = 0; i < columnNameList.size(); i++) {
		    if (outRecord.get(columnNameList.get(i)) != null) {
			row.createCell(i).setCellValue(outRecord.get(columnNameList.get(i)).toString());
		    }
		}

	    }
	}

	// write this workbook to an Output stream.
	File file = new File(getOutFileName());
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

    private String getOutFileName() {
	DateTimeFormatter timeStampPattern = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	String dateStr = timeStampPattern.format(java.time.LocalDateTime.now());

	return FILE_PATH + "VEAR_ETL_OUTPUT_" + dateStr + ".xlsx";
    }
}
