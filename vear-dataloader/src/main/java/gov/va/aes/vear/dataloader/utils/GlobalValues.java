package gov.va.aes.vear.dataloader.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class GlobalValues {

    private GlobalValues() {
    }

    public static List<String> ERROR_MESSAGES = new ArrayList<String>();
    public static List<Map<String, Object>> output = new ArrayList<>();

    public static int TotalInputRecordsCount = 0;
    public static int recordsUpdateCount = 0;
    public static int recordsInsertCount = 0;
    public static int recordsMatchCount = 0;
    public static List<Map<String, Object>> recordsUpdated = new ArrayList<>();
    public static List<Map<String, Object>> recordsInserted = new ArrayList<>();
    public static List<Map<String, Object>> recordsFailingUpdate = new ArrayList<>();
    public static List<Map<String, Object>> recordsFailingInsert = new ArrayList<>();
    public static List<Map<String, Object>> dbRecordsNotFound = new ArrayList<>();

    public static String MAPPING_FILE_PATH = "." + File.separator + "Vear_ETL_Mapping.xlsx";
    public static String INPUT_FILE_PATH = "." + File.separator + "Input_Files" + File.separator;
}
