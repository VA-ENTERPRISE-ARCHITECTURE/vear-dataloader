package gov.va.aes.vear.dataloader.configuration;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public final class GlobalValues {

    private GlobalValues() {
    }

    public static int EXAMPLE_VASI_ID = 1006;
    public static List<String> ERROR_MESSAGES = new ArrayList<String>();

    public static String PickList_Table_Name = "list_option";
    public static String PickList_ListId_Column = "list_id";
    public static String PickList_Key_Column = "OPTION_ID";
    public static String PickList_Value_Column = "description";

    public static int entServiceCount = 0;
    public static int validBusServiceCount = 0;
    public static int addCount = 0;
    public static int updateCount = 0;
    public static int matchCount = 0;

    public static String FILE_PATH = "." + File.separator + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
	    + File.separator;
}
