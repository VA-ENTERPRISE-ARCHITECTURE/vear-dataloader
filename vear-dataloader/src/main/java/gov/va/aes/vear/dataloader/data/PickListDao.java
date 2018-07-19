package gov.va.aes.vear.dataloader.data;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PickListDao {

    @Autowired
    public JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getPickListData(Long picklistTableId) {
	return jdbcTemplate.queryForList("select OPTION_ID, DESCRIPTION from ee.list_option where list_id = ?",
		picklistTableId);
    }

}
