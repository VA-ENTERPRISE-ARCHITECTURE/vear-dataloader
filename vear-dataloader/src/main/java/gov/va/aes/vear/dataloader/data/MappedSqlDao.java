package gov.va.aes.vear.dataloader.data;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class MappedSqlDao {

    @Autowired
    public JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getMappedSqlData(String mappingDataSql) {

	return jdbcTemplate.queryForList(mappingDataSql);
    }

}
