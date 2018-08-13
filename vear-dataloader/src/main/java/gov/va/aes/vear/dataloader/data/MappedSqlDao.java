package gov.va.aes.vear.dataloader.data;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import gov.va.aes.vear.dataloader.model.DatabaseColumn;

@Component
public class MappedSqlDao {

    @Autowired
    public JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getMappedSqlData(DatabaseColumn dbColumn) {
	String mappingDataSql = "select " + dbColumn.getMappedKeyColumn() + ", " + dbColumn.getMappedValueColumn()
		+ " from " + dbColumn.getMappedTableName();
	if (dbColumn.getMappedFilterColumn() != null && !dbColumn.getMappedFilterColumn().trim().equals("")) {
	    mappingDataSql = mappingDataSql + " where " + dbColumn.getMappedFilterColumn();
	}

	return jdbcTemplate.queryForList(mappingDataSql);
    }

}
