package gov.va.aes.vear.dataloader.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class VEAR_DB_Prod_Config {
    @Autowired
    Environment env;

    String _dirsToBeProcessed;

    Map<String, ProjectConfig> _projectConfigMap;

    private static final Logger LOG = Logger.getLogger(VEAR_DB_Prod_Config.class.getName());

    @PostConstruct
    public void init() {
	_projectConfigMap = new HashMap<>();
	ProjectConfig projConfig = new ProjectConfig();
	projConfig.projectName = env.getProperty("vear.project.name");
	String configStr = env.getProperty("vear.dataloader.DbNullEqualsExcelBlank", "false");
	projConfig.compareDbNullEqualsExcelBlank = configStr.equals("true") ? true : false;
	String insertDbStr = env.getProperty("vear.dataloader.avoidVearDbInserts", "false");
	projConfig.avoidVearDbInserts = insertDbStr.equals("true") ? true : false;
	projConfig.inputDateFormat = env.getProperty("vear.dataloader.inputDateFormat", "MM/dd/yyyy");
	_projectConfigMap.put(".", projConfig);
	_dirsToBeProcessed = env.getProperty("vear.dataloader.dirsToBeProcessed", "");
	if (!"".equals(_dirsToBeProcessed)) {
	    String[] dirs = _dirsToBeProcessed.split(",");
	    for (String dir : dirs) {
		ProjectConfig dirProjConfig = new ProjectConfig();
		String dirConfigStr = env.getProperty("vear.dataloader." + dir + ".DbNullEqualsExcelBlank", "false");
		dirProjConfig.compareDbNullEqualsExcelBlank = "true".equals(dirConfigStr) ? true
			: projConfig.compareDbNullEqualsExcelBlank;
		String dirInsertDbStr = env.getProperty("vear.dataloader." + dir + ".avoidVearDbInserts", "false");
		dirProjConfig.avoidVearDbInserts = "true".equals(dirInsertDbStr) ? true : projConfig.avoidVearDbInserts;
		dirProjConfig.inputDateFormat = env.getProperty("vear.dataloader." + dir + ".inputDateFormat",
			projConfig.inputDateFormat);
		_projectConfigMap.put(dir, dirProjConfig);
	    }
	}

    }

    @Bean
    public DataSource dataSource() {
	DriverManagerDataSource dataSource = new DriverManagerDataSource();

	dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
	String dbUrl = env.getRequiredProperty("vear.datasource.url");
	dataSource.setUrl(dbUrl);
	String dbUserName = env.getRequiredProperty("vear.datasource.username");
	dataSource.setUsername(dbUserName);
	dataSource.setPassword(env.getRequiredProperty("vear.datasource.password"));
	LOG.log(Level.INFO, "Database Connection Info - URL:" + dbUrl + " User name: " + dbUserName);
	return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
	JdbcTemplate jdbcTemplate = new JdbcTemplate();
	jdbcTemplate.setDataSource(dataSource());
	return jdbcTemplate;
    }

    @Bean
    public PlatformTransactionManager txManager() {
	return new DataSourceTransactionManager(dataSource());
    }

    @Bean
    public Map<String, ProjectConfig> projectConfigMap() {
	return _projectConfigMap;
    }

    @Bean
    public String dirsToBeProcessed() {
	return _dirsToBeProcessed;
    }

}