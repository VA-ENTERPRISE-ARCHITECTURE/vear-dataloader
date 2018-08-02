package gov.va.aes.vear.dataloader.configuration;

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

import gov.va.aes.vear.dataloader.utils.GlobalValues;

@Configuration
public class VEAR_DB_Prod_Config {
    @Autowired
    Environment env;

    @PostConstruct
    public void init() {
	GlobalValues.projectName = env.getProperty("vear.project.name");
    }

    @Bean
    public DataSource dataSource() {
	DriverManagerDataSource dataSource = new DriverManagerDataSource();

	dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
	dataSource.setUrl(env.getRequiredProperty("vear.datasource.url"));
	dataSource.setUsername(env.getRequiredProperty("vear.datasource.username"));
	dataSource.setPassword(env.getRequiredProperty("vear.datasource.password"));

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

}