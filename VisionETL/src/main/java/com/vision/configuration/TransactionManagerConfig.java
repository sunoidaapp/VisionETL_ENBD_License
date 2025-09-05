package com.vision.configuration;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
public class TransactionManagerConfig {
	/*@Bean
    public PlatformTransactionManager transactionManager() {
        // Configure JTA transaction manager for JBoss
        return new JtaTransactionManagerFactoryBean().getObject();
    }*/
	
	/*@Bean(name = "transactionManager")
    public JtaTransactionManager transactionManager(@Qualifier("dataSource") DataSource dataSource,
                                                   JtaProperties jtaProperties) {
        return new JtaTransactionManagerFactory(dataSource, jtaProperties).getObject();
    }
	*/
	
	private final DataSource dataSource;

    public TransactionManagerConfig(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Bean
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(dataSource);
    }
	
}