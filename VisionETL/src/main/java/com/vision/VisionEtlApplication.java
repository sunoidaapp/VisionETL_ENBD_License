package com.vision;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;

@SpringBootApplication
@EnableScheduling
@EnableEncryptableProperties
@EnableTransactionManagement
public class VisionEtlApplication {

	@Autowired
	public static ApplicationContext appContext;

	@Autowired
	DataSource datasource;

	@Value("${vision.allowed.origin}")
	private String[] allowedOrgin;

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext appContext) {
		this.appContext = appContext;
		return args -> {
		};
	}

	@Bean
	public WebMvcConfigurer corsConfigurer() {
		return new WebMvcConfigurer() {
			@Override
			public void addCorsMappings(CorsRegistry registry) {
				registry.addMapping("/**").allowedOrigins(allowedOrgin)
						.allowedHeaders(CrossOrigin.DEFAULT_ALLOWED_HEADERS)
						.allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS").maxAge(3600L);
			}
		};
	}

	public static void main(String[] args) {
		SpringApplication.run(VisionEtlApplication.class, args);
	}

}
