/*package com.vision.authentication;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
    			.allowedOrigins("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS") // Replace with your allowed methods
                .allowedHeaders(CrossOrigin.DEFAULT_ALLOWED_HEADERS) // Replace with your allowed headers
                .allowCredentials(true)
                .maxAge(3600);
    }
}
*/