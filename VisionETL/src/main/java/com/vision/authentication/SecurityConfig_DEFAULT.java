package com.vision.authentication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
@Profile("DEFAULT")
public class SecurityConfig_DEFAULT extends WebSecurityConfigurerAdapter {

    @Autowired
    private CustomAuthenticationProvider authenticationProvider;
    
    @Autowired
    JwtAuthenticationFilter jwtAuthenticationFilter;
    
    @Value("${app.allowed.urls}")
    private String allowedURLs = ""; 
    

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider(authenticationProvider);
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
    	http.cors().and()
		.csrf().disable() // Disable CSRF protection for API endpoints
		.formLogin().disable() // Disable form-based login
		.httpBasic().disable() // Disable basic authentication
		.authorizeRequests()
        .antMatchers(HttpMethod.OPTIONS, "/**").permitAll() // Allow access to any request with OPTION method without authentication
		.antMatchers(allowedURLs.split(",")).permitAll()  // Allow access to /login & /refreshToken without authentication
		.anyRequest().authenticated()
    	.and()
		.addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
    }
    
    @Bean
	@Override
	public AuthenticationManager authenticationManagerBean() throws Exception {
	    return super.authenticationManagerBean();
	}
    
  /*  @Override
    public void configure(WebSecurity web) throws Exception {
		web.ignoring().antMatchers("/v2/api-docs/**").antMatchers("/swagger.json").antMatchers("/swagger-ui.html")
				.antMatchers("/swagger-resources/**").antMatchers("/webjars/**");
	}*/
  
}

