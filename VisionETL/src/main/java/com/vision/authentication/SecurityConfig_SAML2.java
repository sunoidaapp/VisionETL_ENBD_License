package com.vision.authentication;

import static org.springframework.security.config.Customizer.withDefaults;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.saml2.provider.service.metadata.OpenSamlMetadataResolver;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationFilter;
import org.springframework.security.saml2.provider.service.web.DefaultRelyingPartyRegistrationResolver;
import org.springframework.security.saml2.provider.service.web.RelyingPartyRegistrationResolver;
import org.springframework.security.saml2.provider.service.web.Saml2MetadataFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
@Profile("SAML2")
public class SecurityConfig_SAML2 {
	
	@Autowired
    JwtAuthenticationFilter jwtAuthenticationFilter;

	@Autowired
	private RelyingPartyRegistrationRepository relyingPartyRegistrationRepository;
	
	@Value("${app.allowed.saml.urls}")
    private String allowedURLs = ""; 

	@Bean
	public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
		RelyingPartyRegistrationResolver relyingPartyRegistrationResolver = new DefaultRelyingPartyRegistrationResolver(
				this.relyingPartyRegistrationRepository);

		Saml2MetadataFilter filter = new Saml2MetadataFilter(relyingPartyRegistrationResolver,
				new OpenSamlMetadataResolver());
		http
		.cors().and().csrf().disable()
		.authorizeHttpRequests(authorize -> authorize
				.antMatchers(HttpMethod.OPTIONS, "/**").permitAll()
				// Allow access to /refreshToken without authentication
				.antMatchers(allowedURLs.split(",")).permitAll() 
				.anyRequest().authenticated())
		.saml2Login(withDefaults())
		.saml2Logout(withDefaults())
		.addFilterBefore(filter, Saml2WebSsoAuthenticationFilter.class)
		.addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
		return http.build();
	}
	
	/*@Bean
    public SessionAuthenticationStrategy sessionAuthenticationStrategy() {
        return new SessionFixationProtectionStrategy(); // Configure session strategy
    }*/ 

}