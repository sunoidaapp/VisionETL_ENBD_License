package com.vision.examples;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.stereotype.Component;

@Component
public class ProfileNameProvider {/*

	@Autowired
	private Environment environment;

	public void printActiveProfile() {
		String[] activeProfiles = environment.getActiveProfiles();
		if (activeProfiles.length > 0) {
			String activeProfile = activeProfiles[0];
			System.out.println("Active Profile: " + activeProfile);
		} else {
			System.out.println("No active profiles found.");
		}
	}

	public static void main(String[] args) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		ConfigurableEnvironment environment = new StandardEnvironment();
		context.setEnvironment(environment);
		context.register(ProfileNameProvider.class);
		context.refresh();
		ProfileNameProvider profileNameProvider = context.getBean(ProfileNameProvider.class);
		profileNameProvider.printActiveProfile();
		context.close();
	}
*/}
