package com.vision.authentication;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.login.AccountException;
import javax.security.auth.login.FailedLoginException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

import com.vision.exception.ExceptionCode;
import com.vision.util.Constants;
import com.vision.util.RSAEncryptDecryptUtil;
import com.vision.util.ValidationUtil;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LoginUserServices;

@Component
public class CustomAuthenticationProvider implements AuthenticationProvider {

	private final BCryptPasswordEncoder passwordEncoder;
	
	@Value("${vision.default.auth.type}")
	private String visionAuthType = "LDAP";
	
	@Value("${vision.default.auth.adServers}")
	private String visionAdServers = "skip";

	
	@Value("${vision.inbound.private.key}")
	private String visionInboundAuthPrivateKey;
	
	public CustomAuthenticationProvider() {
		this.passwordEncoder = new BCryptPasswordEncoder();
	}
	
	@Autowired
	private LoginUserServices loginUsersServices;
	
	private static int lastLdapUrlIndex;
	private static final String CONTEXT_FACTORY_CLASS = "com.sun.jndi.ldap.LdapCtxFactory";

	@Override
	public Authentication authenticate(Authentication authentication) throws AuthenticationException {
		String errorMsg = "The username and password entered do not match";
		UserDetails user = null;
		String username = authentication.getName();
		String providedDomainName = "";
		if (username.contains("\\")) {
			providedDomainName = (username.split("\\\\"))[0];
			username = (username.split("\\\\"))[1];
		}
		String password = authentication.getCredentials().toString();
		boolean isAuthenticationSuccess = false;
		
		//LDAP Authentication
		if("skip".equalsIgnoreCase(visionAdServers)) {
//			return new UsernamePasswordAuthenticationToken(user, password, user.getAuthorities());
			isAuthenticationSuccess = true;
		}else if("native".equalsIgnoreCase(visionAdServers)) {
			try {
				password = RSAEncryptDecryptUtil.decryptUsingPrivateKeyString(password, visionInboundAuthPrivateKey);
				ExceptionCode exceptionCode = loginUsersServices.getActiveUserByUserLoginIdOrUserEmailId(username);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					VisionUsersVb lUser = (VisionUsersVb) exceptionCode.getResponse();
					//Vision Password Check
					exceptionCode = loginUsersServices.visionPwdCheck(String.valueOf(lUser.getVisionId()), password);
					if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
						isAuthenticationSuccess = true;
					} else {
						errorMsg = String.format("Credential provided is invalid");
						isAuthenticationSuccess = false;
					}
				} else if(exceptionCode.getErrorCode() == Constants.WE_HAVE_ERROR_DESCRIPTION){
					errorMsg = exceptionCode.getErrorMsg();
					isAuthenticationSuccess = false;
				} else {
					errorMsg = String.format("The credentials are invalid");
					isAuthenticationSuccess = false;
				}
			} catch (Exception e) {
				e.printStackTrace();
				errorMsg = String.format("Authentication failure for user - `%s`", authentication.getName());
			}
		}else {
			try {
				//Decrypt Password
				try {
//					password = AES_Encrypt_DecryptUtil.decrypt(password);
//					password = URLDecoder.decode(password, "UTF-8");
					password = RSAEncryptDecryptUtil.decryptUsingPrivateKeyString(password, visionInboundAuthPrivateKey);
					
				} catch (Exception e) {
					// e.printStackTrace();
					throw new UsernameNotFoundException(errorMsg);
				}
				
				String[] adServerslst = visionAdServers.split(",");
				boolean isValidDomainProvided = false;
				if (ValidationUtil.isValid(providedDomainName)) {
					for (String domainName : adServerslst) {
						if (providedDomainName.equalsIgnoreCase(domainName))
							isValidDomainProvided = true;
					}
				}
				
				if (!ValidationUtil.isValid(providedDomainName) || isValidDomainProvided) {
					if (isValidDomainProvided) {
						adServerslst = new String[1];
						adServerslst[0] = providedDomainName;
					}
					
					for(String adServerName : adServerslst) {
						if (isAuthenticationSuccess)
							continue;
						String[] ldapServerUrlsArr = getActiveDirectoryAuthentication(adServerName.trim());
						if (ldapServerUrlsArr != null && ldapServerUrlsArr.length > 0) {
							lastLdapUrlIndex = 0;
							boolean authResult = authenticate(username, password, adServerName.trim(), ldapServerUrlsArr);
							if (authResult) {
								isAuthenticationSuccess = true;
							} else {
								isAuthenticationSuccess = false;
							}
						} else {
							isAuthenticationSuccess = false;
						}
					}
					
				} else {
					errorMsg = String.format("The provided domain `%s` is not valid", providedDomainName);
					isAuthenticationSuccess = false;
				}
			} catch (Exception e) {
				// e.printStackTrace();
				errorMsg = String.format("Authentication failure for user `%s`", authentication.getName());
			}
		}
		
		if(isAuthenticationSuccess) {
			user = User.builder()
					.username(username)
					.password(passwordEncoder.encode(password))
					.roles("ADMIN")
					.build();
			return new UsernamePasswordAuthenticationToken(user, password, null);
		} else {
			throw new UsernameNotFoundException(errorMsg);
		}
		
	}
	
	public String[] getActiveDirectoryAuthentication(String domainName) throws Exception {
		try {
			return nsLookup(domainName);
		} catch (Exception e) {
			// e.printStackTrace();
			// System.out.println("Domain not Avaible.Trying to Connect to another domains..." + domainName);
			throw e;
		}
	}

	private static String[] nsLookup(String argDomain) throws Exception {
		try {
			Hashtable<Object, Object> env = new Hashtable<Object, Object>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
			env.put("java.naming.provider.url", "dns:");
			DirContext ctx = new InitialDirContext(env);
			Attributes attributes = ctx.getAttributes(String.format("_ldap._tcp.%s", argDomain),
					new String[] { "srv" });
			// try thrice to get the KDC servers before throwing error
			for (int i = 0; i < 3; i++) {
				Attribute a = attributes.get("srv");
				if (a != null) {
					List<String> domainServers = new ArrayList<String>();
					NamingEnumeration<?> enumeration = a.getAll();
					while (enumeration.hasMoreElements()) {
						String srvAttr = (String) enumeration.next();
						// the value are in space separated 0) priority 1)
						// weight 2) port 3) server
						String values[] = srvAttr.toString().split(" ");
						domainServers.add(String.format("ldap://%s:%s", values[3], values[2]));
					}
					String domainServersArray[] = new String[domainServers.size()];
					domainServers.toArray(domainServersArray);
					return domainServersArray;
				}
			}
			throw new Exception("Unable to find srv attribute for the domain " + argDomain);
		} catch (NamingException exp) {
			throw new Exception("Error while performing nslookup. Root Cause: " + exp.getMessage(), exp);
		}
	}

	// LDAP Authentication
	public boolean authenticate(String username, String password, String domainName, String[] ldapServerUrlsArr)
			throws AccountException, FailedLoginException, NamingException {
		if (ldapServerUrlsArr == null || ldapServerUrlsArr.length == 0) {
			throw new AccountException("Unable to find ldap servers");
		}
		if (username == null || password == null || username.trim().length() == 0 || password.trim().length() == 0) {
			throw new FailedLoginException("Username or password is empty");
		}
		int retryCount = 0;
		int currentLdapUrlIndex = lastLdapUrlIndex;
		do {
			retryCount++;
			try {
				Hashtable<Object, Object> env = new Hashtable<Object, Object>();
				env.put(Context.INITIAL_CONTEXT_FACTORY, CONTEXT_FACTORY_CLASS);
				env.put(Context.PROVIDER_URL, ldapServerUrlsArr[currentLdapUrlIndex]);
				env.put(Context.SECURITY_PRINCIPAL, username + "@" + domainName);
				env.put(Context.SECURITY_CREDENTIALS, password);
				DirContext ctx = new InitialDirContext(env);
				ctx.close();
				lastLdapUrlIndex = currentLdapUrlIndex;
				return true;
			} catch (CommunicationException exp) {
				exp.printStackTrace();
				// if the exception of type communication we can assume the AD
				// is not reachable hence retry can be attempted with next
				// available AD
				if (retryCount < ldapServerUrlsArr.length) {
					currentLdapUrlIndex++;
					if (currentLdapUrlIndex == ldapServerUrlsArr.length) {
						currentLdapUrlIndex = 0;
					}
					continue;
				}
				return false;
			}
		} while (true);
	}
	
	

	@Override
	public boolean supports(Class<?> authentication) {
		return authentication.equals(UsernamePasswordAuthenticationToken.class);
	}
}
