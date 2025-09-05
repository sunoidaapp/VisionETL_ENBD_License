package com.vision.authentication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.security.sasl.AuthenticationException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vision.dao.LoginUserDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.Constants;
import com.vision.util.JwtTokenUtil;
import com.vision.vb.BearerTokenVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.LoginUserServices;

import io.jsonwebtoken.Claims;

@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {
    private static final String TOKEN_PREFIX = "Bearer ";
    private static final String HEADER_STRING = "Authorization";
    private static final String R_SESSION_ID = "r";
    private static final String ORIGIN = "Origin";
    
    private static String allowedPaths;
    private static String allowedURLs;
    private static String[] allowedOrgin;
    
	@Autowired
	LoginUserServices loginUsersServices;
	
	@Autowired
	LoginUserDao loginUserDao;
    
    @Value("${app.allowed.paths}")
	public void setAllowedPaths(String allowedPaths) {
    	JwtAuthenticationFilter.allowedPaths = allowedPaths;
	}
    
    @Value("${app.allowed.urls}")
	public void setAllowedURLs(String allowedURLs) {
    	JwtAuthenticationFilter.allowedURLs = allowedURLs;
	}
    
    @Value("${vision.allowed.origin}")
    public void setAllowedOrgin(String[] allowedOrgin) {
    	JwtAuthenticationFilter.allowedOrgin = allowedOrgin;
	}
    
    private static String baseURL;
    
    @Value("${app.baseURL}")
	public void setBbaseURL(String baseURL) {
    	JwtAuthenticationFilter.baseURL = baseURL;
	}
    
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
    	VisionUsersVb userVb = new VisionUsersVb();
		try {
			loginUsersServices.findSystemInfo(request, userVb);
			CustomContextHolder.setContext(userVb);
			if (shouldSkipTokenCheck(request, userVb)) {
				filterChain.doFilter(request, response);
				return;
			}
			ExceptionCode exceptionCode = extractToken(request);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				String bearerTokenEncrypted = (String) exceptionCode.getResponse();
				String rSessionIdReq = (String) exceptionCode.getOtherInfo();
				
				exceptionCode = loginUsersServices.performValidationOnBearerToken(bearerTokenEncrypted, rSessionIdReq);
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					BearerTokenVb existingBtokenVb = (BearerTokenVb) exceptionCode.getResponse();
					String decryptedBT = (String) exceptionCode.getOtherInfo();
					Claims claims = JwtTokenUtil.parseToken(decryptedBT);
					// String username = claims.getSubject();
					String username = "";
					List<String> roles = (List<String>) claims.get("roles");
					String visionId = String.valueOf(claims.get("v"));
					String rSessionId = String.valueOf(claims.get("r"));
					String bSessionId = String.valueOf(claims.get("b"));
					
					List<SimpleGrantedAuthority> authorities = new ArrayList<SimpleGrantedAuthority>();
					if (roles != null) {
						authorities = roles.stream().map(SimpleGrantedAuthority::new).collect(Collectors.toList());
					}

					UsernamePasswordAuthenticationToken authenticationToken = new UsernamePasswordAuthenticationToken(username, null, authorities);

					ObjectMapper objectMapper = new ObjectMapper();
//					userVb = objectMapper.convertValue(claims.get("ud"), VisionUsersVb.class);
					
					exceptionCode = loginUsersServices.getActiveUserByVisionId(visionId);
					if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
						userVb = (VisionUsersVb) exceptionCode.getResponse();
						userVb.setUserGrpProfile(userVb.getUserGroup() + "-" + userVb.getUserProfile());
						String userGrpProf = "";
						int cnt = 1;
						String[] userGrpProfileArr = userVb.getUserGrpProfile().split(",");
						for(int i=0; i<userGrpProfileArr.length; i++) {
							userGrpProf = userGrpProf+"'"+userGrpProfileArr[i]+"'";
							if(cnt != userGrpProfileArr.length)
								userGrpProf = userGrpProf+",";
							cnt++;
						}
						userVb.setUserGrpProfile(userGrpProf);
						userVb.setrSessionId(rSessionId);
						userVb.setbSessionId(bSessionId);
					} else {
						loginUsersServices.writePRD_Suspecious_Token_Audit(rSessionIdReq, null, null, bearerTokenEncrypted, "B", exceptionCode.getErrorMsg(), userVb);
						throw new AuthenticationException("Invalid token");
					}
					
					loginUsersServices.findSystemInfo(request, userVb);
					SecurityContextHolder.getContext().setAuthentication(authenticationToken);
					CustomContextHolder.setContext(userVb);
				} else {
					loginUsersServices.writePRD_Suspecious_Token_Audit(rSessionIdReq, null, null, bearerTokenEncrypted, "B", exceptionCode.getErrorMsg(), userVb);
					throw new AuthenticationException("Invalid token");
				}
				
			} else {
				loginUserDao.writePRD_Suspecious_Token_Audit(null, (String) exceptionCode.getOtherInfo(), null, null, "B", exceptionCode.getErrorMsg(), userVb);
				throw new AuthenticationException("Invalid token"); 
			}
		} catch (Exception e) {
			// e.printStackTrace();
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write(e.getMessage());
            return;
		}
    	filterChain.doFilter(request, response);
    }
    
    private boolean shouldSkipTokenCheck(HttpServletRequest request,VisionUsersVb userVb) {
    	if("OPTIONS".equalsIgnoreCase(request.getMethod())) {
    		return true;
    	} else {
    		String path = request.getRequestURI();
            List<String> allowedPathList = Arrays.asList(allowedPaths.split(","));
            for (String allowedPath : allowedPathList) {
                if (path.contains(allowedPath)) {
                    return true;
                }
            }
            
            List<String> allowedURLList = Arrays.asList(allowedURLs.split(","));
            for (String allowedURL : allowedURLList) {
                if (path.contains(allowedURL) || path.equalsIgnoreCase(baseURL+"/")) {
                    return true;
                }
            }
            
            return false;
    	}
    }

    private ExceptionCode extractToken(HttpServletRequest request) throws Exception {
    	ExceptionCode exceptionCode = new ExceptionCode();
        String bearerTokenEncrypted = request.getHeader(HEADER_STRING);
        if (StringUtils.hasText(bearerTokenEncrypted)){
        	if (bearerTokenEncrypted.startsWith(TOKEN_PREFIX)) {
        		exceptionCode.setResponse(bearerTokenEncrypted.substring(TOKEN_PREFIX.length()));
            } 
        } else {
        	exceptionCode.setErrorMsg("Bearer token is empty");
        	return exceptionCode;
        }
        
        
        String rSessionID = request.getHeader(R_SESSION_ID);
        if (StringUtils.hasText(rSessionID)){
        		exceptionCode.setOtherInfo(rSessionID);
        } else {
        	exceptionCode.setErrorMsg("Session ID is empty");
        	return exceptionCode;
        }
        if(allowedOrgin!=null && allowedOrgin.length > 0) {
        	boolean skip = Arrays.stream(allowedOrgin)
                    .anyMatch(s -> s.equalsIgnoreCase("*"));
        	if(!skip) {
        		String origin = request.getHeader(ORIGIN);
        		if (StringUtils.hasText(origin)){
        			boolean validOrgin = Arrays.stream(allowedOrgin)
                            .anyMatch(s -> s.equalsIgnoreCase(origin));
        			if(!validOrgin) {
        				exceptionCode.setErrorMsg("Invalid origin");
                       	return exceptionCode;
        			}
                } else {
                	exceptionCode.setErrorMsg("Origin is empty");
                	return exceptionCode;
                }
        	}
        }
        exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
        return exceptionCode;
    }

}
