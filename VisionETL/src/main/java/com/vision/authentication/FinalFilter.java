package com.vision.authentication;

public class FinalFilter {
//implements Filter 
	/*
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Initialization code, if needed
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // Your custom logic goes here before the request proceeds through the chain

        // For example, you might log some information
        System.out.println("NewFilter executing...");

        HttpServletRequest httpReq = (HttpServletRequest) request;
        HttpServletResponse httpRes = (HttpServletResponse) response;
        
        String existingHeader = httpRes.getHeaders("Set-Cookie").stream()
                .filter(header -> header.startsWith("JSESSIONID="))
                .findFirst()
                .orElse(null);

        if (existingHeader != null) {
        	System.out.println("***************************************"+httpReq.getRequestURI());
        	System.out.println(existingHeader);
        	String modifiedHeader = existingHeader + "; HttpOnly;";
        	httpRes.setHeader("Set-Cookie", modifiedHeader);
        	httpRes.setHeader("DDSet", modifiedHeader);
        }
        
        // Proceed with the filter chain
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        httpResponse.addHeader("Custom-Header", "Custom-Value");
    }

    @Override
    public void destroy() {
        // Cleanup code, if needed
    }
*/
}
