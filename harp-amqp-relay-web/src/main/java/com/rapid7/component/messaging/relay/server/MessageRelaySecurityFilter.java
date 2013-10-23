/***************************************************************************
* Copyright (c) 2013, Rapid7 Inc
* 
* Redistribution and use in source and binary forms, with or without modification, are
* permitted provided that the following conditions are met:
* 
* * Redistributions of source code must retain the above copyright notice, this list of 
*   conditions and the following disclaimer.
* 
* * Redistributions in binary form must reproduce the above copyright notice, this list of
*   conditions and the following disclaimer in the documentation and/or other materials
*   provided with the distribution.
* 
* * Neither the name of Rapid7 nor the names of its contributors may be used to endorse or
*   promote products derived from this software without specific prior written permission.
* 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
* MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
* THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
* OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
* TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
* 
****************************************************************************/

package com.rapid7.component.messaging.relay.server;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A servlet filter abstract class that can be extended to authenticate and identify clients
 */
public abstract class MessageRelaySecurityFilter implements Filter {

   /**
    * Default constructor. 
    */
   public MessageRelaySecurityFilter() {
	// TODO Auto-generated constructor stub
   }
   
   /**
    * Abstract method to authenticate a client and return the appropriate client ID.
    * Must be overridden by concrete implementations.
    * @param request The servlet request context and parameters.
    * @return The unique ID for the client if authentication was successful.
    * @throws SecurityException If the client cannot successfully authenticate.
    */
   public abstract String getClientID(HttpServletRequest request) throws SecurityException;

   /**
    * @see Filter#destroy()
    */
   public void destroy() {
	// TODO Auto-generated method stub
   }

   /**
    * @see Filter#doFilter(ServletRequest, ServletResponse, FilterChain)
    */
   public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
	
	HttpServletRequest hreq = (HttpServletRequest)request;
	MessageRelayServletRequestWrapper wrapper = new MessageRelayServletRequestWrapper(hreq);
	
	try {
	   wrapper.setRemoteUser(this.getClientID(hreq));
	} catch (SecurityException e) {
	   ((HttpServletResponse)response).sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage());
	}
	
	// pass the request along the filter chain
	chain.doFilter(wrapper, response);
   }

   /**
    * @see Filter#init(FilterConfig)
    */
   public void init(FilterConfig fConfig) throws ServletException {
	// TODO Auto-generated method stub
   }

}
