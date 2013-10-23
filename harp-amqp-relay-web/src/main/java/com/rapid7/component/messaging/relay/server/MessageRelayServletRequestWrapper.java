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

import java.security.Principal;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * Request wrapper that works with the security filter to relay key information 
 * to the MessageRelayServlet implementation.
 * @see MessageRelaySecurityFilter
 * @see MessageRelayServlet
 */
public class MessageRelayServletRequestWrapper extends
      HttpServletRequestWrapper implements HttpServletRequest, ServletRequest {
   
   private class MessageRelayPrincipal implements Principal {
	
	private String m_name;
	
	MessageRelayPrincipal(String name) {
	   m_name = name;
	}

	@Override
      public String getName() {
	   return m_name;
      }
	
   }
   
   public static final String CLIENT_ID_SESSION_KEY = MessageRelayServlet.class.getName() + ".client_id";
   
   private HttpServletRequest m_request;

   public MessageRelayServletRequestWrapper(HttpServletRequest request) {
	super(request);
	m_request = request;
   }
   
   @Override
   public String getRemoteUser() {
	return (String)m_request.getSession().getAttribute(CLIENT_ID_SESSION_KEY);
   }

   @Override
   public Principal getUserPrincipal() {
	return new MessageRelayPrincipal(this.getRemoteUser());
   }
      
   public void setRemoteUser(String userName) {
	m_request.getSession().setAttribute(CLIENT_ID_SESSION_KEY, userName);
   }

   
}
