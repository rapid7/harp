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

import com.rapid7.component.messaging.relay.MessageHandlerChain;
import com.rapid7.component.messaging.relay.amqp.BusConnector;
import com.rapid7.component.messaging.relay.amqp.BusMessageRetriever;
import com.rapid7.component.messaging.relay.amqp.OutputStreamHandler;
import com.rapid7.component.messaging.relay.amqp.SecurityFilterManager;
import com.rapid7.component.messaging.relay.amqp.StandardHandler;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.EncodedMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A Servlet implementation used to handle the server side of the AMQP message 
 * relay pipeline. This servlet accepts a set of optional initialization parameters that
 * can be used to control connection to the "local" amqp message bus:<P>
 * <UL>
 *    <LI>amqp-host : the hostname of the local message bus (Default: "localhost")</LI>
 *    <LI>amqp-port : the port of the local message bus (Default: 5672)</LI>
 *    <LI>amqp-user : the username used to authenticate to the bus</LI>
 *    <LI>amqp-passwd : the password used to authenticate to the bus</LI>
 *    <LI>amqp-connector-name : An identifier for the bus client used by this;
 *    servlet. Only significant if there is more than one MessageRelayServlet instance operating in the </LI>
 *    container (Default: "RELAY_SERVLET_DEFAULT")<P>
 * </UL>
 * The servlet implementation expects to obtain a unique client id by calling the getRemoteUser() method
 * of the supplied HttpServletRequest. It is thus compatible with a wide variety of Filter-based 
 * authentication schemes, as long as the filter implementation returns a request object that implements
 * getRemoteUser() appropriately.
 */

public class MessageRelayServlet extends HttpServlet {

   private static final long serialVersionUID = 1L;

   public static final String PARAM_AMQP_CONNECTOR_NAME = "amqp-connector-name";
   public static final String PARAM_AMQP_HOST_NAME = "amqp-host";
   public static final String PARAM_AMQP_PORT = "amqp-port";
   public static final String PARAM_AMQP_USER = "amqp-user";
   public static final String PARAM_AMQP_PASS = "amqp-passwd";

   public static final int DEFAULT_CHUNK_LIMIT = 100;
   public static final long DEFAULT_CHUNK_TIMEOUT = 5000;

   public static final String DEFAULT_AMQP_CONNECTOR_NAME = "RELAY_SERVLET_DEFAULT";

   private String m_connectorName;

   private SecurityFilterManager m_securityFilter = new SecurityFilterManager();
   
   /**
    * @see HttpServlet#HttpServlet()
    */
   public MessageRelayServlet() {
      super();
   }


   /** 
    * Handle the outgoing response stream for the specified client.
    * @param clientID The unique ID of the client.
    * @param responseStream The OutputStream associated with the HTTP(S) response.
    * @param limit The message chunk limit
    * @param timeout The message chunk timeout
    * @throws IOException
    */
   protected void handleResponseStream(String clientID, OutputStream responseStream, int limit, long timeout) 
         throws IOException {
      // Get a connector to the local message bus
      BusConnector connector = BusConnector.getConnector(m_connectorName);

      // Get the message retriever instance for this client
      BusMessageRetriever retriever = connector.getRetriever(clientID);

      // Stream encoded messages to the response stream using the message retriever
      MessageHandlerChain responseHandlers = new MessageHandlerChain()
      .add(m_securityFilter.getRequestHander(clientID))
      .add(new OutputStreamHandler(responseStream));

      retriever.retrieve(responseHandlers, limit, timeout);
   }

   /**
    * Handle incoming request streams from the specified client.
    * @param clientID the unique ID of the client.
    * @param requestStream The InputStream associated with the HTTP(S) request.
    * @throws IOException
    */
   protected void handleRequestStream(String clientID, InputStream requestStream) 
         throws IOException {
      // Get a connector to the local message bus
      BusConnector connector = BusConnector.getConnector(m_connectorName);
      // Get the message retriever instance for this client
      BusMessageRetriever retriever = connector.getRetriever(clientID);

      // Set up the message handler chain so we can handle acks, control messages,
      // ReplyTo headers, and then republish to the local message bus.
      MessageHandlerChain requestHandlers = new MessageHandlerChain()
      .add(m_securityFilter.getFilter(clientID))
      .add(new StandardHandler(retriever,connector));

      while (true) {
         // Pull encoded messages off the request stream until the client closes it.
         EncodedMessage message = EncodedMessage.parseDelimitedFrom(requestStream);
         if (message == null) break;
         // Submit the encoded messages to the message handler chain.
         requestHandlers.handle(message);
      }
   }

   @Override
   public void init(ServletConfig config) throws ServletException {
      super.init(config);

      // Set up AMQP connection parameters for the local message bus
      m_connectorName = config.getInitParameter(PARAM_AMQP_CONNECTOR_NAME);
      if (m_connectorName == null) m_connectorName = DEFAULT_AMQP_CONNECTOR_NAME;

      String host = config.getInitParameter(PARAM_AMQP_HOST_NAME);
      if (host == null) host = BusConnector.DEFAULT_HOST;

      int port = (config.getInitParameter(PARAM_AMQP_PORT) != null) ?
            Integer.parseInt(config.getInitParameter(PARAM_AMQP_PORT)) : BusConnector.DEFAULT_PORT;

      String user = config.getInitParameter(PARAM_AMQP_USER);
      String passwd = config.getInitParameter(PARAM_AMQP_PASS);

      // Set up the connector to the local message bus if it doesn't already exist.
      BusConnector connector = BusConnector.getConnector(m_connectorName);
      if (connector == null) {
         String hostSpec = host + ":" + port;
         if (user != null) hostSpec = user + "@" + hostSpec;
         connector = BusConnector.createConnector(m_connectorName,hostSpec,passwd);
      }
      
      // Set up the control message buffer queue
      
   }


   @Override
   public void destroy() {
      BusConnector.deleteConnector(m_connectorName);
      super.destroy();
   }


   /**
    * The doGet() method, used by the client to retrieve messages from the server's local
    * message bus. This method understands two request parameters that may be passed
    * by the client:
    *    chunk_limit - The maximum number of messages that will be returned with the request (Default 100).
    *    chunk_timeout - The maximum amount of time, in ms, that the request will run before
    *    ending (default 5000ms).
    * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
    */
   protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

      String clientID = request.getRemoteUser();

      int chunkLimit = DEFAULT_CHUNK_LIMIT;
      String chunkLimitString = request.getParameter("chunk_limit");
      if (chunkLimitString != null) chunkLimit = Integer.valueOf(chunkLimitString).intValue();

      long chunkTimeout = DEFAULT_CHUNK_TIMEOUT;
      String chunkTimeoutString = request.getParameter("chunk_timeout");
      if (chunkTimeoutString != null) chunkTimeout = Long.valueOf(chunkTimeoutString).longValue();

      response.setContentType("application/x-protobuf");
      handleResponseStream(clientID,response.getOutputStream(),chunkLimit,chunkTimeout);
   }

   /**
    * The doPost() method, used by the client to send messages to the server's local message bus. 
    * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
    */
   protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
      String clientID = request.getRemoteUser();
      handleRequestStream(clientID, request.getInputStream());
   }

}
