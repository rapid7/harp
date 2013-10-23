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

package com.rapid7.component.messaging.relay.client;

import com.rapid7.component.messaging.relay.MessageHandler;
import com.rapid7.component.messaging.relay.MessageRetriever;
import com.rapid7.component.messaging.relay.MessageRetrieverEntity;
import java.net.URISyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;

/**
 * A Runnable task implementation that can be used by clients to connect to
 * the server side of the HTTP(S) message relay pipeline and transmit messages
 * from forward queues in the local message bus.
 */
public class MessageTransmitterTask extends MessageRelayTask
{
   private HttpPost m_postRequest;
   private MessageRetriever m_retriever;
   private MessageHandler m_additionalHandler = null;
   
   /**
    * Create a new MessageTransmitterTask
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param retriever A MessageRetriever instance that will be used to retrieve messages from the local
    * message bus for forwarding.
    * @param chunkLimit The maximum number of messages to retrieve with a single HTTP(S) request cycle.
    * @param chunkTimeout The maximum time limit to keep a single HTTP(S) request cycle open. It is important
    * not to set this timeout too high, or intervening HTTP(S) infrastructure (e.g. proxies) may
    * pre-emptively timeout the request, which can result in lost messages.
    * @throws URISyntaxException
    */
   public MessageTransmitterTask(String targetURL, String clientID, MessageRetriever retriever, 
      int chunkLimit, long chunkTimeout) throws URISyntaxException {
      
      super(targetURL,clientID,chunkLimit,chunkTimeout);
      m_postRequest = new HttpPost(m_appURI);
      m_retriever = retriever;
            
   }
   
   /**
    * Create a new MessageTransmitterTask with custom message handlers
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param retriever A MessageRetriever instance that will be used to retrieve messages from the local
    * message bus for forwarding.
    * @param chunkLimit The maximum number of messages to retrieve with a single HTTP(S) request cycle.
    * @param chunkTimeout The maximum time limit to keep a single HTTP(S) request cycle open. It is important
    * not to set this timeout too high, or intervening HTTP(S) infrastructure (e.g. proxies) may
    * @param additionalHandler An additional custom message handler or handler chain through which
    * messages should be processed before being transmitted.
    * pre-emptively timeout the request, which can result in lost messages.
    * @throws URISyntaxException
    */
   public MessageTransmitterTask(String targetURL, String clientID, MessageRetriever retriever, 
      int chunkLimit, long chunkTimeout, MessageHandler additionalHandler) throws URISyntaxException {
      
      this(targetURL,clientID,retriever,chunkLimit,chunkTimeout);
      m_additionalHandler = additionalHandler;
            
   }
   
   /**
    * Create a new MessageTransmitterTask with default chunking parameters.
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param retriever A MessageRetriever instance that will be used to retrieve messages from the local
    * message bus for forwarding.
    * @throws URISyntaxException
    */
   public MessageTransmitterTask(String targetURL, String clientID, MessageRetriever retriever)
	   throws URISyntaxException {
	this(targetURL, clientID, retriever,DEFAULT_CHUNK_LIMIT,DEFAULT_CHUNK_TIMEOUT);
   }


   
   /**
    * Create a new MessageTransmitterTask with default chunking parameters and a custom message handler.
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param retriever A MessageRetriever instance that will be used to retrieve messages from the local
    * message bus for forwarding.
    * @param additionalHandler An additional custom message handler or handler chain through which
    * messages should be processed before being transmitted.
    * @throws URISyntaxException
    */
   public MessageTransmitterTask(String targetURL, String clientID, MessageRetriever retriever,
	   MessageHandler additionalHandler)
	   throws URISyntaxException {
	this(targetURL, clientID, retriever,DEFAULT_CHUNK_LIMIT,DEFAULT_CHUNK_TIMEOUT,additionalHandler);
   }

      
   
   @Override
   protected void doRequest() throws Exception {
      // Create an entity from the MessageRetriever
      HttpEntity entity = new MessageRetrieverEntity(m_retriever,m_chunkLimit,m_chunkTimeout,
      	m_additionalHandler);
      
      // Set the outgoing HTTP(S) POST content to the entity
      m_postRequest.setEntity(entity);
      
      // Execute the POST request
      HttpResponse response = m_httpClient.execute(m_postRequest);
      // Ignore the response, for now
      response.getEntity().getContent().close();
   }
   

}
