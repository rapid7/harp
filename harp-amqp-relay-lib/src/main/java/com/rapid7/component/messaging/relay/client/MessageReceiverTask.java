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
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.EncodedMessage;
import java.io.InputStream;
import java.net.URISyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

/**
 * A Runnable task implementation that can be used by clients to connect to
 * the server side of the HTTP(S) message relay pipeline and receive
 * and process incoming messages.
 */
public class MessageReceiverTask extends MessageRelayTask
{
      
   private HttpGet m_getRequest;
   private MessageHandler m_handler;
     
   /**
    * Create a new MessageReceiverTask
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param handler A MessageHandler instance that will be used to process incoming encoded messages from
    * the relay pipeline.
    * @param chunkLimit The maximum number of messages to retrieve with a single HTTP(S) request cycle.
    * @param chunkTimeout The maximum time limit to keep a single HTTP(S) request cycle open. It is important
    * not to set this timeout too high, or intervening HTTP(S) infrastructure (e.g. proxies) may
    * pre-emptively timeout the request, which can result in lost messages.
    * @throws URISyntaxException
    */
   public MessageReceiverTask(String targetURL, String clientID, MessageHandler handler, 
      int chunkLimit, long chunkTimeout) throws URISyntaxException {
      
      super(targetURL,clientID,chunkLimit,chunkTimeout);
      m_getRequest = new HttpGet(m_appURI);
      m_handler = handler;
      
   }
   
   
   /**
    * Create a new MessageReceiverTask with default chunking parameters
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param handler A MessageHandler instance that will be used to process incoming encoded messages from
    * the relay pipeline.
    * @throws URISyntaxException
    */
   public MessageReceiverTask(String targetURL, String clientID, MessageHandler handler)
            throws URISyntaxException {
      this(targetURL,clientID,handler,DEFAULT_CHUNK_LIMIT,DEFAULT_CHUNK_TIMEOUT);
   }



   @Override
   protected void doRequest() throws Exception {
      // Execute an http get to the relay servlet
      HttpResponse response = m_httpClient.execute(m_getRequest);
      HttpEntity entity = response.getEntity();
      if (entity != null) {
         // Get the input stream for the response
         InputStream instream = entity.getContent();
         try {
            while (true) {
               // Pull encoded messages out of the response stream
               EncodedMessage m = EncodedMessage.parseDelimitedFrom(instream);
               if (m == null) break;
               m_handler.handle(m);
            }
         } finally {
            instream.close();
         }
      }
   }
   

}
