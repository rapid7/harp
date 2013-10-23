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

import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * An abstract Runnable task implementation for handling the client side of the message
 * relay HTTP(S) pipeline. Contains common task logic used for both message reception and
 * message transmission.
 * @see MessageReceiverTask
 * @see MessageTransmitterTask
 */
public abstract class MessageRelayTask implements Runnable
{
   
   public static String DEFAULT_RELAY_APP_PATH = "/harp-amqp-relay-web/relay";
   public static int DEFAULT_CHUNK_LIMIT = 100;
   public static long DEFAULT_CHUNK_TIMEOUT = 5000;
   
   public static long CONNECTION_FAIL_RETRY_PAUSE = 500;
   
   protected URI m_appURI;
   protected String m_clientID;
   protected int m_chunkLimit;
   protected long m_chunkTimeout;
   
   protected HttpClient m_httpClient;
   
   private boolean m_stop = false;
   
   /**
    * Create a new MessageRelayTask
    * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
    * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
    * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
    * to the servlet. Otherwise the supplied path will be used.
    * @param clientID A (unique) identifier for this client.
    * @param chunkLimit The maximum number of messages to retrieve with a single HTTP(S) request cycle.
    * @param chunkTimeout The maximum time limit to keep a single HTTP(S) request cycle open. It is important
    * not to set this timeout too high, or intervening HTTP(S) infrastructure (e.g. proxies) may
    * pre-emptively timeout the request, which can result in lost messages.
    * @throws URISyntaxException
    */
  public MessageRelayTask(String targetURL, String clientID, int chunkLimit, long chunkTimeout) 
            throws URISyntaxException {
      
      m_clientID = clientID;
      m_chunkLimit = chunkLimit;
      m_chunkTimeout = chunkTimeout;
      
      URI targetURI = URI.create(targetURL);
      
      String appPath = DEFAULT_RELAY_APP_PATH;
      String submittedPath = targetURI.getPath();
      if (submittedPath != null && !submittedPath.matches("^/\\S*$")) appPath = submittedPath;
      
      m_appURI = new URIBuilder()
      .setScheme(targetURI.getScheme())
      .setHost(targetURI.getHost())
      .setPort(targetURI.getPort())
      .setPath(appPath)
      .setParameter("client_id", m_clientID)
      .setParameter("chunk_limit", String.valueOf(m_chunkLimit))
      .setParameter("chunk_timeout", String.valueOf(m_chunkTimeout)).build();
      
      m_httpClient = new DefaultHttpClient();
      
   }
   
   
  
  /**
   * Create a new MessageRelayTask with default chunking parameters.
   * @param targetURL The URL of the MessageRelayServlet that sits on the server side of
   * the relay HTTP(S) connection. Note that if this URL has an empty path component, the
   * default relay application path ("/harp-amqp-relay-web/relay") will be used to connect
   * to the servlet. Otherwise the supplied path will be used.
   * @param clientID A (unique) identifier for this client.
   * @throws URISyntaxException
   */
   public MessageRelayTask(String targetURL, String clientID)
            throws URISyntaxException {
      this(targetURL,clientID,DEFAULT_CHUNK_LIMIT,DEFAULT_CHUNK_TIMEOUT);
   }
   
   /**
    * Handle a single request cycle. Must be overridden by child classes.
    * @throws Exception
    */
   protected abstract void doRequest() throws Exception;

   @Override
   public void run() {
      
      m_stop=false;
      
      try {
                          
         while (true) {
            
            // Keep executing request cycles until we get a stop request
            if (m_stop == true) break;
            try {
               this.doRequest();
            } catch (NoHttpResponseException | SocketException e) {
               // The server is unavailable. Hopefully temporarily. Wait a bit then retry
               Thread.sleep(CONNECTION_FAIL_RETRY_PAUSE);
            }
            
         }
      } catch (Exception e) {
         System.err.println("Message Receiver Task Exception:");
         e.printStackTrace();
      } finally {
         m_stop = false;
      }
   }
   
   /**
    * Stop/shutdown the relay task.
    */
   public void stop() {
      m_stop = true;
   }
   

}
