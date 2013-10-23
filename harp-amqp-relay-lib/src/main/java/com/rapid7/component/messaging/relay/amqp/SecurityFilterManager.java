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

package com.rapid7.component.messaging.relay.amqp;

import com.rapid7.component.messaging.relay.MessageHandler;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.EncodedMessage;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class implements a relay-message filter that ensures that clients can only deliver
 * messages to endpoints (queues and exchanges) that have been explicitly forwarded from 
 * the destination bus. It provides a linked pair of MessageHandler implementations via
 * public get methods. The first is a request handler, which should be placed in the outbound 
 * message handling pipeline from the destination bus, that examines outgoing Relay Control 
 * messages for each client and tracks the endpoints that have been forwarded to that client.
 * The second is a filter, which should be placed in the inbound message handling pipeline,
 * that rejects any messages from a given client sent to endpoints that have not been explicitly
 * forwarded to that client (as determined by the matching request handler). This prevents
 * abuse of the relay system by clients who attempt to send messages to unexpected destinations.
 * @see SecurityFilterManager#getRequestHander(String)
 * @see SecurityFilterManager#getFilter(String)
 */
public class SecurityFilterManager
{
    private class ClientEndpoints {
	public Set<String> m_queues = new HashSet<String>();
	public Set<String> m_exchanges = new HashSet<String>();
    }
    
    private Map<String,ClientEndpoints> m_publishedEndpoints = new HashMap<String,ClientEndpoints>();
   
    private class RequestHandler extends ControlMessageHandler {

	private SecurityFilterManager m_manager;
	private String m_clientID;

	public RequestHandler(SecurityFilterManager manager, String clientID) {
	    super(true);
	    m_manager = manager;
	    m_clientID = clientID;
	}


	@Override
	protected void handleQueuePublishRequest(String queueName, boolean fullAck) {
	    m_manager.getClientEndpoints(m_clientID).m_queues.add(queueName);
	}

	@Override
	protected void handleQueueUnpublishRequest(String queueName) {
	    m_manager.getClientEndpoints(m_clientID).m_queues.remove(queueName);
	}

	@Override
	protected void handleExchangePublishRequest(String exchangeName, EndpointType type, 
         Collection<String> bindings, boolean fullAck) {
	    m_manager.getClientEndpoints(m_clientID).m_exchanges.add(exchangeName);
	}

	@Override
	protected void handleExchangeUnpublishRequest(String exchangeName,
         Collection<String> bindings) {
	    m_manager.getClientEndpoints(m_clientID).m_exchanges.remove(exchangeName);
	}

	@Override
	protected void handleUnpublishAllRequest() {
	    m_manager.getClientEndpoints(m_clientID).m_exchanges.clear();
	    m_manager.getClientEndpoints(m_clientID).m_queues.clear();
	}

    }

   private class MessageFilter implements MessageHandler {
      
      private SecurityFilterManager m_manager;
      private String m_clientID;
      
      public MessageFilter(SecurityFilterManager manager, String clientID) {
         m_manager = manager;
         m_clientID = clientID;
      }

      @Override
      public boolean handle(EncodedMessage message) {

	  // First, pass through any ControlMessages or ACKS
	  if (ControlMessageHandler.isControlMessage(message) || AckHandler.isAckMessage(message)) {
	      return true;
	  }

	  if (message.getEnvelope().hasExchange()) {
	      String exchangeDest = message.getEnvelope().getExchange();
	      if (!exchangeDest.isEmpty()) {
		  if (m_manager.getClientEndpoints(m_clientID).m_exchanges.contains(exchangeDest)) {
		      // This is a valid published exchange, forward the message
		      return true;
		  } else {
		      // This is an unknown exchange destination, filter out the message
		      System.err.println("Error: Client '" + m_clientID + "' attempted to relay message "
			      + "to non-published Exchange '" + exchangeDest + "'.");
		      return false;
		  }
	      }
	  }

	  if (message.getEnvelope().hasRoutingKey()) {
	      String queueDest = message.getEnvelope().getRoutingKey();
	      if (!queueDest.isEmpty()) {
		  if (m_manager.getClientEndpoints(m_clientID).m_queues.contains(queueDest)) {
		      // This is a valid published queue, forward the message
		      return true;
		  } else {
		      // This is an unknown queue destination, filter out the message
		      System.err.println("Error: Client '" + m_clientID + "' attempted to relay message "
			      + "to non-published Queue '" + queueDest + "'.");
		      return false;
		  }

	      }
	  }

	  // If we got to here we have an invalid message destination
	  System.err.println("Error: Client '" + m_clientID + "' sent relay message with invalid endpoint");
	  return false;

      }

   }
   
   private ClientEndpoints getClientEndpoints(String clientID) {
       ClientEndpoints endpoints = m_publishedEndpoints.get(clientID);
       if (endpoints == null) {
	   endpoints = new ClientEndpoints();
	   m_publishedEndpoints.put(clientID, endpoints);
       }
       return endpoints;
   }

   /**
    * Get the request handler for a specified client, used to track forwarding requests
    * to that client.
    * @param clientID The unique ID of the client. 
    * @return A MessageHandler instance that should be inserted into the outbound handler
    * chain for the local message bus.
    */
   public MessageHandler getRequestHander(String clientID) {
       return new SecurityFilterManager.RequestHandler(this, clientID);
   }
   
   /**
    * Get the security message filter for a specified client, used to filter out messages
    * from the client sent to invalid (i.e. not-explicitly-forwarded destinations).
    * @param clientID The unique ID of the client.
    * @return A MessageHandler instance that should be inserted into the publishing chain
    * for the local message bus.
    */
   public MessageHandler getFilter(String clientID) {
       return new SecurityFilterManager.MessageFilter(this, clientID);
   }

}
