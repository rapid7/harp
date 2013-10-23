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

import java.util.Collection;

import com.rapid7.component.messaging.relay.MessageHandler;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.EncodedMessage;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A MessageHandler instance that intercepts encoded RelayControlMessage instances and processes
 * them. This abstract base class simply interprets instructions from the message and invokes
 * abstract handler methods that must be overridden by concrete child classes.
 * Any other messages (not of type RelayControlMessage) are forwarded to the supplied downstream
 * MessageHandler instance.
 */
public abstract class ControlMessageHandler implements MessageHandler
{
   
   private boolean m_doForward;
   
   /**
    * Create a ControlMessageHandler instance.
    * @param doForward If true, allow forwarding of the relay control message to subsequent
    * handlers in a chain. Otherwise, filter out the message after it has been processed.
    */
   public ControlMessageHandler(boolean doForward) {
      m_doForward = doForward;
   }
   
   /**
    * Process a RelayControlMessage instance.
    * @param message
    */
   protected void parseControlMessage(EncodedMessage message) {
      if (message.hasPayload()) {
         try {
            RelayControlMessage control = RelayControlMessage.parseFrom(message.getPayload());
            boolean fullAck = control.getFullAck();
            switch (control.getCommand()) {
               case PUBLISH : 
                  if (control.hasEndpointName()) {
                     if (control.hasEndpointType()) {
                        switch(control.getEndpointType()) {
                           case QUEUE:
                              this.handleQueuePublishRequest(control.getEndpointName(),fullAck);
                              break;
                           default:
                              this.handleExchangePublishRequest(
                                    control.getEndpointName(),
                                    control.getEndpointType(),
                                    control.getBindingsList(),
                                    fullAck);
                              break;
                        }
                     } else {
                        // Default publish queue
                	 this.handleQueuePublishRequest(control.getEndpointName(),fullAck);
                     }
                  }
                  break;
               case UNPUBLISH : 
                  if (control.hasEndpointName()) {
                     if (control.hasEndpointType()) {
                        switch(control.getEndpointType()) {
                           case QUEUE:
                              this.handleQueueUnpublishRequest(control.getEndpointName());
                              break;
                           default:
                              this.handleExchangeUnpublishRequest(control.getEndpointName(),control.getBindingsList());
                              break;
                        }
                     } else {
                        // Default unpublish queue
                        this.handleQueueUnpublishRequest(control.getEndpointName());
                     }
                  }
                  break;
               case UNPUBLISH_ALL :
                  this.handleUnpublishAllRequest();
                  break;
            }
         } catch (InvalidProtocolBufferException e) {
            System.err.println("Invalid message transmitted to Relay Control queue.");
         }
      }
      
   }
   
   protected abstract void handleQueuePublishRequest(String queueName, boolean fullAck);
   protected abstract void handleQueueUnpublishRequest(String queueName);
   
   protected abstract void handleExchangePublishRequest(String exchangeName, EndpointType type, 
         Collection<String> bindings, boolean fullAck);
   protected abstract void handleExchangeUnpublishRequest(String exchangeName,
         Collection<String> bindings);
   
   protected abstract void handleUnpublishAllRequest();

   @Override
   public boolean handle(EncodedMessage message) {
      if (isControlMessage(message)) {
         // It's a RelayControlMessage. Process it.
         this.parseControlMessage(message);
         return m_doForward;
      } else {
         // It's not a RelayControlMessage. Return true so it can be passed to downstream handlers.
         return true;
      }
   }
   
   public static boolean isControlMessage(EncodedMessage message) {
       if (message.getEnvelope().hasRoutingKey() 
           && (message.getEnvelope().getExchange().equals(MessageRelayControl.RELAY_CONTROL_EXCHANGE)
           || message.getEnvelope().getRoutingKey().startsWith(MessageRelayControl.RELAY_CONTROL_EXCHANGE)))
	   return true;
       else return false;       
   }


}
