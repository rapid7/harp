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

import java.io.IOException;
import java.util.Collection;

import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;

/**
 * A ControlMessageHandler concrete instance that intercepts encoded RelayControlMessage instances 
 * and uses them to configure forwarding of queues and exchanges for a specific message retriever.
 * Any other messages (not of type RelayControlMessage) are forwarded to the supplied downstream
 * MessageHandler instance.
 */
public class ControlMessageProcessor extends ControlMessageHandler
{
   
   private BusMessageRetriever m_controlRetriever;

   /**
    * Create a ControlMessageHandler instance.
    * @param controlRetriever The BusMessageRetriever to configure.
    * handlers in a chain. Otherwise, filter out the message after it has been processed.
    */
   public ControlMessageProcessor(BusMessageRetriever controlRetriever)
   {
      super(false);
      m_controlRetriever = controlRetriever;
   }

   @Override
   protected void handleQueuePublishRequest(String queueName, boolean fullAck){
      try {
	m_controlRetriever.addTargetQueue(queueName,true,fullAck);
    } catch (IOException e) {
	System.err.println("Error while publishing relay target queue '" + queueName + "': " + e.getMessage());
    }
   }

   @Override
   protected void handleQueueUnpublishRequest(String queueName) {
      try {
	m_controlRetriever.removeTargetQueue(queueName,true);
    } catch (IOException e) {
	System.err.println("Error while removing relay target queue '" + queueName + "': " + e.getMessage());
    }
   }

   @Override
   protected void handleExchangePublishRequest(String exchangeName, EndpointType type, 
         Collection<String> bindings, boolean fullAck) {
      try {
	m_controlRetriever.addTargetExchange(exchangeName,type, bindings, fullAck);
    } catch (IOException e) {
	System.err.println("Error while publishing relay target exchange '" + exchangeName + "': " 
		+ e.getMessage());
    }
   }

   @Override
   protected void handleExchangeUnpublishRequest(String exchangeName,
         Collection<String> bindings) {
      try {
	m_controlRetriever.removeTargetExchange(exchangeName, bindings, true);
    } catch (IOException e) {
	System.err.println("Error while removing relay target exchange '" + exchangeName + "': " 
		+ e.getMessage());
    }
   }

   @Override
   protected void handleUnpublishAllRequest() {
      try {
	m_controlRetriever.removeAllTargets(true);
    } catch (IOException e) {
	System.err.println("Error while removing all relay target endpoints.");
    }
   }


}
