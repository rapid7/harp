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
import java.io.IOException;

/**
 * A MessageHandler implementation that republishes Protobuf encoded messages
 * on to the specified AMQP message bus.
 */
public class MessageRepublisher implements MessageHandler
{
   private BusConnector m_bus;
   private BusMessageRetriever m_ack;
   
   /**
    * Create a MessageRepublisher instance.
    * @param bus The connector on which to republish received encoded messages.
    */
   public MessageRepublisher(BusConnector bus) {
      m_bus = bus;
      m_ack = null;
   }
   
   /**
    * Create a MessageRepublisher instance that does acknowledgments
    * @param bus The connector on which to republish received encoded messages.
    * @param ack A BusMessageRetriever instance used to handle transmission of
    * acknowledgment messages.
    */
   public MessageRepublisher(BusConnector bus, BusMessageRetriever ack) {
      this(bus);
      m_ack = ack;
   }
   
   @Override
   public boolean handle(EncodedMessage message) {
      try {
         // Get the TranscodingPublisher from the connection instance and publish the message.
         m_bus.getPublisher().publish(message);
         if (m_ack != null) {
            // Handle ack
            if (message.getDoAck()) {
               m_ack.sendAck(message.getEnvelope().getDeliveryTag(), message.getChannelID());
            }
         }
      } catch (IOException e) {
         System.err.println("Error while republishing message:");
         e.printStackTrace();
      }
      return true;
   }


}
