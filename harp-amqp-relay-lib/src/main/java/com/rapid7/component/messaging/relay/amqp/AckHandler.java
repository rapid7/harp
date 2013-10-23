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
 * A MessageHandler instance that intercepts encoded AMQP Ack messages and executes the
 * ack operation on the specified BusMessageRetriever (thus closing an end-to-end message
 * acknowledgment chain).
 */
public class AckHandler implements MessageHandler
{
   
   public static String ACK_DEST = "__relay_ack_";
   
   private BusMessageRetriever m_ackRetriever;
   
   /**
    * Create a ControlMessageHandler instance.
    * @param clientID The uniqueID of the client on whose behalf the request is being made.
    * @param controlRetriever The BusMessageRetriever to configure.
    */
   public AckHandler(BusMessageRetriever ackRetriever) {
      m_ackRetriever = ackRetriever;
   }
   
   @Override
   public boolean handle(EncodedMessage message) {
      if (AckHandler.isAckMessage(message)) {
         // It's an ack message. Process it.
         try {
            m_ackRetriever.doAck(message.getEnvelope().getDeliveryTag(), message.getChannelID());
         } catch (IOException e) {
            System.err.println("Unable to complete end-to-end ack for message "
               + message.getEnvelope().getDeliveryTag() + " : " + e.getMessage());
         }
         return false;
      } else {
         // It's not an ack. Return true so it can be processed by downstream handlers.
         return true;
      }
   }
   
   public static boolean isAckMessage(EncodedMessage message) {
      return message.getEnvelope().getExchange().equals(ACK_DEST)
          || message.getEnvelope().getRoutingKey().equals(ACK_DEST);
   }


}
