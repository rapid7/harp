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
 * A MessageHandler implementation that automatically handles forwarding of ReplyTo 
 * queues specified in incoming messages. This allows the AMQP RPC-style pattern to
 * work across the message relay transparently. All incoming messages are then passed to
 * a specified downstream Handler instance.
 */
public class ReplyToHandler implements MessageHandler
{
   private BusMessageRetriever m_responseRetriever;
   private boolean m_fullAck;
   
   /**
    * Create a new ReplyToHandler instance.
    * @param responseRetriever The BusMessageRetriever instance which will be used to retrieve and
    * forward the response message.
    * @param fullAck If true, require full end-to-end transfer acknowledgments for response messages.
    */
   public ReplyToHandler(BusMessageRetriever responseRetriever, boolean fullAck) {
      m_responseRetriever = responseRetriever;
      m_fullAck = fullAck;
   }

   /**
    * Create a new ReplyToHandler instance with full end-to-end acknowledgments.
    * @param responseRetriever The BusMessageRetriever instance which will be used to retrieve and
    * forward the response message.
    */
   public ReplyToHandler(BusMessageRetriever responseRetriever) {
      this(responseRetriever,true);
   }

   @Override
   public boolean handle(EncodedMessage message) {
      if (message.hasProperties() && message.getProperties().hasReplyTo()) {
         String replyQueue = message.getProperties().getReplyTo();
         try {
            // Add the ReplyTo queue as a monitored target queue in the BusMessageRetriever.
            m_responseRetriever.addTargetQueue(replyQueue, true, m_fullAck);
         } catch (IOException e) {
            System.err.println("Unable to handle reply-to queue forwarding: " + e.getMessage());
         }
      }
      // Pass the message downstream.
      return true;
   }


}
