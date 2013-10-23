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

import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.*;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;

/**
 * A utility class that decodes and publishes Protobuf-encoded messages onto the supplied
 * broker connection.
 */
public class TranscodingPublisher
{
   private BusConnector m_bus;
   private Channel m_channel;
   
   /**
    * Create a new TranscodingPublisher instance.
    * @param bus The connector instance for the broker on which messages should be published.
    */
   public TranscodingPublisher(BusConnector bus) {
      m_bus = bus;
   }
   
   /**
    * Publish an EncodedMessage instance onto the bus. Note that all of the necessary
    * destination information is already contained in the message metadata.
    * @param message The encoded message to decode and publish
    * @throws IOException
    */
   public void publish (EncodedMessage message) throws IOException {
      if (m_channel == null || !m_channel.isOpen()) {
         m_channel = m_bus.getConnection().createChannel();
      }
      
      AMQP.BasicProperties basicProps = null;
      if (message.hasProperties()) basicProps = MessagePropertiesTranscoder.decode(message.getProperties());
      
      byte[] payload = null;
      if (message.hasPayload()) payload = message.getPayload().toByteArray();
      
      m_channel.basicPublish(message.getEnvelope().getExchange(),
                             message.getEnvelope().getRoutingKey(),
                             basicProps,
                             payload);
   }
   

}
