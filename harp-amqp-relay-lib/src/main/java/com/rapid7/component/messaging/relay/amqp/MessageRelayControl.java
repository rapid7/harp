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

import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.Command;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.Collection;

/**
 * A set of utility messages that can be used by clients to request forwarding/publication of 
 * local AMQP queues to a remote AMQP broker instance via the relay. Under the covers, these
 * methods generate AMQP messages containing RelayControlMessage instances and send them to
 * the special relay control queue, which is monitored by the message relay system.
 */
public class MessageRelayControl
{
   public static final String RELAY_CONTROL_EXCHANGE = "__relay_control";
   
   protected static void createControlExchange(Channel channel) throws IOException {
      channel.exchangeDeclare(RELAY_CONTROL_EXCHANGE, "fanout");
   }
   
   protected static void bindControlQueue(Channel channel, String queueName) throws IOException {
      createControlExchange(channel);
      channel.queueBind(queueName, RELAY_CONTROL_EXCHANGE, "");
   }
   
   protected static void removeControlQueue(Channel channel, String queueName) throws IOException {
      channel.queueUnbind(queueName, RELAY_CONTROL_EXCHANGE, "");
      channel.queueDelete(queueName);
   }
   
   protected static void sendCommandMessage(Channel channel, String queueName, Command command, 
         EndpointType type, Collection<String> bindings, boolean fullAck) throws IOException {
      RelayControlMessage.Builder builder = RelayControlMessage.newBuilder();
      builder.setCommand(command)
         .setEndpointType(type)
         .setEndpointName(queueName)
         .setFullAck(fullAck);
      if (bindings != null) {
         builder.addAllBindings(bindings);
      }
      MessageRelayControl.createControlExchange(channel);
      channel.basicPublish(RELAY_CONTROL_EXCHANGE, "", null, builder.build().toByteArray());
   }

   /**
    * Request forwarding of a local queue to the remote AMQP instance
    * @param channel A channel used to submit the forward request.
    * @param queueName The name of the queue to forward.
    * @param fullAck If true, require full end-to-end delivery acknowledgment of messages forwarded
    * from the source bus to the destination bus. If false, acknowledge messages as soon as they
    * are transmitted over HTTP. Full end-to-end acknowledgment is more robust, but requires ACK
    * messages to be sent back over the return channel, which doubles total message volume (although
    * the ACK messages are very short, so they should not consume much bandwidth).
    */
   public static void requestQueueForward(Channel channel, String queueName, boolean fullAck) {
      try {
         sendCommandMessage(channel,queueName,Command.PUBLISH,EndpointType.QUEUE, null, fullAck);
      } catch (IOException e) {
         System.err.println("Error while requesting queue forward for queue '" + queueName +"':");
         e.printStackTrace();
      }
      
   }
   
   /**
    * Cancel forwarding of a local queue to the remote AMQP instance
    * @param channel A channel used to submit the cancellation request.
    * @param queueName The name of the queue which should no longer be forwarded.
    */
   public static void requestQueueForwardCancellation(Channel channel, String queueName) {
      try {
         sendCommandMessage(channel,queueName,Command.UNPUBLISH,EndpointType.QUEUE, null, true);
      } catch (IOException e) {
         System.err.println("Error while requesting queue forward for queue '" + queueName +"':");
         e.printStackTrace();
      }
      
   }
   
   /**
    * Request forwarding of a local exchange to the remote AMQP instance
    * @param channel A channel used to submit the forward request.
    * @param exchangeName The name of the exchange to forward.
    * @param fullAck If true, require full end-to-end delivery acknowledgment of messages forwarded
    * from the source bus to the destination bus. If false, acknowledge messages as soon as they
    * are transmitted over HTTP. Full end-to-end acknowledgment is more robust, but requires ACK
    * messages to be sent back over the return channel, which doubles total message volume (although
    * the ACK messages are very short, so they should not consume much bandwidth).
    */
   public static void requestExchangeForward(Channel channel, String exchangeName, EndpointType type,
         Collection<String> bindings, boolean fullAck) {
      try {
         sendCommandMessage(channel,exchangeName,Command.PUBLISH, type , bindings, fullAck);
      } catch (IOException e) {
         System.err.println("Error while requesting exchange forward for exchange '" + exchangeName +"':");
         e.printStackTrace();
      }
      
   }
   
   /**
    * Cancel forwarding of a local exchange to the remote AMQP instance
    * @param channel A channel used to submit the cancellation request.
    * @param queueName The name of the exchange which should no longer be forwarded.
    */
   public static void requestExchangeForwardCancellation(Channel channel, String exchangeName,
         Collection<String> bindings) {
      try {
         sendCommandMessage(channel,exchangeName,Command.UNPUBLISH,EndpointType.FANOUT, bindings, true);
      } catch (IOException e) {
         System.err.println("Error while requesting exchange forward for exchange '" + exchangeName +"':");
         e.printStackTrace();
      }
      
   }
   
   
} 
