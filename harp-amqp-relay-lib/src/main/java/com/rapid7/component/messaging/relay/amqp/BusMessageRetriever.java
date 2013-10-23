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
import com.rapid7.component.messaging.relay.MessageRetriever;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.*;
import com.google.protobuf.ByteString;
import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * A utility class used to retrieve messages from a set of monitored AMQP queues,
 * encode the retrieved messages (as Protobuf) and transmit the encoded messages
 * as a delimited binary stream.
 */
public class BusMessageRetriever implements MessageRetriever
{  
   
   private BusConnector m_connector;
   private Channel m_channel;
   private int m_channelID = 0;
   private Set<String> m_targetQueues = new HashSet<String>();
   private Set<String> m_fullAckQueues = new HashSet<String>();
   private Map<String,Set<String>> m_relayExchangeBindings = new HashMap<String, Set<String>>();
   private Map<String,RetrievalConsumer> m_retrievalConsumers = new HashMap<String,RetrievalConsumer>();
   private String m_clientID;
   private String m_relayControlQueue;
   protected MessageHandler m_currentHandler;
   protected int m_streamCount;
   protected int m_streamLimit;
   protected boolean m_terminationFlag;
   protected Deque<EncodedMessage> m_messageBuffer = new LinkedList<EncodedMessage>();
   
   /**
    * A RabbitMQ client API Consumer implementation that receives messages from a specific monitored
    * queue, encodes them, and writes them to the output stream.
    */
   private class RetrievalConsumer extends DefaultConsumer {
      
      private BusMessageRetriever m_retriever;
      private Channel m_channel;

      public RetrievalConsumer(Channel channel, BusMessageRetriever retriever) throws IOException {
         super(channel);
         m_channel = channel;
         m_retriever = retriever;
      }

      @Override
      public void handleCancel(String consumerTag) throws IOException {
         super.handleCancel(consumerTag);
      }

      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
         throws IOException {
         
         boolean fullAck = m_retriever.isFullAck(envelope.getRoutingKey());
                      
         // First encode the message as Protobuf
         EncodedMessage.Builder messageBuilder = EncodedMessage.newBuilder();
         EncodedEnvelope.Builder envelopeBuilder = EncodedEnvelope.newBuilder();
         envelopeBuilder.setDeliveryTag(envelope.getDeliveryTag());
         envelopeBuilder.setExchange(envelope.getExchange());
         envelopeBuilder.setRoutingKey(envelope.getRoutingKey());
         envelopeBuilder.setIsRedeliver(envelope.isRedeliver());
         messageBuilder.setEnvelope(envelopeBuilder.build());
      
         messageBuilder.setProperties(MessagePropertiesTranscoder.encode(properties));
         if (body != null) messageBuilder.setPayload(ByteString.copyFrom(body));
         if (fullAck) {
            messageBuilder.setDoAck(true);
            messageBuilder.setChannelID(m_retriever.getChannelID());
         }
         
         EncodedMessage em = messageBuilder.build();
         
          // Now handle transmission of the encoded message. We synchronize on the parent
          // BusMessageRetriever to prevent multiple instances from trying to write to
          // the output stream at the same time, which could result in mangled encoding.
         synchronized(m_retriever) {
            if (m_retriever.m_terminationFlag) {
               // We may have ended the HTTP response, so we need to buffer the messages since
               // the associated output stream could be closed at any time.
               m_retriever.m_messageBuffer.add(em);
            } else {

               // Handle the message
               m_retriever.m_currentHandler.handle(em);
               if (!fullAck) {
                  m_channel.basicAck(envelope.getDeliveryTag(), false);
               }
               
               // Increment the message counter, so we know when we have hit the
               // chunk size limit.
               m_retriever.m_streamCount++;
               if (m_retriever.m_streamCount >= m_retriever.m_streamLimit) {
                  // If we have hit the chunk size limit wake up the parent BusMessageRetriever
                  m_retriever.notifyAll();
               }
            }
         }
      }

      @Override
      public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
         super.handleShutdownSignal(consumerTag, sig);
      }
      
      
      
   }
   
   /**
    * Create a BusMessageRetriever using the supplied connection.
    * @param clientID The client on whose behalf this retriever instance is operating
    * @param connector The connection to use.
    * @throws IOException
    */
   public BusMessageRetriever(String clientID, BusConnector connector) throws IOException {
      m_clientID = clientID;
      m_connector = connector;
      m_channel = null;
      // Make sure we always add the relay control queue so we can handle
      // forwarding requests from clients.
      m_relayControlQueue = MessageRelayControl.RELAY_CONTROL_EXCHANGE + "_" + m_clientID;
      this.addTargetQueue(m_relayControlQueue,true,true);
      MessageRelayControl.bindControlQueue(connector.getControlChannel(), m_relayControlQueue);
   }
   
   
   @Override
   public synchronized void retrieve(MessageHandler handler, int limit, long timeout) throws IOException {
      
      long startTime = System.currentTimeMillis();
      
      // Set/reset the data elements that are shared by all of the RetrievalConsumer child threads.
      m_currentHandler = handler;
      m_streamCount = 0;
      m_streamLimit = limit;
      m_terminationFlag = false;
      Channel channel = getChannel();
      

      // Next, Deque & transmit any messages buffered during the previous request
      while (true) {
         EncodedMessage em = m_messageBuffer.pollFirst();
         if (em == null) break;
         m_currentHandler.handle(em);
         if (!em.getDoAck() && !AckHandler.isAckMessage(em)) {
            // Do immediate ack
            System.err.println("Doing immediate ack for " + em.getEnvelope().getExchange() +":" + em.getEnvelope().getRoutingKey()+":"+em.getEnvelope().getDeliveryTag());
            channel.basicAck(em.getEnvelope().getDeliveryTag(), false);
         }
         m_streamCount++;
      }
      
      // Abort if there are no target queues
      if (m_targetQueues.size() == 0) return;
      
      // Now, Start the RetrievalConsumers for each monitored queue and begin 
      /// pulling additional messages from the broker
      for (String queue : m_targetQueues) {
         RetrievalConsumer consumer = m_retrievalConsumers.get(queue);
         if (consumer == null) {
            channel.queueDeclare(queue, false, false, false, null);
            consumer = new RetrievalConsumer(channel,this);
            m_retrievalConsumers.put(queue, consumer);
         }
         try {
            channel.basicConsume(queue,false,queue,consumer);
         } catch (IOException e) {
            e.printStackTrace();
         }
      }
      
      long remainTime = timeout - (System.currentTimeMillis() - startTime);
      
      try {
         // Go to sleep for a time period up to the timeout
         if (remainTime > 0) this.wait(remainTime);
      } catch (InterruptedException e) {
      } finally {
         // Request shutdown the retrieval consumers. Note that they still
         // may receive some messages after the cancel command is sent, and
         // these "stray" messages will need to be buffered.
         if (channel.isOpen()) {
            for (RetrievalConsumer c : m_retrievalConsumers.values()) {
               channel.basicCancel(c.getConsumerTag());
            }
         }
         // Set the termination flag so the child RetrievalConsumers know to buffer
         // messages at this point.
         m_terminationFlag = true;
      }
        
      
   }
   
   /**
    * Add a target queue for monitoring/forwarding.
    * @param queueName The name of the queue to monitor.
    * @param addEndpoint If true, declare the queue.
    * @throws IOException
    */
   public synchronized void addTargetQueue(String queueName, boolean addEndpoint, boolean fullAck) 
            throws IOException {
      if (addEndpoint) m_connector.declareQueue(queueName);
      m_targetQueues.add(queueName);
      if (fullAck) {
         m_fullAckQueues.add(queueName);
      }
   }
   
   /**
    * Stop monitoring a target queue
    * @param queueName The name of the queue to stop monitoring
    * @param removeEndpoint If true, remove this queue (since it probably only exists to forward messages
    * to another broker).
    * @throws IOException
    */
   public synchronized void removeTargetQueue(String queueName, boolean removeEndpoint) throws IOException {
      if (queueName == m_relayControlQueue) {
         // Not allowed
         return;
      }
      m_retrievalConsumers.remove(queueName);
      m_targetQueues.remove(queueName);
      m_fullAckQueues.remove(queueName);
      if (removeEndpoint) m_connector.removeQueue(queueName);
   }
   
   /**
    * Get the unique relay queue name for the specified exchange and client.
    * @param exchangeName The name of the exchange supporting the relay queue.
    * @param clientID The unique ID of the client.
    * @return
    */
   protected String getRelayQueueForExchange(String exchangeName) {
      return exchangeName + "_RELAY_" + m_clientID;
   }
   
   /**
    * Add a target exchange to relay to the specified client. This will set up a fanout exchange
    * with the specified name (if it doesn't already exist), bind a unique relay queue for the client to
    * the exchange, and set up relaying of messages directed from the exchange to the relay queue.
    * This means that any messages directed to the exchange will be fanned out to all subscribing 
    * clients. It also means that this operation will fail if the specified exchange already exists 
    * and is not a fanout type exchange, so exchange forwarding needs to be used with care.
    * @param exchangeName The name of the exchange to forward/monitor.
    * @param clientID The unique ID of the client that is requesting to forward/monitor the exchange.
    * @throws IOException
    */
   public synchronized void addTargetExchange(String exchangeName, EndpointType type, 
         Collection<String> bindings, boolean fullAck) throws IOException {
      String relayQueueName = this.getRelayQueueForExchange(exchangeName);
      if (bindings != null) {
         Set<String> relayBindings = m_relayExchangeBindings.get(exchangeName);
         if (relayBindings == null) {
            relayBindings = new HashSet<String>();
            m_relayExchangeBindings.put(exchangeName, relayBindings);
         }
         relayBindings.addAll(bindings);
      }
      m_connector.declareExchangeRelay(exchangeName, relayQueueName, type, bindings);
      this.addTargetQueue(relayQueueName, false, fullAck);
   }
   
   /**
    * Cancels forwarding of the specified exchange to the specified client. Note that, while this
    * operation can remove the underlying relay queue (if removeEndpoint is true), it will not
    * remove the forwarded exchange itself, as there may be other clients listening to that exchange.
    * @param exchangeName The name of the forwarded/monitored exchange.
    * @param clientID The unique ID of the client.
    * @param removeEndpoint If true, remove the underlying relay queue endpoint.
    * @throws IOException
    */
   public synchronized void removeTargetExchange(String exchangeName, Collection<String> bindings,
         boolean removeEndpoint)
            throws IOException {
      String relayQueueName = this.getRelayQueueForExchange(exchangeName);
      m_connector.removeExchangeRelay(exchangeName, relayQueueName, bindings);
      boolean removeRelayQueue = true;
      if (bindings != null) {
         Set<String> relayBindings = m_relayExchangeBindings.get(exchangeName);
         if (relayBindings != null) {
            relayBindings.removeAll(bindings);
            if (relayBindings.size() > 0) removeRelayQueue = false;
         }
      }
      if (removeRelayQueue) this.removeTargetQueue(relayQueueName, removeEndpoint);
   }
   
   /**
    * Stop monitoring all target queues
    * @param removeEndpoints If true, remove the associated queue endpoints from the broker.
    * @throws IOException
    */
   public synchronized void removeAllTargets(boolean removeEndpoints) throws IOException {
      for (String queueName : m_targetQueues) {
         if (queueName == m_relayControlQueue) continue;
         m_retrievalConsumers.remove(queueName);
         if (removeEndpoints)  m_connector.removeQueue(queueName);
      }
      m_targetQueues.clear();
      m_fullAckQueues.clear();
      m_targetQueues.add(m_relayControlQueue);
      m_fullAckQueues.add(m_relayControlQueue);
   }
   
   /**
    * Close open channels to the broker
    * @throws IOException
    */
   public synchronized void close() throws IOException {
     if (m_channel != null) m_channel.close();
     m_channel = null;
   }
   
   public synchronized void dispose() throws IOException {
      this.close();
      MessageRelayControl.removeControlQueue(m_connector.getControlChannel(), m_relayControlQueue);
   }
   
   protected void sendAck(long deliveryTag, int channelID) throws IOException {
      
      EncodedMessage ackMessage = EncodedMessage.newBuilder()
               .setEnvelope(EncodedEnvelope.newBuilder()
                  .setDeliveryTag(deliveryTag)
                  .setExchange(AckHandler.ACK_DEST)
                  .setRoutingKey(AckHandler.ACK_DEST)
                  .build())
                  .setChannelID(channelID)
               .build();
      
      synchronized(this) {
         if (m_currentHandler != null && !m_terminationFlag) {
            m_currentHandler.handle(ackMessage);
         } else {
            m_messageBuffer.add(ackMessage);
         }
      }
      
   }

   protected synchronized void doAck(long deliveryTag, int channelID) throws IOException {
      Channel ackChannel = getChannel(channelID);
      if (ackChannel != null) {
         ackChannel.basicAck(deliveryTag, false);
      } else {
         System.err.println("Unable to ACK message " + deliveryTag + ". Channel ID mismatch.");
      }
   }
   
   public boolean isFullAck(String queueName) {
      return m_fullAckQueues.contains(queueName);
   }
   
   /**
    * A simple test
    */
   public static void main(String[] args) {
      try {

         BusConnector connector = BusConnector.getConnector();
         
         BusMessageRetriever m = connector.getRetriever("DEFAULT");
         m.addTargetQueue("testA",true,false);
         m.addTargetQueue("testB",true,false);
         
         final PipedOutputStream po = new PipedOutputStream();
         final PipedInputStream pi = new PipedInputStream(po);
         
         Runnable outputHandler = new Runnable() {
            
            public void run() {
               while(true) {
                  try {
                     EncodedMessage m = EncodedMessage.parseDelimitedFrom(pi);
                     if (m != null) {
                        System.out.println("   " + m.getEnvelope().getRoutingKey() + " < " + m.getPayload().toStringUtf8());
                     }
                  } catch (IOException e) {
                     e.printStackTrace();
                  }
               }
            }
         };
                  
         Thread outputThread = new Thread(outputHandler);
         outputThread.start();
         
         
         while (true) {
            System.out.println("Start retrieval block");
            m.retrieve(new OutputStreamHandler(po),3,10000);
            System.out.println("End retrieval block");
         }

      } catch (Exception e) {
         e.printStackTrace();
      }
   }


   protected Channel getChannel() throws IOException {
      if (m_channel == null || !m_channel.isOpen()) {
         if (m_channel != null && !m_channel.isOpen()) {
            System.err.println("Message retrieval channel for client " + this.m_clientID
               + " closed unexpectedly: " + m_channel.getCloseReason().getMessage());
         }
         m_channel = m_connector.getConnection().createChannel();
         m_channelID++;
         m_channel.basicQos(1);
      }
      return m_channel;

   }
   
   protected Channel getChannel(int channelID) {
      if (m_channelID == channelID && m_channel != null && m_channel.isOpen()) return m_channel;
      else return null;
   }
   
   protected int getChannelID() {
      return m_channelID;
   }

   

}
