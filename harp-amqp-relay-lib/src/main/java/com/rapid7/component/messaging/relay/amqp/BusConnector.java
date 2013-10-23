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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to simplify managing connections to an AMQP broker. Supports a centralized pool of 
 * multiple, named connection instances. Leverages the RabbitMQ AMQP client API, but should be compatible
 * with other AMQP-compliant brokers (like QPid).
 */
public class BusConnector
{
   public static final String DEFAULT_CONNECTOR = "DEFAULT";
   public static final String DEFAULT_HOST = "localhost";
   public static final int DEFAULT_PORT = -1;
      
   private static Map<String,BusConnector> sm_instanceMap = new HashMap<String,BusConnector>();
   
   private Connection m_connection;
   private Map<String,BusMessageRetriever> m_retrieverMap = new HashMap<String,BusMessageRetriever>();
   private TranscodingPublisher m_publisher;
   private String m_hostname;
   private int m_port;
   private String m_username;
   private String m_password;
   private Channel m_controlChannel;
   
   private BusConnector(String hostname, int port, String username, String password) {
      m_hostname = hostname;
      m_port = port;
      m_username = username;
      m_password = password;
   }
   
   /**
    * Get a BusMessageRetriever instance which can be used to monitor incoming messages on
    * a defined set of queues using this connection.
    * 
    * @param clientID A unique identifier for the specific client associated with this retriever.
    * This allows the connection to support retrievers for multiple clients (which may be forwarding
    * different endpoints).
    * @return The BusMessageRetriever instance for the specified client. 
    * @throws IOException
    */
   public BusMessageRetriever getRetriever(String clientID) throws IOException {

      BusMessageRetriever retriever = m_retrieverMap.get(clientID);
      if (retriever == null) {
         retriever = new BusMessageRetriever(clientID,this);
         m_retrieverMap.put(clientID, retriever);
      }
      
      return retriever;
   }
   
   /**
    * Get a TranscodingPublisher instance for this connection, which can
    * be used to publish encoded messages from the HTTP stream onto the bus.
    * @return The TranscodingPublisher instance associated with this connection.
    * @throws IOException
    */
   public TranscodingPublisher getPublisher() throws IOException {
      if (m_publisher == null) m_publisher = new TranscodingPublisher(this);
      return m_publisher;
   }
   
   /**
    * Get a BusConnector instance by name
    * @param instanceName The instance name for the BusConnector
    * @return The named BusConnector instance, or null if it has not yet been created.
    */
   public static BusConnector getConnector(String instanceName) {
      return sm_instanceMap.get(instanceName);
   }
   
   /**
    * Get the default BusConnector instance, which connector to localhost at the 
    * default port with no credentials. This is simply a convenience, as most production
    * scenarios will require a more complex connection specification.
    * @return The default BusConnector instance.
    */
   public static BusConnector getConnector() {
      BusConnector connector = BusConnector.getConnector(DEFAULT_CONNECTOR);
      if (connector == null) connector = BusConnector.createConnector(DEFAULT_CONNECTOR, DEFAULT_HOST);
      return connector;
   }
   
   /**
    * Create a new named BusConnector instance.
    * @param instanceName The name for the new instance, which should be unique
    * @param hostSpec The AMQP host to connect to. Host specifications have the syntax:<p>
    * {@code [<username>@]<host>[:<port>]}.
    * @param password If a username is specified in the hostSpec, a password for that user
    * can be supplied here, or null if there is no password.
    * @return The newly created BusConnector instance.
    */
   public static BusConnector createConnector(String instanceName,
      String hostSpec, String password) {
      
      int port = DEFAULT_PORT;
      String username = null;
      int userIndex = hostSpec.indexOf('@');
      if (userIndex > -1) {
         username = hostSpec.substring(0, userIndex);
         hostSpec = hostSpec.substring(userIndex+1);
      }
      int portIndex = hostSpec.indexOf(':');
      if (portIndex > -1) {
         port = Integer.parseInt(hostSpec.substring(portIndex+1));
         hostSpec = hostSpec.substring(0, portIndex);
      }
      
      if (sm_instanceMap.containsKey(instanceName)) {
         deleteConnector(instanceName);
      }
      BusConnector bc = new BusConnector(hostSpec,port,username,password);
      sm_instanceMap.put(instanceName, bc);
      return bc;
   }
   
   /**
    * Create a new named BusConnector without a connection password.
    * @param instanceName The name for the new instance, which should be unique
    * @param hostSpec The AMQP host to connect to. Host specifications have the syntax:<p>
    * {@code [<username>@]<host>[:<port>]}.
    * @return The newly created BusConnector instance.
    */
   public static BusConnector createConnector(String instanceName, String hostSpec) {
      return BusConnector.createConnector(instanceName,hostSpec,null);
   }
   
   /**
    * Close and remove the named BusConnector instance.
    * @param instanceName The name of the instance.
    */
   public static void deleteConnector(String instanceName) {
      BusConnector bc = sm_instanceMap.remove(instanceName);
      if (bc != null) {
         if (bc.m_connection != null) {
            try {
               for (BusMessageRetriever r : bc.m_retrieverMap.values()) {
                  r.dispose();
               }
               if (bc.m_controlChannel != null && bc.m_controlChannel.isOpen()) bc.m_controlChannel.close();
               bc.m_connection.close();
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
      }
   }

   /**
    * Get the AMQP Connection object for this BusConnector. Initializes the connection if necessary.
    * @return The live Connection object associated with this connector.
    * @throws IOException
    */
   public Connection getConnection() throws IOException {
      if (m_connection == null) {
         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(m_hostname);
         if (m_port > 0 ) factory.setPort(m_port);
         if (m_username != null) factory.setUsername(m_username);
         if (m_password != null) factory.setPassword(m_password);
         m_connection = factory.newConnection();
      }
      return m_connection;
   }
   
   /**
    * Get a special AMQP channel used to internally manage the broker and relay configurations.
    * @return The Channel.
    * @throws IOException
    */
   protected Channel getControlChannel() throws IOException {
      if (m_controlChannel == null || !m_controlChannel.isOpen()) {
         m_controlChannel = this.getConnection().createChannel();
      }
      return m_controlChannel;
   }
   
   /**
    * Declare a new queue using this connection.
    * @param queueName The name of the queue to declare.
    * @throws IOException
    */
   public void declareQueue(String queueName) throws IOException {
      this.getControlChannel().queueDeclare(queueName, false, false, false, null);
   }
   
   /**
    * Remove a queue using this connection
    * @param queueName The name of the queue to remove
    * @throws IOException
    */
   public void removeQueue(String queueName) throws IOException {
      this.getControlChannel().queueDelete(queueName);
   }
   
   /**
    * Set up an exchange relay by creating a fanout Exchange and binding the specified 
    * relay queue to it. This operation will fail if the Exchange already exists and is
    * not a fanout type exchange.
    * @param exchangeName The name of the exchange to set up.
    * @param relayQueueName The name of the relay queue to bind to the exchange.
    * @param type The type of the exchange to relay.
    * @param bindings A list of bindings (routing keys) to bind to the exchange
    * @throws IOException
    */
   public void declareExchangeRelay(String exchangeName, String relayQueueName, EndpointType type, 
         Collection<String> bindings) throws IOException {
      Channel channel = this.getControlChannel();
      channel.exchangeDeclare(exchangeName, exchangeTypeText(type));
      channel.queueDeclare(relayQueueName, false, false, false, null);
      if (bindings != null && bindings.size() > 0) {
         for (String binding : bindings) {
            channel.queueBind(relayQueueName, exchangeName, binding);
         }
      } else {
         channel.queueBind(relayQueueName, exchangeName, "");
      }
   }
   
   
   public void removeExchangeRelay(String exchangeName, String relayQueueName, 
         Collection<String> bindings) throws IOException {
      Channel channel = this.getControlChannel();
      if (bindings != null && bindings.size() > 0) {
         for (String binding : bindings) {
            channel.queueUnbind(relayQueueName, exchangeName, binding);
         }
      } else {
         channel.queueUnbind(relayQueueName, exchangeName, "");
      }
      
   }
   
   public static String exchangeTypeText(EndpointType type) {
      switch (type) {
         case DIRECT : return "direct";
         case FANOUT : return "fanout";
         case TOPIC : return "topic";
         case HEADERS : return "headers";
         default : return "fanout";
      }
   }


}
