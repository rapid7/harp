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

package com.rapid7.component.messaging.relay.tools;

import com.rapid7.component.messaging.relay.amqp.MessageRelayControl;
import com.rabbitmq.client.*;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * A simple command line client which monitors the specified AMQP queue and prints the contents of
 * incoming messages. Also sets up forwarding of the specified queue via the message relay system.
 */
public class MessageMonitor
{

   /**
    * @param args
    */
   public static void main(String[] args)
   {
            
      Options options = new Options();
      options.addOption("host", true, "AMQP Broker Hostname");
      
      CommandLineParser parser = new BasicParser();
      CommandLine commandLine = null;
      try {
          // parse the command line arguments
          commandLine = parser.parse( options, args );
      }
      catch( ParseException exp ) {
          System.err.println( "Invalid Command Line: " + exp.getMessage() );
          System.exit(1);
      }
      
      String queueName = commandLine.getArgs()[0];
      String hostName = "localhost";
      if (commandLine.hasOption("host")) {
         hostName = commandLine.getOptionValue("host");
      }
      
      try {
         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(hostName);
         Connection connection = factory.newConnection();
         Channel channel = connection.createChannel();
         
         channel.queueDeclare(queueName, false, false, false, null);
         
         // Request forwarding of the queue
         MessageRelayControl.requestQueueForward(channel, queueName, true);
         
         QueueingConsumer consumer = new QueueingConsumer(channel);
         channel.basicConsume(queueName, false, consumer);
         
         while (true) {
           QueueingConsumer.Delivery delivery = consumer.nextDelivery();
           String message = new String(delivery.getBody());
           System.out.println("> " + message);
           channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
         }
         
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

}
