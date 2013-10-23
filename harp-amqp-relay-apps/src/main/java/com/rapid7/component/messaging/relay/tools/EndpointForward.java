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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.rapid7.component.messaging.relay.amqp.MessageRelayControl;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.RelayControlMessage.EndpointType;
import com.rabbitmq.client.*;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * A simple command line client to generate a forward request for the specified endpoint
 */
public class EndpointForward
{

   /**
    * @param args
    */
   public static void main(String[] args)
   {
            
      Options options = new Options();
      options.addOption("host", true, "AMQP Broker Hostname");
      options.addOption("e",true,"Exchange endpoint forward (as opposed to queue)");
      options.addOption("b",true,"Comma delimited list of exchange bindings to forward");
      options.addOption("c",false,"Cancel the specified forward");
      
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
      
      String endpoint = commandLine.getArgs()[0];
      String hostName = "localhost";
      boolean doExchange = false;
      EndpointType exchangeType = EndpointType.FANOUT;
      Set<String> bindings = null;
      boolean doCancel = false;
      if (commandLine.hasOption("host")) {
         hostName = commandLine.getOptionValue("host");
      }
      if (commandLine.hasOption("e")) {
         doExchange = true;
         String exchangeTypeString = commandLine.getOptionValue("e");
         if (exchangeTypeString.equalsIgnoreCase("direct")) exchangeType = EndpointType.DIRECT;
         else if (exchangeTypeString.equalsIgnoreCase("fanout")) exchangeType = EndpointType.FANOUT;
         else if (exchangeTypeString.equalsIgnoreCase("topic")) exchangeType = EndpointType.TOPIC;
         else if (exchangeTypeString.equalsIgnoreCase("headers")) exchangeType = EndpointType.HEADERS;
         else {
            System.err.println( "Invalid Exchange Type : " + exchangeTypeString );
            System.exit(1);
         }
      }
      if (commandLine.hasOption("b")) {
         bindings = new HashSet<String>();
         String bindingsListString = commandLine.getOptionValue("b");
         if (bindingsListString.lastIndexOf(',') == -1) {
            bindings.add(bindingsListString);
         } else {
            bindings.addAll(Arrays.asList(bindingsListString.split(",")));
         }
      }
      if (commandLine.hasOption("c")) doCancel = true;
      
      try {
         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(hostName);
         Connection connection = factory.newConnection();
         Channel channel = connection.createChannel();
         
         if (doExchange) {
            if (doCancel) {
               MessageRelayControl.requestExchangeForwardCancellation(channel, endpoint, bindings);
            } else {
               MessageRelayControl.requestExchangeForward(channel, endpoint, exchangeType, bindings, true);
            }
         } else {
            if (doCancel) {
               MessageRelayControl.requestQueueForwardCancellation(channel, endpoint);
            } else {
               channel.queueDeclare(endpoint, false, false, false, null);
               MessageRelayControl.requestQueueForward(channel, endpoint, true);
            }
         }
         
         Thread.sleep(250);
         System.exit(0);
         
      } catch (Exception e) {
         e.printStackTrace();
         System.exit(1);
      }
   }

}
