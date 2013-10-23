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

package com.rapid7.component.messaging.relay.client.example;


import com.rapid7.component.messaging.relay.MessageHandlerChain;
import com.rapid7.component.messaging.relay.amqp.BusConnector;
import com.rapid7.component.messaging.relay.amqp.BusMessageRetriever;
import com.rapid7.component.messaging.relay.amqp.SecurityFilterManager;
import com.rapid7.component.messaging.relay.amqp.StandardHandler;
import com.rapid7.component.messaging.relay.client.MessageReceiverTask;
import com.rapid7.component.messaging.relay.client.MessageTransmitterTask;
import java.net.UnknownHostException;
import org.apache.commons.cli.*;



/**
 * A simple executable client for the message relay system. Demonstrates how to use
 * the client APIs to set up a pipeline to the server.
 */
public class SimpleRelayClient
{
   
   /**
    * Process command line arguments
    * @param args The command line argument array
    * @return
    */
   @SuppressWarnings("static-access")
   public CommandLine parseCommandLine(String[] args) {
      
      Options options = new Options();
      options.addOption(OptionBuilder.hasArg()
         .withArgName("host")
         .withDescription("Hostname of AMQP Broker to connect to")
         .withLongOpt("broker")
         .create("b"));
      options.addOption(OptionBuilder.hasArg()
         .withArgName("clientID")
         .withDescription("Client identifier to pass to the message relay server")
         .withLongOpt("client")
         .create("c"));
      
      CommandLineParser parser = new BasicParser();
      CommandLine commandLine = null;
      try {
          // parse the command line arguments
          commandLine = parser.parse( options, args );
      }
      catch( ParseException exp ) {
          new HelpFormatter().printHelp("<command> [OPTIONS] <relay-server-url>", options);
          System.exit(1);
      }
      
      return commandLine;
      
   }

   public static void main(String[] args)
   {
       SimpleRelayClient client = new SimpleRelayClient();
       client.run(args);
   }

    public void run(String[] args)
    {
      final String brokerName = "local-broker";
      final String serverName = "remote";
      String brokerHost = BusConnector.DEFAULT_HOST;
      String clientID;
      try {
         clientID = java.net.InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e1) {
         clientID = "simple-relay-client";
      }
      
      CommandLine commandLine = parseCommandLine(args);
      
      // Get command line overrides for the AMQP broker host spec
      // and the client identifier
      if (commandLine.hasOption("b")) {
         brokerHost = commandLine.getOptionValue("b");
      }
      
      if (commandLine.hasOption("c")) {
         clientID = commandLine.getOptionValue("c");
      }
      
      // The first non-option argument is treated as the message relay servlet URL
      String appURL = commandLine.getArgs()[0];
      
      // Create a connection to the local message bus
      BusConnector connector = BusConnector.createConnector(brokerName,brokerHost);
      
      // Create a Security Filter to ensure we're only getting valid messages
      SecurityFilterManager securityFilter = new SecurityFilterManager();
      
      try {
         
         // Now we create two threads - one to handle incoming messages and the
         // other to handle outgoing messages.

         //Relay outgoing messages to the server
         BusMessageRetriever localRetriever = connector.getRetriever(serverName);
         final MessageTransmitterTask transmitterTask = 
                  new MessageTransmitterTask(appURL,clientID,localRetriever,
                  	securityFilter.getRequestHander(serverName));
         final Thread transmitterThread = new Thread(transmitterTask);
         
         //Handle incoming messages from the server side
         MessageHandlerChain handlerChain = new MessageHandlerChain()
            .add(securityFilter.getFilter(serverName))
            .add(new StandardHandler(localRetriever,connector));
         final MessageReceiverTask receiverTask = 
                  new MessageReceiverTask(appURL,clientID,handlerChain);
         final Thread receiverThread  = new Thread(receiverTask);
         
         System.out.print("Starting Up...");
         transmitterThread.start();
         receiverThread.start();
         System.out.println(" Started.");
         
         // Create a shutdown task so we can clean up when the user kills the
         // client.
         Runnable shutdownTask = new Runnable() {
            @Override public void run() {
               System.out.println("\nShutting Down...");
               transmitterTask.stop();
               receiverTask.stop();
               try
               {
                  transmitterThread.join();
                  receiverThread.join();
                  BusConnector.deleteConnector(brokerName);
               }catch (InterruptedException e) {
                  System.out.println("Shutdown Interrupted");
               }
            }
         };
         final Thread shutdownThread = new Thread(shutdownTask);
         Runtime.getRuntime().addShutdownHook(shutdownThread);
         
         
      } catch (Exception e) {
         e.printStackTrace();
      }
      

   }

}
