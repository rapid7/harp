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

import com.rabbitmq.client.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.commons.cli.*;

/**
 * 
 */
public class MessageGenerator
{

   /**
    * A simple command line client to generate textual AMQP messages and send them to the designated
    * AMQP endpoint. Useful for testing.
    */
   public static void main(String[] args)
   {
      
      Options options = new Options();
      options.addOption("host", true, "AMQP Broker Hostname");
      options.addOption("exchange", false, "Send to an exchange rather than a queue endpoint");
      options.addOption("routing", true, "Send with the supplied routing key");
      options.addOption("auto",false,"Automatically generate messages by incrementing an integer counter");
      options.addOption("throttle",true,"Add a delay of this many milliseconds between each sent message");
      
      CommandLineParser parser = new BasicParser();
      CommandLine commandLine = null;
      try {
          // parse the command line arguments
          commandLine = parser.parse( options, args );
      }
      catch( ParseException exp ) {
         new HelpFormatter().printHelp("<command> [OPTIONS] <endpoint>", options);
         System.exit(1);
      }
      
      String endpoint = commandLine.getArgs()[0];
      String exchange = "";
      boolean doExchange = false;
      String route = endpoint;
      String hostName = "localhost";
      if (commandLine.hasOption("host")) {
         hostName = commandLine.getOptionValue("host");
      }
      int throttleDelay = 0;
      if (commandLine.hasOption("throttle")) {
         throttleDelay = Integer.parseInt(commandLine.getOptionValue("throttle"));
      }
      if (commandLine.hasOption("exchange")) {
         exchange = endpoint;
         doExchange = true;
         if (commandLine.hasOption("routing")) route = commandLine.getOptionValue("routing");
         else route = "";
      }
      
      boolean auto = commandLine.hasOption("auto");
      
      try {
         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(hostName);
         Connection connection = factory.newConnection();
         Channel channel = connection.createChannel();
         
         if (!doExchange) channel.queueDeclare(endpoint, false, false, false, null);
         
         BufferedReader br = 
                  new BufferedReader(new InputStreamReader(System.in));
         
         for (int i = 0; true; i++) {
            String line;
            if (auto) line = "Message " + i;
            else line = br.readLine();
            if (line == null) continue;
            if (line.isEmpty()) break;
            channel.basicPublish(exchange, route, null, line.getBytes());
            if (throttleDelay > 0) Thread.sleep(throttleDelay);
         }
         
         channel.close();
         connection.close();
         
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

}
