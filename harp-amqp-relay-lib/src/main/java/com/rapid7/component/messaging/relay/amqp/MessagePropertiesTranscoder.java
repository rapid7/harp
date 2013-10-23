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

import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.EncodedProperties;
import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.Header;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A utility class to encode/decode AMQP message properties as Protobuf.
 */
public class MessagePropertiesTranscoder
{
   
   public static EncodedProperties encode(BasicProperties properties) {
      
      Set<Header> headers = new HashSet<Header>();
      if (properties.getHeaders() != null) {
         for (String headerName : properties.getHeaders().keySet()) {
            headers.add(Header.newBuilder().setName(headerName)
               .setValue(properties.getHeaders().get(headerName).toString()).build());
         }
      }
      
      EncodedProperties.Builder propertiesBuilder = EncodedProperties.newBuilder();
      if (properties.getAppId() != null) propertiesBuilder.setAppID(properties.getAppId());
      if (properties.getContentEncoding() != null) propertiesBuilder.setContentEncoding(properties.getContentEncoding());
      if (properties.getContentType() != null) propertiesBuilder.setContentType(properties.getContentType());
      if (properties.getCorrelationId() != null) propertiesBuilder.setCorrelationId(properties.getCorrelationId());
      if (properties.getDeliveryMode() != null) propertiesBuilder.setDeliveryMode(properties.getDeliveryMode());
      if (properties.getExpiration() != null) propertiesBuilder.setExpiration(properties.getExpiration());
      if (properties.getMessageId() != null) propertiesBuilder.setMessageID(properties.getMessageId());
      if (properties.getPriority() != null) propertiesBuilder.setPriority(properties.getPriority());
      if (properties.getReplyTo() != null) propertiesBuilder.setReplyTo(properties.getReplyTo());
      if (properties.getTimestamp() != null) propertiesBuilder.setTimestamp(properties.getTimestamp().getTime());
      if (properties.getType() != null) propertiesBuilder.setType(properties.getType());
      if (properties.getUserId() != null) propertiesBuilder.setUserId(properties.getUserId());
      if (headers.size() > 0) propertiesBuilder.addAllHeader(headers);
      
      return propertiesBuilder.build();

   }
   
   public static AMQP.BasicProperties decode(EncodedProperties properties) {
      
      AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
      
      if (properties.hasAppID()) builder.appId(properties.getAppID());
      if (properties.hasContentEncoding()) builder.contentEncoding(properties.getContentEncoding());
      if (properties.hasContentType()) builder.contentType(properties.getContentType());
      if (properties.hasCorrelationId()) builder.correlationId(properties.getCorrelationId());
      if (properties.hasDeliveryMode()) builder.deliveryMode(properties.getDeliveryMode());
      if (properties.hasExpiration()) builder.expiration(properties.getExpiration());
      if (properties.hasMessageID()) builder.messageId(properties.getMessageID());
      if (properties.hasPriority()) builder.priority(properties.getPriority());
      if (properties.hasReplyTo()) builder.replyTo(properties.getReplyTo());
      if (properties.hasTimestamp()) builder.timestamp(new Date(properties.getTimestamp()));
      if (properties.hasType()) builder.type(properties.getType());
      if (properties.hasUserId()) builder.userId(properties.getUserId());
      if (properties.getHeaderCount() > 0) {
         Map<String,Object> headerMap = new HashMap<String,Object>();
         for (Header h : properties.getHeaderList()) {
            headerMap.put(h.getName(), h.getValue());
         }
         builder.headers(headerMap);
      }
      
      return builder.build();
      
      
   }

}
