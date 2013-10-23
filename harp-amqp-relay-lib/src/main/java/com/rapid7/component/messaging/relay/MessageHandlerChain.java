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

package com.rapid7.component.messaging.relay;

import com.rapid7.component.messaging.relay.encoding.MessageRelayEncoding.EncodedMessage;
import java.util.ArrayList;
import java.util.List;

/**
 * A class used to chain together a sequence of child MessageHandlers
 * into a single compound MessageHandler instance.
 */
public class MessageHandlerChain implements MessageHandler
{
   private List<MessageHandler> m_chain = new ArrayList<MessageHandler>();
   
   /**
    * Add a new MessageHandler instance to the end of the chain.
    * @param handler The MessageHandler child instance to add to the chain.
    * @return The updated MessageHandlerChain instance, so this method can be used builder-style.
    */
   public MessageHandlerChain add(MessageHandler handler) {
      m_chain.add(handler);
      return this;
   }

   @Override
   public boolean handle(EncodedMessage message) {
      for (MessageHandler handler : m_chain) {
         if (!(handler.handle(message))) return false;
      }
      return true;
   }


}
