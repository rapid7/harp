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

import com.rapid7.component.messaging.relay.amqp.OutputStreamHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.entity.AbstractHttpEntity;

/**
 * An HttpEntity which obtains content from a MessageRetriever instance.
 * @see MessageRetriever
 */
public class MessageRetrieverEntity extends AbstractHttpEntity
{

   private MessageRetriever m_retriever;
   private int m_chunkLimit;
   private long m_chunkTimeout;
   private MessageHandler m_additionalHandler = null;
   
   protected static ExecutorService sm_threadPool = Executors.newCachedThreadPool();

   public MessageRetrieverEntity(MessageRetriever retriever, int chunkLimit, long chunkTimeout) {
      m_retriever = retriever;
      m_chunkLimit = chunkLimit;
      m_chunkTimeout = chunkTimeout;
   }

   public MessageRetrieverEntity(MessageRetriever retriever, int chunkLimit, long chunkTimeout,
         MessageHandler additionalHandler) {
      this(retriever,chunkLimit,chunkTimeout);
      m_additionalHandler = additionalHandler;
   }

   @Override
   public InputStream getContent() throws IOException, IllegalStateException {
      // Create piped stream pair
      final PipedOutputStream po = new PipedOutputStream();
      final PipedInputStream pi = new PipedInputStream(po);

      // Set up output handler
      final MessageHandlerChain outputHandler = new MessageHandlerChain();
      if (m_additionalHandler != null) outputHandler.add(m_additionalHandler);
      outputHandler.add(new OutputStreamHandler(po));

      Runnable retrieveTask = new Runnable() {
         @Override public void run() {
            try {
               m_retriever.retrieve(outputHandler, m_chunkLimit, m_chunkTimeout);
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
      };
      sm_threadPool.submit(retrieveTask);
      return pi;
   }

   @Override
   public long getContentLength() {
      return -1;
   }

   @Override
   public boolean isRepeatable() {
      return false;
   }

   @Override
   public boolean isStreaming() {
      return true;
   }

   @Override
   public void writeTo(OutputStream os) throws IOException {
      final MessageHandlerChain outputHandler = new MessageHandlerChain();
      if (m_additionalHandler != null) outputHandler.add(m_additionalHandler);
      outputHandler.add(new OutputStreamHandler(os));
      m_retriever.retrieve(outputHandler, m_chunkLimit, m_chunkTimeout);
   }

}
