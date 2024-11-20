/**
 * Copyright (c) 2024, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __TRECUL_SERVICE_COMPLETION_PORT_HH__
#define __TRECUL_SERVICE_COMPLETION_PORT_HH__

#include <mutex>

#include "RuntimePort.hh"

class DataflowScheduler;
class ServiceCompletionFifo;
class ServiceCompletionPort;

class ServiceCompletionFifo {
private:
  /**
   * Lock that protects mQueue.  This lock must be taken before taking any locks
   * on scheduler queues.
   */
  std::mutex mLock;
  /**
   * The actual fifo itself.
   */
  RuntimeFifo<RecordBuffer, 14> mQueue;
  // Total number of records that have entered this fifo (including those that
  // have left).
  uint64_t mRecordsRead;
  // Target is the output of this; used for making
  // the completion port on operator.
  ServiceCompletionPort * mTarget;
  // We need to be able to talk to the operator scheduler
  // to tell it about changes to my state.
  DataflowScheduler & mTargetScheduler;
public:
  ServiceCompletionFifo(DataflowScheduler & targetScheduler);
  ~ServiceCompletionFifo();

  /** 
   * The target for the fifo.
   */
  ServiceCompletionPort * getTarget()
  {
    return mTarget;
  }

  /**
   * Service can write to here (this is the "source"
   * of the fifo; no port necessary since services
   * are scheduled by DataflowScheduler).
   */
  void write(RecordBuffer buf);
  
  /**
   * Implement sync on behalf of the ports.
   */
  void sync(ServiceCompletionPort & port);

  /**
   * Move a chunk of data from the channel queue into the target port 
   * local cache.   Reach into the scheduler(s) associated with the ports and
   * notify them about the event.
   */
  void writeSomeToPort();

  /**
   * The number of elements currently in the channel.
   * Does not account for the number of elements in
   * read endpoint or write endpoints.  Note that it 
   * could safely account for the size of the target cache.
   * In fact that might be convenient when thinking of other
   * types of channels in which the size of the target cache
   * might be much larger (here I am thinking of a port
   * that serves as a proxy for a very large buffer in a different
   * thread or machine).
   * If the channel is not buffered then size is either 0 or
   * "infinity".
   */
  uint64_t getSize() 
  {
    return mQueue.getSize();
  }
};

/**
 * A ServiceCompletionPort is a port on which an operator
 * gets notification that an async service request has 
 * completed.
 */
class ServiceCompletionPort : public RuntimePort
{
private:
  ServiceCompletionFifo & mChannel;

public:
  ServiceCompletionPort(ServiceCompletionFifo& channel)
    :
    RuntimePort(RuntimePort::TARGET),
    mChannel(channel)
  {
  }

  ~ServiceCompletionPort()
  {
  }

  /**
   * Provide access to the fifo for writing (used
   * by the Service to send in completion.
   */
  ServiceCompletionFifo& getFifo() 
  {
    return mChannel;
  }

  /**
   * Transfer between local endpoint and the channel fifo.
   */
  void sync() { mChannel.sync(*this); }
  
  /**
   * Return the size of the attached channel.
   */
  uint64_t getSize() const 
  {
    return mChannel.getSize(); 
  }

  /**
   * The local buffer on this port.
   */
  RuntimePort::local_buffer_type & getLocalBuffer() 
  { 
    return mLocalBuffer; 
  }
};


#endif
