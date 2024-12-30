/**
 * Copyright (c) 2025, Akamai Technologies
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

#ifndef __MESSAGEPASSING_REMOTING_H
#define __MESSAGEPASSING_REMOTING_H

#include <map>
#include <vector>
#include <boost/program_options.hpp>
#include "RuntimeProcess.hh"

// The following are all MPI specific (or at least related
// to the simple flow control protocol we layer on top of MPI).
class RuntimeMessageDemuxOperatorType;
class RuntimeBufferPoolOperatorType;
class RuntimeDataAvailableOperatorType;
class RuntimeMessageReceiverOperatorType;
class RuntimeSendAvailableOperatorType;
class RuntimeMessageSendOperatorType;
class RemoteReceiveFifo;

class MessagePassingRemoting : public RuntimeProcess
{
private:
  // This operators go into a separate thread.
  RuntimeMessageDemuxOperatorType * mDemuxType;
  RuntimeOperator * mDemux;
  DataflowScheduler * mMPIScheduler;
 
  DataflowScheduler & getMPIScheduler();
  
  // Right now we are giving each partition that needs one
  // a buffer pool.  That will be the wrong thing in a model in which
  // we have multiple processes and multiple partitions within each process.
  RuntimeBufferPoolOperatorType * mBufferPoolType;
  std::map<int32_t, RuntimeOperator *> mBufferPool;
  RuntimeOperator& getBufferPool(int32_t partition);
 
  // These are privately allocated operator types that didn't come from
  // the plan.  TODO: Get rid of these.  We should not be requiring
  // that operators have associated operator types.
  std::vector<RuntimeDataAvailableOperatorType *> mDataAvailableTypes;
  std::vector<RuntimeMessageReceiverOperatorType *> mReceiverTypes;
  std::vector<RuntimeSendAvailableOperatorType *> mSendAvailableTypes;
  std::vector<RuntimeMessageSendOperatorType *> mSenderTypes;
 
  // These are used to connect DataAvailable and MessageReceiver operators
  // to the same operator (and ultimately to operators in another process).
  std::vector<RemoteReceiveFifo *> mRemoteReceiveChannels;
 
  void connectFromBufferPool(RuntimeOperator & op, int32_t inputPort, int32_t partition);
  void connectToBufferPool(RuntimeOperator & op, int32_t outputPort, int32_t partition);
  void connectFromDemux(RuntimeOperator & op, int32_t inputPort, int32_t partition);
  void connectToDemux(RuntimeOperator & op, int32_t outputPort, int32_t partition);
  void connectRemoteReceive(RuntimeOperator & sourceA, int32_t outputPortA,
 			    RuntimeOperator & sourceB, int32_t outputPortB,
 			    RuntimeOperator & target, int32_t inputPort,
 			    int32_t partition);
public:
  MessagePassingRemoting();
  ~MessagePassingRemoting(); 
  void addRemoteSource(const InterProcessFifoSpec& spec,
                       RuntimeOperator & sourceOp,
                       int32_t sourcePartition,
                       int32_t sourcePartitionConstraintIndex,
                       int32_t targetPartition,
                       int32_t targetPartitionConstraintIndex,
                       int32_t tag) override;
  void addRemoteTarget(const InterProcessFifoSpec& spec,
                       RuntimeOperator & targetOp,
                       int32_t targetPartition,
                       int32_t targetPartitionConstraintIndex,
                       int32_t sourcePartition,
                       int32_t sourcePartitionConstraintIndex,
                       int32_t tag) override;
  void runRemote(std::vector<std::shared_ptr<std::thread> >& threads) override;

  static bool isMPI();
  static int runMPI(boost::program_options::variables_map & vm);
};
  
#endif

