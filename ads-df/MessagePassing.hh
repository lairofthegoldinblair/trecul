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

#ifndef __MESSAGEPASSING_H
#define __MESSAGEPASSING_H

#include <thread>
#include "mpi.h"
#include "RuntimeOperator.hh"

class RuntimeMessageDemuxOperatorType : public RuntimeOperatorType
{
  friend class RuntimeMessageSendOperator;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
  }
public:
  RuntimeMessageDemuxOperatorType()
    :
    RuntimeOperatorType("RuntimeMessageDemuxOperatorType")
  {
  }
  ~RuntimeMessageDemuxOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
  }
};

/**
 * This operator demuxes asynchronous MPI requests
 */
class RuntimeMessageDemuxOperator : public RuntimeOperatorBase<RuntimeMessageDemuxOperatorType>
{
private:
  enum State { START, READ, WRITE };
  State mState;

  MPI_Request * mRequests;
  int * mIndexes;
  MPI_Status * mStatuses;
  class MessagePassingStatus * mMessageStatuses;
  int mOutcount;
  int mOutcountIt;
  RuntimePort * mActivePorts;
  std::map<RuntimePort *, std::size_t> mPortToIndex;
  int32_t mActiveRequests;
  // MPI doesn't support a timeout on MPI_Waitsome so we use a thread
  // and a generalized request to fake one out.
  bool mStopTimerThread;
  std::thread * mTimerThread;
  class ConcurrentQueue * mQueue;
  void wakeupTimer();
public:
  RuntimeMessageDemuxOperator(RuntimeOperator::Services& services, const RuntimeMessageDemuxOperatorType& ty);
  ~RuntimeMessageDemuxOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

/**
 * This operator manages a finite collection of buffers that
 * may be used for messaging.  For scalability reasons we cannot
 * allow every endpoint to have a dedicated buffer(s).
 * BufferPool input ports come in pairs: 
 * Even number input ports handle allocate requests
 * Odd number ports handle free requests
 * For each even/allocate input port idx there is a corresponding
 * output port at idx/2 that is for the completion of the allocate
 * request.
 */
class RuntimeBufferPoolOperatorType : public RuntimeOperatorType
{
  friend class RuntimeBufferPoolOperator;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
  }
public:
  RuntimeBufferPoolOperatorType()
    :
    RuntimeOperatorType("RuntimeBufferPoolOperatorType")
  {
  }
  ~RuntimeBufferPoolOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
  }
};

class RuntimeBufferPoolOperator : public RuntimeOperatorBase<RuntimeBufferPoolOperatorType>
{
private:
  enum State { START, READ, WRITE };
  State mState;
  RuntimePort * mActivePorts;
  std::map<RuntimePort *, std::size_t> mPortToIndex;  
  uint8_t * mAlloced;
  uint64_t mNumAlloced;
  uint64_t mNumFreed;
public:
  RuntimeBufferPoolOperator(RuntimeOperator::Services& services, const RuntimeBufferPoolOperatorType & );
  ~RuntimeBufferPoolOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

/**
 * This operator listens for sender data available requests.
 * It has a single input port which is for MPI recv completion events.
 * OutputPort 0 is for MPI recv requests.
 * OutputPort 1 is where availability message go.
 */
class RuntimeDataAvailableOperatorType : public RuntimeOperatorType
{
  friend class RuntimeDataAvailableOperator;
private:
  int mRank;
  int mTag;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mRank);
    ar & BOOST_SERIALIZATION_NVP(mTag);
  }
  RuntimeDataAvailableOperatorType()
    :
    RuntimeOperatorType("RuntimeDataAvailableOperatorType"),
    mRank(0),
    mTag(0)
  {
  }
public:
  RuntimeDataAvailableOperatorType(int rank, int tag)
    :
    RuntimeOperatorType("RuntimeDataAvailableOperatorType"),
    mRank(rank),
    mTag(tag)
  {
  }
  ~RuntimeDataAvailableOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
  }
};

class RuntimeDataAvailableOperator : public RuntimeOperatorBase<RuntimeDataAvailableOperatorType>
{
public:
  enum InputPort { DATA_RECV_COMPLETE=0 };
  enum OutputPort { DATA_RECV_REQUEST=0, RECORD_OUTPUT=1 };
private:
  enum State { START, READ, WRITE_0, WRITE_1, WRITE_EOF_0, WRITE_EOF_1 };
  State mState;
  int32_t * mBuffer;
  int32_t mSourceRank;
  int32_t mSourceTag;
  MPI_Request mRequest;
public:
  RuntimeDataAvailableOperator(RuntimeOperator::Services& services, const RuntimeDataAvailableOperatorType & ty);
  ~RuntimeDataAvailableOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

/**
 * MPIServiceRecvBuffer implements the receive side
 * buffering policy.  This policy is a simple hysteresis
 * in which the receiver stops reading at a certain buffer
 * size and then starts reading again at a lower (generally
 * non zero) size.
 * The operator has 2 input ports and 4 output ports.
 * Input Port 0 : This port receives completion of buffer available requests.
 * Messages arrive over this port only when the operator has an
 * outstanding request for a buffer.
 * Input Port 1 : This port receives completion of receive requests.
 * Messages arrive over this port only when the operator has
 * and outstanding async receive request.
 * Output Port 0 : This sends buffer allocate requests.
 * Output Port 1 : This sends receive requests.
 * Output Port 2 : This send buffer free requests.
 * Output Port 3 : This is the output port that the operator
 * writes deserialized records to.  Arbitrary dataflow operators
 * may connect to this port.
 * This port has to be a special kind of port because its associated
 * channel capacity is coming from the total amount reported through
 * data available requests.
 */
class RuntimeMessageReceiverOperatorType : public RuntimeOperatorType
{
  friend class RuntimeMessageReceiverOperator;
private:
  RecordTypeDeserialize mDeserialize;
  RecordTypeMalloc mMalloc;
  int mRank;
  int mTag;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mDeserialize);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mRank);
    ar & BOOST_SERIALIZATION_NVP(mTag);
  }
  RuntimeMessageReceiverOperatorType()
    :
    mRank(0),
    mTag(0)
  {
  }
public:
  RuntimeMessageReceiverOperatorType(const RecordTypeDeserialize & deserializeFunctor,
			  const RecordTypeMalloc & mallocFunctor,
			  int rank,
			  int tag)
    :
    RuntimeOperatorType("RuntimeMessageReceiverOperatorType"),
    mDeserialize(deserializeFunctor),
    mMalloc(mallocFunctor),
    mRank(rank),
    mTag(tag)
  {
  }
  ~RuntimeMessageReceiverOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
  }
};

class RuntimeMessageReceiverOperator : public RuntimeOperatorBase<RuntimeMessageReceiverOperatorType>
{
public:
  enum InputPort { BUFFER_ALLOC_COMPLETE=0, DATA_RECV_COMPLETE=1 };
  enum OutputPort { BUFFER_ALLOC_REQUEST=0, DATA_RECV_REQUEST=1, BUFFER_FREE_REQUEST=2, RECORD_OUTPUT=3 };
private:
  enum State { START, READ_BUFFER_COMPLETE, READ_RECV_COMPLETE,
	       WRITE, WRITE_BUFFER_REQUEST, WRITE_RECV_REQUEST, WRITE_BUFFER_FREE,
	       WRITE_RECORD_OUTPUT_EOF, WRITE_BUFFER_REQUEST_EOF, WRITE_RECV_REQUEST_EOF, WRITE_BUFFER_FREE_EOF};
  State mState;
  uint8_t * mBuffer;
  uint8_t * mBufferIt;
  uint8_t * mBufferEnd;
  int32_t mBufferSz;
  int mSourceRank;
  int mSourceTag;
  MPI_Request mRequest;
  RecordBuffer mRecordBuffer;
  RecordBufferIterator mRecordBufferIt;
  bool mIsDone;
  // int64_t mTotalAvailable;
  // int32_t mAvailableCap;
  // bool mAvailableClosed;
public:
  RuntimeMessageReceiverOperator(RuntimeOperator::Services& services, const RuntimeMessageReceiverOperatorType& ty);
  ~RuntimeMessageReceiverOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

/**
 * Input 0 : Send data available request completions arrive on this port.
 * Input 1 : Data records arrive here
 * Output 0 : Send data available requst completions arrive on this port.
 * Output 1 : Data records are send here.
 */
class RuntimeSendAvailableOperatorType : public RuntimeOperatorType
{
  friend class RuntimeMessageSendAvailableOperator;
private:
  int mRank;
  int mTag;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mRank);
    ar & BOOST_SERIALIZATION_NVP(mTag);
  }
  RuntimeSendAvailableOperatorType()
    :
    mRank(0),
    mTag(0)
  {
  }
public:
  RuntimeSendAvailableOperatorType(int rank,
				   int tag)
    :
    RuntimeOperatorType("RuntimeSendAvailableOperatorType"),
    mRank(rank),
    mTag(tag)
  {
  }
  ~RuntimeSendAvailableOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
  }
};

class RuntimeMessageSendAvailableOperator : public RuntimeOperatorBase<RuntimeSendAvailableOperatorType>
{
public:
  enum InputPort { DATA_AVAILABLE_COMPLETE=0, RECORD_INPUT=1 };
  enum OutputPort { DATA_AVAILABLE_REQUEST=0, RECORD_OUTPUT=1 };
private:
  enum State { START, READ, 
	       WRITE, WRITE_SEND_AVAILABLE_REQUEST,
	       WRITE_SEND_AVAILABLE_REQUEST_EOF };
  State mState;
  RecordBuffer mBuffer;
  int32_t mSendBuffer;
  int64_t mAccumulate;
  int mSourceRank;
  int mSourceTag;
  MPI_Request mRequest;
  RuntimePort * mActivePorts;
  bool isData(RuntimePort * port)
  {
    return port == getInputPorts()[1];
  }
  bool isSendAvailableRequestActive()
  {
    return mSendBuffer != std::numeric_limits<int32_t>::min();
  }
  void clearSendAvailableRequest()
  {
    mSendBuffer = std::numeric_limits<int32_t>::min();
  }

public:
  RuntimeMessageSendAvailableOperator(RuntimeOperator::Services & services, const RuntimeSendAvailableOperatorType & ty);
  ~RuntimeMessageSendAvailableOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

/**
 * SendOperator is responsible for serializing and sending
 * records.
 * Input 0 : Buffer allocation requests are completed.
 * Input 1 : Send data request completions arrive on this port.
 * Input 2 : Actual data arrives on this port.
 * Output 0 : Buffer allocation requests are initiated on this port.
 * Output 1 : Send data requests are initiated on this port.
 * Output 2 : Buffer free are initiated on this port.
 */
class RuntimeMessageSendOperatorType : public RuntimeOperatorType
{
  friend class RuntimeMessageSendOperator;
private:
  RecordTypeSerialize mSerialize;
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  int mRank;
  int mTag;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mSerialize);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mRank);
    ar & BOOST_SERIALIZATION_NVP(mTag);
  }
  RuntimeMessageSendOperatorType()
    :
    mRank(0),
    mTag(0)
  {
  }
public:
  RuntimeMessageSendOperatorType(const RecordTypeSerialize & serializeFunctor,
                                 const TreculFunctionReference & freeRef,
                                 const TreculRecordFreeRuntime & freeOp,
                                 int rank,
                                 int tag)
    :
    RuntimeOperatorType("RuntimeMessageSendOperatorType"),
    mSerialize(serializeFunctor),
    mFreeRef(freeRef),
    mFree(freeOp),
    mRank(rank),
    mTag(tag)
  {
    BOOST_ASSERT(!!mFree);
  }
  ~RuntimeMessageSendOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
  }
};

class RuntimeMessageSendOperator : public RuntimeOperatorBase<RuntimeMessageSendOperatorType>
{
public:
  enum InputPort { BUFFER_ALLOC_COMPLETE=0, DATA_SEND_COMPLETE=1, RECORD_INPUT=2 };
  enum OutputPort { BUFFER_ALLOC_REQUEST=0, DATA_SEND_REQUEST=1, BUFFER_FREE_REQUEST=2 };
private:
  enum State { START, READ_RECORD, READ_SEND_DATA_COMPLETE, READ_BUFFER_ALLOC_COMPLETE,
	       WRITE_BUFFER_ALLOC, WRITE_SEND_DATA_REQUEST, WRITE_BUFFER_FREE,
	       WRITE_BUFFER_ALLOC_EOF, WRITE_SEND_DATA_REQUEST_EOF, WRITE_BUFFER_FREE_EOF };
  State mState;
  uint8_t * mBuffer;
  uint8_t * mBufferIt;
  uint8_t * mBufferEnd;
  int32_t mBufferSz;
  int mSourceRank;
  int mSourceTag;
  MPI_Request mRequest;
  RecordBuffer mRecordBuffer;
  RecordBufferIterator mRecordBufferIt;
  bool mIsDone;
public:
  RuntimeMessageSendOperator(RuntimeOperator::Services & s, const RuntimeMessageSendOperatorType & ty);
  ~RuntimeMessageSendOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

#endif
