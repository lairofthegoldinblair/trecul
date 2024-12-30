#include <chrono>
#include <condition_variable>
// Replace with Boost.Log
#include <iostream>
#include <boost/assert.hpp>
#include "MessagePassing.hh"

class MessagePassingStatus
{
public:
  int count;
  int cancelled;
  int MPI_SOURCE;
  int MPI_TAG;
  int MPI_ERROR;  
};

// Functions for implementing an MPI generalized request
int query_fn(void * extra_state, MPI_Status * status)
{
  MPI_Status_set_elements(status, MPI_INT, 1);
  MPI_Status_set_cancelled(status, 0);
  status->MPI_SOURCE = MPI_UNDEFINED;
  status->MPI_TAG = MPI_UNDEFINED;
  return MPI_SUCCESS;
}

int free_fn(void * extra_state)
{
  return MPI_SUCCESS;
}

int cancel_fn(void * extra_state, int complete)
{
  if (!complete)
    MPI_Abort(MPI_COMM_WORLD, 99);
  return MPI_SUCCESS;
}

RuntimeOperator * RuntimeMessageDemuxOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeMessageDemuxOperator(s, *this);
}

class ConcurrentQueue
{
private:
  std::mutex mGuard;
  std::condition_variable mCondVar;
  std::deque<MPI_Request> mQueue;
public:
  ConcurrentQueue()
  {
  }
  ~ConcurrentQueue()
  {
  }
  
  MPI_Request dequeue()
  {
    std::unique_lock<std::mutex> lock(mGuard);
    while(mQueue.size() == 0) {
      mCondVar.wait(lock);
    }
    MPI_Request r = mQueue.back();
    mQueue.pop_back();
    mCondVar.notify_one();
    return r;
  }

  void enqueue(MPI_Request buf)
  {
    std::unique_lock<std::mutex> lock(mGuard);
    while(mQueue.size() >= 1400) {
      mCondVar.wait(lock);
    }
    mQueue.push_front(buf);
    mCondVar.notify_one();
  }
};

RuntimeMessageDemuxOperator::RuntimeMessageDemuxOperator(RuntimeOperator::Services& services, const RuntimeMessageDemuxOperatorType& ty)
  :
  RuntimeOperatorBase<RuntimeMessageDemuxOperatorType>(services, ty),
  mState(START),
  mRequests(NULL),
  mIndexes(NULL),
  mStatuses(NULL),
  mMessageStatuses(NULL),
  mOutcount(0),
  mActivePorts(NULL),
  mStopTimerThread(false),
  mTimerThread(NULL),
  mQueue(NULL)
{
}

RuntimeMessageDemuxOperator::~RuntimeMessageDemuxOperator()
{
  if (mTimerThread) {
    mStopTimerThread = true;
    mTimerThread->join();
    delete mTimerThread;
    mTimerThread = NULL;
  }
  delete [] mMessageStatuses;
  delete [] mStatuses;
  delete [] mIndexes;
  delete [] mRequests;
  delete mQueue;
}

void RuntimeMessageDemuxOperator::wakeupTimer()
{
  try {
    while(!mStopTimerThread) {
      MPI_Request save = mQueue->dequeue();
      // Check if we're done
      if (save == -1) break;
      // Give the request a bit of time...
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      // If there is a generalized request set.  Then kick it.
      // Save the request for debugging purposes.
      if (save != MPI_REQUEST_NULL) {
	int flag;
	MPI_Status status;
	// Check whether we have already completed this guy.  Don't free
	// the request though (use get_status rather than MPI_Test).
	MPI_Request_get_status(save, &flag, &status);
	if (0 == flag) {
	  MPI_Grequest_complete(save);
	} else {
	  std::cerr << "RuntimeMessageDemuxOperator::wakeupTimer received completed generalized request" << std::endl;
	}
      } else {
	std::cerr << "RuntimeMessageDemuxOperator::wakeupTimer received MPI_REQUEST_NULL" << std::endl;
      }
    } 
  }catch(std::exception& ex) {
    std::cerr << "Failure in wake up thread: " << ex.what() << std::endl;
  }
}

void RuntimeMessageDemuxOperator::start()
{
  mActiveRequests = 0;
  mRequests = new MPI_Request [getInputPorts().size() + 1];
  mIndexes = new int [getInputPorts().size() + 1];
  mStatuses = new MPI_Status[getInputPorts().size() + 1];
  mMessageStatuses = new MessagePassingStatus[getInputPorts().size() + 1];
  for (std::size_t i=0; i<=getInputPorts().size(); i++) {
    mRequests[i] = MPI_REQUEST_NULL;
    MPI_Status_set_error(&mStatuses[i], MPI_SUCCESS);
    MPI_Status_set_source(&mStatuses[i], 0);
    MPI_Status_set_tag(&mStatuses[i], 0);
    MPI_Status_set_cancelled(&mStatuses[i], 0);
    MPI_Status_set_elements(&mStatuses[i], MPI_BYTE, 0);
  }
  std::size_t idx = 0;
  for(std::vector<RuntimePort *>::iterator it = input_port_begin();
      it != input_port_end();
      ++it) {
    if (mActivePorts == NULL)
      mActivePorts = *it;
    else
      mActivePorts->request_link_after(**it);
    mPortToIndex[*it] = idx++;
  }
  mStopTimerThread = false;
  mQueue = new ConcurrentQueue();
  mTimerThread = new std::thread([this](){this->wakeupTimer();});
  mState = START;
  onEvent(NULL);
}

void RuntimeMessageDemuxOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(mActivePorts || mActiveRequests > 0) {
      // Collect all available requests out there and populate for waiting.  If
      // anyone has sent EOF then mark our closing bit.
      while(mActivePorts && !readWouldBlock(*mActivePorts)) {
	requestRead(*mActivePorts);
	mState = READ;
	return;
      case READ:
	RecordBuffer buf;
	read(port, buf);
	if (RecordBuffer::isEOS(buf)) {
	  mActivePorts = port->request_unlink();
	  //std::cout << "Demux read EOS port=" << mPortToIndex[port] << "; mActivePorts =" << mActivePorts << "; pid=" << getpid() << std::endl;
	} else {
	  std::size_t idx = mPortToIndex[port]; 
	  if (mRequests[idx] != MPI_REQUEST_NULL) {
	    std::string msg((boost::format("Don't allow multiple outstanding requests per remote MPI endpoint.  port=%1%") % idx).str());
	    std::cerr << msg.c_str() << std::endl;
	    throw std::runtime_error(msg);
	  }
	  if (idx >= getInputPorts().size()) {
	    throw std::runtime_error((boost::format("Creating request on invalid request index: %1%") % idx).str());
	  }
	  mRequests[idx] = *reinterpret_cast<MPI_Request *>(buf.Ptr);
	  mActiveRequests += 1;
	  //std::cout << "Demux read request on port=" << idx  << "; pid=" << getpid() << std::endl;
	}
      }

      // Check to make sure there is a wakeup request set before 
      // I go to sleep.  TODO: Do we need to protect this with a lock (or otherwise arrange for an atomic write)
      // because the wakeup lock might look at it?  It may very well
      // be that the setting of the MPI_Request slot is atomic by MPI_Grequest_start itself.
      if (mRequests[getInputPorts().size()] == MPI_REQUEST_NULL) {
	int ret = MPI_Grequest_start(query_fn, 
				     free_fn, 
				     cancel_fn, 
				     NULL, 
				     &mRequests[getInputPorts().size()]);
	if (ret != MPI_SUCCESS) {
	  throw std::runtime_error((boost::format("MPI_Grequest_start failed: "
						  "error=") % ret).str());
	}
	mActiveRequests += 1;
	mQueue->enqueue(mRequests[getInputPorts().size()]);
      }

      {
	 int ret = MPI_Waitsome(getInputPorts().size()+1, 
				mRequests, 
				&mOutcount, 
				mIndexes, 
				mStatuses);
	if (ret != MPI_SUCCESS) {
	  if (ret == MPI_ERR_IN_STATUS) {
	    std::string err = "MPI_Waitsome failed.  ";
	    for(int i=0; i<mOutcount; i++) {
	      MPI_Status & s(mStatuses[i]);
	      err += (boost::format("Detail:[Source=%1%,Tag=%2%,Error=%3%]; ") %
		      s.MPI_SOURCE %
		      s.MPI_TAG %
		      s.MPI_ERROR).str();
	    }
	    throw std::runtime_error(err);
	  } else { 
	    throw std::runtime_error((boost::format("MPI_Waitsome failed with error %1%") % ret).str());
	  }
	}
      }

      // Decrement active requests.
      BOOST_ASSERT(mOutcount <= mActiveRequests);
      mActiveRequests -= mOutcount;

      // For any real completed requests, write out the completion
      // notification.
      for(mOutcountIt=0; mOutcountIt<mOutcount; mOutcountIt++) {
	// Index requests through mIndexes, index status directly by i 
	// We must use the status on recv requests to find out how much
	// data was read.  Not sure if there is any harm in calling this 
	// for completion of MPI_Isend.
	// For the moment, we implement a bit of a hack and send the 
	// address of MPI_Status as the message.  This presumes that
	// the receiver will not make another call until it is done with
	// this status.
	// int cnt=0;
	// MPI_Get_count(&mStatuses[i], MPI_BYTE, &cnt);

	// Make sure request is cleared.
	BOOST_ASSERT(MPI_REQUEST_NULL == mRequests[mIndexes[mOutcountIt]]);

	// Ignore wake up call
	if(std::size_t(mIndexes[mOutcountIt]) < getInputPorts().size()) {
	  // std::cout << "Demux received request completion on port=" << mIndexes[mOutcountIt] <<
	  //   "; MPI_SOURCE=" << mStatuses[mOutcountIt].MPI_SOURCE <<
	  //   "; MPI_TAG=" << mStatuses[mOutcountIt].MPI_TAG <<
	  //   "; MPI_ERROR=" << mStatuses[mOutcountIt].MPI_ERROR << 
	  //   "; pid=" << getpid() << std::endl;
	  requestWrite(mIndexes[mOutcountIt]);
	  mState = WRITE;
	  return;
	case WRITE:
	  // std::cout << "Demux write request completion to port=" << mIndexes[mOutcountIt] << "; pid=" << getpid() << std::endl;
	  // I am flushing on each write because I don't want to accidentally have the last 
	  // completed request be the Grequest and then skip flushing altogether.
	  // TODO: Optimize this.
	  // Subtle point is that we need to send the status back to the caller.
	  // However due to the indexing of MPI_Status in MPI_Waitsome we can't
	  // guarantee that the MPI_Status slot associated with this call to MPI_Waitsome
	  // won't change.  We are currently assuming that clients cannot make multiple
	  // requests so we copy status info into a structure indexed by the request slot
	  // and pass that pointer back.  Of course we could heap allocate a struct but
	  // that seems unecessary.  The other advantage of this approach is that it is
	  // encapsulating a few more MPI details in the demuxer.
	  MPI_Get_count(&mStatuses[mOutcountIt], MPI_BYTE, &mMessageStatuses[mIndexes[mOutcountIt]].count);
	  MPI_Test_cancelled(&mStatuses[mOutcountIt], &mMessageStatuses[mIndexes[mOutcountIt]].cancelled);
          MPI_Status_get_source(&mStatuses[mOutcountIt], &mMessageStatuses[mIndexes[mOutcountIt]].MPI_SOURCE);
          MPI_Status_get_tag(&mStatuses[mOutcountIt], &mMessageStatuses[mIndexes[mOutcountIt]].MPI_TAG);
          MPI_Status_get_error(&mStatuses[mOutcountIt], &mMessageStatuses[mIndexes[mOutcountIt]].MPI_ERROR);
	  writeAndSync(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mMessageStatuses[mIndexes[mOutcountIt]])));
	} 
      }
    }

    mQueue->enqueue(-1);
    mStopTimerThread = true;
    mTimerThread->join();
    delete mTimerThread;
    mTimerThread = NULL;
  }  
}

void RuntimeMessageDemuxOperator::shutdown()
{
  delete mQueue;
  mQueue = nullptr;
}

RuntimeOperator * RuntimeBufferPoolOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeBufferPoolOperator(s, *this);
}

RuntimeBufferPoolOperator::RuntimeBufferPoolOperator(RuntimeOperator::Services& services, const RuntimeBufferPoolOperatorType & ty)
  :
  RuntimeOperatorBase<RuntimeBufferPoolOperatorType>(services,ty),
  mActivePorts(NULL),
  mNumAlloced(0),
  mNumFreed(0)
{
}

RuntimeBufferPoolOperator::~RuntimeBufferPoolOperator()
{
}
void RuntimeBufferPoolOperator::start()
{
  std::size_t idx = 0;
  for(std::vector<RuntimePort *>::iterator it = input_port_begin();
      it != input_port_end();
      ++it) {
    if (mActivePorts == NULL)
      mActivePorts = *it;
    else
      mActivePorts->request_link_after(**it);
    mPortToIndex[*it] = idx++;
  }
  mState=START;
  onEvent(NULL);
}

void RuntimeBufferPoolOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(mActivePorts) {
      requestRead(*mActivePorts);
      mState = READ;
      return;
    case READ:
      {
	// We actually maintain a pool of buffers since
	// these are very large and malloc will be hitting
	// mmap or something expensive to satisfy such allocations.
	
	{
	  std::size_t idx = mPortToIndex[port];
	  RecordBuffer buf;
	  read(port, buf);
	  if (RecordBuffer::isEOS(buf)) {
	    mActivePorts = port->request_unlink();
	    continue;
	  } 
	  if (idx % 2 == 1) {
	    // Free
	    delete [] buf.Ptr;
	    mNumFreed -= 1;
	    continue;
	  } else {
	    // Malloc
	    mAlloced = new uint8_t [*reinterpret_cast<int32_t*>(buf.Ptr)];
	    mNumAlloced += 1;
	    requestWrite(idx/2);
	  }
	}
	mState = WRITE;
	return;
      case WRITE:
	write(port, RecordBuffer(mAlloced), true);
	mAlloced = NULL;
      }
    }
  }
}

void RuntimeBufferPoolOperator::shutdown()
{
}

RuntimeOperator * RuntimeDataAvailableOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeDataAvailableOperator(s, *this);
}

RuntimeDataAvailableOperator::RuntimeDataAvailableOperator(RuntimeOperator::Services& services, const RuntimeDataAvailableOperatorType & ty)
  :
  RuntimeOperatorBase<RuntimeDataAvailableOperatorType>(services,ty),
  mState(START),
  mBuffer(NULL),
  mSourceRank(ty.mRank),
  mSourceTag(ty.mTag),
  mRequest(MPI_REQUEST_NULL)
{
}

RuntimeDataAvailableOperator::~RuntimeDataAvailableOperator()
{
}

void RuntimeDataAvailableOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeDataAvailableOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      // Make an MPI receive request and push that request to the listener.
      mBuffer = new int32_t [1];
      *mBuffer = -1;
      {
	// std::cout << "RuntimeDataAvailableOperator initiate data available message mSourceRank=" << mSourceRank <<
	//   "; mSourceTag=" << mSourceTag << "; pid=" << getpid() << std::endl;
	int ret = MPI_Irecv(mBuffer, 4, MPI_BYTE, mSourceRank, mSourceTag, MPI_COMM_WORLD, &mRequest);
	if (ret != MPI_SUCCESS)
	  throw std::runtime_error((boost::format("RuntimeDataAvailableOperator::onEvent MPI_Irecv failed.  Error=%1%") % ret).str());
      }
      requestWrite(DATA_RECV_REQUEST);
      mState = WRITE_0;
      return;
    case WRITE_0:
      write(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mRequest)), true);
      // Wait for callback
      requestRead(DATA_RECV_COMPLETE);
      mState = READ;
      return;
    case READ:
      // We got an MessagePassingStatus back.  Make sure there isn't a short read.
      {
	RecordBuffer buf;
	read(port, buf);
	BOOST_ASSERT(mSourceRank==reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_SOURCE);
	BOOST_ASSERT(mSourceTag==reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_TAG);
	BOOST_ASSERT(4==reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->count);
      }
      // TODO: Should assert that we get our request pointer back.
      // TODO: What to do about an error on this tag?
      // The protocol is that we receive a 0 payload in the data available message
      // to signal the last message.
      if (*mBuffer <= 0) {
	delete [] mBuffer;
	mBuffer = NULL;
	// Make downstream aware that we are closing up shop
	requestWrite(DATA_RECV_REQUEST);
	mState = WRITE_EOF_0;
	return;
      case WRITE_EOF_0:
	write(port, RecordBuffer(NULL), true);
	requestWrite(RECORD_OUTPUT);
	mState = WRITE_EOF_1;
	return;
      case WRITE_EOF_1:
	write(port, RecordBuffer(NULL), true);
	break;
      }
      // Transfer the buffer out of here.
      requestWrite(RECORD_OUTPUT);
      mState = WRITE_1;
      return;
    case WRITE_1:
      write(port, RecordBuffer(reinterpret_cast<uint8_t *>(mBuffer)), true);
      mBuffer = NULL;      
    }
  }
}

void RuntimeDataAvailableOperator::shutdown()
{
}

RuntimeOperator * RuntimeMessageReceiverOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeMessageReceiverOperator(s, *this);
}

RuntimeMessageReceiverOperator::RuntimeMessageReceiverOperator(RuntimeOperator::Services& services, const RuntimeMessageReceiverOperatorType& ty)
  :
  RuntimeOperatorBase<RuntimeMessageReceiverOperatorType>(services, ty),
  mState(START),
  mBuffer(NULL),
  mBufferEnd(NULL),
  mBufferSz(128*1024),
  mSourceRank(ty.mRank),
  mSourceTag(ty.mTag),
  mRequest(MPI_REQUEST_NULL),
  mIsDone(false)
{
}

RuntimeMessageReceiverOperator::~RuntimeMessageReceiverOperator()
{
}
  
void RuntimeMessageReceiverOperator::start()
{
  mIsDone = false;
  mState = START;
  onEvent(NULL);
}

void RuntimeMessageReceiverOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(!mIsDone) {
      
      // // Always try to read a bunch of availability messages as possible so that we have
      // // an accurate picture of what is on the other side of the channel.  Avoid extreme 
      // // starvation of other requests though.
      // mAvailabilityCap=10;
      // while(!mAvailableClosed &&
      // 	    mAvailabilityCap-- > 0 &&
      // 	    !readWouldBlock(0)) {
      // 	requestRead(0);
      // 	mState = READ_AVAILABLE;
      // 	return;
      // case READ_AVAIALBLE:
      // 	{
      // 	  RecordBuffer buf;
      // 	  read(port, buf);
      // 	  if (RecordBuffer::isEOS(buf)) {
      // 	    mAvailableClosed = true;
      // 	    break;
      // 	  }
      // 	  mTotalAvailable += *reinterpret_cast<int32_t *>(buf.Ptr);
      // 	  delete [] buf.Ptr;
      // 	}
      // }
      // // TODO: If we have mTotalAvailable==0 then we should just do a blocking read on available.
      
      // TODO: Figure out how to propagate mTotalAvailable to the connect channel.

      // Get a buffer for reading.
      // std::cout << "RuntimeMessageReceiverOperator requesting buffer from pid=" << getpid() << std::endl;

#ifdef USE_BUFFER_POOL_OPERATOR
      //////////////////////////////////////////////////////////////
      /// Here is code for interfacing with the BufferPoolOperator
      //////////////////////////////////////////////////////////////
      requestWrite(BUFFER_ALLOC_REQUEST);
      mState = WRITE_BUFFER_REQUEST;
      return;
    case WRITE_BUFFER_REQUEST:
      {
    	write(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mBufferSz)), true);
      }
      requestRead(BUFFER_ALLOC_COMPLETE);
      mState = READ_BUFFER_COMPLETE;
      // std::cout << "RuntimeMessageReceiverOperator waiting for buffer request from pid=" << getpid() << std::endl;
      return;
    case READ_BUFFER_COMPLETE:
      {
    	RecordBuffer buf;
    	read(port, buf);
    	mBuffer = buf.Ptr;
    	// For debugging.
    	memset(mBuffer, 0, mBufferSz);
      }
      // std::cout << "RuntimeMessageReceiverOperator received buffer from pid=" << getpid() << std::endl;
#else
    	mBuffer = new uint8_t [mBufferSz];
    	// For debugging.
    	memset(mBuffer, 0, mBufferSz);
#endif

      // Note that we haven't checked whether the send really has anything.
      // As a result we are using a buffer that we may not in fact need.
      // TODO: Get the data available messages in this operator so that
      // it can optimize the memory utilization.
      {
	// std::cout << "RuntimeMessageReceiverOperator sending write request to demux from mSourceRank=" << mSourceRank << "; mSourceTag=" <<
	//   mSourceTag << "; pid=" << getpid() << std::endl;
	int ret = MPI_Irecv(mBuffer, mBufferSz, MPI_BYTE, mSourceRank, mSourceTag, MPI_COMM_WORLD, &mRequest);
	if (ret != MPI_SUCCESS)
	  throw std::runtime_error((boost::format("RuntimeMessageReceiverOperator::onEvent MPI_Irecv failed.  Error=%1%") % ret).str());
      }
      requestWrite(DATA_RECV_REQUEST);
      mState = WRITE_RECV_REQUEST;
      return;
    case WRITE_RECV_REQUEST:
      write(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mRequest)), true);
      // std::cout << "RuntimeMessageReceiverOperator sent write request to demux from pid=" << getpid() << std::endl;
      requestRead(DATA_RECV_COMPLETE);
      mState = READ_RECV_COMPLETE;
      return;
    case READ_RECV_COMPLETE:
      {
	RecordBuffer buf;
	read(port, buf);
	int cnt=reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->count;
	// This should really never happen.  Perhaps we could try to read again
	// instead of bailing out?
	if (cnt == 0)
	  throw std::runtime_error("Zero length message received");
	mIsDone = (mBuffer[0] == 0x01);
	// std::cout << "RuntimeMessageReceiverOperator received message size=" << cnt << 
	//   "; mIsDone=" << mIsDone << 
	//   "; MPI_SOURCE=" << reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_SOURCE << 
	//   "; MPI_TAG=" << reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_TAG << 
	//   "; MPI_ERROR=" << reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_ERROR << 
	//   "; pid=" << getpid() << std::endl;
	BOOST_ASSERT(mSourceRank==reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_SOURCE);
	BOOST_ASSERT(mSourceTag==reinterpret_cast<MessagePassingStatus *>(buf.Ptr)->MPI_TAG);
	// Skip over header.
	mBufferIt = mBuffer+1;
	mBufferEnd = mBuffer + cnt;
      }
      
      // Deserialize records, write them and return buffer
      // TODO: Handling EOS
      while(mBufferIt < mBufferEnd) {
	if (mRecordBuffer.Ptr == NULL) {
	  mRecordBuffer = getMyOperatorType().mMalloc.malloc();
	  mRecordBufferIt.init(mRecordBuffer);
	}
	if(getMyOperatorType().mDeserialize.Do(mBufferIt, mBufferEnd, mRecordBufferIt, mRecordBuffer)) {
	  requestWrite(RECORD_OUTPUT);
	  mState = WRITE;
	  return;
	case WRITE:
	  write(port, mRecordBuffer, false);
	  mRecordBuffer = RecordBuffer(NULL);
	  mRecordBufferIt.clear();
	} 
      }

#ifdef USE_BUFFER_POOL_OPERATOR
      // Free the buffer 
      requestWrite(BUFFER_FREE_REQUEST);
      mState = WRITE_BUFFER_FREE;
      return;
    case WRITE_BUFFER_FREE:
      write(port, RecordBuffer(mBuffer), true);
      mBuffer = mBufferEnd = NULL;
#else
      delete [] mBuffer;
      mBuffer = mBufferEnd = NULL;
#endif
    }

    // std::cout << "RuntimeMessageReceiverOperator sending EOS to downstream in pid=" << getpid() <<std::endl;
    // If this was the last buffer then write EOS to all necessary ports

    // Close Buffer Pool alloc port
    requestWrite(BUFFER_ALLOC_REQUEST);
    mState = WRITE_BUFFER_REQUEST_EOF;
    return;
  case WRITE_BUFFER_REQUEST_EOF:
    write(port, RecordBuffer(NULL), true);
	
    // Close Demux receive request port
    requestWrite(DATA_RECV_REQUEST);
    mState = WRITE_RECV_REQUEST_EOF;
    return;
  case WRITE_RECV_REQUEST_EOF:
    write(port, RecordBuffer(NULL), true);

    // Close Buffer Pool Free
    requestWrite(BUFFER_FREE_REQUEST);
    mState = WRITE_BUFFER_FREE_EOF;
    return;
  case WRITE_BUFFER_FREE_EOF:
    write(port, RecordBuffer(NULL), true);
    
    // Close up data output.
    requestWrite(RECORD_OUTPUT);
    mState = WRITE_RECORD_OUTPUT_EOF;
    return;
  case WRITE_RECORD_OUTPUT_EOF:
    write(port, RecordBuffer(NULL), true);
  }
}

void RuntimeMessageReceiverOperator::shutdown()
{
}

RuntimeOperator * RuntimeSendAvailableOperatorType::create(RuntimeOperator::Services& s) const
{
  return new RuntimeMessageSendAvailableOperator(s, *this);
}

RuntimeMessageSendAvailableOperator::RuntimeMessageSendAvailableOperator(RuntimeOperator::Services & services, const RuntimeSendAvailableOperatorType & ty)
  :
  RuntimeOperatorBase<RuntimeSendAvailableOperatorType>(services, ty),
  mSendBuffer(std::numeric_limits<int32_t>::min()),
  mAccumulate(0),
  mSourceRank(ty.mRank),
  mSourceTag(ty.mTag)
{
}

RuntimeMessageSendAvailableOperator::~RuntimeMessageSendAvailableOperator()
{
}

void RuntimeMessageSendAvailableOperator::start()
{
  clearSendAvailableRequest();
  mActivePorts = getInputPorts()[0];
  mActivePorts->request_link_after(*getInputPorts()[1]);
  mState = START;
  onEvent(NULL);
}

void RuntimeMessageSendAvailableOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(mActivePorts) {
      // Read a record and increment counter.  If counter hits some
      // batching factor, then initiate a send of that count to 
      // the receive end.  Pass data record downstream to operator
      // that actually sends the record.
      requestRead(*mActivePorts);
      mState = READ;
      return;
    case READ:
      read(port, mBuffer);
      if (isData(port)) {
	if (!RecordBuffer::isEOS(mBuffer)) {
	  mAccumulate += 1;
	} else {
	  //std::cout << "RuntimeMessageSendAvailableOperator EOS seen on input: unlinking port in pid=" << getpid() << std::endl;
	  mActivePorts = port->request_unlink();
	  BOOST_ASSERT (NULL != mActivePorts);
	}
	requestWrite(RECORD_OUTPUT);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mBuffer, RecordBuffer::isEOS(mBuffer));
      } else {
	// We are done if we have seen EOS and we just sent a zero length available.
	bool done = mActivePorts->request_count()==1 && mSendBuffer==0;
	// Make a note that there is no longer an active request.
	clearSendAvailableRequest();
	// If done, unlink 
	if (done) {
	  //std::cout << "RuntimeMessageSendAvailableOperator final message send completion received: unlinking port in pid=" << getpid() << std::endl;
	  mActivePorts = mActivePorts->request_unlink();
	  if (NULL != mActivePorts)
	    throw std::runtime_error("Internal Error: NULL!=mActivePorts");
	}
      }

      if (!isSendAvailableRequestActive() && 
	  (mAccumulate >= 2000 || 
	   (NULL!=mActivePorts && mActivePorts->request_count() == 1))) {
	// At this point we have either accumulated enough records to justify sending
	// the count over to receiver or we have seen EOS and need to flush out
	// whatever accumulated counts remain.
	mSendBuffer = (int32_t) std::min(mAccumulate, (int64_t) std::numeric_limits<int32_t>::max());
	mAccumulate -= mSendBuffer;
	{
	  // std::cout << "RuntimeMessageSendAvailableOperator sending data available message rank=" << mSourceRank << 
	  //   "; mSourceTag=" << mSourceTag << "; pid=" << getpid() << std::endl;
	  int ret = MPI_Isend(&mSendBuffer, 4, MPI_BYTE, mSourceRank, mSourceTag, MPI_COMM_WORLD, &mRequest);
	  if (ret != MPI_SUCCESS)
	    throw std::runtime_error((boost::format("RuntimeMessageSendAvailableOperator::onEvent MPI_Isend failed.  Error=%1%") % ret).str());
	}
	requestWrite(DATA_AVAILABLE_REQUEST);
	mState = WRITE_SEND_AVAILABLE_REQUEST;
	return;
      case WRITE_SEND_AVAILABLE_REQUEST:
	write(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mRequest)), true);
      } 
    }

    // Send EOF to MPI demuxer (we have already sent EOF to the record output).
    requestWrite(DATA_AVAILABLE_REQUEST);
    mState = WRITE_SEND_AVAILABLE_REQUEST_EOF;
    return;
  case WRITE_SEND_AVAILABLE_REQUEST_EOF:
    write(port, RecordBuffer(NULL), true);
    //std::cout << "RuntimeMessageSendAvailableOperator::onEvent complete from pid=" << getpid() << std::endl; 
  }
}

void RuntimeMessageSendAvailableOperator::shutdown()
{
}

RuntimeOperator * RuntimeMessageSendOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeMessageSendOperator(s, *this);
}

RuntimeMessageSendOperator::RuntimeMessageSendOperator(RuntimeOperator::Services & s, const RuntimeMessageSendOperatorType & ty)
  :
  RuntimeOperatorBase<RuntimeMessageSendOperatorType>(s, ty),
  mState(START),
  mBuffer(NULL),
  mBufferEnd(NULL),
  mBufferSz(128*1024),
  mSourceRank(ty.mRank),
  mSourceTag(ty.mTag),
  mRequest(MPI_REQUEST_NULL),
  mIsDone(false)
{
}

RuntimeMessageSendOperator::~RuntimeMessageSendOperator()
{
}

void RuntimeMessageSendOperator::start()
{
  mIsDone = false;
  mState = START;
  onEvent(NULL);
}

void RuntimeMessageSendOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(!mIsDone) {
#ifdef USE_BUFFER_POOL_OPERATOR
      // Allocate a buffer
      requestWrite(BUFFER_ALLOC_REQUEST);
      mState = WRITE_BUFFER_ALLOC;
      return;
    case WRITE_BUFFER_ALLOC:
      write(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mBufferSz)), true);
      
      requestRead(BUFFER_ALLOC_COMPLETE);
      mState = READ_BUFFER_ALLOC_COMPLETE;
      return;
    case READ_BUFFER_ALLOC_COMPLETE:
      {
	RecordBuffer buf;
	read(port, buf);
	mBuffer = buf.Ptr;
	// The buffer has a single byte header that indicates
	// whether this is a final buffer.
	mBuffer[0] = 0x00;
	mBufferIt = mBuffer+1;
	mBufferEnd = mBuffer+mBufferSz;
      }
#else
	mBuffer = new uint8_t [mBufferSz];
	// The buffer has a single byte header that indicates
	// whether this is a final buffer.
	mBuffer[0] = 0x00;
	mBufferIt = mBuffer+1;
	mBufferEnd = mBuffer+mBufferSz;
#endif

      // Read records and serialize to fill up the buffer
      while(mBufferIt < mBufferEnd) {
	// Go get a record if we aren't in the middle of one.
	if (mRecordBuffer.Ptr == NULL) {
	  requestRead(RECORD_INPUT);
	  mState = READ_RECORD;
	  return;
	case READ_RECORD:
	  {
	      // std::cout << "RuntimeMessageSendOperator::onEvent : input record from pid=" << getpid() << std::endl; 
	    read(port, mRecordBuffer);
	    if (!RecordBuffer::isEOS(mRecordBuffer)) {
	      mRecordBufferIt.init(mRecordBuffer);
	    } else {
	      // std::cout << "RuntimeMessageSendOperator::onEvent : input EOS from pid=" << getpid() << std::endl; 
	      // Set the EOF flag and break out of this loop.
	      mBuffer[0] = 0x01;
	      mIsDone = true;
	      break;
	    }
	  }
	}
	// Try to serialize the whole thing.  This can fail if we exhaust the available output
	// buffer.
	if (getMyOperatorType().mSerialize.doit(mBufferIt, mBufferEnd, mRecordBufferIt, mRecordBuffer)) {
	  // Done with it so free record.
	  getMyOperatorType().mFree.free(mRecordBuffer);
	  mRecordBuffer = RecordBuffer();
	  mRecordBufferIt.clear();
	} else {
	  BOOST_ASSERT(mBufferIt == mBufferEnd);
	}
      }

      // Check that our logic is OK.
      BOOST_ASSERT (mBufferIt <= mBufferEnd);

      //  Send to the receiver.
      {
	// std::cout << "RuntimeMessageSendOperator::onEvent : sending record to receiver rank=" << mSourceRank << 
	//   "; mSourceTag=" << mSourceTag << "; pid=" << getpid() << std::endl;
	int ret = MPI_Isend(mBuffer, (mBufferIt - mBuffer), MPI_BYTE, mSourceRank, mSourceTag, MPI_COMM_WORLD, &mRequest);
	if (ret != MPI_SUCCESS)
	  throw std::runtime_error((boost::format("RuntimeMessageMessageSendOperator::onEvent MPI_Isend failed.  Error=%1%") % ret).str());
      }
      requestWrite(DATA_SEND_REQUEST);
      mState = WRITE_SEND_DATA_REQUEST;
      return;
    case WRITE_SEND_DATA_REQUEST:
      write(port, RecordBuffer(reinterpret_cast<uint8_t *>(&mRequest)), true);

      // Wait for completion.  We can't really do anything until message is done.
      // TODO: For better throughput we could start the process of serializing
      // another buffer.  We could also implement spilling here (which is a
      // better idea than it seems because we could spill the serialized buffers
      // and just pick them up and put them on the wire when needed; the disadvantage
      // is that it would screw up flow control of the scheduler if we did it here
      // in the operator).
      // std::cout << "RuntimeMessageSendOperator::onEvent waiting for send completion from demux pid=" << getpid() << std::endl;
      requestRead(DATA_SEND_COMPLETE);
      mState = READ_SEND_DATA_COMPLETE;
      return;
    case READ_SEND_DATA_COMPLETE:
      {
	// This is just returning a pointer to an MessagePassingStatus
	// that is owned by the Demuxer. Not much we can do with it.
	RecordBuffer buf;
	read(port, buf);
      }
      // std::cout << "RuntimeMessageSendOperator::onEvent received send completion from demux pid=" << getpid() << std::endl;

#ifdef USE_BUFFER_POOL_OPERATOR
      // Free the buffer back to the pool.
      requestWrite(BUFFER_FREE_REQUEST);
      mState = WRITE_BUFFER_FREE;
      return;
    case WRITE_BUFFER_FREE:
      write(port, RecordBuffer(mBuffer), true);
      mBuffer = mBufferIt = mBufferEnd = NULL;
      // std::cout << "RuntimeMessageSendOperator::onEvent buffer freed pid=" << getpid() << std::endl;
#else
      delete [] mBuffer;
      mBuffer = mBufferIt = mBufferEnd = NULL;
#endif
    }

    // std::cout << "RuntimeMessageSendOperator::onEvent : sending EOS from pid=" << getpid() << std::endl; 
    // Send EOF to all necessary ports.
    requestWrite(BUFFER_ALLOC_REQUEST);
    mState = WRITE_BUFFER_ALLOC_EOF;
    return;
  case WRITE_BUFFER_ALLOC_EOF:
    write(port, RecordBuffer(NULL), true);	
    requestWrite(DATA_SEND_REQUEST);
    mState = WRITE_SEND_DATA_REQUEST_EOF;
    return;
  case WRITE_SEND_DATA_REQUEST_EOF:
    write(port, RecordBuffer(NULL), true);
    requestWrite(BUFFER_FREE_REQUEST);
    mState = WRITE_BUFFER_FREE_EOF;
    return;
  case WRITE_BUFFER_FREE_EOF:
    write(port, RecordBuffer(NULL), true);
    // std::cout << "RuntimeMessageSendOperator::onEvent complete from pid=" << getpid() << std::endl; 
  }
}

void RuntimeMessageSendOperator::shutdown()
{
}

