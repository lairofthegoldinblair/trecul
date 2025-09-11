/**
 * Copyright (c) 2012, Akamai Technologies
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

#include <unistd.h>
#include <fcntl.h>

#include "RuntimeProcess.hh"
#include "RuntimeOperator.hh"
#include "RuntimePlan.hh"
#include "DataflowRuntime.hh"
#include "SuperFastHash.h"
#include "GraphBuilder.hh"
#include "ServiceCompletionPort.hh"

#if defined(TRECUL_HAS_HADOOP)
#include "MapReduceJob.hh"
#endif

#if defined(TRECUL_HAS_MPI)
#include "MessagePassingRemoting.hh"
#endif

#include <condition_variable>
#include <fstream>
#include <memory>
#include <thread>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>
#include <boost/bind/bind.hpp>
#include <boost/regex.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#if defined(__APPLE__) || defined(__APPLE_CC__)
#include <sys/param.h>
#include <mach-o/dyld.h>
#if !defined(environ)
extern char ** environ;
#endif
#endif

#if defined(linux) || defined(__linux) || defined(__linux__)
#include <sys/resource.h>
#endif

namespace po = boost::program_options;

static void checkRegularFileExists(const std::string& filename)
{
  std::filesystem::path p (filename);
  if (!std::filesystem::exists(p)) {
    throw std::runtime_error((boost::format("%1% does not exist") %
			      filename).str());
  }
  if (!std::filesystem::is_regular_file(p)) {
    throw std::runtime_error((boost::format("%1% is not a regular file") %
			      filename).str());
  }  
}

static void readInputFile(const std::string& filename, 
			  std::string& contents)
{
  checkRegularFileExists(filename);
  std::stringstream ostr;
  std::fstream mapFile(filename.c_str(), std::ios_base::in);
  std::copy(std::istreambuf_iterator<char>(mapFile),
	    std::istreambuf_iterator<char>(),
	    std::ostreambuf_iterator<char>(ostr));
  contents = ostr.str();
}

std::filesystem::path Executable::getPath()
{
#if defined(linux) || defined(__linux) || defined(__linux__)
  // Linux we may query /proc
  char buf[PATH_MAX+1];
  ssize_t len=0;
  if((len = ::readlink("/proc/self/exe", &buf[0], PATH_MAX)) != -1)
    buf[len] = 0;
  else {
    int err = errno;
    throw std::runtime_error((boost::format("Failed to resolve executable path: %1%") %
			      err).str());
  }
  return buf;
#elif defined(__APPLE__) || defined(__APPLE_CC__)
  char buf[MAXPATHLEN+1];
  uint32_t pathLen = MAXPATHLEN;
  int ret = _NSGetExecutablePath(&buf[0], &pathLen);
  if (0 == ret) {
    return buf;
  } else if (-1 == ret) {
    char * dynbuf = new char [pathLen];
    ret = _NSGetExecutablePath(dynbuf, &pathLen);
    if (0 == ret) {
      return dynbuf;
    } else {
      return "/";
    }
  } else {
    return "/";
  }
#else
#error "Unsupported platform"
#endif
}

void RuntimeProcess::addRemoteSource(const InterProcessFifoSpec& spec,
                                     RuntimeOperator & sourceOp,
                                     int32_t sourcePartition,
                                     int32_t sourcePartitionConstraintIndex,
                                     int32_t targetPartition,
                                     int32_t targetPartitionConstraintIndex,
                                     int32_t tag)
{
  throw std::runtime_error("Standard dataflow process does not support remote repartitioning/shuffle");
}

void RuntimeProcess::addRemoteTarget(const InterProcessFifoSpec& spec,
                                     RuntimeOperator & targetOp,
                                     int32_t targetPartition,
                                     int32_t targetPartitionConstraintIndex,
                                     int32_t sourcePartition,
                                     int32_t sourcePartitionConstraintIndex,
                                     int32_t tag)
{
  throw std::runtime_error("Standard dataflow process does not support remote repartitioning/shuffle");
}

RuntimeProcess::RuntimeProcess()
  :
  mPartitionStart(0),
  mPartitionEnd(0),
  mNumPartitions(0)
{
}

void RuntimeProcess::init(int32_t partitionStart, 
			  int32_t partitionEnd,
			  int32_t numPartitions,
			  const RuntimeOperatorPlan& plan)
{
  mPartitionStart = partitionStart;
  mPartitionEnd = partitionEnd;
  mNumPartitions = numPartitions;
  if (partitionStart < 0 || partitionEnd >= numPartitions)
    throw std::runtime_error("Invalid partition allocation to process");
  for(int32_t i=partitionStart; i<=partitionEnd; i++) {
    mSchedulers[i] = new DataflowScheduler(i, numPartitions);
  }

  for(RuntimeOperatorPlan::operator_const_iterator it = plan.operator_begin();
      it != plan.operator_end();
      ++it) {
    createOperators(*it->get());
  }
  for(RuntimeOperatorPlan::intraprocess_fifo_const_iterator it = plan.straight_line_begin();
      it != plan.straight_line_end();
      ++it) {
    connectStraightLine(*it);
  }
  for(RuntimeOperatorPlan::interprocess_fifo_const_iterator it = plan.crossbar_begin();
      it != plan.crossbar_end();
      ++it) {
    connectCrossbar(*it);
  }
  for(RuntimeOperatorPlan::interprocess_fifo_const_iterator it = plan.broadcast_begin();
      it != plan.broadcast_end();
      ++it) {
    connectBroadcast(*it);
  }
  for(RuntimeOperatorPlan::interprocess_fifo_const_iterator it = plan.collect_begin();
      it != plan.collect_end();
      ++it) {
    connectCollect(*it);
  }

  for(std::vector<InProcessFifo *>::iterator channel = mChannels.begin();
      channel != mChannels.end();
      ++channel) {
    if ((*channel)->getTarget()->getOperator().getNumInputs() == 1)
      (*channel)->setBuffered(false);
  }
}

RuntimeProcess::~RuntimeProcess()
{
  for(std::map<int32_t, DataflowScheduler*>::iterator it = mSchedulers.begin();
      it != mSchedulers.end();
      ++it) {
    delete it->second;
  }
  for(std::vector<RuntimeOperator * >::iterator opit = mAllOperators.begin();
      opit != mAllOperators.end();
      ++opit) {
    delete *opit;
  }  
  for(std::vector<InProcessFifo *>::iterator chit = mChannels.begin();
      chit != mChannels.end();
      ++chit) {
    delete *chit;
  }
  for(std::vector<ServiceCompletionFifo *>::iterator chit = mServiceChannels.begin();
      chit != mServiceChannels.end();
      ++chit) {
    delete *chit;
  }
}

void RuntimeProcess::connectInProcess(RuntimeOperator & source, int32_t outputPort, int32_t sourcePartition,
				      RuntimeOperator & target, int32_t inputPort, int32_t targetPartition,
				      bool buffered)
{
  connectInProcess(source, outputPort, *mSchedulers[sourcePartition], 
		   target, inputPort, *mSchedulers[targetPartition], buffered);
}

void RuntimeProcess::connectInProcess(RuntimeOperator & source, 
				      int32_t outputPort, 
				      DataflowScheduler & sourceScheduler,
				      RuntimeOperator & target, 
				      int32_t inputPort, 
				      DataflowScheduler & targetScheduler,
				      bool buffered)
{
  // Check that we haven't already connected things up
  if (source.getNumOutputs() > outputPort && nullptr != *(source.output_port_begin() + outputPort)) {
    throw std::runtime_error("RuntimeProcess::connectInProcess source operator is already connected");
  }
  if (target.getNumInputs() > inputPort && nullptr != *(target.input_port_begin() + inputPort)) {
    throw std::runtime_error("RuntimeProcess::connectInProcess target operator is already connected");
  }
  mChannels.push_back(new InProcessFifo(sourceScheduler, targetScheduler, buffered));
  source.setOutputPort(mChannels.back()->getSource(), outputPort);
  mChannels.back()->getSource()->setOperator(source);
  target.setInputPort(mChannels.back()->getTarget(), inputPort);
  mChannels.back()->getTarget()->setOperator(target);
}

void RuntimeProcess::connectStraightLine(const IntraProcessFifoSpec& spec)
{
  std::vector<int32_t> spartitions;
  spec.getSourceOperator()->getPartitions(mPartitionStart,
					  mPartitionEnd,
					  spartitions);
  std::vector<int32_t> tpartitions;
  spec.getTargetOperator()->getPartitions(mPartitionStart, 
					  mPartitionEnd,
					  tpartitions);
  if (spartitions != tpartitions)
    throw std::runtime_error("Invalid plan: straight line connection specified on operators that are not in the same partitions.");
  for(std::vector<int32_t>::const_iterator i=spartitions.begin();
      i != spartitions.end();
      ++i) {
    RuntimeOperator * sourceOp = getOperator(spec.getSourceOperator()->Operator, *i);
    if (sourceOp==NULL) throw std::runtime_error("Operator not created");
    RuntimeOperator * targetOp = getOperator(spec.getTargetOperator()->Operator, *i);
    if (targetOp==NULL) throw std::runtime_error("Operator not created");
    connectInProcess(*sourceOp, spec.getSourcePort(), *i,
		     *targetOp, spec.getTargetPort(), *i, spec.getBuffered());
  }  
}

void RuntimeProcess::connectCrossbar(const InterProcessFifoSpec& spec)
{
  // Get the partitions within this process for each operator.
  std::vector<int32_t> spartitions;
  spec.getSourceOperator()->getPartitions(mPartitionStart, mPartitionEnd, spartitions);
  std::vector<int32_t> tpartitions;
  spec.getTargetOperator()->getPartitions(mPartitionStart, mPartitionEnd, tpartitions);

  // To calculate MPI tags in crossbars, we need to know the index/position
  // of a partition within the vector of partitions the operator lives on.
  for(std::vector<int32_t>::const_iterator i=spartitions.begin();
      i != spartitions.end();
      ++i) {
    addSource(spec, *i, 
              spec.getSourceOperator()->getPartitionPosition(*i));
  } 
  for(std::vector<int32_t>::const_iterator i=tpartitions.begin();
      i != tpartitions.end();
      ++i) {
    addTarget(spec, *i, 
              spec.getTargetOperator()->getPartitionPosition(*i));
  } 
}

void RuntimeProcess::connectBroadcast(const InterProcessFifoSpec& spec)
{
  // Get the partitions within this process for each operator.
  std::vector<int32_t> spartitions;
  spec.getSourceOperator()->getPartitions(mPartitionStart, mPartitionEnd, spartitions);
  std::vector<int32_t> tpartitions;
  spec.getTargetOperator()->getPartitions(mPartitionStart, mPartitionEnd, tpartitions);

  if (spartitions.size() > 1) {
    throw std::runtime_error("INTERNAL_ERROR: connectBroadcast has more than 1 source partition");
  }

  // To calculate MPI tags in crossbars, we need to know the index/position
  // of a partition within the vector of partitions the operator lives on.
  if (spartitions.size() > 0) {
    if (0 != spec.getSourceOperator()->getPartitionPosition(spartitions[0])) {
      throw std::runtime_error("INTERNAL_ERROR: connectBroadcast has more than 1 source partition");
    }
    addSource(spec, spartitions[0], 0);
  } 
  for(std::vector<int32_t>::const_iterator i=tpartitions.begin();

      i != tpartitions.end();
      ++i) {
    addTarget(spec, *i, 
              spec.getTargetOperator()->getPartitionPosition(*i));
  } 
}

void RuntimeProcess::connectCollect(const InterProcessFifoSpec& spec)
{
  // Get the partitions within this process for each operator.
  std::vector<int32_t> spartitions;
  spec.getSourceOperator()->getPartitions(mPartitionStart, mPartitionEnd, spartitions);
  std::vector<int32_t> tpartitions;
  spec.getTargetOperator()->getPartitions(mPartitionStart, mPartitionEnd, tpartitions);

  if (tpartitions.size() > 1) {
    throw std::runtime_error("INTERNAL_ERROR: connectCollect has more than 1 target partition");
  }

  // To calculate MPI tags in crossbars, we need to know the index/position
  // of a partition within the vector of partitions the operator lives on.
  for(std::vector<int32_t>::const_iterator i=spartitions.begin();
      i != spartitions.end();
      ++i) {
    addSource(spec, *i, 
              spec.getSourceOperator()->getPartitionPosition(*i));
  } 
  if (tpartitions.size() > 0) {
    if (0 != spec.getTargetOperator()->getPartitionPosition(tpartitions[0])) {
      throw std::runtime_error("INTERNAL_ERROR: connectCollect has more than 1 target partition");
    }
    addTarget(spec, tpartitions[0], 0);
  } 
}

void RuntimeProcess::addSource(const InterProcessFifoSpec& spec,
                               int32_t sourcePartition,
                               int32_t sourcePartitionConstraintIndex)
{
  if (!spec.getSourceOperator()->Operator->isPartitioner() &&
      !spec.getTargetOperator()->Operator->isCollector()) {
    throw std::runtime_error("RuntimeProcess::addSource called without partitioner or collector");
  }
  
  // Get the source operator we are connecting.
  RuntimeOperator * sourceOp = getOperator(spec.getSourceOperator()->Operator, sourcePartition);
  if (sourceOp==NULL) throw std::runtime_error("Operator not created");

  // TODO: Sanity checking about either doing a full crossbar between ports 0,0 or
  // allowing sequential partitioner to go to an arbitrary port, sequential collector
  // from an arbitrary port.

  int tagBase = spec.getTag() + sourcePartitionConstraintIndex*spec.getTargetOperator()->getPartitionCount(mNumPartitions);
  // Iterate over all the target partitions we are going to connect to.
  int numSet = 0;
  boost::dynamic_bitset<> targetPartitions;
  getPartitions(spec.getTargetOperator(), targetPartitions);
  for(std::size_t pos = targetPartitions.find_first();
      pos != boost::dynamic_bitset<>::npos;
      pos = targetPartitions.find_next(pos)) {
    int32_t sourcePort = spec.getSourceOperator()->Operator->isPartitioner() ? numSet : spec.getSourcePort();
    if (hasPartition(pos)) {
      // Target partition is in this process.
      // Both operators must have been instantiated in this context already.
      RuntimeOperator * targetOp = getOperator(spec.getTargetOperator()->Operator, (int32_t) pos);
      if (targetOp==NULL) throw std::runtime_error("Operator not created");

      // Here is where crossbar vs. sequential partition vs. sequential collector differ.
      // In the latter two cases, one of these port ids actually comes from the fifo spec.
      int32_t targetPort = spec.getTargetOperator()->Operator->isCollector() ? sourcePartitionConstraintIndex : spec.getTargetPort();
      connectInProcess(*sourceOp, sourcePort, sourcePartition,
                       *targetOp, targetPort, (int32_t) pos, true);
    } else {
      addRemoteSource(spec,
                      *sourceOp,
                      sourcePartition,
                      sourcePartitionConstraintIndex,
                      pos,
                      sourcePort,
                      tagBase + numSet);
    }
    numSet += 1;
  }
  // If not connecting from a partitioner then the target must be sequential
  BOOST_ASSERT(spec.getSourceOperator()->Operator->isPartitioner() || 1 == numSet);
}

void RuntimeProcess::addTarget(const InterProcessFifoSpec& spec,
                               int32_t targetPartition,
                               int32_t targetPartitionConstraintIndex)
{
  if (!spec.getSourceOperator()->Operator->isPartitioner() &&
      !spec.getTargetOperator()->Operator->isCollector()) {
    throw std::runtime_error("RuntimeProcess::addSource called without partitioner or collector");
  }

  // Get the source operator we are connecting.
  RuntimeOperator * targetOp = getOperator(spec.getTargetOperator()->Operator, targetPartition);
  if (targetOp==NULL) throw std::runtime_error("Operator not created");

  int tagBase = spec.getTag() + targetPartitionConstraintIndex;
  // Iterate over all the source partitions we are going to connect to.
  int numSet = 0;
  boost::dynamic_bitset<> sourcePartitions;
  getPartitions(spec.getSourceOperator(), sourcePartitions);
  for(std::size_t pos = sourcePartitions.find_first();
      pos != boost::dynamic_bitset<>::npos;
      pos = sourcePartitions.find_next(pos)) {
    int32_t targetPort = spec.getTargetOperator()->Operator->isCollector() ? numSet : spec.getTargetPort();
    if (hasPartition(pos)) {
      // Both operators must have been instantiated in this context already.
      RuntimeOperator * sourceOp = getOperator(spec.getSourceOperator()->Operator, pos);
      if (sourceOp==NULL) throw std::runtime_error("Operator not created");
      // Don't create an in-process channel as that has/will be done in addSource.
    } else {
      addRemoteTarget(spec,
                      *targetOp,
                      targetPartition,
                      targetPartitionConstraintIndex,
                      pos,
                      targetPort,
                      tagBase + spec.getTargetOperator()->getPartitionCount(mNumPartitions)*numSet);
    }
    numSet += 1;
  }
  // If not connecting to a collector then the source must be sequential
  BOOST_ASSERT(spec.getTargetOperator()->Operator->isCollector() || 1 == numSet);
}

RuntimeOperator * RuntimeProcess::createOperator(const RuntimeOperatorType * ty, int32_t partition)
{
  std::map<int32_t, DataflowScheduler*>::const_iterator sit = mSchedulers.find(partition);
  if (sit == mSchedulers.end())
    throw std::runtime_error((boost::format("Internal Error: failed to create scheduler for data partition %1%") % partition).str());
  RuntimeOperator * op = ty->create(*sit->second);
  mAllOperators.push_back(op);
  mPartitionIndex[partition].push_back(op);
  mTypePartitionIndex[ty][partition] = op;
  for(int32_t i=0; i<ty->numServiceCompletionPorts(); ++i) {
    ServiceCompletionFifo * serviceChannel = new ServiceCompletionFifo(*sit->second);
    op->setCompletionPort(serviceChannel->getTarget(), i);
    serviceChannel->getTarget()->setOperator(*op);
    mServiceChannels.push_back(serviceChannel);
  }
  return op;
}

void RuntimeProcess::createOperators(const AssignedOperatorType& ty)
{
  std::vector<int32_t> partitions;
  ty.getPartitions(mPartitionStart, mPartitionEnd, partitions);
  for(std::vector<int32_t>::const_iterator i=partitions.begin();
      i != partitions.end();
      ++i) {
    createOperator(ty.Operator, *i);
  }
}

DataflowScheduler& RuntimeProcess::getScheduler(int32_t partition)
{
  return *mSchedulers[partition];
}

const std::vector<RuntimeOperator*>& RuntimeProcess::getOperators(int32_t partition)
{
  std::map<int32_t, std::vector<RuntimeOperator*> >::const_iterator it = mPartitionIndex.find(partition);
  return it->second;
}

RuntimeOperator * RuntimeProcess::getOperator(const RuntimeOperatorType* ty, int32_t partition)
{
  std::map<const RuntimeOperatorType *, std::map<int32_t, RuntimeOperator *> >::const_iterator it1=mTypePartitionIndex.find(ty);
  if (it1==mTypePartitionIndex.end()) return NULL;
  std::map<int32_t, RuntimeOperator *>::const_iterator it2=it1->second.find(partition);
  if (it2==it1->second.end()) return NULL;
  return it2->second;
}

void RuntimeProcess::getPartitions(const AssignedOperatorType * opType,
				   boost::dynamic_bitset<> & result)
{
  opType->getPartitions(mNumPartitions, result);
}

void RuntimeProcess::run(DataflowScheduler& s)
{
  try {
    // TODO: Signal and exception handling.
    s.run();
    s.cleanup();
  } catch(std::exception& ex) {
    std::cerr << "Failure in scheduler thread: " << ex.what() << std::endl;
  }
}

class DataflowSchedulerThreadRunner 
{
private:
  bool mFailed;
  std::string mMessage;
  DataflowScheduler& mScheduler;
  std::mutex & mMutex;
  std::deque<DataflowSchedulerThreadRunner*> & mCompletedQueue;
  std::condition_variable & mCondVar;
 
public:
  DataflowSchedulerThreadRunner(DataflowScheduler& s,
                                std::mutex & mut,
                                std::deque<DataflowSchedulerThreadRunner *> & completedQueue,
                                std::condition_variable & condVar);
  bool isFailed() const { return mFailed; }
  const std::string& getMessage() const { return mMessage; }
  void run();
  void cancel();
};

DataflowSchedulerThreadRunner::DataflowSchedulerThreadRunner(DataflowScheduler & s,
                                                             std::mutex & mut,
                                                             std::deque<DataflowSchedulerThreadRunner *> & completedQueue,
                                                             std::condition_variable & condVar)
  :
  mFailed(false),
  mScheduler(s),
  mMutex(mut),
  mCompletedQueue(completedQueue),
  mCondVar(condVar)
{
}

void DataflowSchedulerThreadRunner::run()
{
  try {
    // TODO: genericize to accept a functor argument.
    mScheduler.run();
    mScheduler.cleanup();
  } catch(std::exception& ex) {
    mMessage = ex.what();
    mFailed = true;
  }
  std::unique_lock lk(mMutex);
  mCompletedQueue.push_back(this);
  lk.unlock();
  mCondVar.notify_one();
}

void DataflowSchedulerThreadRunner::cancel()
{
  mScheduler.cancel();
}

void RuntimeProcess::validateGraph()
{
  // Sanity check that all of the operator ports are properly configured.
  for(std::vector<RuntimeOperator * >::iterator opit = mAllOperators.begin();
      opit != mAllOperators.end();
      ++opit) {
    for(RuntimeOperator::input_port_iterator pit = (*opit)->input_port_begin();
	pit != (*opit)->input_port_end();
	++pit) {
      if (*opit != (*pit)->getOperatorPtr()) {
	throw std::runtime_error("Internal Error: Incorrectly configured input port");
      }
    }
    for(RuntimeOperator::input_port_iterator pit = (*opit)->input_port_begin();
	pit != (*opit)->input_port_end();
	++pit) {
      if (*opit != (*pit)->getOperatorPtr()) {
	throw std::runtime_error("Internal Error: Incorrectly configured output port");
      }
    }
  }
}

void RuntimeProcess::runInit()
{
  validateGraph();
  for(std::map<int32_t, DataflowScheduler*>::iterator it = mSchedulers.begin();
      it != mSchedulers.end();
      ++it) {
    it->second->setOperators(mPartitionIndex[it->first]);
  }
  BOOST_ASSERT(mSchedulers.size() == 1);
  mSchedulers.begin()->second->init();
}

DataflowScheduler::RunCompletion RuntimeProcess::runSome(int64_t maxIterations)
{
  BOOST_ASSERT(mSchedulers.size() == 1);
  return mSchedulers.begin()->second->runSome(maxIterations);
}

bool RuntimeProcess::runSome()
{
  BOOST_ASSERT(mSchedulers.size() == 1);
  return mSchedulers.begin()->second->runSome();
}

void RuntimeProcess::runComplete()
{
  BOOST_ASSERT(mSchedulers.size() == 1);
  mSchedulers.begin()->second->complete();
  mSchedulers.begin()->second->cleanup();
}

void RuntimeProcess::run()
{
  validateGraph();
  // Simple model: one thread per scheduler.
  std::vector<std::shared_ptr<std::thread> > threads;
  std::vector<std::shared_ptr<DataflowSchedulerThreadRunner> > runners;

  // Start any threads necessary for remote execution
  runRemote(threads);

  std::mutex mutex;
  std::deque<DataflowSchedulerThreadRunner*> completedQueue;
  std::condition_variable condVar;
  // Now start schedulers for each partition.
  for(std::map<int32_t, DataflowScheduler*>::iterator it = mSchedulers.begin();
      it != mSchedulers.end();
      ++it) {
    it->second->setOperators(mPartitionIndex[it->first]);
    runners.push_back(std::shared_ptr<DataflowSchedulerThreadRunner>(new DataflowSchedulerThreadRunner(*it->second, mutex, completedQueue, condVar)));
    threads.push_back(std::shared_ptr<std::thread>(new std::thread(std::bind(&DataflowSchedulerThreadRunner::run, runners.back()))));
  }

  // // Print out the state of channels every now and then
  // for (int k=0; k<10; ++k) {
  //   for(std::vector<InProcessFifo *>::const_iterator channel = mChannels.begin();
  // 	channel != mChannels.end();
  // 	++channel) {
  //     std::cout << "Channel[" << 
  // 	(*channel)->getSource()->getOperatorPtr()->getName().c_str() <<
  // 	"," <<
  // 	(*channel)->getTarget()->getOperatorPtr()->getName().c_str() <<
  // 	"] Size=" << (*channel)->getSize() << "; Source Buffer Size=" <<
  // 	(*channel)->getSource()->getLocalBuffer().getSize() << "; Target Buffer Size=" <<
  // 	(*channel)->getTarget()->getLocalBuffer().getSize() << std::endl;
  //   }
  //   boost::this_thread::sleep(boost::posix_time::milliseconds(5000));
  //   std::cout << "=================================================" << std::endl;
  // }
  
  // Wait for workers to complete.  If a thread fails then cancel the rest.
  std::size_t numCompleted=0;
  bool cancelled(false);
  while(numCompleted < runners.size()) {
    std::unique_lock lk(mutex);
    condVar.wait(lk, [&completedQueue] { return completedQueue.size() > 0; });
    numCompleted += completedQueue.size();
    for(auto runner : completedQueue) {
      if (!cancelled && runner->isFailed()) {
        for(auto & r : runners) {
          r->cancel();
        }
        cancelled = true;
      }
    }
    completedQueue.clear();
  }

  
  for(std::vector<std::shared_ptr<std::thread> >::iterator it = threads.begin();
      it != threads.end();
      ++it) {
    (*it)->join();
  }

  // Check for errors and rethrow.
  int32_t numThreadErrors=0;
  std::stringstream errorMessages;
  for(std::vector<std::shared_ptr<DataflowSchedulerThreadRunner> >::iterator it = runners.begin();
      it != runners.end();
      ++it) {
    if ((*it)->isFailed()) {
      numThreadErrors += 1;
      if (numThreadErrors > 1) {
        errorMessages << "\n";
      }
      errorMessages << ((*it)->getMessage().size() == 0 ? "No message detail" : (*it)->getMessage().c_str());
    }
  }
  if (numThreadErrors) {
    if (numThreadErrors > 1) {
      std::cerr << "Failures";
    } else {
      std::cerr << "Failure"; 
    }
    std::cerr << " in scheduler thread: " << errorMessages.str().c_str() << std::endl;
    throw std::runtime_error(errorMessages.str());
  }
}

class Timer
{
private:
  boost::posix_time::ptime mTick;
  int32_t mPartition;
public:
  Timer(int32_t partition);
  ~Timer();
};

Timer::Timer(int32_t partition)
  :
  mPartition(partition)
{
  mTick = boost::posix_time::microsec_clock::universal_time();
}

Timer::~Timer()
{
  boost::posix_time::ptime tock = boost::posix_time::microsec_clock::universal_time();
  std::cout << "ExecutionTime:\t" << mPartition << "\t" << (tock - mTick) << std::endl;
}

void PlanRunner::createSerialized64PlanFromFile(const std::string& f,
						int32_t partitions,
						std::string& p)
{
  PlanCheckContext ctxt;
  DataflowGraphBuilder gb(ctxt);
  gb.buildGraphFromFile(f);  
  std::shared_ptr<RuntimeOperatorPlan> plan = gb.create(partitions);
  p = PlanGenerator::serialize64(plan);
}

static bool checkRequiredArgs(const po::variables_map& vm,
			      const std::vector<std::pair<std::string, std::string> >& args)
{
  typedef std::vector<std::pair<std::string, std::string> > pairs;
  bool ok=true;
  for(pairs::const_iterator p = args.begin();
      p != args.end();
      ++p) {
    if (0==vm.count(p->first) && 0 < vm.count(p->second)) {
      std::cerr << (boost::format("Cannot use \"%1%\" option without a \"%2%\" option") %
		    p->second % p->first).str().c_str() << "\n";
      ok = false;
    }
  }
  return ok;
}

#if defined(linux) || defined(__linux) || defined(__linux__)
static bool setProcessLimit(rlim_t desired, int resource, const char * resource_name)
{
  struct rlimit rl;
  if (getrlimit(resource, &rl) < 0) {
    std::cerr << "getrlimit(" << resource_name << ") failed: " << strerror(errno) << std::endl;
    return false;
  }
  rl.rlim_cur = (std::min)(desired, rl.rlim_max);
  if (setrlimit(resource, &rl) < 0) {
    std::cerr << "setrlimit(" << resource_name << ") failed: " << strerror(errno) << std::endl;
    return false;
  }
  return true;
}
#endif

int PlanRunner::run(int argc, char ** argv)
{
  // Make sure this symbol can be dlsym'd
  // TODO: I think a better way to do this is to export a pointer to the function
  // from the module.
  int dummy=9923;
  SuperFastHash((char *) &dummy, sizeof(int), sizeof(int));
  
#if defined(TRECUL_HAS_HADOOP)
  // If a Hadoop installation is present, then setup appropriate env.
  HadoopSetup::setEnvironment();
#endif

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("case-insensitive", "should identifiers be case insensitive")
    ("compile", "generate dataflow plan but don't run")
    ("serial", po::value<int32_t>(), "specific partition against which to run a dataflow")
    ("partitions", po::value<int32_t>(), "number of partitions for the flow")
    ("plan", "run dataflow from a compiled plan")
    ("file", po::value<std::string>(), "input script file to be run in process")
#if defined(linux) || defined(__linux) || defined(__linux__)
    ("max-open-files", po::value<uint32_t>(), "maximum number of open files to allow")
    ("max-core-file-size", po::value<int32_t>(), "maximum core file size (-1 unlimited)")
#endif
#if defined(TRECUL_HAS_HADOOP)
    ("map", po::value<std::string>(), "input mapper script file for jobs run through Hadoop pipes")
    ("reduce", po::value<std::string>(), "input reducer script file for jobs run through Hadoop pipes")
    ("numreduces", po::value<int32_t>(), "number of reducers")
    ("nojvmreuse", "suppress JVM reuse in map reduce programs")
    ("name", po::value<std::string>(), "job name to run as")
    ("input", po::value<std::string>(), "input directory for jobs run through Hadoop pipes")
    ("output", po::value<std::string>(), "output directory for jobs run through Hadoop pipes")
    ("jobqueue", po::value<std::string>(), "job queue for jobs run through Hadoop pipes")
    ("task-timeout", po::value<int32_t>(), "task timeout for jobs run through Hadoop pipes")
    ("speculative-execution", po::value<std::string>(), 
     "speculative execution settings for jobs run through Hadoop pipes (both|none|map|reduce)")
    ("proxy", "use proxy ads-hp-client for jobs run through Hadoop pipes")
#endif
    ;

  po::variables_map vm;        
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);    
  
  if (vm.count("help") ||
      (0 == vm.count("file") && 0 == vm.count("map"))) {
    std::cerr << desc << "\n";
    return 1;
  }

  // Validation steps
  if (vm.count("file") && (vm.count("map") || vm.count("reduce"))) {
    std::cerr << "Cannot use both \"file\" and \"map\"/\"reduce\" options" << std::endl;
    std::cerr << desc << std::endl;
    return 1;    
  }
  std::vector<std::pair<std::string,std::string> > pairs;
  pairs.push_back(std::make_pair("file", "compile"));
  pairs.push_back(std::make_pair("file", "plan"));
#if (TRECUL_HAS_HADOOP)
  pairs.push_back(std::make_pair("map", "reduce"));
  pairs.push_back(std::make_pair("map", "input"));
  pairs.push_back(std::make_pair("map", "output"));
  pairs.push_back(std::make_pair("map", "proxy"));
  pairs.push_back(std::make_pair("map", "nojvmreuse"));
  pairs.push_back(std::make_pair("map", "jobqueue"));
  pairs.push_back(std::make_pair("map", "speculative-execution"));
  pairs.push_back(std::make_pair("map", "task-timeout"));
#endif
  if (!checkRequiredArgs(vm, pairs)) {
    std::cerr << desc << std::endl;
    return 1;    
  }

  if (vm.count("case-insensitive")) {
    TypeCheckConfiguration::get().caseInsensitive(true);
  }

#if defined(linux) || defined(__linux) || defined(__linux__)
  if (vm.count("max-open-files") > 0) {
    if (!setProcessLimit(vm["max-open-files"].as<uint32_t>(), RLIMIT_NOFILE, "RLIMIT_NOFILE")) {
      return 1;
    }
  }
  if (vm.count("max-core-file-size") > 0) {
    if (!setProcessLimit(vm["max-core-file-size"].as<uint32_t>() >= 0 ? vm["max-core-file-size"].as<uint32_t>() : RLIM_INFINITY, RLIMIT_CORE, "RLIMIT_CORE")) {
      return 1;
    }
  }
#endif  
  
  if (vm.count("compile")) {
    std::string inputFile(vm["file"].as<std::string>());
    int32_t partitions=1;
    if (vm.count("partitions")) {
      partitions = vm["partitions"].as<int32_t>();
    }
    std::string buf;
    createSerialized64PlanFromFile(inputFile, partitions, buf);
    std::cout << buf.c_str();
    return 0;
  } else if (vm.count("plan")) {
    Timer t(0);
    std::string inputFile(vm["file"].as<std::string>());
    checkRegularFileExists(inputFile);
    int32_t partition=0;
    if (vm.count("serial")) {
      partition = vm["serial"].as<int32_t>();
    }
    int32_t partitions=1;
    if (vm.count("partitions")) {
      partitions = vm["partitions"].as<int32_t>();
    }
    std::size_t sz = std::filesystem::file_size(inputFile);
    std::ifstream istr(inputFile.c_str());
    std::vector<char> encoded(sz);
    istr.read(&encoded[0], sz);

    std::shared_ptr<RuntimeOperatorPlan> tmp = PlanGenerator::deserialize64(&encoded[0] ,
									      encoded.size());
    RuntimeProcess p;
    p.init(partition,partition,partitions,*tmp.get());
    p.run();
    return 0;
#if defined(TRECUL_HAS_HADOOP)
  } else if (vm.count("map")) {    
    bool useHp(vm.count("proxy") > 0);
    bool jvmReuse(vm.count("nojvmreuse") == 0);
    std::string inputDir(vm.count("input") ? 
			 vm["input"].as<std::string>().c_str() : 
			 "/1_2048/serials");
    std::string outputDir(vm.count("output") ? 
			  vm["output"].as<std::string>().c_str() : 
			  "");
    std::string jobQueue(vm.count("jobqueue") ? 
			 vm["jobqueue"].as<std::string>().c_str() : 
			 "");
    AdsDfSpeculativeExecution speculative(vm.count("speculative-execution") ?
					  vm["speculative-execution"].as<std::string>().c_str() :
					  "both");
    int32_t timeout(vm.count("task-timeout") ? 
		    vm["task-timeout"].as<int32_t>() : 
		    AdsPipesJobConf::DEFAULT_TASK_TIMEOUT);

    std::string mapProgram;
    readInputFile(vm["map"].as<std::string>(), mapProgram);

    std::string reduceProgram;
    int32_t reduces=0;
    if(vm.count("reduce")) {
      readInputFile(vm["reduce"].as<std::string>(), reduceProgram);
      if (vm.count("numreduces")) {
	reduces = vm["numreduces"].as<int32_t>();
      } else {
	reduces = 1;
      }
    }

    std::string name;
    if (vm.count("name")) {
      name = vm["name"].as<std::string>();
    } else {
      std::string map = vm["map"].as<std::string>();
      name += map + "-mapper";
      if (vm.count("reduce")) {
	name += "," + vm["reduce"].as<std::string>() + "-reducer";
      }
    }

    return MapReducePlanRunner::runMapReduceJob(mapProgram,
						reduceProgram,
						name,
						jobQueue,
						inputDir,
						outputDir,
						reduces,
						jvmReuse,
						useHp,
						speculative,
						timeout);
#endif
#if defined(TRECUL_HAS_MPI)
  } else if (MessagePassingRemoting::isMPI()) {
    return MessagePassingRemoting::runMPI(vm);
#endif
  } else {
    std::string inputFile(vm["file"].as<std::string>());
    if (!boost::algorithm::equals(inputFile, "-")) {
      checkRegularFileExists(inputFile);
    }
    int32_t partitions=1;
    if (vm.count("partitions")) {
      partitions = vm["partitions"].as<int32_t>();
    }
    int32_t partition=-1;
    if (vm.count("serial")) {
      partition = vm["serial"].as<int32_t>();
    }
    // Make sure partitions is at least as large as partition+1;
    if (partition >= partitions)
      partitions = partition+1;
    PlanCheckContext ctxt;
    DataflowGraphBuilder gb(ctxt);
    gb.buildGraphFromFile(inputFile);
    std::shared_ptr<RuntimeOperatorPlan> plan = gb.create(partitions);
    RuntimeProcess p;
    if (partition < 0) {
      p.init(0,partitions-1,partitions,*plan.get());
    } else {
      p.init(partition,partition,partitions,*plan.get());
    }
    p.run();
    return 0;
  }
}

