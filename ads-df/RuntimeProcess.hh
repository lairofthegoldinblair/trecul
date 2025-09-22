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

#ifndef __RUNTIMEPROCESS_H
#define __RUNTIMEPROCESS_H

#include <filesystem>
#include <memory>
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <map>
#include <boost/asio/io_service.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

#include "DataflowRuntime.hh"

class RuntimeOperator;
class RuntimeOperatorType;
class RuntimeOperatorPlan;
class AssignedOperatorType;
class DataflowScheduler;
class IntraProcessFifoSpec;
class InterProcessFifoSpec;
class InProcessFifo;
class RuntimeProcess;

/**
 * A runtime operator process contains a collection of partitions that
 * are executed by one or more threads.  
 */
class RuntimeProcess
{
private:
  // The partitions this process is managing.
  int32_t mPartitionStart;
  int32_t mPartitionEnd;
  int32_t mNumPartitions;

  // The dataflow scheduler for a partition
  std::map<int32_t, DataflowScheduler*> mSchedulers;

  // As operators are created we need to be able to lookup the operator
  // the operator type and partition number.
  std::map<const RuntimeOperatorType *, std::map<int32_t, RuntimeOperator *> > mTypePartitionIndex;
  std::map<int32_t, std::vector<RuntimeOperator*> > mPartitionIndex;
  std::vector<RuntimeOperator * > mAllOperators;
  // Channels connecting operators that both live in this process
  std::vector<InProcessFifo *> mChannels;
  // Service Completion Channels
  std::vector<class ServiceCompletionFifo *> mServiceChannels;
  // ASIO service
  boost::asio::io_service mIOService;

  /**
   * Create all of the operators in the required partitions.
   */
  void createOperators(const AssignedOperatorType& ty);

  const std::vector<RuntimeOperator*>& getOperators(int32_t partition);

  void connectStraightLine(const IntraProcessFifoSpec& spec);
  void connectCrossbar(const InterProcessFifoSpec& spec);
  void connectBroadcast(const InterProcessFifoSpec& spec);
  void connectCollect(const InterProcessFifoSpec& spec);

  // Add repartitioning sources and targets
  void addSource(const InterProcessFifoSpec& spec, 
                 int32_t sourcePartition, 
                 int32_t sourcePartitionConstraintIndex);
  void addTarget(const InterProcessFifoSpec& spec, 
                 int32_t targetPartition, 
                 int32_t targetPartitionConstraintIndex);
  /**
   * Validate graph prior to running.
   */
  void validateGraph();
protected:
  // Helper shim for running a scheduler.
  // TODO: Extra stuff to get any exceptions back.
  static void run(DataflowScheduler& s);
  
public:
  RuntimeProcess();
  virtual ~RuntimeProcess();

  void init(int32_t partitionStart, 
	    int32_t partitionEnd,
	    int32_t numPartitions,
	    const RuntimeOperatorPlan& plan);
  /**
   * Get all operators of a particular type.
   */
  template <class _OpType>
  void getOperatorOfType(std::vector<_OpType*>& ret) {
    for(std::vector<RuntimeOperator * >::iterator it = mAllOperators.begin();
	it != mAllOperators.end();
	++it) {
      _OpType * tmp = dynamic_cast<_OpType *>(*it);
      if (tmp != NULL)
	ret.push_back(tmp);
    }
  }

  /**
   * Run the dataflow and return on completion.
   */
  void run();

  /**
   * Initialize the dataflow for cooperative multitask running within an
   * existing ambient thread.
   * In this model of execution, operators have the option of yielding to
   * the scheduler for a reason other than reading or writing to a port (e.g.
   * waiting on a socket managed outside of dataflow).  The scheduler then exits
   * back to the controlling thread by returning from runSome().  It is then 
   * incumbent upon the controlling thread to pass an event to appropriate 
   * operators to enable them to be unblocked.  At that point, the controller
   * can invoke runSome() again.
   */
  void runInit();
  DataflowScheduler::RunCompletion runSome(int64_t maxIterations);
  bool runSome();
  void runComplete();

  /**
   * The following methods are available for a remoting derived class
   * to modify the process dataflow graph.
   */

  /**
   * Connect ports of two operators in the same process (but perhaps
   * different threads.
   */
  void connectInProcess(RuntimeOperator & source, int32_t outputPort, int32_t sourcePartition,
			RuntimeOperator & target, int32_t inputPort, int32_t targetPartition, bool buffered);
  void connectInProcess(RuntimeOperator & source, 
			int32_t outputPort, 
			DataflowScheduler & sourceScheduler,
			RuntimeOperator & target, 
			int32_t inputPort, 
			DataflowScheduler & targetScheduler, 
			bool buffered);
  /**
   * Create an operator in a single partition managed by this process and update
   * all indexes on the operator collection.
   */
  RuntimeOperator * createOperator(const RuntimeOperatorType * ty, int32_t partition);

  /**
   * Get the operator created from a type in a particular partition.
   */
  RuntimeOperator * getOperator(const RuntimeOperatorType* ty, int32_t partition);

  /**
   * Test whether the process contains the partition in question.
   */
  bool hasPartition(std::size_t partition)
  {
    return partition <= (std::size_t) std::numeric_limits<int32_t>::max() &&
      mPartitionStart <= (int32_t) partition && mPartitionEnd >= (int32_t) partition;
  }

  /**
   * Get the partitions for a given operator type.
   */
  void getPartitions(const AssignedOperatorType * opType,
		     boost::dynamic_bitset<> & result);

  /**
   * Get the dataflow scheduler for a partition.
   */
  DataflowScheduler& getScheduler(int32_t partition);

  /**
   * The following methods are implemented for a remoting strategy.
   * Default implementation is to throw an exception
   * since the default doesn't support remoting.
   */
  virtual void addRemoteSource(const InterProcessFifoSpec& spec,
                               RuntimeOperator & sourceOp,
                               int32_t sourcePartition,
                               int32_t sourcePartitionConstraintIndex,
                               int32_t targetPartition,
                               int32_t targetPartitionConstraintIndex,
                               int32_t tag);
  virtual void addRemoteTarget(const InterProcessFifoSpec& spec,
                               RuntimeOperator & targetOp,
                               int32_t targetPartition,
                               int32_t targetPartitionConstraintIndex,
                               int32_t sourcePartition,
                               int32_t sourcePartitionConstraintIndex,
                               int32_t tag);
  virtual void runRemote(std::vector<std::shared_ptr<std::thread> >& threads) {}
};

class PlanRunner
{
private:
  static void createSerialized64PlanFromFile(const std::string& f,
					     int32_t partitions,
					     std::string& plan);
public:
  static int run(int argc, char ** argv);
};

class Executable
{
public:
  /**
   * Get full path to the executable that is running.
   */
  static std::filesystem::path getPath();
};

#endif
