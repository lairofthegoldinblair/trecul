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

#ifndef __RUNTIME_OPERATOR_HH
#define __RUNTIME_OPERATOR_HH

#include <boost/assert.hpp>
#include <boost/core/ignore_unused.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include "RuntimePlan.hh"
#include "DataflowRuntime.hh"
#include "RecordType.hh"
#include "IQLInterpreter.hh"
#include "SuperFastHash.h"
#include "LogicalOperator.hh"

#include <boost/unordered/unordered_map.hpp>

class HashFunction
{
public:
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * inputType,
                              const std::vector<std::string>& fields,
                              const std::string& name = "hash");
};

class EqualsFunction
{
public:
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * lhs,
                              const RecordType * rhs,
                              const std::vector<std::string>& fields,
                              const std::string& name = "eq",
                              bool areNullsEqual=false);
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * lhs,
                              const RecordType * rhs,
                              const std::vector<SortKey>& fields,
                              const std::string& name = "eq",
                              bool areNullsEqual=false);
};

class KeyPrefixFunction
{
public:
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * inputType,
                              const std::string& field,
                              const std::string& name = "keyPrefix")
  {
    std::vector<std::string> fields;
    fields.push_back(field);
    return get(ctxt, inputType, fields, name);
  }

  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * inputType,
                              const std::vector<std::string>& fields,
                              const std::string& name = "keyPrefix");
};

class LessThanFunction
{
public:
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * inputType,
                              const std::string& field,
                              const std::string& name = "lessThan")
  {
    std::vector<std::string> fields;
    fields.push_back(field);
    return get(ctxt, inputType, inputType, fields, name);
  }
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * inputType,
                              const std::vector<std::string>& fields,
                              const std::string& name = "lessThan")
  {
    return get(ctxt, inputType, inputType, fields, name);
  }
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * lhs,
                              const RecordType * rhs,
                              const std::vector<std::string>& fields,
                              const std::string& name = "lessThan");
  /**
   * If sortNulls is false comparisons with NULLs use SQL-like 3 valued
   * logic. If sortNulls is true NULLs are treated as either low or high values (e.g. 
   * as is necessary for sorting); by default NULLs sort low but that may
   * be tweaked by a configuration change.
   */
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * lhs,
                              const RecordType * rhs,
                              const std::vector<SortKey>& fields,
                              bool sortNulls,
                              const std::string& name = "lessThan");
};

class CompareFunction
{
public:
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * lhs,
                              const RecordType * rhs,
                              const std::vector<SortKey>& leftFields,
                              const std::vector<SortKey>& rightFields,
                              const std::string& name = "compare");
};

class SortKeyPrefixFunction
{
public:
  static TreculFunction * get(PlanCheckContext & ctxt,
                              const RecordType * input,
                              const std::vector<SortKey>& fields,
                              const std::string& name = "keyPrefix");
};

/**
 * Stores operator types and associations between
 * logical operator ports and operator type ports
 * so that logical plan graphs can be transformed 
 * into graphs of operator types without assuming
 * a 1-1 correspondence between logical ops and operator
 * types.
 * We could also build this into the logical graph
 * as some kind of annotation scheme.
 */
class RuntimePlanBuilder
{
public:
  struct OpTypePort
  {
    RuntimeOperatorType* OpType;
    std::size_t Index;
    OpTypePort(RuntimeOperatorType * opType = NULL,
	       std::size_t opTypePort=0)
      :
      OpType(opType),
      Index(opTypePort)
    {
    }
  };
  struct InternalEdge
  {
    OpTypePort Source;
    OpTypePort Target;
    const RecordType * Type;
    TreculFunctionReference Free;
    bool Buffered;
    InternalEdge()
      :
      Buffered(true)
    {
    }
    InternalEdge(RuntimeOperatorType * sourceOpType,
		 std::size_t sourcePort,
		 RuntimeOperatorType * targetOpType,
		 std::size_t targetPort,
		 bool buffered)
      :
      Source(sourceOpType, sourcePort),
      Target(targetOpType, targetPort),
      Type(nullptr),
      Buffered(buffered)
    {
    }
    InternalEdge(RuntimeOperatorType * sourceOpType,
		 std::size_t sourcePort,
		 RuntimeOperatorType * targetOpType,
		 std::size_t targetPort,
                 const RecordType * ty,
                 const TreculFreeOperation & freeFunctor,
		 bool buffered)
      :
      Source(sourceOpType, sourcePort),
      Target(targetOpType, targetPort),
      Type(ty),
      Free(freeFunctor.getReference()),
      Buffered(buffered)
    {
    }
  };
private:
  std::vector<RuntimeOperatorType *> mOpTypes;
  std::map<LogicalOperator*, std::vector<OpTypePort> > mInputPortMap;
  std::map<LogicalOperator*, std::vector<OpTypePort> > mOutputPortMap;
  std::vector<InternalEdge> mInternalEdges;
  RuntimePartitionConstraint mPartitions;
  
  static void mapPort(LogicalOperator * op, std::size_t port,
		      RuntimeOperatorType * opType, std::size_t opTypePort,
		      std::map<LogicalOperator*, std::vector<OpTypePort> >& portMap)
  {
    if (portMap[op].size() <= port+1)
      portMap[op].resize(port+1);
    portMap[op][port].OpType = opType;
    portMap[op][port].Index = opTypePort;
  }

public:
  RuntimePlanBuilder();

  void setPartitionConstraint(RuntimePartitionConstraint && partitions)
  {
    mPartitions = std::move(partitions);
  }
  
  void addOperatorType(RuntimeOperatorType * opType)
  {
    if (!mPartitions.isDefault()) {
      opType->setPartitionConstraint(mPartitions);
    }
    mOpTypes.push_back(opType);
  }
  /**
   * Bind a RuntimeType port to a corresponding Logical graph port.
   * There is a logical edge attached to the corresponding logical
   * graph port and this will be turned into a runtime edge later.
   */
  void mapInputPort(LogicalOperator * op, std::size_t port,
		    RuntimeOperatorType * opType, std::size_t opTypePort)
  {
    mapPort(op, port, opType, opTypePort, mInputPortMap);
  }
  void mapOutputPort(LogicalOperator * op, std::size_t port,
		    RuntimeOperatorType * opType, std::size_t opTypePort)
  {
    mapPort(op, port, opType, opTypePort, mOutputPortMap);
  }
  /**
   * Create an edge that has no representation in the logical plan.
   */
  void connect(RuntimeOperatorType * sourceType, std::size_t sourcePort,
	       RuntimeOperatorType * targetType, std::size_t targetPort,
	       bool buffered=true)
  {
    mInternalEdges.push_back(InternalEdge(sourceType, sourcePort,
					  targetType, targetPort, 
					  buffered));
  }
  void connect(RuntimeOperatorType * sourceType, std::size_t sourcePort,
	       RuntimeOperatorType * targetType, std::size_t targetPort,
               const RecordType * ty, const TreculFreeOperation & freeFunctor,
	       bool buffered=true)
  {
    mInternalEdges.push_back(InternalEdge(sourceType, sourcePort,
					  targetType, targetPort,
                                          ty, freeFunctor,
					  buffered));
  }
  typedef std::vector<RuntimeOperatorType *>::iterator optype_iterator;
  optype_iterator begin_operator_types() { return mOpTypes.begin(); }
  optype_iterator end_operator_types() { return mOpTypes.end(); }
  std::pair<RuntimeOperatorType *, std::size_t> mapInputPort(LogicalOperator* op, std::size_t port);
  std::pair<RuntimeOperatorType *, std::size_t> mapOutputPort(LogicalOperator* op, std::size_t port);
  typedef std::vector<InternalEdge>::iterator internal_edge_iterator;
  internal_edge_iterator begin_internal_edges() { return mInternalEdges.begin(); }
  internal_edge_iterator end_internal_edges() { return mInternalEdges.end(); }
};

class RuntimeOperatorType;

class RuntimeOperator 
{
public:
  // I don't yet know where Services is going to come from so typedef it here
  typedef DataflowScheduler Services;
  typedef RuntimePort* port_type;
private:
  /**
   * Input and output ports of the operator.
   */
  std::vector<RuntimePort*> mInputPorts;
  std::vector<RuntimePort*> mOutputPorts;
  /**
   * Completion Ports for async service responses
   */
  std::vector<RuntimePort*> mCompletionPorts;
  /**
   * The OperatorType that this RuntimeOperator is created from.
   */
  const RuntimeOperatorType& mOperatorType;
  /**
   * Interfaces to services provided to the operator by the runtime.
   * In particular, read and write requests and read/writes are services
   * provided by the runtime services.
   */
  Services& mServices;
  /**
   * Time spent executing this operator.
   */
  uint64_t mTicks;

protected:
  /**
   * This is being exposed so that an operator may
   * create graphs at run time.  It would be nice
   * to abstract this out a bit.
   */
  Services& getServices()
  {
    return mServices;
  }
  /**
   * Interface that RuntimeOperators use to read and write data.  This is an asynchronous
   * interface.  An operator registers one or more read/write requests through this API,
   * then exists back to the scheduler.  The scheduler then calls back into the operator
   * with the port on which the IO operation has completed and the operator can Read/Write as
   * appropriate.
   * Note that if an operator has request more than one read or write, it only receives
   * a completion notification for one of them and the rest are cancelled (must be reissued).
   */
  void requestRead(std::size_t port)
  {
    BOOST_ASSERT(port < getInputPorts().size());
    // Guarantee that this port is NOT on a request list.
    RuntimePort * p = mInputPorts[port];
    p->request_init();
    mServices.requestRead(p);
  }
  void requestCompletion(std::size_t port)
  {
    BOOST_ASSERT(port < getCompletionPorts().size());
    // Guarantee that this port is NOT on a request list.
    RuntimePort * p = mCompletionPorts[port];
    p->request_init();
    mServices.requestRead(p);
  }
  void requestRead(RuntimePort & ports)
  {
    mServices.requestRead(&ports);
  }
  void read(RuntimePort * port, RecordBuffer& buf)
  {
    mServices.read(port, buf);
  }
  RuntimePort::local_buffer_type& readLocal(RuntimePort * port)
  {
    return mServices.readLocal(port);
  }
  void flushLocal(RuntimePort * port) 
  {
    mServices.flushLocal(port);
  }
  bool readWouldBlock(RuntimePort & ports)
  {
    return mServices.readWouldBlock(ports);
  }
  void requestWrite(int32_t port)
  {
    BOOST_ASSERT(std::size_t(port) < getOutputPorts().size());
    mServices.requestWrite(mOutputPorts[port]);
  }
  void requestWrite(RuntimePort & port)
  {
    mServices.requestWrite(&port);
  }
  void requestWriteThrough(int32_t port)
  {
    BOOST_ASSERT(std::size_t(port) < getOutputPorts().size());
    mServices.requestWriteThrough(mOutputPorts[port]);
  }
  void requestWriteThrough(RuntimePort & port)
  {
    mServices.requestWriteThrough(&port);
  }
  void requestIO(RuntimePort & reads, RuntimePort & writes)
  {
    mServices.requestIO(&reads, &writes);
  }
  void write(RuntimePort * port, RecordBuffer buf, bool flush)
  {
    mServices.write(port, buf, flush);
  }
  void writeAndSync(RuntimePort * port, RecordBuffer buf)
  {
    mServices.writeAndSync(port, buf);
  }
  boost::asio::io_service& getIOService()
  {
    return mServices.getIOService();
  }

  std::vector<RuntimePort*>& getInputPorts()
  {
    return mInputPorts;
  }
  std::vector<RuntimePort*>& getOutputPorts()
  {
    return mOutputPorts;
  }
  std::vector<RuntimePort*>& getCompletionPorts()
  {
    return mCompletionPorts;
  }
  const RuntimeOperatorType & getOperatorType()
  {
    return mOperatorType;
  }
  int32_t getPartition() 
  {
    return mServices.getPartition();
  }
  int32_t getNumPartitions() 
  {
    return mServices.getNumPartitions();
  }
public:
  /**
   * Create the operator instance from the corresponding type.
   */
  RuntimeOperator(Services& services, const RuntimeOperatorType& opType);

  /**
   * Destructor
   */
  virtual ~RuntimeOperator();

  /**
   * Graph building so that this operator can be connected to others.
   */
  void addInputPort(RuntimePort * inputPort) 
  {
    mInputPorts.push_back(inputPort);
  }
  void addOutputPort(RuntimePort * outputPort)
  {
    mOutputPorts.push_back(outputPort);
  }
  void setInputPort(RuntimePort * inputPort, std::size_t pos)
  {
    if (mInputPorts.size() <= pos+1)
      mInputPorts.resize(pos+1, NULL);
    mInputPorts[pos] = inputPort;
  }
  void setOutputPort(RuntimePort * outputPort, std::size_t pos)
  {
    if (mOutputPorts.size() <= pos+1)
      mOutputPorts.resize(pos+1, NULL);
    mOutputPorts[pos] = outputPort;
  }
  void setCompletionPort(RuntimePort * completionPort, std::size_t pos)
  {
    if (mCompletionPorts.size() <= pos+1)
      mCompletionPorts.resize(pos+1, NULL);
    mCompletionPorts[pos] = completionPort;
  }
  std::size_t getNumInputs() const
  {
    return mInputPorts.size();
  }
  std::size_t getNumOutputs() const
  {
    return mOutputPorts.size();
  }

  typedef std::vector<RuntimePort*>::iterator input_port_iterator;
  input_port_iterator input_port_begin() { return mInputPorts.begin(); }
  input_port_iterator input_port_end() { return mInputPorts.end(); }

  typedef std::vector<RuntimePort*>::iterator output_port_iterator;
  output_port_iterator output_port_begin() { return mOutputPorts.begin(); }
  output_port_iterator output_port_end() { return mOutputPorts.end(); }

  typedef std::vector<RuntimePort*>::iterator completion_port_iterator;
  output_port_iterator completion_port_begin() { return mCompletionPorts.begin(); }
  output_port_iterator completion_port_end() { return mCompletionPorts.end(); }

  /** 
   * Perform any initialization before events fire.  Potentially
   * request a write if this operator is a generator of data.
   */
  virtual void start() = 0;

  /**
   * Coroutine method that drives the operator's behavior.
   */
  virtual void onEvent(RuntimePort * port) =0;

  /**
   * shutdown of the scheduler is occurring.
   */
  virtual void shutdown() =0;

  /**
   * Name of the operator.
   */
  const std::string & getName() const
  {
    return mOperatorType.getName();
  }
  
  /**
   * update tick count
   */
  void addTicks(uint64_t ticks)
  {
    mTicks += ticks;
  }
  uint64_t getTicks() const
  {
    return mTicks;
  }
};

template <class _Type>
class RuntimeOperatorBase : public RuntimeOperator
{
protected:
  const _Type & getMyOperatorType() 
  {
    return *static_cast<const _Type *>(&getOperatorType());
  }
public:
  RuntimeOperatorBase(RuntimeOperator::Services & s , const _Type& t) 
    :
    RuntimeOperator(s, *static_cast<const RuntimeOperatorType *>(&t))
  {
  }
};

class LogicalFilter : public LogicalOperator
{
private:
  TreculFreeOperation * mFree;
  TreculFunction * mPredicate;
  int64_t mLimit;
public:
  LogicalFilter();
  ~LogicalFilter();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeFilterOperatorType : public RuntimeOperatorType
{
  friend class RuntimeFilterOperator;
private:
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  TreculFunctionReference mPredicateRef;
  TreculFunctionRuntime mPredicate;
  int64_t mLimit;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mPredicateRef);
    ar & BOOST_SERIALIZATION_NVP(mLimit);
  }
  RuntimeFilterOperatorType()
  {
  }
public:
  /**
   * NULL predicate means TRUE.
   */
  RuntimeFilterOperatorType(const RecordType * input,
			    const TreculFreeOperation & free,
			    TreculFunction * predicate,
			    int64_t limit)
    :
    RuntimeOperatorType("RuntimeFilterOperatorType"),
    mFreeRef(free.getReference()),
    mPredicateRef(nullptr != predicate ? predicate->getReference() : TreculFunctionReference()),
    mLimit(limit)
  {
  }
  ~RuntimeFilterOperatorType() 
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
    if (!mPredicateRef.empty()) {
      mPredicate = m.getFunction<TreculFunctionRuntime>(mPredicateRef);
    }
  }
};

class RuntimeFilterOperator : public RuntimeOperatorBase<RuntimeFilterOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;
  RecordBuffer mInput;
  int64_t mNumRecords;
  class InterpreterContext * mRuntimeContext;
public:
  RuntimeFilterOperator(RuntimeOperator::Services& services, 
			const RuntimeFilterOperatorType& opType);
  ~RuntimeFilterOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class CopyOp : public LogicalOperator
{
private:
  TreculFreeOperation * mFree;
  std::vector<const TreculTransfer *> mTransfers;
  class RuntimeCopyOperatorType * mOpType;
  void init(PlanCheckContext & ctxt,
	    const RecordType * inputType,
	    const std::vector<std::string>& transfers,
	    const std::vector<bool>& pics);
public:
  CopyOp();
  CopyOp(PlanCheckContext & ctxt,
	  const RecordType * inputType,
	  const std::vector<std::string>& transfers);
  ~CopyOp();
  class RuntimeCopyOperatorType & getOpType();
  const RecordType * getOutputType(std::size_t idx) const;
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeCopyOperatorType : public RuntimeOperatorType
{
  friend class RuntimeCopyOperator;
private:
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  std::vector<TreculTransferReference> mTransferRefs;
  std::vector<TreculTransferRuntime> mTransfers;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mTransferRefs);
  }
  RuntimeCopyOperatorType()
  {
  }
public:
  RuntimeCopyOperatorType(const TreculFreeOperation & freeFunctor,
			  const std::vector<const TreculTransfer *>& transfers);
  RuntimeCopyOperatorType(const TreculFreeOperation & freeFunctor,
			  const std::vector<const TreculTransfer *>& transfers,
			  const std::vector<bool>& pics);
  ~RuntimeCopyOperatorType();
  RuntimeOperator * create(RuntimeOperator::Services& s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
    mTransfers.clear();
    mTransfers.resize(mTransferRefs.size());
    for(std::size_t i=0; i<mTransferRefs.size(); ++i) {
      mTransfers[i] = m.getTransfer<TreculTransferRuntime>(mTransferRefs[i]);
    }
  }
};

class RuntimeCopyOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;
  RecordBuffer mInput;
  RuntimeOperator::output_port_iterator mOutputIt;
  class InterpreterContext * mRuntimeContext;
  const RuntimeCopyOperatorType & getCopyType() { return *reinterpret_cast<const RuntimeCopyOperatorType *>(&getOperatorType()); }
public:
  RuntimeCopyOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimeCopyOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalDevNull : public LogicalOperator
{
private:
  TreculFreeOperation * mFree;
public:
  LogicalDevNull();
  ~LogicalDevNull();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeDevNullOperatorType : public RuntimeOperatorType
{
  friend class RuntimeDevNullOperator;
private:
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
  }
  RuntimeDevNullOperatorType()
  {
  }
public:
  RuntimeDevNullOperatorType(const TreculFreeOperation & freeFunctor)
    :
    RuntimeOperatorType("RuntimeDevNullOperatorType"),
    mFreeRef(freeFunctor.getReference())
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
  }
};

class RuntimeDevNullOperator : public RuntimeOperator
{
private:
  enum State { START, READ };
  State mState;
  
  const RuntimeDevNullOperatorType &  getDevNullType()
  {
    return *reinterpret_cast<const RuntimeDevNullOperatorType *>(&getOperatorType());
  }
public:
  RuntimeDevNullOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalPrint : public LogicalOperator
{
private:
  int32_t mNumToPrint;
  int32_t mPrintFrequency;
  TreculPrintOperation * mPrint;
  TreculFunction * mPredicate;
  TreculTransfer * mTransfer;
  TreculFreeOperation * mFree;
public:
  LogicalPrint();
  ~LogicalPrint();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimePrintOperatorType : public RuntimeOperatorType
{
  friend class RuntimePrintOperator;
private:
  const RecordType * mRecordType;
  TreculFunctionReference mPrintRef;
  TreculRecordPrintRuntime mPrint;
  int32_t mNumToPrint;
  int32_t mPrintFrequency;
  TreculFunctionReference mPredicateRef;
  TreculFunctionRuntime mPredicate;
  TreculTransferReference mToPrintRef;
TreculTransferRuntime mToPrint;
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mPrintRef);
    ar & BOOST_SERIALIZATION_NVP(mNumToPrint);
    ar & BOOST_SERIALIZATION_NVP(mPrintFrequency);
    ar & BOOST_SERIALIZATION_NVP(mPredicateRef);
    ar & BOOST_SERIALIZATION_NVP(mToPrintRef);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
  }
  RuntimePrintOperatorType()
    :
    mRecordType(NULL),
    mNumToPrint(0),
    mPrintFrequency(1)
  {
  }
public:
  RuntimePrintOperatorType(const RecordType * ty, const TreculPrintOperation & print, int32_t numToPrint, int32_t printFrequency,
			   const TreculFunction * pred,
			   const TreculTransfer * xfer,
                           const TreculFreeOperation * freeOp)
    :
    RuntimeOperatorType("RuntimePrintOperatorType"),
    mRecordType(ty),
    mPrintRef(print.getReference()),
    mNumToPrint(numToPrint),
    mPrintFrequency(printFrequency),
    mPredicateRef(pred ? pred->getReference() : TreculFunctionReference()),
    mToPrintRef(xfer ? xfer->getReference() : TreculTransferReference()),
    mFreeRef(freeOp ? freeOp->getReference() : TreculFunctionReference())
  {
    BOOST_ASSERT((xfer != nullptr && freeOp != nullptr) || (xfer == nullptr && freeOp == nullptr));
  }
  ~RuntimePrintOperatorType()
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  const RecordType * getOutputType() const { return mRecordType; }
  void loadFunctions(TreculModule & m) override
  {
    mPrint = m.getFunction<TreculRecordPrintRuntime>(mPrintRef);
    if (!mPredicateRef.empty()) {
      mPredicate = m.getFunction<TreculFunctionRuntime>(mPredicateRef);
    }
    if (!mFreeRef.empty()) {
      mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
    }
    if (!mToPrintRef.empty()) {
      mToPrint = m.getTransfer<TreculTransferRuntime>(mToPrintRef);
    }
  }
};

class RuntimePrintOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE };
  State mState;
  int32_t mNumPrinted;
  int32_t mNumToPrint;
  int32_t mPrintFrequency;
  int64_t mNumProcessed;
  RecordBuffer mInput;
  boost::iostreams::stream<boost::iostreams::file_descriptor_sink> mStream;
  class InterpreterContext * mRuntimeContext;
  const RuntimePrintOperatorType &  getPrintType()
  {
    return *reinterpret_cast<const RuntimePrintOperatorType *>(&getOperatorType());
  }
public:
  RuntimePrintOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimePrintOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalGenerate : public LogicalOperator
{
private:
  const RecordType * mInputType;
  TreculFreeOperation * mInputFree;
  const RecordType * mStateType;
  TreculFreeOperation * mStateFree;
  TreculFunction * mNumRecords;
  TreculTransfer2 * mTransfer;

public:
  LogicalGenerate();
  ~LogicalGenerate();
  void init(PlanCheckContext& log,
	    const std::string& output,
	    const std::string& numRecords,
	    const RecordType * input);
  const RecordType * getTarget();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);    
  RuntimeOperatorType * create();
};

class RuntimeGenerateOperatorType : public RuntimeOperatorType
{
  friend class RuntimeGenerateOperator;
private:
  FieldAddress mRecordCount;
  FieldAddress mPartitionCount;
  FieldAddress mPartition;
  TreculFunctionReference mLoopUpperBoundRef;
  TreculFunctionRuntime mLoopUpperBound;
  RecordTypeMalloc mStateMalloc;
  TreculFunctionReference mStateFreeRef;
  TreculRecordFreeRuntime mStateFree;
  TreculFunctionReference mInputFreeRef;
  TreculRecordFreeRuntime mInputFree;
  TreculTransferReference mModuleRef;
  TreculTransfer2Runtime mModule;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mRecordCount);
    ar & BOOST_SERIALIZATION_NVP(mPartitionCount);
    ar & BOOST_SERIALIZATION_NVP(mPartition);
    ar & BOOST_SERIALIZATION_NVP(mLoopUpperBoundRef);
    ar & BOOST_SERIALIZATION_NVP(mStateMalloc);
    ar & BOOST_SERIALIZATION_NVP(mStateFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mInputFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mModuleRef);
  }
  RuntimeGenerateOperatorType()
  {
  }
public:
  RuntimeGenerateOperatorType(const std::string& name,
                              const TreculFreeOperation & inputFreeFunctor,
			      const RecordType * stateType,
                              const TreculFreeOperation & stateFreeFunctor,
                              const TreculTransfer2 & transfer,
			      TreculFunction * upperBound);
  ~RuntimeGenerateOperatorType();

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mStateFree = m.getFunction<TreculRecordFreeRuntime>(mStateFreeRef);
    mInputFree = m.getFunction<TreculRecordFreeRuntime>(mInputFreeRef);
    if (!mLoopUpperBoundRef.empty()) {
      mLoopUpperBound = m.getFunction<TreculFunctionRuntime>(mLoopUpperBoundRef);
    } 
    mModule = m.getTransfer<TreculTransfer2Runtime>(mModuleRef);
 }
};

class RuntimeGenerateOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;
  uint32_t mIter;
  uint32_t mEnd;
  RecordBuffer mInput;
  RecordBuffer mStateRecord;
  class InterpreterContext * mRuntimeContext;
  const RuntimeGenerateOperatorType & getGenerateType() { return *reinterpret_cast<const RuntimeGenerateOperatorType *>(&getOperatorType()); }
public:
  RuntimeGenerateOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimeGenerateOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalGroupBy : public LogicalOperator
{
public:
  enum Algorithm { SORT, HASH, HYBRID };
private:
  std::vector<std::string> mSortGroupKeys;
  std::vector<std::string> mHashGroupKeys;
  std::string mProgram;
  TreculFreeOperation * mFree;  
  TreculAggregate * mAggregate;
  TreculFreeOperation * mAggregateFree;
  TreculFunction * mHash;
  TreculFunction * mHashEq;
  TreculFunction * mSortEq;
  Algorithm mAlgorithm;
  bool mIsRunningTotal;
public:
  LogicalGroupBy(Algorithm a);
  ~LogicalGroupBy();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class AggregateFunctionSpec
{
public:
  enum Kind { COUNT, SUM };
private:
  std::string mExpr;
  Kind mKind;
  std::string mAs;
  FieldType::FieldTypeEnum mType;
public:
  AggregateFunctionSpec(const std::string& expr,
			Kind kind,
			const std::string& as,
			FieldType::FieldTypeEnum ty=FieldType::INT32)
    :
    mExpr(expr),
    mKind(kind),
    mAs(as),
    mType(ty)
  {
  }
  const std::string& getExpr() const { return mExpr; }
  Kind getKind() const { return mKind; }
  const std::string& getAs() const { return mAs; }
  FieldType::FieldTypeEnum getType() const { return mType; }  
};

template <class _OpType>
class GroupBy
{
private:
  TreculAggregate * aggregate;
  TreculFunction * hasher;
  TreculFunction  * equals;
  _OpType * opType2;
  
public:
  GroupBy(PlanCheckContext & ctxt,
	  const RecordType * inputType,
	  const std::vector<std::string>& groupFields,
	  const std::vector<AggregateFunctionSpec>& outputFields);
  ~GroupBy();
  _OpType & getOpType() { return *opType2; }
  const RecordType * getOutputType() const { return aggregate->getTarget(); }
};

template <class _OpType>
GroupBy<_OpType>::GroupBy(PlanCheckContext & ctxt,
		 const RecordType * inputType,
		 const std::vector<std::string>& groupFields,
		 const std::vector<AggregateFunctionSpec>& outputFields)
{
  // Aggregate record contains a copy of group key fields and aggregate function values.
  // With the simple aggregates we support now, we can just initialize to 0.
  std::string initializer;
  for(std::size_t i=0; i<groupFields.size(); i++) {
    if (initializer.size() > 0) initializer += ",";
    initializer += (boost::format("%1%") % groupFields[i]).str();
  }
  for(std::size_t i=0; i<outputFields.size(); i++) {
    if (initializer.size() > 0) initializer += ",";
    initializer += (boost::format("%2% AS %1%") % 
		    outputFields[i].getAs() %
		    (outputFields[i].getType() == FieldType::INT32 ? "0" : "0.0e+00")).str();
  }

  // Build what happens when a record updates the aggregate.
  std::string updater;
  for(std::size_t i=0; i<outputFields.size(); i++) {
    if (updater.size() > 0) updater += "\n";
    switch (outputFields[i].getKind()) {
    case AggregateFunctionSpec::COUNT:
      updater += (boost::format("SET %1% = %1% + 1") % outputFields[i].getAs()).str();
      break;
    case AggregateFunctionSpec::SUM:
      updater += (boost::format("SET %1% = %1% + %2%") % outputFields[i].getAs() % outputFields[i].getExpr()).str();
      break;
    default:
      throw std::runtime_error("Invalid aggregate function kind");
    }
  }  
  aggregate = new TreculAggregate(ctxt,
                                  ctxt.getCodeGenerator(),
                                  "groupby", 
                                  inputType,
                                  initializer,
                                  updater,
                                  groupFields);

  // Hash on all of the group keys
  std::vector<const RecordType *> inputOnly;
  inputOnly.push_back(inputType);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(ctxt, emptyMembers);
  inputOnly.push_back(&emptyTy);
  std::string hashargs;
  for(std::size_t i=0; i<groupFields.size(); i++) {
    if (hashargs.size() > 0) hashargs += ",";
    hashargs += (boost::format("%1%") % groupFields[i]).str();
  }
  hasher = new TreculFunction(ctxt, ctxt.getCodeGenerator(), "xfer5hash", inputOnly, (boost::format("#(%1%)") % hashargs).str());

  // This is kinda hacky.  When we compare two records to each other,
  // we need to refer to one field from each record.  Since we do this
  // by name, we generate a temporary prefix
  // IMPORTANT: for hash table equality, the table type comes first
  // in the list!
  std::vector<const RecordType *> eqTypes;
  eqTypes.push_back(aggregate->getAggregate());
  eqTypes.push_back(inputType);
  std::string eqPred;
  for(std::size_t i=0; i<groupFields.size(); i++) {
    if (eqPred.size() > 0) eqPred += " AND ";
    eqPred += (boost::format("input1.%1% = input0.%1%") % groupFields[i]).str();
  }
  equals = new TreculFunction(ctxt, ctxt.getCodeGenerator(), "xfer5eq", 
				  eqTypes, 
				  eqPred);
  opType2 = new _OpType(TreculFreeOperation(ctxt.getCodeGenerator(), inputType),
                        hasher,
                        equals,
                        *aggregate,
                        TreculFreeOperation(ctxt.getCodeGenerator(), aggregate->getAggregate()));
}

template <class _OpType>
GroupBy<_OpType>::~GroupBy()
{
  delete aggregate;
  delete hasher;
  delete equals;
  delete opType2;
}

struct RecordTypeHasher : std::unary_function<RecordBuffer, std::size_t>
{
  TreculFunctionRuntime::LLVMFuncType Func;
  class InterpreterContext * Context;
  std::size_t operator() (const RecordBuffer & val) const
  {
    int32_t ret;
    ((*Func)((char *)val.Ptr, nullptr, &ret, Context));
    return (std::size_t) ret;
  }

  RecordTypeHasher()
    :
    Context(nullptr)
  {
  }

  RecordTypeHasher(const TreculFunctionRuntime & f,
		   class InterpreterContext * ctxt)
    :
    Func(f.getRawFunction()),
    Context(ctxt)
  {
  }
};

struct RecordTypeEquals : std::binary_function<RecordBuffer, RecordBuffer, bool>
{
  TreculFunctionRuntime::LLVMFuncType Func;
  class InterpreterContext * Context;
  bool operator() (const RecordBuffer & lhs, const RecordBuffer & rhs) const
  {
    int32_t ret;
    ((*Func)( (char *) lhs.Ptr, (char *) rhs.Ptr, &ret, Context));
    return ret != 0;
  }

  RecordTypeEquals()
    :
    Context(nullptr)
  {
  }

  RecordTypeEquals(const TreculFunctionRuntime & f,
		   class InterpreterContext * ctxt)
    :
    Func(f.getRawFunction()),
    Context(ctxt)
  {
  }
};

// struct OffsetVarcharHasher : std::unary_function<RecordBuffer, std::size_t>
// {
//   std::size_t Offset;
//   std::size_t operator() (const RecordBuffer & val) const
//   {
//     const Varchar * tmp = ((const Varchar *) (val.Ptr + Offset));
//     return SuperFastHash(tmp->Ptr, tmp->Size);
//   }
// };

// struct OffsetVarcharEquals : std::binary_function<RecordBuffer, RecordBuffer, bool>
// {
//   std::size_t Offset;
//   bool operator() (const RecordBuffer & lhs, const RecordBuffer & rhs) const
//   {
//     const Varchar * l = ((const Varchar *) (lhs.Ptr + Offset));
//     const Varchar * r = ((const Varchar *) (rhs.Ptr + Offset));
//     return l->Size == r->Size && 0 == memcmp(l->Ptr, r->Ptr, l->Size);
//   }
// };

/**
 * Interesting thing about this hash table is that it supports having different data structures
 * in the table and used as probes (which is of course necessary for database style applications).
 */
class paged_hash_table
{
  friend class scan_iterator;
public:

  // TODO: Make sense of this on x64
  // We want the page of the hash table to be at least 2 cache lines since
  // in many cases we will only need to look at hash values to know that the page doesn't match.
  // TODO: Incorporate bitmap vectors/Bloom filters
  enum { PageSizeLog2 = 7, PageSize=128, PageEntries=10/*PageSize/(sizeof(uint32_t) + sizeof(RecordBuffer))*/, Sentinel= PageEntries - 1 };

  // This is the structure of a page of a bucket of the hash table
  class bucket_page
  {
  public:
    // The first PageEntries-1 bits here are used as
    // markers for when a Value has been matched in a 
    // join.  This is required for outer join processing.
    // The remaining bits can be used for a Bloom filter when
    // I take the time to get that working.
    uint64_t Bitmap;
    uint32_t Hash[PageEntries];
    RecordBuffer Value[PageEntries-1];
    bucket_page * Next;
    bucket_page()
      :
      Bitmap(0),
      Next(NULL)
    {
      memset(&Hash[0], 0, sizeof(Hash));
    }
    bucket_page(uint32_t hashValue, RecordBuffer value)
      :
      Bitmap(0),
      Next(NULL)
    {
      memset(&Hash[0], 0, sizeof(Hash));
      Hash[0] = hashValue;
      Value[0] = value;
    }
    void mark_value(const uint32_t * hashPtr)
    {
      Bitmap |= (1UL << (hashPtr - &Hash[0]));
    }
    bool marked(const uint32_t * hashPtr) const
    {
      return (Bitmap & (1UL << (hashPtr - &Hash[0]))) != 0;
    }
  };

  // class raw_bucket_iterator
  // {
  // private:
  //   // Current Page
  //   bucket_page * mPage;
  //   // Current position within bucket_page::Hash array
  //   uint32_t * mHashPtr;

  //   void increment()
  //   {
  //     if (&mPage->Hash[Sentinel] == mHashPtr) {
  //     }
  //   }
  // public:
  //   raw_bucket_iterator ()
  //     :
  //     mPage(NULL),
  //     mHashPtr(NULL)
  //   {
  //   }
  //   // Preincrement
  //   raw_bucket_iterator& operator++()
  //   {
  //     increment();
  //     return *this;
  //   }
  //   // Postincrement
  //   raw_bucket_iterator operator++(int )
  //   {
  //     raw_bucket_iterator tmp(mPage, mHashPtr);
  //     increment();
  //     return tmp;
  //   }
  // };
  class insert_predicate 
  {
  public:
    TreculFunctionRuntime::LLVMFuncType HashFunc;
    RecordBuffer InsertThis;
    insert_predicate(TreculFunctionRuntime::LLVMFuncType hashFunc)
      :
      HashFunc(hashFunc)
    {
    }
    uint32_t hash(InterpreterContext * ctxt)
    {
      int32_t ret;
      ((*HashFunc)( (char *) InsertThis.Ptr, NULL, &ret, ctxt));
      return (uint32_t) ret;      
    }
    bool equals (RecordBuffer buf, InterpreterContext * )
    {
      return InsertThis.Ptr == buf.Ptr;
    }
  };
  class probe_predicate 
  {
  public:
    TreculFunctionRuntime::LLVMFuncType HashFunc;
    TreculFunctionRuntime::LLVMFuncType EqFunc;
    RecordBuffer ProbeThis;
    probe_predicate(TreculFunctionRuntime::LLVMFuncType hashFunc,
                    TreculFunctionRuntime::LLVMFuncType eqFunc)
      :
      HashFunc(hashFunc),
      EqFunc(eqFunc)
    {
    }
    probe_predicate(const TreculFunctionRuntime & hashFunc,
		    const TreculFunctionRuntime & eqFunc)
      :
      HashFunc(hashFunc.getRawFunction()),
      EqFunc(eqFunc.getRawFunction())
    {
    }
    uint32_t hash(InterpreterContext * ctxt)
    {
      int32_t ret;
      ((*HashFunc)( (char *) ProbeThis.Ptr, NULL, &ret, ctxt));
      return (uint32_t) ret;      
    }
    bool equals (RecordBuffer buf, InterpreterContext * ctxt)
    {
      int32_t tmp;
      (*EqFunc)((char *)buf.Ptr, (char *)ProbeThis.Ptr, &tmp, ctxt);
      return tmp != 0;
    }
  };

  class scan_true_predicate
  {
  public:
    bool test(bucket_page * , uint32_t * hashPtr) 
    {
      return true;
    }
  };

  class scan_not_marked_predicate
  {
  public:
    bool test(bucket_page * p, uint32_t * hashPtr) 
    {
      return !p->marked(hashPtr);
    }
  };

  template<typename _Pred>
  class scan_iterator
  {
  public:
    // Current page
    bucket_page * mPage;
    // Current bucket
    bucket_page * mBucket;
    // Last bucket head + 1
    bucket_page * mBucketEnd;
    // Current position within the bucket_page::Hash array
    uint32_t * mHashPtr;
    // Predicate for testing output
    _Pred mPredicate;
    // State of the coroutine
    enum State { START, NEXT, DONE };
    State mState;

    scan_iterator()
      :
      mPage(NULL),
      mBucket(NULL),
      mBucketEnd(NULL),
      mHashPtr(NULL),
      mState(START)
    {
    }
    scan_iterator(paged_hash_table& table)
      :
      mPage(table.mBuckets),
      mBucket(table.mBuckets),
      mBucketEnd(table.mBuckets + table.mNumBuckets),
      mHashPtr(NULL),
      mState(START)
    {
    }
    void init(paged_hash_table& table)
    {
      mPage = table.mBuckets;
      mBucket = table.mBuckets;
      mBucketEnd = table.mBuckets + table.mNumBuckets;
      mHashPtr = NULL;
      mState = START;
    }
    bucket_page * next_page()
    {
      return mPage;
    }
    void advance_page()
    {
      mPage = mPage->Next;
    }
    // Current value of the location
    RecordBuffer& value()
    {
      return value(mHashPtr, next_page());
    }

    static RecordBuffer& value(uint32_t * hashPtr, bucket_page * p)
    {
      return p->Value[hashPtr - &(p->Hash[0])];      
    }

    bool next(InterpreterContext * ctxt)
    {
      bucket_page * p = next_page();
      switch(mState) {
      case START:
	while(p != NULL) {
	  // Start of page.  Iterate over the page
	  for(mHashPtr = &p->Hash[0]; *mHashPtr != 0; ++mHashPtr) {
	    if (!mPredicate.test(p, mHashPtr))
	      continue;
	    mState = NEXT;
	    return true;
	  case NEXT:
	    ;
	  }

	  // End of bucket page.  Is there another page?
	  if(&p->Hash[Sentinel] == mHashPtr && p->Next) {
	    // We are at the end of a full node.  Advance and continue.
	    advance_page();
	    p = next_page();
	    continue;
	  } else if (++mBucket != mBucketEnd) {
	    // End of a partial page.  This must be the last page of bucket.
	    // Move to next bucket
	    p = mPage = mBucket;
	    continue;
	  } else {
	    // End of a partial page.  This must be the last page of bucket.
	    // Break outta here cause we don't have any more buckets.
	    break;
	  }
	}
	 
	// Safe to call next as many time as you want.  You just keep
	// getting false back.
	do {
	  mState = DONE;
	  return false;
	case DONE:
	  ;
	} while(true);
      }

      // Never get here
      return false;
    }
  };

  typedef scan_iterator<scan_true_predicate> scan_all_iterator;
  typedef scan_iterator<scan_not_marked_predicate> scan_not_marked_iterator;

  // Where am I within the bucket
  template <class _Pred>
  class query_iterator
  {
  public:
    // Current page
    bucket_page * mPage;
    // Current position within the bucket_page::Hash array
    uint32_t * mHashPtr;
    // Am I positioned in front of the first page?
    bool mStutter;
    // The hash value of the record that we are looking up
    uint32_t mQueryHashValue;
    // Equality predicate.  May be much more than just comparing
    // the keys we hashed.
    _Pred mQueryPredicate;
    // State of the coroutine
    enum State { START, NEXT, DONE };
    State mState;

    // Navigation
    // In general, mPage is lagging one behind
    // current.  This makes it easier to do inserts
    // of new pages.  For the first page though
    // we "stutter" and don't advance.
    void advance_page()
    {
      mPage  = next_page();
      mStutter = false;
    }

  public:
    query_iterator()
      :
      mPage(NULL),
      mHashPtr(NULL),
      mStutter(true),
      mQueryHashValue(0),
      mState(START)
    {
    }

    query_iterator(const _Pred& queryPredicate)
      :
      mPage(NULL),
      mHashPtr(NULL),
      mStutter(true),
      mQueryHashValue(0),
      mQueryPredicate(queryPredicate),
      mState(START)
    {
    }

    void init(bucket_page * page,
	      uint32_t queryHashValue)
    {
      mPage=page;
      mHashPtr=NULL;
      mStutter=true;
      mQueryHashValue=queryHashValue;
      mState=START;
    }

    // The value of next page.
    bucket_page * next_page() const 
    {
      return mStutter ? mPage : mPage->Next; 
    }

    // Current value of the location
    RecordBuffer value()
    {
      return value(mHashPtr, next_page());
    }

    static RecordBuffer value(uint32_t * hashPtr, bucket_page * p)
    {
      return p->Value[hashPtr - &(p->Hash[0])];      
    }

    bool next(InterpreterContext * ctxt)
    {
      bucket_page * p = next_page();
      switch(mState) {
      case START:
	while(p != NULL) {
	  // Start of page.  Iterate over the page
	  for(mHashPtr = &p->Hash[0]; *mHashPtr != 0; ++mHashPtr) {
	    BOOST_ASSERT(mHashPtr <= &p->Hash[Sentinel]);
	    if(*mHashPtr == mQueryHashValue) {
	      if(mQueryPredicate.equals(value(mHashPtr, p), ctxt)) {
	      // Match.  Mark as such. Return with true.
		p->mark_value(mHashPtr);
	      mState = NEXT;
	      return true;
	    case NEXT:
	      ;
	      }
	    }
	  }

	  // End of bucket page.  Is there another page?
	  if(&p->Hash[Sentinel] == mHashPtr) {
	    // We are at the end of a full node.  Advance and continue.
	    advance_page();
	    p = next_page();
	    continue;
	  } else {
	    // End of a partial page.  This must be the last page.
	    // Break outta here cause we're done.
	    break;
	  }
	}
	 
	// Safe to call next as many time as you want.  You just keep
	// getting false back.
	do {
	  mState = DONE;
	  return false;
	case DONE:
	  ;
	} while(true);
      }

      // Never get here
      return false;
    }

    void link(bucket_page * page)
    {
      page->Next = mPage->Next;
      mPage->Next = page;
    }

    void set(uint32_t hashValue, RecordBuffer value)
    {
      *mHashPtr = hashValue;
      bucket_page * p = next_page();
      p->Value[mHashPtr - &p->Hash[0]] = value;
    }
  };

private:
  bool mOwnTableData;
  int32_t mNumBuckets;
  bucket_page * mBuckets;
  // Table hash function
  insert_predicate mTableHash;
  double mLoadFactor;
  int64_t mSize;

  static void insert(bucket_page * p,
		     RecordBuffer tableInput,
		     uint32_t h);
public:
  paged_hash_table(bool ownTableData, const TreculFunctionRuntime & tableHash);
  ~paged_hash_table();
  void insert(RecordBuffer tableInput, InterpreterContext * ctxt);
  // TODO: Templatize on probe_predicate (this should also be used in insert with
  // and insert/table predicate).
  void insert(RecordBuffer tableInput, 
	      query_iterator<probe_predicate> & it);
  void find(query_iterator<probe_predicate> & it, InterpreterContext * ctxt);
  // Increase number of buckets due to load factor
  void grow(double growthFactor);
  void clear();
};

class RuntimeHashGroupByOperatorType : public RuntimeOperatorType
{
public:
  typedef RecordTypeEquals equals_type;
  typedef RecordTypeHasher hasher_type;
  friend class RuntimeHashGroupByOperator;
  friend class RuntimeHybridGroupByOperator;
private:
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  TreculFunctionReference mAggregateFreeRef;
  TreculRecordFreeRuntime mAggregateFree;
  TreculAggregateReference mAggregateRef;
TreculAggregateRuntime mAggregate;
  TreculFunctionReference mHashFunRef;
  TreculFunctionRuntime mHashFun;
  TreculFunctionReference mHashKeyEqFunRef;
  TreculFunctionRuntime mHashKeyEqFun;
  TreculFunctionReference mSortKeyEqFunRef;
  TreculFunctionRuntime mSortKeyEqFun;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mAggregateFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mAggregateRef);
    ar & BOOST_SERIALIZATION_NVP(mHashFunRef);
    ar & BOOST_SERIALIZATION_NVP(mHashKeyEqFunRef);
    ar & BOOST_SERIALIZATION_NVP(mSortKeyEqFunRef);
  }
  RuntimeHashGroupByOperatorType()
  {
  }  
public:
  RuntimeHashGroupByOperatorType(const TreculFreeOperation & freeFunctor, 
				 const TreculFunction & hashFun,
				 const TreculFunction & hashEqFun,
				 const TreculAggregate & agg,
                                 const TreculFreeOperation & aggregateFreeFunctor, 
				 const TreculFunction * sortEqFun=NULL)
    :
    RuntimeOperatorType("RuntimeHashGroupByOperatorType"),
    mFreeRef(freeFunctor.getReference()),
    mAggregateFreeRef(aggregateFreeFunctor.getReference()),
    mAggregateRef(agg.getReference()),
    mHashFunRef(hashFun.getReference()),
    mHashKeyEqFunRef(hashEqFun.getReference()),
    mSortKeyEqFunRef(sortEqFun ? sortEqFun->getReference() : TreculFunctionReference())
  {
  }
  RuntimeHashGroupByOperatorType(const TreculFreeOperation & freeFunctor, 
				 const TreculFunction * hashFun,
				 const TreculFunction * hashEqFun,
				 const TreculAggregate & agg,
                                 const TreculFreeOperation & aggregateFreeFunctor)
    :
    RuntimeHashGroupByOperatorType(freeFunctor, *hashFun, *hashEqFun, agg, aggregateFreeFunctor, nullptr)
  {
  }
  ~RuntimeHashGroupByOperatorType()
  {
  }

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
    mAggregateFree = m.getFunction<TreculRecordFreeRuntime>(mAggregateFreeRef);
    mAggregate = m.getAggregate(mAggregateRef);
    mHashFun = m.getFunction<TreculFunctionRuntime>(mHashFunRef);
    mHashKeyEqFun = m.getFunction<TreculFunctionRuntime>(mHashKeyEqFunRef);
    if (!mSortKeyEqFunRef.empty()) {
      mSortKeyEqFun = m.getFunction<TreculFunctionRuntime>(mSortKeyEqFunRef);
    }
  }
};

class RuntimeHashGroupByOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  paged_hash_table mTable;
  paged_hash_table::query_iterator<paged_hash_table::probe_predicate> mSearchIterator;
  paged_hash_table::scan_all_iterator mScanIterator;
  const RuntimeHashGroupByOperatorType & getHashGroupByType() { return *reinterpret_cast<const RuntimeHashGroupByOperatorType *>(&getOperatorType()); }
public:
  RuntimeHashGroupByOperator(RuntimeOperator::Services& services, const RuntimeHashGroupByOperatorType& opType);
  ~RuntimeHashGroupByOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class RuntimeSortGroupByOperatorType : public RuntimeOperatorType
{
public:
  typedef RecordTypeEquals equals_type;
  typedef RecordTypeHasher hasher_type;
  friend class RuntimeSortGroupByOperator;
private:
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  TreculFunctionReference mAggregateFreeRef;
  TreculRecordFreeRuntime mAggregateFree;
  TreculAggregateReference mAggregateRef;
TreculAggregateRuntime mAggregate;
  TreculFunctionReference mEqFunRef;
  TreculFunctionRuntime mEqFun;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mAggregateFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mAggregateRef);
    ar & BOOST_SERIALIZATION_NVP(mEqFunRef);
  }
  RuntimeSortGroupByOperatorType()
  {
  }  
public:
  RuntimeSortGroupByOperatorType(const TreculFreeOperation & freeFunctor, 
				 const TreculFunction * hashFun,
				 const TreculFunction * eqFun,
				 const TreculAggregate & agg,
                                 const TreculFreeOperation & aggregateFreeFunctor)
    :
    RuntimeOperatorType("RuntimeSortGroupByOperatorType"),
    mFreeRef(freeFunctor.getReference()),
    mAggregateFreeRef(aggregateFreeFunctor.getReference()),
    mAggregateRef(agg.getReference()),
    mEqFunRef(eqFun != nullptr ? eqFun->getReference() : TreculFunctionReference())
  {
    boost::ignore_unused(hashFun);
  }
  ~RuntimeSortGroupByOperatorType()
  {
  }

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
    mAggregateFree = m.getFunction<TreculRecordFreeRuntime>(mAggregateFreeRef);
    mAggregate = m.getAggregate(mAggregateRef);
    if (!mEqFunRef.empty()) {
      mEqFun = m.getFunction<TreculFunctionRuntime>(mEqFunRef);
    }
  }
};

class RuntimeSortGroupByOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  RecordBuffer mInput;
  RecordBuffer mCurrentAggregate;
  const RuntimeSortGroupByOperatorType & getSortGroupByType() { return *reinterpret_cast<const RuntimeSortGroupByOperatorType *>(&getOperatorType()); }
public:
  RuntimeSortGroupByOperator(RuntimeOperator::Services& services, const RuntimeSortGroupByOperatorType& opType);
  ~RuntimeSortGroupByOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class RuntimeSortRunningTotalOperatorType : public RuntimeOperatorType
{
public:
  typedef RecordTypeEquals equals_type;
  typedef RecordTypeHasher hasher_type;
  friend class RuntimeSortRunningTotalOperator;
private:
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  TreculFunctionReference mAggregateFreeRef;
  TreculRecordFreeRuntime mAggregateFree;
  TreculAggregateReference mAggregateRef;
TreculAggregateRuntime mAggregate;
  TreculFunctionReference mEqFunRef;
  TreculFunctionRuntime mEqFun;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mAggregateFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mAggregateRef);
    ar & BOOST_SERIALIZATION_NVP(mEqFunRef);
  }
  RuntimeSortRunningTotalOperatorType()
  {
  }  
public:
  RuntimeSortRunningTotalOperatorType(const TreculFreeOperation & freeFunctor, 
				      const TreculFunction * hashFun,
				      const TreculFunction * eqFun,
				      const TreculAggregate & agg,
                                      const TreculFreeOperation & aggregateFreeFunctor)
    :
    RuntimeOperatorType("RuntimeSortRunningTotalOperatorType"),
    mFreeRef(freeFunctor.getReference()),
    mAggregateFreeRef(aggregateFreeFunctor.getReference()),
    mAggregateRef(agg.getReference()),
    mEqFunRef(eqFun != nullptr ? eqFun->getReference() : TreculFunctionReference())
  {
    boost::ignore_unused(hashFun);
  }
  ~RuntimeSortRunningTotalOperatorType()
  {
  }

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
    mAggregateFree = m.getFunction<TreculRecordFreeRuntime>(mAggregateFreeRef);
    mAggregate = m.getAggregate(mAggregateRef);
    if (!mEqFunRef.empty()) {
      mEqFun = m.getFunction<TreculFunctionRuntime>(mEqFunRef);      
    }
  }
};

class RuntimeSortRunningTotalOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  RecordBuffer mInput;
  RecordBuffer mCurrentAggregate;
  const RuntimeSortRunningTotalOperatorType & getSortRunningTotalType() { return *reinterpret_cast<const RuntimeSortRunningTotalOperatorType *>(&getOperatorType()); }
public:
  RuntimeSortRunningTotalOperator(RuntimeOperator::Services& services, const RuntimeSortRunningTotalOperatorType& opType);
  ~RuntimeSortRunningTotalOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class equals_func
{
public:
  TreculFunctionRuntime::LLVMFuncType Func;
  bool operator() (RecordBuffer lhs, RecordBuffer rhs, class InterpreterContext * ctxt) const
  {
    int32_t ret;
    ((*Func)( (char *) lhs.Ptr, (char *) rhs.Ptr, &ret, ctxt));
    return ret != 0;
  }
};

class hash_func
{
public:
  TreculFunctionRuntime::LLVMFuncType Func;
  uint32_t operator() (RecordBuffer buf, class InterpreterContext * ctxt)
  {
    int32_t ret;
    ((*Func)( (char *) buf.Ptr, NULL, &ret, ctxt));
    return (uint32_t) ret;
  }
};

class HashJoin : public LogicalOperator
{
public:
  enum { TABLE_PORT=0, PROBE_PORT=1 };
  enum JoinType { INNER, FULL_OUTER, LEFT_OUTER, RIGHT_OUTER, RIGHT_SEMI, RIGHT_ANTI_SEMI };
private:
  const RecordType * mTableInput;
  const RecordType * mProbeInput;
  TreculFreeOperation * mTableFree;
  TreculFreeOperation * mProbeFree;
  TreculFunction * mTableHash;
  TreculFunction * mProbeHash;
  TreculFunction * mEq;
  TreculTransfer2 * mTransfer;
  TreculTransfer * mSemiJoinTransfer;
  TreculTransfer * mTableMakeNullableTransfer;
  TreculTransfer * mProbeMakeNullableTransfer;
  TreculFreeOperation * mTableMakeNullableFree;
  TreculFreeOperation * mProbeMakeNullableFree;
  JoinType mJoinType;
  bool mJoinOne;

  void init(PlanCheckContext & ctxt,
	    const std::vector<std::string>& tableKeys,
	    const std::vector<std::string>& probeKeys,
	    const std::string& residual,
	    const std::string& transfer);
public:
  HashJoin(JoinType joinType);
  HashJoin(PlanCheckContext & ctxt,
	   const RecordType * tableInput,
	   const RecordType * probeInput,
	   const std::string& tableKey,
	   const std::string& probeKey,
	   const std::string& residual,
	   const std::string& transfer,
	   bool joinOne=false);
  HashJoin(PlanCheckContext & ctxt,
	   HashJoin::JoinType joinType,
	   const RecordType * tableInput,
	   const RecordType * probeInput,
	   const std::string& tableKey,
	   const std::string& probeKey,
	   const std::string& residual,
	   const std::string& transfer);
  HashJoin(PlanCheckContext & ctxt,
	   const RecordType * tableInput,
	   const RecordType * probeInput,
	   const std::vector<std::string>& tableKeys,
	   const std::vector<std::string>& probeKeys,
	   const std::string& residual,
	   const std::string& transfer,
	   bool joinOne=false);
  ~HashJoin();

  const RecordType * getOutputType() const 
  {
    return mTransfer->getTarget();
  }

  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
  RuntimeOperatorType * create() const;
};

class RuntimeHashJoinOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeHashJoinOperator;
private:
  TreculFunctionReference mTableFreeRef;
  TreculRecordFreeRuntime mTableFree;
  TreculFunctionReference mProbeFreeRef;
  TreculRecordFreeRuntime mProbeFree;
  TreculFunctionReference mTableHashFunRef;
  TreculFunctionRuntime mTableHashFun;
  TreculFunctionReference mProbeHashFunRef;
  TreculFunctionRuntime mProbeHashFun;
  TreculFunctionReference mEqFunRef;
  TreculFunctionRuntime mEqFun;
  TreculTransferReference mTransferModuleRef;
TreculTransfer2Runtime mTransferModule;
  TreculTransferReference mSemiJoinTransferModuleRef;
TreculTransferRuntime mSemiJoinTransferModule;
  HashJoin::JoinType mJoinType;
  bool mJoinOne;
  TreculTransferReference mProbeMakeNullableTransferModuleRef;
TreculTransferRuntime mProbeMakeNullableTransferModule;
  RecordTypeMalloc mProbeNullMalloc;
  TreculFunctionReference mProbeNullFreeRef;
  TreculRecordFreeRuntime mProbeNullFree;
  TreculTransferReference mTableMakeNullableTransferModuleRef;
TreculTransferRuntime mTableMakeNullableTransferModule;
  RecordTypeMalloc mTableNullMalloc;
  TreculFunctionReference mTableNullFreeRef;
  TreculRecordFreeRuntime mTableNullFree;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mTableFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mProbeFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mTableHashFunRef);
    ar & BOOST_SERIALIZATION_NVP(mProbeHashFunRef);
    ar & BOOST_SERIALIZATION_NVP(mEqFunRef);
    ar & BOOST_SERIALIZATION_NVP(mTransferModuleRef);
    ar & BOOST_SERIALIZATION_NVP(mSemiJoinTransferModuleRef);
    ar & BOOST_SERIALIZATION_NVP(mJoinType);
    ar & BOOST_SERIALIZATION_NVP(mJoinOne);
    ar & BOOST_SERIALIZATION_NVP(mProbeMakeNullableTransferModuleRef);
    ar & BOOST_SERIALIZATION_NVP(mProbeNullMalloc);    
    ar & BOOST_SERIALIZATION_NVP(mProbeNullFreeRef);    
    ar & BOOST_SERIALIZATION_NVP(mTableMakeNullableTransferModuleRef);
    ar & BOOST_SERIALIZATION_NVP(mTableNullMalloc);    
    ar & BOOST_SERIALIZATION_NVP(mTableNullFreeRef);    
  }
  RuntimeHashJoinOperatorType()
    :
    mJoinType(HashJoin::INNER),
    mJoinOne(false)
  {
  }
public:
  RuntimeHashJoinOperatorType(const TreculFreeOperation & tableFreeFunctor, 
			      const TreculFreeOperation & probeFreeFunctor,
			      const TreculFunction & tableHashFun,
			      const TreculFunction & probeHashFun,
			      const TreculFunction & eqFun,
			      const TreculTransfer2 & transferFun,
			      bool joinOne=false)
    :
    RuntimeOperatorType("RuntimeHashJoinOperatorType"),
    mTableFreeRef(tableFreeFunctor.getReference()),
    mProbeFreeRef(probeFreeFunctor.getReference()),
    mTableHashFunRef(tableHashFun.getReference()),
    mProbeHashFunRef(probeHashFun.getReference()),
    mEqFunRef(eqFun.getReference()),
    mTransferModuleRef(transferFun.getReference()),
    mJoinType(HashJoin::INNER),
    mJoinOne(joinOne)
  {
  }
  RuntimeHashJoinOperatorType(const TreculFreeOperation & tableFreeFunctor, 
			      const TreculFreeOperation & probeFreeFunctor,
			      const TreculFunction & tableHashFun,
			      const TreculFunction & probeHashFun,
			      const TreculFunction & eqFun,
			      const TreculTransfer * transferFun,
			      HashJoin::JoinType joinType)
    :
    RuntimeOperatorType("RuntimeHashJoinOperatorType"),
    mTableFreeRef(tableFreeFunctor.getReference()),
    mProbeFreeRef(probeFreeFunctor.getReference()),
    mTableHashFunRef(tableHashFun.getReference()),
    mProbeHashFunRef(probeHashFun.getReference()),
    mEqFunRef(eqFun.getReference()),
    mSemiJoinTransferModuleRef(transferFun && !transferFun->isIdentity() ? 
                               transferFun->getReference() : TreculTransferReference()),
    mJoinType(joinType),
    mJoinOne(false)
  {
  }
  RuntimeHashJoinOperatorType(HashJoin::JoinType joinType,
			      const TreculFreeOperation & tableFreeFunctor, 
			      const TreculFreeOperation & probeFreeFunctor,
			      const TreculFunction & tableHashFun,
			      const TreculFunction & probeHashFun,
			      const TreculFunction & eqFun,
			      const TreculTransfer2 & transfer2Fun,
			      const TreculTransfer & tableNullableTransferFun,
			      const TreculTransfer & probeNullableTransferFun,
                              const TreculFreeOperation & tableNullableFreeFunctor,
                              const TreculFreeOperation & probeNullableFreeFunctor)
    :
    RuntimeOperatorType("RuntimeHashJoinOperatorType"),
    mTableFreeRef(tableFreeFunctor.getReference()),
    mProbeFreeRef(probeFreeFunctor.getReference()),
    mTableHashFunRef(tableHashFun.getReference()),
    mProbeHashFunRef(probeHashFun.getReference()),
    mEqFunRef(eqFun.getReference()),
    mTransferModuleRef(transfer2Fun.getReference()),
    mJoinType(joinType),
    mJoinOne(false),
    mProbeMakeNullableTransferModuleRef(probeNullableTransferFun.getReference()),
    mProbeNullMalloc(probeNullableTransferFun.getTarget()->getMalloc()),
    mProbeNullFreeRef(probeNullableFreeFunctor.getReference()),
    mTableMakeNullableTransferModuleRef(tableNullableTransferFun.getReference()),
    mTableNullMalloc(tableNullableTransferFun.getTarget()->getMalloc()),
    mTableNullFreeRef(tableNullableFreeFunctor.getReference())
  {
  }
  
  ~RuntimeHashJoinOperatorType();

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mTableFree = m.getFunction<TreculRecordFreeRuntime>(mTableFreeRef);
    mProbeFree = m.getFunction<TreculRecordFreeRuntime>(mProbeFreeRef);
    if (!mTableNullFreeRef.empty()) {
      mTableNullFree = m.getFunction<TreculRecordFreeRuntime>(mTableNullFreeRef);
    }
    if (!mProbeNullFreeRef.empty()) {
      mProbeNullFree = m.getFunction<TreculRecordFreeRuntime>(mProbeNullFreeRef);
    }
    mTableHashFun = m.getFunction<TreculFunctionRuntime>(mTableHashFunRef);
    mProbeHashFun = m.getFunction<TreculFunctionRuntime>(mProbeHashFunRef);
    mEqFun = m.getFunction<TreculFunctionRuntime>(mEqFunRef);
    if (!mTransferModuleRef.empty()) {
      mTransferModule = m.getTransfer<TreculTransfer2Runtime>(mTransferModuleRef);
    }
    if (!mSemiJoinTransferModuleRef.empty()) {
      mSemiJoinTransferModule = m.getTransfer<TreculTransferRuntime>(mSemiJoinTransferModuleRef);
    }
    if (!mProbeMakeNullableTransferModuleRef.empty()) {
      mProbeMakeNullableTransferModule = m.getTransfer<TreculTransferRuntime>(mProbeMakeNullableTransferModuleRef);
    }
    if (!mTableMakeNullableTransferModuleRef.empty()) {
      mTableMakeNullableTransferModule = m.getTransfer<TreculTransferRuntime>(mTableMakeNullableTransferModuleRef);
    }
  }
};

class RuntimeHashJoinOperator : public RuntimeOperator
{
private:
  enum State { START, READ_TABLE, READ_PROBE, WRITE, WRITE_UNMATCHED, WRITE_TABLE_UNMATCHED, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  paged_hash_table mTable;
  paged_hash_table::query_iterator<paged_hash_table::probe_predicate> mSearchIterator;
  RecordBuffer mNullProbeRecord;
  RecordBuffer mNullTableRecord;
  paged_hash_table::scan_not_marked_iterator mScanIterator;
  const RuntimeHashJoinOperatorType & getHashJoinType() { return *reinterpret_cast<const RuntimeHashJoinOperatorType *>(&getOperatorType()); }

  RecordBuffer onTableNonMatch(RecordBuffer tableBuf);
  RecordBuffer onRightOuterMatch();
  RecordBuffer onRightOuterNonMatch();
  RecordBuffer onInner();
  RecordBuffer onSemi();
public:
  RuntimeHashJoinOperator(RuntimeOperator::Services& services, const RuntimeHashJoinOperatorType& opType);
  ~RuntimeHashJoinOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class RuntimeCrossJoinOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeCrossJoinOperator;
private:
  TreculFunctionReference mTableFreeRef;
  TreculRecordFreeRuntime mTableFree;
  TreculFunctionReference mProbeFreeRef;
  TreculRecordFreeRuntime mProbeFree;
  TreculFunctionReference mEqFunRef;
  TreculFunctionRuntime mEqFun;
  TreculTransferReference mTransferModuleRef;
TreculTransfer2Runtime mTransferModule;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mTableFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mProbeFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mEqFunRef);
    ar & BOOST_SERIALIZATION_NVP(mTransferModuleRef);
  }
  RuntimeCrossJoinOperatorType()
  {
  }
public:
  RuntimeCrossJoinOperatorType(const TreculFreeOperation & tableFreeFunctor, 
			       const TreculFreeOperation & probeFreeFunctor,
			       const TreculFunction * eqFun,
			       const TreculTransfer2 & transferFun)
    :
    RuntimeOperatorType("RuntimeCrossJoinOperatorType"),
    mTableFreeRef(tableFreeFunctor.getReference()),
    mProbeFreeRef(probeFreeFunctor.getReference()),
    mEqFunRef(eqFun ? eqFun->getReference() : TreculFunctionReference()),
    mTransferModuleRef(transferFun.getReference())
  {
  }
  
  ~RuntimeCrossJoinOperatorType();

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mTableFree = m.getFunction<TreculRecordFreeRuntime>(mTableFreeRef);
    mProbeFree = m.getFunction<TreculRecordFreeRuntime>(mProbeFreeRef);
    if (!mEqFunRef.empty()) {
      mEqFun = m.getFunction<TreculFunctionRuntime>(mEqFunRef);
    }
    mTransferModule = m.getTransfer<TreculTransfer2Runtime>(mTransferModuleRef);
  }
};

class RuntimeCrossJoinOperator : public RuntimeOperator
{
private:
  enum State { START, READ_TABLE, READ_PROBE, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  RecordBuffer mProbe;
  std::vector<RecordBuffer> mTable;
  std::vector<RecordBuffer>::iterator mScanIterator;
  const RuntimeCrossJoinOperatorType & getCrossJoinType() { return *reinterpret_cast<const RuntimeCrossJoinOperatorType *>(&getOperatorType()); }
public:
  RuntimeCrossJoinOperator(RuntimeOperator::Services& services, const RuntimeCrossJoinOperatorType& opType);
  ~RuntimeCrossJoinOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalExchange : public LogicalOperator
{  
private:
  TreculFunction * mHashFunction;
  TreculFreeOperation * mFree;
  TreculFunction * mKeyPrefix;
  TreculFunction * mKeyEq;
public:
  LogicalExchange();
  ~LogicalExchange();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class LogicalPartition : public LogicalOperator
{  
private:
  TreculFunction * mHashFunction;
  TreculFreeOperation * mFree;
public:
  LogicalPartition();
  ~LogicalPartition();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeHashPartitionerOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeHashPartitionerOperator;
private:
  TreculFunctionReference mHashFunRef;
  TreculFunctionRuntime mHashFun;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mHashFunRef);
  }
  RuntimeHashPartitionerOperatorType()
  {
  }
public:
  RuntimeHashPartitionerOperatorType(const TreculFunction & hashFun)
    :
    RuntimeOperatorType("RuntimeHashPartitionerOperatorType"),
    mHashFunRef(hashFun.getReference())
  {
  }
  ~RuntimeHashPartitionerOperatorType();
  bool isPartitioner() const { return true; }
  void loadFunctions(TreculModule & m) override
  {
    mHashFun = m.getFunction<TreculFunctionRuntime>(mHashFunRef);
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

class RuntimeHashPartitionerOperator : public RuntimeOperatorBase<RuntimeHashPartitionerOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  RecordBuffer mBuffer;
  output_port_iterator mOutputIt;
public:
  RuntimeHashPartitionerOperator(RuntimeOperator::Services& services, const RuntimeHashPartitionerOperatorType& opType);
  ~RuntimeHashPartitionerOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class RuntimeBroadcastPartitionerOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeBroadcastPartitionerOperator;
private:
  TreculTransferReference mTransferRef;
TreculTransferRuntime mTransfer;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mTransferRef);
  }
  RuntimeBroadcastPartitionerOperatorType()
  {
  }
public:
  RuntimeBroadcastPartitionerOperatorType(const TreculTransfer & xfer)
    :
    RuntimeOperatorType("RuntimeBroadcastPartitionerOperatorType"),
    mTransferRef(xfer.getReference())
  {
  }
  ~RuntimeBroadcastPartitionerOperatorType();
  bool isPartitioner() const { return true; }
  void loadFunctions(TreculModule & m) override
  {
    mTransfer = m.getTransfer<TreculTransferRuntime>(mTransferRef);
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

class RuntimeBroadcastPartitionerOperator : public RuntimeOperatorBase<RuntimeBroadcastPartitionerOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;
  class InterpreterContext * mRuntimeContext;
  RecordBuffer mInput;
  output_port_iterator mOutputIt;
public:
  RuntimeBroadcastPartitionerOperator(RuntimeOperator::Services& services, const RuntimeBroadcastPartitionerOperatorType& opType);
  ~RuntimeBroadcastPartitionerOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalCollect : public LogicalOperator
{
private:
  TreculFunction * mKeyPrefix;
  TreculFunction * mKeyEq;
public:
  LogicalCollect();
  ~LogicalCollect();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

template <class OpType>
class RuntimeNondeterministicCollectorOperator;

class RuntimeNondeterministicCollectorOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeNondeterministicCollectorOperator<RuntimeNondeterministicCollectorOperatorType>;
private:
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
  }
public:
  RuntimeNondeterministicCollectorOperatorType()
    :
    RuntimeOperatorType("RuntimeNondeterministicCollectorOperatorType")
  {
  }
  ~RuntimeNondeterministicCollectorOperatorType();
  bool isCollector() const { return true; }
  void loadFunctions(TreculModule & m) override
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

class SortMergeJoin : public LogicalOperator
{
public:
  enum JoinType { INNER, FULL_OUTER, LEFT_OUTER, RIGHT_OUTER, RIGHT_SEMI, RIGHT_ANTI_SEMI };
  static bool isInnerOrOuter(JoinType joinType);
  static TreculTransfer * makeNullableTransfer(PlanCheckContext& ctxt,
						   const RecordType * input);

private:
  JoinType mJoinType;
  const RecordType * mLeftInput;
  const RecordType * mRightInput;
  TreculFreeOperation * mLeftFree;
  TreculFreeOperation * mRightFree;
  TreculFunction * mLeftKeyCompare;
  TreculFunction * mRightKeyCompare;
  TreculFunction * mLeftRightKeyCompare;
  TreculFunction * mResidual;
  TreculTransfer2 * mMatchTransfer;
  TreculTransfer * mLeftMakeNullableTransfer;
  TreculTransfer * mRightMakeNullableTransfer;
  TreculFreeOperation * mLeftMakeNullableFree;
  TreculFreeOperation * mRightMakeNullableFree;

  void init(PlanCheckContext & ctxt,
	    const std::vector<SortKey>& leftKeys,
	    const std::vector<SortKey>& rightKeys,
	    const std::string& residual,
	    const std::string& matchTransfer);
public:
  SortMergeJoin(JoinType joinType);
  SortMergeJoin(PlanCheckContext & ctxt,
		JoinType joinType,
		const RecordType * leftInput,
		const RecordType * rightInput,
		const std::vector<SortKey>& leftKeys,
		const std::vector<SortKey>& rightKeys,
		const std::string& residual,
		const std::string& matchTransfer);
  ~SortMergeJoin();
  const RecordType * getOutputType() const;
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
  RuntimeOperatorType * create() const;
};

class RuntimeSortMergeJoinOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeSortMergeJoinOperator;
  enum InputPorts { LEFT_PORT=0, RIGHT_PORT=1 };
private:
  // The type of join.
  SortMergeJoin::JoinType mJoinType;
  // Compare keys in two records from the
  // left input.
  TreculFunctionReference mLeftKeyCompareFunRef;
  TreculFunctionRuntime mLeftKeyCompareFun;
  // Compare keys in two records from the
  // right input.
  TreculFunctionReference mRightKeyCompareFunRef;
  TreculFunctionRuntime mRightKeyCompareFun;
  // Compare keys in one record from the left
  // with one from the right input. Do we need this?
  TreculFunctionReference mLeftRightKeyCompareFunRef;
  TreculFunctionRuntime mLeftRightKeyCompareFun;
  // Compare record from the left
  // with one from the right input.  
  TreculFunctionReference mEqFunRef;
  TreculFunctionRuntime mEqFun;
  // What to do when we have a match.
  TreculTransferReference mMatchTransferRef;
TreculTransfer2Runtime mMatchTransfer;
  // For right/full outer joins, coerce left inputs
  // to be nullable as needed.
  // For (anti) semi joins we use this to transfer.
  TreculTransferReference mLeftMakeNullableTransferRef;
TreculTransferRuntime mLeftMakeNullableTransfer;
  // For left/full outer joins, coerce right inputs
  // to be nullable as needed.
  TreculTransferReference mRightMakeNullableTransferRef;
TreculTransferRuntime mRightMakeNullableTransfer;
  // How to create new records.
  // How to free
  TreculFunctionReference mLeftFreeRef;
  TreculRecordFreeRuntime mLeftFree;
  TreculFunctionReference mRightFreeRef;
  TreculRecordFreeRuntime mRightFree;
  // Create and delete nullable left records
  // for outer joins.
  RecordTypeMalloc mLeftNullMalloc;
  TreculFunctionReference mLeftNullFreeRef;
  TreculRecordFreeRuntime mLeftNullFree;
  // Create and delete nullable right records
  // for outer joins.
  RecordTypeMalloc mRightNullMalloc;
  TreculFunctionReference mRightNullFreeRef;
  TreculRecordFreeRuntime mRightNullFree;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mJoinType);
    ar & BOOST_SERIALIZATION_NVP(mLeftKeyCompareFunRef);
    ar & BOOST_SERIALIZATION_NVP(mRightKeyCompareFunRef);
    ar & BOOST_SERIALIZATION_NVP(mLeftRightKeyCompareFunRef);
    ar & BOOST_SERIALIZATION_NVP(mEqFunRef);    
    ar & BOOST_SERIALIZATION_NVP(mMatchTransferRef);    
    ar & BOOST_SERIALIZATION_NVP(mLeftMakeNullableTransferRef);    
    ar & BOOST_SERIALIZATION_NVP(mRightMakeNullableTransferRef);    
    ar & BOOST_SERIALIZATION_NVP(mLeftFreeRef);    
    ar & BOOST_SERIALIZATION_NVP(mRightFreeRef);    
    ar & BOOST_SERIALIZATION_NVP(mLeftNullMalloc);    
    ar & BOOST_SERIALIZATION_NVP(mLeftNullFreeRef);    
    ar & BOOST_SERIALIZATION_NVP(mRightNullMalloc);    
    ar & BOOST_SERIALIZATION_NVP(mRightNullFreeRef);    
  }
  RuntimeSortMergeJoinOperatorType()
    :
    RuntimeOperatorType("RuntimeSortMergeJoinOperatorType"),
    mJoinType(SortMergeJoin::INNER)
  {
  }
public:
  RuntimeSortMergeJoinOperatorType(SortMergeJoin::JoinType joinType,
				   const RecordType* leftInput,
				   const RecordType * rightInput,
                                   const TreculFreeOperation & leftFreeFunctor,
                                   const TreculFreeOperation & rightFreeFunctor,
				   const TreculFunction & leftKeyCompare,
				   const TreculFunction & rightKeyCompare,
				   const TreculFunction & leftRightKeyCompare,
				   const TreculFunction * eqFun,
				   const TreculTransfer2 * matchTransfer,
				   const TreculTransfer * leftMakeNullableTransfer,
				   const TreculTransfer * rightMakeNullableTransfer,
                                   const TreculFreeOperation & leftNullableFreeFunctor,
                                   const TreculFreeOperation & rightNullableFreeFunctor)
    :
    RuntimeOperatorType("RuntimeSortMergeJoinOperatorType"),
    mJoinType(joinType),
    mLeftKeyCompareFunRef(leftKeyCompare.getReference()),
    mRightKeyCompareFunRef(rightKeyCompare.getReference()),
    mLeftRightKeyCompareFunRef(leftRightKeyCompare.getReference()),
    mEqFunRef(eqFun ? eqFun->getReference() : TreculFunctionReference()),
    mMatchTransferRef(matchTransfer ? matchTransfer->getReference() : TreculTransferReference()),
    mLeftMakeNullableTransferRef(leftMakeNullableTransfer && 
			  (joinType==SortMergeJoin::FULL_OUTER ||
			   joinType==SortMergeJoin::RIGHT_OUTER ||
			   !leftMakeNullableTransfer->isIdentity()) ? 
			  leftMakeNullableTransfer->getReference() : TreculTransferReference()),
    mRightMakeNullableTransferRef(rightMakeNullableTransfer && 
			  (joinType==SortMergeJoin::FULL_OUTER ||
			   joinType==SortMergeJoin::LEFT_OUTER) ? 
			  rightMakeNullableTransfer->getReference() : TreculTransferReference()),
    mLeftFreeRef(leftFreeFunctor.getReference()),
    mRightFreeRef(rightFreeFunctor.getReference()),
    mLeftNullMalloc(leftMakeNullableTransfer->getTarget()->getMalloc()),
    mLeftNullFreeRef(leftNullableFreeFunctor.getReference()),
    mRightNullMalloc(rightMakeNullableTransfer->getTarget()->getMalloc()),
    mRightNullFreeRef(rightNullableFreeFunctor.getReference())
  {
  }
  ~RuntimeSortMergeJoinOperatorType();
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
    mLeftFree = m.getFunction<TreculRecordFreeRuntime>(mLeftFreeRef);
    mRightFree = m.getFunction<TreculRecordFreeRuntime>(mRightFreeRef);
    mLeftNullFree = m.getFunction<TreculRecordFreeRuntime>(mLeftNullFreeRef);
    mRightNullFree = m.getFunction<TreculRecordFreeRuntime>(mRightNullFreeRef);
    mLeftKeyCompareFun = m.getFunction<TreculFunctionRuntime>(mLeftKeyCompareFunRef);
    mRightKeyCompareFun = m.getFunction<TreculFunctionRuntime>(mRightKeyCompareFunRef);
    mLeftRightKeyCompareFun = m.getFunction<TreculFunctionRuntime>(mLeftRightKeyCompareFunRef);
    if (!mEqFunRef.empty()) {
      mEqFun = m.getFunction<TreculFunctionRuntime>(mEqFunRef);
    }
    if (!mMatchTransferRef.empty()) {
      mMatchTransfer = m.getTransfer<TreculTransfer2Runtime>(mMatchTransferRef);
    }
    if (!mLeftMakeNullableTransferRef.empty()) {
      mLeftMakeNullableTransfer = m.getTransfer<TreculTransferRuntime>(mLeftMakeNullableTransferRef);
    }
    if (!mRightMakeNullableTransferRef.empty()) {
      mRightMakeNullableTransfer = m.getTransfer<TreculTransferRuntime>(mRightMakeNullableTransferRef);
    }
  }
};

class RuntimeSortMergeJoinOperator : public RuntimeOperatorBase<RuntimeSortMergeJoinOperatorType>
{
private:
  enum State { START, 
	       READ_LEFT_INIT, 
	       READ_RIGHT_INIT, 
	       READ_LEFT_LT, 
	       READ_LEFT_EQ, 
	       READ_RIGHT_LT, 
	       READ_RIGHT_EQ, 
	       WRITE_MATCH, 
	       WRITE_LAST_MATCH, 
	       WRITE_LEFT_NON_MATCH, 
	       WRITE_RIGHT_NON_MATCH, 
	       READ_LEFT_DRAIN,
	       READ_RIGHT_DRAIN,
	       WRITE_LEFT_NON_MATCH_LT, 
	       WRITE_RIGHT_NON_MATCH_LT, 
	       WRITE_LEFT_NON_MATCH_DRAIN, 
	       WRITE_RIGHT_NON_MATCH_DRAIN, 
	       WRITE_EOS };
  State mState;
  // Stores a sort run; assume fits in memory for now.
  class RunEntry
  {
  public:
    RecordBuffer Buffer;
    bool Matched;
    RunEntry()
      :
      Matched(false)
    {
    }
    RunEntry(RecordBuffer buf)
      :
      Buffer(buf),
      Matched(false)
    {
    }
  };
  typedef std::vector<RunEntry> left_buffer_type;
  left_buffer_type mLeftBuffer;
  // One past the last record in the left buffer.
  RecordBuffer mLeftInput;
  // A single record from right input
  RecordBuffer mRightInput;
  // Iterator for performing cross product of buffer
  // with right stream.
  left_buffer_type::iterator mIt;
  class InterpreterContext * mRuntimeContext;
  // Keep track of whether there is match for right record. 
  bool mMatchFound;
  // Remember a match that was found.  We delay writing until
  // we find a second match so we can apply move semantics optimization.
  RecordBuffer mLastMatch;
  // NULLs for left,right input in outer join processing
  RecordBuffer mLeftNulls;
  RecordBuffer mRightNulls;

  void onMatch(RuntimePort * port);
  void onRightNonMatch(RuntimePort * port);
  void onLeftNonMatch(RuntimePort * port, RecordBuffer buf);
public:
  RuntimeSortMergeJoinOperator(RuntimeOperator::Services& services, const RuntimeSortMergeJoinOperatorType& opType);
  ~RuntimeSortMergeJoinOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

class LogicalUnionAll : public LogicalOperator
{
private:
  // We support two policies here.  First is an approximation of
  // fair merging of inputs (though non blocking).  The other is
  // a deterministic concatenation of streams (first stream, followed
  // by second and so on).  The latter is not very common but can be 
  // useful (e.g. creating header and footer records and inserting them
  // into a stream).
  bool mConcat;
public:
  LogicalUnionAll();
  ~LogicalUnionAll();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeUnionAllOperatorType : public RuntimeOperatorType
{
private:
  bool mConcat;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mConcat);
  }
public:
  RuntimeUnionAllOperatorType(bool concat=false)
    :
    RuntimeOperatorType("RuntimeUnionAllOperatorType"),
    mConcat(concat)
  {
  }
  ~RuntimeUnionAllOperatorType();
  void loadFunctions(TreculModule & ) override {}
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

class LogicalUnpivot : public LogicalOperator
{
private:
  LogicalGenerate * mGenerator;
  HashJoin * mCrossJoin;
public:
  LogicalUnpivot();
  ~LogicalUnpivot();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);    
};

class LogicalSwitch : public LogicalOperator
{
private:
  TreculFunction * mSwitcher;
public:
  LogicalSwitch();
  ~LogicalSwitch();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeSwitchOperatorType : public RuntimeOperatorType
{
  friend class RuntimeSwitchOperator;
private:
  TreculFunctionReference mSwitcherRef;
  TreculFunctionRuntime mSwitcher;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mSwitcherRef);
  }
  RuntimeSwitchOperatorType()
  {
  }
public:
  RuntimeSwitchOperatorType(const RecordType * input,
			    const TreculFunction & switcher)
    :
    RuntimeOperatorType("RuntimeSwitchOperatorType"),
    mSwitcherRef(switcher.getReference())
  {
  }
  ~RuntimeSwitchOperatorType() 
  {
  }
  void loadFunctions(TreculModule & m) override
  {
    mSwitcher = m.getFunction<TreculFunctionRuntime>(mSwitcherRef);
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

class RuntimeSwitchOperator : public RuntimeOperatorBase<RuntimeSwitchOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;
  uint32_t mNumOutputs;
  RecordBuffer mInput;
  class InterpreterContext * mRuntimeContext;
public:
  RuntimeSwitchOperator(RuntimeOperator::Services& services, 
			const RuntimeSwitchOperatorType& opType);
  ~RuntimeSwitchOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

#endif
