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

#if !defined(__IQLINTERPRETER_HH)
#define __IQLINTERPRETER_HH

#include <string>
#include <vector>
#include <set>
#include <map>
#include <boost/dynamic_bitset.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/regex.hpp>

// decNumber
extern "C" {
#include "decimal128.h"
}

#include "RecordType.hh"
#include "TypeCheckContext.hh"

// Forward decl
class CodeGenerationContext;

class RecordType;

namespace llvm {
  class Module;
  class Type;
  class Value;
  namespace legacy {
    class FunctionPassManager;
  }
}

#include "RecordBuffer.hh"

class InterpreterContext {
private:
  decContext mDecimalContext;
  std::set<void *> mToFree;

  typedef std::map<std::string, boost::regex> regex_cache_type;
  static const size_t MAX_REGEX_CACHE = 20;
  regex_cache_type mRegexCache; 
public:
  InterpreterContext();
  ~InterpreterContext();
  decContext * getDecimalContext() {
    return &mDecimalContext;
  }
  void * malloc(size_t sz);
  void erase(void * ptr);
  void clear();
  bool regex_match(const char* regex, const char* string);
};

class LLVMBase
{
protected:
  class CodeGenerationContext * mContext;
  llvm::legacy::FunctionPassManager * mFPM;

  void InitializeLLVM();
  void ConstructFunction(const std::string& funName, const std::vector<std::string>& recordArgs);
  void ConstructFunction(const std::string& funName, const std::vector<std::string>& recordArgs, llvm::Type * returnType);
  void CreateMemcpyIntrinsic();  
  void CreateMemsetIntrinsic();  
  void CreateMemcmpIntrinsic();  
  void createTransferFunction(const std::string& funName,
			      const RecordType * input,
			      const RecordType * output);
  void createTransferFunction(const std::string & mFunName,
			      const std::vector<const RecordType *>& mSources,
			      const std::vector<boost::dynamic_bitset<> >& masks,
			      const RecordType * output);
  void createUpdate(const std::string & mFunName,
		    const std::vector<const RecordType *>& mSources,
		    const std::vector<boost::dynamic_bitset<> >& masks);
  
public:
  LLVMBase();
  virtual ~LLVMBase();
  llvm::Value * LoadAndValidateExternalFunction(const char * externalFunctionName, 
						llvm::Type * funTy);
  llvm::Value * LoadAndValidateExternalFunction(const char * treculName, 
						const char * implName, 
						llvm::Type * funTy);
};

class IQLUpdateModule
{
private:
  std::string mFunName;
  std::string mObjectFile;
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  LLVMFuncType mFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from IR
  void initImpl(std::unique_ptr<llvm::Module> module);
  // Load the object file into the JIT
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLUpdateModule()
    :
    mFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLUpdateModule(const std::string& funName, 
                  std::unique_ptr<llvm::Module> module);
  ~IQLUpdateModule();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const;
};

/**
 * Execute a list of IQL statements in place on a collection
 * of records.  This supports imperative programming and thus
 * is really useful for things like updating aggregates in
 * group by calculations.
 */
class RecordTypeInPlaceUpdate : public LLVMBase
{
private:
  std::vector<const RecordType *> mSources;
  std::string mFunName;
  std::string mStatements;
  std::unique_ptr<IQLUpdateModule> mModule;

  void init(class DynamicRecordContext& recCtxt, 
	    const std::string & funName, 
	    const std::vector<const RecordType *>& sources, 
	    const std::vector<boost::dynamic_bitset<> >& sourceMasks,
	    const std::string& statements);
public:
  /**
   * Compile a sequence of statements to execute against a collection of
   * record buffers.
   * Right now this will throw if there are any name collisions among
   * the inputs.  TODO: Support aliasing or multi-part names to disambiguate.
   */
  RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
			  const std::string & funName, 
			  const std::vector<const RecordType *>& sources, 
			  const std::string& statements);

  RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
			  const std::string & funName, 
			  const std::vector<const RecordType *>& sources, 
			  const std::vector<boost::dynamic_bitset<> >& sourceMasks,
			  const std::string& statements);

  ~RecordTypeInPlaceUpdate();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt);

  /**
   * Create serializable update function.
   */
  IQLUpdateModule * create() const;
};

class IQLTransferModule
{
private:
  RecordTypeMalloc mMalloc;
  std::string mCopyFunName;
  std::string mMoveFunName;
  std::string mObjectFile;
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from IR
  void initImpl(std::unique_ptr<llvm::Module> module);
  // Load the object file into the JIT
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLTransferModule()
    :
    mCopyFunction(NULL),
    mMoveFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLTransferModule(const RecordTypeMalloc & targetMalloc,
		    const std::string& copyFunName, 
		    const std::string& moveFunName, 
		    std::unique_ptr<llvm::Module> module);
  ~IQLTransferModule();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove) const;
};

/**
 * Transform an input record into an output.
 * This assumes we are provided with a list of
 * expressions each of which generates an output field.
 * It does not support statements/procedural logic.
 */
class RecordTypeTransfer : public LLVMBase
{
private:
  const RecordType * mSource;
  const RecordType * mTarget;
  std::string mFunName;
  std::string mTransfer;

  std::unique_ptr<IQLTransferModule> mModule;
  bool mIsIdentity;
public:
  /**
   * Get abstract syntax tree.
   */
  static class IQLRecordConstructor * getAST(class DynamicRecordContext& recCtxt,
					     const std::string& xfer);

  /**
   * Get the list of free variables in the transfer.
   */
  static void getFreeVariables(const std::string& xfer,
			       std::set<std::string>& freeVariables);
  /**
   * Perform a transfer of the source record to a target following the specification
   * provided in the program transfer.
   */
  RecordTypeTransfer(class DynamicRecordContext& recCtxt, 
		     const std::string & funName, 
		     const RecordType * source, 
		     const std::string& transfer);

  ~RecordTypeTransfer();

  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove);

  /**
   * The target type of the transfer.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  /**
   * Is this an identity transfer?
   */
  bool isIdentity() const 
  {
    return mIsIdentity;
  }

  /** 
   * create a serializable IQLTransferModule that implements the program.
   */
  IQLTransferModule * create() const;
};

class IQLTransferModule2
{
private:
  RecordTypeMalloc mMalloc;
  std::string mCopyFunName;
  std::string mMoveFunName;
  std::string mObjectFile;
  // TODO: Must genericize
  typedef void (*LLVMFuncType)(char*, char*, char *, class InterpreterContext *);
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from IR
  void initImpl(std::unique_ptr<llvm::Module> module);
  // Load the object file into the JIT
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLTransferModule2()
    :
    mCopyFunction(NULL),
    mMoveFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLTransferModule2(const RecordTypeMalloc & recordMalloc,
		     const std::string& copyFunName, 
		     const std::string& moveFunName, 
		     std::unique_ptr<llvm::Module> module);
  ~IQLTransferModule2();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & sourceA, 
	       RecordBuffer & sourceB,
	       RecordBuffer & target, 
	       class InterpreterContext * ctxt, 
	       bool isSourceAMove,
	       bool isSourceBMove) const
  {
    RecordBuffer sources[2] = { sourceA, sourceB };
    bool isSourceMove [2] = { isSourceAMove, isSourceBMove };
    execute(&sources[0], &isSourceMove[0], 2, target, ctxt);
    sourceA = sources[0];
    sourceB = sources[1];
  }
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer * sources, 
	       bool * isSourceMove,
	       int32_t numSources,
	       RecordBuffer & target, 
	       class InterpreterContext * ctxt) const;
};

class RecordTypeTransfer2 : public LLVMBase
{
private:
  std::vector<AliasedRecordType> mSources;
  const RecordType * mTarget;
  std::string mFunName;
  std::string mTransfer;

  std::unique_ptr<IQLTransferModule2> mModule;
public:
  /**
   * Perform a transfer of the source record to a target following the specification
   * provided in the program transfer.
   */
  RecordTypeTransfer2(class DynamicRecordContext& recCtxt, 
		      const std::string & funName, 
		      const std::vector<AliasedRecordType>& source, 
		      const std::string& transfer);

  ~RecordTypeTransfer2();

  void execute(RecordBuffer & sourceA, 
	       RecordBuffer & sourceB,
	       RecordBuffer & target, 
	       class InterpreterContext * ctxt, 
	       bool isSourceAMove,
	       bool isSourceBMove)
  {
    RecordBuffer sources[2] = { sourceA, sourceB };
    bool isSourceMove [2] = { isSourceAMove, isSourceBMove };
    execute(&sources[0], &isSourceMove[0], 2, target, ctxt);
    sourceA = sources[0];
    sourceB = sources[1];
  }
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer * sources, 
	       bool * isSourceMove,
	       int32_t numSources,
	       RecordBuffer & target, 
	       class InterpreterContext * ctxt);

  /**
   * The target type of the transfer.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  /**
   * Get a serializable module corresponding to this transfer.
   */
  IQLTransferModule2 * create() const;
};

class IQLFunctionModule
{
public:
  typedef void (*LLVMFuncType)(char*, char*, int32_t *, class InterpreterContext *);
private:
  std::string mFunName;
  std::string mObjectFile;
  LLVMFuncType mFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from IR
  void initImpl(std::unique_ptr<llvm::Module> module);
  // Load the object file into the JIT
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLFunctionModule()
    :
    mFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLFunctionModule(const std::string& funName, 
		    std::unique_ptr<llvm::Module> module);
  ~IQLFunctionModule();
  /**
   * Execute the method.
   */
  int32_t execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt) const;
  /**
   * For those who want to make a copy of the function pointer into another
   * data structure...
   */
  LLVMFuncType getRawFunction () const { return mFunction; }
};

/**
 * Evaluate an integer value expression against a set of input records.
 * TODO: Templatize on the output parameter.  Gracefully handle an
 * arbitrary number of input records (currently must be one or two).
 */
class RecordTypeFunction : public LLVMBase
{
public:
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, int32_t *, class InterpreterContext *);
private:
  std::vector<AliasedRecordType> mSources;
  std::string mFunName;
  std::string mStatements;

  std::unique_ptr<IQLFunctionModule> mModule;

  void init(class DynamicRecordContext& recCtxt);
public:
  /**
   * Get abstract syntax tree.
   */
  static class IQLExpression * getAST(class DynamicRecordContext& recCtxt,
				      const std::string& xfer);

  /**
   * Compile an expression against a set of input sources.
   * Right now this will throw if there are any name collisions among
   * the inputs.  
   */
  RecordTypeFunction(class DynamicRecordContext& recCtxt, 
  		     const std::string & funName, 
  		     const std::vector<const RecordType *> sources, 
  		     const std::string& statements);
  RecordTypeFunction(class DynamicRecordContext& recCtxt, 
  		     const std::string & funName, 
  		     const std::vector<AliasedRecordType>& sources, 
  		     const std::string& statements);

  ~RecordTypeFunction();

  /**
   * Evaluate and return.
   */
  int32_t execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt);
  
  /**
   * Create a serializable representation of the function that can be sent on the wire.
   */
  IQLFunctionModule * create() const;
};

class IQLAggregateModule
{
private:
  RecordTypeMalloc mAggregateMalloc;
  RecordTypeMalloc mTransferMalloc;
  std::string mInitName;
  std::string mUpdateName;
  std::string mTransferName;
  std::string mObjectFile;
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  typedef void (*LLVMFuncType2)(char*, char*, char*, class InterpreterContext *);
  LLVMFuncType mInitFunction;
  LLVMFuncType mUpdateFunction;
  LLVMFuncType mTransferFunction;
  class IQLRecordBufferMethodHandle * mImpl;
  bool mIsTransferIdentity;

  // Create the LLVM module from IR
  void initImpl(std::unique_ptr<llvm::Module> module);
  // Load the object file into the JIT
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mInitName);
    ar & BOOST_SERIALIZATION_NVP(mUpdateName);
    ar & BOOST_SERIALIZATION_NVP(mTransferName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
    ar & BOOST_SERIALIZATION_NVP(mAggregateMalloc);
    ar & BOOST_SERIALIZATION_NVP(mTransferMalloc);
    ar & BOOST_SERIALIZATION_NVP(mIsTransferIdentity);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mInitName);
    ar & BOOST_SERIALIZATION_NVP(mUpdateName);
    ar & BOOST_SERIALIZATION_NVP(mTransferName);
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
    ar & BOOST_SERIALIZATION_NVP(mAggregateMalloc);
    ar & BOOST_SERIALIZATION_NVP(mTransferMalloc);
    ar & BOOST_SERIALIZATION_NVP(mIsTransferIdentity);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLAggregateModule()
    :
    mInitFunction(NULL),
    mUpdateFunction(NULL),
    mTransferFunction(NULL),
    mImpl(NULL),
    mIsTransferIdentity(false)
  {
  }
public:
  IQLAggregateModule(const RecordTypeMalloc& aggregateMalloc,
		     const RecordTypeMalloc& targetMalloc,
		     const std::string& initName, 
		     const std::string& updateName,
		     const std::string& transferName,
		     std::unique_ptr<llvm::Module> module,
		     bool isTransferIdentity);
  ~IQLAggregateModule();

  /**
   * Is executeTransfer an identity mapping?
   */
  bool getIsTransferIdentity() const 
  {
    return mIsTransferIdentity; 
  }
  /**
   * Execute methods for aggregate functions.
   */
  void executeInit(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt) const;
  void executeUpdate(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt) const;
  void executeTransfer(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt) const;
  void executeTransfer(RecordBuffer & source1, RecordBuffer& source2, RecordBuffer & target, class InterpreterContext * ctxt) const;
};

/**
 * Transform an input record into an output.
 * This assumes we are provided with a list of
 * expressions each of which generates an output field.
 * It does not support statements/procedural logic.
 */
class RecordTypeAggregate : public LLVMBase
{
private:
  const RecordType * mSource;
  const RecordType * mAggregate;
  const RecordType * mTarget;
  std::string mInitializeFun;
  std::string mUpdateFun;
  std::string mTransferFun;

  // Is the transfer the identity?
  bool mIsIdentity;

  void createUpdateFunction(const std::vector<std::string>& groupKeys);
public:
  void init(class DynamicRecordContext& recCtxt, 
	    const std::string & funName, 
	    const RecordType * source, 
	    const std::string& init,
	    const std::string& update,
	    const std::vector<std::string>& groupKeys,
	    bool isOlap=false);
  /**
   * Perform an aggregate transfer of an input record.
   */
  RecordTypeAggregate(class DynamicRecordContext& recCtxt, 
		      const std::string & funName, 
		      const RecordType * source, 
		      const std::string& transfer,
		      const std::vector<std::string>& groupKeys,
		      bool isOlap=false);

  /**
   * Perform an aggregate transfer of an input record.
   */
  RecordTypeAggregate(class DynamicRecordContext& recCtxt, 
		      const std::string & funName, 
		      const RecordType * source, 
		      const std::string& init,
		      const std::string& update,
		      const std::vector<std::string>& groupKeys,
		      bool isOlap=false);

  ~RecordTypeAggregate();

  /**
   * The target type of the transfer.
   * The target record is created not from the source but
   * rather from the aggregate record.  In those cases in which
   * the transfer from aggregate to final record is an identity
   * use the method isFinalTransferIdentity.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  /**
   * The aggregate type of the transfer.
   * The aggregate type contains the group fields as well as 
   * one field for each aggregate function expression.  The
   * initialize function is used to create the initial values
   * in the aggregate record based on an source record.  The update
   * function takes an aggregate record and a source record and
   * updates the values of the aggregate in response to the source.
   */
  const RecordType * getAggregate() const
  {
    return mAggregate;
  }

  /**
   * Is this the transfer from aggregate to final an
   * identity?
   */
  bool isFinalTransferIdentity() const 
  {
    return mIsIdentity;
  }

  /** 
   * create a serializable IQLAggregateModule that implements the program.
   */
  IQLAggregateModule * create() const;
};

#endif
