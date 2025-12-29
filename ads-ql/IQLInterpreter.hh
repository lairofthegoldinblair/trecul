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

class TreculFunctionReference
{
private:
  std::string mFunName;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  public:
  TreculFunctionReference()
  {
  }

  TreculFunctionReference(const std::string& funName)
    :
    mFunName(funName)
  {
  }

  ~TreculFunctionReference()
  {
  }

  bool empty() const
  {
    return mFunName.empty();
  }

  const std::string & getName() const
  {
    return mFunName;
  }
};

class TreculTransferReference
{
  friend class TreculModule;
private:
  std::string mCopyFunName;
  std::string mMoveFunName;
  RecordTypeMalloc mMalloc;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
  }

public:
  TreculTransferReference()
  {
  }

  TreculTransferReference(const std::string& copyFunName,
                          const std::string& moveFunName,
                          const RecordType * transferType)
    :
    mCopyFunName(copyFunName),
    mMoveFunName(moveFunName),
    mMalloc(transferType->getMalloc())
  {
  }

  TreculTransferReference(const std::string& copyFunName,
                          const std::string& moveFunName,
                          const RecordTypeMalloc & m)
    :
    mCopyFunName(copyFunName),
    mMoveFunName(moveFunName),
    mMalloc(m)
  {
  }

  ~TreculTransferReference()
  {
  }

  bool empty() const
  {
    return mCopyFunName.empty() || mMoveFunName.empty();
  }
};

class TreculAggregateReference
{
  friend class TreculModule;
private:
  RecordTypeMalloc mAggregateMalloc;
  RecordTypeMalloc mTransferMalloc;
  std::string mInitName;
  std::string mUpdateName;
  std::string mTransferName;
  bool mIsTransferIdentity;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mAggregateMalloc);
    ar & BOOST_SERIALIZATION_NVP(mTransferMalloc);
    ar & BOOST_SERIALIZATION_NVP(mInitName);
    ar & BOOST_SERIALIZATION_NVP(mUpdateName);
    ar & BOOST_SERIALIZATION_NVP(mTransferName);
    ar & BOOST_SERIALIZATION_NVP(mIsTransferIdentity);
  }

public:
  TreculAggregateReference()
  {
  }

  TreculAggregateReference(const std::string& initName,
                           const std::string& updateName,
                           const std::string& transferName,
                           const RecordType * aggregateType,
                           const RecordType * transferType,
                           bool isTransferIdentity)
    :
    mAggregateMalloc(aggregateType->getMalloc()),
    mTransferMalloc(transferType->getMalloc()),
    mInitName(initName),
    mUpdateName(updateName),
    mTransferName(transferName),
    mIsTransferIdentity(isTransferIdentity)
  {
  }

  TreculAggregateReference(const std::string& initName,
                           const std::string& updateName,
                           const std::string& transferName,
                           const RecordTypeMalloc & aggregateMalloc,
                           const RecordTypeMalloc & transferMalloc,
                           bool isTransferIdentity)
    :
    mAggregateMalloc(aggregateMalloc),
    mTransferMalloc(transferMalloc),
    mInitName(initName),
    mUpdateName(updateName),
    mTransferName(transferName),
    mIsTransferIdentity(isTransferIdentity)
  {
  }

  ~TreculAggregateReference()
  {
  }

  /**
   * Is executeTransfer an identity mapping?
   */
  bool getIsTransferIdentity() const 
  {
    return mIsTransferIdentity; 
  }

  bool empty() const
  {
    return mInitName.empty() || mUpdateName.empty() || mTransferName.empty();
  }
};

class TreculUpdateRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
private:
  LLVMFuncType mFunction;
public:
  TreculUpdateRuntime()
    :
    mFunction(nullptr)
  {
  }
  TreculUpdateRuntime(LLVMFuncType f)
    :
    mFunction(f)
  {
  } 
  ~TreculUpdateRuntime()
  {
  }

  operator bool() const
  {
    return mFunction != nullptr;
  }
  
  /**
   * Execute the method.
   */
  void execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const;
};

class TreculFunctionRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char*, int32_t *, class InterpreterContext *);
private:
  LLVMFuncType mFunction;
public:
  TreculFunctionRuntime()
    :
    mFunction(nullptr)
  {
  }
  TreculFunctionRuntime(LLVMFuncType f)
    :
    mFunction(f)
  {
  } 
  ~TreculFunctionRuntime();

  operator bool() const
  {
    return mFunction != nullptr;
  }
  
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
 * Execute a list of IQL statements in place on a collection
 * of records.  This supports imperative programming and thus
 * is really useful for things like updating aggregates in
 * group by calculations.
 */
class TreculRecordOperations
{
private:
  std::string mFunName;

public:
  /**
   * Compile a sequence of statements to execute against a collection of
   * record buffers.
   * Right now this will throw if there are any name collisions among
   * the inputs.  TODO: Support aliasing or multi-part names to disambiguate.
   */
  TreculRecordOperations(CodeGenerationContext & codeGen,
                         const RecordType * recordType,
                         const std::string& funName,
                         std::function<void()> op);
  
  ~TreculRecordOperations();

  TreculFunctionReference getReference() const
  {
    return TreculFunctionReference(mFunName);
  }
};

class TreculFreeOperation : public TreculRecordOperations
{
public:
  TreculFreeOperation(CodeGenerationContext & codeGen, const RecordType * recordType);
};

class TreculPrintOperation : public TreculRecordOperations
{
public:
  TreculPrintOperation(CodeGenerationContext & codeGen, const RecordType * recordType, bool isBinary = false);
};

class TreculRecordFreeRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char*, int32_t);
private:
  LLVMFuncType mFunction;
public:
  TreculRecordFreeRuntime(LLVMFuncType func)
    :
    mFunction(func)
  {
  }
  TreculRecordFreeRuntime()
    :
    mFunction(nullptr)
  {
  }
  operator bool() const
  {
    return mFunction != nullptr;
  }
  void free(RecordBuffer & buf) const
  {
    if (buf.Ptr != nullptr) {
      (*mFunction)((char *) buf.Ptr, nullptr, 0);
      buf.Ptr = nullptr;
    }
  }
};

class TreculRecordPrintRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char*, int32_t);
private:
  LLVMFuncType mFunction;
public:
  TreculRecordPrintRuntime(LLVMFuncType func)
    :
    mFunction(func)
  {
  }
  TreculRecordPrintRuntime()
    :
    mFunction(nullptr)
  {
  }
  operator bool() const
  {
    return mFunction != nullptr;
  }
  static void imbue(std::ostream& ostr);
  void print(RecordBuffer buf, std::ostream& ostr, bool outputRecordDelimiter=true) const
  {
    (*mFunction)((char *) buf.Ptr, (char *)&ostr, outputRecordDelimiter);
  }
};

/**
 * Create code to asynchronously serialize a Trecul record
 */
class TreculRecordSerialize
{
private:
  std::string mFunName;

public:
  /**
   * Compile a function to serialize a Trecul record of type recordType
   */
  TreculRecordSerialize(CodeGenerationContext & codeGen,
                        const RecordType * recordType,
                        const std::string& funName);
  
  TreculRecordSerialize(CodeGenerationContext & codeGen,
                        const RecordType * recordType)
    :
    TreculRecordSerialize(codeGen, recordType, "serialize")
  {
  }
  
  ~TreculRecordSerialize();

  TreculFunctionReference getReference() const
  {
    return TreculFunctionReference(mFunName);
  }
};

/**
 * Create code to asynchronously deserialize a Trecul record
 */
class TreculRecordDeserialize
{
private:
  std::string mFunName;

public:
  /**
   * Compile a function to deserialize a Trecul record of type recordType
   */
  TreculRecordDeserialize(CodeGenerationContext & codeGen,
                        const RecordType * recordType,
                        const std::string& funName);

  TreculRecordDeserialize(CodeGenerationContext & codeGen,
                        const RecordType * recordType)
    :
    TreculRecordDeserialize(codeGen, recordType, "deserialize")
  {
  }
  
  ~TreculRecordDeserialize();

  TreculFunctionReference getReference() const
  {
    return TreculFunctionReference(mFunName);
  }
};

struct TreculSerializationState
{
  uint8_t * output{nullptr};
  uint8_t * outputEnd{nullptr};
  int64_t state{0};

  struct deleter
  {
    void operator()(TreculSerializationState * ptr) const;
  };
  typedef std::unique_ptr<TreculSerializationState, deleter> pointer;
  static pointer get(const RecordType * ty);
};

class TreculRecordSerializeRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char **, char*, TreculSerializationState *, int32_t * , class InterpreterContext *);
private:
  LLVMFuncType mFunction;
public:
  TreculRecordSerializeRuntime(LLVMFuncType func)
    :
    mFunction(func)
  {
  }
  TreculRecordSerializeRuntime()
    :
    mFunction(nullptr)
  {
  }
  operator bool() const
  {
    return mFunction != nullptr;
  }
  bool serialize(const RecordBuffer & buf, char * & output, char * outputEnd, TreculSerializationState & state, class InterpreterContext * ctxt) const
  {
    int32_t ret=0;
    (*mFunction)((char *) buf.Ptr, &output, outputEnd, &state, &ret, ctxt);
    return ret > 0;
  }
};

class TreculRecordSerializedLengthRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char **, char*, TreculSerializationState *, int32_t * , class InterpreterContext *);
private:
  LLVMFuncType mFunction;
public:
  TreculRecordSerializedLengthRuntime(LLVMFuncType func)
    :
    mFunction(func)
  {
  }
  TreculRecordSerializedLengthRuntime()
    :
    mFunction(nullptr)
  {
  }
  operator bool() const
  {
    return mFunction != nullptr;
  }
  std::size_t length(RecordBuffer & buf, TreculSerializationState & state, class InterpreterContext * ctxt) const
  {
    int32_t ret=0;
    char * dummy{nullptr};
    (*mFunction)((char *) buf.Ptr, &dummy, nullptr, &state, &ret, ctxt);
    return ret;
  }
};

class TreculRecordDeserializeRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, const char **, const char*, TreculSerializationState *, int32_t * , class InterpreterContext *);
private:
  LLVMFuncType mFunction;
public:
  TreculRecordDeserializeRuntime(LLVMFuncType func)
    :
    mFunction(func)
  {
  }
  TreculRecordDeserializeRuntime()
    :
    mFunction(nullptr)
  {
  }
  operator bool() const
  {
    return mFunction != nullptr;
  }
  bool deserialize(RecordBuffer & buf, const char * & input, const char * inputEnd, TreculSerializationState & state, class InterpreterContext * ctxt) const
  {
    int32_t ret=0;
    (*mFunction)((char *) buf.Ptr, &input, inputEnd, &state, &ret, ctxt);
    return ret > 0;
  }
};

/**
 * Create code to calculate serialized lengtb of a Trecul record
 */
class TreculRecordSerializedLength
{
private:
  std::string mFunName;

public:
  /**
   * Compile a function to serialize a Trecul record of type recordType
   */
  TreculRecordSerializedLength(CodeGenerationContext & codeGen,
                              const RecordType * recordType,
                              const std::string& funName);
  
  TreculRecordSerializedLength(CodeGenerationContext & codeGen,
                              const RecordType * recordType)
    :
    TreculRecordSerializedLength(codeGen, recordType, "serializeLength")
  {
  }
  
  ~TreculRecordSerializedLength();

  TreculFunctionReference getReference() const
  {
    return TreculFunctionReference(mFunName);
  }
};

class TreculTransferRuntime
{
public:
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
private:
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  RecordTypeMalloc mMalloc;
public:
  TreculTransferRuntime()
    :
    mCopyFunction(nullptr),
    mMoveFunction(nullptr)
  {
  }
  TreculTransferRuntime(LLVMFuncType copyFun, LLVMFuncType moveFun, RecordTypeMalloc m)
    :
    mCopyFunction(copyFun),
    mMoveFunction(moveFun),
    mMalloc(m)
  {
  } 
  ~TreculTransferRuntime()
  {
  }

  operator bool() const
  {
    return mCopyFunction != nullptr && mMoveFunction != nullptr;
  }
  
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove) const;
};

/**
 * Execute a list of IQL statements in place on a collection
 * of records.  This supports imperative programming and thus
 * is really useful for things like updating aggregates in
 * group by calculations.
 */
class TreculInPlaceUpdate
{
private:
  std::vector<const RecordType *> mSources;
  std::string mFunName;
  std::string mStatements;

  void init(class DynamicRecordContext& recCtxt, 
            class CodeGenerationContext & codeGen,
	    const std::string & funName, 
	    const std::vector<const RecordType *>& sources, 
	    const std::vector<boost::dynamic_bitset<> >& sourceMasks,
	    const std::string& statements);

protected:
  const std::string & getFunName() const
  {
    return mFunName;
  }
  
public:
  /**
   * Compile a sequence of statements to execute against a collection of
   * record buffers.
   * Right now this will throw if there are any name collisions among
   * the inputs.  TODO: Support aliasing or multi-part names to disambiguate.
   */
  TreculInPlaceUpdate(class DynamicRecordContext& recCtxt, 
                      class CodeGenerationContext & codeGen,
                      const std::string & funName, 
                      const std::vector<const RecordType *>& sources, 
                      const std::string& statements);

  TreculInPlaceUpdate(class DynamicRecordContext& recCtxt, 
                      class CodeGenerationContext & codeGen,
                      const std::string & funName, 
                      const std::vector<const RecordType *>& sources, 
                      const std::vector<boost::dynamic_bitset<> >& sourceMasks,
                      const std::string& statements);

  ~TreculInPlaceUpdate();

  TreculFunctionReference getReference() const
  {
    return TreculFunctionReference(mFunName);
  }
};

class TreculFunction
{
private:
  std::vector<AliasedRecordType> mSources;
  std::string mFunName;
  std::string mStatements;

  void init(class DynamicRecordContext& recCtxt,
            class CodeGenerationContext & codeGen);

protected:
  const std::string & getFunName() const
  {
    return mFunName;
  }
  
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
  TreculFunction(class DynamicRecordContext& recCtxt,
                 class CodeGenerationContext & codeGen,
                 const std::string & funName, 
                 const std::vector<const RecordType *> & sources, 
                 const std::string& statements);
  TreculFunction(class DynamicRecordContext& recCtxt, 
                 class CodeGenerationContext & codeGen,
                 const std::string & funName, 
                 const std::vector<AliasedRecordType>& sources, 
                 const std::string& statements);

  ~TreculFunction();

  TreculFunctionReference getReference() const
  {
    return TreculFunctionReference(mFunName);
  }
};

/**
 * Transform an input record into an output.
 * This assumes we are provided with a list of
 * expressions each of which generates an output field.
 * It does not support statements/procedural logic.
 */
class TreculTransfer 
{
private:
  const RecordType * mSource;
  const RecordType * mTarget;
  std::string mFunName;
  std::string mCopyFunName;
  std::string mMoveFunName;
  std::string mTransfer;
  bool mIsIdentity;

protected:
  const std::string & getCopyFunName() const
  {
    return mCopyFunName;
  }
  const std::string & getMoveFunName() const
  {
    return mMoveFunName;
  }
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
  TreculTransfer(class DynamicRecordContext& recCtxt,
                 class CodeGenerationContext & codeGen,
                 const std::string & funName, 
                 const RecordType * source, 
                 const std::string& transfer);

  ~TreculTransfer();

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

  TreculTransferReference getReference() const
  {
    return TreculTransferReference(mCopyFunName, mMoveFunName, mTarget);
  }
};

class TreculTransfer2Runtime
{
public:
  typedef void (*LLVMFuncType)(char*, char *, char*, class InterpreterContext *);
private:
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  RecordTypeMalloc mMalloc;
public:
  TreculTransfer2Runtime()
    :
    mCopyFunction(nullptr),
    mMoveFunction(nullptr)
  {
  }
  TreculTransfer2Runtime(LLVMFuncType copyFun, LLVMFuncType moveFun, RecordTypeMalloc m)
    :
    mCopyFunction(copyFun),
    mMoveFunction(moveFun),
    mMalloc(m)
  {
  } 
  ~TreculTransfer2Runtime()
  {
  }

  operator bool() const
  {
    return mCopyFunction != nullptr && mMoveFunction != nullptr;
  }
  
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

class TreculTransfer2
{
private:
  std::vector<AliasedRecordType> mSources;
  const RecordType * mTarget;
  std::string mFunName;
  std::string mCopyFunName;
  std::string mMoveFunName;
  std::string mTransfer;

protected:
  const std::string & getCopyFunName() const
  {
    return mCopyFunName;
  }
  const std::string & getMoveFunName() const
  {
    return mMoveFunName;
  }
public:
  /**
   * Perform a transfer of the source record to a target following the specification
   * provided in the program transfer.
   */
  TreculTransfer2(class DynamicRecordContext& recCtxt,
                  class CodeGenerationContext & codeGen,
                  const std::string & funName, 
                  const std::vector<AliasedRecordType> & sources,
                  const std::string& transfer);

  ~TreculTransfer2();

  /**
   * The target type of the transfer.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  TreculTransferReference getReference() const
  {
    return TreculTransferReference(mCopyFunName, mMoveFunName, mTarget);
  }
};

class TreculAggregateRuntime
{
public:
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  typedef void (*LLVMFuncType2)(char*, char*, char*, class InterpreterContext *);
private:
  RecordTypeMalloc mAggregateMalloc;
  RecordTypeMalloc mTransferMalloc;
  LLVMFuncType mInitFunction;
  LLVMFuncType mUpdateFunction;
  LLVMFuncType mTransferFunction;

public:
  TreculAggregateRuntime()
    :
    mInitFunction(nullptr),
    mUpdateFunction(nullptr),
    mTransferFunction(nullptr)
  {
  }
  TreculAggregateRuntime(LLVMFuncType initFun,
                         LLVMFuncType updateFun,
                         LLVMFuncType transferFun,
                         RecordTypeMalloc aggregateMalloc,
                         RecordTypeMalloc transferMalloc)
    :
    mAggregateMalloc(aggregateMalloc),
    mTransferMalloc(transferMalloc),
    mInitFunction(initFun),
    mUpdateFunction(updateFun),
    mTransferFunction(transferFun)
  {
  } 
  ~TreculAggregateRuntime()
  {
  }

  operator bool() const
  {
    return mInitFunction != nullptr && mUpdateFunction != nullptr && mTransferFunction != nullptr;
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
class TreculAggregate
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

  void createUpdateFunction(class CodeGenerationContext & codeGen,
                            const std::vector<std::string>& groupKeys);
protected:
  const std::string & getInitializeFunName() const
  {
    return mInitializeFun;
  }
  const std::string & getUpdateFunName() const
  {
    return mUpdateFun;
  }
  const std::string & getTransferFunName() const
  {
    return mTransferFun;
  }
  bool getIsTransferIdentity() const
  {
    return mIsIdentity;
  }
public:
  void init(class DynamicRecordContext& recCtxt,
            class CodeGenerationContext & codeGen,
	    const std::string & funName, 
	    const RecordType * source, 
	    const std::string& init,
	    const std::string& update,
	    const std::vector<std::string>& groupKeys,
	    bool isOlap=false);
  /**
   * Perform an aggregate transfer of an input record.
   */
  TreculAggregate(class DynamicRecordContext& recCtxt, 
                  class CodeGenerationContext & codeGen,
                  const std::string & funName, 
                  const RecordType * source, 
                  const std::string& transfer,
                  const std::vector<std::string>& groupKeys,
                  bool isOlap=false);

  /**
   * Perform an aggregate transfer of an input record.
   */
  TreculAggregate(class DynamicRecordContext& recCtxt, 
                  class CodeGenerationContext & codeGen,
                  const std::string & funName, 
                  const RecordType * source, 
                  const std::string& init,
                  const std::string& update,
                  const std::vector<std::string>& groupKeys,
                  bool isOlap=false);

  ~TreculAggregate();

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

  TreculAggregateReference getReference() const;
};

class TreculModule
{
private:
  std::string mObjectFile;
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
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mObjectFile);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  
  void * getFunctionPointer(const TreculFunctionReference & fun);
public:
  TreculModule()
    :
    mImpl(nullptr)
  {
  }
  TreculModule(std::unique_ptr<llvm::Module> module);
  TreculModule(CodeGenerationContext & ctxt);
  ~TreculModule();

  template<typename T>
  T getFunction(const TreculFunctionReference & fun)
  {
    return T(reinterpret_cast<typename T::LLVMFuncType>(getFunctionPointer(fun)));
  }

  template<typename T>
  T getTransfer(const TreculTransferReference & xfer)
  {
    return T(reinterpret_cast<typename T::LLVMFuncType>(getFunctionPointer(xfer.mCopyFunName)),
             reinterpret_cast<typename T::LLVMFuncType>(getFunctionPointer(xfer.mMoveFunName)),
             xfer.mMalloc);  
  }
  TreculAggregateRuntime getAggregate(const TreculAggregateReference & agg)
  {
    return TreculAggregateRuntime(reinterpret_cast<typename TreculAggregateRuntime::LLVMFuncType>(getFunctionPointer(agg.mInitName)),
                                  reinterpret_cast<typename TreculAggregateRuntime::LLVMFuncType>(getFunctionPointer(agg.mUpdateName)),
                                  reinterpret_cast<typename TreculAggregateRuntime::LLVMFuncType>(getFunctionPointer(agg.mTransferName)),
                                  agg.mAggregateMalloc,
                                  agg.mTransferMalloc);  
  }
};

class LLVMBase
{
protected:
  class CodeGenerationContext * mContext;

  void InitializeLLVM();
  
public:
  LLVMBase();
  virtual ~LLVMBase();
};

class IQLUpdateModule
{
public:
  typedef TreculUpdateRuntime::LLVMFuncType LLVMFuncType;
private:
  TreculFunctionReference mFunName;
  TreculModule mModule;
  TreculUpdateRuntime mFunction;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);

    mFunction = mModule.getFunction<TreculUpdateRuntime>(mFunName);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLUpdateModule()
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
class RecordTypeInPlaceUpdate : public LLVMBase, public TreculInPlaceUpdate
{
private:
  std::unique_ptr<IQLUpdateModule> mModule;

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

class IQLRecordTypeOperationModule
{
private:
  std::string mFunName;
  std::string mObjectFile;
  typedef void (*LLVMFuncType)(char*, char*, int32_t);
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
  public:
  IQLRecordTypeOperationModule()
  :
  mFunction(nullptr),
    mImpl(nullptr)
  {
  }
  IQLRecordTypeOperationModule(const std::string& funName, 
                               std::unique_ptr<llvm::Module> module);
  IQLRecordTypeOperationModule(const IQLRecordTypeOperationModule & rhs)
    :
    mFunName(rhs.mFunName),
    mObjectFile(rhs.mObjectFile)
  {
    initImpl();
  }
  ~IQLRecordTypeOperationModule();

  IQLRecordTypeOperationModule & operator=(const IQLRecordTypeOperationModule & rhs);

  /**
   * Execute operation on a record
   */
  void execute(RecordBuffer & source) const;
  /**
   * Execute a print operation
   */
  void execute(RecordBuffer & source, std::ostream & ostr, bool outputRecordDelimiter) const;
};

/**
 * Execute a list of IQL statements in place on a collection
 * of records.  This supports imperative programming and thus
 * is really useful for things like updating aggregates in
 * group by calculations.
 */
class RecordTypeOperations : public LLVMBase
{
private:
  std::string mFunName;
  std::unique_ptr<IQLUpdateModule> mModule;

  void init(const RecordType * recordType, std::function<void()> op);

public:

  /**
   * Compile a sequence of statements to execute against a collection of
   * record buffers.
   * Right now this will throw if there are any name collisions among
   * the inputs.  TODO: Support aliasing or multi-part names to disambiguate.
   */
  RecordTypeOperations(const RecordType * recordType, const std::string& funName, std::function<void()> op);
  
  ~RecordTypeOperations();

  /**
   * Create serializable update function.
   */
  IQLRecordTypeOperationModule * create() const;
};

class RecordTypeFreeOperation : public RecordTypeOperations
{
public:
  RecordTypeFreeOperation(const RecordType * recordType);
};

class RecordTypePrintOperation : public RecordTypeOperations
{
public:
  RecordTypePrintOperation(const RecordType * recordType, bool isBinary = false);
};

class IQLTransferModule
{
public:
  typedef TreculTransferRuntime::LLVMFuncType LLVMFuncType;
private:
  TreculTransferReference mFunName;
  TreculModule mModule;
  TreculTransferRuntime mFunction;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
    mFunction = mModule.getTransfer<TreculTransferRuntime>(mFunName);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLTransferModule()
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
class RecordTypeTransfer : public LLVMBase, public TreculTransfer
{
private:
  std::unique_ptr<IQLTransferModule> mModule;
public:
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
   * create a serializable IQLTransferModule that implements the program.
   */
  IQLTransferModule * create() const;
};

class IQLTransferModule2
{
public:
  typedef TreculTransfer2Runtime::LLVMFuncType LLVMFuncType;
private:
  TreculTransferReference mFunName;
  TreculModule mModule;
  TreculTransfer2Runtime mFunction;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
    mFunction = mModule.getTransfer<TreculTransfer2Runtime>(mFunName);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLTransferModule2()
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
    mFunction.execute(sourceA, sourceB, target, ctxt, isSourceAMove, isSourceBMove);
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

class RecordTypeTransfer2 : public LLVMBase, public TreculTransfer2
{
private:
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
   * Get a serializable module corresponding to this transfer.
   */
  IQLTransferModule2 * create() const;
};

class IQLFunctionModule
{
public:
  typedef TreculFunctionRuntime::LLVMFuncType LLVMFuncType;
private:
  TreculFunctionReference mFunName;
  TreculModule mModule;
  TreculFunctionRuntime mFunction;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);

    mFunction = mModule.getFunction<TreculFunctionRuntime>(mFunName);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLFunctionModule()
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
  LLVMFuncType getRawFunction () const { return mFunction.getRawFunction(); }
};

/**
 * Evaluate an integer value expression against a set of input records.
 * TODO: Templatize on the output parameter.  Gracefully handle an
 * arbitrary number of input records (currently must be one or two).
 */
class RecordTypeFunction : public LLVMBase, public TreculFunction
{
public:
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, int32_t *, class InterpreterContext *);
private:
  std::unique_ptr<IQLFunctionModule> mModule;

public:
  /**
   * Compile an expression against a set of input sources.
   * Right now this will throw if there are any name collisions among
   * the inputs.  
   */
  RecordTypeFunction(class DynamicRecordContext& recCtxt, 
  		     const std::string & funName, 
  		     const std::vector<const RecordType *> & sources, 
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
public:
  typedef TreculAggregateRuntime::LLVMFuncType LLVMFuncType;
private:
  TreculAggregateReference mFunName;
  TreculModule mModule;
  TreculAggregateRuntime mFunction;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mModule);
    mFunction = mModule.getAggregate(mFunName);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLAggregateModule()
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
    return mFunName.getIsTransferIdentity();
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
class RecordTypeAggregate : public LLVMBase, public TreculAggregate
{
public:
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
   * create a serializable IQLAggregateModule that implements the program.
   */
  IQLAggregateModule * create() const;
};

class RecordTypeFree
{
private:
  IQLRecordTypeOperationModule * mModule;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
public:
  RecordTypeFree()
    :
    mModule(nullptr)
  {
  }
  RecordTypeFree(const RecordTypeFree & rhs);
  RecordTypeFree(const RecordType * recordType);
  ~RecordTypeFree()
  {
    delete mModule;
  }
  RecordTypeFree & operator=(const RecordTypeFree & );
  void free(RecordBuffer & buf) const
  {
    if (buf.Ptr != nullptr) {
      mModule->execute(buf);
      buf.Ptr = nullptr;
    }
  }
};

class RecordTypePrint
{
private:
  IQLRecordTypeOperationModule * mModule;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mModule);
  }
public:
  RecordTypePrint()
    :
    mModule(nullptr)
  {
  }
  RecordTypePrint(const RecordTypePrint & );
  RecordTypePrint(const RecordType * recordType);
  RecordTypePrint(const RecordType * recordType,
		  char fieldDelimter, char recordDelimiter, 
                  char arrayDelimiter, char escapeChar);
  ~RecordTypePrint()
  {
    delete mModule;
  }
  RecordTypePrint & operator=(const RecordTypePrint & rhs);
  void imbue(std::ostream& ostr) const;
  void print(RecordBuffer buf, std::ostream& ostr, bool emitNewLine=true) const
  {
    mModule->execute(buf, ostr, emitNewLine);
  }
};

#endif
