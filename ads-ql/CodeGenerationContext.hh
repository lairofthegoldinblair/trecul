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

#ifndef __CODEGENERATIONCONTEXT_HH
#define __CODEGENERATIONCONTEXT_HH

#include <map>
#include <stack>
#include <string>

#include <boost/dynamic_bitset.hpp>
#include <boost/shared_ptr.hpp>

#include "llvm/IR/IRBuilder.h"

#include "LLVMGen.h"

namespace llvm {
  class BasicBlock;
  class ConstantInt;
  class Function;
  class LLVMContext;
  class Type;
  class Value;
}
class CodeGenerationContext;
class FieldType;
class SequentialType;
class FieldAddress;
class BitcpyOp;
class BitsetOp;

typedef struct IQLToLLVMRecordMapStruct * IQLToLLVMRecordMapRef;
typedef const struct IQLRecordTypeStruct * IQLRecordTypeRef;
typedef const struct IQLToLLVMValueStruct * IQLToLLVMValueRef;

IQLToLLVMRecordMapRef wrap(std::map<std::string, std::pair<std::string, const class RecordType*> > * r);
std::map<std::string, std::pair<std::string, const class RecordType*> > * unwrap(IQLToLLVMRecordMapRef r);
const class RecordType * unwrap(IQLRecordTypeRef r);
IQLRecordTypeRef wrap(const class RecordType * r);

class IQLToLLVMValue
{
public:
  enum ValueType { eGlobal, eLocal };
  virtual ~IQLToLLVMValue() {}
  virtual llvm::Value * getValue(CodeGenerationContext * ctxt) const =0;
  virtual llvm::Value * getNull(CodeGenerationContext * ctxt) const =0;
  virtual bool isLiteralNull() const =0;
  virtual ValueType getValueType() const =0;
};

/**
 * An IQL r-value.
 * 
 * Represents the value of an IQL expression.  The value
 * may be nullable or not.  In the former case, this contains
 * a boolean value indicating whether the value is NULL or not.
 * Note that some IQL values are passed around "by-value" and some
 * are passed around "by reference".  In general, values that are
 * 8 bytes or less are passed by value and those bigger than 8 bytes
 * are passed by reference.  Exceptions to this rule are CHAR(N) values
 * which are currently passed by reference even when they are less than 
 * 8 bytes.
 * For all fixed length types, storage is allocated on the
 * stack or inline in the target record struct so there is no
 * need for reference counting.  For variable length types (e.g.
 * VARCHAR) there is heap allocated data in play and we must 
 * make sure that data is not leaked.
 */
class IQLToLLVMRValue : public IQLToLLVMValue
{
private:
  // The actual value
  llvm::Value * mValue;
  // NULL bit.  This should be non-NULL only on nullable values.
  llvm::Value * mIsNull;
  // Is this value a pointer to a global location,
  // a shallow copy of a global location 
  // (hence it may contain global pointers) or a completely
  // local value.
  ValueType mValueType;
public:
  IQLToLLVMRValue (llvm::Value * val, ValueType globalOrLocal);
  
  IQLToLLVMRValue (llvm::Value * val, llvm::Value * isNull, 
                   ValueType globalOrLocal);

  llvm::Value * getValue(CodeGenerationContext * ctxt) const override;
  llvm::Value * getNull(CodeGenerationContext * ctxt) const override;
  bool isLiteralNull() const override;
  ValueType getValueType() const override;
  static const IQLToLLVMRValue * get(CodeGenerationContext * ctxt, 
                                     llvm::Value * val, 
                                     IQLToLLVMValue::ValueType globalOrLocal);
  static const IQLToLLVMRValue * get(CodeGenerationContext * ctxt, 
                                     llvm::Value * val,
                                     llvm::Value * nv,
                                     IQLToLLVMValue::ValueType globalOrLocal);
};

class IQLToLLVMTypedValue
{
private:
  const IQLToLLVMValue * mValue;
  const FieldType * mType;
public:
  explicit IQLToLLVMTypedValue(const IQLToLLVMValue * value=NULL, const FieldType * ty=NULL)
    :
    mValue(value),
    mType(ty)
  {
  }

  const IQLToLLVMValue * getValue() const {
    return mValue; 
  }
  const FieldType * getType() const {
    return mType;
  }
};

/**
 * An IQL l-value
 *
 * The subtle thing about this abstraction is the opaqueness of the
 * NULL bit.  We do not assume that a NULL bit has an address so
 * we provide an operational interface that allows the bit to be
 * set but hides the representation.  This is necessary because we want
 * to allow a bit in a bitfield to represent the NULL and we cannot
 * take the address of a bit.
 */
class IQLToLLVMLValue : public IQLToLLVMValue
{
public:
  virtual ~IQLToLLVMLValue() {}
  /**
   * Retrieve the current pointer to value without retrieving
   * the NULL bit.
   */  
  virtual const IQLToLLVMValue * getValuePointer(CodeGenerationContext * ctxt) const =0;
  virtual void setNull(CodeGenerationContext * ctxt, bool isNull) const =0;
  virtual bool isNullable() const =0;
};

/**
 * LValue abstraction around a field in an IQL record.
 * These guys have an ordinary pointer to the data value
 * but in the nullable case they sit on top of a bit position
 * in a bit field for the null indicator.
 */
class IQLToLLVMField : public IQLToLLVMLValue
{
private:
  std::string mMemberName;
  llvm::Value * mBasePointer;
  const RecordType * mRecordType;
public:
  IQLToLLVMField(CodeGenerationContext * ctxt,
		 const RecordType * recordType,
		 const std::string& memberName,
		 const std::string& recordName);
  IQLToLLVMField(const RecordType * recordType,
		 const std::string& memberName,
		 llvm::Value * basePointer);
  ~IQLToLLVMField();
  const IQLToLLVMValue * getValuePointer(CodeGenerationContext * ctxt) const override;
  void setNull(CodeGenerationContext * ctxt, bool isNull) const override;
  bool isNullable() const override;
  llvm::Value * getValue(CodeGenerationContext * ctxt) const override;
  llvm::Value * getNull(CodeGenerationContext * ctxt) const override;
  bool isLiteralNull() const override;
  ValueType getValueType() const override;
};

/**
 * LValue abstraction around an element in a Trecul array.
 * These guys have an ordinary pointer to the data value
 * and the null bit (if present) is a pointer to int8 with an
 * offset to the null bit.
 */
class IQLToLLVMArrayElement : public IQLToLLVMLValue
{
private:
  IQLToLLVMTypedValue mValue;
  llvm::Value * mNullBytePtr;
  llvm::Value * mNullByteMask;
public:
  IQLToLLVMArrayElement(IQLToLLVMTypedValue val,
                        llvm::Value * nullBytePtr,
                        llvm::Value * nullByteMask);
  IQLToLLVMArrayElement(IQLToLLVMTypedValue val);
  ~IQLToLLVMArrayElement();
  const IQLToLLVMValue * getValuePointer(CodeGenerationContext * ctxt) const;
  void setNull(CodeGenerationContext * ctxt, bool isNull) const;
  bool isNullable() const;
  llvm::Value * getValue(CodeGenerationContext * ctxt) const override;
  llvm::Value * getNull(CodeGenerationContext * ctxt) const override;
  bool isLiteralNull() const override;
  ValueType getValueType() const override;
};

class IQLToLLVMLocal : public IQLToLLVMLValue
{
private:
  IQLToLLVMTypedValue mValue;
  // Pointer to alloca'd i1 (will likely be lowered to an i8).
  // Note that IQLToLLVMValue::mNullBit is a value not a pointer/memory location
  // which is why this is here and not inside IQLToLLVMLocal::mValue.
  llvm::Value * mNullBit;
public:
  IQLToLLVMLocal(IQLToLLVMTypedValue lval,
		 llvm::Value * lvalNull);

  ~IQLToLLVMLocal();

  const IQLToLLVMValue * getValuePointer(CodeGenerationContext * ctxt) const;
  llvm::Value * getNullBitPointer() const;
  void setNull(CodeGenerationContext * ctxt, bool isNull) const;
  bool isNullable() const;
  llvm::Value * getValue(CodeGenerationContext * ctxt) const override;
  llvm::Value * getNull(CodeGenerationContext * ctxt) const override;
  bool isLiteralNull() const override;
  ValueType getValueType() const override;
};

class IQLToLLVMStackRecord
{
public:
  llvm::BasicBlock * StartBB;
  llvm::BasicBlock * ThenBB;
  llvm::BasicBlock * ElseBB;
  llvm::BasicBlock * MergeBB;
};

class IQLToLLVMSwitchRecord
{
public:
  // The basic block from which we switch.
  // Saved so we can insert switch inst after we know
  // all about the cases.
  llvm::BasicBlock * Top;
  // The cases.
  std::vector<std::pair<llvm::ConstantInt*, llvm::BasicBlock *> > Cases;
  // The Exit block.  Here so cases can branch to it.
  llvm::BasicBlock * Exit;
};

// The merge block of current CASE
class IQLToLLVMCaseState
{
public:
  class IQLToLLVMLocal * Local;
  llvm::BasicBlock * MergeBB;
  llvm::BasicBlock * ElseBB;
  IQLToLLVMCaseState(class IQLToLLVMLocal * local, llvm::BasicBlock * mergeBB)
    :
    Local(local),
    MergeBB(mergeBB),
    ElseBB(NULL)
  {
  }
  ~IQLToLLVMCaseState()
  {
    delete Local;
  }

  llvm::BasicBlock * getMergeBlock()
  {
    return MergeBB;
  }
};

class CodeGenerationFunctionContext {
public:
  llvm::IRBuilder<> * Builder;
  class TreculSymbolTable * mSymbolTable;
  llvm::Function * Function;
  IQLToLLVMRecordMapRef RecordArguments;
  IQLRecordTypeRef OutputRecord;
  void * AllocaCache;
  CodeGenerationFunctionContext();
};

// TODO: Should have just made this whole thing opaque rather
// than trying to expose a lower level API to the ANTLR tree
// parser.
class CodeGenerationContext {
public:
  /**
   * Type of the cache of alloca'd locals
   * that we can reuse.  Keeping the number
   * of these small can make a big impact on the
   * amount of memory used during SSA creation (Mem2Reg pass).
   */
  typedef std::map<const llvm::Type*, 
		   std::vector<llvm::Value *> > local_cache;
  
  typedef IQLToLLVMValue::ValueType (CodeGenerationContext::*UnaryOperatorMemFn) (const IQLToLLVMValue * lhs, 
										  const FieldType * lhsType, 
										  llvm::Value * ret,
										  const FieldType * retType);
  typedef IQLToLLVMValue::ValueType (CodeGenerationContext::*BinaryOperatorMemFn) (const IQLToLLVMValue * lhs, 
										   const FieldType * lhsType, 
										   const IQLToLLVMValue * rhs, 
										   const FieldType * rhsType,
										   llvm::Value * ret,
										   const FieldType * retType);
private:
  bool mOwnsModule;
  class TreculSymbolTable * mSymbolTable;
  // In some cases we want the Trecul name of a function
  // to match the C function that provides the implementation
  // and in other not.  Translate from Trecul name to implementation
  // name here.  Note that this should be getting resolved during type
  // check not during code generation.
  std::map<std::string, std::string> mTreculNameToSymbol;

  /**
   * Private Interface to the variable length datatype (including VARCHAR)
   */
  llvm::Value * buildVarArrayIsSmall(llvm::Value * varcharPtr);

  /**
   * Add INTERVAL and DATE/DATETIME.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildDateAdd(const IQLToLLVMValue * lhs, 
					 const FieldType * lhsType, 
					 const IQLToLLVMValue * rhs, 
					 const FieldType * rhsType, 
					 llvm::Value * ret, 
					 const FieldType * retType);

  /**
   * Add CHAR(M) and CHAR(N).  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCharAdd(const IQLToLLVMValue * lhs, 
					 const FieldType * lhsType, 
					 const IQLToLLVMValue * rhs, 
					 const FieldType * rhsType, 
					 llvm::Value * ret, 
					 const FieldType * retType);

  /**
   * Wrap a unary operation in the canonical NULL handling code
   * which says that a result is NULL if its argument is.
   */
  const IQLToLLVMValue * buildNullableUnaryOp(const IQLToLLVMValue * lhs, 
					      const FieldType * lhsType, 
					      const FieldType * resultType,
					      UnaryOperatorMemFn unOp);
  /**
   * Wrap a unary operation in a wrapper that doesn't modify the null bit
   * and assumes that argument is in fact not null.
   * This method is useful for using a UnaryOperationMemFn inside another
   * UnaryOperationMemFn or BinaryOperationMemFn in those cases that the 
   * outer wrapper is already dealing with the null bit.
   */
  const IQLToLLVMValue * buildNonNullableUnaryOp(const IQLToLLVMValue * lhs, 
					      const FieldType * lhsType, 
					      const FieldType * resultType,
					      UnaryOperatorMemFn unOp);

  /**
   * Wrap a binary operation in the canonical NULL handling code
   * which says that a result is NULL if either of its argument are.
   */
  const IQLToLLVMValue * buildNullableBinaryOp(const IQLToLLVMValue * lhs, 
					       const FieldType * lhsType, 
					       const IQLToLLVMValue * rhs, 
					       const FieldType * rhsType, 
					       const FieldType * resultType,
					       BinaryOperatorMemFn binOp);
  void buildMemcpy(llvm::Value * sourcePtr,
		   const FieldAddress& sourceOffset,
		   llvm::Value * targetPtr, 
		   const FieldAddress& targetOffset,
		   int64_t sz);
  void buildMemcpy(const std::string& sourceArg,
		   const FieldAddress& sourceOffset,
		   const std::string& targetArg, 
		   const FieldAddress& targetOffset,
		   int64_t sz);
  void buildMemset(llvm::Value * targetPtr,
		   const FieldAddress& targetOffset,
		   int8_t value,
		   int64_t sz);
  llvm::Value * buildMemcmp(llvm::Value * sourcePtr,
			    const FieldAddress& sourceOffset,
			    llvm::Value * targetPtr, 
			    const FieldAddress& targetOffset,
			    int64_t sz);
  void buildBitcpy(const BitcpyOp& op,
		   llvm::Value * sourcePtr,
		   llvm::Value * targetPtr);
  void buildBitset(const BitsetOp& op,
		   llvm::Value * targetPtr);
  // Copy all of the fields matching a regex
  void buildSetFieldsRegex(const std::string& sourceName,
			   const RecordType * sourceType,
			   const std::string& expr,
			   const std::string& rename,
			   const std::string& recordName,
			   int * pos);

  // Copy a range from an array to another.   Assumes that storage for any VARIABLE_ARRAYs has been allocated
  void buildArrayElementwiseCopy(const IQLToLLVMValue * e, 
                                 const SequentialType * argType,
                                 const IQLToLLVMValue * beginIdx,
                                 const IQLToLLVMValue * endIdx,
                                 const IQLToLLVMValue * retBeginIdx, 
                                 const IQLToLLVMValue * ret, 
                                 const SequentialType * retType);
  // Copy an from an array to another when element types match and are copyable.
  // Assumes that storage for any VARIABLE_ARRAYs has been allocated
  void buildArrayCopyableCopy(const IQLToLLVMValue * e, 
                              const SequentialType * argType,
                              llvm::Value * ret, 
                              const SequentialType * retType);
  // Alloca a VARIABLE_ARRAY and dynamically allocate storage for arrayLen elements.
  llvm::Value * buildVariableArrayAllocate(const SequentialType * arrayType, llvm::Value * arrayLen, bool trackAllocation);
  // dynamically allocate storage for arrayLen elements in a pointer to VARIABLE_ARRAY
  void buildVariableArrayAllocate(llvm::Value * e, const SequentialType * arrayType, llvm::Value * arrayLen, bool trackAllocation);

  // How does this code generator treat values of types? Does it pass them
  // around as values or does it pass references around.
  static bool isValueType(const FieldType *);
  static llvm::Value * trimAlloca(llvm::Value * result, const FieldType * resultTy);

  static bool isChar(llvm::Type * ty);
  static bool isChar(llvm::Value * val);
  static int32_t getCharArrayLength(llvm::Type * ty);
  static int32_t getCharArrayLength(llvm::Value * val);

public:
  llvm::LLVMContext * LLVMContext;
  llvm::Module * LLVMModule;
  llvm::IRBuilder<> * LLVMBuilder;
  llvm::Type * LLVMDecContextPtrType;
  llvm::Type * LLVMDecimal128Type;
  llvm::Type * LLVMVarcharType;
  llvm::Type * LLVMDatetimeType;
  llvm::Type * LLVMCidrV4Type;
  // This is set by the code generator not by the caller
  llvm::Function * LLVMFunction;
  // Alias to record type mapping for inputs
  IQLToLLVMRecordMapRef IQLRecordArguments;
  // Output record type for expression lists.
  IQLRecordTypeRef IQLOutputRecord;
  // Memcpy
  llvm::Value * LLVMMemcpyIntrinsic;
  // Memset
  llvm::Value * LLVMMemsetIntrinsic;
  // Memcmp
  llvm::Value * LLVMMemcmpIntrinsic;
  // Move or copy semantics
  int32_t IQLMoveSemantics;
  // A stack for constructs like if/then/else
  std::stack<class IQLToLLVMStackRecord* > IQLStack;
  // A stack of switch builders
  std::stack<class IQLToLLVMSwitchRecord* > IQLSwitch;
  // A stack of CASE builders
  std::stack<class IQLToLLVMCaseState* > IQLCase;
  // Indicator whether we have generated an
  // identity transfer
  bool IsIdentity;

  // For aggregate function these
  // are for update operations
  CodeGenerationFunctionContext Update;
  // For aggregate function these
  // are for initialize operations
  CodeGenerationFunctionContext Initialize;
  // For aggregate function these
  // are for transfer operations
  CodeGenerationFunctionContext Transfer;
  // HACK: used for naming variables corresponding
  // to aggregate functions.
  int AggFn;
  // Value factory
  std::vector<IQLToLLVMValue *> ValueFactory;
  // Alloca cache
  local_cache * AllocaCache;
  // String pool
  std::map<std::string, const IQLToLLVMValue *> StringPool;

  CodeGenerationContext();
  ~CodeGenerationContext();
  /**
   * Give up ownership of Module.  This happens
   * when an execution engine is created since the
   * EE takes ownership of the module.
   */
  void disownModule();

  /**
   * Define a variable
   */
  void defineVariable(const char * name,
		      llvm::Value * val,
		      llvm::Value * nullVal,
                      const FieldType * ft,
		      IQLToLLVMValue::ValueType globalOrLocal);

  /**
   * Define a field of a record
   */
  void defineFieldVariable(llvm::Value * basePointer,
			   const char * prefix,
			   const char * memberName,
			   const RecordType * recordType);

  /**
   * Free all memory allocated by a record
   */
  void buildRecordTypeFree(const RecordType * recordType);

  /** 
   * Free all memory associated with a value
   */
  void buildFree(const IQLToLLVMValue * val, const FieldType * ft);

  /**
   * Print a record
   */
  void buildRecordTypePrint(const RecordType * recordType, char fieldDelimiter, char recordDelimiter, char escapeChar);

  /** 
   * Print a value
   */
  void buildPrint(const IQLToLLVMValue * val, const FieldType * ft, char fieldDelimiter, char escapeChar);

  /**
   * Lookup an l-value in the symbol table.
   */
  const IQLToLLVMLValue * lookup(const char * name, const char * name2);

  /**
   * Lookup an r-value in the symbol table.
   */
  const IQLToLLVMValue * lookupBasePointer(const char * name);

  /**
   * Lookup an r-value in the symbol table.
   */
  const IQLToLLVMValue * lookupFunction(const char * name);

  /**
   * Get pointer to the execution context argument as
   * an LLVM C wrapper datatype.
   */
  llvm::Value * getContextArgumentRef();

  /**
   * Initialize the members of code generation that correspond
   * to the function context.
   */
  void reinitializeForTransfer(const class TypeCheckConfiguration & typeCheckConfig);

  /**
   * Reinitialize some state for compiling a new function.
   * TODO: This and reinitializeForTransfer should be merged.
   */
  void reinitialize();

  /**
   * Initialize the members of code generation that correspond
   * to the function context.
   */
  void createFunctionContext(const class TypeCheckConfiguration & typeCheckConfig);

  /**
   * Dump contents of symbol table.
   */
  void dumpSymbolTable();

  /**
   * Handle the changes between compilation contexts for aggregates
   */
  void restoreAggregateContext(CodeGenerationFunctionContext * fCtxt);

  /**
   * Save the Aggregate function state.
   */
  void saveAggregateContext(CodeGenerationFunctionContext * fCtxt);

  /**
   * Add addresses of the members of the input record into the symbol table.
   */
  void addInputRecordType(const char * name, 
			  const char * argumentName, 
			  const RecordType * rec);
  void addInputRecordType(const char * name, 
			  const char * argumentName, 
			  const RecordType * rec,
			  const boost::dynamic_bitset<>& mask);
  /**
   * Add an external library function (C calling convention)
   */
  llvm::Value * addExternalFunction(const char * treculName,
				    const char * implName,
				    llvm::Type * funTy);

  /**
   * Local variable
   */
  void buildDeclareLocal(const char * nm, const FieldType * ft);
  void buildLocalVariable(const char * nm, const IQLToLLVMValue * init, const FieldType * ft);

  /**
   * Build a while loop.
   * 
   * To use this:
   * Call whileBegin before generating the condition predicate.
   * Call whileStatementBlock after generating the condition predicate and before
   * any of the statements.
   * Call whileFinish after generating all of the statements in the
   * body of the loop.
   */
  void whileBegin();
  void whileStatementBlock(const IQLToLLVMValue * condVal,
			   const FieldType * condTy);
  void whileFinish();

  /**
   * Conditionally branch using a possibly nullable boolean
   * condition value.
   */
  void conditionalBranch(const IQLToLLVMValue * condVal,
			 const FieldType * condTy,
			 llvm::BasicBlock * trueBranch,
			 llvm::BasicBlock * falseBranch);

  /**
   * Build an array expression
   */
  const IQLToLLVMValue * buildArray(std::vector<IQLToLLVMTypedValue>& vals,
				    FieldType * arrayTy);
  const IQLToLLVMValue * buildGlobalConstArray(std::vector<IQLToLLVMTypedValue>& vals,
					       FieldType * arrayTy);
  const IQLToLLVMValue * buildGlobalConstString(const std::string & str);

  /**
   * Reference an element of an array
   */
  const IQLToLLVMValue * buildArrayRef(const IQLToLLVMValue * arr,
                                       const FieldType * arrType,
                                       const IQLToLLVMValue * idx,
                                       const FieldType * idxType,
                                       const FieldType * retType);
  /**
   * Build an lvalue from a position in an array.
   */
  const IQLToLLVMLValue * buildArrayLValue(const IQLToLLVMValue * arr,
                                           const FieldType * arrType,
					   const IQLToLLVMValue * idx,
                                           const FieldType * idxType,
                                           const FieldType * retType);

  /**
   * Concatenate two arrays or an element and an array
   */
  const IQLToLLVMValue * buildArrayConcat(const IQLToLLVMValue * lhs,
                                          const FieldType * lhsType,
                                          const IQLToLLVMValue * rhs,
                                          const FieldType * rhsType,
                                          const FieldType * retType);

  /**
   * Call a function.
   */
  IQLToLLVMValue::ValueType buildCall(const char * f,
				      const std::vector<IQLToLLVMTypedValue> & args,
				      llvm::Value * retTmp,
				      const FieldType * retType);
  const IQLToLLVMValue * buildCall(const char * f,
				   const std::vector<IQLToLLVMTypedValue> & args,
				   const FieldType * retType);
  /**
   * Cast non null value to INT8.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastInt8(const IQLToLLVMValue * e, 
                                          const FieldType * argType, 
                                          llvm::Value * ret, 
					   const FieldType * retType);
  const IQLToLLVMValue * buildCastInt8(const IQLToLLVMValue * e, 
                                       const FieldType * argType, 
                                       const FieldType * retType);

  /**
   * Cast non null value to INT16.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastInt16(const IQLToLLVMValue * e, 
					   const FieldType * argType, 
					   llvm::Value * ret, 
					   const FieldType * retType);
  const IQLToLLVMValue * buildCastInt16(const IQLToLLVMValue * e, 
					const FieldType * argType, 
					const FieldType * retType);

  /**
   * Cast non null value to INT32.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastInt32(const IQLToLLVMValue * e, 
					   const FieldType * argType, 
					   llvm::Value * ret, 
					   const FieldType * retType);
  const IQLToLLVMValue * buildCastInt32(const IQLToLLVMValue * e, 
					const FieldType * argType, 
					const FieldType * retType);

  /**
   * Cast non null value to INT64.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastInt64(const IQLToLLVMValue * e, 
					   const FieldType * argType, 
					   llvm::Value * ret, 
					   const FieldType * retType);
  const IQLToLLVMValue * buildCastInt64(const IQLToLLVMValue * e, 
					const FieldType * argType, 
					const FieldType * retType);

  /**
   * Cast non null value to FLOAT.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastFloat(const IQLToLLVMValue * e, 
                                           const FieldType * argType, 
                                           llvm::Value * ret, 
                                           const FieldType * retType);
  const IQLToLLVMValue * buildCastFloat(const IQLToLLVMValue * e, 
                                        const FieldType * argType, 
                                        const FieldType * retType);

  /**
   * Cast non null value to DOUBLE.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastDouble(const IQLToLLVMValue * e, 
					    const FieldType * argType, 
					    llvm::Value * ret, 
					    const FieldType * retType);
  const IQLToLLVMValue * buildCastDouble(const IQLToLLVMValue * e, 
					 const FieldType * argType, 
					 const FieldType * retType);

  /**
   * Cast non null value to DECIMAL.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastDecimal(const IQLToLLVMValue * e, 
					     const FieldType * argType, 
					     llvm::Value * ret, 
					     const FieldType * retType);
  const IQLToLLVMValue * buildCastDecimal(const IQLToLLVMValue * e, 
					  const FieldType * argType, 
					  const FieldType * retType);

  /**
   * Cast non null value to DATE.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastDate(const IQLToLLVMValue * e, 
					  const FieldType * argType, 
					  llvm::Value * ret, 
					  const FieldType * retType);
  const IQLToLLVMValue * buildCastDate(const IQLToLLVMValue * e, 
				       const FieldType * argType, 
				       const FieldType * retType);

  /**
   * Cast non null value to DATETIME.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastDatetime(const IQLToLLVMValue * e, 
					      const FieldType * argType, 
					      llvm::Value * ret, 
					      const FieldType * retType);
  const IQLToLLVMValue * buildCastDatetime(const IQLToLLVMValue * e, 
					   const FieldType * argType, 
					   const FieldType * retType);

  /**
   * Cast non null value to CHAR(N).  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastChar(const IQLToLLVMValue * e, 
					     const FieldType * argType, 
					     llvm::Value * ret, 
					     const FieldType * retType);
  const IQLToLLVMValue * buildCastChar(const IQLToLLVMValue * e, 
					  const FieldType * argType, 
					  const FieldType * retType);

  /**
   * Cast non null value to VARCHAR.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastVarchar(const IQLToLLVMValue * e, 
					     const FieldType * argType, 
					     llvm::Value * ret, 
					     const FieldType * retType);
  const IQLToLLVMValue * buildCastVarchar(const IQLToLLVMValue * e, 
					  const FieldType * argType, 
					  const FieldType * retType);

  /**
   * Cast non null value to FIXED_ARRAY.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastFixedArray(const IQLToLLVMValue * e, 
                                                const FieldType * argType, 
                                                llvm::Value * ret, 
                                                const FieldType * retType);
  const IQLToLLVMValue * buildCastFixedArray(const IQLToLLVMValue * e, 
                                             const FieldType * argType, 
                                             const FieldType * retType);

  /**
   * Cast non null value to VARIABLE_ARRAY.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastVariableArray(const IQLToLLVMValue * e, 
                                                   const FieldType * argType, 
                                                   llvm::Value * ret, 
                                                   const FieldType * retType);
  const IQLToLLVMValue * buildCastVariableArray(const IQLToLLVMValue * e, 
                                                const FieldType * argType, 
                                                const FieldType * retType);

  /**
   * Cast non null value to IPV4.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastIPv4(const IQLToLLVMValue * e, 
                                          const FieldType * argType, 
                                          llvm::Value * ret, 
                                          const FieldType * retType);
  const IQLToLLVMValue * buildCastIPv4(const IQLToLLVMValue * e, 
                                       const FieldType * argType, 
                                       const FieldType * retType);

  /**
   * Cast non null value to CIDRV4.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastCIDRv4(const IQLToLLVMValue * e, 
                                            const FieldType * argType, 
                                            llvm::Value * ret, 
                                            const FieldType * retType);
  const IQLToLLVMValue * buildCastCIDRv4(const IQLToLLVMValue * e, 
                                         const FieldType * argType, 
                                         const FieldType * retType);

  /**
   * Cast non null value to IPV6.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastIPv6(const IQLToLLVMValue * e, 
                                          const FieldType * argType, 
                                          llvm::Value * ret, 
                                          const FieldType * retType);
  const IQLToLLVMValue * buildCastIPv6(const IQLToLLVMValue * e, 
                                       const FieldType * argType, 
                                       const FieldType * retType);

  /**
   * Cast non null value to CIDRV6.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastCIDRv6(const IQLToLLVMValue * e, 
                                            const FieldType * argType, 
                                            llvm::Value * ret, 
                                            const FieldType * retType);
  const IQLToLLVMValue * buildCastCIDRv6(const IQLToLLVMValue * e, 
                                         const FieldType * argType, 
                                         const FieldType * retType);

  /**
   * Cast non null value from one type to another.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCast(const IQLToLLVMValue * e, 
				      const FieldType * argType, 
				      llvm::Value * ret, 
				      const FieldType * retType);
  /**
   * Cast a value from one type to another taking NULL values into consideration
   */
  const IQLToLLVMValue * buildCast(const IQLToLLVMValue * e, 
				   const FieldType * argType, 
				   const FieldType * retType);

  /**
   * Cast a non-null value from one type (perhaps nullable) to another (doesn't
   * handle null bits).  This method is useful for type promotion inside of
   * UnaryOperationMemFns and BinaryOperationMemFns in which case the wrapper
   * is dealing with the null bit.
   */
  const IQLToLLVMValue * buildCastNonNullable(const IQLToLLVMValue * e, 
					      const FieldType * argType, 
					      const FieldType * retType);

  /**
   * Add rhs to lhs.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildAdd(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildAdd(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Subtract rhs from lhs.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildSub(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildSub(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Multiply rhs and lhs.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildMul(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildMul(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Divide lhs by rhs.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildDiv(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildDiv(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Remainder of division of lhs by rhs.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildMod(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildMod(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Negate an expression.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildNegate(const IQLToLLVMValue * lhs,
					const FieldType * lhsTy,
					llvm::Value * ret,
					const FieldType * retTy);
  const IQLToLLVMValue * buildNegate(const IQLToLLVMValue * e, 
				     const FieldType * argType, 
				     const FieldType * retType);
  /**
   * Bitwise and.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildBitwiseAnd(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildBitwiseAnd(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Bitwise or.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildBitwiseOr(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildBitwiseOr(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Bitwise xor.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildBitwiseXor(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);
  const IQLToLLVMValue * buildBitwiseXor(const IQLToLLVMValue * lhs, 
				  const FieldType * lhsType, 
				  const IQLToLLVMValue * rhs, 
				  const FieldType * rhsType, 
				  const FieldType * retType);

  /**
   * Bitwise not.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildBitwiseNot(const IQLToLLVMValue * lhs,
					    const FieldType * lhsTy,
					    llvm::Value * ret,
					    const FieldType * retTy);
  const IQLToLLVMValue * buildBitwiseNot(const IQLToLLVMValue * e, 
					 const FieldType * argType, 
					 const FieldType * retType);
  /**
   * Interface to the VARCHAR datatype.
   */
  llvm::Value * buildVarcharGetSize(llvm::Value * varcharPtr);
  llvm::Value * buildVarcharGetPtr(llvm::Value * varcharPtr);

  /**
   * Interface to the FIXED_ARRAY datatype.
   */
  llvm::Value * getFixedArrayData(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getFixedArrayData(llvm::Value * ptr, const FieldType * ty);
  llvm::Value * getFixedArrayNull(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getFixedArrayNull(llvm::Value * ptr, const FieldType * ty);
  
  /**
   * Interface to the VARIABLE_ARRAY datatype.
   */
  llvm::Value * buildVarArrayGetSize(llvm::Value * varcharPtr);
  llvm::Value * buildVarArrayGetPtr(llvm::Value * varcharPtr, llvm::Type * retTy);
  llvm::Value * getVariableArrayData(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getVariableArrayData(llvm::Value * ptr, const FieldType * ty);
  llvm::Value * getVariableArrayNull(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getVariableArrayNull(llvm::Value * ptr, const FieldType * ty);

  /**
   * Unified interface to FIXED_ARRAY and VARIABLE_ARRAY datatypes.
   */
  llvm::Value * getArraySize(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getArraySize(llvm::Value * ptr, const FieldType * ty);
  llvm::Value * getArrayData(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getArrayData(llvm::Value * ptr, const FieldType * ty);
  llvm::Value * getArrayNull(const IQLToLLVMValue * e, const FieldType * ty);
  llvm::Value * getArrayNull(llvm::Value * ptr, const FieldType * ty);
  
  /**
   * Reuse of local variables so that we don't put too much pressure
   * on mem2reg to eliminate them.
   */
  llvm::Value * getCachedLocal(llvm::Type * ty);
  void returnCachedLocal(llvm::Value * v);

  /**   
   * Create a mutable variable by doing an alloca in the entry block of the function.
   */
  llvm::Value * buildEntryBlockAlloca(llvm::Type * ty, 
				      const char * name);

  /**
   * Assignment
   */

  // Helper for buildSetValue2 that removes a value from local heap tracking
  void buildEraseAllocationTracking(const IQLToLLVMValue * iqlVal, const FieldType * ft);
  // This method sets a value that is assumed to be non-null and type promoted.
  void buildSetValue2(const IQLToLLVMValue * iqlVal,
		      const IQLToLLVMLValue * iqllvalue,
		      const FieldType * ft);
  // This method sets a possibly null value that is type promoted
  void buildSetNullableValue(const IQLToLLVMLValue * lval,
			     const IQLToLLVMValue * val,
			     const FieldType * ft,
			     bool allowNullToNonNull);
  // This method sets a possible null value to a variable loc which is assumed to be of the
  // correct type (this method doesn't do type promotion either).
  void buildSetValue(const IQLToLLVMValue * iqlVal, const char * loc, const FieldType * ft);
  // This method sets a possible nullable value providing type promotion as needed
  void buildSetNullableValue(const IQLToLLVMLValue * lval,
			     const IQLToLLVMValue * val,
			     const FieldType * valTy,
			     const FieldType * lvalTy);

  /**
   * CASE statements
   */
  void buildCaseBlockBegin(const FieldType * caseType);
  void buildCaseBlockIf(const IQLToLLVMValue * condVal);
  void buildCaseBlockThen(const IQLToLLVMValue *value, const FieldType * valueType, const FieldType * caseType, bool allowNullToNonNull);
  const IQLToLLVMValue * buildCaseBlockFinish(const FieldType * caseType);

  /**
   * LEAST(...) and GREATEST(...)
   */
  const IQLToLLVMValue * buildLeastGreatest(const std::vector<IQLToLLVMTypedValue> & args,
					    const FieldType * retTy,
					    bool isLeast);
  
  /**
   * IFNULL()
   */
  const IQLToLLVMValue * buildIsNullFunction(const std::vector<IQLToLLVMTypedValue> & args,
					     const FieldType * retTy);

  /**
   * Implement e1 && e2 as CASE WHEN e1 THEN e2 ELSE false END for now.
   */
  void buildBeginAnd(const FieldType * retType);
  void buildAddAnd(const IQLToLLVMValue * lhs,
		   const FieldType * lhsType,
		   const FieldType * retType);
  const IQLToLLVMValue * buildAnd(const IQLToLLVMValue * rhs,
				  const FieldType * rhsType,
				  const FieldType * retType);

  /**
   * Implement e1 || e2 as CASE WHEN e1 THEN true ELSE e2 END for now.
   */
  void buildBeginOr(const FieldType * retType);
  void buildAddOr(const IQLToLLVMValue * lhs,
		  const FieldType * lhsType,
		  const FieldType * retType);
  const IQLToLLVMValue * buildOr(const IQLToLLVMValue * rhs,
				 const FieldType * rhsType,
				 const FieldType * retType);

  /**
   * Negate a logical expression.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildNot(const IQLToLLVMValue * lhs,
				     const FieldType * lhsTy,
				     llvm::Value * ret,
				     const FieldType * retTy);
  const IQLToLLVMValue * buildNot(const IQLToLLVMValue * e, 
				  const FieldType * argType, 
				  const FieldType * retType);
  /**
   * IS NULL predicate
   */
  const IQLToLLVMValue * buildIsNull(const IQLToLLVMValue * val);
  const IQLToLLVMValue * buildIsNull(const IQLToLLVMValue * lhs,
				     const FieldType * lhsType, const FieldType * retType, int isNotNull);

  /**
   * binary comparisons
   */
  IQLToLLVMValue::ValueType buildVarcharCompare(llvm::Value * e1, 
						llvm::Value * e2,
						llvm::Value * ret,
						IQLToLLVMPredicate opCode);
  // Comparison of non null values
  IQLToLLVMValue::ValueType buildCompare(const IQLToLLVMValue * lhs, 
					 const FieldType * lhsType, 
					 const IQLToLLVMValue * rhs, 
					 const FieldType * rhsType,
					 llvm::Value * ret,
					 const FieldType * retType,
					 IQLToLLVMPredicate op);
  // Template to make members suitable for buildNullableBinaryOp
  template <IQLToLLVMPredicate _Op>
  IQLToLLVMValue::ValueType buildCompare(const IQLToLLVMValue * lhs, 
					 const FieldType * lhsType, 
					 const IQLToLLVMValue * rhs, 
					 const FieldType * rhsType,
					 llvm::Value * ret,
					 const FieldType * retType)
  {
    return buildCompare(lhs, lhsType, rhs, rhsType, ret, retType, _Op);
  }

  const IQLToLLVMValue * buildCompare(const IQLToLLVMValue * lhs, 
				      const FieldType * lhsType, 
				      const IQLToLLVMValue * rhs, 
				      const FieldType * rhsType,
				      const FieldType * resultType,
				      IQLToLLVMPredicate op);

  const IQLToLLVMValue *
  buildArrayElementwiseEquals(const IQLToLLVMValue * lhs, 
                              const IQLToLLVMValue * rhs,
                              const FieldType * promoted,
                              const FieldType * retType);
  const IQLToLLVMValue *
  buildArrayElementwiseCompare(const IQLToLLVMValue * lhs, 
                               const IQLToLLVMValue * rhs,
                               const FieldType * promoted,
                               const FieldType * retType,
                               IQLToLLVMPredicate op);

  const IQLToLLVMValue *
  buildStructElementwiseCompare(const IQLToLLVMValue * lhs, 
                                const IQLToLLVMValue * rhs,
                                const FieldType * promoted,
                                const std::vector<const FieldType *> members,
                                const FieldType * retType,
                                IQLToLLVMPredicate op);
  
  /**
   * Hash a sequence of values with helper methods
   */
  llvm::Value * buildHashInitValue(class DynamicRecordContext & ctxt, llvm::Value * sz, llvm::Value * previousHash, llvm::Value * firstFlag);
  void buildHash(const std::vector<IQLToLLVMTypedValue> & args, llvm::Value * previousHash, llvm::Value * firstFlag);
  void buildArrayElementwiseHash(const IQLToLLVMValue * e, 
                                 const SequentialType * argType,
                                 llvm::Value * previousHash,
                                 llvm::Value * firstFlag);
  const IQLToLLVMValue * buildHash(const std::vector<IQLToLLVMTypedValue> & args);

  /**
   * Create a poor man's normalized key out of a sequence of sort keys
   */
  const IQLToLLVMValue * buildSortPrefix(const IQLToLLVMValue * arg, const FieldType * argTy);
  const IQLToLLVMValue * buildSortPrefix(const std::vector<IQLToLLVMTypedValue> & args,
					 const FieldType * retTy);

  /**
   * RETURN from a function
   */
  void buildReturnValue(const IQLToLLVMValue * iqlVal, const FieldType * retType);

  /**
   * ?:
   */
  void buildBeginIfThenElse(const IQLToLLVMValue * condVal);
  void buildElseIfThenElse();
  const IQLToLLVMValue * buildEndIfThenElse(const IQLToLLVMValue * thenVal, const FieldType * thenTy,
					    const IQLToLLVMValue * elseVal, const FieldType * elseTy,
					    const FieldType * retTy);

  /**
   * IF ELSE statements
   */
  void buildBeginIf();
  void buildBeginElse();
  void buildEndIf(const IQLToLLVMValue * condVal);
  
  /**
   * SWITCH statements
   */
  void buildBeginSwitch();
  void buildEndSwitch(const IQLToLLVMValue * switchExpr);
  void buildBeginSwitchCase(const char * caseVal);
  void buildEndSwitchCase();

  /**
   * INTERVAL type constructor
   */
  const IQLToLLVMValue * buildInterval(const char * intervalType,
				       const  IQLToLLVMValue * e);
  /**
   * Literals
   */
  const IQLToLLVMValue * buildDateLiteral(const char * val);
  const IQLToLLVMValue * buildDatetimeLiteral(const char * val);
  const IQLToLLVMValue * buildDecimalInt32Literal(const char * val);
  const IQLToLLVMValue * buildDecimalInt64Literal(const char * val);
  const IQLToLLVMValue * buildDoubleLiteral(const char * val);
  const IQLToLLVMValue * buildVarcharLiteral(const char * val);
  const IQLToLLVMValue * buildDecimalLiteral(const char * val);
  const IQLToLLVMValue * buildIPv4Literal(const char * val);
  const IQLToLLVMValue * buildIPv6Literal(const char * val);
  const IQLToLLVMValue * buildTrue();
  const IQLToLLVMValue * buildFalse();
  const IQLToLLVMValue * buildNull();

  /**
   * In lieu of general constant folding make sure that cast of string
   * to date works
   */
  const IQLToLLVMValue * buildLiteralCast(const char * val,
					  const char * typeName);

  /**
   * Aggregate function support
   */
  const IQLToLLVMValue * buildAggregateFunction(const char * fn,
						const IQLToLLVMValue * e,
						const FieldType * retTy);
  

  /**
   * Record Construction
   */

  // This logic identifies LLVM values that are direct references 
  // to an input record.  The purpose here is to identify transfers
  // that are actually identity functions.  The point is that there
  // are cases in which knowing a transfer is identity may allow a
  // record buffer to be passed through without modification thus
  // completely bypassing memory allocation and transfer code.
  // References to fields may be reference or value.
  void buildSetField(int * pos, const IQLToLLVMValue * val);
  // Copy all of the field (same as regex .* unless in the
  // broken "move semantics" mode).
  void buildSetFields(const char * recordName, int * pos);
  void buildQuotedId(const char * quotedId, const char * rename, int * pos);

  // Should we make these private as some point?
  /**
   * Make the result of an LLVM compare into the result of an IQL compare 
   */
  const IQLToLLVMValue * buildCompareResult(llvm::Value * boolVal);
  IQLToLLVMValue::ValueType buildCompareResult(llvm::Value * boolVal,
					       llvm::Value * ret);
  const IQLToLLVMValue * buildRef(const IQLToLLVMValue * allocAVal, const FieldType * retTy);
  const IQLToLLVMValue * buildVariableRef(const char * var,
					  const char * var2,
					  const FieldType * varTy);
  
  // Is val a pointer to value of type ft?
  static bool isPointerToValueType(llvm::Value * val, const FieldType * ft);
};

class AggregateFunction
{
public:
  static boost::shared_ptr<AggregateFunction> get(const char * fn);
  virtual ~AggregateFunction() {}
  virtual void update(CodeGenerationContext * ctxt,
		      const std::string& old,
		      const IQLToLLVMValue * inc,
		      const FieldType * ft) =0;
  virtual const IQLToLLVMValue * initialize(CodeGenerationContext * ctxt,
					    const FieldType * ft) =0;
};

#endif
