/**
 * Copyright (c) 2012-2021, Akamai Technologies
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

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/address_v6.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "CodeGenerationContext.hh"
#include "LLVMGen.h"
#include "RecordType.hh"
#include "TypeCheckContext.hh"

#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils/Cloning.h"

/**
 * Call a decimal binary operator.
 */

std::vector<IQLToLLVMValue *> & IQLToLLVMValue::getValueFactory(CodeGenerationContext * ctxt)
{
  return ctxt->ValueFactory;
}


IQLToLLVMRValue::IQLToLLVMRValue (llvm::Value * val, 
                                  IQLToLLVMValue::ValueType globalOrLocal)
  :
  mValue(val),
  mIsNull(NULL),
  mValueType(globalOrLocal)
{
}
  
IQLToLLVMRValue::IQLToLLVMRValue (llvm::Value * val, llvm::Value * isNull, 
                                  IQLToLLVMValue::ValueType globalOrLocal)
  :
  mValue(val),
  mIsNull(isNull),
  mValueType(globalOrLocal)
{
}

llvm::Value * IQLToLLVMRValue::getValue(CodeGenerationContext * ) const 
{ 
  return mValue; 
}

llvm::Value * IQLToLLVMRValue::getNull(CodeGenerationContext * ) const 
{ 
  return mIsNull; 
}

bool IQLToLLVMRValue::isLiteralNull() const 
{ 
  return mValue == NULL; 
}

IQLToLLVMValue::ValueType IQLToLLVMRValue::getValueType() const 
{ 
  return mValueType; 
}

const IQLToLLVMRValue * IQLToLLVMRValue::get(CodeGenerationContext * ctxt, 
                                             llvm::Value * val, 
                                             IQLToLLVMValue::ValueType globalOrLocal)
{
  return get(ctxt, val, NULL, globalOrLocal);
}

const IQLToLLVMRValue * IQLToLLVMRValue::get(CodeGenerationContext * ctxt, 
                                             llvm::Value * val,
                                             llvm::Value * nv,
                                             IQLToLLVMValue::ValueType globalOrLocal)
{
  IQLToLLVMRValue * tmp = new IQLToLLVMRValue(val, nv, globalOrLocal);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMField::IQLToLLVMField(CodeGenerationContext * ctxt,
			       const RecordType * recordType,
			       const std::string& memberName,
			       const std::string& recordName)
  :
  mMemberName(memberName),
  mMemberIdx(std::numeric_limits<std::size_t>::max()),
  mBasePointer(NULL),
  mRecordType(recordType)
{
  mBasePointer = ctxt->lookupBasePointer(recordName.c_str())->getValue(ctxt);
}

IQLToLLVMField::IQLToLLVMField(const RecordType * recordType,
			       const std::string& memberName,
			       llvm::Value * basePointer)
  :
  mMemberName(memberName),
  mMemberIdx(std::numeric_limits<std::size_t>::max()),
  mBasePointer(basePointer),
  mRecordType(recordType)
{
}

IQLToLLVMField::IQLToLLVMField(const RecordType * recordType,
                               std::size_t memberIdx,
                               llvm::Value * basePointer)
  :
  mMemberIdx(memberIdx),
  mBasePointer(basePointer),
  mRecordType(recordType)
{
}

const FieldType * IQLToLLVMField::getType() const
{
  return mMemberName.empty() ? mRecordType->GetMember(mMemberIdx).GetType() : mRecordType->getMember(mMemberName).GetType();
}


IQLToLLVMField::~IQLToLLVMField() 
{
}

void IQLToLLVMField::setNull(CodeGenerationContext *ctxt, bool isNull) const
{
  if (mMemberName.empty()) {
    mRecordType->LLVMMemberSetNull(mMemberIdx, ctxt, mBasePointer, isNull);
  } else {
    mRecordType->LLVMMemberSetNull(mMemberName, ctxt, mBasePointer, isNull);
  }
}

const IQLToLLVMValue * IQLToLLVMField::getValuePointer(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mMemberName.empty() ?
    mRecordType->LLVMMemberGetPointer(mMemberIdx, ctxt, mBasePointer) :
    mRecordType->LLVMMemberGetPointer(mMemberName, ctxt, mBasePointer, false);
  // Don't worry about Nullability, it is dealt separately  
  const IQLToLLVMValue * val = 
    IQLToLLVMRValue::get(ctxt, 
                         outputVal, 
                         NULL, 
                         IQLToLLVMValue::eGlobal);  
  return val;
}

bool IQLToLLVMField::isNullable() const
{
  const FieldType * outputTy = getType();
  return outputTy->isNullable();
}

llvm::Value * IQLToLLVMField::getValue(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mMemberName.empty() ?
    mRecordType->LLVMMemberGetPointer(mMemberIdx, ctxt, mBasePointer) :
    mRecordType->LLVMMemberGetPointer(mMemberName, ctxt, mBasePointer, false);
  
  const FieldType * ft = getType();
  if(ctxt->isPointerToValueType(outputVal, ft)) {
    outputVal = ctxt->LLVMBuilder->CreateLoad(ft->LLVMGetType(ctxt), outputVal);
  }
  return outputVal;
}

llvm::Value * IQLToLLVMField::getNull(CodeGenerationContext * ctxt) const
{
  llvm::Value * nullVal = NULL;
  if (isNullable()) {
    nullVal = mMemberName.empty() ?
      mRecordType->LLVMMemberGetNull(mMemberIdx, ctxt, mBasePointer) :
      mRecordType->LLVMMemberGetNull(mMemberName, ctxt, mBasePointer);
  }
  return nullVal;
}

bool IQLToLLVMField::isLiteralNull() const
{
  return false;
}

IQLToLLVMValue::ValueType IQLToLLVMField::getValueType() const
{
  return IQLToLLVMValue::eGlobal;
}

IQLToLLVMField * IQLToLLVMField::get(CodeGenerationContext * ctxt,
				     const RecordType * recordType,
				     const std::string& memberName,
				     const std::string& recordName)
{
  IQLToLLVMField * tmp = new IQLToLLVMField(ctxt, recordType, memberName, recordName);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMField * IQLToLLVMField::get(CodeGenerationContext * ctxt,
				     const RecordType * recordType,
				     const std::string& memberName,
				     llvm::Value * basePointer)
{
  IQLToLLVMField * tmp = new IQLToLLVMField(recordType, memberName, basePointer);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMField * IQLToLLVMField::get(CodeGenerationContext * ctxt,
				     const RecordType * recordType,
				     std::size_t memberIdx,
				     llvm::Value * basePointer)
{
  IQLToLLVMField * tmp = new IQLToLLVMField(recordType, memberIdx, basePointer);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMArrayElement::IQLToLLVMArrayElement(IQLToLLVMTypedValue val,
                                             llvm::Value * nullBytePtr,
                                             llvm::Value * nullByteMask)
  :
  mValue(val),
  mNullBytePtr(nullBytePtr),
  mNullByteMask(nullByteMask)
{
}

IQLToLLVMArrayElement::IQLToLLVMArrayElement(IQLToLLVMTypedValue val)
  :
  mValue(val),
  mNullBytePtr(nullptr),
  mNullByteMask(nullptr)
{
}

IQLToLLVMArrayElement::~IQLToLLVMArrayElement()
{
}

const IQLToLLVMValue * IQLToLLVMArrayElement::getValuePointer(CodeGenerationContext * ctxt) const
{
  return mValue.getValue();
}

void IQLToLLVMArrayElement::setNull(CodeGenerationContext * ctxt, bool isNull) const
{
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  llvm::Value * val = isNull ?
    b->CreateAnd(b->CreateLoad(b->getInt8Ty(), mNullBytePtr), b->CreateNot(mNullByteMask)) :
    b->CreateOr(b->CreateLoad(b->getInt8Ty(), mNullBytePtr), mNullByteMask);
  b->CreateStore(val, mNullBytePtr);
}

bool IQLToLLVMArrayElement::isNullable() const
{
  return mNullBytePtr != NULL;
}

llvm::Value * IQLToLLVMArrayElement::getValue(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mValue.getValue()->getValue(ctxt);
  if(ctxt->isPointerToValueType(outputVal, mValue.getType())) {
    outputVal = ctxt->LLVMBuilder->CreateLoad(mValue.getType()->LLVMGetType(ctxt),
                                              outputVal);
  }
  return outputVal;
}

llvm::Value * IQLToLLVMArrayElement::getNull(CodeGenerationContext * ctxt) const
{
  if (!isNullable()) {
    return nullptr;
  }
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  llvm::Value * v = b->CreateAnd(b->CreateLoad(b->getInt8Ty(), mNullBytePtr), mNullByteMask);
  v = b->CreateICmpEQ(v, b->getInt8(0));
  return v;
}

bool IQLToLLVMArrayElement::isLiteralNull() const
{
  return false;
}

IQLToLLVMValue::ValueType IQLToLLVMArrayElement::getValueType() const
{
  return mValue.getValue()->getValueType();
}

IQLToLLVMArrayElement * IQLToLLVMArrayElement::get(CodeGenerationContext * ctxt,
						   IQLToLLVMTypedValue val,
						   llvm::Value * nullBytePtr,
						   llvm::Value * nullByteMask)
{
  IQLToLLVMArrayElement * tmp = new IQLToLLVMArrayElement(val, nullBytePtr, nullByteMask);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMArrayElement * IQLToLLVMArrayElement::get(CodeGenerationContext * ctxt,
						   IQLToLLVMTypedValue val)
{
  IQLToLLVMArrayElement * tmp = new IQLToLLVMArrayElement(val);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMLocal::IQLToLLVMLocal(IQLToLLVMTypedValue val,
			       llvm::Value * nullBit,
                               llvm::Type * nullBitTy)
  :
  mValue(val),
  mNullBit(nullBit),
  mNullBitType(nullBitTy)
{
  BOOST_ASSERT((mNullBit != nullptr && mNullBitType != nullptr) || (mNullBit == nullptr && mNullBitType == nullptr));
}

IQLToLLVMLocal::~IQLToLLVMLocal()
{
}

const IQLToLLVMValue * IQLToLLVMLocal::getValuePointer(CodeGenerationContext * ctxt) const
{
  return mValue.getValue();
}

llvm::Value * IQLToLLVMLocal::getNullBitPointer() const
{
  return mNullBit;
}

void IQLToLLVMLocal::setNull(CodeGenerationContext * ctxt, bool isNull) const
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  b->CreateStore(isNull ? b->getTrue() : b->getFalse(), mNullBit);
}

bool IQLToLLVMLocal::isNullable() const
{
  return mNullBit != NULL;
}

llvm::Value * IQLToLLVMLocal::getValue(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mValue.getValue()->getValue(ctxt);
  if(ctxt->isPointerToValueType(outputVal, mValue.getType())) {
    outputVal = ctxt->LLVMBuilder->CreateLoad(mValue.getType()->LLVMGetType(ctxt),
                                              outputVal);
  }
  return outputVal;
}

llvm::Value * IQLToLLVMLocal::getNull(CodeGenerationContext * ctxt) const
{
  if (isNullable()) {
    llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
    return b->CreateLoad(mNullBitType, mNullBit);
  } else {
    return nullptr;
  }
}

bool IQLToLLVMLocal::isLiteralNull() const
{
  return false;
}

IQLToLLVMValue::ValueType IQLToLLVMLocal::getValueType() const
{
  return mValue.getValue()->getValueType();
}

IQLToLLVMLocal * IQLToLLVMLocal::get(CodeGenerationContext * ctxt,
				     IQLToLLVMTypedValue lval,
				     llvm::Value * lvalNull,
                                     llvm::Type * lvalNullTy)
{
  IQLToLLVMLocal * tmp = new IQLToLLVMLocal(lval, lvalNull, lvalNullTy);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMLocal * IQLToLLVMLocal::get(CodeGenerationContext * ctxt,
				     IQLToLLVMTypedValue lval)
{
  IQLToLLVMLocal * tmp = new IQLToLLVMLocal(lval, nullptr, nullptr);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

IQLToLLVMArgument::IQLToLLVMArgument(llvm::Value * val, const FieldType * ty, llvm::Value * basePointer)
  :
  mValue(val),
  mType(ty),
  mBasePointer(basePointer)
{
}

IQLToLLVMArgument::~IQLToLLVMArgument()
{
}

const IQLToLLVMValue * IQLToLLVMArgument::getValuePointer(CodeGenerationContext * ctxt) const
{
  return IQLToLLVMRValue::get(ctxt, mValue, IQLToLLVMValue::eGlobal);
}

void IQLToLLVMArgument::setNull(CodeGenerationContext * ctxt, bool isNull) const
{
}

bool IQLToLLVMArgument::isNullable() const
{
  return false;
}

llvm::Value * IQLToLLVMArgument::getValue(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mValue;
  if(ctxt->isPointerToValueType(outputVal, mType)) {
    outputVal = ctxt->LLVMBuilder->CreateLoad(mType->LLVMGetType(ctxt),
                                              outputVal);
  }
  return outputVal;
}

llvm::Value * IQLToLLVMArgument::getNull(CodeGenerationContext * ctxt) const
{
  return nullptr;
}

bool IQLToLLVMArgument::isLiteralNull() const
{
  return false;
}

IQLToLLVMValue::ValueType IQLToLLVMArgument::getValueType() const
{
  return IQLToLLVMValue::eGlobal;
}

llvm::Value * IQLToLLVMArgument::getBasePointer(CodeGenerationContext * ctxt) const
{
  return mBasePointer;
}

IQLToLLVMArgument * IQLToLLVMArgument::get(CodeGenerationContext * ctxt,
					   llvm::Value * val,
					   const FieldType * ty,
					   llvm::Value * basePointer)
{
  IQLToLLVMArgument * tmp = new IQLToLLVMArgument(val, ty, basePointer);
  getValueFactory(ctxt).push_back(tmp);
  return tmp;
}

CodeGenerationFunctionContext::CodeGenerationFunctionContext()
  :
  Builder(NULL),
  mSymbolTable(NULL),
  Function(NULL),
  RecordArguments(NULL),
  OutputRecord(NULL),
  AllocaCache(NULL)
{
}

void CodeGenerationFunctionContext::clear()
{
  delete Builder;
  Builder = nullptr;
  delete unwrap(RecordArguments);
  RecordArguments = nullptr; 
  delete mSymbolTable;
  mSymbolTable = nullptr; 
  delete AllocaCache;
  AllocaCache = nullptr; 
  Function = nullptr;
  OutputRecord = nullptr;
}

CodeGenerationContext::CodeGenerationContext()
  :
  mOwnsModule(true),
  mSymbolTable(NULL),
  AggFn(0),
  AllocaCache(NULL),
  IQLOutputRecord(NULL),
  IQLRecordArguments(NULL),
  LLVMMemcpyIntrinsic(NULL),
  LLVMMemsetIntrinsic(NULL),
  LLVMMemcmpIntrinsic(NULL),
  LLVMContext(new llvm::LLVMContext()),
  LLVMModule(new llvm::Module("my cool JIT", *LLVMContext)),
  LLVMBuilder(NULL),
  LLVMDecContextPtrType(NULL),
  LLVMDecimal128Type(NULL),
  LLVMVarcharType(NULL),
  LLVMCidrV4Type(NULL),
  LLVMInt32Type(NULL),
  LLVMFunction(NULL),
  IQLMoveSemantics(0),
  IsIdentity(true)
{
  // We disable this because we want to avoid it's installation of
  // signal handlers.
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  std::unique_ptr<llvm::TargetMachine> tm(llvm::EngineBuilder().selectTarget());
  LLVMModule->setDataLayout(tm->createDataLayout());

  this->createFunctionContext(TypeCheckConfiguration::get());

  LLVMInt8Type = llvm::Type::getInt8Ty(*this->LLVMContext);
  LLVMInt16Type = llvm::Type::getInt16Ty(*this->LLVMContext);
  LLVMInt32Type = llvm::Type::getInt32Ty(*this->LLVMContext);
  LLVMInt64Type = llvm::Type::getInt64Ty(*this->LLVMContext);
  LLVMFloatType = llvm::Type::getFloatTy(*this->LLVMContext);
  LLVMDoubleType = llvm::Type::getDoubleTy(*this->LLVMContext);
#if DECSUBST
  static unsigned numDecContextMembers(8);
#else
  static unsigned numDecContextMembers(7);
#endif
  // Declare extern functions for decNumber
  llvm::Type * decContextMembers[numDecContextMembers];
  decContextMembers[0] = llvm::Type::getInt32Ty(*this->LLVMContext);
  decContextMembers[1] = llvm::Type::getInt32Ty(*this->LLVMContext);
  decContextMembers[2] = llvm::Type::getInt32Ty(*this->LLVMContext);
  // This is actually enum; is this OK?
  decContextMembers[3] = llvm::Type::getInt32Ty(*this->LLVMContext);
  // These two are actually unsigned in decContext
  decContextMembers[4] = llvm::Type::getInt32Ty(*this->LLVMContext);
  decContextMembers[5] = llvm::Type::getInt32Ty(*this->LLVMContext);
  decContextMembers[6] = llvm::Type::getInt8Ty(*this->LLVMContext);
#if DECSUBST
  decContextMembers[7] = llvm::Type::getInt8Ty(*this->LLVMContext);
#endif
  this->LLVMDecContextPtrType = llvm::PointerType::get(llvm::StructType::get(*this->LLVMContext,
										 llvm::ArrayRef(&decContextMembers[0], numDecContextMembers),
										 0),
							   0);
  // Don't quite understand LLVM behavior of what happens with you pass { [16 x i8] } by value on the call stack.
  // It seems to align each member at 4 bytes and therefore is a gross overestimate of the actually representation.
  //llvm::Type * decimal128Member = LLVMArrayType(llvm::Type::getInt8Ty(*ctxt->LLVMContext), DECIMAL128_Bytes);
  llvm::Type * decimal128Members[DECIMAL128_Bytes/sizeof(int32_t)];
  for(unsigned int i=0; i<DECIMAL128_Bytes/sizeof(int32_t); ++i)
    decimal128Members[i]= llvm::Type::getInt32Ty(*this->LLVMContext);;
  this->LLVMDecimal128Type = llvm::StructType::get(*this->LLVMContext,
						       llvm::ArrayRef(&decimal128Members[0], DECIMAL128_Bytes/sizeof(int32_t)),
						       1);

  // Set up VARCHAR type
  llvm::Type * varcharMembers[3];
  varcharMembers[0] = llvm::Type::getInt32Ty(*this->LLVMContext);
  varcharMembers[1] = llvm::Type::getInt32Ty(*this->LLVMContext);
  varcharMembers[2] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  this->LLVMVarcharType = llvm::StructType::get(*this->LLVMContext,
						    llvm::ArrayRef(&varcharMembers[0], 3),
						    0);
  // DATETIME runtime type
  this->LLVMDatetimeType = llvm::Type::getInt64Ty(*this->LLVMContext);
  // CIDRV4 runtime type
  varcharMembers[0] = llvm::Type::getInt32Ty(*this->LLVMContext);
  varcharMembers[1] = llvm::Type::getInt8Ty(*this->LLVMContext);
  this->LLVMCidrV4Type = llvm::StructType::get(*this->LLVMContext,
                                                   llvm::ArrayRef(&varcharMembers[0], 2),
                                                   0);
  this->LLVMIPV6Type = llvm::ArrayType::get(llvm::Type::getInt8Ty(*LLVMContext), 16U);
  this->LLVMCidrV6Type = llvm::ArrayType::get(llvm::Type::getInt8Ty(*LLVMContext), 17U);
  // DATETIME runtime type
  this->LLVMInt32Type = llvm::Type::getInt32Ty(*this->LLVMContext);

  // Try to register the program as a source of symbols to resolve against.
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(0, NULL);
  // Prototypes for external functions we want to provide access to.
  // TODO:Handle redefinition of function
  llvm::Type * argumentTypes[10];
  unsigned numArguments=0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  llvm::Type * funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  llvm::Value * libFunVal = LoadAndValidateExternalFunction("InternalDecimalAdd", funTy);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalSub", funTy);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalMul", funTy);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalDiv", funTy);
  
  numArguments=0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalNeg", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromChar", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::Type::getInt8Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromInt8", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::Type::getInt16Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromInt16", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromInt32", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromInt64", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::Type::getFloatTy(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromFloat", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromDouble", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalFromDatetime", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalDecimalCmp", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("round", "InternalDecimalRound", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharAdd", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharCopy", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharAllocate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharErase", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharEquals", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharRLike", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharNE", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharLT", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharLE", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharGT", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharGE", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  funTy = llvm::FunctionType::get(this->LLVMDatetimeType, 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDatetimeFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(this->LLVMDatetimeType, 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDatetimeFromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalDateFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalCharFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("length", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt8Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromInt8", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt16Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromInt16", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromInt32", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromInt64", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getFloatTy(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromFloat", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromDouble", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromIPv4", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromCIDRv4", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromIPv6", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
				  llvm::ArrayRef(&argumentTypes[0], numArguments),
				  0);
  libFunVal = LoadAndValidateExternalFunction("InternalVarcharFromCIDRv6", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt8FromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt8FromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt8FromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt8FromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt8FromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt16Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt16FromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt16Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt16FromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt16Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt16FromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt16Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt16FromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt16Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt16FromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt32FromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt32FromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt32FromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt32FromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt32Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt32FromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt64Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt64FromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt64Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt64FromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt64Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt64FromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt64Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt64FromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt64Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalInt64FromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getDoubleTy(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalDoubleFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getDoubleTy(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalDoubleFromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getDoubleTy(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalDoubleFromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getDoubleTy(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalDoubleFromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getDoubleTy(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), 
			   llvm::ArrayRef(&argumentTypes[0], numArguments), 
			   0);
  libFunVal = LoadAndValidateExternalFunction("InternalDoubleFromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("urldecode", "InternalVarcharUrlDecode", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("urlencode", "InternalVarcharUrlEncode", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("replace", "InternalVarcharReplace", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("locate", "InternalVarcharLocate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("substr", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("trim", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("ltrim", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("rtrim", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("lower", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("upper", funTy);

  // Date functions
  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("dayofweek", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("dayofmonth", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("dayofyear", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("month", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("year", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("last_day", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datediff", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("julian_day", funTy);

  // Datetime functions
  numArguments = 0;
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("utc_timestamp", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("unix_timestamp", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("from_unixtime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datetime_add_day", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datetime_add_month", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datetime_add_year", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datetime_add_second", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datetime_add_minute", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("datetime_add_hour", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date_add_day", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date_add_month", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date_add_year", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date_add_second", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date_add_minute", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt64Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("date_add_hour", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("SuperFastHash", funTy);

  // Print Functions
  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::Type::getInt8Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::Type::getInt8Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintCharRaw", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintInt64", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintDouble", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt32Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintIPv4", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getInt64Ty(*this->LLVMContext);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintCIDRv4", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintIPv6", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalPrintCIDRv6", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("ceil", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("floor", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("sqrt", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("log", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("exp", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("sin", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("cos", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("asin", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::Type::getDoubleTy(*this->LLVMContext);
  funTy = llvm::FunctionType::get(llvm::Type::getDoubleTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("acos", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("free", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("ip_address", "InternalIPAddress", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("parse_ip_address", "InternalParseIPAddress", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  // argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("InternalIPAddressAddrBlockMatch", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0);
  funTy = llvm::FunctionType::get(llvm::Type::getInt32Ty(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("is_v4_ip_address", "InternalIsV4IPAddress", funTy);

  numArguments = 0;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  libFunVal = LoadAndValidateExternalFunction("InternalArrayException", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = llvm::PointerType::get(this->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = this->LLVMDecContextPtrType;
  funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext), llvm::ArrayRef(&argumentTypes[0], numArguments), 0);
  LoadAndValidateExternalFunction("getcwd", "InternalGetCwd", funTy);

  // LLVM intrinsics we want to use
  CreateMemcpyIntrinsic();
  CreateMemsetIntrinsic();
  CreateMemcmpIntrinsic();
  // Compiler for the code
    
  mFPM = new llvm::legacy::FunctionPassManager(LLVMModule);

  // Set up the optimizer pipeline.
  // Do simple "peephole" optimizations and bit-twiddling optzns.
  mFPM->add(llvm::createInstructionCombiningPass());
  // Reassociate expressions.
  mFPM->add(llvm::createReassociatePass());
  // Eliminate Common SubExpressions.
  mFPM->add(llvm::createGVNPass());
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  mFPM->add(llvm::createCFGSimplificationPass());

  mFPM->doInitialization();
}

CodeGenerationContext::~CodeGenerationContext()
{
  delete mFPM;
  typedef std::vector<IQLToLLVMValue *> factory;
  for(factory::iterator it = ValueFactory.begin();
      it != ValueFactory.end();
      ++it) {
    delete *it;
  }
  // Delete but do not double delete all allocated builders
  std::set<llvm::IRBuilder<> *> builders;
  if (LLVMBuilder) {
    builders.insert(LLVMBuilder);
  }
  if (Update.Builder != nullptr) {
    builders.insert(Update.Builder);
  }
  if (Initialize.Builder != nullptr) {
    builders.insert(Initialize.Builder);
  }
  if (Transfer.Builder != nullptr) {
    builders.insert(Transfer.Builder);
  }
  for(auto b : builders) {
    delete b;
  }
  
  // Delete but do not double delete all allocated symbol tables
  std::set<TreculSymbolTable *> tables;
  if (mSymbolTable) {
    tables.insert(mSymbolTable);
  }
  if (Update.mSymbolTable != nullptr) {
    tables.insert(Update.mSymbolTable);
  }
  if (Initialize.mSymbolTable != nullptr) {
    tables.insert(Initialize.mSymbolTable);
  }
  if (Transfer.mSymbolTable != nullptr) {
    tables.insert(Transfer.mSymbolTable);
  }
  for(auto b : tables) {
    delete b;
  }
  
  // Delete but do not double delete all allocated record args
  std::set<IQLToLLVMRecordMapRef> args;
  if (IQLRecordArguments) {
    args.insert(IQLRecordArguments);
  }
  if (Update.RecordArguments != nullptr) {
    args.insert(Update.RecordArguments);
  }
  if (Initialize.RecordArguments != nullptr) {
    args.insert(Initialize.RecordArguments);
  }
  if (Transfer.RecordArguments != nullptr) {
    args.insert(Transfer.RecordArguments);
  }
  for(auto b : args) {
    delete unwrap(b);
  }

  if (mOwnsModule && LLVMModule) {
    delete LLVMModule;
    LLVMModule = NULL;
  }
  if (LLVMContext) {
    delete LLVMContext;
    LLVMContext = NULL;
  }

  // Delete but do not double delete all allocated alloca caches
  std::set<local_cache *> caches;
  if (AllocaCache) {
    caches.insert(AllocaCache);
  }
  if (Update.AllocaCache != nullptr) {
    caches.insert(Update.AllocaCache);
  }
  if (Initialize.AllocaCache != nullptr) {
    caches.insert(Initialize.AllocaCache);
  }
  if (Transfer.AllocaCache != nullptr) {
    caches.insert(Transfer.AllocaCache);
  }
  for(auto b : caches) {
    delete b;
  }

  while(IQLCase.size()) {
    delete IQLCase.top();
    IQLCase.pop();
  }
}

void CodeGenerationContext::disownModule()
{
  mOwnsModule = false;
}

std::unique_ptr<llvm::Module> CodeGenerationContext::takeModule()
{
  return llvm::CloneModule(*LLVMModule);
}

llvm::Type * CodeGenerationContext::getType(const RecordType * ty)
{
  std::vector<llvm::Type *> members;
  for(RecordType::const_member_iterator it = ty->begin_members(), end = ty->end_members(); it != end; ++it) {
    members.push_back(it->getType()->LLVMGetType(this));
  }
  return llvm::StructType::get(*LLVMContext,
                               llvm::ArrayRef(&members[0], members.size()),
                               0);
}

llvm::Type * CodeGenerationContext::getType(const CharType * ty)
{
  return llvm::ArrayType::get(llvm::Type::getInt8Ty(*LLVMContext), (unsigned) (ty->GetSize() + 1));
}

llvm::Type * CodeGenerationContext::getType(const FixedArrayType * ty)
{
  if (ty->getElementType()->isNullable()) {
    llvm::Type * fixedArrayMembers[2];
    fixedArrayMembers[0] = llvm::ArrayType::get(ty->getElementType()->LLVMGetType(this), (unsigned) ty->GetSize());
    fixedArrayMembers[1] = llvm::ArrayType::get(LLVMBuilder->getInt8Ty(), ty->GetNullSize());
    llvm::StructType * structTy =  llvm::StructType::get(*LLVMContext,
                                                         llvm::ArrayRef(&fixedArrayMembers[0], 2),
                                                         0);
    // if (auto JTMBOrErr = llvm::orc::JITTargetMachineBuilder::detectHost()) {
    //   std::optional<llvm::orc::JITTargetMachineBuilder> JTMB = std::move(*JTMBOrErr);
    //   if (auto DLOrErr = JTMB->getDefaultDataLayoutForTarget()) {
    //     std::optional<llvm::DataLayout> DL = std::move(*DLOrErr);
    //     const llvm::StructLayout * layout = DL->getStructLayout(structTy);
    //     BOOST_ASSERT(layout->getElementOffset(0) == ty->GetDataOffset());
    //     BOOST_ASSERT(layout->getElementOffset(1) == ty->GetNullOffset());
    //     if (layout->getSizeInBytes() != ty->GetAllocSize()) {
    //       throw std::runtime_error((boost::format("layout->getSizeInBytes() != GetAllocSize(); layout->getSizeInBytes() = %1% GetAllocSize() = %2%") %
    //                                 layout->getSizeInBytes() %
    //                                 ty->GetAllocSize()).str());
    //     }
    //   }
    // }
    return structTy;
  } else {
    return llvm::ArrayType::get(ty->getElementType()->LLVMGetType(this), (unsigned) ty->GetSize());
  }
}

bool CodeGenerationContext::isValueType(const FieldType * ft)
{
  switch(ft->GetEnum()) {    
  case FieldType::VARCHAR:
  case FieldType::CHAR:
  case FieldType::FIXED_ARRAY:
  case FieldType::VARIABLE_ARRAY:
  case FieldType::BIGDECIMAL:
  case FieldType::IPV6:
  case FieldType::CIDRV6:
  case FieldType::FUNCTION:
  case FieldType::NIL:
  case FieldType::STRUCT:
    return false;
  case FieldType::INT8:
  case FieldType::INT16:
  case FieldType::INT32:
  case FieldType::INT64:
  case FieldType::FLOAT:
  case FieldType::DOUBLE:
  case FieldType::DATETIME:
  case FieldType::DATE:
  case FieldType::IPV4:
  case FieldType::CIDRV4:
  case FieldType::INTERVAL:
    return true;
  default:
    throw std::runtime_error("INTERNAL ERROR: CodeGenerationContext::isValueType unexpected type");
  }
}

bool CodeGenerationContext::isPointerToValueType(llvm::Value * val, const FieldType * ft)
{
  return isValueType(ft) && llvm::Type::PointerTyID == val->getType()->getTypeID();
}

void CodeGenerationContext::defineVariable(const char * name,
					   llvm::Value * val,
                                           const FieldType * ft, 
					   llvm::Value * nullVal,
					   llvm::Type * nullValTy,
					   IQLToLLVMValue::ValueType globalOrLocal)
{
  const IQLToLLVMValue * tmp = IQLToLLVMRValue::get(this, val, 
                                                    NULL, globalOrLocal);
  IQLToLLVMLocal * local = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(tmp, ft), nullVal, nullValTy);
  mSymbolTable->add(name, NULL, local);
}

void CodeGenerationContext::defineFieldVariable(llvm::Value * basePointer,
						const char * prefix,
						const char * memberName,
						const RecordType * recordType)
{
  IQLToLLVMField * field = IQLToLLVMField::get(this,
					       recordType,
					       memberName,
					       basePointer);
  mSymbolTable->add(prefix, memberName, field);
}

void CodeGenerationContext::buildFree(const IQLToLLVMValue * val, const FieldType * ft)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * f = b->GetInsertBlock()->getParent();

  llvm::Value * callArgs[1];
  llvm::Function * fn = LLVMModule->getFunction("free");
  if (FieldType::VARCHAR == ft->GetEnum()) {
    llvm::Value * varcharPtr = val->getValue(this);
    // If large model varchar then free pointer
    llvm::BasicBlock * smallBB = llvm::BasicBlock::Create(*c, "small", f);
    llvm::BasicBlock * largeBB = llvm::BasicBlock::Create(*c, "large", f);
    llvm::BasicBlock * contBB = llvm::BasicBlock::Create(*c, "cont", f);
    b->CreateCondBr(buildVarArrayIsSmall(varcharPtr), smallBB, largeBB);
    b->SetInsertPoint(smallBB);
    // Nothing in small case
    b->CreateBr(contBB);
    b->SetInsertPoint(largeBB);
    // Large case get pointer from varchar struct and free it
    callArgs[0] = b->CreateLoad(b->getPtrTy(),
                                b->CreateStructGEP(LLVMVarcharType, varcharPtr, 2));
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 1), "");
    b->CreateBr(contBB);
    b->SetInsertPoint(contBB);
  } else if (FieldType::FIXED_ARRAY == ft->GetEnum() ||
             FieldType::VARIABLE_ARRAY == ft->GetEnum()) {
    // TODO: Optimize and only generate loop if there is another variable array or varchar below this
    // array
    // Types of elements are different so elementwise iteration and conversion is necessary
    const FieldType * eltTy = dynamic_cast<const SequentialType *>(ft)->getElementType();
    BOOST_ASSERT(nullptr != eltTy);
    if (FieldType::VARCHAR == eltTy->GetEnum() ||
        FieldType::FIXED_ARRAY == eltTy->GetEnum() ||
        FieldType::VARIABLE_ARRAY == eltTy->GetEnum() ||
	FieldType::STRUCT == eltTy->GetEnum()) {
      // Loop over the array call Free
      // Constants one is used frequently
      const IQLToLLVMValue * zero = buildFalse();
      const IQLToLLVMValue * one  = buildTrue();
      const IQLToLLVMValue * sz = IQLToLLVMRValue::get(this, getArraySize(val, ft), IQLToLLVMValue::eLocal);
      // INTEGER type used frequently in this method
      FieldType * int32Type = Int32Type::Get(ft->getContext());

      // DECLARE idx = 0
      // Allocate and initialize counter
      llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
      const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
      IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
      buildSetNullableValue(counterLValue, zero, int32Type, int32Type);
        
      whileBegin();

      // while (idx < sz)
      const IQLToLLVMValue * pred = buildCompare(buildRef(counter, int32Type), int32Type, sz, int32Type, int32Type, IQLToLLVMOpLT);
      whileStatementBlock(pred, int32Type);

      const IQLToLLVMValue * idx = buildRef(counter, int32Type);
      const IQLToLLVMValue * elt = buildArrayRef(val, ft, idx, int32Type, eltTy);
      buildFree(elt, eltTy);
      
      // SET idx = idx + 1
      buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
      whileFinish();
    }    
    if (FieldType::VARIABLE_ARRAY == ft->GetEnum()) {
      callArgs[0] = b->CreateLoad(b->getPtrTy(),
                                  b->CreateStructGEP(LLVMVarcharType, val->getValue(this), 2));
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 1), "");
    }
  } else if (FieldType::STRUCT == ft->GetEnum()) {
    const RecordType * structType = dynamic_cast<const RecordType *>(ft);
    for(std::size_t i=0; i < structType->GetSize(); ++i) {
      const FieldType * eltType = structType->getElementType(i);
      const IQLToLLVMValue * idx = IQLToLLVMRValue::get(this, b->getInt64(i), IQLToLLVMValue::eLocal);
      const IQLToLLVMValue * elt = buildRowRef(val, ft, idx, Int64Type::Get(ft->getContext()), eltType);
      buildFree(elt, eltType);
    }
  }
}

void CodeGenerationContext::buildRecordTypeFree(const RecordType * recordType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  const IQLToLLVMValue * basePointer = lookupBasePointer("__BasePointer__");
  llvm::Value * callArgs[1];
  llvm::Function * fn = LLVMModule->getFunction("free");

  for(auto it = recordType->begin_members(); it != recordType->end_members(); ++it) {
    if (FieldType::VARCHAR == it->GetType()->GetEnum() ||
        FieldType::FIXED_ARRAY == it->GetType()->GetEnum() ||
        FieldType::VARIABLE_ARRAY == it->GetType()->GetEnum() ||
        FieldType::STRUCT == it->GetType()->GetEnum()) {
      const IQLToLLVMValue * field = buildVariableRef(it->GetName().c_str(), it->GetType());
      buildFree(field, it->GetType());
    }
  }

  callArgs[0] = b->CreateLoad(b->getPtrTy(), basePointer->getValue(this));
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 1), "");
}

void CodeGenerationContext::buildRecordTypePrint(const RecordType * recordType, char fieldDelimiter, char recordDelimiter, char escapeChar)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * ostrPointer = b->CreateLoad(b->getPtrTy(), lookupBasePointer("__OutputStreamPointer__")->getValue(this));

  // Create a C-string with the delimiters
  llvm::Value * fieldDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString(std::string(1, fieldDelimiter))->getValue(this), b->getPtrTy());
  llvm::Value * recordDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString(std::string(1, recordDelimiter))->getValue(this), b->getPtrTy());

  // For printing delimiters
  llvm::Value * callArgs[2];
  callArgs[1] = ostrPointer;
  llvm::Function * fn = LLVMModule->getFunction("InternalPrintCharRaw");
  
  for(auto it = recordType->begin_members(); it != recordType->end_members(); ++it) {
    if (it != recordType->begin_members()) {
      // output field delimiter
      callArgs[0] = fieldDelimiterStringPtr;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");
    }
    const IQLToLLVMValue * field = buildVariableRef(it->GetName().c_str(), it->GetType());
    buildPrint(field, it->GetType(), fieldDelimiter, escapeChar);
  }

  const FieldType * int32Type = Int32Type::Get(recordType->getContext());

  // If requested output record delimiter, output it.
  // A bit of a hack here since __OutputRecordDelimiter__ local does not have
  // type information so we add it here.
  IQLToLLVMLocal outputRecordDelimiter(IQLToLLVMTypedValue(lookup("__OutputRecordDelimiter__")->getValuePointer(this), int32Type), nullptr, nullptr);
  const IQLToLLVMValue * pred = buildCompare(buildRef(&outputRecordDelimiter, int32Type), int32Type, buildTrue(), int32Type, int32Type, IQLToLLVMOpEQ);
  buildBeginIf();
  // output record delimiter
  callArgs[0] = recordDelimiterStringPtr;
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");
  buildEndIf(pred);
}

void CodeGenerationContext::buildPrint(const IQLToLLVMValue * val, const FieldType * ft, char fieldDelimiter, char escapeChar)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * ostrPointer = b->CreateLoad(b->getPtrTy(), lookupBasePointer("__OutputStreamPointer__")->getValue(this));
  llvm::Value * fieldDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString(std::string(1, fieldDelimiter))->getValue(this), b->getPtrTy());
  llvm::Value * beginArrayDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString("{")->getValue(this), b->getPtrTy());
  llvm::Value * endArrayDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString("}")->getValue(this), b->getPtrTy());
  llvm::Value * beginStructDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString("(")->getValue(this), b->getPtrTy());
  llvm::Value * endStructDelimiterStringPtr = b->CreateBitCast(buildGlobalConstString(")")->getValue(this), b->getPtrTy());
  llvm::Value * nullStringPtr = b->CreateBitCast(buildGlobalConstString("\\N")->getValue(this), b->getPtrTy());

  // For printing delimiters and NULL string
  llvm::Value * callArgs[3];
  llvm::Function * charFn = LLVMModule->getFunction("InternalPrintCharRaw");

  // // Checking for NULL (last parameter in call means we are checking IsNotNull)
  const IQLToLLVMValue * pred = buildIsNull(val, ft, Int32Type::Get(ft->getContext()), true);
  buildBeginIf();
  switch(ft->GetEnum()) {
  case FieldType::VARCHAR:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintVarchar");
      callArgs[0] = val->getValue(this);
      callArgs[1] = b->getInt8(escapeChar);
      callArgs[2] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "");  
      break;
    }
  case FieldType::CHAR:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintChar");
      callArgs[0] = b->CreateBitCast(val->getValue(this), b->getPtrTy());
      callArgs[1] = b->getInt8(escapeChar);
      callArgs[2] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "");  
      break;
    }
  case FieldType::BIGDECIMAL:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintDecimal");
      callArgs[0] = val->getValue(this);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::INT8:
  case FieldType::INT16:
  case FieldType::INT32:
  case FieldType::INT64:
  case FieldType::INTERVAL:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintInt64");
      callArgs[0] = buildCast(val, ft, Int64Type::Get(ft->getContext()))->getValue(this);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::FLOAT:
  case FieldType::DOUBLE:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintDouble");
      callArgs[0] = buildCast(val, ft, DoubleType::Get(ft->getContext()))->getValue(this);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::DATETIME:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintDatetime");
      callArgs[0] = val->getValue(this);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::DATE:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintDate");
      callArgs[0] = val->getValue(this);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::IPV4:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintIPv4");
      callArgs[0] = val->getValue(this);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::CIDRV4:
    {
      // Pass CIDRv4 arg as int64_t
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintCIDRv4");
      llvm::Value * argTmp = buildEntryBlockAlloca(ft->LLVMGetType(this), "cidrv4ArgTmp");
      b->CreateStore(val->getValue(this), argTmp);
      llvm::Type * int64PtrTy = llvm::PointerType::get(b->getInt64Ty(), 0);
      argTmp = LLVMBuilder->CreateBitCast(argTmp, int64PtrTy, "charcnvcasttmp1");
      callArgs[0] = b->CreateLoad(b->getInt64Ty(), argTmp);
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::IPV6:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintIPv6");
      callArgs[0] = b->CreateBitCast(val->getValue(this), b->getPtrTy());
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::CIDRV6:
    {
      llvm::Function * fn = LLVMModule->getFunction("InternalPrintCIDRv6");
      callArgs[0] = b->CreateBitCast(val->getValue(this), b->getPtrTy());
      callArgs[1] = ostrPointer;
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::FIXED_ARRAY:
  case FieldType::VARIABLE_ARRAY:
    {
      callArgs[0] = beginArrayDelimiterStringPtr;
      callArgs[1] = ostrPointer;
      b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");  

      const SequentialType * arrType = dynamic_cast<const SequentialType *>(ft);
      BOOST_ASSERT(nullptr != arrType);
      const FieldType * eltType = arrType->getElementType();
      // // Loop over the array and print
      // // Constants one is used frequently
      const IQLToLLVMValue * one  = buildTrue();
      const IQLToLLVMValue * zero  = buildFalse();
      // INTEGER type used frequently in this method
      FieldType * int32Type = Int32Type::Get(ft->getContext());

      // DECLARE idx = 0
      // Allocate and initialize counter
      llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
      const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
      IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
      buildSetNullableValue(counterLValue, buildFalse(), int32Type, int32Type);
      
      const IQLToLLVMValue * endIdx = IQLToLLVMRValue::get(this, getArraySize(val, ft), IQLToLLVMValue::eLocal);
      
      whileBegin();
      
      // while (idx < endIdx)
      const IQLToLLVMValue * loopPred = buildCompare(buildRef(counter, int32Type), int32Type, endIdx, int32Type, int32Type, IQLToLLVMOpLT);
      whileStatementBlock(loopPred, int32Type);

      // IF idx > 0 PRINT fieldDelimiter
      loopPred = buildCompare(buildRef(counter, int32Type), int32Type, zero, int32Type, int32Type, IQLToLLVMOpGT);
      buildBeginIf();
      callArgs[0] = fieldDelimiterStringPtr;
      callArgs[1] = ostrPointer;
      b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");
      buildEndIf(loopPred);

      const IQLToLLVMValue * idx = buildRef(counter, int32Type);
      const IQLToLLVMValue * elt = buildArrayRef(val, ft, idx, int32Type, eltType);
      buildPrint(elt, eltType, fieldDelimiter, escapeChar);

      // SET idx = idx + 1
      buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
      whileFinish();      

      callArgs[0] = endArrayDelimiterStringPtr;
      callArgs[1] = ostrPointer;
      b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  case FieldType::STRUCT:
    {
      callArgs[0] = beginStructDelimiterStringPtr;
      callArgs[1] = ostrPointer;
      b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");  

      const RecordType * structType = dynamic_cast<const RecordType *>(ft);
      for(std::size_t i=0; i < structType->GetSize(); ++i) {
        if (i > 0) {
          callArgs[0] = fieldDelimiterStringPtr;
          callArgs[1] = ostrPointer;
          b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");
        }

        const FieldType * eltType = structType->getElementType(i);
        const IQLToLLVMValue * idx = IQLToLLVMRValue::get(this, b->getInt64(i), IQLToLLVMValue::eLocal);
        const IQLToLLVMValue * elt = buildRowRef(val, ft, idx, Int64Type::Get(ft->getContext()), eltType);
        buildPrint(elt, eltType, fieldDelimiter, escapeChar);
      }
      
      callArgs[0] = endStructDelimiterStringPtr;
      callArgs[1] = ostrPointer;
      b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");  
      break;
    }
  default:
    throw std::runtime_error("Print not implemented");
  }
  buildBeginElse();
  callArgs[0] = nullStringPtr;
  callArgs[1] = ostrPointer;
  b->CreateCall(charFn->getFunctionType(), charFn, llvm::ArrayRef(&callArgs[0], 2), "");  
  buildEndIf(pred);
}

const IQLToLLVMLValue * 
CodeGenerationContext::lookup(const char * name)
{
  TreculSymbolTableEntry * lval = mSymbolTable->lookup(name); 
  return lval->getValue();
}

const IQLToLLVMValue * 
CodeGenerationContext::lookupBasePointer(const char * name)
{
  TreculSymbolTableEntry * lval = mSymbolTable->lookup(name);
  return lval->getValue()->getValuePointer(this);
}

llvm::Value * CodeGenerationContext::getContextArgumentRef()
{
  return lookupBasePointer("__DecimalContext__")->getValue(this);
}

void CodeGenerationContext::reinitializeForTransfer(const TypeCheckConfiguration & typeCheckConfig)
{
  delete AllocaCache;
  delete mSymbolTable;
  mSymbolTable = new TreculSymbolTable(typeCheckConfig);
  AllocaCache = new local_cache();
  AggFn = 0;
}

void CodeGenerationContext::reinitialize()
{
  // Reinitialize and create transfer
  mSymbolTable->clear();
  LLVMFunction = NULL;
  unwrap(IQLRecordArguments)->clear();
  for(auto & c : *AllocaCache) {
    c.second.clear();
  }
  AggFn = 0;
}

void CodeGenerationContext::createFunctionContext(const TypeCheckConfiguration & typeCheckConfig)
{
  // Should only call this when these members are null, but...
  BOOST_ASSERT(nullptr == LLVMBuilder);
  BOOST_ASSERT(nullptr == mSymbolTable);
  BOOST_ASSERT(nullptr == IQLRecordArguments);
  BOOST_ASSERT(nullptr == AllocaCache);
  LLVMBuilder = new llvm::IRBuilder<>(*LLVMContext);
  mSymbolTable = new TreculSymbolTable(typeCheckConfig);
  LLVMFunction = NULL;
  IQLRecordArguments = wrap(new std::map<std::string, std::pair<std::string, const RecordType*> >());
  IQLOutputRecord = NULL;
  AllocaCache = new local_cache();
}

void CodeGenerationContext::dumpSymbolTable()
{
  // mSymbolTable->dump();
}

void CodeGenerationContext::restoreAggregateContext(CodeGenerationFunctionContext * fCtxt)
{
  BOOST_ASSERT(nullptr == this->LLVMBuilder);
  BOOST_ASSERT(nullptr == this->mSymbolTable);
  BOOST_ASSERT(nullptr == this->LLVMFunction);
  BOOST_ASSERT(nullptr == this->IQLRecordArguments);
  BOOST_ASSERT(nullptr == this->IQLOutputRecord);
  BOOST_ASSERT(nullptr == this->AllocaCache);
  std::swap(this->LLVMBuilder, fCtxt->Builder);
  std::swap(this->mSymbolTable, fCtxt->mSymbolTable);
  std::swap(this->LLVMFunction, fCtxt->Function);
  std::swap(this->IQLRecordArguments, fCtxt->RecordArguments);
  std::swap(this->IQLOutputRecord, fCtxt->OutputRecord);
  std::swap(this->AllocaCache, fCtxt->AllocaCache);
}

void CodeGenerationContext::saveAggregateContext(CodeGenerationFunctionContext * fCtxt)
{
  BOOST_ASSERT(nullptr == fCtxt->Builder);
  BOOST_ASSERT(nullptr == fCtxt->mSymbolTable);
  BOOST_ASSERT(nullptr == fCtxt->Function);
  BOOST_ASSERT(nullptr == fCtxt->RecordArguments);
  BOOST_ASSERT(nullptr == fCtxt->OutputRecord);
  BOOST_ASSERT(nullptr == fCtxt->AllocaCache);
  std::swap(fCtxt->Builder, this->LLVMBuilder);
  std::swap(fCtxt->mSymbolTable, this->mSymbolTable);
  std::swap(fCtxt->Function, this->LLVMFunction);
  std::swap(fCtxt->RecordArguments, this->IQLRecordArguments);
  std::swap(fCtxt->OutputRecord, this->IQLOutputRecord);
  std::swap(fCtxt->AllocaCache, this->AllocaCache);
}

void CodeGenerationContext::addInputRecordType(const char * name, 
					       const char * argumentName, 
					       const RecordType * rec)
{
  boost::dynamic_bitset<> mask;
  mask.resize(rec->size(), true);
  addInputRecordType(name, argumentName, rec, mask);
}

void CodeGenerationContext::addInputRecordType(const char * name, 
					       const char * argumentName, 
					       const RecordType * rec,
					       const boost::dynamic_bitset<>& mask)
{
  llvm::Value * basePointer = lookup(argumentName)->getValuePointer(this)->getValue(this);
  for(RecordType::const_member_iterator it = rec->begin_members();
      it != rec->end_members();
      ++it) {
    std::size_t idx = (std::size_t) std::distance(rec->begin_members(), it);
    if (!mask.test(idx)) continue;
    rec->LLVMMemberGetPointer(it->GetName(), 
			      this, 
			      basePointer,
			      true, // Put the member into the symbol table
			      name);
  }
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(IQLRecordArguments));
  recordTypes[name] = std::make_pair(argumentName, rec);

  // Add the record itself as a value in the symbol table, but as a pointer not a pointer to pointer
  llvm::IRBuilder<> * b = LLVMBuilder;
  auto valPointer = b->CreateLoad(b->getPtrTy(), basePointer);
  mSymbolTable->add(name, nullptr, IQLToLLVMArgument::get(this, valPointer, rec, basePointer));  
}

llvm::Value * CodeGenerationContext::addExternalFunction(const char * treculName,
							 const char * implName,
							 llvm::Type * funTy)
{
  mTreculNameToSymbol[treculName] = implName;
  return llvm::Function::Create(llvm::dyn_cast<llvm::FunctionType>(funTy), 
				llvm::GlobalValue::ExternalLinkage,
				implName, LLVMModule);
  
}

void CodeGenerationContext::buildDeclareLocal(const char * nm, const FieldType * ft)
{
  // TODO: Check for duplicate declaration
  llvm::Value * allocAVal = buildEntryBlockAlloca(ft,nm);
  if (ft->isNullable()) {
    // NULL handling
    llvm::Value * nullVal = NULL;
    llvm::IRBuilder<> * b = LLVMBuilder;
    nullVal = buildEntryBlockAlloca(b->getInt1Ty(), 
                                    (boost::format("%1%NullBit") %
                                     nm).str().c_str());
    b->CreateStore(b->getFalse(), nullVal);
    defineVariable(nm, allocAVal, ft, nullVal, b->getInt1Ty(), IQLToLLVMValue::eLocal);
  } else {
    defineVariable(nm, allocAVal, ft, nullptr, nullptr, IQLToLLVMValue::eLocal);
  }

}

void CodeGenerationContext::buildLocalVariable(const char * nm, const IQLToLLVMValue * init, const FieldType * ft)
{
  // TODO: Temporary hack dealing with a special case in which the
  // initializing expression has already allocated a slot for the
  // lvalue.
  if (ft->GetEnum() == FieldType::FIXED_ARRAY && 
      !ft->isNullable() &&
      init->getValueType() == IQLToLLVMValue::eLocal) {
    defineVariable(nm, init->getValue(this), ft, nullptr, nullptr, IQLToLLVMValue::eLocal);    
  } else {
    // Allocate local
    buildDeclareLocal(nm, ft);
    // Create a temporary LValue object
    const IQLToLLVMLValue * localLVal = lookup(nm);
    // Set the value; no need to type promote since type of the
    // variable is inferred from the initializer
    // No need to free a global lhs since this is local and just created
    buildSetNullableValue(localLVal, init, ft, false, false);
  }
}

void CodeGenerationContext::whileBegin()
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the condition, loop body and continue.
  std::stack<class IQLToLLVMStackRecord* > & stk(IQLStack);
  stk.push(new IQLToLLVMStackRecord());
  stk.top()->StartBB = b->GetInsertBlock();
  stk.top()->ThenBB = llvm::BasicBlock::Create(*c, "whileCond", TheFunction);
  stk.top()->ElseBB = llvm::BasicBlock::Create(*c, "whileBody");
  stk.top()->MergeBB = llvm::BasicBlock::Create(*c, "whileCont");

  // We do an unconditional branch to the condition block
  // so the loop has somewhere to branch to.
  b->CreateBr(stk.top()->ThenBB);
  b->SetInsertPoint(stk.top()->ThenBB);  
}

void CodeGenerationContext::whileStatementBlock(const IQLToLLVMValue * condVal,
						const FieldType * condTy)
{  
  // Test the condition and branch 
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * f = b->GetInsertBlock()->getParent();
  std::stack<class IQLToLLVMStackRecord* > & stk(IQLStack);
  f->insert(f->end(), stk.top()->ElseBB);
  conditionalBranch(condVal, condTy, stk.top()->ElseBB, stk.top()->MergeBB);
  b->SetInsertPoint(stk.top()->ElseBB);
}

void CodeGenerationContext::whileFinish()
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * f = b->GetInsertBlock()->getParent();
  std::stack<class IQLToLLVMStackRecord* > & stk(IQLStack);

  // Branch to reevaluate loop predicate
  b->CreateBr(stk.top()->ThenBB);
  f->insert(f->end(), stk.top()->MergeBB);
  b->SetInsertPoint(stk.top()->MergeBB);

  // Done with this entry
  delete stk.top();
  stk.pop();
}

void CodeGenerationContext::conditionalBranch(const IQLToLLVMValue * condVal,
					      const FieldType * condTy,
					      llvm::BasicBlock * trueBranch,
					      llvm::BasicBlock * falseBranch)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * f = b->GetInsertBlock()->getParent();
  
  // Handle ternary logic here
  llvm::Value * nv = condVal->getNull(this);
  if (nv) {
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "notNull", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, falseBranch);
    b->SetInsertPoint(notNullBB);
  }
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(this),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, trueBranch, falseBranch);
}

const IQLToLLVMValue * 
CodeGenerationContext::buildArray(std::vector<IQLToLLVMTypedValue>& vals,
				  const FieldType * arrayTy)
{
  // Detect if this is an array of constants
  // TODO: We need analysis or attributes that tell us whether the
  // array is const before we can make it static.  Right now we are just
  // making an generally invalid assumption that an array of numeric
  // constants is in fact const.
  bool isConstArray=true;
  for(std::vector<IQLToLLVMTypedValue>::iterator v = vals.begin(),
	e = vals.end(); v != e; ++v) {
    if (!v->getType()->isNumeric() || !llvm::isa<llvm::Constant>(v->getValue()->getValue(this))) {
      isConstArray = false;
      break;
    }
  }

  if (isConstArray) {
    return buildGlobalConstArray(vals, arrayTy);
  }

  // TODO: This is potentially inefficient.  Will LLVM remove the extra copy?
  // Even if it does, how much are we adding to the compilation time while
  // it cleans up our mess.
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  const FieldType * eltTy = dynamic_cast<const SequentialType*>(arrayTy)->getElementType();
  BOOST_ASSERT(nullptr != eltTy);
  llvm::Value * result = buildEntryBlockAlloca(arrayTy, "nullableBinOp");
  const IQLToLLVMValue * arrayVal = IQLToLLVMRValue::get(this, result, IQLToLLVMValue::eLocal);
  int32_t sz = vals.size();
  const VariableArrayType * varArrayTy = dynamic_cast<const VariableArrayType *>(arrayTy);
  if (nullptr != varArrayTy) {
    buildVariableArrayAllocate(result, varArrayTy, b->getInt32(sz), true);
  }
  for (int32_t i=0; i<sz; ++i) {
    // TODO: type promotions???
    const IQLToLLVMLValue * arrElt = buildArrayLValue(arrayVal, arrayTy,
                                                      IQLToLLVMRValue::get(this, b->getInt64(i), IQLToLLVMValue::eLocal), Int64Type::Get(arrayTy->getContext()),
                                                      eltTy);                      
    
    buildSetNullableValue(arrElt, vals[i].getValue(), vals[i].getType(), eltTy);
  }

  // return pointer to array
  return arrayVal;
}

const IQLToLLVMValue * 
CodeGenerationContext::buildGlobalConstArray(std::vector<IQLToLLVMTypedValue>& vals,
					     const FieldType * arrayTy)
{
  llvm::Module * m = LLVMModule;
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::ArrayType * arrayType = 
    llvm::dyn_cast<llvm::ArrayType>(arrayTy->LLVMGetType(this));
  BOOST_ASSERT(arrayType != NULL);
  llvm::GlobalVariable * globalArray = 
    new llvm::GlobalVariable(*m, arrayType, true, llvm::GlobalValue::InternalLinkage,
                             0, "constArray");
  globalArray->setAlignment(llvm::MaybeAlign(16));

  // Make initializer for the global.
  std::vector<llvm::Constant *> initializerArgs;
  for(std::vector<IQLToLLVMTypedValue>::const_iterator v = vals.begin(),
	e = vals.end(); v != e; ++v) {
    initializerArgs.push_back(llvm::cast<llvm::Constant>(v->getValue()->getValue(this)));
  }
  llvm::Constant * constArrayInitializer = 
    llvm::ConstantArray::get(arrayType, initializerArgs);
  globalArray->setInitializer(constArrayInitializer);

  
  return IQLToLLVMRValue::get(this, globalArray, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * 
CodeGenerationContext::buildGlobalConstString(const std::string & str)
{
  auto it = StringPool.find(str);
  if (StringPool.end() != it) {
    return it->second;
  }
  llvm::Module * m = LLVMModule;
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::ArrayType * arrayType = llvm::ArrayType::get(b->getInt8Ty(), str.size()+1);
  llvm::GlobalVariable * globalArray = 
    new llvm::GlobalVariable(*m, arrayType, true, llvm::GlobalValue::InternalLinkage,
                             0, "constArray");
  globalArray->setAlignment(llvm::MaybeAlign(16));

  // Make initializer for the global.
  std::vector<llvm::Constant *> initializerArgs;
  for(auto & v : str) {
    initializerArgs.push_back(llvm::cast<llvm::Constant>(b->getInt8(v)));
  }
  initializerArgs.push_back(llvm::cast<llvm::Constant>(b->getInt8(0)));
  llvm::Constant * constArrayInitializer = 
    llvm::ConstantArray::get(arrayType, initializerArgs);
  globalArray->setInitializer(constArrayInitializer);

  
  auto ret = IQLToLLVMRValue::get(this, globalArray, IQLToLLVMValue::eLocal);
  StringPool[str] = ret;
  return ret;
}

const IQLToLLVMValue * CodeGenerationContext::buildArrayRef(const IQLToLLVMValue * arr,
                                                            const FieldType * arrType,
                                                            const IQLToLLVMValue * idx,
                                                            const FieldType * idxType,
                                                            const FieldType * retType)
{
  return buildArrayLValue(arr, arrType, idx, idxType, retType);
}

const IQLToLLVMLValue * CodeGenerationContext::buildArrayLValue(const IQLToLLVMValue * arr,
                                                                const FieldType * arrType,
                                                                const IQLToLLVMValue * idx,
                                                                const FieldType * idxType,
                                                                const FieldType * retType)
{
  llvm::Module * m = LLVMModule;
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // llvm::Function *f = b->GetInsertBlock()->getParent();
  llvm::Value * lval = arr->getValue(this);

  // Convert index to int64
  // TODO: Not handling NULL so type check should be enforcing this!
  llvm::Value * idxVal = idx->getValue(this);
  idxVal = b->CreateSExt(idxVal, b->getInt64Ty());

  const FieldType * eltType = nullptr;
  const FixedArrayType * fixedArrType = dynamic_cast<const FixedArrayType *>(arrType);
  if (fixedArrType != nullptr) {
    eltType = fixedArrType->getElementType();
  } else {
    eltType = dynamic_cast<const VariableArrayType *>(arrType)->getElementType();
  }
  
  llvm::Value * dataPtr = getArrayData(arr, arrType);
  dataPtr = b->CreateGEP(eltType->LLVMGetType(this),
                         dataPtr,
                         idxVal,
                         "arrayLValue");
  const IQLToLLVMValue * iqlDataPtr = IQLToLLVMRValue::get(this, dataPtr, arr->getValueType());
  if (eltType->isNullable()) {
    // This means an array of nullable elements.
    llvm::Value * nullPtr = getArrayNull(arr, arrType);
    llvm::Value * bytePos = b->CreateLShr(idxVal, 3);
    llvm::Value * mask = b->CreateShl(b->getInt8(1), b->CreateTrunc(b->CreateSub(idxVal, b->CreateShl(bytePos, 3)), b->getInt8Ty()));
    llvm::Value * nullBytePtr = b->CreateGEP(b->getInt8Ty(),
                                             nullPtr,
                                             bytePos,
                                             "arrayIsNull");
    return IQLToLLVMArrayElement::get(this, IQLToLLVMTypedValue(iqlDataPtr, eltType), nullBytePtr, mask);
  } else {
    // This includes the case of an array of non-nullable elements

    // Seeing about 50% performance overhead for the bounds checking.
    // Not implementing this until I have some optimization mechanism
    // for safely removing them in some cases (perhaps using LLVM).

    // const llvm::PointerType * ptrType = llvm::dyn_cast<llvm::PointerType>(lval->getType());
    // llvm::Type * lvalType = ptrType->getPointerElementType();
    // BOOST_ASSERT(ptrType != NULL);
    // llvm::Value * lowerBound = b->CreateICmpSLT(idxVal,
    // 						b->getInt64(0));
    // llvm::Value * upperBound = b->CreateICmpSGE(idxVal,
    // 						b->getInt64(arrayType->getNumElements()));
    // llvm::Value * cond = b->CreateOr(lowerBound, upperBound);
    // // TODO: Make a single module-wide basic block for the exceptional case?
    // llvm::BasicBlock * goodBlock = 
    //   llvm::BasicBlock::Create(*c, "arrayDereference", f);
    // llvm::BasicBlock * exceptionBlock = 
    //   llvm::BasicBlock::Create(*c, "arrayIndexException", f);
    // // Branch and set block
    // b->CreateCondBr(cond, exceptionBlock, goodBlock);
    // // Array out of bounds exception
    // b->SetInsertPoint(exceptionBlock); 
    // b->CreateCall(m->getFunction("InternalArrayException"));
    // // We should never make the branch since we actually
    // // throw in the called function.
    // b->CreateBr(goodBlock);

    // // // Array check good: emit the value.
    // // b->SetInsertPoint(goodBlock);
    // lvalType = arrayType->getElementType();
    // lval = b->CreateBitCast(lval, llvm::PointerType::get(lvalType,0));
    // // GEP to get pointer to the correct offset.
    // llvm::Value * gepIndexes[1] = { idxVal };
    // lval = b->CreateInBoundsGEP(lvalType, lval, llvm::ArrayRef<llvm::Value*>(&gepIndexes[0], &gepIndexes[1]));
    // return IQLToLLVMLocal::get(this, IQLToLLVMRValue::get(this, 
    //                                               lval,
    //                                               IQLToLLVMValue::eLocal),
    //                           NULL);
    return IQLToLLVMArrayElement::get(this, IQLToLLVMTypedValue(iqlDataPtr, eltType));
  }
}

const IQLToLLVMValue *
CodeGenerationContext::buildArrayConcat(const IQLToLLVMValue * lhs,
                                        const FieldType * lhsType,
                                        const IQLToLLVMValue * rhs,
                                        const FieldType * rhsType,
                                        const FieldType * retType)
{
  if (retType->GetEnum() == FieldType::FIXED_ARRAY ||
      retType->GetEnum() == FieldType::VARIABLE_ARRAY) {
    const SequentialType * retArrType = dynamic_cast<const SequentialType *>(retType);
    const FieldType * int32Type = Int32Type::Get(retType->getContext());
    const SequentialType * lhsArrType = dynamic_cast<const SequentialType *>(lhsType);
    const IQLToLLVMValue * lhsSize = nullptr != lhsArrType ? IQLToLLVMRValue::get(this, getArraySize(lhs, lhsArrType), IQLToLLVMValue::eLocal) : buildTrue();
    const SequentialType * rhsArrType = dynamic_cast<const SequentialType *>(rhsType);
    const IQLToLLVMValue * rhsSize = nullptr != rhsArrType ? IQLToLLVMRValue::get(this, getArraySize(rhs, rhsArrType), IQLToLLVMValue::eLocal) : buildTrue();
    llvm::Value * ret = buildEntryBlockAlloca(retType->LLVMGetType(this), "arrayConcatRet");
    const IQLToLLVMValue * retVal = IQLToLLVMRValue::get(this, ret, IQLToLLVMValue::eLocal);
    if (retType->GetEnum() == FieldType::VARIABLE_ARRAY) {
      const IQLToLLVMValue * retSize = buildAdd(lhsSize, int32Type, rhsSize, int32Type, int32Type);
      buildVariableArrayAllocate(ret, retArrType, retSize->getValue(this), true);
    }
    if (nullptr != lhsArrType) {
      // TODO: Generalize the memcpy code to handle this case...
      buildArrayElementwiseCopy(lhs, lhsArrType, buildFalse(), lhsSize, buildFalse(), retVal, retArrType);
    } else {
      const FieldType * retEltTy = retArrType->getElementType();
      const IQLToLLVMValue * converted = buildCast(lhs, lhsType, retEltTy);
      const IQLToLLVMLValue * arrayLValue = buildArrayLValue(retVal, retType, buildFalse(), int32Type, retEltTy);
      buildSetNullableValue(arrayLValue, converted, retEltTy, retEltTy); 
    }
    if (nullptr != rhsArrType) {
      buildArrayElementwiseCopy(rhs, rhsArrType, buildFalse(), rhsSize, lhsSize, retVal, retArrType); 
    } else {
      const FieldType * retEltTy = retArrType->getElementType();
      const IQLToLLVMValue * converted = buildCast(rhs, rhsType, retEltTy);
      const IQLToLLVMLValue * arrayLValue = buildArrayLValue(retVal, retType, lhsSize, int32Type, retEltTy);
      buildSetNullableValue(arrayLValue, converted, retEltTy, retEltTy); 
    }
    return IQLToLLVMRValue::get(this, ret, IQLToLLVMValue::eLocal);
  }
  return nullptr;
}

llvm::Value * CodeGenerationContext::getRowData(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getRowData(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getRowData(llvm::Value * ptr, const FieldType * ty)
{
  BOOST_ASSERT(FieldType::STRUCT == ty->GetEnum());
  const RecordType * structTy = reinterpret_cast<const RecordType*>(ty);
  llvm::IRBuilder<> * b = LLVMBuilder;
  ptr = b->CreateBitCast(ptr, b->getPtrTy());
  // GEP to get pointer to data and cast back to pointer to struct
  return b->CreateBitCast(b->CreateGEP(b->getInt8Ty(), ptr, b->getInt64(structTy->GetDataOffset()), ""),
                          llvm::PointerType::get(structTy->LLVMGetType(this), 0));
}

llvm::Value * CodeGenerationContext::getRowNull(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getRowNull(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getRowNull(llvm::Value * ptr, const FieldType * ty)
{
  BOOST_ASSERT(FieldType::STRUCT == ty->GetEnum());
  const RecordType * structTy = reinterpret_cast<const RecordType*>(ty);
  if(!structTy->hasNullFields()) {
    return nullptr;
  }
  llvm::IRBuilder<> * b = LLVMBuilder;
  ptr = b->CreateBitCast(ptr, b->getPtrTy());
  // GEP to get pointer to offset
  return b->CreateGEP(b->getInt8Ty(), ptr, b->getInt64(structTy->GetNullOffset()), "");
}


const IQLToLLVMValue * 
CodeGenerationContext::buildRow(std::vector<IQLToLLVMTypedValue>& vals,
                                FieldType * rowTy)
{
  // TODO: This is potentially inefficient.  Will LLVM remove the extra copy?
  // Even if it does, how much are we adding to the compilation time while
  // it cleans up our mess.
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  const RecordType * structTy = dynamic_cast<const RecordType*>(rowTy);
  llvm::Type * retTy = structTy->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nullableBinOp");
  const IQLToLLVMValue * structVal = IQLToLLVMRValue::get(this, result, IQLToLLVMValue::eLocal);
  int32_t sz = structTy->GetSize();
  for (int32_t i=0; i<sz; ++i) {
    const FieldType * eltTy = structTy->getElementType(i);
    const IQLToLLVMLValue * structElt = buildRowLValue(structVal, structTy,
                                                       IQLToLLVMRValue::get(this, b->getInt64(i), IQLToLLVMValue::eLocal), Int64Type::Get(structTy->getContext()),
                                                       eltTy);
    // No need for type promotion or freeing lhs since this is a c'tor
    buildSetNullableValue(structElt, vals[i].getValue(), eltTy, false, false);
  }

  // return pointer to struct
  return structVal;
}

const IQLToLLVMLValue * CodeGenerationContext::buildRowRef(const IQLToLLVMValue * row,
                                                           const FieldType * rowType,
                                                           std::size_t idx,
                                                           const FieldType * retType)
{
  const IQLToLLVMValue * iqlIdx = IQLToLLVMRValue::get(this, LLVMBuilder->getInt64(idx), IQLToLLVMValue::eLocal);
  return buildRowRef(row, rowType, iqlIdx, Int64Type::Get(rowType->getContext()), retType);
}

const IQLToLLVMLValue * CodeGenerationContext::buildRowRef(const IQLToLLVMValue * row,
                                                           const FieldType * rowType,
                                                           const IQLToLLVMValue * idx,
                                                           const FieldType * idxType,
                                                           const FieldType * retType)
{
  return buildRowLValue(row, rowType, idx, idxType, retType);
}

const IQLToLLVMLValue * CodeGenerationContext::buildRowRef(const IQLToLLVMValue * row,
                                                           const FieldType * rowType,
                                                           const std::string & member,
                                                           const FieldType * retType)
{
  return buildRowLValue(row, rowType, member, retType);
}

const IQLToLLVMLValue * CodeGenerationContext::buildRowLValue(const IQLToLLVMValue * row,
                                                              const FieldType * rowType,
                                                              const IQLToLLVMValue * idx,
                                                              const FieldType * idxType,
                                                              const FieldType * retType)
{
  llvm::Module * m = LLVMModule;
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * lval = row->getValue(this);

  // Index must be constant integer
  llvm::ConstantInt * idxVal = llvm::cast<llvm::ConstantInt>(idx->getValue(this));
  if (nullptr == idxVal) {
    throw std::runtime_error("index accessing row member must be a constant integer");
  }
  uint64_t i = idxVal->getZExtValue();

  const RecordType * structType = dynamic_cast<const RecordType *>(rowType);

  // We want the base pointer in this IQLToLLVMField to be a pointer to a uint8_t *.
  auto arg = dynamic_cast<const IQLToLLVMArgument *>(row);
  if (nullptr != arg) {
    return IQLToLLVMField::get(this, structType, i, arg->getBasePointer(this));
  } else {
    llvm::Type * int8Ptr = llvm::PointerType::get(b->getInt8Ty(), 0);
    llvm::Value * argTmp = buildEntryBlockAlloca(int8Ptr, "rowEltBaseArgTmp");
    lval = b->CreateBitCast(lval, int8Ptr, "rowEltBase");
    b->CreateStore(lval, argTmp);  
    return IQLToLLVMField::get(this, structType, i, argTmp);
  }
}

const IQLToLLVMLValue * CodeGenerationContext::buildRowLValue(const IQLToLLVMValue * row,
                                                              const FieldType * rowType,
                                                              const std::string & member,
                                                              const FieldType * retType)
{
  const RecordType * recTy = dynamic_cast<const RecordType *>(rowType);
  BOOST_ASSERT(nullptr != recTy);
  BOOST_ASSERT(recTy->hasMember(member));
  std::size_t idx = recTy->getMemberIndex(member);
  const IQLToLLVMValue * iqlIdx = IQLToLLVMRValue::get(this, LLVMBuilder->getInt64(idx), IQLToLLVMValue::eLocal);
  return buildRowLValue(row, rowType, iqlIdx, Int64Type::Get(rowType->getContext()), retType);
}

void CodeGenerationContext::buildStructElementwiseCopy(const IQLToLLVMValue * e, 
                                                       const RecordType * argType,
                                                       std::size_t beginIdx,
                                                       std::size_t endIdx,
                                                       std::size_t retBeginIdx, 
                                                       const IQLToLLVMValue * ret, 
                                                       const RecordType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // INTEGER type used frequently in this method
  FieldType * int64Type = Int64Type::Get(argType->getContext());
  for(std::size_t sourceIdx=beginIdx, destIdx=retBeginIdx; sourceIdx < endIdx; ++sourceIdx, ++destIdx) {
    // Types of elements are different so elementwise iteration and conversion is necessary
    const FieldType * argEltTy = argType->getElementType(sourceIdx);
    const FieldType * retEltTy = retType->getElementType(destIdx);
      
    const IQLToLLVMValue * converted = buildCast(buildRowRef(e, argType, sourceIdx, argEltTy), argEltTy, retEltTy);
    const IQLToLLVMValue * retIdx = IQLToLLVMRValue::get(this, b->getInt64(destIdx), IQLToLLVMValue::eLocal);
    const IQLToLLVMLValue * rowLValue = buildRowLValue(ret, retType, retIdx, int64Type, retEltTy);
    // Don't free lhs of elements since all freeing was done at the container level
    buildSetNullableValue(rowLValue, converted, retEltTy, false, false);
  }
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCall(const char * treculName,
				 const std::vector<IQLToLLVMTypedValue> & args,
				 llvm::Value * retTmp,
				 const FieldType * retType)
{
  // Get the implementation name of the function.
  std::map<std::string,std::string>::const_iterator it = mTreculNameToSymbol.find(treculName);
  if (mTreculNameToSymbol.end() == it) {
    throw std::runtime_error((boost::format("Unable to find implementation for "
					    "function %1%") % treculName).str());
  }
  llvm::LLVMContext & c(*LLVMContext);
  llvm::IRBuilder<> * b = LLVMBuilder;

  std::vector<llvm::Value *> callArgs;
  llvm::Function * fn = LLVMModule->getFunction(it->second.c_str());
  if (fn == NULL) {
    throw std::runtime_error((boost::format("Call to function %1% passed type checking "
					    "but implementation function %2% does not exist") %
			      treculName % it->second).str());
  }
  
  for(std::size_t i=0; i<args.size(); i++) {
    if (args[i].getType()->GetEnum() == FieldType::CHAR ||
        args[i].getType()->GetEnum() == FieldType::IPV6 ||
        args[i].getType()->GetEnum() == FieldType::CIDRV6) {
      llvm::Value * e = args[i].getValue()->getValue(this);
      // CHAR(N), CIDRV6 and IPV6 arg must pass by reference
      // Pass as a pointer to int8.  pointer to char(N) is too specific
      // for a type signature.
      llvm::Type * int8Ptr = llvm::PointerType::get(b->getInt8Ty(), 0);
      llvm::Value * ptr = LLVMBuilder->CreateBitCast(e, int8Ptr, "charcnvcasttmp1");
      callArgs.push_back(ptr);
    } else if (args[i].getType()->GetEnum() == FieldType::CIDRV4) {
      // Pass as int64_t since passing as CidrV4 doesn't work.   Passing as int64_t is what
      // Clang does so we do that to.   We need to alloca a temporary so that we have a pointer to cast.
      llvm::Value * argTmp = buildEntryBlockAlloca(args[i].getType()->LLVMGetType(this), "cidrv4ArgTmp");
      b->CreateStore(args[i].getValue()->getValue(this), argTmp);
      llvm::Type * int64PtrTy = llvm::PointerType::get(b->getInt64Ty(), 0);
      argTmp = LLVMBuilder->CreateBitCast(argTmp, int64PtrTy, "charcnvcasttmp1");
      callArgs.push_back(b->CreateLoad(b->getInt64Ty(), argTmp));
    } else {
      callArgs.push_back(args[i].getValue()->getValue(this));
    }
  }
  
  llvm::FunctionType * fnTy = fn->getFunctionType();
  if (fnTy->getReturnType() == llvm::Type::getVoidTy(c)) {
    // Validate the calling convention.  If returning void must
    // also take RuntimeContext as last argument and take pointer
    // to return as next to last argument.
    if (callArgs.size() + 2 != fnTy->getNumParams() ||
	fnTy->getParamType(fnTy->getNumParams()-1) != 
	LLVMDecContextPtrType ||
	!fnTy->getParamType(fnTy->getNumParams()-2)->isPointerTy())
      throw std::runtime_error("Internal Error");


    // Must alloca a value for the return value and pass as an arg.
    // No guarantee that the type of the formal of the function is exactly
    // the same as the LLVM ret type (in particular, CHAR(N) return
    // values will have a int8* formal) so we do a bitcast.   Also fixed arrays
    // will be passed as pointers to the element type as well.
    llvm::Value * retVal = retTmp;
    if (FieldType::CHAR == retType->GetEnum()) {
      retVal = LLVMBuilder->CreateBitCast(retVal,
					  llvm::PointerType::get(b->getInt8Ty(), 0),
					  "callReturnTempCast"); 
    } else if(FieldType::FIXED_ARRAY == retType->GetEnum()) {
      const FixedArrayType * arrTy = dynamic_cast<const FixedArrayType*>(retType);
      retVal = LLVMBuilder->CreateBitCast(retVal,
					  llvm::PointerType::get(arrTy->getElementType()->LLVMGetType(this), 0),
					  "callReturnTempCast"); 
    }
    // const llvm::Type * retTy = retType->LLVMGetType(this);
    // // The return type is determined by next to last argument.
    // llvm::Type * retArgTy = llvm::cast<llvm::PointerType>(fnTy->getParamType(fnTy->getNumParams()-2))->getElementType();
    // if (retTy != retArgTy) {
    //   const llvm::ArrayType * arrTy = llvm::dyn_cast<llvm::ArrayType>(retTy);
    //   if (retArgTy != b->getInt8Ty() ||
    //       NULL == arrTy ||
    //       arrTy->getElementType() != b->getInt8Ty()) {
    //     throw std::runtime_error("INTERNAL ERROR: mismatch between IQL function "
    //     			 "return type and LLVM formal argument type.");
    //   }
    //   retVal = LLVMBuilder->CreateBitCast(retVal,
    //     				  llvm::PointerType::get(retArgTy, 0),
    //     				  "callReturnTempCast");
    // }
    callArgs.push_back(retVal);					
    // Also must pass the context for allocating the string memory.
    llvm::Value * contextVal = getContextArgumentRef();
    // BOOST_ASSERT(llvm::cast<llvm::PointerType>(contextVal->getType())->getElementType() == LLVMDecContextPtrType);
    callArgs.push_back(LLVMBuilder->CreateLoad(LLVMDecContextPtrType,
					       contextVal,
					       "ctxttmp"));    
    LLVMBuilder->CreateCall(fnTy,
			    fn, 
			    llvm::ArrayRef(&callArgs[0], callArgs.size()),
			    "");
    // Return was the second to last entry in the arg list.
    return IQLToLLVMValue::eLocal;
  } else {
    llvm::Value * r = LLVMBuilder->CreateCall(fnTy,
                                              fn, 
                                              llvm::ArrayRef(&callArgs[0], callArgs.size()),
                                              "call");
    b->CreateStore(r, retTmp);
    return IQLToLLVMValue::eLocal;
  }
}

const IQLToLLVMValue *
CodeGenerationContext::buildCall(const char * f,
				 const std::vector<IQLToLLVMTypedValue> & args,
				 const FieldType * retType)
{
  if (boost::algorithm::iequals(f, "least")) {
    return buildLeastGreatest(args, retType, true);
  } else if (boost::algorithm::iequals(f, "greatest")) {
    return buildLeastGreatest(args, retType, false);
  } else if (boost::algorithm::iequals(f, "isnull") ||
	     boost::algorithm::iequals(f, "ifnull")) {
    return buildIsNullFunction(args, retType);
  } else if (boost::algorithm::iequals(f, "hash")) {
    return buildHash(args);
  } else if (boost::algorithm::iequals(f, "family")) {
    return buildNullableUnaryOp(args[0].getValue(), args[0].getType(), retType, &CodeGenerationContext::buildFamily);  
  } else if (boost::algorithm::iequals(f, "masklen")) {
    return buildNullableUnaryOp(args[0].getValue(), args[0].getType(), retType, &CodeGenerationContext::buildMasklen);  
  } else {
    llvm::Type * retTy = retType->LLVMGetType(this);
    llvm::Value * retTmp = buildEntryBlockAlloca(retTy, "callReturnTemp");
    IQLToLLVMValue::ValueType vt = buildCall(f, args, retTmp, retType);
    // Get rid of unecessary temporary if applicable.
    retTmp = trimAlloca(retTmp, retType);
    if (isPointerToValueType(retTmp, retType)) {
      // Unlikely but possible to get here.  Pointers to 
      // value type are almost surely trimmed above.
      llvm::IRBuilder<> * b = LLVMBuilder;
      retTmp = b->CreateLoad(retTy, retTmp);
    }
    return IQLToLLVMRValue::get(this, retTmp, vt);
  }
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastInt8(const IQLToLLVMValue * e, 
                                     const FieldType * argType, 
                                     llvm::Value * ret, 
                                     const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::INT16:
  case FieldType::INT32:
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateTrunc(e1, 
				       b->getInt8Ty(),
				       "castInt8ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FLOAT:
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPToSI(e1, 
					b->getInt8Ty(),
					"castDoubleToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalInt8FromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalInt8FromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalInt8FromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalInt8FromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalInt8FromDatetime", args, ret, retType);
    }
  default:
    // TODO: Cast INTEGER to DECIMAL
    throw std::runtime_error ((boost::format("Cast to TINYINT from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastInt8(const IQLToLLVMValue * e, 
                                                            const FieldType * argType, 
                                                            const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastInt8);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastInt16(const IQLToLLVMValue * e, 
				      const FieldType * argType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
    {
      llvm::Value * r = b->CreateSExt(e1, 
				      b->getInt16Ty(),
				      "castInt8ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::INT16:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::INT32:
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateTrunc(e1, 
				       b->getInt16Ty(),
				       "castInt64ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FLOAT:
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPToSI(e1, 
					b->getInt16Ty(),
					"castDoubleToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalInt16FromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalInt16FromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalInt16FromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalInt16FromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalInt16FromDatetime", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to SMALLINT from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastInt16(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastInt16);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastInt32(const IQLToLLVMValue * e, 
				      const FieldType * argType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
  case FieldType::INT16:
    {
      llvm::Value * r = b->CreateSExt(e1, 
				      b->getInt32Ty(),
				      "castInt8ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::INTERVAL:
  case FieldType::INT32:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateTrunc(e1, 
				       b->getInt32Ty(),
				       "castInt64ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FLOAT:
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPToSI(e1, 
					b->getInt32Ty(),
					"castDoubleToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalInt32FromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalInt32FromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalInt32FromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalInt32FromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalInt32FromDatetime", args, ret, retType);
    }
  case FieldType::IPV4:
    {
      auto in = e->getValue(this);
      auto out = b->CreateOr(b->CreateOr(b->CreateOr(b->CreateShl(b->CreateAnd(in, b->getInt32(0xff)), 24),
                                                     b->CreateShl(b->CreateAnd(in, b->getInt32(0xff00)), 8)),
                                         b->CreateLShr(b->CreateAnd(in, b->getInt32(0xff0000)), 8)),
                             b->CreateLShr(b->CreateAnd(in, b->getInt32(0xff000000)), 24));
      b->CreateStore(out, ret);
      return IQLToLLVMValue::eLocal;
    }
  default:
    throw std::runtime_error ((boost::format("Cast to INTEGER from %1% not "
					     "implemented.") % 
			       argType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastInt32(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastInt32);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastInt64(const IQLToLLVMValue * e, 
				      const FieldType * argType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
  case FieldType::INT16:
  case FieldType::INT32:
  case FieldType::INTERVAL:
    {
      llvm::Value * r = b->CreateSExt(e1, 
				      b->getInt64Ty(),
				      "castInt32ToInt64");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::INT64:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::FLOAT:
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPToSI(e1, 
					b->getInt64Ty(),
					"castDoubleToInt64");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalInt64FromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalInt64FromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalInt64FromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalInt64FromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalInt64FromDatetime", args, ret, retType);
    }
  default:
    // TODO: 
    throw std::runtime_error ((boost::format("Cast to BIGINT from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastInt64(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastInt64);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastFloat(const IQLToLLVMValue * e, 
                                      const FieldType * argType, 
                                      llvm::Value * ret, 
                                      const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
  case FieldType::INT16:
  case FieldType::INT32:
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateSIToFP(e1, 
					b->getFloatTy(),
					"castIntToFloat");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FLOAT:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPTrunc(e1, 
                                         b->getFloatTy(),
                                         "castDoubleToFloat");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
    // case FieldType::CHAR:
    //   {
    //     return buildCall("InternalDoubleFromChar", args, ret, retType);
    //   }
    // case FieldType::VARCHAR:
    //   {
    //     return buildCall("InternalDoubleFromVarchar", args, ret, retType);
    //   }
    // case FieldType::BIGDECIMAL:
    //   {
    //     return buildCall("InternalDoubleFromDecimal", args, ret, retType);
    //   }
    // case FieldType::DATE:
    //   {
    //     return buildCall("InternalDoubleFromDate", args, ret, retType);
    //   }
    // case FieldType::DATETIME:
    //   {
    //     return buildCall("InternalDoubleFromDatetime", args, ret, retType);
    //   }
  default:
    throw std::runtime_error ((boost::format("Cast to REAL from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastFloat(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastFloat);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDouble(const IQLToLLVMValue * e, 
				       const FieldType * argType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
  case FieldType::INT16:
  case FieldType::INT32:
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateSIToFP(e1, 
					b->getDoubleTy(),
					"castIntToDouble");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FLOAT:
    {
      llvm::Value * r = b->CreateFPExt(e1, 
                                       b->getDoubleTy(),
                                       "castFloatToDouble");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::DOUBLE:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::CHAR:
    {
      return buildCall("InternalDoubleFromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDoubleFromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalDoubleFromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalDoubleFromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalDoubleFromDatetime", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to DOUBLE PRECISION from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDouble(const IQLToLLVMValue * e, 
                                                              const FieldType * argType, 
                                                              const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDouble);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDecimal(const IQLToLLVMValue * e, 
                                        const FieldType * argType, 
                                        llvm::Value * ret, 
                                        const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
    {
      return buildCall("InternalDecimalFromInt8", args, ret, retType);
    }
  case FieldType::INT16:
    {
      return buildCall("InternalDecimalFromInt16", args, ret, retType);
    }
  case FieldType::INT32:
    {
      return buildCall("InternalDecimalFromInt32", args, ret, retType);
    }
  case FieldType::INT64:
    {
      return buildCall("InternalDecimalFromInt64", args, ret, retType);
    }
  case FieldType::FLOAT:
    {
      return buildCall("InternalDecimalFromFloat", args, ret, retType);
    }
  case FieldType::DOUBLE:
    {
      return buildCall("InternalDecimalFromDouble", args, ret, retType);
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalDecimalFromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDecimalFromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    b->CreateStore(b->CreateLoad(argType->LLVMGetType(this), e1), ret);
    return e->getValueType();
  case FieldType::DATE:
    {
      return buildCall("InternalDecimalFromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalDecimalFromDatetime", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to DECIMAL from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDecimal(const IQLToLLVMValue * e, 
                                                               const FieldType * argType, 
                                                               const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDecimal);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDate(const IQLToLLVMValue * e, 
				     const FieldType * argType, 
				     llvm::Value * ret, 
				     const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDateFromVarchar", args, ret, retType);
    }
  case FieldType::DATE:
    b->CreateStore(e1, ret);
    return e->getValueType();
  default:
    throw std::runtime_error ((boost::format("Cast to DATE from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDate(const IQLToLLVMValue * e, 
							    const FieldType * argType, 
							    const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDate);
}

// TODO: Support all directions of casting.
IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDatetime(const IQLToLLVMValue * e, 
					 const FieldType * argType, 
					 llvm::Value * ret, 
					 const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDatetimeFromVarchar", args, ret, retType);
    }
  case FieldType::DATETIME:
    b->CreateStore(e1, ret);
    return e->getValueType();
  case FieldType::DATE:
    {
      return buildCall("InternalDatetimeFromDate", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to DATE from %1% not "
  					     "implemented.") % 
  			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDatetime(const IQLToLLVMValue * e, 
                                                                const FieldType * argType, 
                                                                const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDatetime);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastChar(const IQLToLLVMValue * e, 
				     const FieldType * argType, 
				     llvm::Value * ret, 
				     const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * e1 = e->getValue(this);
  // Must bitcast to match calling convention.
  llvm::Type * int8Ptr = llvm::PointerType::get(b->getInt8Ty(), 0);
  llvm::Value * ptr = b->CreateBitCast(ret, int8Ptr);

  if (argType->GetEnum() == FieldType::CHAR) {
    int32_t toCopy=argType->GetSize() < retType->GetSize() ? argType->GetSize() : retType->GetSize();
    int32_t toSet=retType->GetSize() - toCopy;
    llvm::Value * args[5];
    if (toCopy > 0) {
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
      // memcpy arg1 at offset 0
      args[0] = ptr;
      args[1] = b->CreateBitCast(e1, int8Ptr);
      args[2] = b->getInt64(toCopy);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    }
    if (toSet > 0) {
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
      // memset spaces at offset toCopy
      args[0] = b->CreateGEP(b->getInt8Ty(), ptr, b->getInt64(toCopy), "");;
      args[1] = b->getInt8(' ');
      args[2] = b->getInt64(toSet);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    }
    // Null terminate the string
    b->CreateStore(b->getInt8(0), b->CreateGEP(b->getInt8Ty(), ptr, b->getInt64(retType->GetSize()), ""));
  } else if (argType->GetEnum() == FieldType::VARCHAR) {
    llvm::Value * callArgs[3];
    llvm::Function * fn = LLVMModule->getFunction("InternalCharFromVarchar");
    callArgs[0] = e1;
    callArgs[1] = ptr;
    callArgs[2] = b->getInt32(retType->GetSize());
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "");
  } else {
    throw std::runtime_error("Only supporting CAST from VARCHAR to CHAR(N)");
  }
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildCastChar(const IQLToLLVMValue * e, 
							    const FieldType * argType, 
							    const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastChar);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastVarchar(const IQLToLLVMValue * e, 
					const FieldType * argType, 
					llvm::Value * ret, 
					const FieldType * retType)
{
  llvm::Value * e1 = e->getValue(this);
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT8:
    return buildCall("InternalVarcharFromInt8", args, ret, retType);
  case FieldType::INT16:
    return buildCall("InternalVarcharFromInt16", args, ret, retType);
  case FieldType::INT32:
    return buildCall("InternalVarcharFromInt32", args, ret, retType);
  case FieldType::INT64:
    return buildCall("InternalVarcharFromInt64", args, ret, retType);
  case FieldType::FLOAT:
    return buildCall("InternalVarcharFromFloat", args, ret, retType);
  case FieldType::DOUBLE:
    return buildCall("InternalVarcharFromDouble", args, ret, retType);
  case FieldType::CHAR:
    return buildCall("InternalVarcharFromChar", args, ret, retType);
  case FieldType::BIGDECIMAL:
    return buildCall("InternalVarcharFromDecimal", args, ret, retType);
  case FieldType::DATE:
    return buildCall("InternalVarcharFromDate", args, ret, retType);
  case FieldType::DATETIME:
    return buildCall("InternalVarcharFromDatetime", args, ret, retType);
  case FieldType::IPV4:
    return buildCall("InternalVarcharFromIPv4", args, ret, retType);
  case FieldType::CIDRV4:
    return buildCall("InternalVarcharFromCIDRv4", args, ret, retType);
  case FieldType::IPV6:
    return buildCall("InternalVarcharFromIPv6", args, ret, retType);
  case FieldType::CIDRV6:
    return buildCall("InternalVarcharFromCIDRv6", args, ret, retType);
  default:
    throw std::runtime_error ((boost::format("Cast to VARCHAR from %1% not "
  					     "implemented.") % 
  			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastVarchar(const IQLToLLVMValue * e, 
							       const FieldType * argType, 
							       const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastVarchar);
}

// Return pointer of element type pointing to beginning of fixed array data
llvm::Value * CodeGenerationContext::getFixedArrayData(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getFixedArrayData(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getFixedArrayData(llvm::Value * ptr, const FieldType * ty)
{
  BOOST_ASSERT(FieldType::FIXED_ARRAY == ty->GetEnum());
  const FixedArrayType * arrTy = dynamic_cast<const FixedArrayType*>(ty);
  llvm::IRBuilder<> * b = LLVMBuilder;
  ptr = b->CreateBitCast(ptr, b->getPtrTy());
  // GEP to get pointer to data and cast back to pointer to element
  return b->CreateBitCast(b->CreateGEP(b->getInt8Ty(), ptr, b->getInt64(arrTy->GetDataOffset()), ""),
                          llvm::PointerType::get(arrTy->getElementType()->LLVMGetType(this), 0));
}

// Return int8Ptr pointing to beginning of fixed array null bitmask
llvm::Value * CodeGenerationContext::getFixedArrayNull(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getFixedArrayNull(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getFixedArrayNull(llvm::Value * ptr, const FieldType * ty)
{
  BOOST_ASSERT(FieldType::FIXED_ARRAY == ty->GetEnum());
  const FixedArrayType * arrTy = dynamic_cast<const FixedArrayType*>(ty);
  BOOST_ASSERT(nullptr != arrTy);
  if(!arrTy->getElementType()->isNullable()) {
    return nullptr;
  }
  llvm::IRBuilder<> * b = LLVMBuilder;
  ptr = b->CreateBitCast(ptr, b->getPtrTy());
  // GEP to get pointer to offset
  return b->CreateGEP(b->getInt8Ty(), ptr, b->getInt64(arrTy->GetNullOffset()), "");
}

// Return pointer of element type pointing to beginning of variable array data
llvm::Value * CodeGenerationContext::getVariableArrayData(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getVariableArrayData(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getVariableArrayData(llvm::Value * ptr, const FieldType * ty)
{
  BOOST_ASSERT(FieldType::VARIABLE_ARRAY == ty->GetEnum());
  llvm::Type * llvmPtrToEltType = dynamic_cast<const SequentialType *>(ty)->getElementType()->LLVMGetType(this);
  BOOST_ASSERT(nullptr != llvmPtrToEltType);
  llvmPtrToEltType = llvm::PointerType::get(llvmPtrToEltType, 0);
  return buildVarArrayGetPtr(ptr, llvmPtrToEltType);
}

// Return int8Ptr pointing to beginning of variable array null bitmask
llvm::Value * CodeGenerationContext::getVariableArrayNull(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getVariableArrayNull(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getVariableArrayNull(llvm::Value * ptr, const FieldType * ty)
{
  BOOST_ASSERT(FieldType::VARIABLE_ARRAY == ty->GetEnum());
  const VariableArrayType * arrTy = dynamic_cast<const VariableArrayType*>(ty);
  BOOST_ASSERT(nullptr != arrTy);
  if(!arrTy->getElementType()->isNullable()) {
    return nullptr;
  }
  llvm::IRBuilder<> * b = LLVMBuilder;
  // GEP to get pointer to offset
  llvm::Value * offset = b->CreateMul(b->getInt32(arrTy->getElementType()->GetAllocSize()), buildVarArrayGetSize(ptr));
  llvm::Value * dataPtr = buildVarArrayGetPtr(ptr, b->getPtrTy());
  return b->CreateGEP(b->getInt8Ty(), dataPtr, b->CreateSExt(offset, b->getInt64Ty()), "");
}

llvm::Value * CodeGenerationContext::getArraySize(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getArraySize(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getArraySize(llvm::Value * ptr, const FieldType * ty)
{
  if (FieldType::FIXED_ARRAY == ty->GetEnum()) {
    return LLVMBuilder->getInt32(ty->GetSize());
  } else {
    return buildVarArrayGetSize(ptr);
  }
}

llvm::Value * CodeGenerationContext::getArrayData(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getArrayData(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getArrayData(llvm::Value * ptr, const FieldType * ty)
{
  if (FieldType::FIXED_ARRAY == ty->GetEnum()) {
    return getFixedArrayData(ptr, ty);
  } else {
    return getVariableArrayData(ptr, ty);
  }
}

llvm::Value * CodeGenerationContext::getArrayNull(const IQLToLLVMValue * e, const FieldType * ty)
{
  return getArrayNull(e->getValue(this), ty);
}

llvm::Value * CodeGenerationContext::getArrayNull(llvm::Value * ptr, const FieldType * ty)
{
  if (FieldType::FIXED_ARRAY == ty->GetEnum()) {
    return getFixedArrayNull(ptr, ty);
  } else {
    return getVariableArrayNull(ptr, ty);
  }
}

void CodeGenerationContext::buildArrayElementwiseCopy(const IQLToLLVMValue * e, 
                                                      const SequentialType * argType,
                                                      const IQLToLLVMValue * beginIdx,
                                                      const IQLToLLVMValue  * endIdx,
                                                      const IQLToLLVMValue * retBeginIdx, 
                                                      const IQLToLLVMValue * ret, 
                                                      const SequentialType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Types of elements are different so elementwise iteration and conversion is necessary
  const FieldType * argEltTy = argType->getElementType();
  const FieldType * retEltTy = retType->getElementType();
      
  // Loop over the array and call cast between element types
  // Constants one is used frequently
  const IQLToLLVMValue * one  = buildTrue();
  // INTEGER type used frequently in this method
  FieldType * int32Type = Int32Type::Get(argType->getContext());

  // DECLARE idx = beginIdx
  // Allocate and initialize counter
  llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
  const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
  buildSetNullableValue(counterLValue, beginIdx, int32Type, int32Type);
        
  // DECLARE retIdx = retBeginIdx
  // Allocate and initialize counter
  llvm::Value * retAllocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"retIdx");
  const IQLToLLVMValue * retCounter = IQLToLLVMRValue::get(this, retAllocAVal, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * retCounterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(retCounter, int32Type));
  buildSetNullableValue(retCounterLValue, retBeginIdx, int32Type, int32Type);
        
  whileBegin();

  // while (idx < endIdx)
  const IQLToLLVMValue * pred = buildCompare(buildRef(counter, int32Type), int32Type, endIdx, int32Type, int32Type, IQLToLLVMOpLT);
  whileStatementBlock(pred, int32Type);

  const IQLToLLVMValue * idx = buildRef(counter, int32Type);
  const IQLToLLVMValue * converted = buildCast(buildArrayRef(e, argType, idx, int32Type, argEltTy), argEltTy, retEltTy);
  const IQLToLLVMValue * retIdx = buildRef(retCounter, int32Type);
  const IQLToLLVMLValue * arrayLValue = buildArrayLValue(ret, retType, retIdx, int32Type, retEltTy);
  // Don't free lhs of elements since all freeing was done at the container level
  buildSetNullableValue(arrayLValue, converted, retEltTy, false, false);

  // SET idx = idx + 1
  buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
  // SET retIdx = retIdx + 1
  buildSetNullableValue(retCounterLValue, buildAdd(buildRef(retCounter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
  whileFinish();
}

void CodeGenerationContext::buildArrayCopyableCopy(const IQLToLLVMValue * e, 
                                                   const SequentialType * argType,
                                                   llvm::Value * ret, 
                                                   const SequentialType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // How many bytes can we copy? Minimum of array sizes.
  const FieldType * int32Type = Int32Type::Get(argType->getContext());
  const FieldType * int64Type = Int64Type::Get(argType->getContext());
  const IQLToLLVMValue * argSz = IQLToLLVMRValue::get(this, getArraySize(e, argType), IQLToLLVMValue::eLocal);
  const IQLToLLVMValue * retSz = IQLToLLVMRValue::get(this, getArraySize(ret, retType), IQLToLLVMValue::eLocal);
  std::vector<IQLToLLVMTypedValue> sizes = { IQLToLLVMTypedValue(argSz, int32Type), IQLToLLVMTypedValue(retSz, int32Type) };
  const IQLToLLVMValue * toCopy=buildLeastGreatest(sizes, int32Type, true);
  // Any remaining bytes in the target must have null bits set
  // toSet = retSz - toCopy
  const IQLToLLVMValue * toSet=buildSub(retSz, int32Type, toCopy, int32Type, int32Type);
  const IQLToLLVMValue * seven = buildDecimalInt32Literal("7");
  const IQLToLLVMValue * eight = buildDecimalInt32Literal("8");
  // nullBytesToCopy = (toCopy + 7)/8
  const IQLToLLVMValue * nullBytesToCopy = buildDiv(buildAdd(toCopy, int32Type, seven, int32Type, int32Type), int32Type, eight, int32Type, int32Type);
  llvm::Value * args[5];
  // IF (toCopy>0)
  const IQLToLLVMValue * pred = buildCompare(toCopy, int32Type, buildFalse(), int32Type, int32Type, IQLToLLVMOpGT);
  buildBeginIf();
  llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
  // memcpy data at offset 0
  args[0] = b->CreateBitCast(getArrayData(ret, retType), b->getPtrTy());
  args[1] = b->CreateBitCast(getArrayData(e, argType), b->getPtrTy());
  args[2] = buildMul(buildCast(toCopy, int32Type, int64Type), int64Type,
                     IQLToLLVMRValue::get(this, b->getInt32(retType->getElementType()->GetAllocSize()), IQLToLLVMValue::eLocal), int64Type,
                     int64Type)->getValue(this);
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
  // memcpy null bits at offset 0
  llvm::Value * targetNullBytes = getArrayNull(ret, retType);
  if (targetNullBytes != nullptr) {
    if(argType->getElementType()->isNullable()) {
      args[0] = targetNullBytes;
      args[1] = getArrayNull(e, argType);
      args[2] = buildCast(nullBytesToCopy, int32Type, int64Type)->getValue(this);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    } else {
      // Everything is non-null but there is no null bitmap in argument
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
      args[0] = getArrayNull(ret, retType);
      args[1] = b->getInt8(0xff);
      args[2] = buildCast(nullBytesToCopy, int32Type, int64Type)->getValue(this);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    }
  }
  buildEndIf(pred);


  if (retType->getElementType()->isNullable()) {
    // Make sure final null byte copied from argument to result is in good shape
    // bitsToKeep = toCopy - 8*(toCopy/8);
    const IQLToLLVMValue * bitsToKeep = buildSub(toCopy, int32Type, buildMul(eight, int32Type, buildDiv(toCopy, int32Type, eight, int32Type, int32Type), int32Type, int32Type), int32Type, int32Type);
    //IF (bitsToKeep > 0) {
    pred = buildCompare(bitsToKeep, int32Type, buildFalse(), int32Type, int32Type, IQLToLLVMOpGT);
    buildBeginIf();
    // There are bits in the last bytes copied that we need to make sure are cleared
    // bitsToKeepMask = (1U << bitsToKeep) - 1;
    llvm::Value * bitsToKeepMask = b->CreateTrunc(b->CreateSub(b->CreateShl(b->getInt32(1), bitsToKeep->getValue(this)), b->getInt32(1)), b->getInt8Ty());
    // lastNullByteOffset = nullBytesToCopy - 1
    llvm::Value * lastNullByteOffset = b->CreateSExt(b->CreateSub(nullBytesToCopy->getValue(this), b->getInt32(1)), b->getInt64Ty());
    llvm::Value * lastNullByteCopied = b->CreateGEP(b->getInt8Ty(), getFixedArrayNull(ret, retType), lastNullByteOffset, "");
    b->CreateStore(b->CreateAnd(b->CreateLoad(b->getInt8Ty(), lastNullByteCopied), bitsToKeepMask),
                   lastNullByteCopied);
    buildEndIf(pred);

    // If there are remaining full bytes worth of null bits in result to set to zero, do that here to make
    // the corresponding data values NULL
    // retNullSize = (retSz + 7)/8
    const IQLToLLVMValue * retNullSize = buildDiv(buildAdd(retSz, int32Type, seven, int32Type, int32Type), int32Type, eight, int32Type, int32Type);
    // IF (retNullSize > nullBytesToCopy) {
    pred = buildCompare(retNullSize, int32Type, nullBytesToCopy, int32Type, int32Type, IQLToLLVMOpGT);
    buildBeginIf();
    llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
    args[0] = b->CreateGEP(b->getInt8Ty(), getFixedArrayNull(ret, retType), buildCast(nullBytesToCopy, int32Type, int64Type)->getValue(this), "");
    args[1] = b->getInt8(0);
    args[2] = buildCast(buildSub(retNullSize, int32Type, nullBytesToCopy, int32Type, int32Type), int32Type, int64Type)->getValue(this);
    args[3] = b->getInt32(1);
    args[4] = b->getInt1(0);
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    buildEndIf(pred);
  }
}

llvm::Value * CodeGenerationContext::buildVariableArrayAllocate(const SequentialType * arrayType, llvm::Value * arrayLen, bool trackAllocation)
{
  llvm::Value * arr = buildEntryBlockAlloca(arrayType, "");
  buildVariableArrayAllocate(arr, arrayType, arrayLen, trackAllocation);
  return arr;
}

void CodeGenerationContext::buildVariableArrayAllocate(llvm::Value * e, const SequentialType * arrayType, llvm::Value * arrayLen, bool trackAllocation)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Allocate storage for the VARIABLE_ARRAY
  llvm::Value * toAlloc = b->CreateMul(arrayLen, b->getInt32(arrayType->getElementType()->GetAllocSize()));
  if (arrayType->getElementType()->isNullable()) {
    llvm::Value * nullBitmaskLength = b->CreateLShr(b->CreateAdd(arrayLen, b->getInt32(7)), 3);
    toAlloc = b->CreateAdd(toAlloc, nullBitmaskLength);
  }
  llvm::Value * callArgs[5];
  llvm::Function * fn = LLVMModule->getFunction("InternalVarcharAllocate");
  callArgs[0] = arrayLen;
  callArgs[1] = toAlloc;
  callArgs[2] = e;
  callArgs[3] = b->getInt32(trackAllocation ? 1 : 0);
  callArgs[4] = b->CreateLoad(LLVMDecContextPtrType, getContextArgumentRef());
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 5), "");
}

void CodeGenerationContext::buildEraseAllocationTracking(const IQLToLLVMValue * iqlVal, const FieldType * ft)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // If VARCHAR or VARARRAY must erase tracking 
  if (FieldType::VARCHAR == ft->GetEnum() || FieldType::VARIABLE_ARRAY == ft->GetEnum()) {
    llvm::Value * llvmVal = iqlVal->getValue(this);
    llvm::Value * callArgs[2];
    llvm::Function * fn = LLVMModule->getFunction("InternalVarcharErase");
    callArgs[0] = llvmVal;
    callArgs[1] = b->CreateLoad(LLVMDecContextPtrType, getContextArgumentRef());
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");
  }
  // If container type must recurse and erase members
  if (FieldType::FIXED_ARRAY == ft->GetEnum() || FieldType::VARIABLE_ARRAY == ft->GetEnum()) {
    const SequentialType * arrType = dynamic_cast<const SequentialType *>(ft);
    const FieldType * eltType = arrType->getElementType();
    if (FieldType::VARCHAR == eltType->GetEnum() || FieldType::VARIABLE_ARRAY == eltType->GetEnum() ||
        FieldType::FIXED_ARRAY == eltType->GetEnum() || FieldType::STRUCT == eltType->GetEnum()) {
      // Loop over the array and call erase
      // Constants one is used frequently
      const IQLToLLVMValue * one  = buildTrue();
      // INTEGER type used frequently in this method
      FieldType * int32Type = Int32Type::Get(ft->getContext());

      // DECLARE idx = 0
      // Allocate and initialize counter
      llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
      const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
      IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
      buildSetNullableValue(counterLValue, buildFalse(), int32Type, int32Type);

      const IQLToLLVMValue * endIdx = IQLToLLVMRValue::get(this, getArraySize(iqlVal, ft), IQLToLLVMValue::eLocal);
      
      whileBegin();
      
      // while (idx < endIdx)
      const IQLToLLVMValue * pred = buildCompare(buildRef(counter, int32Type), int32Type, endIdx, int32Type, int32Type, IQLToLLVMOpLT);
      whileStatementBlock(pred, int32Type);

      const IQLToLLVMValue * idx = buildRef(counter, int32Type);
      const IQLToLLVMValue * elt = buildArrayRef(iqlVal, ft, idx, int32Type, eltType);
      buildEraseAllocationTracking(elt, eltType);

      // SET idx = idx + 1
      buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
      whileFinish();      
    }
  } else if (FieldType::STRUCT == ft->GetEnum()) {
    const RecordType * structType = dynamic_cast<const RecordType *>(ft);
    for(std::size_t i=0; i<structType->getNumElements(); ++i) {
      const FieldType * eltType = structType->getElementType(i);
      if (FieldType::VARCHAR == eltType->GetEnum() || FieldType::VARIABLE_ARRAY == eltType->GetEnum() ||
          FieldType::FIXED_ARRAY == eltType->GetEnum() || FieldType::STRUCT == eltType->GetEnum()) {
        const IQLToLLVMValue * elt = buildRowRef(iqlVal, ft, i, eltType);
        buildEraseAllocationTracking(elt, eltType);
      }
    }
  }
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastFixedArray(const IQLToLLVMValue * e, 
                                           const FieldType * argType, 
                                           llvm::Value * ret, 
                                           const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;

  if (argType->GetEnum() != FieldType::FIXED_ARRAY) {
    throw std::runtime_error("Only supporting CAST from fixed array to fixed array");
  }
  const FixedArrayType * argArrTy = dynamic_cast<const FixedArrayType *>(argType);
  const FixedArrayType * retArrTy = dynamic_cast<const FixedArrayType *>(retType);
  int32_t toCopy=argType->GetSize() < retType->GetSize() ? argType->GetSize() : retType->GetSize();
  int32_t toSet=retType->GetSize() - toCopy;
  int32_t nullBytesToCopy = (toCopy+7)/8;
  llvm::Value * args[5];
  if (argArrTy->getElementType()->GetEnum() != FieldType::VARCHAR &&
      argArrTy->getElementType()->GetEnum() != FieldType::VARIABLE_ARRAY &&
      argArrTy->getElementType()->GetEnum() == retArrTy->getElementType()->GetEnum()) {
    // Just resizing a fixed array of copyable types
    if (toCopy > 0) {
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
      // memcpy data at offset 0
      args[0] = b->CreateBitCast(getArrayData(ret, retType), b->getPtrTy());
      args[1] = b->CreateBitCast(getArrayData(e, argType), b->getPtrTy());
      args[2] = b->getInt64(toCopy*retArrTy->getElementType()->GetAllocSize());
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
      // memcpy null bits at offset 0
      llvm::Value * targetNullBytes = getArrayNull(ret, retType);
      if (targetNullBytes != nullptr) {
        if(argArrTy->getElementType()->isNullable()) {
          args[0] = targetNullBytes;
          args[1] = getArrayNull(e, argType);
          args[2] = b->getInt64(nullBytesToCopy);
          args[3] = b->getInt32(1);
          args[4] = b->getInt1(0);
          b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
        } else {
          // Everything is non-null but there is no null bitmap in argument
          BOOST_ASSERT(nullBytesToCopy <= retArrTy->GetNullSize());
          llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
          args[0] = getArrayNull(ret, retType);
          args[1] = b->getInt8(0xff);
          args[2] = b->getInt64(nullBytesToCopy);
          args[3] = b->getInt32(1);
          args[4] = b->getInt1(0);
          b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
        }
      }
    }
  } else if (toCopy>0) {
    buildArrayElementwiseCopy(e, dynamic_cast<const SequentialType *>(argType),
                              buildFalse(), IQLToLLVMRValue::get(this, b->getInt32(toCopy), IQLToLLVMValue::eLocal),
                              buildFalse(), IQLToLLVMRValue::get(this, ret, IQLToLLVMValue::eLocal), dynamic_cast<const SequentialType *>(retType));
  }

  // Make sure final null byte copied from argument to result is in good shape
  uint8_t bitsToKeep = toCopy - 8*(toCopy/8);
  if (bitsToKeep > 0 && retArrTy->getElementType()->isNullable()) {
    // There are bits in the last bytes copied that we need to make sure are cleared
    uint8_t bitsToKeepMask = (1U << bitsToKeep) - 1;
    int32_t lastNullByteOffset = nullBytesToCopy - 1;
    llvm::Value * lastNullByteCopied = b->CreateGEP(b->getInt8Ty(), getFixedArrayNull(ret, retType), b->getInt64(lastNullByteOffset), "");
    b->CreateStore(b->CreateAnd(b->CreateLoad(b->getInt8Ty(), lastNullByteCopied),
                                b->getInt8(bitsToKeepMask)),
                   lastNullByteCopied);
  }

  // If there are remaining full bytes worth of null bits in result to set to zero, do that here to make
  // the corresponding data values NULL
  if (retArrTy->GetNullSize() > nullBytesToCopy) {
    BOOST_ASSERT(retArrTy->getElementType()->isNullable());
    llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
    args[0] = b->CreateGEP(b->getInt8Ty(), getFixedArrayNull(ret, retType), b->getInt64(nullBytesToCopy), "");
    args[1] = b->getInt8(0);
    args[2] = b->getInt64(retArrTy->GetNullSize() - nullBytesToCopy);
    args[3] = b->getInt32(1);
    args[4] = b->getInt1(0);
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
  }

  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildCastFixedArray(const IQLToLLVMValue * e, 
                                                                  const FieldType * argType, 
                                                                  const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastFixedArray);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastVariableArray(const IQLToLLVMValue * e, 
                                              const FieldType * argType, 
                                              llvm::Value * ret, 
                                              const FieldType * retType)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;

  switch(argType->GetEnum()) {
  case FieldType::FIXED_ARRAY:
    {
      // Allocate space for array values
      buildVariableArrayAllocate(ret, dynamic_cast<const SequentialType *>(retType), getArraySize(e, argType), true);
      // Copy values [0,getArraySize()) from source to target
      buildArrayElementwiseCopy(e, dynamic_cast<const SequentialType *>(argType),
                                buildFalse(), IQLToLLVMRValue::get(this, getArraySize(e, argType), IQLToLLVMValue::eLocal),
                                buildFalse(), IQLToLLVMRValue::get(this, ret, IQLToLLVMValue::eLocal), dynamic_cast<const SequentialType *>(retType));
      return IQLToLLVMValue::eLocal;      
    }
  default:
    throw std::runtime_error ((boost::format("Cast to VARIABLE_ARRAY from %1% not "
  					     "implemented.") % 
  			       argType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastVariableArray(const IQLToLLVMValue * e, 
                                                                     const FieldType * argType, 
                                                                     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastVariableArray);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastIPv4(const IQLToLLVMValue * e, 
                                     const FieldType * argType, 
                                     llvm::Value * ret, 
                                     const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;

  switch(argType->GetEnum()) {
  case FieldType::INT32:
    {
      auto in = e->getValue(this);
      auto out = b->CreateOr(b->CreateOr(b->CreateOr(b->CreateShl(b->CreateAnd(in, b->getInt32(0xff)), 24),
                                                     b->CreateShl(b->CreateAnd(in, b->getInt32(0xff00)), 8)),
                                         b->CreateLShr(b->CreateAnd(in, b->getInt32(0xff0000)), 8)),
                             b->CreateLShr(b->CreateAnd(in, b->getInt32(0xff000000)), 24));
      b->CreateStore(out, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::IPV4:
    b->CreateStore(e->getValue(this), ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::CIDRV4:
    {
      llvm::Type * structType = argType->LLVMGetType(this);
      // We need pointer type to access struct members giving prefix, so alloca and copy
      llvm::Value * cidrValue = e->getValue(this);
      llvm::Value * tmp = buildEntryBlockAlloca(structType, "cidrv4convert");
      b->CreateStore(cidrValue, tmp);
      cidrValue = tmp;
      llvm::Value * prefix = b->CreateLoad(retType->LLVMGetType(this), b->CreateStructGEP(structType, cidrValue, 0));
      b->CreateStore(prefix, ret);
      return IQLToLLVMValue::eLocal;      
    }
  case FieldType::IPV6:
  case FieldType::CIDRV6:
    {
      // Copy v4 address bytes from e[12]-e[15] (i.e. assume this is a v4 mapped address)
      ret = b->CreateBitCast(ret, b->getPtrTy(0));
      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(12);    
      llvm::Value * args[5];
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
      args[0] = ret;
      args[1] = b->CreateGEP(argType->LLVMGetType(this), e->getValue(this), llvm::ArrayRef(&gepIndexes[0], 2), "ipv6");;
      args[2] = b->getInt64(4);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FIXED_ARRAY:
    {
      b->CreateStore(b->getInt32(0), ret);
      int32_t bytesToCopy = std::min(argType->GetSize(), 4);
      const FieldType * eltType = dynamic_cast<const FixedArrayType*>(argType)->getElementType();
      const FieldType * int8FieldType = Int8Type::Get(argType->getContext(), eltType->isNullable());
      const FieldType * int32FieldType = Int32Type::Get(argType->getContext(), eltType->isNullable());
      const FieldType * int32FieldTypeNotNull = Int32Type::Get(argType->getContext(), false);
      for(int32_t i=0; i<bytesToCopy; ++i) {
        // Extract element, cast to int8_t and then promote up to int32_t.
        const IQLToLLVMValue * idx = IQLToLLVMRValue::get(this, b->getInt32(i), IQLToLLVMValue::eLocal);
        const IQLToLLVMValue * elt = buildArrayRef(e, argType, idx, int32FieldTypeNotNull, eltType);
        elt = buildCastInt32(buildCastInt8(elt, eltType, int8FieldType),
                             int8FieldType, int32FieldType);
        // Make any NULL elt equal to zero
        std::vector<IQLToLLVMTypedValue> ifNullArgs;
        ifNullArgs.emplace_back(elt, int32FieldType);
        ifNullArgs.emplace_back(IQLToLLVMRValue::get(this, b->getInt32(0), IQLToLLVMValue::eLocal), int32FieldTypeNotNull);
        elt = buildIsNullFunction(ifNullArgs, int32FieldTypeNotNull);
        // Now shift byte into position and OR into the result
        llvm::Value * tmp = b->CreateOr(b->CreateShl(elt->getValue(this), 8*i), b->CreateLoad(b->getInt32Ty(), ret));
        b->CreateStore(tmp, ret);
      }
      return IQLToLLVMValue::eLocal;
    }
  default:
    throw std::runtime_error ((boost::format("Cast to IPV4 from %1% not "
  					     "implemented.") % 
  			       argType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastIPv4(const IQLToLLVMValue * e, 
                                                            const FieldType * argType, 
                                                            const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastIPv4);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastCIDRv4(const IQLToLLVMValue * e, 
                                       const FieldType * argType, 
                                       llvm::Value * ret, 
                                       const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;

  switch(argType->GetEnum()) {
  case FieldType::IPV4:
    {
      const FieldType * prefixLengthTy = Int8Type::Get(retType->getContext());
      const IQLToLLVMValue * prefixLength = IQLToLLVMRValue::get(this, b->getInt8(32), IQLToLLVMValue::eLocal);
      return buildDiv(e, argType, prefixLength, prefixLengthTy, ret, retType); 
    }
  case FieldType::CIDRV4:
    b->CreateStore(e->getValue(this), ret);
    return IQLToLLVMValue::eLocal;
  default:
    throw std::runtime_error ((boost::format("Cast to CIDRV4 from %1% not "
  					     "implemented.") % 
  			       argType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastCIDRv4(const IQLToLLVMValue * e, 
                                                              const FieldType * argType, 
                                                              const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastCIDRv4);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastIPv6(const IQLToLLVMValue * e, 
                                     const FieldType * argType, 
                                     llvm::Value * ret, 
                                     const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;

  switch(argType->GetEnum()) {
  case FieldType::IPV4:
    {
      // First setup 12 byte prefix used for V4 mapped IPV6 addresses
      llvm::ArrayType * arrayTy = llvm::ArrayType::get(llvm::Type::getInt8Ty(*LLVMContext), 12);
      // This is the global variable itself
      llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*LLVMModule, 
                                                                  arrayTy,
                                                                  false, llvm::GlobalValue::ExternalLinkage, 0, "v4mappedprefix");
      // The value to initialize the global
      llvm::Constant * constArrayMembers[12];
      for(int i=0 ; i<10; i++) {
        constArrayMembers[i] = llvm::ConstantInt::get(b->getInt8Ty(), 0, true);
      }
      for(int i=10 ; i<12; i++) {
        constArrayMembers[i] = llvm::ConstantInt::get(b->getInt8Ty(), 0xFF, true);
      }
      llvm::Constant * globalVal = llvm::ConstantArray::get(arrayTy, llvm::ArrayRef(&constArrayMembers[0], 12));
      globalVar->setInitializer(globalVal);

      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(0);    
      // memcpy v4mappedprefix to ret
      llvm::Value * args[5];
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
      args[0] = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "ipv6");
      args[1] = b->CreateGEP(arrayTy, globalVar, llvm::ArrayRef(&gepIndexes[0], 2), "v4mapped");;
      args[2] = b->getInt64(12);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");

      // Copy v4 address bytes to ret[12]-ret[15]
      llvm::Value * v4Addr = buildEntryBlockAlloca(argType->LLVMGetType(this), "tmpv4");
      b->CreateStore(e->getValue(this), v4Addr);
      v4Addr = b->CreateBitCast(v4Addr, b->getPtrTy(0));
      gepIndexes[1] = b->getInt64(12);    
      args[0] = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "ipv6");
      args[1] = v4Addr;
      args[2] = b->getInt64(4);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");

      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CIDRV4:
    {
      const IPv4Type * ipv4Ty = IPv4Type::Get(argType->getContext(), false);
      const IQLToLLVMValue * prefix = buildCastIPv4(e, argType, ipv4Ty);
      return buildCastIPv6(prefix, ipv4Ty, ret, retType);
    }
  case FieldType::IPV6:
    b->CreateStore(e->getValue(this), ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::CIDRV6:
    {
      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(0);    
      // memcpy v4mappedprefix to ret
      llvm::Value * args[5];
      llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
      args[0] = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "ipv6");
      args[1] = b->CreateGEP(argType->LLVMGetType(this), e->getValue(this), llvm::ArrayRef(&gepIndexes[0], 2), "v6cidr");;
      args[2] = b->getInt64(16);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::FIXED_ARRAY:
    {
      int32_t bytesToCopy = std::min(argType->GetSize(), 16);
      const FieldType * eltType = dynamic_cast<const FixedArrayType*>(argType)->getElementType();
      const FieldType * int8FieldType = Int8Type::Get(argType->getContext(), eltType->isNullable());
      const FieldType * int8FieldTypeNotNull = Int8Type::Get(argType->getContext(), false);
      const FieldType * int32FieldTypeNotNull = Int32Type::Get(argType->getContext(), false);
      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(0);    
      for(int32_t i=0; i<bytesToCopy; ++i) {
        // Extract element, cast to int8_t 
        const IQLToLLVMValue * idx = IQLToLLVMRValue::get(this, b->getInt32(i), IQLToLLVMValue::eLocal);
        const IQLToLLVMValue * elt = buildArrayRef(e, argType, idx, int32FieldTypeNotNull, eltType);
        elt = buildCastInt8(elt, eltType, int8FieldType);
        // Make any NULL elt equal to zero
        std::vector<IQLToLLVMTypedValue> ifNullArgs;
        ifNullArgs.emplace_back(elt, int8FieldType);
        ifNullArgs.emplace_back(IQLToLLVMRValue::get(this, b->getInt8(0), IQLToLLVMValue::eLocal), int8FieldTypeNotNull);
        elt = buildIsNullFunction(ifNullArgs, int8FieldTypeNotNull);
        // Store value in array position
        gepIndexes[1] = b->getInt64(i);            
        llvm::Value * tgt =  b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "ipv6");
        b->CreateStore(elt->getValue(this), tgt);
      }
      if (bytesToCopy < 16) {
        llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
        llvm::Value * gepIndexes[2];
        gepIndexes[0] = b->getInt64(0);    
        gepIndexes[1] = b->getInt64(bytesToCopy);
        llvm::Value * args[5];
        args[0] = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "ipv6");
        args[1] = b->getInt8(0);
        args[2] = b->getInt64(16 - bytesToCopy);
        args[3] = b->getInt32(1);
        args[4] = b->getInt1(0);
        b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
      }
      return IQLToLLVMValue::eLocal;
    }
  default:
    throw std::runtime_error ((boost::format("Cast to IPV6 from %1% not "
  					     "implemented.") % 
  			       argType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastIPv6(const IQLToLLVMValue * e, 
                                                            const FieldType * argType, 
                                                            const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastIPv6);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastCIDRv6(const IQLToLLVMValue * e, 
                                       const FieldType * argType, 
                                       llvm::Value * ret, 
                                       const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;

  switch(argType->GetEnum()) {
  case FieldType::IPV4:
    {
      const IPv4Type * ipv4Ty = IPv4Type::Get(argType->getContext(), false);
      const IPv6Type * ipv6Ty = IPv6Type::Get(argType->getContext(), false);
      llvm::Type * llvmIpv6Ty = llvm::PointerType::get(ipv6Ty->LLVMGetType(this), 0);
      // Cast prefix from v4 to v6
      buildCastIPv6(e, ipv4Ty, b->CreateBitCast(ret, llvmIpv6Ty), ipv6Ty);
      // Set prefix length in the result
      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(16);
      llvm::Value * prefix_length_ptr  = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "prefix_length");
      b->CreateStore(b->getInt8(128), prefix_length_ptr);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::IPV6:
    {
      const FieldType * prefixLengthTy = Int8Type::Get(retType->getContext());
      const IQLToLLVMValue * prefixLength = IQLToLLVMRValue::get(this, b->getInt8(128), IQLToLLVMValue::eLocal);
      return buildDiv(e, argType, prefixLength, prefixLengthTy, ret, retType); 
    }
  case FieldType::CIDRV4:
    {
      const IPv4Type * ipv4Ty = IPv4Type::Get(argType->getContext(), false);
      const IPv6Type * ipv6Ty = IPv6Type::Get(argType->getContext(), false);
      llvm::Type * llvmIpv6Ty = llvm::PointerType::get(ipv6Ty->LLVMGetType(this), 0);
      // Cast prefix from v4 to v6
      const IQLToLLVMValue * prefix = buildCastIPv4(e, argType, ipv4Ty);
      buildCastIPv6(prefix, ipv4Ty, b->CreateBitCast(ret, llvmIpv6Ty), ipv6Ty);
      // prefix length of cidr + 96, to access prefix length, alloca and copy so we can GEP
      llvm::Value * prefix_length = buildEntryBlockAlloca(argType->LLVMGetType(this), "tmpcidrv4");
      b->CreateStore(e->getValue(this), prefix_length);
      prefix_length = b->CreateLoad(b->getInt8Ty(), b->CreateStructGEP(argType->LLVMGetType(this), prefix_length, 1));
      prefix_length = b->CreateAdd(prefix_length, b->getInt8(96));
      // Set prefix length in the result
      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(16);
      llvm::Value * prefix_length_ptr  = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "prefix_length");
      b->CreateStore(prefix_length, prefix_length_ptr);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CIDRV6:
    b->CreateStore(e->getValue(this), ret);
    return IQLToLLVMValue::eLocal;
  default:
    throw std::runtime_error ((boost::format("Cast to CIDRV6 from %1% not "
  					     "implemented.") % 
  			       argType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastCIDRv6(const IQLToLLVMValue * e, 
                                                              const FieldType * argType, 
                                                              const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastCIDRv6);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildCast(const IQLToLLVMValue * e, 
							   const FieldType * argType, 
							   llvm::Value * ret, 
							   const FieldType * retType)
{
  switch(retType->GetEnum()) {
  case FieldType::INT8:
    return buildCastInt8(e, argType, ret, retType);
  case FieldType::INT16:
    return buildCastInt16(e, argType, ret, retType);
  case FieldType::INT32:
    return buildCastInt32(e, argType, ret, retType);
  case FieldType::INT64:
    return buildCastInt64(e, argType, ret, retType);
  case FieldType::CHAR:
    return buildCastChar(e, argType, ret, retType);
  case FieldType::VARCHAR:
    return buildCastVarchar(e, argType, ret, retType);
  case FieldType::BIGDECIMAL:
    return buildCastDecimal(e, argType, ret, retType);
  case FieldType::FLOAT:
    return buildCastFloat(e, argType, ret, retType);
  case FieldType::DOUBLE:
    return buildCastDouble(e, argType, ret, retType);
  case FieldType::DATETIME:
    return buildCastDatetime(e, argType, ret, retType);
  case FieldType::DATE:
    return buildCastDate(e, argType, ret, retType);
  case FieldType::FIXED_ARRAY:
    return buildCastFixedArray(e, argType, ret, retType);
  case FieldType::VARIABLE_ARRAY:
    return buildCastVariableArray(e, argType, ret, retType);
  case FieldType::IPV4:
    return buildCastIPv4(e, argType, ret, retType);
  case FieldType::CIDRV4:
    return buildCastCIDRv4(e, argType, ret, retType);
  case FieldType::IPV6:
    return buildCastIPv6(e, argType, ret, retType);
  case FieldType::CIDRV6:
    return buildCastCIDRv6(e, argType, ret, retType);
  default:
    // Programming error; this should have been caught during type check.
    throw std::runtime_error("Invalid type cast");    
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCast(const IQLToLLVMValue * e, 
							const FieldType * argType, 
							const FieldType * retType)
{
  return argType->GetEnum() == retType->GetEnum() && argType == retType->clone(argType->isNullable()) ? e :
    buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCast);
}

const IQLToLLVMValue * CodeGenerationContext::buildCastNonNullable(const IQLToLLVMValue * e, 
								   const FieldType * argType, 
								   const FieldType * retType)
{
  return argType->GetEnum() == retType->GetEnum() && argType == retType->clone(argType->isNullable()) ? e :
    buildNonNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCast);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildAdd(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  if ((lhsType != NULL && lhsType->GetEnum() == FieldType::INTERVAL) ||
      (rhsType != NULL && rhsType->GetEnum() == FieldType::INTERVAL)) {
    // Special case handling of datetime/interval addition.
    return buildDateAdd(lhs, lhsType, rhs, rhsType, ret, retType);
  }
  if ((lhsType != NULL && lhsType->GetEnum() == FieldType::CHAR)) {
    return buildCharAdd(lhs, lhsType, rhs, rhsType, ret, retType);
  }

  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);

  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateAdd(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->isFloatingPoint()) {
    llvm::Value * r = b->CreateFAdd(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalDecimalAdd", args, ret, retType);
  } else if (retType->GetEnum() == FieldType::VARCHAR) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalVarcharAdd", args, ret, retType);
  } else {
    throw std::runtime_error("INTERNAL ERROR: Invalid Type");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildAdd(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildAdd);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildSub(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  if (lhsType->GetEnum() == FieldType::DATE ||
      lhsType->GetEnum() == FieldType::DATETIME) {
    // Negate the interval value (which is integral)
    // then call add
    llvm::Value * neg = 
      b->CreateNeg(rhs->getValue(this));
    rhs = IQLToLLVMRValue::get(this, neg, IQLToLLVMValue::eLocal);
    return buildDateAdd(lhs, lhsType, rhs, rhsType, ret, retType);
  } else {  
    lhs = buildCastNonNullable(lhs, lhsType, retType);
    rhs = buildCastNonNullable(rhs, rhsType, retType);
    llvm::Value * e1 = lhs->getValue(this);
    llvm::Value * e2 = rhs->getValue(this);
    if (retType->isIntegral()) {
      b->CreateStore(b->CreateSub(e1, e2), ret);
      return IQLToLLVMValue::eLocal;
    } else if (retType->isFloatingPoint()) {
      b->CreateStore(b->CreateFSub(e1, e2), ret);
      return IQLToLLVMValue::eLocal;
    } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
      std::vector<IQLToLLVMTypedValue> args;
      args.emplace_back(lhs, lhsType);
      args.emplace_back(rhs, rhsType);
      return buildCall("InternalDecimalSub", args, ret, retType);
    } else {
      throw std::runtime_error("INTERNAL ERROR: unexpected type in subtract");
    }
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildSub(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildSub);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildMul(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);
  if (retType->isIntegral()) {
    b->CreateStore(b->CreateMul(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->isFloatingPoint()) {
    b->CreateStore(b->CreateFMul(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalDecimalMul", args, ret, retType);
  } else {
    throw std::runtime_error("INTERNAL ERROR: unexpected type in multiply");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildMul(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildMul);
}

void CodeGenerationContext::zeroHostBits(const IQLToLLVMValue * prefixLength,
                                         llvm::Value * ret, 
                                         const FieldType * retType)

{
  llvm::IRBuilder<> * b = LLVMBuilder;
  auto retBytes = FieldType::CIDRV6 == retType->GetEnum() ? 16 : 4;

  llvm::Value * gepIndexes[2];
  auto bytesToKeepPtr = b->CreateAlloca(b->getInt8Ty());
  b->CreateStore(b->CreateUDiv(prefixLength->getValue(this), b->getInt8(8)), bytesToKeepPtr);
  auto bitsInBytesToKeep = b->CreateMul(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt8(8));
  auto pred = b->CreateICmpNE(prefixLength->getValue(this), bitsInBytesToKeep);
  pred = b->CreateZExt(pred, b->getInt32Ty());
  buildBeginIf();
  llvm::Value * lastByteToKeepPtr = nullptr;
  if (FieldType::CIDRV6 == retType->GetEnum()) {
    gepIndexes[0] = b->getInt64(0);    
    gepIndexes[1] = b->CreateSExt(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt64Ty());
    lastByteToKeepPtr = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "last_byte");
  } else {
    lastByteToKeepPtr = b->CreateBitCast(b->CreateStructGEP(retType->LLVMGetType(this), ret, 0), b->getPtrTy());
    lastByteToKeepPtr = b->CreateInBoundsGEP(b->getInt8Ty(), lastByteToKeepPtr, b->CreateSExt(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt64Ty()));
  }
  auto bitsToKeep = b->CreateSub(prefixLength->getValue(this), bitsInBytesToKeep);
  b->CreateStore(b->CreateAnd(b->CreateLoad(b->getInt8Ty(), lastByteToKeepPtr), b->CreateShl(b->getInt8(-1), b->CreateSub(b->getInt8(8), bitsToKeep))),
                 lastByteToKeepPtr);
  b->CreateStore(b->CreateAdd(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt8(1)), bytesToKeepPtr);
  buildEndIf(IQLToLLVMRValue::get(this, pred, IQLToLLVMValue::eLocal));

  pred = b->CreateICmpULT(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt8(retBytes));
  pred = b->CreateZExt(pred, b->getInt32Ty());
  llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
  llvm::Value * args[5];
  buildBeginIf();
  if (FieldType::CIDRV6 == retType->GetEnum()) {
    gepIndexes[0] = b->getInt64(0);    
    gepIndexes[1] = b->CreateSExt(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt64Ty());
    args[0] = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "cidr_prefix");
  } else {
    args[0] = b->CreateBitCast(b->CreateStructGEP(retType->LLVMGetType(this), ret, 0), b->getPtrTy());
    args[0] = b->CreateInBoundsGEP(b->getInt8Ty(), args[0], b->CreateSExt(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt64Ty()));
  }
  args[1] = b->getInt8(0);
  args[2] = b->CreateSub(b->getInt64(retBytes), b->CreateSExt(b->CreateLoad(b->getInt8Ty(), bytesToKeepPtr), b->getInt64Ty()));
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
  buildEndIf(IQLToLLVMRValue::get(this, pred, IQLToLLVMValue::eLocal));
}

void CodeGenerationContext::CreateMemcpyIntrinsic()
{
  llvm::Module * mod = this->LLVMModule;

  llvm::PointerType* PointerTy_3 = llvm::PointerType::get(llvm::IntegerType::get(mod->getContext(), 8), 0);
  
  std::vector<llvm::Type*>FuncTy_7_args;
  FuncTy_7_args.push_back(PointerTy_3);
  FuncTy_7_args.push_back(PointerTy_3);
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 64));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 32));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 1));
  llvm::FunctionType* FuncTy_7 = llvm::FunctionType::get(
							 /*Result=*/llvm::Type::getVoidTy(mod->getContext()),
							 /*Params=*/FuncTy_7_args,
							 /*isVarArg=*/false);

  llvm::Function* func_llvm_memcpy_i64 = llvm::Function::Create(
								/*Type=*/FuncTy_7,
								/*Linkage=*/llvm::GlobalValue::ExternalLinkage,
								/*Name=*/"llvm.memcpy.p0i8.p0i8.i64", mod); // (external, no body)
  func_llvm_memcpy_i64->setCallingConv(llvm::CallingConv::C);
  func_llvm_memcpy_i64->setDoesNotThrow();

  this->LLVMMemcpyIntrinsic = func_llvm_memcpy_i64;
}

void CodeGenerationContext::CreateMemsetIntrinsic()
{
  llvm::Module * mod = this->LLVMModule;

  llvm::PointerType* PointerTy_3 = llvm::PointerType::get(llvm::IntegerType::get(mod->getContext(), 8), 0);
  
  std::vector<llvm::Type*>FuncTy_7_args;
  FuncTy_7_args.push_back(PointerTy_3);
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 8));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 64));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 32));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 1));
  llvm::FunctionType* FuncTy_7 = llvm::FunctionType::get(
    /*Result=*/llvm::Type::getVoidTy(mod->getContext()),
    /*Params=*/FuncTy_7_args,
    /*isVarArg=*/false);

  llvm::Function* func_llvm_memset_i64 = llvm::Function::Create(
    /*Type=*/FuncTy_7,
    /*Linkage=*/llvm::GlobalValue::ExternalLinkage,
    /*Name=*/"llvm.memset.p0i8.i64", mod); // (external, no body)
  func_llvm_memset_i64->setCallingConv(llvm::CallingConv::C);
  func_llvm_memset_i64->setDoesNotThrow();

  this->LLVMMemsetIntrinsic = func_llvm_memset_i64;
}

void CodeGenerationContext::CreateMemcmpIntrinsic()
{
  llvm::Module * mod = this->LLVMModule;

  llvm::PointerType* PointerTy_0 = llvm::PointerType::get(llvm::IntegerType::get(mod->getContext(), 8), 0);

  std::vector<llvm::Type*>FuncTy_12_args;
  FuncTy_12_args.push_back(PointerTy_0);
  FuncTy_12_args.push_back(PointerTy_0);
  FuncTy_12_args.push_back(llvm::IntegerType::get(mod->getContext(), 64));
  llvm::FunctionType* FuncTy_12 = llvm::FunctionType::get(
							  /*Result=*/llvm::IntegerType::get(mod->getContext(), 32),
							  /*Params=*/FuncTy_12_args,
							  /*isVarArg=*/false);

  llvm::Function* func_memcmp = llvm::Function::Create(
    /*Type=*/FuncTy_12,
    /*Linkage=*/llvm::GlobalValue::ExternalLinkage,
    /*Name=*/"memcmp", mod); // (external, no body)
  func_memcmp->setCallingConv(llvm::CallingConv::C);
  func_memcmp->setDoesNotThrow();

  this->LLVMMemcmpIntrinsic = func_memcmp;
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildDiv(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;

  // First check for the case of building a CIDR from a prefix and length
  if (lhsType->GetEnum() == FieldType::IPV4) {
    // Store the prefix as 32 bit int
    b->CreateStore(lhs->getValue(this), b->CreateStructGEP(retType->LLVMGetType(this), ret, 0));
    // Cast prefix length to int8 and set in second member of struct
    rhs = buildCastNonNullable(rhs, rhsType, Int8Type::Get(rhsType->getContext()));
    b->CreateStore(rhs->getValue(this), b->CreateStructGEP(retType->LLVMGetType(this), ret, 1));

    zeroHostBits(rhs, ret, retType);

    return IQLToLLVMValue::eLocal;
  } else if (lhsType->GetEnum() == FieldType::IPV6) {
    llvm::Value * gepIndexes[2];
    gepIndexes[0] = b->getInt64(0);    
    gepIndexes[1] = b->getInt64(0);    
    // memcpy prefix
    llvm::Value * args[5];
    llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
    // args[0] = b->CreateGEP(b->getInt8Ty(), ret, b->getInt64(0), "cidr_prefix");
    args[0] = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "cidr_prefix");
    args[1] = b->CreateGEP(lhsType->LLVMGetType(this), lhs->getValue(this), llvm::ArrayRef(&gepIndexes[0], 2), "prefix");;
    args[2] = b->getInt64(16);
    args[3] = b->getInt32(1);
    args[4] = b->getInt1(0);
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    // Cast prefix length to int8 and set in last position of array
    rhs = buildCastNonNullable(rhs, rhsType, Int8Type::Get(rhsType->getContext()));
    gepIndexes[0] = b->getInt64(0);    
    gepIndexes[1] = b->getInt64(16);
    llvm::Value * length_ptr  = b->CreateGEP(retType->LLVMGetType(this), ret, llvm::ArrayRef(&gepIndexes[0], 2), "prefix_length");
    b->CreateStore(rhs->getValue(this), length_ptr);

    zeroHostBits(rhs, ret, retType);

    return IQLToLLVMValue::eLocal;
  }
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);
  if (retType->isIntegral()) {
    b->CreateStore(b->CreateSDiv(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->isFloatingPoint()) {
    b->CreateStore(b->CreateFDiv(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalDecimalDiv", args, ret, retType);
  } else {
    throw std::runtime_error("INTERNAL ERROR: unexpected type in multiply");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildDiv(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildDiv);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildMod(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);
  if (retType->isIntegral()) {
    b->CreateStore(b->CreateSRem(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("INTERNAL ERROR: unexpected type in modulus");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildMod(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildMod);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildNegate(const IQLToLLVMValue * lhs,
				   const FieldType * lhsTy,
				   llvm::Value * ret,
				   const FieldType * retTy)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * e1 = lhs->getValue(this);
  if (retTy->isIntegral()) {
    llvm::Value * r = b->CreateNeg(e1);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retTy->isFloatingPoint()) {
    llvm::Value * r = b->CreateFNeg(e1);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsTy);
    return buildCall("InternalDecimalNeg", args, ret, retTy);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildNegate(const IQLToLLVMValue * e, 
							  const FieldType * argType, 
							  const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildNegate);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildDateAdd(const IQLToLLVMValue * lhs, 
				    const FieldType * lhsType, 
				    const IQLToLLVMValue * rhs, 
				    const FieldType * rhsType, 
				    llvm::Value * retVal, 
				    const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  if (lhsType != NULL && lhsType->GetEnum() == FieldType::INTERVAL) {
    std::swap(lhs, rhs);
    std::swap(lhsType, rhsType);
  }
  const IntervalType * intervalType = dynamic_cast<const IntervalType *>(rhsType);
  IntervalType::IntervalUnit unit = intervalType->getIntervalUnit();
  llvm::Value * callArgs[2];
  static const char * types [] = {"datetime", "date"};
  const char * ty = 
    lhsType->GetEnum() == FieldType::DATETIME ? types[0] : types[1];
  std::string 
    fnName((boost::format(
			  unit == IntervalType::DAY ? "%1%_add_day" : 
			  unit == IntervalType::HOUR ? "%1%_add_hour" :
			  unit == IntervalType::MINUTE ? "%1%_add_minute" :
			  unit == IntervalType::MONTH ? "%1%_add_month" :
			  unit == IntervalType::SECOND ? "%1%_add_second" :
			  "%1%_add_year") % ty).str());
  llvm::Function * fn = LLVMModule->getFunction(fnName.c_str());
  callArgs[0] = lhs->getValue(this);
  callArgs[1] = rhs->getValue(this);
  llvm::Value * ret = b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 2), "");
  b->CreateStore(ret, retVal);
  return IQLToLLVMValue::eLocal;  
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCharAdd(const IQLToLLVMValue * lhs, 
				    const FieldType * lhsType, 
				    const IQLToLLVMValue * rhs, 
				    const FieldType * rhsType, 
				    llvm::Value * ret, 
				    const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);

  llvm::Type * int8Ptr = b->getPtrTy(0);
  unsigned lhsSz = lhsType->GetSize();
  unsigned rhsSz = rhsType->GetSize();
  // Allocate target storage and
  // memcpy the two char variables to the target at appropriate offsets.
  // Allocate storage for return value.  Bit cast for argument to memcpy
  llvm::Value * retPtrVal = b->CreateBitCast(ret, int8Ptr);
    
  // Get a pointer to int8_t for args
  llvm::Value * tmp1 = b->CreateBitCast(e1, int8Ptr);
  llvm::Value * tmp2 = b->CreateBitCast(e2, int8Ptr);

  llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
  llvm::Value * args[5];
  // memcpy arg1 at offset 0
  args[0] = retPtrVal;
  args[1] = tmp1;
  args[2] = b->getInt64(lhsSz);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
  // memcpy arg2 at offset lhsSz (be sure to copy the trailing 0 to null terminate).
  args[0] = b->CreateGEP(b->getInt8Ty(), retPtrVal, b->getInt64(lhsSz), "");
  args[1] = tmp2;
  args[2] = b->getInt64(rhsSz+1);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
    
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseAnd(const IQLToLLVMValue * lhs, 
				       const FieldType * lhsType, 
				       const IQLToLLVMValue * rhs, 
				       const FieldType * rhsType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);
  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateAnd(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("bitwise operations only supported for integer values");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseAnd(const IQLToLLVMValue * lhs, 
							      const FieldType * lhsType, 
							      const IQLToLLVMValue * rhs, 
							      const FieldType * rhsType, 
							      const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildBitwiseAnd);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseOr(const IQLToLLVMValue * lhs, 
				      const FieldType * lhsType, 
				      const IQLToLLVMValue * rhs, 
				      const FieldType * rhsType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);
  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateOr(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("bitwise operations only supported for integer values");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseOr(const IQLToLLVMValue * lhs, 
							     const FieldType * lhsType, 
							     const IQLToLLVMValue * rhs, 
							     const FieldType * rhsType, 
							     const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildBitwiseOr);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseXor(const IQLToLLVMValue * lhs, 
				       const FieldType * lhsType, 
				       const IQLToLLVMValue * rhs, 
				       const FieldType * rhsType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);
  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateXor(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("bitwise operations only supported for integer values");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseXor(const IQLToLLVMValue * lhs, 
							      const FieldType * lhsType, 
							      const IQLToLLVMValue * rhs, 
							      const FieldType * rhsType, 
							      const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildBitwiseXor);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseNot(const IQLToLLVMValue * lhs,
                                       const FieldType * lhsTy,
                                       llvm::Value * ret,
                                       const FieldType * retTy)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * r = b->CreateNot(e1);
  b->CreateStore(r, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseNot(const IQLToLLVMValue * e, 
                                                              const FieldType * argType, 
                                                              const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildBitwiseNot);
}

llvm::Value * 
CodeGenerationContext::buildVarArrayIsSmall(llvm::Value * varcharPtr)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Access first bit of the structure to see if large or small.
  llvm::Value * firstByte = 
    b->CreateLoad(b->getInt8Ty(), b->CreateBitCast(varcharPtr, b->getPtrTy()));
  return 
    b->CreateICmpEQ(b->CreateAnd(b->getInt8(1U),
				 firstByte),
		    b->getInt8(0U));
}

llvm::Value * 
CodeGenerationContext::buildVarArrayGetSize(llvm::Value * varcharPtr)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * f = b->GetInsertBlock()->getParent();

  llvm::Value * ret = b->CreateAlloca(b->getInt32Ty());
  
  llvm::BasicBlock * smallBB = llvm::BasicBlock::Create(*c, "small", f);
  llvm::BasicBlock * largeBB = llvm::BasicBlock::Create(*c, "large", f);
  llvm::BasicBlock * contBB = llvm::BasicBlock::Create(*c, "cont", f);
  
  b->CreateCondBr(buildVarArrayIsSmall(varcharPtr), smallBB, largeBB);
  b->SetInsertPoint(smallBB);
  llvm::Value * firstByte = 
    b->CreateLoad(b->getInt8Ty(), b->CreateBitCast(varcharPtr, b->getPtrTy()));
  b->CreateStore(b->CreateSExt(b->CreateAShr(b->CreateAnd(b->getInt8(0xfe),
							  firstByte),  
					     b->getInt8(1U)),
			       b->getInt32Ty()),
		 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(largeBB);
  llvm::Value * firstDWord = b->CreateLoad(b->getInt32Ty(),
                                           b->CreateStructGEP(LLVMVarcharType, varcharPtr, 0));
  b->CreateStore(b->CreateAShr(b->CreateAnd(b->getInt32(0xfffffffe),
					    firstDWord),  
			       b->getInt32(1U)),
		 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(contBB);
  return b->CreateLoad(b->getInt32Ty(), ret);
}

llvm::Value * 
CodeGenerationContext::buildVarcharGetSize(llvm::Value * varcharPtr)
{
  return buildVarArrayGetSize(varcharPtr);
}

llvm::Value * 
CodeGenerationContext::buildVarArrayGetPtr(llvm::Value * varcharPtr, llvm::Type * retTy)
{
  // TODO: Only handling "Large" model vararrays right now.   Should we also support small model
  // (e.g. it would optimize TINYINT[] as we do with VARCHAR).
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  return b->CreateBitCast(b->CreateLoad(b->getPtrTy(),
                                        b->CreateStructGEP(LLVMVarcharType, varcharPtr, 2)),
                          retTy);
}

llvm::Value * 
CodeGenerationContext::buildVarcharGetPtr(llvm::Value * varcharPtr)
{
  // return buildVarArrayGetPtr(varcharPtr, LLVMBuilder->getPtrTy());
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * f = b->GetInsertBlock()->getParent();

  llvm::Value * ret = b->CreateAlloca(b->getPtrTy());
  
  llvm::BasicBlock * smallBB = llvm::BasicBlock::Create(*c, "small", f);
  llvm::BasicBlock * largeBB = llvm::BasicBlock::Create(*c, "large", f);
  llvm::BasicBlock * contBB = llvm::BasicBlock::Create(*c, "cont", f);
  
  b->CreateCondBr(buildVarArrayIsSmall(varcharPtr), smallBB, largeBB);
  b->SetInsertPoint(smallBB);
  b->CreateStore(b->CreateConstGEP1_64(b->getInt8Ty(),
                                       b->CreateBitCast(varcharPtr, b->getPtrTy()), 
				       1),
		 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(largeBB);
  b->CreateStore(b->CreateLoad(b->getPtrTy(),
                               b->CreateStructGEP(LLVMVarcharType, varcharPtr, 2)),
                 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(contBB);
  return b->CreateLoad(b->getPtrTy(), ret);
}

const IQLToLLVMValue * CodeGenerationContext::buildCompareResult(llvm::Value * boolVal)
{
  llvm::IRBuilder<> * b = LLVMBuilder;  
  llvm::Value * int32RetVal = b->CreateZExt(boolVal,
					    b->getInt32Ty(),
					    "cmpresultcast");
  return IQLToLLVMRValue::get(this, int32RetVal, IQLToLLVMValue::eLocal);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildCompareResult(llvm::Value * boolVal,
								    llvm::Value * ret)
{
  llvm::IRBuilder<> * b = LLVMBuilder;  
  llvm::Value * int32RetVal = b->CreateZExt(boolVal,
					    b->getInt32Ty(),
					    "cmpresultcast");
  b->CreateStore(int32RetVal, ret);
  return IQLToLLVMValue::eLocal;
}

void CodeGenerationContext::buildMemcpy(llvm::Value * sourcePtr,
					const FieldAddress& sourceOffset,
					llvm::Value * targetPtr, 
					const FieldAddress& targetOffset,
					int64_t sz)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcpyIntrinsic);
  llvm::Value * args[5];
  args[0] = targetOffset.getPointer("memcpy_tgt", this, targetPtr);
  args[1] = sourceOffset.getPointer("memcpy_src", this, sourcePtr);
  args[2] = b->getInt64(sz);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(1);
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
}

void CodeGenerationContext::buildMemcpy(const std::string& sourceArg,
					const FieldAddress& sourceOffset,
					const std::string& targetArg, 
					const FieldAddress& targetOffset,
					int64_t sz)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * sourcePtr = b->CreateLoad(b->getPtrTy(),
                                          lookupBasePointer(sourceArg.c_str())->getValue(this),
					  "srccpy");	
  llvm::Value * targetPtr = b->CreateLoad(b->getPtrTy(),
                                          lookupBasePointer(targetArg.c_str())->getValue(this),
					  "tgtcpy");
  buildMemcpy(sourcePtr, sourceOffset, targetPtr, targetOffset, sz);
}

void CodeGenerationContext::buildMemset(llvm::Value * targetPtr,
					const FieldAddress& targetOffset,
					int8_t value,
					int64_t sz)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemsetIntrinsic);
  llvm::Value * args[5];
  // GEP to get pointers at offsets and then call the intrinsic.
  args[0] = targetOffset.getPointer("memset_tgt", this, targetPtr);
  args[1] = b->getInt8(value);
  args[2] = b->getInt64(sz);
  // TODO: Speed things up by making use of alignment info we have.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 5), "");
}

llvm::Value * CodeGenerationContext::buildMemcmp(llvm::Value * sourcePtr,
						 const FieldAddress& sourceOffset,
						 llvm::Value * targetPtr, 
						 const FieldAddress& targetOffset,
						 int64_t sz)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function * fn = llvm::cast<llvm::Function>(LLVMMemcmpIntrinsic);
  // Implement bit casting to the right argument types.
  if (llvm::Type::PointerTyID != sourcePtr->getType()->getTypeID())
    throw std::runtime_error("sourcePtr argument to memcmp not pointer type");
  if (llvm::Type::PointerTyID != targetPtr->getType()->getTypeID())
    throw std::runtime_error("targetPtr argument to memcmp not pointer type");
  // This is what we expect.
  llvm::Type * int8Ptr = llvm::PointerType::get(b->getInt8Ty(), 0);
  // If not already pointers to int8, bitcast to that
  if (sourcePtr->getType() != int8Ptr)
    sourcePtr = b->CreateBitCast(sourcePtr, int8Ptr, "memcmp_lhs_cvt");
  if (targetPtr->getType() != int8Ptr)
    targetPtr = b->CreateBitCast(targetPtr, int8Ptr, "memcmp_rhs_cvt");

  llvm::Value * args[3];
  // GEP to get pointers at offsets and then call the intrinsic.
  args[0] = targetOffset.getPointer("memcmp_lhs", this, targetPtr);
  args[1] = sourceOffset.getPointer("memcmp_rhs", this, sourcePtr);
  args[2] = b->getInt64(sz);
  
  return b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&args[0], 3), "memcmp");
}

void CodeGenerationContext::buildBitcpy(const BitcpyOp& op,
					llvm::Value * sourcePtr,
					llvm::Value * targetPtr)
{
  // Implement shift as targetDword = targetDword&targetMask | ((sourceWord & sourceMask) >> N)
  // for an appropriate targetMask

  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  
  // Get int32 ptrs for source and target
  llvm::Type * int32Ty = b->getInt32Ty();
  llvm::Type * int32PtrTy = b->getPtrTy();
  llvm::Value * source = op.mSourceOffset.getPointer("bitcpySource", this, 
						     sourcePtr);
  source = b->CreateBitCast(source, int32PtrTy);
  llvm::Value * target = op.mTargetOffset.getPointer("bitcpyTarget", this, 
						     targetPtr);
  target = b->CreateBitCast(target, int32PtrTy);

  if (op.mShift < 0) {
    // We are shift source to the right.  Get mask for target
    // bits that we should leave in place.  Be careful here to
    // get 1's in any shifted in positions of targetMask by shifting before
    // negating.
    int32_t sourceShift = -op.mShift;
    uint32_t targetMask = ~((op.mSourceBitmask) >> sourceShift);
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(int32Ty, target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(int32Ty, source),
					      b->getInt32(op.mSourceBitmask));
    maskedSource = b->CreateLShr(maskedSource, sourceShift);
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  } else if (op.mShift > 0) {
    // We are shift source to the left
    int32_t sourceShift = op.mShift;
    uint32_t targetMask = ~((op.mSourceBitmask) << sourceShift);
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(int32Ty, target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(int32Ty, source),
					      b->getInt32(op.mSourceBitmask));
    maskedSource = b->CreateShl(maskedSource, sourceShift);
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  } else {
    uint32_t targetMask = ~op.mSourceBitmask;
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(int32Ty, target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(int32Ty, source),
					      b->getInt32(op.mSourceBitmask));
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  }
}

void CodeGenerationContext::buildBitset(const BitsetOp& op,
					llvm::Value * targetPtr)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  
  // Get int32 ptr for target
  llvm::Type * int32Ty = b->getInt32Ty();
  llvm::Type * int32PtrTy = b->getPtrTy();
  llvm::Value * target = op.mTargetOffset.getPointer("bitsetTarget", this, 
						     targetPtr);
  target = b->CreateBitCast(target, int32PtrTy);

  // Set bits in bitmask preserving any bits outside mask that
  // are already set.
  llvm::Value * tgt = b->CreateLoad(int32Ty, target);
  llvm::Value * newBits = b->getInt32(op.mTargetBitmask);
  b->CreateStore(b->CreateOr(tgt, newBits),
		 target);
}

void CodeGenerationContext::buildSetFieldsRegex(const std::string& sourceName,
						const RecordType * sourceType,
						const std::string& expr,
						const std::string& rename,
						const std::string& recordName,
						int * pos)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  llvm::Value * sourcePtr = b->CreateLoad(b->getPtrTy(), lookupBasePointer(sourceName.c_str())->getValue(this));
  llvm::Value * targetPtr = b->CreateLoad(b->getPtrTy(), lookupBasePointer("__OutputPointer__")->getValue(this));
			       
  RecordTypeCopy c(sourceType,
		   unwrap(IQLOutputRecord),
		   expr,
		   rename, 
		   pos);

  // Perform bitset as needed
  for(std::vector<BitsetOp>::const_iterator opit = c.getBitset().begin();
      opit != c.getBitset().end();
      ++opit) {
    buildBitset(*opit, targetPtr);			  
  }
  // Perform bitcpy as needed
  for(std::vector<BitcpyOp>::const_iterator opit = c.getBitcpy().begin();
      opit != c.getBitcpy().end();
      ++opit) {
    buildBitcpy(*opit, sourcePtr, targetPtr);
  }
  // Perform memcpy's as needed.
  // Explicitly copy fields as needed.
  for(std::vector<MemcpyOp>::const_iterator opit = c.getMemcpy().begin();
      opit != c.getMemcpy().end();
      ++opit) {
    buildMemcpy(sourcePtr,
		opit->mSourceOffset,
		targetPtr,
		opit->mTargetOffset,
		opit->mSize);
			  
  }
  for(RecordTypeCopy::set_type::const_iterator fit = c.getSet().begin();
      fit != c.getSet().end();
      ++fit) {
    int tmp=fit->second;
    auto lval = recordName.size() ?
      buildVariableRef(recordName.c_str(), sourceType,
                       fit->first.GetName().c_str(), fit->first.GetType()) :
      buildVariableRef(fit->first.GetName().c_str(), fit->first.GetType());
      
    buildSetField(&tmp, lval);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildRef(const IQLToLLVMValue * allocAVal,
						       const FieldType * resultTy)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  /* Two cases here.  Value types (8 bytes and less) are loaded whereas larger types 
     such as varchar and decimal are passed by reference.  Values that are value are always local */
  if (isPointerToValueType(allocAVal->getValue(this), resultTy)) {
    //std::cout << "Loading variable " << var << "\n";
    return IQLToLLVMRValue::get(this, 
                                b->CreateLoad(resultTy->LLVMGetType(this),
                                              allocAVal->getValue(this)),
                                allocAVal->getNull(this),
                                IQLToLLVMValue::eLocal);
  } else {
    //std::cout << "Variable reference " << var << "\n";
    return allocAVal;
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildVariableRef(const char * var,
                                                               const FieldType * varTy,
							       const char * var2,
							       const FieldType * retTy)
{
  /* Lookup value in symbol table */
  /* TODO: Handle undefined reference */
  const IQLToLLVMLValue * lval = lookup(var);
  if (nullptr != var2) {
    const RecordType * recTy = dynamic_cast<const RecordType *>(varTy);
    if (nullptr == recTy) {
      std::cout << "Prefixed variable reference with prefix type " << varTy->toString() << " not record: " << var << "." << var2 << std::endl;
    }
    BOOST_ASSERT(nullptr != recTy);
    lval = buildRowRef(lval, recTy, recTy->getMemberIndex(var2), recTy->getMember(var2).GetType());
  }
  return lval;
}

const IQLToLLVMValue * CodeGenerationContext::buildVariableRef(const char * var,
                                                               const FieldType * varTy)
{
  return buildVariableRef(var, varTy, nullptr, nullptr);
}

const IQLToLLVMValue * CodeGenerationContext::buildNullableUnaryOp(const IQLToLLVMValue * lhs, 
								   const FieldType * lhsType, 
								   const FieldType * resultType,
								   UnaryOperatorMemFn unOp)
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * n1 = lhs->getNull(this);
  BOOST_ASSERT((lhsType->isNullable() && n1 != NULL) ||
	       (!lhsType->isNullable() && n1 == NULL));
  if (lhs->isLiteralNull()) {
    return buildNull();
  }
  llvm::Value * nv = NULL;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nullableUnOp");    
  // This will be filled in by the binOp we call.
  IQLToLLVMValue::ValueType vt = IQLToLLVMValue::eLocal;
  if (n1 != NULL) {
    // The function we are working on.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    // Create blocks for the then/value and else (likely to be next conditional).  
    // Insert the 'then/value' block at the end of the function.
    llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
    llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);

    // Test if NULL and branch.  
    nv = n1;
    b->CreateCondBr(b->CreateNot(nv), thenBB, mergeBB);
    // Emit then value.
    b->SetInsertPoint(thenBB);  
    vt = (this->*unOp)(lhs, lhsType, result, resultType);
    b->CreateBr(mergeBB);    
    b->SetInsertPoint(mergeBB);
  } else {
    vt = (this->*unOp)(lhs, lhsType, result, resultType);
    result = trimAlloca(result, resultType);
  }
  // Return either pointer or value
  if (isPointerToValueType(result, resultType)) {
    result = b->CreateLoad(retTy, result);
  }
  return IQLToLLVMRValue::get(this, result, nv, vt);
}

const IQLToLLVMValue * CodeGenerationContext::buildNonNullableUnaryOp(const IQLToLLVMValue * lhs, 
								      const FieldType * lhsType, 
								      const FieldType * resultType,
								      UnaryOperatorMemFn unOp)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nonNullUnOp");    
  IQLToLLVMValue::ValueType vt = (this->*unOp)(lhs, lhsType, result, resultType);
  // Clean up the alloca since we really only needed it to call the cast
  // methods
  result = trimAlloca(result, resultType);
  // Return either pointer or value
  if (isPointerToValueType(result, resultType)) {
    result = b->CreateLoad(retTy, result);
  }
  // Just propagate the null value of the incoming arg
  return IQLToLLVMRValue::get(this, result, lhs->getNull(this), vt);
}

const IQLToLLVMValue * CodeGenerationContext::buildNullableBinaryOp(const IQLToLLVMValue * lhs, 
								    const FieldType * lhsType, 
								    const IQLToLLVMValue * rhs, 
								    const FieldType * rhsType, 
								    const FieldType * resultType,
								    BinaryOperatorMemFn binOp)
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * n1 = lhs->getNull(this);
  llvm::Value * n2 = rhs->getNull(this);
  BOOST_ASSERT((lhsType->isNullable() && n1 != NULL) ||
	       (!lhsType->isNullable() && n1 == NULL));
  BOOST_ASSERT((rhsType->isNullable() && n2 != NULL) ||
	       (!rhsType->isNullable() && n2 == NULL));
  if (lhs->isLiteralNull() || rhs->isLiteralNull()) {
    return buildNull();
  }
  llvm::Value * nv = NULL;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nullableBinOp");    
  // This will be filled in by the binOp we call.
  IQLToLLVMValue::ValueType vt = IQLToLLVMValue::eLocal;
  if (n1 != NULL || n2 != NULL) {
    // The function we are working on.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    // Create blocks for the then/value and else (likely to be next conditional).  
    // Insert the 'then/value' block at the end of the function.
    llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
    llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);

    // Test if NULL and branch.  We may have one or two values to check
    nv = n1 != NULL ? (n2 != NULL ? b->CreateOr(n1,n2) : n1) : n2;
    b->CreateCondBr(b->CreateNot(nv), thenBB, mergeBB);
    // Emit then value.
    b->SetInsertPoint(thenBB);  
    vt = (this->*binOp)(lhs, lhsType, rhs, rhsType, result, resultType);
    b->CreateBr(mergeBB);    
    b->SetInsertPoint(mergeBB);
  } else {
    vt = (this->*binOp)(lhs, lhsType, rhs, rhsType, result, resultType);
    result = trimAlloca(result, resultType);
  }
  // Return either pointer or value
  if (isPointerToValueType(result, resultType)) {
    result = b->CreateLoad(retTy, result);
  }
  return IQLToLLVMRValue::get(this, result, nv, vt);
}

llvm::Value * CodeGenerationContext::trimAlloca(llvm::Value * result, 
						const FieldType * resultType)
{
  if (isPointerToValueType(result, resultType)) {
    // Check whether the last instruction is a store to the result
    // Check whether that is the only use of result.  If so then
    // we can replace result by the value stored in it.  
    // The point here is to avoid too many alloca's from proliferating;
    // they will be removed by mem2reg during optimziation but
    // for really huge programs that takes a lot of time and memory.
    // It is much faster to fix up the problem here.
    llvm::AllocaInst * AI = 
      llvm::dyn_cast<llvm::AllocaInst>(result);
    if (AI != NULL && 
	AI->hasOneUse()) {
      if(llvm::StoreInst * SI = 
	 llvm::dyn_cast<llvm::StoreInst>(*AI->use_begin())) {
	if (!SI->isVolatile() && SI->getPointerOperand() == AI) {
	  // Conditions satisfied. Get rid of the store and the
	  // alloca and replace result with the value of the
	  // store.
	  llvm::Value * val = SI->getValueOperand();
	  SI->eraseFromParent();
	  AI->eraseFromParent();
	  return val;
	}
      }
    }
  }
  return result;
}

llvm::Value * CodeGenerationContext::getCachedLocal(const FieldType * ty)
{
  local_cache::iterator it = AllocaCache->find(ty);
  if (it == AllocaCache->end() || 0==it->second.size()) return NULL;
  llvm::Value * v = it->second.back();
  it->second.pop_back();
  return v;
}

void CodeGenerationContext::returnCachedLocal(llvm::Value * v, const FieldType * ty)
{
  const llvm::PointerType * pty = llvm::dyn_cast<llvm::PointerType>(v->getType());
  local_cache c(*AllocaCache);
  local_cache::iterator it = c.find(ty);
  if (it == c.end()) {
    c[ty] = std::vector<llvm::Value *>();
  }
  c[ty].push_back(v);
}

llvm::Value * CodeGenerationContext::buildEntryBlockAlloca(llvm::Type * ty, const char * name)
{
  // Create a new builder positioned at the beginning of the entry block of the function
  llvm::Function* TheFunction = llvm::dyn_cast<llvm::Function>(LLVMFunction);
  llvm::IRBuilder<> TmpB(&TheFunction->getEntryBlock(),
                         TheFunction->getEntryBlock().begin());
  return TmpB.CreateAlloca(ty, 0, name);
}

llvm::Value * CodeGenerationContext::buildEntryBlockAlloca(const FieldType * ty, const char * name)
{
  auto tmp = buildEntryBlockAlloca(ty->LLVMGetType(this), name);
  if (FieldType::VARIABLE_ARRAY == ty->GetEnum()) {
    llvm::IRBuilder<> * b = LLVMBuilder;
    llvm::Type * varcharType = ty->LLVMGetType(this);
    llvm::Type * int64PtrTy = llvm::PointerType::get(b->getInt64Ty(), 0);
    b->CreateStore(b->getInt64(0), b->CreateBitCast(b->CreateStructGEP(varcharType, tmp, 2), int64PtrTy));
  }
  return tmp;
}  

void CodeGenerationContext::buildSetValue2(const IQLToLLVMValue * iqlVal,
					   const IQLToLLVMLValue * iqllvalue,
					   const FieldType * ft,
					   bool freeGlobalLeftHandSide)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * lvalue = iqllvalue->getValuePointer(this)->getValue(this);
  if (NULL == lvalue)
    throw std::runtime_error("Undefined variable ");

  //
  // Perform any necessary type promotion of rvalue.
  //
  // TODO: Do we really need type promotion in this method at all anymore or has
  // it been handled by wrappers?
  // TODO: What are the interactions between type promotion and the
  // local/global value stuff going on in here?  Namely should we really
  // do type promotions first before handling global/local?
  // TODO: It really would have been easier to determine the type promotion
  // at type checking time when we had the IQL types handy.
  BOOST_ASSERT(llvm::Type::PointerTyID == lvalue->getType()->getTypeID());
  llvm::Value * llvmVal = iqlVal->getValue(this);

  // DECIMAL/VARCHAR expressions return a reference/pointer.  
  // Before setting we must load.  Perhaps we'd be better off with
  // a memcpy instrinsic here.
  if (ft->GetEnum() == FieldType::BIGDECIMAL ||
      ft->GetEnum() == FieldType::CHAR ||
      ft->GetEnum() == FieldType::IPV6 ||
      ft->GetEnum() == FieldType::CIDRV6) {
    // TODO: Should probably use memcpy rather than load/store
    llvmVal = b->CreateLoad(ft->LLVMGetType(this), llvmVal);
  } else if (ft->GetEnum() == FieldType::VARCHAR) {
    // TODO:
    // Four cases here depending on the global/local dichotomy for the source
    // value and the target variable.
    // Source value global, Target Variable global : Must deep copy string (unless move semantics specified) remove from interpreter heap
    // Source value global, Target Variable local : Must deep copy string, must record in interpreter heap
    // Source value local, Target Variable global : May shallow copy but must remove source memory from interpreter heap.  TODO: This doesn't seem right!  What if two globals refer to the same local value?  Is this possible?  It may not be if the local is a temporary created for a string expression but it may be if we have a string variable on the stack.  It seems we need  use-def chain to know what is the right thing to do here.
    // Source value local, Target Variable local : May shallow copy string
    // Note that above rules depend critically on the assumption that strings are not
    // modifiable (because the above rules allow multiple local variables to 
    // reference the same underlying pointer).  
    // Also the above rules depend on the fact that
    // we know in all case where a value came from.  With mutable local variables it would
    // possible that the variable could have a local value at some point and a global value at
    // another. Removing this ambiguity is why global value assignments to local variables must
    // deep copy using the local heap.
    // Move semantics make all of this much more complicated and I don't think I
    // understand how to do this properly at this point.
    // For global values that contain pointers (e.g. strings)
    // we must copy the string onto the appropriate heap or at least disassociate from
    // internal heap tracking.  Here we take the latter path.  It is cheaper but less general
    // in that it assumes that the heap used by the IQL runtime is the same as that used
    // by the client of the runtime.
    if (freeGlobalLeftHandSide && iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      buildFree(iqllvalue, ft);
    }
    if (iqlVal->getValueType() != IQLToLLVMValue::eLocal ||
        (iqllvalue->getValueType() == IQLToLLVMValue::eGlobal && nullptr != dynamic_cast<const IQLToLLVMLValue *>(iqlVal))) {
      // TODO: IF we swap args 1,2 we should be able to use buildCall()
      // Call to copy the varchar before setting.
      llvm::Value * callArgs[4];
      llvm::Function * fn = LLVMModule->getFunction("InternalVarcharCopy");
      callArgs[0] = llvmVal;
      callArgs[1] = buildEntryBlockAlloca(LLVMVarcharType, "");
      callArgs[2] = b->getInt32(iqllvalue->getValueType() == IQLToLLVMValue::eGlobal ? 0 : 1);
      callArgs[3] = b->CreateLoad(LLVMDecContextPtrType, getContextArgumentRef());
      b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 4), "");
      llvmVal = callArgs[1];
    } else if (iqlVal->getValueType() == IQLToLLVMValue::eLocal &&
               iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      buildEraseAllocationTracking(iqlVal, ft);
    }
    // Load before store since we have a pointer to the Varchar.
    llvmVal = b->CreateLoad(ft->LLVMGetType(this), llvmVal);    
  } else if (ft->GetEnum() == FieldType::VARIABLE_ARRAY ||
             ft->GetEnum() == FieldType::FIXED_ARRAY) {
    if (freeGlobalLeftHandSide && iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      buildFree(iqllvalue, ft);
    }

    if (iqlVal->getValueType() != IQLToLLVMValue::eLocal ||
        (iqllvalue->getValueType() == IQLToLLVMValue::eGlobal && nullptr != dynamic_cast<const IQLToLLVMLValue *>(iqlVal))) {
      // Allocate storage for the VARIABLE_ARRAY, tracking allocation unless the lvalue is global
      const SequentialType * arrayType = dynamic_cast<const SequentialType *>(ft);
      llvm::Value * arrayLen = getArraySize(iqlVal, ft);
      if (FieldType::VARIABLE_ARRAY == ft->GetEnum()) {
        llvmVal = buildVariableArrayAllocate(arrayType, arrayLen, iqllvalue->getValueType() != IQLToLLVMValue::eGlobal);
      } else {
        llvmVal = buildEntryBlockAlloca(ft->LLVMGetType(this), "");
      }
      
      // Now we must copy over all of the elements in a loop
      const IQLToLLVMValue * tmpArrVal = IQLToLLVMRValue::get(this, llvmVal, iqllvalue->getValueType());
      buildArrayElementwiseCopy(iqlVal, arrayType,
                                buildFalse(), IQLToLLVMRValue::get(this, arrayLen, IQLToLLVMValue::eLocal),
                                buildFalse(), tmpArrVal, arrayType);
    } else if (iqlVal->getValueType() == IQLToLLVMValue::eLocal &&
               iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      buildEraseAllocationTracking(iqlVal, ft);
    }
    // Load before store since we have a pointer to the VarArray
    llvmVal = b->CreateLoad(ft->LLVMGetType(this), llvmVal);    
  } else if (ft->GetEnum() == FieldType::STRUCT) {
    if (freeGlobalLeftHandSide && iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      buildFree(iqllvalue, ft);
    }

    if (iqlVal->getValueType() != IQLToLLVMValue::eLocal ||
        (iqllvalue->getValueType() == IQLToLLVMValue::eGlobal && nullptr != dynamic_cast<const IQLToLLVMLValue *>(iqlVal))) {
      // Allocate storage for the STRUCT
      const RecordType * structType = dynamic_cast<const RecordType *>(ft);
      llvmVal = buildEntryBlockAlloca(ft->LLVMGetType(this), "");
      
      // Now we must copy over all of the elements in a loop
      const IQLToLLVMValue * tmpStructVal = IQLToLLVMRValue::get(this, llvmVal, iqllvalue->getValueType());
      buildStructElementwiseCopy(iqlVal, structType,
                                 0, structType->getNumElements(),
                                 0, tmpStructVal, structType);
    } else if (iqlVal->getValueType() == IQLToLLVMValue::eLocal &&
               iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      buildEraseAllocationTracking(iqlVal, ft);
    }
    // Load before store since we have a pointer to the Struct
    llvmVal = b->CreateLoad(ft->LLVMGetType(this), llvmVal);    
  } 

  // Finally we can just issue the store.
  b->CreateStore(llvmVal, lvalue);
}

// set an rvalue into an lvalue handling nullability
// if allowNullToNonNull is false this throws if the 
// rvalue is nullable and the lvalue is not nullable.
// if true this is allowed.  It is incumbent on the caller
// to know if sufficient runtime checks have been performed
// in order for us to safely coerce a nullable value into
// a nonnullable one (e.g. IFNULL(x,y) is the canonical example).
void CodeGenerationContext::buildSetNullableValue(const IQLToLLVMLValue * lval,
						  const IQLToLLVMValue * val,
						  const FieldType * ft,
						  bool allowNullToNonNull,
						  bool freeGlobalLeftHandSide)
{
  // Check nullability of value and target
  if (lval->isNullable()) {
    // Unwrap to C++
    llvm::LLVMContext * c = LLVMContext;
    llvm::IRBuilder<> * b = LLVMBuilder;
    if (val->isLiteralNull()) {
      // NULL literal
      lval->setNull(this, true);
    } else if (val->getNull(this) != NULL) {
      // Set or clear NULL bit.  Only set value if val is NOT NULL
      // Code we are generating is the following pseudocode:
      // if (!isNull(val)) {
      //   clearNull(member);
      //   setValue(member, val);
      // } else {
      //   setNull(member);
      //
    
      // The function we are working on.
      llvm::Function *f = b->GetInsertBlock()->getParent();
      // Create blocks for the then/value and else (likely to be next conditional).  
      // Insert the 'then/value' block at the end of the function.
      llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
      llvm::BasicBlock * elseBB = llvm::BasicBlock::Create(*c, "else", f);
      llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);
      // Test if NULL and branch
      b->CreateCondBr(b->CreateNot(val->getNull(this)), 
		      thenBB, elseBB);
      // Emit then value.
      b->SetInsertPoint(thenBB);  
      lval->setNull(this, false);
      buildSetValue2 (val, lval, ft, freeGlobalLeftHandSide);
      b->CreateBr(mergeBB);

      // Now the NULL case: here we just clear the NULL bit
      b->SetInsertPoint(elseBB);
      lval->setNull(this, true);
      b->CreateBr(mergeBB);
      b->SetInsertPoint(mergeBB);
    } else {
      // Setting non-nullable value into a nullable lvalue.
      buildSetValue2 (val, lval, ft, freeGlobalLeftHandSide);
      lval->setNull(this, false);
    }
  } else {
    BOOST_ASSERT(allowNullToNonNull ||
		 val->getNull(this) == NULL);
    buildSetValue2 (val, lval, ft, freeGlobalLeftHandSide);
  }
}

void CodeGenerationContext::buildSetNullableValue(const IQLToLLVMLValue * lval,
						  const IQLToLLVMValue * val,
						  const FieldType * valType,
						  const FieldType * lvalType)
{
  // TODO: Would it be more efficient to push the type promotion down because we
  // are checking nullability twice this way.
  const IQLToLLVMValue * cvt = buildCast(val, valType, lvalType);
  buildSetNullableValue(lval, cvt, lvalType, false, true);
}

void CodeGenerationContext::buildSetValue(const IQLToLLVMValue * iqlVal, const char * loc, const FieldType * ft)
{
  const IQLToLLVMLValue * lval = lookup(loc);
  buildSetNullableValue(lval, iqlVal, ft, false, true);
}

void CodeGenerationContext::buildCaseBlockBegin(const FieldType * caseType)
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Function *f = b->GetInsertBlock()->getParent();

  // Create a merge block with a PHI node 
  // Save the block and PHI so that incoming values may be added.
  // The block itself will be emitted at the end of the CASE expression.
  llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "casemerge", f);
  // Allocate space for a result.  Try to reuse space used for a
  // completed CASE.
  llvm::Value * result = getCachedLocal(caseType);
  if (result == NULL) {
    llvm::Type * retTy = caseType->LLVMGetType(this);
    result = buildEntryBlockAlloca(retTy, "caseResult");    
  }
  llvm::Value * nullVal = NULL;
  llvm::Type * nullValTy = NULL;
  if (caseType->isNullable()) {
    nullValTy = b->getInt1Ty();
    nullVal = buildEntryBlockAlloca(nullValTy, "caseNullBit");
  }     
  IQLToLLVMLocal * lVal = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(IQLToLLVMRValue::get(this, result, IQLToLLVMValue::eLocal), caseType), nullVal, nullValTy);
  IQLCase.push(new IQLToLLVMCaseState(lVal, mergeBB));  
  BOOST_ASSERT(IQLCase.size() != 0);
}

void CodeGenerationContext::buildCaseBlockIf(const IQLToLLVMValue * condVal)
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // The function we are working on.
  llvm::Function *f = b->GetInsertBlock()->getParent();
  // Create blocks for the then/value and else (likely to be next conditional).  
  // Insert the 'then/value' block at the end of the function.
  llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
  // Save else block so we can append it after the value is emitted 
  BOOST_ASSERT(IQLCase.size() != 0);
  BOOST_ASSERT(IQLCase.top()->ElseBB == NULL);
  IQLCase.top()->ElseBB = llvm::BasicBlock::Create(*c, "else", f);
  // Handle ternary logic here
  llvm::Value * nv = condVal->getNull(this);
  if (nv) {
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "notNull", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, IQLCase.top()->ElseBB);
    b->SetInsertPoint(notNullBB);
  }
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(this),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, thenBB, IQLCase.top()->ElseBB);
  // Emit then value.
  b->SetInsertPoint(thenBB);  
}

void CodeGenerationContext::buildCaseBlockThen(const IQLToLLVMValue *value, const FieldType * valueType, const FieldType * caseType, bool allowNullToNonNull)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  // Convert to required type 
  const IQLToLLVMValue * cvtVal = buildCast(value, valueType, caseType);
  if (NULL == cvtVal) {
    // This should always succeed as type checking should have
    // validated the conversion.
    llvm::Type * cvtTy = caseType->LLVMGetType(this);
    throw std::runtime_error("INTERNAL ERROR: Failed type promotion during code generation");
  }
  BOOST_ASSERT(IQLCase.size() != 0);
  IQLToLLVMCaseState * state = IQLCase.top();
  // Store converted value in the case return variable
  buildSetNullableValue(state->Local, cvtVal, 
			caseType, allowNullToNonNull, true);
  // Branch to block with PHI
  b->CreateBr(state->getMergeBlock());
  
  // Emit else block if required (will be required
  // if we are WHEN but not for an ELSE).
  if (state->ElseBB) {
    b->SetInsertPoint(state->ElseBB);
    state->ElseBB = NULL;
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCaseBlockFinish(const FieldType * caseType)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  // Emit merge block 
  BOOST_ASSERT(IQLCase.size() != 0);
  b->SetInsertPoint(IQLCase.top()->getMergeBlock());

  // This is our return value
  IQLToLLVMLocal * lVal = IQLCase.top()->Local;
  // Pointer to value we allocated
  llvm::Value *result=lVal->getValuePointer(this)->getValue(this);
  // llvm::Value *result=rVal->getValue(this);
  // Get the null bit if a nullable value
  llvm::Value * nullBit = lVal->getNull(this);
  // llvm::Value * nullBit = rVal->getNull(this);
  // Return either pointer or value
  if (result != NULL &&
      isPointerToValueType(result, caseType)) {
    // The pointer to the local will not be used beyond this point,
    // so it may be reused.
    returnCachedLocal(result, caseType);
    result = b->CreateLoad(caseType->LLVMGetType(this), result);
  }
  // Done with this CASE so pop off.
  delete IQLCase.top();
  IQLCase.pop();

  return IQLToLLVMRValue::get(this, result, nullBit, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildLeastGreatest(const std::vector<IQLToLLVMTypedValue> & args,
								 const FieldType * retTy,
								 bool isLeast)
{
  // Recursively generate Greatest(x1,x2,..., xN) = Greatest(Greatest(x1,x2),x3, ..., xN)
  // Greatest(x1,x2) = x1 < x2 ? x2 : x1
  // and similarly for least.
  if (args.size() == 2) {
    buildCaseBlockBegin(retTy);        
    // Hack!  Essentially we are implementing a tree rewrite during code generation and
    // unfortunately I need access to a type that would have been computed during semantic
    // analysis post-tree rewrite.  Create the correct type here.  This is dangerous since I
    // don't have access to the context in which other types were created so pointer comparison
    // of types won't work properly.  In addition I am breaking the abstraction of the type checker
    // and its representation of boolean types
    DynamicRecordContext tmpCtxt;
    Int32Type * tmpTy = Int32Type::Get(tmpCtxt, args[0].getType()->isNullable() || args[1].getType()->isNullable());
    buildCaseBlockIf(buildCompare(args[0].getValue(), 
				  args[0].getType(),
				  args[1].getValue(), 
				  args[1].getType(),
				  tmpTy,
				  isLeast ? IQLToLLVMOpLT : IQLToLLVMOpGT));
    buildCaseBlockThen(args[0].getValue(), args[0].getType(), retTy, false);
    buildCaseBlockThen(args[1].getValue(), args[1].getType(), retTy, false);
    return buildCaseBlockFinish(retTy);
  } else if (args.size() == 1) {
    // Convert type if necessary.
    return buildCast(args[0].getValue(), args[0].getType(), retTy);
  } else {
    BOOST_ASSERT(args.size() > 2);
    std::vector<IQLToLLVMTypedValue> a(args.begin(), args.begin()+2);
    std::vector<IQLToLLVMTypedValue> b(1, IQLToLLVMTypedValue(buildLeastGreatest(a, retTy, isLeast), retTy));
    b.insert(b.end(), args.begin()+2, args.end());
    return buildLeastGreatest(b, retTy, isLeast);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildIsNullFunction(const std::vector<IQLToLLVMTypedValue> & args,
								  const FieldType * retTy)
{
  const IQLToLLVMValue * val = args[0].getValue();
  const IQLToLLVMValue * alt = args[1].getValue();
  if (val->getNull(this) == NULL) {
    // If val is not nullable just return it.
    return val;
  } else {
    buildCaseBlockBegin(retTy);        
    buildCaseBlockIf(buildIsNull(args[0].getValue()));
    buildCaseBlockThen(args[1].getValue(), args[1].getType(), retTy, false);
    buildCaseBlockThen(args[0].getValue(), args[0].getType(), retTy, true);
    return buildCaseBlockFinish(retTy); 
  }
}

void CodeGenerationContext::buildBeginAnd(const FieldType * retType)
{
  buildCaseBlockBegin(retType);
}

void CodeGenerationContext::buildAddAnd(const IQLToLLVMValue * lhs,
					const FieldType * lhsType,
					const FieldType * retType)
{
  buildCaseBlockIf(lhs);
}

const IQLToLLVMValue * CodeGenerationContext::buildAnd(const IQLToLLVMValue * rhs,
						       const FieldType * rhsType,
						       const FieldType * retType)
{
  buildCaseBlockThen(rhs, rhsType, retType, false);
  // HACK! I need the FieldType for boolean and I don't have another way of getting it
  DynamicRecordContext tmpCtxt;
  Int32Type * tmpTy = Int32Type::Get(tmpCtxt, false);
  buildCaseBlockThen(buildFalse(), tmpTy, retType, false); 
  return buildCaseBlockFinish(retType);
}

void CodeGenerationContext::buildBeginOr(const FieldType * retType)
{
  buildCaseBlockBegin(retType);
}

void CodeGenerationContext::buildAddOr(const IQLToLLVMValue * lhs,
				       const FieldType * lhsType,
				       const FieldType * retType)
{
  buildCaseBlockIf(lhs);
  // HACK! I need the FieldType for boolean and I don't have another way of getting it
  DynamicRecordContext tmpCtxt;
  Int32Type * tmpTy = Int32Type::Get(tmpCtxt, false);
  buildCaseBlockThen(buildTrue(), tmpTy, retType, false); 
}

const IQLToLLVMValue * CodeGenerationContext::buildOr(const IQLToLLVMValue * rhs,
						      const FieldType * rhsType,
						      const FieldType * retType)
{
  buildCaseBlockThen(rhs, rhsType, retType, false);
  return buildCaseBlockFinish(retType);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildNot(const IQLToLLVMValue * lhs,
				const FieldType * lhsTy,
				llvm::Value * ret,
				const FieldType * retTy)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * one = b->getInt32(1);
  llvm::Value * e1 = lhs->getValue(this);
  // TODO: Currently representing booleans as int32_t
  // Implement NOT as x+1 & 1
  llvm::Value * r = b->CreateAnd(b->CreateAdd(e1,one),one);
  b->CreateStore(r, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildNot(const IQLToLLVMValue * e, 
						       const FieldType * argType, 
						       const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildNot);
}

const IQLToLLVMValue * CodeGenerationContext::buildIsNull(const IQLToLLVMValue * val)
{
  llvm::Value * nv = val->getNull(this);
  if (nv) {
    // Extend to 32 bit integer.
    return buildCompareResult(nv);
  } else {
    return buildTrue();
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildIsNull(const IQLToLLVMValue * lhs,
							  const FieldType * lhsType, 
							  const FieldType * retType, 
							  int isNotNull)
{
  llvm::Value * nv = lhs->getNull(this);
  if (nv) {
    // Nullable value.  Must check the null... 
    if (isNotNull) {
      llvm::IRBuilder<> * b = LLVMBuilder;
      nv = b->CreateNot(nv);
    }
    return buildCompareResult(nv);
  } else {
    return isNotNull ? buildTrue() : buildFalse();
  }
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildVarcharCompare(llvm::Value * e1, 
					   llvm::Value * e2,
					   llvm::Value * ret,
					   IQLToLLVMPredicate opCode)
{
  // TODO: Inline.  For now call an external function.
  const char * booleanFuncs [] = { "InternalVarcharEquals",
				   "InternalVarcharNE",
				   "InternalVarcharGT",
				   "InternalVarcharGE",
				   "InternalVarcharLT",
				   "InternalVarcharLE",
                                   "InternalVarcharRLike"
  };
  const char * tmpNames [] = { "varchareqtmp",
			       "varcharnetmp",
			       "varchargttmp",
			       "varchargetmp",
			       "varcharlttmp",
			       "varcharletmp",
                               "varcharrliketmp"
  };
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * callArgs[3];
  llvm::Function * fn = LLVMModule->getFunction(booleanFuncs[opCode]);
  callArgs[0] = e1;
  callArgs[1] = e2;
  callArgs[2] = b->CreateLoad(LLVMDecContextPtrType, getContextArgumentRef());
  llvm::Value * cmp = b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), tmpNames[opCode]);
  b->CreateStore(cmp, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue *
CodeGenerationContext::buildArrayElementwiseEquals(const IQLToLLVMValue * lhs, 
                                                   const IQLToLLVMValue * rhs,
                                                   const FieldType * promoted,
                                                   const FieldType * retType)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  const FieldType * eltType = reinterpret_cast<const SequentialType*>(promoted)->getElementType();

  // Constants zero and one are used frequently
  const IQLToLLVMValue * zero = buildFalse();
  const IQLToLLVMValue * one  = buildTrue();
  // INTEGER type used frequently in this method
  FieldType * int32Type = Int32Type::Get(promoted->getContext());

  const IQLToLLVMValue * lhsSz = IQLToLLVMRValue::get(this, getArraySize(lhs, promoted), IQLToLLVMValue::eLocal);
  const IQLToLLVMValue * rhsSz = IQLToLLVMRValue::get(this, getArraySize(rhs, promoted), IQLToLLVMValue::eLocal);
  std::vector<IQLToLLVMTypedValue> sizes = { IQLToLLVMTypedValue(lhsSz, int32Type), IQLToLLVMTypedValue(rhsSz, int32Type) };
  const IQLToLLVMValue * sz=buildLeastGreatest(sizes, int32Type, true);

  // DECLARE ret = true
  // Allocate return value and initialize to true
  const IQLToLLVMValue * ret = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(), "ret"), nullptr, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * retLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(ret, int32Type));
  buildSetNullableValue(retLValue, one, int32Type, int32Type);
  
  // DECLARE idx = 0
  // Allocate and initialize counter
  llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
  const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
  buildSetNullableValue(counterLValue, zero, int32Type, int32Type);

  // DECLARE notDone = true
  // notDone flag is a hack since I haven't implemented IF and BREAK.   Initialize to true.
  const IQLToLLVMValue * notDone = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(),"notDone"), IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * notDoneLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(notDone, int32Type));
  buildSetNullableValue(notDoneLValue, one, int32Type, int32Type);

  whileBegin();

  // while (notDone AND i < sz)
  buildBeginAnd(int32Type);
  buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
  const IQLToLLVMValue * pred = buildAnd(buildCompare(buildRef(counter, int32Type), int32Type, sz, int32Type, int32Type, IQLToLLVMOpLT), int32Type, int32Type);
  whileStatementBlock(pred, int32Type);

  // TODO: Add IF and BREAK to language to simplify this
  const IQLToLLVMValue * idx = buildRef(counter, int32Type);

  // SET ret = CASE WHEN notDone AND rhs[idx] <> lhs[idx] THEN 0 ELSE ret END
  buildBeginAnd(int32Type);
  buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
  pred = buildAnd(buildCompare(buildArrayRef(rhs, promoted, idx, int32Type, eltType), eltType, buildArrayRef(lhs, promoted, idx, int32Type, eltType), eltType, int32Type, IQLToLLVMOpNE), int32Type, int32Type);
  buildCaseBlockBegin(int32Type);
  buildCaseBlockIf(pred);
  buildCaseBlockThen(zero, int32Type, int32Type, false);
  buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
  buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
  // SET notDone = CASE WHEN notDone AND rhs[idx] <> lhs[idx] THEN 0 ELSE notDone END
  buildCaseBlockBegin(int32Type);
  buildCaseBlockIf(pred);
  buildCaseBlockThen(zero, int32Type, int32Type, false);
  buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
  buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);

  // SET idx = idx + 1
  buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
  whileFinish();

  return buildRef(ret, int32Type);
}

const IQLToLLVMValue *
CodeGenerationContext::buildArrayElementwiseCompare(const IQLToLLVMValue * lhs, 
                                                    const IQLToLLVMValue * rhs,
                                                    const FieldType * promoted,
                                                    const FieldType * retType,
                                                    IQLToLLVMPredicate op)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  const FieldType * eltType = reinterpret_cast<const SequentialType*>(promoted)->getElementType();

  // Constants zero and one are used frequently
  const IQLToLLVMValue * zero = buildFalse();
  const IQLToLLVMValue * one  = buildTrue();
  // INTEGER type used frequently in this method
  FieldType * int32Type = Int32Type::Get(promoted->getContext());

  const IQLToLLVMValue * lhsSz = IQLToLLVMRValue::get(this, getArraySize(lhs, promoted), IQLToLLVMValue::eLocal);
  const IQLToLLVMValue * rhsSz = IQLToLLVMRValue::get(this, getArraySize(rhs, promoted), IQLToLLVMValue::eLocal);
  std::vector<IQLToLLVMTypedValue> sizes = { IQLToLLVMTypedValue(lhsSz, int32Type), IQLToLLVMTypedValue(rhsSz, int32Type) };
  const IQLToLLVMValue * sz=buildLeastGreatest(sizes, int32Type, true);

  // DECLARE ret = false
  // Allocate return value and initialize to false
  const IQLToLLVMValue * ret = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(), "ret"), nullptr, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * retLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(ret, int32Type));
  buildSetNullableValue(retLValue, zero, int32Type, int32Type);
  
  // DECLARE idx = 0
  // Allocate and initialize counter
  llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
  const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
  buildSetNullableValue(counterLValue, zero, int32Type, int32Type);

  // DECLARE notDone = true
  // notDone flag is a hack since I haven't implemented IF and BREAK.   Initialize to true.
  const IQLToLLVMValue * notDone = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(),"notDone"), IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * notDoneLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(notDone, int32Type));
  buildSetNullableValue(notDoneLValue, one, int32Type, int32Type);

  whileBegin();

  // while (notDone AND i < sz)
  buildBeginAnd(int32Type);
  buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
  const IQLToLLVMValue * pred = buildAnd(buildCompare(buildRef(counter, int32Type), int32Type, sz, int32Type, int32Type, IQLToLLVMOpLT), int32Type, int32Type);
  whileStatementBlock(pred, int32Type);

  // TODO: Add IF and BREAK to language to simplify this
  const IQLToLLVMValue * idx = buildRef(counter, int32Type);

  // SET ret = CASE WHEN notDone AND rhs[idx] < lhs[idx] THEN 0 ELSE ret END
  buildBeginAnd(int32Type);
  buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
  pred = buildAnd(buildCompare(buildArrayRef(rhs, promoted, idx, int32Type, eltType), eltType, buildArrayRef(lhs, promoted, idx, int32Type, eltType), eltType, int32Type, op), int32Type, int32Type);
  buildCaseBlockBegin(int32Type);
  buildCaseBlockIf(pred);
  buildCaseBlockThen(zero, int32Type, int32Type, false);
  buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
  buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
  // SET notDone = CASE WHEN notDone AND rhs[idx] < lhs[idx] THEN 0 ELSE notDone END
  buildCaseBlockBegin(int32Type);
  buildCaseBlockIf(pred);
  buildCaseBlockThen(zero, int32Type, int32Type, false);
  buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
  buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);

  // SET ret = CASE WHEN notDone AND lhs[idx] < rhs[idx] THEN 1 ELSE ret END
  buildBeginAnd(int32Type);
  buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
  pred = buildAnd(buildCompare(buildArrayRef(lhs, promoted, idx, int32Type, eltType), eltType, buildArrayRef(rhs, promoted, idx, int32Type, eltType), eltType, int32Type, op), int32Type, int32Type);
  buildCaseBlockBegin(int32Type);
  buildCaseBlockIf(pred);
  buildCaseBlockThen(one, int32Type, int32Type, false);
  buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
  buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
  // SET notDone = CASE WHEN notDone AND lhs[idx] < rhs[idx] THEN 0 ELSE notDone END
  buildCaseBlockBegin(int32Type);
  buildCaseBlockIf(pred);
  buildCaseBlockThen(zero, int32Type, int32Type, false);
  buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
  buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);

  // SET idx = idx + 1
  buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
  whileFinish();

  return buildRef(ret, int32Type);
}

void CodeGenerationContext::buildArrayElementwiseHash(const IQLToLLVMValue * e, 
                                                      const SequentialType * argType,
                                                      llvm::Value * previousHash,
                                                      llvm::Value * firstFlag)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Types of elements are different so elementwise iteration and conversion is necessary
  const FieldType * argEltTy = argType->getElementType();
      
  // Loop over the array and call hash between element types
  // Constants one is used frequently
  const IQLToLLVMValue * zero = buildFalse();
  const IQLToLLVMValue * one  = buildTrue();
  const IQLToLLVMValue * sz = IQLToLLVMRValue::get(this, getArraySize(e, argType), IQLToLLVMValue::eLocal);
  // INTEGER type used frequently in this method
  FieldType * int32Type = Int32Type::Get(argType->getContext());

  // DECLARE idx = 0
  // Allocate and initialize counter
  llvm::Value * allocAVal = buildEntryBlockAlloca(b->getInt32Ty(),"idx");
  const IQLToLLVMValue * counter = IQLToLLVMRValue::get(this, allocAVal, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * counterLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(counter, int32Type));
  buildSetNullableValue(counterLValue, zero, int32Type, int32Type);
        
  whileBegin();

  // while (idx < sz)
  const IQLToLLVMValue * pred = buildCompare(buildRef(counter, int32Type), int32Type, sz, int32Type, int32Type, IQLToLLVMOpLT);
  whileStatementBlock(pred, int32Type);

  const IQLToLLVMValue * idx = buildRef(counter, int32Type);
  const IQLToLLVMValue * elt = buildArrayRef(e, argType, idx, int32Type, argEltTy);
  std::vector<IQLToLLVMTypedValue> args( { IQLToLLVMTypedValue(elt, argEltTy) } );
  buildHash(args, previousHash, firstFlag);

  // SET idx = idx + 1
  buildSetNullableValue(counterLValue, buildAdd(buildRef(counter, int32Type), int32Type, one, int32Type, int32Type), int32Type, int32Type);  
  whileFinish();
}

const IQLToLLVMValue *
CodeGenerationContext::buildStructElementwiseEquals(const IQLToLLVMValue * lhs, 
                                                    const IQLToLLVMValue * rhs,
                                                    const RecordType * promoted,
                                                    const FieldType * retType)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  const IQLToLLVMValue * lhsMember = buildRowRef(lhs, promoted, 0, promoted->getElementType(0));
  const IQLToLLVMValue * rhsMember = buildRowRef(rhs, promoted, 0, promoted->getElementType(0));
  const IQLToLLVMValue * cmp = buildCompare(lhsMember, promoted->getElementType(0), rhsMember, promoted->getElementType(0), retType, IQLToLLVMOpEQ);
  for(std::size_t i=1; i<promoted->getNumElements(); ++i) {
    buildBeginAnd(retType);
    buildAddAnd(cmp, retType, retType);
    lhsMember = buildRowRef(lhs, promoted, i, promoted->getElementType(i));
    rhsMember = buildRowRef(rhs, promoted, i, promoted->getElementType(i));
    cmp = buildAnd(buildCompare(lhsMember, promoted->getElementType(i), rhsMember, promoted->getElementType(i), retType, IQLToLLVMOpEQ), retType, retType);
  }
  return cmp;
}

const IQLToLLVMValue *
CodeGenerationContext::buildStructElementwiseCompare(const IQLToLLVMValue * lhs, 
                                                     const IQLToLLVMValue * rhs,
                                                     const RecordType * promoted,
                                                     const FieldType * retType,
                                                     IQLToLLVMPredicate op)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  // Constants zero and one are used frequently
  const IQLToLLVMValue * zero = buildFalse();
  const IQLToLLVMValue * one  = buildTrue();
  // INTEGER type used frequently in this method
  FieldType * int32Type = Int32Type::Get(promoted->getContext());

  // DECLARE ret = false
  // Allocate return value and initialize to false
  const IQLToLLVMValue * ret = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(), "ret"), nullptr, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * retLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(ret, int32Type));
  buildSetNullableValue(retLValue, zero, int32Type, int32Type);
  
  // DECLARE notDone = true
  // notDone flag is a hack since I haven't implemented IF and BREAK.   Initialize to true.
  const IQLToLLVMValue * notDone = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(),"notDone"), IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * notDoneLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(notDone, int32Type));
  buildSetNullableValue(notDoneLValue, one, int32Type, int32Type);

  for(std::size_t i=0; i<promoted->getNumElements(); ++i) {
    auto memberType = promoted->getElementType(i);
    const IQLToLLVMValue * lhsMember = buildRowRef(lhs, promoted, i, memberType);
    const IQLToLLVMValue * rhsMember = buildRowRef(rhs, promoted, i, memberType);

    // SET ret = CASE WHEN notDone AND rhs[idx] < lhs[idx] THEN 0 ELSE ret END
    buildBeginAnd(int32Type);
    buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
    const IQLToLLVMValue * pred = buildAnd(buildCompare(rhsMember, memberType, lhsMember, memberType, int32Type, op), int32Type, int32Type);
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(zero, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
    // SET notDone = CASE WHEN notDone AND rhs[idx] < lhs[idx] THEN 0 ELSE notDone END
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(zero, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);

    // SET ret = CASE WHEN notDone AND lhs[idx] < rhs[idx] THEN 1 ELSE ret END
    buildBeginAnd(int32Type);
    buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
    pred = buildAnd(buildCompare(lhsMember, memberType, rhsMember, memberType, int32Type, op), int32Type, int32Type);
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(one, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
    // SET notDone = CASE WHEN notDone AND lhs[idx] < rhs[idx] THEN 0 ELSE notDone END
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(zero, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  }
  
  return buildRef(ret, int32Type);
}

const IQLToLLVMValue *
CodeGenerationContext::buildStructElementwiseCompare(const IQLToLLVMValue * lhs, 
                                                     const IQLToLLVMValue * rhs,
                                                     const FieldType * promoted,
                                                     const std::vector<const FieldType *> members,
                                                     const FieldType * retType,
                                                     IQLToLLVMPredicate op)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  // Constants zero and one are used frequently
  const IQLToLLVMValue * zero = buildFalse();
  const IQLToLLVMValue * one  = buildTrue();
  // INTEGER type used frequently in this method
  FieldType * int32Type = Int32Type::Get(promoted->getContext());

  // DECLARE ret = false
  // Allocate return value and initialize to false
  const IQLToLLVMValue * ret = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(), "ret"), nullptr, IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * retLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(ret, int32Type));
  buildSetNullableValue(retLValue, zero, int32Type, int32Type);
  
  // DECLARE notDone = true
  // notDone flag is a hack since I haven't implemented IF and BREAK.   Initialize to true.
  const IQLToLLVMValue * notDone = IQLToLLVMRValue::get(this, buildEntryBlockAlloca(b->getInt32Ty(),"notDone"), IQLToLLVMValue::eLocal);
  IQLToLLVMLocal * notDoneLValue = IQLToLLVMLocal::get(this, IQLToLLVMTypedValue(notDone, int32Type));
  buildSetNullableValue(notDoneLValue, one, int32Type, int32Type);


  // We need pointer types to access struct members so alloca and copy if needed.
  llvm::Type * structType = promoted->LLVMGetType(this);
  llvm::Value * lhsValue = lhs->getValue(this);
  llvm::Value * rhsValue = rhs->getValue(this);
  if (isValueType(promoted)) {
    llvm::Value * tmp = buildEntryBlockAlloca(structType, "lhscidrv4cmp");
    b->CreateStore(lhsValue, tmp);
    lhsValue = tmp;
    tmp = buildEntryBlockAlloca(structType, "rhscidrv4cmp");
    b->CreateStore(rhsValue, tmp);
    rhsValue = tmp;
  }

  for(std::size_t i=0; i<members.size(); ++i) {
    const IQLToLLVMValue * lhsMember = IQLToLLVMRValue::get(this, b->CreateLoad(members[i]->LLVMGetType(this), b->CreateStructGEP(structType, lhsValue, i)), IQLToLLVMValue::eLocal);
    const IQLToLLVMValue * rhsMember = IQLToLLVMRValue::get(this, b->CreateLoad(members[i]->LLVMGetType(this), b->CreateStructGEP(structType, rhsValue, i)), IQLToLLVMValue::eLocal);

    // SET ret = CASE WHEN notDone AND rhs[idx] < lhs[idx] THEN 0 ELSE ret END
    buildBeginAnd(int32Type);
    buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
    const IQLToLLVMValue * pred = buildAnd(buildCompare(rhsMember, members[i], lhsMember, members[i], int32Type, op), int32Type, int32Type);
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(zero, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
    // SET notDone = CASE WHEN notDone AND rhs[idx] < lhs[idx] THEN 0 ELSE notDone END
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(zero, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);

    // SET ret = CASE WHEN notDone AND lhs[idx] < rhs[idx] THEN 1 ELSE ret END
    buildBeginAnd(int32Type);
    buildAddAnd(buildRef(notDone, int32Type), int32Type, int32Type);
    pred = buildAnd(buildCompare(lhsMember, members[i], rhsMember, members[i], int32Type, op), int32Type, int32Type);
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(one, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(ret, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(retLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  
    // SET notDone = CASE WHEN notDone AND lhs[idx] < rhs[idx] THEN 0 ELSE notDone END
    buildCaseBlockBegin(int32Type);
    buildCaseBlockIf(pred);
    buildCaseBlockThen(zero, int32Type, int32Type, false);
    buildCaseBlockThen(buildRef(notDone, int32Type), int32Type, int32Type, false);
    buildSetNullableValue(notDoneLValue, buildCaseBlockFinish(int32Type), int32Type, int32Type);
  }
  
  return buildRef(ret, int32Type);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCompare(const IQLToLLVMValue * lhs, 
				    const FieldType * lhsType, 
				    const IQLToLLVMValue * rhs, 
				    const FieldType * rhsType,
				    llvm::Value * ret,
				    const FieldType * retType,
				    IQLToLLVMPredicate op)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  llvm::CmpInst::Predicate intOp = llvm::CmpInst::ICMP_EQ;
  llvm::CmpInst::Predicate uintOp = llvm::CmpInst::ICMP_EQ;
  llvm::CmpInst::Predicate realOp = llvm::CmpInst::FCMP_FALSE;
  switch(op) {
  case IQLToLLVMOpEQ:
    intOp = llvm::CmpInst::ICMP_EQ;
    uintOp = llvm::CmpInst::ICMP_EQ;
    realOp = llvm::CmpInst::FCMP_OEQ;
    break;
  case IQLToLLVMOpNE:
    intOp = llvm::CmpInst::ICMP_NE;
    uintOp = llvm::CmpInst::ICMP_EQ;
    realOp = llvm::CmpInst::FCMP_ONE;
    break;
  case IQLToLLVMOpGT:
    intOp = llvm::CmpInst::ICMP_SGT;
    uintOp = llvm::CmpInst::ICMP_UGT;
    realOp = llvm::CmpInst::FCMP_OGT;
    break;
  case IQLToLLVMOpGE:
    intOp = llvm::CmpInst::ICMP_SGE;
    uintOp = llvm::CmpInst::ICMP_UGE;
    realOp = llvm::CmpInst::FCMP_OGE;
    break;
  case IQLToLLVMOpLT:
    intOp = llvm::CmpInst::ICMP_SLT;
    uintOp = llvm::CmpInst::ICMP_ULT;
    realOp = llvm::CmpInst::FCMP_OLT;
    break;
  case IQLToLLVMOpLE:
    intOp = llvm::CmpInst::ICMP_SLE;
    uintOp = llvm::CmpInst::ICMP_ULE;
    realOp = llvm::CmpInst::FCMP_OLE;
    break;
  case IQLToLLVMOpRLike:
    // This will only happen with string types
    break;
  }
  // UGLY: I don't actually what type I should be promoting to since that is determined during type check
  // but not being stored in the syntax tree.  Recompute here.
  // TODO: Fix this by storing promoted type in syntax tree or rewriting the syntax tree with a cast.
  const FieldType * promoted = TypeCheckContext::leastCommonTypeNullable(lhsType, rhsType);
  lhs = buildCastNonNullable(lhs, lhsType, promoted);
  rhs = buildCastNonNullable(rhs, rhsType, promoted);
  
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  llvm::Value * e1 = lhs->getValue(this);
  llvm::Value * e2 = rhs->getValue(this);

  // TODO: I should be able to reliably get the length of fixed size fields from the FieldType
  llvm::Value * r = NULL;
  if (promoted->isIntegral() || promoted->GetEnum() == FieldType::DATE || promoted->GetEnum() == FieldType::DATETIME) {
    return buildCompareResult(b->CreateICmp(intOp, e1, e2), ret);
  } else if (promoted->GetEnum() == FieldType::IPV4) {
    return buildCompareResult(b->CreateICmp(uintOp, e1, e2), ret);
  } else if (lhsType->isFloatingPoint() || rhsType->isFloatingPoint()) {
    // TODO: Should this be OEQ or UEQ?
    // TODO: Should be comparison within EPS?
    return buildCompareResult(b->CreateFCmp(realOp, e1, e2), ret);
  } else if (promoted->GetEnum() == FieldType::VARCHAR) {
    return buildVarcharCompare(e1, e2, ret, op);
  } else if (promoted->GetEnum() == FieldType::CHAR) {

    // TODO: I don't really know what the semantics of char(N) equality are (e.g. when the sizes aren't equal).
    // FWIW, semantics of char(N) equality is that one compares assuming
    // space padding to MAX(M,N)
    // For now I am draconian and say they are never equal????
    if (lhsType->GetSize() != rhsType->GetSize()) {
      r = b->getInt32(0);
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    } else {
      llvm::Value * m = buildMemcmp(e1, FieldAddress(), e2, FieldAddress(), lhsType->GetSize());
      // Compare result to zero and return
      return buildCompareResult(b->CreateICmp(intOp, b->getInt32(0),m),
				ret);
    }
  } else if (promoted->GetEnum() == FieldType::BIGDECIMAL) {
    // Call decNumberCompare
    llvm::Value * callArgs[4];
    llvm::Function * fn = LLVMModule->getFunction("InternalDecimalCmp");
    callArgs[0] = e1;
    callArgs[1] = e2;
    callArgs[2] = buildEntryBlockAlloca(b->getInt32Ty(), "decimalCmpRetPtr");
    callArgs[3] = b->CreateLoad(LLVMDecContextPtrType, getContextArgumentRef());
    b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 4), "");
    
    // Compare result to zero and return
    return buildCompareResult(b->CreateICmp(intOp, b->getInt32(0), b->CreateLoad(b->getInt32Ty(), callArgs[2])),
			      ret);
  } else if (promoted->GetEnum() == FieldType::FIXED_ARRAY) {

    // TODO: I don't really know what the semantics of ARRAY[N] equality are (e.g. when the sizes aren't equal).
    // FWIW, semantics of char(N) equality is that one compares assuming
    // space padding to MAX(M,N)
    // For now I am draconian and say they are never equal????  
    if (lhsType->GetSize() != rhsType->GetSize()) {
      r = b->getInt32(0);
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    } else {
      const FieldType * eltType = dynamic_cast<const FixedArrayType*>(promoted)->getElementType();
      auto idx = IQLToLLVMRValue::get(this, b->getInt32(0), IQLToLLVMValue::eLocal);
      const FieldType * idxTy = Int32Type::Get(promoted->getContext(), false);
      const IQLToLLVMValue * cmp = nullptr;
      switch(op) {
      case IQLToLLVMOpEQ:
      case IQLToLLVMOpNE:
        cmp = buildCompare(buildArrayRef(lhs, promoted, idx, idxTy, eltType), eltType, buildArrayRef(rhs, promoted, idx, idxTy, eltType), eltType, retType, IQLToLLVMOpEQ);
        for(std::size_t i=1; i<promoted->GetSize(); ++i) {
          buildBeginAnd(retType);
          buildAddAnd(cmp, retType, retType);
          idx = IQLToLLVMRValue::get(this, b->getInt32(i), IQLToLLVMValue::eLocal); 
          cmp = buildAnd(buildCompare(buildArrayRef(lhs, promoted, idx, idxTy, eltType), eltType, buildArrayRef(rhs, promoted, idx, idxTy, eltType), eltType, retType, IQLToLLVMOpEQ), retType, retType);
        }
        if (IQLToLLVMOpNE == op) {
          cmp = buildNot(cmp, retType, retType);
        }
        BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
        b->CreateStore(cmp->getValue(this), ret);
        return IQLToLLVMValue::eLocal;
        break;
      case IQLToLLVMOpGT:
      case IQLToLLVMOpLE:
        cmp = buildArrayElementwiseCompare(lhs, rhs, promoted, retType, IQLToLLVMOpGT);
        if (IQLToLLVMOpLE == op) {
          cmp = buildNot(cmp, retType, retType);
        }
        BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
        b->CreateStore(cmp->getValue(this), ret);
        return IQLToLLVMValue::eLocal;
        break;
      case IQLToLLVMOpGE:
      case IQLToLLVMOpLT:
        cmp = buildArrayElementwiseCompare(lhs, rhs, promoted, retType, IQLToLLVMOpLT);
        if (IQLToLLVMOpGE == op) {
          cmp = buildNot(cmp, retType, retType);
        }
        BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
        b->CreateStore(cmp->getValue(this), ret);
        return IQLToLLVMValue::eLocal;
        break;
      }
    }
  } else if (promoted->GetEnum() == FieldType::VARIABLE_ARRAY) {
    // TODO: I don't really know what the semantics of ARRAY[N] equality are (e.g. when the sizes aren't equal)
    // I think the right answer is pad with NULLs
    const IQLToLLVMValue * cmp = nullptr;
    switch(op) {
    case IQLToLLVMOpEQ:
    case IQLToLLVMOpNE:
      cmp = buildArrayElementwiseEquals(lhs, rhs, promoted, retType);
      if (IQLToLLVMOpNE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    case IQLToLLVMOpGT:
    case IQLToLLVMOpLE:
      cmp = buildArrayElementwiseCompare(lhs, rhs, promoted, retType, IQLToLLVMOpGT);
      if (IQLToLLVMOpLE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    case IQLToLLVMOpGE:
    case IQLToLLVMOpLT:
      cmp = buildArrayElementwiseCompare(lhs, rhs, promoted, retType, IQLToLLVMOpLT);
      if (IQLToLLVMOpGE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    }
  } else if (promoted->GetEnum() == FieldType::CIDRV4) {
    std::vector<const FieldType *> memberTypes;
    memberTypes.push_back(IPv4Type::Get(promoted->getContext(), false));
    memberTypes.push_back(Int8Type::Get(promoted->getContext(), false));
    // This is written in such a way that it could be used to compare general struct types
    // (provided we know the FieldTypes of the struct members).  I hope this is paving the way
    // for me to add user defined structs to Trecul.

    const IQLToLLVMValue * cmp = nullptr;
    switch(op) {
    case IQLToLLVMOpEQ:
    case IQLToLLVMOpNE:
      {
        llvm::Type * structType = promoted->LLVMGetType(this);
        // We need pointer types to access struct members so alloca and copy if needed.
        llvm::Value * lhsValue = lhs->getValue(this);
        llvm::Value * rhsValue = rhs->getValue(this);
        if (isValueType(promoted)) {
          llvm::Value * tmp = buildEntryBlockAlloca(structType, "lhscidrv4cmp");
          b->CreateStore(lhsValue, tmp);
          lhsValue = tmp;
          tmp = buildEntryBlockAlloca(structType, "rhscidrv4cmp");
          b->CreateStore(rhsValue, tmp);
          rhsValue = tmp;
        }
        const IQLToLLVMValue * lhsMember = IQLToLLVMRValue::get(this, b->CreateLoad(memberTypes[0]->LLVMGetType(this), b->CreateStructGEP(structType, lhsValue, 0)), IQLToLLVMValue::eLocal);
        const IQLToLLVMValue * rhsMember = IQLToLLVMRValue::get(this, b->CreateLoad(memberTypes[0]->LLVMGetType(this), b->CreateStructGEP(structType, rhsValue, 0)), IQLToLLVMValue::eLocal);
        cmp = buildCompare(lhsMember, memberTypes[0], rhsMember, memberTypes[0], retType, IQLToLLVMOpEQ);
        for(std::size_t i=1; i<memberTypes.size(); ++i) {
          buildBeginAnd(retType);
          buildAddAnd(cmp, retType, retType);
          lhsMember = IQLToLLVMRValue::get(this, b->CreateLoad(memberTypes[i]->LLVMGetType(this), b->CreateStructGEP(structType, lhsValue, i)), IQLToLLVMValue::eLocal);
          rhsMember = IQLToLLVMRValue::get(this, b->CreateLoad(memberTypes[i]->LLVMGetType(this), b->CreateStructGEP(structType, rhsValue, i)), IQLToLLVMValue::eLocal);
          cmp = buildAnd(buildCompare(lhsMember, memberTypes[i], rhsMember, memberTypes[i], retType, IQLToLLVMOpEQ), retType, retType);
        }
        if (IQLToLLVMOpNE == op) {
          cmp = buildNot(cmp, retType, retType);
        }
        BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
        b->CreateStore(cmp->getValue(this), ret);
        return IQLToLLVMValue::eLocal;
      }
    case IQLToLLVMOpGT:
    case IQLToLLVMOpLE:
      cmp = buildStructElementwiseCompare(lhs, rhs, promoted, memberTypes, retType, IQLToLLVMOpGT);
      if (IQLToLLVMOpLE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    case IQLToLLVMOpGE:
    case IQLToLLVMOpLT:
      cmp = buildStructElementwiseCompare(lhs, rhs, promoted, memberTypes, retType, IQLToLLVMOpLT);
      if (IQLToLLVMOpGE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    default:
      throw std::runtime_error("CodeGenerationContext::buildCompare CIDRv4 compare not implemented");
    }
  } else if (promoted->GetEnum() == FieldType::IPV6 ||
             promoted->GetEnum() == FieldType::CIDRV6) {
    llvm::Value * m = buildMemcmp(e1, FieldAddress(), e2, FieldAddress(), promoted->GetEnum() == FieldType::IPV6 ? 16 : 17);
    // Compare result to zero and return
    return buildCompareResult(b->CreateICmp(intOp, b->getInt32(0),m), ret);
  } else if (promoted->GetEnum() == FieldType::STRUCT) {
    const RecordType * recTy = dynamic_cast<const RecordType *>(promoted);
    const IQLToLLVMValue * cmp = nullptr;
    switch(op) {
    case IQLToLLVMOpEQ:
    case IQLToLLVMOpNE:
      cmp = buildStructElementwiseEquals(lhs, rhs, recTy, retType);
      if (IQLToLLVMOpNE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
    case IQLToLLVMOpGT:
    case IQLToLLVMOpLE:
      cmp = buildStructElementwiseCompare(lhs, rhs, recTy, retType, IQLToLLVMOpGT);
      if (IQLToLLVMOpLE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    case IQLToLLVMOpGE:
    case IQLToLLVMOpLT:
      cmp = buildStructElementwiseCompare(lhs, rhs, recTy, retType, IQLToLLVMOpLT);
      if (IQLToLLVMOpGE == op) {
        cmp = buildNot(cmp, retType, retType);
      }
      BOOST_ASSERT(cmp->getValueType() == IQLToLLVMValue::eLocal);
      b->CreateStore(cmp->getValue(this), ret);
      return IQLToLLVMValue::eLocal;
      break;
    default:
      throw std::runtime_error("CodeGenerationContext::buildCompare struct type compare not implemented");
    }
  } else {
    throw std::runtime_error("CodeGenerationContext::buildCompare unexpected type");
  }
  // Copy result into provided storage.
  b->CreateStore(r, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildCompare(const IQLToLLVMValue * lhs, 
							   const FieldType * lhsType, 
							   const IQLToLLVMValue * rhs, 
							   const FieldType * rhsType,
							   const FieldType * resultType,
							   IQLToLLVMPredicate op)
{
  BinaryOperatorMemFn opFun=NULL;
  switch(op) {
  case IQLToLLVMOpEQ:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpEQ>;
    break;
  case IQLToLLVMOpNE:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpNE>;
    break;
  case IQLToLLVMOpGT:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpGT>;
    break;
  case IQLToLLVMOpGE:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpGE>;
    break;
  case IQLToLLVMOpLT:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpLT>;
    break;
  case IQLToLLVMOpLE:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpLE>;
    break;
  case IQLToLLVMOpRLike:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpRLike>;
    break;
  default:
    throw std::runtime_error("Unexpected predicate type");
  }
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, resultType, opFun);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildSubnetContains(const IQLToLLVMValue * lhs, 
                                                                  const FieldType * lhsType, 
                                                                  const IQLToLLVMValue * rhs, 
                                                                  const FieldType * rhsType,
                                                                  llvm::Value * ret,
                                                                  const FieldType * retType)
{
  if (lhsType->isIntegral()) {
    lhs = buildCast(lhs, lhsType, retType);
    rhs = buildCast(rhs, rhsType, retType);
    llvm::IRBuilder<> * b = LLVMBuilder;
    b->CreateStore(b->CreateAShr(lhs->getValue(this), rhs->getValue(this)), ret);
    return IQLToLLVMValue::eLocal;
  } else {
    auto lhsCidrV6Type = CIDRv6Type::Get(lhsType->getContext(), lhsType->isNullable());
    auto rhsCidrV6Type = CIDRv6Type::Get(lhsType->getContext(), rhsType->isNullable());
    lhs = buildCast(lhs, lhsType, lhsCidrV6Type);
    rhs = buildCast(rhs, rhsType, rhsCidrV6Type);
    lhsType = lhsCidrV6Type;
    rhsType = rhsCidrV6Type;
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsCidrV6Type);
    args.emplace_back(rhs, rhsCidrV6Type);
    return buildCall("InternalIPAddressAddrBlockMatch", args, ret, retType);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildSubnetContains(const IQLToLLVMValue * lhs, 
                                                                  const FieldType * lhsType, 
                                                                  const IQLToLLVMValue * rhs, 
                                                                  const FieldType * rhsType,
                                                                  const FieldType * resultType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, resultType, &CodeGenerationContext::buildSubnetContains);
}

const IQLToLLVMValue * CodeGenerationContext::buildSubnetContainsEquals(const IQLToLLVMValue * lhs, 
                                                                        const FieldType * lhsType, 
                                                                        const IQLToLLVMValue * rhs, 
                                                                        const FieldType * rhsType,
                                                                        const FieldType * resultType)
{
  auto lhsCidrV6Type = CIDRv6Type::Get(lhsType->getContext(), lhsType->isNullable());
  auto rhsCidrV6Type = CIDRv6Type::Get(lhsType->getContext(), rhsType->isNullable());
  lhs = buildCast(lhs, lhsType, lhsCidrV6Type);
  rhs = buildCast(rhs, rhsType, rhsCidrV6Type);
  lhsType = lhsCidrV6Type;
  rhsType = rhsCidrV6Type;
  buildBeginOr(resultType);
  buildAddOr(buildCompare(lhs, lhsType, rhs, rhsType, resultType, IQLToLLVMOpEQ), resultType, resultType);
  return buildOr(buildSubnetContains(lhs, lhsType, rhs, rhsType, resultType), resultType, resultType);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildSubnetContainedBy(const IQLToLLVMValue * lhs, 
                                                                        const FieldType * lhsType, 
                                                                        const IQLToLLVMValue * rhs, 
                                                                        const FieldType * rhsType,
                                                                        llvm::Value * ret,
                                                                        const FieldType * retType)
{
  if (lhsType->isIntegral()) {
    lhs = buildCast(lhs, lhsType, retType);
    rhs = buildCast(rhs, rhsType, retType);
    llvm::IRBuilder<> * b = LLVMBuilder;
    b->CreateStore(b->CreateShl(lhs->getValue(this), rhs->getValue(this)), ret);
    return IQLToLLVMValue::eLocal;
  } else {
    return buildSubnetContains(rhs, rhsType, lhs, lhsType, ret, retType);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildSubnetContainedBy(const IQLToLLVMValue * lhs, 
                                                                        const FieldType * lhsType, 
                                                                        const IQLToLLVMValue * rhs, 
                                                                        const FieldType * rhsType,
                                                                        const FieldType * resultType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, resultType, &CodeGenerationContext::buildSubnetContainedBy);
}

const IQLToLLVMValue * CodeGenerationContext::buildSubnetContainedByEquals(const IQLToLLVMValue * lhs, 
                                                                        const FieldType * lhsType, 
                                                                        const IQLToLLVMValue * rhs, 
                                                                        const FieldType * rhsType,
                                                                        const FieldType * resultType)
{
  return buildSubnetContainsEquals(rhs, rhsType, lhs, lhsType, resultType);
}

const IQLToLLVMValue * CodeGenerationContext::buildSubnetSymmetricContainsEquals(const IQLToLLVMValue * lhs, 
                                                                                 const FieldType * lhsType, 
                                                                                 const IQLToLLVMValue * rhs, 
                                                                                 const FieldType * rhsType,
                                                                                 const FieldType * resultType)
{
  auto lhsCidrV6Type = CIDRv6Type::Get(lhsType->getContext(), lhsType->isNullable());
  auto rhsCidrV6Type = CIDRv6Type::Get(lhsType->getContext(), rhsType->isNullable());
  lhs = buildCast(lhs, lhsType, lhsCidrV6Type);
  rhs = buildCast(rhs, rhsType, rhsCidrV6Type);
  lhsType = lhsCidrV6Type;
  rhsType = rhsCidrV6Type;
  buildBeginOr(resultType);
  buildAddOr(buildCompare(lhs, lhsType, rhs, rhsType, resultType, IQLToLLVMOpEQ), resultType, resultType);
  buildAddOr(buildSubnetContains(lhs, lhsType, rhs, rhsType, resultType), resultType, resultType);
  return buildOr(buildSubnetContains(rhs, rhsType, lhs, lhsType, resultType), resultType, resultType);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildFamily(const IQLToLLVMValue * arg, 
                                                             const FieldType * argType, 
                                                             llvm::Value * ret, 
                                                             const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  switch(argType->GetEnum()) {
  case FieldType::IPV4:
  case FieldType::CIDRV4:
    b->CreateStore(b->getInt32(4), ret);
    break;
  case FieldType::IPV6:
    {
      auto int8Type = Int8Type::Get(argType->getContext(), false);
      auto int32Type = Int32Type::Get(argType->getContext(), false);
      auto arrayTy = FixedArrayType::Get(argType->getContext(), 12, int8Type, false);
      std::vector<IQLToLLVMTypedValue> vals;
      for(std::size_t i=0; i<10; i++) {
        vals.emplace_back(IQLToLLVMRValue::get(this, b->getInt8(0), IQLToLLVMValue::eLocal), int8Type);
      }
      vals.emplace_back(IQLToLLVMRValue::get(this, b->getInt8(-1), IQLToLLVMValue::eLocal), int8Type);
      vals.emplace_back(IQLToLLVMRValue::get(this, b->getInt8(-1), IQLToLLVMValue::eLocal), int8Type);
      auto v4MappedPrefix = buildGlobalConstArray(vals, arrayTy);
      auto cmp = IQLToLLVMRValue::get(this, buildMemcmp(v4MappedPrefix->getValue(this), FieldAddress(), arg->getValue(this), FieldAddress(), 12), IQLToLLVMValue::eLocal);
      buildBeginIfThenElse(buildCompare(cmp, int32Type, buildFalse(), int32Type, int32Type, IQLToLLVMOpEQ));
      auto four = buildDecimalInt32Literal("4");
      buildElseIfThenElse();
      auto six = buildDecimalInt32Literal("6");
      b->CreateStore(buildEndIfThenElse(four, int32Type, six, int32Type, int32Type)->getValue(this), ret);
      break;
    } 
  case FieldType::CIDRV6:
    {
      // A CIDRV6 is v4mapped iff prefixLength>=96 and prefix is v4 mapped
      auto int32Type = Int32Type::Get(argType->getContext(), false);
      auto localVar = buildEntryBlockAlloca(int32Type->LLVMGetType(this), "");
      buildMasklen(arg, argType, localVar, int32Type);
      auto prefixLength = IQLToLLVMRValue::get(this, b->CreateLoad(int32Type->LLVMGetType(this), localVar), IQLToLLVMValue::eLocal);
      auto condition = buildCompare(prefixLength, int32Type, buildDecimalInt32Literal("96"), int32Type, int32Type, IQLToLLVMOpLT);
      buildBeginIf();
      b->CreateStore(buildDecimalInt32Literal("6")->getValue(this), ret);
      buildBeginElse();
      auto ipv6Type = IPv6Type::Get(argType->getContext(), argType->isNullable());
      buildFamily(arg, ipv6Type, ret, retType);
      buildEndIf(condition);
      // buildBeginIfThenElse(buildCompare(prefixLength, int32Type, buildDecimalInt32Literal("96"), int32Type, int32Type, IQLToLLVMOpLT));
      // auto six = buildDecimalInt32Literal("6");
      // buildElseIfThenElse();      
      // // A bit of a hack (though an efficient one) : we just pass a CIDRV6 as if it were an IPV6 for a v4MappedPrefix comparison
      // vals.resize(0);
      // auto ipv6Type = IPv6Type::Get(argType->getContext(), argType->isNullable());
      // vals.emplace_back(arg, ipv6Type);
      // auto ipFamily = buildCall("family", vals, retType);
      // b->CreateStore(buildEndIfThenElse(six, int32Type, ipFamily, int32Type, int32Type)->getValue(this), ret);
      break;
    }
  }
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildMasklen(const IQLToLLVMValue * arg, 
                                                              const FieldType * argType, 
                                                              llvm::Value * ret, 
                                                              const FieldType * retType)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  switch(argType->GetEnum()) {
  case FieldType::IPV4:
    b->CreateStore(b->getInt32(32), ret);
    break;
  case FieldType::CIDRV4:
    {
      llvm::Type * structType = argType->LLVMGetType(this);
      // We need pointer type to access struct members giving prefix length, so alloca and copy
      llvm::Value * cidrValue = arg->getValue(this);
      llvm::Value * tmp = buildEntryBlockAlloca(structType, "cidrv4convert");
      b->CreateStore(cidrValue, tmp);
      cidrValue = tmp;
      llvm::Value * prefix_length = b->CreateLoad(b->getInt8Ty(), b->CreateStructGEP(structType, cidrValue, 1));
      prefix_length = b->CreateZExt(prefix_length, b->getInt32Ty());
      b->CreateStore(prefix_length, ret);
      break;
    }
  case FieldType::IPV6:
    b->CreateStore(b->getInt32(128), ret);
    break;
  case FieldType::CIDRV6:
    {
      // Set prefix length in the result
      llvm::Value * gepIndexes[2];
      gepIndexes[0] = b->getInt64(0);    
      gepIndexes[1] = b->getInt64(16);
      llvm::Value * prefix_length_ptr  = b->CreateGEP(argType->LLVMGetType(this), arg->getValue(this), llvm::ArrayRef(&gepIndexes[0], 2), "prefix_length");
      llvm::Value * prefix_length = b->CreateLoad(b->getInt8Ty(), prefix_length_ptr);
      prefix_length = b->CreateZExt(prefix_length, b->getInt32Ty());
      b->CreateStore(prefix_length, ret);
      break;
    }
  }
  return IQLToLLVMValue::eLocal;
}

llvm::Value * CodeGenerationContext::buildHashInitValue(DynamicRecordContext & ctxt, llvm::Value * sz, llvm::Value * previousHash, llvm::Value * firstFlag)
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  const FieldType * int32Type = Int32Type::Get(ctxt);
  buildBeginIfThenElse(buildCompare(IQLToLLVMRValue::get(this, b->CreateLoad(b->getInt32Ty(), firstFlag), IQLToLLVMValue::eLocal),
                                    int32Type,
                                    buildTrue(),
                                    int32Type,
                                    int32Type,
                                    IQLToLLVMOpEQ));
  buildElseIfThenElse();
  llvm::Value * prev = b->CreateLoad(b->getInt32Ty(), previousHash);
  return buildEndIfThenElse(IQLToLLVMRValue::get(this, sz, IQLToLLVMValue::eLocal), int32Type,
                            IQLToLLVMRValue::get(this, prev, IQLToLLVMValue::eLocal), int32Type,
                            int32Type)->getValue(this);
}

void CodeGenerationContext::buildHash(const std::vector<IQLToLLVMTypedValue> & args, llvm::Value * previousHash, llvm::Value * firstFlag)
{
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * callArgs[3];
  llvm::Function * fn = LLVMModule->getFunction("SuperFastHash");

  for(std::size_t i=0; i<args.size(); i++) {
    llvm::Value * argVal = args[i].getValue()->getValue(this);
    llvm::Type * argTy = llvm::PointerType::get(b->getInt8Ty(), 0);
    // TODO: Not handling NULL values in a coherent way.
    // TODO: I should be able to reliably get the length of fixed size fields from the LLVM type.
    if (argVal->getType() == b->getInt8Ty()) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hash8tmp");
      b->CreateStore(argVal, tmpVal);
      llvm::Value * sz = b->getInt32(1);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (argVal->getType() == b->getInt16Ty()) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hash32tmp");
      b->CreateStore(argVal, tmpVal);
      llvm::Value * sz = b->getInt32(2);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (argVal->getType() == b->getInt32Ty() ||
	       argVal->getType() == b->getFloatTy()) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hash32tmp");
      b->CreateStore(argVal, tmpVal);
      llvm::Value * sz = b->getInt32(4);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (argVal->getType() == b->getInt64Ty() ||
	       argVal->getType() == b->getDoubleTy()) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hash64tmp");
      b->CreateStore(argVal, tmpVal);
      llvm::Value * sz = b->getInt32(8);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (args[i].getType()->GetEnum() == FieldType::CIDRV4) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hashcidrv4tmp");
      b->CreateStore(argVal, tmpVal);
      llvm::Value * sz = b->getInt32(5);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (args[i].getType()->GetEnum() == FieldType::VARCHAR) {
      llvm::Value * sz = buildVarcharGetSize(argVal);
      callArgs[0] = buildVarcharGetPtr(argVal);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (args[i].getType()->GetEnum() == FieldType::VARIABLE_ARRAY) {
      auto arrayTy = dynamic_cast<const SequentialType*>(args[i].getType());
      if (!arrayTy->getElementType()->isNullable() && FieldType::VARCHAR != arrayTy->getElementType()->GetEnum() &&
          FieldType::VARIABLE_ARRAY != arrayTy->getElementType()->GetEnum()) {
        auto eltPtrType = arrayTy->getElementType()->LLVMGetType(this)->getPointerTo(0);
        llvm::Value * sz = b->CreateMul(buildVarArrayGetSize(argVal), b->getInt32(arrayTy->getElementType()->GetSize()));
        callArgs[0] = b->CreateBitCast(buildVarArrayGetPtr(argVal, eltPtrType), argTy);
        callArgs[1] = sz;
        // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
        callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
        b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
        b->CreateStore(b->getInt32(0), firstFlag);
      } else {
        buildArrayElementwiseHash(args[i].getValue(), arrayTy, previousHash, firstFlag);
      }
    } else if (args[i].getType()->GetEnum() == FieldType::FIXED_ARRAY) {
      auto arrayTy = dynamic_cast<const SequentialType*>(args[i].getType());
      if (!arrayTy->getElementType()->isNullable() && FieldType::VARCHAR != arrayTy->getElementType()->GetEnum() &&
          FieldType::VARIABLE_ARRAY != arrayTy->getElementType()->GetEnum()) {
        llvm::Value * sz = b->getInt32(args[i].getType()->GetAllocSize());
        callArgs[0] = b->CreateBitCast(argVal, argTy);
        callArgs[1] = sz;
        // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);    
        callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
        b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
        b->CreateStore(b->getInt32(0), firstFlag);
      } else {
        buildArrayElementwiseHash(args[i].getValue(), arrayTy, previousHash, firstFlag);
      }
    } else if (args[i].getType()->GetEnum() == FieldType::STRUCT) {
      auto recTy = dynamic_cast<const RecordType*>(args[i].getType());
      for(std::size_t j=0; j<recTy->getNumElements(); ++j) {
        auto eltTy = recTy->getElementType(j);
        auto elt = buildRowRef(args[i].getValue(), recTy, j, eltTy);
        std::vector<IQLToLLVMTypedValue> hashArgs( { IQLToLLVMTypedValue(elt, eltTy) } );
        buildHash(hashArgs, previousHash, firstFlag);
      }
    } else if (args[i].getType()->GetEnum() == FieldType::CHAR) {
      // Don't has on trailing null terminator in char type to maintain
      // consistency with varchar hashing.
      unsigned arrayLen = args[i].getType()->GetSize();
      llvm::Value * sz = b->getInt32(arrayLen);
      callArgs[0] = b->CreateBitCast(argVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);    
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (args[i].getType()->GetEnum() == FieldType::BIGDECIMAL ||
               args[i].getType()->GetEnum() == FieldType::IPV6) {
      llvm::Value * sz = b->getInt32(16);
      callArgs[0] = b->CreateBitCast(argVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else if (args[i].getType()->GetEnum() == FieldType::CIDRV6) {
      llvm::Value * sz = b->getInt32(17);
      callArgs[0] = b->CreateBitCast(argVal, argTy);
      callArgs[1] = sz;
      // callArgs[2] = i==0 ? sz : b->CreateLoad(b->getInt32Ty(), previousHash);
      callArgs[2] = buildHashInitValue(args[i].getType()->getContext(), sz, previousHash, firstFlag);
      b->CreateStore(b->CreateCall(fn->getFunctionType(), fn, llvm::ArrayRef(&callArgs[0], 3), "hash"), previousHash);
      b->CreateStore(b->getInt32(0), firstFlag);
    } else {
      throw std::runtime_error("CodeGenerationContext::buildHash unexpected type");
    }
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildHash(const std::vector<IQLToLLVMTypedValue> & args)
{
  // Save the previous hash so we can feed it into the next.  
  llvm::Value * previousHash=buildEntryBlockAlloca(LLVMBuilder->getInt32Ty(), "buildHashRet");
  llvm::Value * firstFlag=buildEntryBlockAlloca(LLVMBuilder->getInt32Ty(), "buildHashFirstFlag");
  // Set random number as the hash of an single empty array (or empty row)
  LLVMBuilder->CreateStore(LLVMBuilder->getInt32(9234), previousHash);
  LLVMBuilder->CreateStore(LLVMBuilder->getInt32(1), firstFlag);
  buildHash(args, previousHash, firstFlag);
  return IQLToLLVMRValue::get(this, LLVMBuilder->CreateLoad(LLVMBuilder->getInt32Ty(), previousHash), IQLToLLVMValue::eLocal);
}

// TODO: Handle all types.
// TODO: Handle case of TINYINT, SMALLINT, char[N] for N < 4 and BOOLEAN which don't
// fully utilize the prefix.
// TODO: Use FieldType as necessary....
const IQLToLLVMValue * CodeGenerationContext::buildSortPrefix(const IQLToLLVMValue * arg, 
							      const FieldType * argTy)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * argVal = arg->getValue(this);
  llvm::Value * retVal = NULL;
  // 31 bits for the prefix
  static const int32_t prefixBits = 31;
  if (argVal->getType() == b->getInt32Ty()) {
    // For signed integer quantity, reverse top bit then shift down to get 31 bits
    // of precision.
    llvm::Value * offsetVal = 
      b->CreateXor(argVal, b->getInt32(0x80000000), "int32PrefixOffset");
    retVal = b->CreateLShr(offsetVal, b->getInt32(32-prefixBits), "int32Prefix");
  } else if (argVal->getType() == b->getInt64Ty()) {
    // For signed integer quantity, reverse top bit then shift down to get 31 bits
    // of precision.
    llvm::Value * offsetVal = 
      b->CreateXor(argVal, b->getInt64(0x8000000000000000LL), "int64PrefixOffset");
    retVal = b->CreateLShr(offsetVal, b->getInt64(64-prefixBits), "int64PrefixAs64");
    retVal = b->CreateTrunc(retVal, b->getInt32Ty(), "int64Prefix");
  } else if (argTy->GetEnum() == FieldType::CHAR ||
             argTy->GetEnum() == FieldType::IPV6 ||
             argTy->GetEnum() == FieldType::CIDRV6) {
    // Take top 32 bits of array, byte swap into a uint32_t and then divide by 2
    // TODO: If there are less than 32 bits in the array then there is room
    // for parts of other field(s) in the prefix...
    retVal = b->getInt32(0);
    int numBytes = argTy->GetSize() >= 4 ? 
      4 : (int) argTy->GetSize();
    for(int i = 0; i<numBytes; ++i) {
      llvm::Value * tmp = b->CreateLoad(b->getInt8Ty(),
                                        b->CreateConstInBoundsGEP2_64(argTy->LLVMGetType(this), argVal, 0, i));
      tmp = b->CreateShl(b->CreateZExt(tmp, b->getInt32Ty()),
			 b->getInt32(8*(3-i)));
      retVal = b->CreateOr(retVal, tmp, "a");
    }
    retVal = b->CreateLShr(retVal, b->getInt32(32-prefixBits));
  } else if (argTy->GetEnum() == FieldType::STRUCT) {
    auto recTy = dynamic_cast<const RecordType*>(argTy);
    auto eltTy = recTy->getElementType(0);
    auto elt = buildRowRef(arg, recTy, 0, eltTy);    
    return buildSortPrefix(elt, eltTy);
  } else {
    // TODO: This is suboptimal but allows sorting to work.
    // TODO: Handle other types...
    retVal = b->getInt32(1);
  }

  return IQLToLLVMRValue::get(this, retVal, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildSortPrefix(const std::vector<IQLToLLVMTypedValue> & args,
							      const FieldType * retTy)
{
  // TODO: Assume for the moment that one field generates
  // the entire prefix.
  const IQLToLLVMValue * arg = args[0].getValue();
  const FieldType * argTy = args[0].getType();
  if (arg->getNull(this)) {
    const IQLToLLVMValue * zero = 
      IQLToLLVMRValue::get(this, 
                           retTy->getZero(this),
                           IQLToLLVMValue::eLocal);
    buildCaseBlockBegin(retTy);
    buildCaseBlockIf(buildIsNull(arg));
    buildCaseBlockThen(zero, retTy, retTy, false);
    const IQLToLLVMValue * p = buildSortPrefix(arg, argTy);
    buildCaseBlockThen(p, retTy, retTy, false);
    return buildCaseBlockFinish(retTy);
  } else {
    return buildSortPrefix(arg, argTy);
  }
}

void CodeGenerationContext::buildReturnValue(const IQLToLLVMValue * iqlVal, const FieldType * retType)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Due to our uniform implementation of mutable variables, our return location is actually
  // a pointer to a pointer (should be pointer to retType)
  llvm::Value * locOfLoc = lookup("__ReturnValue__")->getValuePointer(this)->getValue(this);
  llvm::Value * loc = b->CreateLoad(llvm::PointerType::get(retType->LLVMGetType(this), 0), locOfLoc);

  llvm::Value * llvmVal = iqlVal->getValue(this);

  llvm::BasicBlock * mergeBB = NULL;
  llvm::Value * nv = iqlVal->getNull(this);
  if (nv) {
    // We are only prepared to deal with NULLABLE int right now
    // which in fact is only return boolean values.  For backward
    // compatibility we return 0 for NULL.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    if (llvmVal->getType() != b->getInt32Ty()) {
      throw std::runtime_error("Only supporting return of nullable values for INTEGERs");
    }
    llvm::BasicBlock * nullBB = llvm::BasicBlock::Create(*c, "retNullBlock", f);
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "retNotNullBlock", f);
    mergeBB = llvm::BasicBlock::Create(*c, "retMergeBlock", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, nullBB);
    b->SetInsertPoint(nullBB);
    b->CreateStore(b->getInt32(0), loc);
    b->CreateBr(mergeBB);
    b->SetInsertPoint(notNullBB);
  }

  // Expressions are either by value or by reference.  For reference types
  // we load before store (maybe we should use the memcpy intrinsic instead).
  if (isValueType(retType)) {
    b->CreateStore(llvmVal, loc); 
  } else {
    llvm::Value * val = b->CreateLoad(retType->LLVMGetType(this), llvmVal);
    b->CreateStore(val, loc);
  }
  // The caller expects a BB into which it can insert LLVMBuildRetVoid.
  if (mergeBB) {
    llvm::IRBuilder<> * b = LLVMBuilder;
    b->CreateBr(mergeBB);
    b->SetInsertPoint(mergeBB);
  }
}

void CodeGenerationContext::buildBeginIf()
{
  // TODO: Use method CodeGenerationContext::conditionalBranch as it supports ternary logic (e.g. NULL condition)
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the then and else cases.  Insert the 'then' block at the
  // end of the function.
  IQLStack.push(new IQLToLLVMStackRecord());
  IQLStack.top()->StartBB = b->GetInsertBlock();
  IQLStack.top()->ThenBB = llvm::BasicBlock::Create(*c, "then", TheFunction);
  IQLStack.top()->ElseBB = nullptr;
  IQLStack.top()->MergeBB = llvm::BasicBlock::Create(*c, "ifcont");
  b->SetInsertPoint(IQLStack.top()->ThenBB);  
}

void CodeGenerationContext::buildBeginElse()
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  BOOST_ASSERT(IQLStack.top()->ElseBB == nullptr);
  // Unconditional branch from end of then block to the continue block.  
  b->CreateBr(IQLStack.top()->MergeBB);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the else case.  Insert the 'else' block at the
  // end of the function.
  IQLStack.top()->ElseBB = llvm::BasicBlock::Create(*c, "else", TheFunction);
  b->SetInsertPoint(IQLStack.top()->ElseBB);
}

void CodeGenerationContext::buildEndIf(const IQLToLLVMValue * condVal)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;

  // Unconditional branch from then/else to continue
  b->CreateBr(IQLStack.top()->MergeBB);

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // We saved the original BB in StartBB.   Now that we know if we have an else block
  // we can emit the conditional branch into this block
  b->SetInsertPoint(IQLStack.top()->StartBB);  

  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(this),
					  b->getInt32(0),
					  "boolCast");

  // Branch and set block (either else block or directly to continue)
  b->CreateCondBr(boolVal, IQLStack.top()->ThenBB, nullptr != IQLStack.top()->ElseBB ? IQLStack.top()->ElseBB : IQLStack.top()->MergeBB);

  // Resume emitting into contBlock
  TheFunction->insert(TheFunction->end(), IQLStack.top()->MergeBB);
  b->SetInsertPoint(IQLStack.top()->MergeBB);

  // Done with the stack record
  delete IQLStack.top();
  IQLStack.pop();
}

void CodeGenerationContext::buildBeginIfThenElse(const IQLToLLVMValue * condVal)
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the then and else cases.  Insert the 'then' block at the
  // end of the function.
  IQLStack.push(new IQLToLLVMStackRecord());
  IQLStack.top()->StartBB = b->GetInsertBlock();
  IQLStack.top()->ThenBB = llvm::BasicBlock::Create(*c, "then", TheFunction);
  IQLStack.top()->ElseBB = llvm::BasicBlock::Create(*c, "else");
  IQLStack.top()->MergeBB = llvm::BasicBlock::Create(*c, "ifcont");

  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(this),
					  b->getInt32(0),
					  "boolCast");

  // Branch and set block
  b->CreateCondBr(boolVal, IQLStack.top()->ThenBB, IQLStack.top()->ElseBB);
  // Emit then value.
  b->SetInsertPoint(IQLStack.top()->ThenBB);  
}

void CodeGenerationContext::buildElseIfThenElse()
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // We saved block.
  b->CreateBr(IQLStack.top()->MergeBB);
  // Codegen of 'Then' can change the current block, update ThenBB for the PHI.
  IQLStack.top()->ThenBB = b->GetInsertBlock();
  
  // Emit else block.
  TheFunction->insert(TheFunction->end(), IQLStack.top()->ElseBB);
  b->SetInsertPoint(IQLStack.top()->ElseBB);
}

const IQLToLLVMValue * CodeGenerationContext::buildEndIfThenElse(const IQLToLLVMValue * thenVal, const FieldType * thenTy,
								 const IQLToLLVMValue * elseVal, const FieldType * elseTy,
								 const FieldType * retTy)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  b->CreateBr(IQLStack.top()->MergeBB);
  // Codegen of 'Else' can change the current block, update ElseBB for the PHI.
  IQLStack.top()->ElseBB = b->GetInsertBlock();
  
  // It is possible for the branches to differ with respect to their
  // locality.  If one side is local then we have to make a local
  // copy a global value.
  if (thenVal->getValueType() != elseVal->getValueType()) {
    // TODO: Handle this case by making a local copy of a global.
    throw std::runtime_error("Internal Error: If then else with a mix of local and global values not handled");
  }


  // Emit merge block.
  TheFunction->insert(TheFunction->end(), IQLStack.top()->MergeBB);
  b->SetInsertPoint(IQLStack.top()->MergeBB);

  // Perform conversion in the merge block
  thenVal = buildCast(thenVal, thenTy, retTy);
  elseVal = buildCast(elseVal, elseTy, retTy);
  llvm::Value * e1 = thenVal->getValue(this);
  llvm::Value * e2 = elseVal->getValue(this);

  llvm::PHINode *PN = b->CreatePHI(retTy->LLVMGetType(this), 2, "iftmp");

  PN->addIncoming(e1, IQLStack.top()->ThenBB);
  PN->addIncoming(e2, IQLStack.top()->ElseBB);

  // Done with these blocks. Pop the stack of blocks.
  delete IQLStack.top();
  IQLStack.pop();

  return IQLToLLVMRValue::get(this,
                              PN,
                              thenVal->getValueType());
}

void CodeGenerationContext::buildBeginSwitch()
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;

  // Create a new builder for switch
  IQLSwitch.push(new IQLToLLVMSwitchRecord());
  IQLSwitch.top()->Top = b->GetInsertBlock();
  IQLSwitch.top()->Exit = llvm::BasicBlock::Create(*c, "switchEnd");
}

void CodeGenerationContext::buildEndSwitch(const IQLToLLVMValue * switchExpr)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // Get switch builder and pop.
  IQLToLLVMSwitchRecord * s = IQLSwitch.top();
  IQLSwitch.pop();

  // Emit and switch expr instuctions (e.g. loads) into Top basic block
  b->SetInsertPoint(s->Top);
  llvm::Value * v = switchExpr->getValue(this);
  
  // Add the switch statement to Top and add cases
  llvm::SwitchInst * si = llvm::SwitchInst::Create(v,
						   s->Exit, 
						   s->Cases.size(),
						   s->Top);
  typedef std::vector<std::pair<llvm::ConstantInt*, 
                                llvm::BasicBlock*> > CaseVec;
  for(CaseVec::iterator it = s->Cases.begin();
      it != s->Cases.end();
      ++it) {
    si->addCase(it->first, it->second);
  }

  // Set builder to exit block and declare victory.
  TheFunction->insert(TheFunction->end(), s->Exit);
  b->SetInsertPoint(s->Exit);

  delete s;
}

void CodeGenerationContext::buildBeginSwitchCase(const char * caseVal)
{
  // Unwrap to C++
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // Add block and set as insertion point
  llvm::BasicBlock * bb = llvm::BasicBlock::Create(*c, "switchCase");;
  llvm::ConstantInt * llvmConst = llvm::ConstantInt::get(b->getInt32Ty(),
							 caseVal,
							 10);
  IQLSwitch.top()->Cases.push_back(std::make_pair(llvmConst, bb));

  TheFunction->insert(TheFunction->end(), bb);
  b->SetInsertPoint(bb);
}

void CodeGenerationContext::buildEndSwitchCase()
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Get switch builder for exit block
  IQLToLLVMSwitchRecord * s = IQLSwitch.top();
  // Unconditional branch to exit block (implicit break).
  b->CreateBr(s->Exit);
}

const IQLToLLVMValue * CodeGenerationContext::buildInterval(const char * intervalType,
							    const  IQLToLLVMValue * e)
{
  // Right now we are storing as INT32 and type checking is enforcing that.
  return e;
}

const IQLToLLVMValue * CodeGenerationContext::buildDateLiteral(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Strip quotes
  std::string str(val);
  str = str.substr(1, str.size()-2);
  boost::gregorian::date d = boost::gregorian::from_string(str);
  BOOST_STATIC_ASSERT(sizeof(boost::gregorian::date) == sizeof(int32_t));
  int32_t int32Date = *reinterpret_cast<int32_t *>(&d);

  return IQLToLLVMRValue::get(this, 
                              b->getInt32(int32Date),
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDatetimeLiteral(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Strip quotes
  std::string str(val);
  str = str.substr(1, str.size()-2);
  BOOST_STATIC_ASSERT(sizeof(boost::posix_time::ptime) == sizeof(int64_t));
  int64_t int64Date = 0;
  if (str.size() == 10) {
    boost::posix_time::ptime t(boost::gregorian::from_string(str));
    int64Date = *reinterpret_cast<int64_t *>(&t);
  } else {
    boost::posix_time::ptime t = boost::posix_time::time_from_string(str);
    int64Date = *reinterpret_cast<int64_t *>(&t);
  }
  return IQLToLLVMRValue::get(this, 
                              b->getInt64(int64Date),
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDecimalInt32Literal(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  return IQLToLLVMRValue::get(this, 
                              llvm::ConstantInt::get(b->getInt32Ty(), llvm::StringRef(val), 10),
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDecimalInt64Literal(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Peel off the LL suffix
  std::string lit(val);
  lit = lit.substr(0, lit.size()-2);
  return IQLToLLVMRValue::get(this, 
                              llvm::ConstantInt::get(b->getInt64Ty(), llvm::StringRef(lit.c_str()), 10), 
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDoubleLiteral(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  return IQLToLLVMRValue::get(this, 
                              llvm::ConstantFP::get(b->getDoubleTy(), llvm::StringRef(val)), 
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildVarcharLiteral(const char * val)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  // Strip off quotes
  // TODO: Proper unquotification
  int32_t len = strlen(val);
  BOOST_ASSERT(len >= 2);
  
  std::string str(val);
  str = str.substr(1, str.size()-2);
  // Unescape stuff
  boost::replace_all(str, "\\\\", "\\");
  boost::replace_all(str, "\\b", "\b");
  boost::replace_all(str, "\\t", "\t");
  boost::replace_all(str, "\\n", "\n");
  boost::replace_all(str, "\\f", "\f");
  boost::replace_all(str, "\\r", "\r");
  boost::replace_all(str, "\\'", "'");

  llvm::Type * int8Ty = b->getInt8Ty();
  if (str.size() < Varchar::MIN_LARGE_STRING_SIZE) {
    // Create type for small varchar
    llvm::Type * varcharMembers[2];
    varcharMembers[0] = int8Ty;
    varcharMembers[1] = llvm::ArrayType::get(int8Ty,
					     sizeof(VarcharLarge) - 1);
    llvm::StructType * smallVarcharTy =
      llvm::StructType::get(*c, llvm::ArrayRef(&varcharMembers[0], 2), false);
    // Create the global
    llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*LLVMModule, 
								smallVarcharTy,
								false, llvm::GlobalValue::ExternalLinkage, 0, "smallVarcharLiteral");    
    // Initialize it
    // TODO: Encapsulate magic computation here of 2*sz which puts
    // the size into upper 7 bits and 0 into low bit.
    std::size_t sz = 2*str.size();
    llvm::Constant * constMembers[2];
    constMembers[0] = llvm::ConstantInt::get(int8Ty, sz, true);
    llvm::Constant * arrayMembers[sizeof(VarcharLarge)-1];
    for(std::size_t i=0; i<str.size(); i++) {
      arrayMembers[i] = llvm::ConstantInt::get(int8Ty, str[i], true);
    }
    for(std::size_t i=str.size(); i < sizeof(VarcharLarge)-1; i++) {
      arrayMembers[i] = llvm::ConstantInt::get(int8Ty, 0, true);
    }
    constMembers[1] = 
      llvm::ConstantArray::get(llvm::ArrayType::get(int8Ty, sizeof(VarcharLarge) - 1), 
			       llvm::ArrayRef(&arrayMembers[0], sizeof(VarcharLarge)-1));
    llvm::Constant * globalVal = llvm::ConstantStruct::get(smallVarcharTy, llvm::ArrayRef(&constMembers[0], 2));
    globalVar->setInitializer(globalVal);
    // Cast to varchar type
    llvm::Value * val = 
      llvm::ConstantExpr::getBitCast(globalVar, llvm::PointerType::get(LLVMVarcharType, 0));
    return IQLToLLVMRValue::get(this, val, IQLToLLVMValue::eLocal);
  } else {
    // TODO: Remember the string is stored as a global so that we don't create it multiple times...
    // Put the string in as a global variable and then create a struct that references it.  
    // This is the global variable itself
    llvm::Type * globalVarTy = llvm::ArrayType::get(int8Ty, str.size() + 1);
    llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*LLVMModule, 
								globalVarTy,
								false, llvm::GlobalValue::ExternalLinkage, 0, "str");
    // This is the string value.  Set it as an initializer.
    llvm::Constant * constStr = llvm::ConstantDataArray::getString(*c,
								   llvm::StringRef(str.c_str(), str.size()),
								   true);
    globalVar->setInitializer(constStr);
    
    // Now to reference the global we have to use a const GEP.  To pass
    // by value into the copy method we have to create a stack variable
    // in order to get an address.
    uint32_t sz = (uint32_t) str.size();
    // Flip the bit to make this a Large model string.
    sz = 2*sz + 1;
    llvm::Constant * constStructMembers[3];
    constStructMembers[0] = llvm::ConstantInt::get(b->getInt32Ty(), sz, true);
    // Padding for alignment
    constStructMembers[1] = llvm::ConstantInt::get(b->getInt32Ty(), 0, true);
    llvm::Constant * constGEPIndexes[2];
    constGEPIndexes[0] = llvm::ConstantInt::get(b->getInt64Ty(), llvm::StringRef("0"), 10);
    constGEPIndexes[1] = llvm::ConstantInt::get(b->getInt64Ty(), llvm::StringRef("0"), 10);
    // The pointer to string
    constStructMembers[2] = llvm::ConstantExpr::getGetElementPtr(globalVarTy, globalVar, llvm::ArrayRef(&constGEPIndexes[0], 2));
    llvm::Constant * globalString = llvm::ConstantStruct::getAnon(*c, llvm::ArrayRef(&constStructMembers[0], 3), false);
    llvm::Value * globalStringAddr = buildEntryBlockAlloca(LLVMVarcharType, "globalliteral");
    b->CreateStore(globalString, globalStringAddr);
    
    return IQLToLLVMRValue::get(this, globalStringAddr, IQLToLLVMValue::eGlobal);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildDecimalLiteral(const char * val)
{
  llvm::LLVMContext * c = LLVMContext;
  llvm::IRBuilder<> * b = LLVMBuilder;
  decimal128 dec;
  decContext decCtxt;
  decContextDefault(&decCtxt, DEC_INIT_DECIMAL128); // no traps, please
  decimal128FromString(&dec, val, &decCtxt);

  // This is the global variable itself
  llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*LLVMModule, 
                                                              LLVMDecimal128Type,
                                                              false, llvm::GlobalValue::ExternalLinkage, 0, val);
  // The value to initialize the global
  llvm::Constant * constStructMembers[4];
  for(int i=0 ; i<4; i++) {
    constStructMembers[i] = llvm::ConstantInt::get(b->getInt32Ty(), ((int32_t *) &dec)[i], true);
  }
  llvm::Constant * globalVal = llvm::ConstantStruct::getAnon(*c, llvm::ArrayRef(&constStructMembers[0], 4), true);
  globalVar->setInitializer(globalVal);

  return IQLToLLVMRValue::get(this, 
                              globalVar,
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildIPv4Literal(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  auto bytesVal = boost::asio::ip::make_address_v4(val).to_bytes();
  auto intVal = *((int32_t *) &bytesVal);
  return IQLToLLVMRValue::get(this, b->getInt32(intVal), IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildIPv6Literal(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::ArrayType * arrayTy = llvm::ArrayType::get(llvm::Type::getInt8Ty(*LLVMContext), 16);
  auto bytesVal = boost::asio::ip::make_address_v6(val).to_bytes();

  // This is the global variable itself
  llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*LLVMModule, 
                                                              arrayTy,
                                                              false, llvm::GlobalValue::ExternalLinkage, 0, val);
  // The value to initialize the global
  llvm::Constant * constArrayMembers[16];
  for(int i=0 ; i<16; i++) {
    constArrayMembers[i] = llvm::ConstantInt::get(b->getInt8Ty(), bytesVal[i], true);
  }
  llvm::Constant * globalVal = llvm::ConstantArray::get(arrayTy, llvm::ArrayRef(&constArrayMembers[0], 16));
  globalVar->setInitializer(globalVal);

  return IQLToLLVMRValue::get(this, 
                              globalVar,
                              IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildTrue()
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * t = b->getInt32(1);
  return IQLToLLVMRValue::get(this, t, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildFalse()
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * t = b->getInt32(0);
  return IQLToLLVMRValue::get(this, t, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildNull()
{
  llvm::IRBuilder<> * b = LLVMBuilder;
  llvm::Value * t = b->getInt1(0);
  llvm::Value * tmp = NULL;
  return IQLToLLVMRValue::get(this, tmp, t, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildLiteralCast(const char * val,
							       const char * typeName)
{
  if (boost::algorithm::iequals("date", typeName)) {
    return buildDateLiteral(val);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }  
}

void CodeGenerationContext::beginAggregateFunction()
{
  saveAggregateContext(&Transfer);
  restoreAggregateContext(&Update);
}

const IQLToLLVMValue * CodeGenerationContext::buildAggregateFunction(const char * fn,
								     const IQLToLLVMValue * e,
                                                                     const FieldType * exprTy,
								     const FieldType * retTy)
{
  // Name of variable of this function
  std::string aggFn = (boost::format("__AggFn%1%__") % AggFn).str();
  // Take the return value and increment the aggregate variable
  // in the update context

  // TODO: Get the aggregate function instance
  // from the symbol table.
  std::shared_ptr<AggregateFunction> agg = AggregateFunction::get(fn);
  agg->update(this, aggFn, e, exprTy, retTy);
  // Move temporarily to the initialization context and provide init
  // for the aggregate variable
  saveAggregateContext(&Update);
  restoreAggregateContext(&Initialize);

  int saveIsIdentity = IsIdentity;
  buildSetAggregateField(agg->initialize(this, retTy));
  IsIdentity = saveIsIdentity;

  // Shift back to transfer context and return a reference to the
  // aggregate variable corresponding to this aggregate function.
  saveAggregateContext(&Initialize);
  restoreAggregateContext(&Transfer);

  return buildVariableRef(aggFn.c_str(), retTy);
}

void CodeGenerationContext::buildSetField(int * pos, const IQLToLLVMValue * val)
{
  const RecordType * outputRecord = unwrap(IQLOutputRecord);
  std::string memberName = outputRecord->GetMember(*pos).GetName();
  FieldAddress outputAddress = outputRecord->getFieldAddress(memberName);
  const FieldType * fieldType = outputRecord->GetMember(*pos).GetType();
  IQLToLLVMField fieldLVal(this, outputRecord, memberName, "__OutputPointer__");

  *pos += 1;

  // Track whether this transfer is an identity or not.
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(IQLRecordArguments));
  if (recordTypes.size() == 1 && 
      recordTypes.begin()->second.second->size() == outputRecord->size()) {
    // TODO: Verify that the types are structurally the same.
    const std::string& inputArg(recordTypes.begin()->second.first);
    const RecordType * inputType = recordTypes.begin()->second.second;
    // Now check whether we are copying a field directly from input
    // to output at exactly the same offset/address
    llvm::Value * llvmVal = val->getValue(this);
    if (NULL == llvmVal) {
      IsIdentity = false;
    } else {
      if (llvm::LoadInst * load = llvm::dyn_cast<llvm::LoadInst>(llvmVal)) {
	llvmVal = load->getOperand(0);
      }
      FieldAddress inputAddress;
      llvm::Value * inputBase = lookupBasePointer(inputArg.c_str())->getValue(this);
      if (!inputType->isMemberPointer(this, llvmVal, inputBase, inputAddress) ||
	  inputAddress != outputAddress) {    
	IsIdentity = false;
      }
    }
  } else {
    IsIdentity = false;
  }

  buildSetNullableValue(&fieldLVal, val, fieldType, false, true);
}

void CodeGenerationContext::buildSetAggregateField(const IQLToLLVMValue * val)
{
  buildSetField(&AggFn, val);
}

void CodeGenerationContext::buildSetFields(const char * recordName, int * pos)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = LLVMBuilder;
  typedef std::map<std::string, std::pair<std::string, const RecordType *> > RecordArgs;
  // Copy all fields from the source record to the output.
  const RecordArgs& recordTypes(*unwrap(IQLRecordArguments));
  // Find the record struct in the named inputs.
  RecordArgs::const_iterator it = recordTypes.find(recordName);
  if (it == recordTypes.end()) {
    std::string namedInputs;
    for(RecordArgs::const_iterator argIt = recordTypes.begin();
	argIt != recordTypes.end();
	++argIt) {
      if (namedInputs.size()) namedInputs += ", ";
      namedInputs += argIt->first;
    }
    throw std::runtime_error((boost::format("Undefined input record: %1%."
					    " Defined records: %2%.") % 
			      recordName %
			      namedInputs).str());
  }
  // If move semantics specified then we can just reduce this to a
  // couple of memcpy's and memset's.  Replace all source fields in the symbol table with
  // corresponding targets.
  if(IQLMoveSemantics) {
    RecordTypeMove mv(it->second.second, unwrap(IQLOutputRecord));
    for(std::vector<MemcpyOp>::const_iterator opit = mv.getMemcpy().begin();
	opit != mv.getMemcpy().end();
	++opit) {
      // TODO: How do I actually know what input argument these are bound to?  It should be resolved
      // by the above query to IQLInputRecords.
      buildMemcpy(it->second.first,
                  opit->mSourceOffset,
                  "__OutputPointer__",
                  opit->mTargetOffset,
                  opit->mSize);
    }
    for(std::vector<MemsetOp>::const_iterator opit = mv.getMemset().begin();
	opit != mv.getMemset().end();
	++opit) {
      buildMemset(b->CreateLoad(b->getPtrTy(), lookupBasePointer(it->second.first.c_str())->getValue(this)),
                  opit->mSourceOffset,
                  0,
                  opit->mSize);
    }
    
    // For each input member moved, get address of the corresponding field in
    // the target but don't put into symbol table.  
    // TODO: Any expressions that reference these values will be broken now!
    // TODO: If the target does not have the same name as the source then
    // we have a bad situation since the source has now been cleared.
    // It is probably better to create a new basic block at the end of
    // the function and put the memset stuff there.
    for(RecordType::const_member_iterator mit = it->second.second->begin_members();
	mit != it->second.second->end_members();
	++mit) {
      const RecordType * out = unwrap(IQLOutputRecord);
      std::string memberName = out->GetMember(*pos).GetName();
      out->LLVMMemberGetPointer(memberName, 
				this, 
				lookupBasePointer("__OutputPointer__")->getValue(this),
				false
				);

      *pos += 1;
    }
  } else {    
    buildSetFieldsRegex(it->second.first,
			it->second.second, ".*", "", recordName, pos);
  }
}

void CodeGenerationContext::buildQuotedId(const char * quotedId, const char * rename, int * pos)
{
  // Strip off backticks.
  std::string strExpr(quotedId);
  strExpr = strExpr.substr(1, strExpr.size() - 2);
  std::string renameExpr(rename ? rename : "``");
  renameExpr = renameExpr.substr(1, renameExpr.size() - 2);
  typedef std::map<std::string, std::pair<std::string, const RecordType *> > RecordArgs;
  const RecordArgs& recordTypes(*unwrap(IQLRecordArguments));
  for(RecordArgs::const_iterator it = recordTypes.begin();
      it != recordTypes.end();
      ++it) {
    buildSetFieldsRegex(it->second.first,
			it->second.second, strExpr, renameExpr, "", pos);
  }
}

void CodeGenerationContext::ConstructFunction(const std::string& funName, const std::vector<std::string>& recordArgs)
{
  ConstructFunction(funName, recordArgs, llvm::Type::getVoidTy(*this->LLVMContext));
}

void CodeGenerationContext::ConstructFunction(const std::string& funName, 
				 const std::vector<std::string>& recordArgs,
				 llvm::Type * retType)
{
  // Setup LLVM access to our external structure.  Here we just set a char* pointer and we manually
  // create typed pointers to offsets/members.  We could try to achieve the same effect with
  // defining a struct but it isn't exactly clear how alignment rules in the LLVM data layout might
  // make this difficult.
  std::vector<const char *> argumentNames;
  std::vector<llvm::Type *> argumentTypes;
  for(std::vector<std::string>::const_iterator it = recordArgs.begin();
      it != recordArgs.end();
      ++it) {
    argumentNames.push_back(it->c_str());
    argumentTypes.push_back(llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0));
  }
  // If we have a non-void ret type then add it as a special out param
  if (retType != llvm::Type::getVoidTy(*this->LLVMContext)) {
    argumentNames.push_back("__ReturnValue__");
    argumentTypes.push_back(llvm::PointerType::get(retType, 0));
  }
  // Every Function takes the decContext pointer as a final argument
  argumentNames.push_back("__DecimalContext__");
  argumentTypes.push_back(this->LLVMDecContextPtrType);
  llvm::FunctionType * funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext),
						       llvm::ArrayRef(&argumentTypes[0], argumentTypes.size()),
						       0);
  this->LLVMFunction = llvm::Function::Create(funTy, llvm::GlobalValue::ExternalLinkage, funName.c_str(), this->LLVMModule);
  llvm::BasicBlock * entryBlock = llvm::BasicBlock::Create(*this->LLVMContext, "EntryBlock", this->LLVMFunction);
  this->LLVMBuilder->SetInsertPoint(entryBlock);
  for(unsigned int i = 0; i<argumentNames.size(); i++) {
    llvm::Value * allocAVal = this->buildEntryBlockAlloca(argumentTypes[i], argumentNames[i]);
    llvm::Value * arg = &this->LLVMFunction->arg_begin()[i];
    this->defineVariable(argumentNames[i], allocAVal,
			     NULL, NULL, NULL, IQLToLLVMValue::eGlobal);
    // Set names on function arguments
    arg->setName(argumentNames[i]);
    // Store argument in the alloca 
    this->LLVMBuilder->CreateStore(arg, allocAVal);
  }  
}

void CodeGenerationContext::ConstructFunction(const std::string& funName, 
                                 const std::vector<std::string> & argumentNames,
                                 const std::vector<llvm::Type *> & argumentTypes)
{
  llvm::FunctionType * funTy = llvm::FunctionType::get(llvm::Type::getVoidTy(*this->LLVMContext),
						       llvm::ArrayRef(&argumentTypes[0], argumentTypes.size()),
						       0);
  this->LLVMFunction = llvm::Function::Create(funTy, llvm::GlobalValue::ExternalLinkage, funName.c_str(), this->LLVMModule);
  llvm::BasicBlock * entryBlock = llvm::BasicBlock::Create(*this->LLVMContext, "EntryBlock", this->LLVMFunction);
  this->LLVMBuilder->SetInsertPoint(entryBlock);
  for(unsigned int i = 0; i<argumentNames.size(); i++) {
    llvm::Value * allocAVal = this->buildEntryBlockAlloca(argumentTypes[i], argumentNames[i].c_str());
    llvm::Value * arg = &this->LLVMFunction->arg_begin()[i];
    this->defineVariable(argumentNames[i].c_str(), allocAVal,
			     NULL, NULL, NULL, IQLToLLVMValue::eGlobal);
    // Set names on function arguments
    arg->setName(argumentNames[i]);
    // Store argument in the alloca 
    this->LLVMBuilder->CreateStore(arg, allocAVal);
  }  
}

void CodeGenerationContext::createFunction(const std::vector<AliasedRecordType>& sources,
                                           llvm::Type * returnType,
                                           std::string & suggestedName)
{
  reinitialize();
  std::vector<std::string> argumentNames;
  for(std::size_t i=0; i<sources.size(); i++) {
    argumentNames.push_back((boost::format("__BasePointer%1%__") % i).str());
  }
  this->ConstructFunction(suggestedName, argumentNames, returnType);

  // Inject the members of the input struct into the symbol table.
  // For the moment just make sure we don't have any ambiguous references
  for(std::vector<AliasedRecordType>::const_iterator it = sources.begin();
      it != sources.end();
      ++it) {
    this->addInputRecordType(it->getAlias().c_str(), 
                             (boost::format("__BasePointer%1%__") % (it - sources.begin())).str().c_str(), 			   
                             it->getType());
  }

  // Special context entry for output record required by 
  // transfer but not by function
  this->IQLOutputRecord = NULL;
  this->IQLMoveSemantics = 0;

  suggestedName = LLVMFunction->getName();
}

void CodeGenerationContext::createRecordTypeOperation(const RecordType * input,
                                                      std::string& suggestedName)
{
  reinitialize();
  std::vector<std::string> argumentNames;
  argumentNames.push_back("__BasePointer__");
  argumentNames.push_back("__OutputStreamPointer__");
  argumentNames.push_back("__OutputRecordDelimiter__");
  std::vector<llvm::Type *> argumentTypes;
  argumentTypes.push_back(llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0));
  argumentTypes.push_back(llvm::PointerType::get(llvm::Type::getInt8Ty(*this->LLVMContext), 0));
  argumentTypes.push_back(llvm::Type::getInt32Ty(*this->LLVMContext));
  // Create the function object with its arguments (these go into the
  // new freshly created symbol table).
  ConstructFunction(suggestedName, argumentNames, argumentTypes);
  // Inject the members of the input struct into the symbol table.
  this->addInputRecordType("input", "__BasePointer__", input);
  // Special context entry for output record required by 
  // transfer but not by record type op
  this->IQLMoveSemantics = 0;
  this->IQLOutputRecord = nullptr;
  suggestedName = LLVMFunction->getName();
}

void CodeGenerationContext::createTransferFunction(const RecordType * input,
                                                   const RecordType * output,
                                                   std::string& suggestedFunName)
{
  std::vector<AliasedRecordType> sources { AliasedRecordType("input", input) };
  createTransferFunction(sources, output, suggestedFunName);
}

void CodeGenerationContext::createTransferFunction(const std::vector<AliasedRecordType>& sources,
                                                   const RecordType * output,
                                                   std::string & suggestedFunName)
{
  std::vector<boost::dynamic_bitset<>> masks;
  for(const auto & s : sources) {
    masks.emplace_back(s.getType()->size());
    masks.back().set();
  }
  createTransferFunction(sources, masks, output, suggestedFunName);
}

void CodeGenerationContext::createTransferFunction(const std::vector<AliasedRecordType>& sources,
                                                   const std::vector<boost::dynamic_bitset<> >& masks,
                                                   const RecordType * output,
                                                   std::string & suggestedFunName)
{
  // Setup LLVM access to our external structure(s).  
  std::vector<std::string> argumentNames;
  for(std::vector<AliasedRecordType>::const_iterator it = sources.begin(); 
      it != sources.end();
      ++it) {
    argumentNames.push_back(sources.size() == 1 ? 
			    "__BasePointer__" : 
			    (boost::format("__BasePointer%1%__") % (it - sources.begin())).str().c_str());
  }
  argumentNames.push_back("__OutputPointer__");
  ConstructFunction(suggestedFunName, argumentNames);

  for(std::vector<AliasedRecordType>::const_iterator it = sources.begin(); 
      it != sources.end();
      ++it) {
    std::size_t i = (std::size_t) (it - sources.begin());
    const boost::dynamic_bitset<> mask(masks[i]);
    // Inject the members of the input struct into the symbol table.
    this->addInputRecordType(it->getAlias().c_str(),
                             sources.size() == 1 ? 
                             "__BasePointer__" :  
                             (boost::format("__BasePointer%1%__") % i).str().c_str(), 
                             it->getType(),
                             mask);
  }
  // Special context entry for output record required by statement list
  this->IQLOutputRecord = wrap(output);
  suggestedFunName = LLVMFunction->getName();
}

void CodeGenerationContext::createTransferFunction(const std::vector<const RecordType *>& sources,
                                                   const std::vector<boost::dynamic_bitset<> >& masks,
                                                   const RecordType * output,
                                                   std::string & suggestedFunName)
{
  // Setup LLVM access to our external structure(s).  
  std::vector<std::string> argumentNames;
  for(std::size_t i=0; i<sources.size(); i++)
    argumentNames.push_back((boost::format("__BasePointer%1%__") % i).str());
  if (output != NULL)
    argumentNames.push_back("__OutputPointer__");
  ConstructFunction(suggestedFunName, argumentNames);

  // Inject the members of the input struct into the symbol table.
  // For the moment just make sure we don't have any ambiguous references
  std::set<std::string> uniqueNames;
  for(std::vector<const RecordType *>::const_iterator it = sources.begin();
      it != sources.end();
      ++it) {
    std::size_t i = (std::size_t) (it - sources.begin());
    const boost::dynamic_bitset<> mask(masks[i]);
    for(RecordType::const_member_iterator mit = (*it)->begin_members();
	mit != (*it)->end_members();
	++mit) {
      if (mask.test(mit-(*it)->begin_members())) {
	if (uniqueNames.end() != uniqueNames.find(mit->GetName()))
	  throw std::runtime_error("Field names must be unique in in place update statements");
	uniqueNames.insert(mit->GetName());
      }
    }
    this->addInputRecordType((boost::format("input%1%") % i).str().c_str(), 
				 (boost::format("__BasePointer%1%__") % i).str().c_str(), 			   
				 *it,
				 mask);
  }
  this->IQLOutputRecord = wrap(output);
  suggestedFunName = LLVMFunction->getName();
}

void CodeGenerationContext::createUpdate(const std::vector<const RecordType *>& sources,
                                         const std::vector<boost::dynamic_bitset<> >& masks,
                                         std::string & funName)
{
  createTransferFunction(sources, masks, nullptr, funName);
  IQLMoveSemantics = 0;
}

void CodeGenerationContext::completeFunctionContext()
{
  LLVMBuilder->CreateRetVoid();  
  llvm::verifyFunction(*LLVMFunction);  
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *codeGen.LLVMModule;
  // // Now run optimizer over the IR
  mFPM->run(*LLVMFunction);
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *codeGen.LLVMModule;
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();

  // Clean up 
  reinitialize();
}

void CodeGenerationContext::createAggregateContexts(const TypeCheckConfiguration & typeCheckConfig)
{
 reinitialize();
 saveAggregateContext(&Update);
 createFunctionContext(typeCheckConfig);
 saveAggregateContext(&Initialize);
 createFunctionContext(typeCheckConfig);
 saveAggregateContext(&Transfer);
}

void CodeGenerationContext::completeAggregateContexts()
{
  // Complete all builders
  Update.Builder->CreateRetVoid();
  Initialize.Builder->CreateRetVoid();
  Transfer.Builder->CreateRetVoid();
  
  llvm::verifyFunction(*Update.Function);
  llvm::verifyFunction(*Initialize.Function);
  llvm::verifyFunction(*Transfer.Function);
  
  // // Now run optimizer over the IR
  mFPM->run(*Update.Function);
  mFPM->run(*Initialize.Function);
  mFPM->run(*Transfer.Function);

  // Clean up 
  Initialize.clear();
  Transfer.clear();
  restoreAggregateContext(&Update);
  reinitialize();
}

class NullInitializedAggregate : public AggregateFunction
{
protected:
  virtual void updateNull(CodeGenerationContext * ctxt,
			  const std::string& aggFn,
			  const IQLToLLVMValue * inc,
			  const FieldType * incTy,
			  const IQLToLLVMLValue * fieldLVal,
			  const FieldType * ft)=0;
  virtual void updateNotNull(CodeGenerationContext * ctxt,
			     const std::string& aggFn,
			     const IQLToLLVMValue * inc,
			     const FieldType * incTy,
			     const IQLToLLVMLValue * fieldLVal,
			     const FieldType * ft,
			     llvm::BasicBlock * mergeBlock)=0;
  
public:
  ~NullInitializedAggregate() {}
  void update(CodeGenerationContext * ctxt,
	      const std::string& old,
	      const IQLToLLVMValue * inc,
              const FieldType * incTy,
	      const FieldType * retTy) ;
  const IQLToLLVMValue * initialize(CodeGenerationContext * ctxt,
				    const FieldType * retTy);  
};

void NullInitializedAggregate::update(CodeGenerationContext * ctxt,
                                      const std::string& aggFn,
                                      const IQLToLLVMValue * inc,
                                      const FieldType * incTy,
                                      const FieldType * retTy)
{
  // Code generate:
  //    IF inc IS NOT NULL THEN (only if input is nullable)
  //      IF old IS NOT NULL THEN 
  //        SET old = old + inc
  //      ELSE
  //        SET old = inc

  // Unwrap to C++
  llvm::LLVMContext * c = ctxt->LLVMContext;
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Merge block 
  llvm::BasicBlock * mergeBlock = 
    llvm::BasicBlock::Create(*c, "cont", TheFunction);
  // LValue around the field
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(ctxt->IQLRecordArguments));
  const RecordType * outputRecord = recordTypes.find("input1")->second.second;
  BOOST_ASSERT(outputRecord != NULL);
  IQLToLLVMField fieldLVal(ctxt, outputRecord, aggFn, "__BasePointer1__");
  // If a nullable increment, check for nullability
  llvm::Value * nv = inc->getNull(ctxt);
  if (nv) {
    // We must skip nullable inputs
    llvm::BasicBlock * updateBlock = 
      llvm::BasicBlock::Create(*c, "aggCkNullInc", TheFunction);
    // Check NULL bit, branch and set block
    b->CreateCondBr(b->CreateNot(nv), updateBlock, mergeBlock);
    // Emit update and branch to merge.
    b->SetInsertPoint(updateBlock);    
    BOOST_ASSERT(incTy->isNullable());
  } else {
    BOOST_ASSERT(!incTy->isNullable());
  }
  
  llvm::BasicBlock * notNullBlock = 
    llvm::BasicBlock::Create(*c, "aggNotNull", TheFunction);
  llvm::BasicBlock * nullBlock = 
    llvm::BasicBlock::Create(*c, "aggNull", TheFunction);
  const IQLToLLVMValue * old = ctxt->buildVariableRef(aggFn.c_str(), retTy);
  BOOST_ASSERT(old->getNull(ctxt));
  b->CreateCondBr(b->CreateNot(old->getNull(ctxt)), notNullBlock, nullBlock);
  // Increment value not null case
  b->SetInsertPoint(notNullBlock);    
  updateNotNull(ctxt, aggFn, inc, incTy, &fieldLVal, retTy, mergeBlock);
  b->CreateBr(mergeBlock);
  // set value null case
  b->SetInsertPoint(nullBlock);    
  updateNull(ctxt, aggFn, inc, incTy, &fieldLVal, retTy);
  b->CreateBr(mergeBlock);

  b->SetInsertPoint(mergeBlock);
}

const IQLToLLVMValue * NullInitializedAggregate::initialize(CodeGenerationContext * ctxt,
                                                            const FieldType * )
{
  return ctxt->buildNull();
}

class SumAggregate : public NullInitializedAggregate
{
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  const IQLToLLVMValue * inc,
		  const FieldType * incTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     const IQLToLLVMValue * inc,
		     const FieldType * incTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  ~SumAggregate() {}
};

void SumAggregate::updateNull(CodeGenerationContext * ctxt,
			      const std::string& aggFn,
			      const IQLToLLVMValue * inc,
			      const FieldType * incTy,
			      const IQLToLLVMLValue * fieldLVal,
			      const FieldType * ft)
{
  ctxt->buildSetNullableValue(fieldLVal, inc, ft, false, true);
}

void SumAggregate::updateNotNull(CodeGenerationContext * ctxt,
				 const std::string& aggFn,
				 const IQLToLLVMValue * inc,
				 const FieldType * incTy,
				 const IQLToLLVMLValue * fieldLVal,
				 const FieldType * ft,
				 llvm::BasicBlock * mergeBlock)
{
  const IQLToLLVMValue * sum = ctxt->buildAdd(inc,
					      incTy,
					      ctxt->buildVariableRef(aggFn.c_str(), ft),
					      ft,
					      ft);
  ctxt->buildSetNullableValue(fieldLVal, sum, ft, false, true);
}


class MaxMinAggregate : public NullInitializedAggregate
{
private:
  bool mIsMax;
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  const IQLToLLVMValue * inc,
		  const FieldType * incTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     const IQLToLLVMValue * inc,
		     const FieldType * incTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  MaxMinAggregate(bool isMax);
};

MaxMinAggregate::MaxMinAggregate(bool isMax)
  :
  mIsMax(isMax)
{
}

void MaxMinAggregate::updateNull(CodeGenerationContext * ctxt,
                                 const std::string& aggFn,
                                 const IQLToLLVMValue * inc,
                                 const FieldType * incTy,
                                 const IQLToLLVMLValue * fieldLVal,
                                 const FieldType * ft)
{
  ctxt->buildSetNullableValue(fieldLVal, inc, ft, false, true);
}

void MaxMinAggregate::updateNotNull(CodeGenerationContext * ctxt,
				    const std::string& aggFn,
				    const IQLToLLVMValue * inc,
				    const FieldType * incTy,
				    const IQLToLLVMLValue * fieldLVal,
				    const FieldType * ft,
				    llvm::BasicBlock * mergeBlock)
{
  // Unwrap to C++
  llvm::LLVMContext * c = ctxt->LLVMContext;
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // A block in which to do the update 
  llvm::BasicBlock * updateBlock = 
    llvm::BasicBlock::Create(*c, "then", TheFunction);

  const IQLToLLVMValue * old = ctxt->buildVariableRef(aggFn.c_str(), ft);
  const IQLToLLVMValue * condVal = ctxt->buildCompare(old, ft, 
						      inc, incTy,
						      // Currently returning int32_t for bool
						      Int32Type::Get(ft->getContext(), true),
						      mIsMax ? IQLToLLVMOpLT : IQLToLLVMOpGT);
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(ctxt),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, updateBlock, mergeBlock);
  // Emit update and branch to merge.
  b->SetInsertPoint(updateBlock);    
  ctxt->buildSetValue(inc, aggFn.c_str(), ft);
}

class ArrayConcatAggregate : public NullInitializedAggregate
{
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  const IQLToLLVMValue * inc,
		  const FieldType * incTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     const IQLToLLVMValue * inc,
		     const FieldType * incTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  ~ArrayConcatAggregate() {}
};

void ArrayConcatAggregate::updateNull(CodeGenerationContext * ctxt,
                                      const std::string& aggFn,
                                      const IQLToLLVMValue * inc,
                                      const FieldType * incTy,
                                      const IQLToLLVMLValue * fieldLVal,
                                      const FieldType * ft)
{
  // Create a one element array and set that as value
  std::vector<IQLToLLVMTypedValue> vals = { IQLToLLVMTypedValue(inc, incTy) };
  ctxt->buildSetNullableValue(fieldLVal, ctxt->buildArray(vals, ft), ft, false, true);  
}

void ArrayConcatAggregate::updateNotNull(CodeGenerationContext * ctxt,
                                         const std::string& aggFn,
                                         const IQLToLLVMValue * inc,
                                         const FieldType * incTy,
                                         const IQLToLLVMLValue * fieldLVal,
                                         const FieldType * ft,
                                         llvm::BasicBlock * mergeBlock)
{
  const IQLToLLVMValue * arr = ctxt->buildArrayConcat(ctxt->buildVariableRef(aggFn.c_str(), ft), ft, inc, incTy, ft);
  ctxt->buildSetNullableValue(fieldLVal, arr, ft, false, true);
}

std::shared_ptr<AggregateFunction> AggregateFunction::get(const char * fn)
{
  AggregateFunction * agg = NULL;
  if (boost::algorithm::iequals(fn, "max")) {
    agg = new MaxMinAggregate(true);
  } else if (boost::algorithm::iequals(fn, "min")) {
    agg = new MaxMinAggregate(false);
  } else if (boost::algorithm::iequals(fn, "sum")) {
    agg = new SumAggregate();
  } else if (boost::algorithm::iequals(fn, "array_concat")) {
    agg = new ArrayConcatAggregate();
  } else {
    throw std::runtime_error ((boost::format("Unknown aggregate function: %1%")%
			       fn).str());
  }
  return std::shared_ptr<AggregateFunction>(agg);
}

