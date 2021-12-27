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

#include <iostream>
#include <boost/format.hpp>
#include <boost/regex.hpp>
#include <boost/static_assert.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// LLVM Includes
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Instructions.h"

#include "LLVMGen.h"
#include "CodeGenerationContext.hh"
#include "RecordType.hh"
#include "IQLExpression.hh"

#include "decimal128.h"
#include "md5.h"

void FieldAddress::setArrayNull(RecordBuffer buffer, const FixedArrayType * ty, int32_t idx) const
{
  if (!ty->getElementType()->isNullable()) {
    return;
  }
  int32_t nullByte = idx >> 3;
  uint8_t mask = 1U << (idx - (nullByte << 3));
  uint8_t * nullBytePtr = (buffer.Ptr + mOffset + ty->GetNullOffset() + nullByte);
  *nullBytePtr &= (~mask);
}

void FieldAddress::clearArrayNull(RecordBuffer buffer, const FixedArrayType * ty, int32_t idx) const
{
  if (!ty->getElementType()->isNullable()) {
    return;
  }
  int32_t nullByte = idx >> 3;
  uint8_t mask = 1U << (idx - (nullByte << 3));
  uint8_t * nullBytePtr = (buffer.Ptr + mOffset + ty->GetNullOffset() + nullByte);
  *nullBytePtr |= mask;
}

bool FieldAddress::isArrayNull(RecordBuffer buffer, const FixedArrayType * ty, int32_t idx) const
{
  if (!ty->getElementType()->isNullable()) {
    return false;
  }
  int32_t nullByte = idx >> 3;
  uint8_t mask = 1U << (idx - (nullByte << 3));
  uint8_t * nullBytePtr = (buffer.Ptr + mOffset + ty->GetNullOffset() + nullByte);
  uint8_t ret = *nullBytePtr & mask;
  return ret == 0;
}
  
llvm::Value * FieldAddress::getPointer(const std::string& member, 
				       CodeGenerationContext * ctxt, 
				       llvm::Value * basePointer) const
{
  llvm::IRBuilder<> * builder = ctxt->LLVMBuilder;
  return builder->CreateGEP(builder->getInt8Ty(),
                            basePointer,
			    builder->getInt64(mOffset),
			    ("raw" + member).c_str());
}

llvm::Value * FieldAddress::isNull(const std::string& member, 
				   CodeGenerationContext * ctxt, 
				   llvm::Value * basePointer) const
{
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  if (mPosition != 0xffffffff) {
    // NULL means there is a zero bit
    uint32_t dwordPos = mPosition >> 5;
    llvm::ConstantInt * mask = b->getInt32(1U << (mPosition - (dwordPos << 5)));
    llvm::Value * dwordPtr = b->CreateGEP(b->getInt8Ty(),
                                          basePointer,
					  b->getInt64(dwordPos*sizeof(uint32_t)),
					  ("isNull" + member).c_str());
    dwordPtr = b->CreateBitCast(dwordPtr, 
				llvm::PointerType::get(b->getInt32Ty(),0));
    llvm::Value * v = b->CreateAnd(b->CreateLoad(b->getInt32Ty(), dwordPtr), mask);
    return b->CreateICmpEQ(v, b->getInt32(0));
  } else {
    return b->getFalse();
  }
}

void FieldAddress::setNull(const std::string& member, 
			   CodeGenerationContext * ctxt, 
			   llvm::Value * basePointer,
			   bool isNull) const
{
  // Illegal to setNull(true) on non nullable field,
  // noop to setNull(false) on non nullable field.
  if (mPosition == 0xffffffff && isNull)
    throw std::runtime_error("Error trying to set NULL value in non NULLABLE field");
  
  if (mPosition != 0xffffffff) {
    llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
    // NULL means there is a zero bit
    uint32_t dwordPos = mPosition >> 5;
    llvm::ConstantInt * mask = b->getInt32(1U << (mPosition - (dwordPos << 5)));
    llvm::Value * dwordPtr = b->CreateGEP(b->getInt8Ty(),
                                          basePointer,
					  b->getInt64(dwordPos*sizeof(uint32_t)),
					  ("isNull" + member).c_str());
    dwordPtr = b->CreateBitCast(dwordPtr, 
				llvm::PointerType::get(b->getInt32Ty(),0));
    llvm::Value * val = isNull ?
      b->CreateAnd(b->CreateLoad(b->getInt32Ty(), dwordPtr), b->CreateNot(mask)) :
      b->CreateOr(b->CreateLoad(b->getInt32Ty(), dwordPtr), mask);
    b->CreateStore(val, dwordPtr);
  }   
}

void FieldAddress::dump() const
{
  std::cout << mOffset;
}

DynamicRecordContext::DynamicRecordContext()
{
}

DynamicRecordContext::~DynamicRecordContext()
{
  for(std::map<Digest, FieldType *>::iterator it = mTypes.begin();
      it != mTypes.end();
      ++it) {
    //int64_t key = it->first;
    FieldType * val = it->second;
    delete val;
  }

  for(std::set<const RecordType*>::iterator it = mRecords.begin();
      it != mRecords.end();
      ++it) {
    delete *it;
  }

  for(std::set<IQLExpression*>::iterator it = mExprs.begin();
      it != mExprs.end();
      ++it) {
    delete *it;
  }
}

FieldType * DynamicRecordContext::lookup(const Digest& id) const
{
  std::map<Digest, FieldType *>::const_iterator it = mTypes.find(id);
  return it == mTypes.end() ? NULL : it->second;
}

void DynamicRecordContext::add(const Digest& id, FieldType * val)
{
  mTypes[id] = val;
}

void DynamicRecordContext::add(const RecordType * ty)
{
  mRecords.insert(ty);
}

void DynamicRecordContext::add(IQLExpression * expr)
{
  mExprs.insert(expr);
}

llvm::Type * FieldType::LLVMGetType(CodeGenerationContext * ctxt) const
{
  switch(mType) {
  case VARCHAR:
    return ctxt->LLVMVarcharType;
  case CHAR:
    return llvm::ArrayType::get(llvm::Type::getInt8Ty(*ctxt->LLVMContext), (unsigned) (mSize + 1));
  case BIGDECIMAL:
    return ctxt->LLVMDecimal128Type;
  case INT8:
    return llvm::Type::getInt8Ty(*ctxt->LLVMContext);
  case INT16:
    return llvm::Type::getInt16Ty(*ctxt->LLVMContext);
  case INT32:
    return llvm::Type::getInt32Ty(*ctxt->LLVMContext);
  case INT64:
    return llvm::Type::getInt64Ty(*ctxt->LLVMContext);
  case FLOAT:
    return llvm::Type::getFloatTy(*ctxt->LLVMContext);
  case DOUBLE:
    return llvm::Type::getDoubleTy(*ctxt->LLVMContext);
  case DATETIME:
    {
      BOOST_STATIC_ASSERT(sizeof(boost::posix_time::ptime) == 8);
      return llvm::Type::getInt64Ty(*ctxt->LLVMContext);
    }
  case DATE:
    {
      BOOST_STATIC_ASSERT(sizeof(boost::gregorian::date) == 4);
      return llvm::Type::getInt32Ty(*ctxt->LLVMContext);
    }
  case IPV4:
    static_assert(sizeof(boost::asio::ip::address_v4::bytes_type) == 4);
    return llvm::Type::getInt32Ty(*ctxt->LLVMContext);
  case CIDRV4:
    {
      static_assert(sizeof(boost::asio::ip::address_v4::bytes_type) == 4);
      static_assert(sizeof(CidrV4Runtime) == 5);
      return ctxt->LLVMCidrV4Type;
    }
  case IPV6:
    static_assert(sizeof(boost::asio::ip::address_v6::bytes_type) == 16);
    return llvm::ArrayType::get(llvm::Type::getInt8Ty(*ctxt->LLVMContext), 16U);
  case CIDRV6:
    static_assert(sizeof(boost::asio::ip::address_v6::bytes_type) == 16);
    return llvm::ArrayType::get(llvm::Type::getInt8Ty(*ctxt->LLVMContext), 17U);
  case INTERVAL:
    return llvm::Type::getInt32Ty(*ctxt->LLVMContext);
  default:
    throw std::runtime_error("Invalid Type value");
  }
}

void FieldType::AppendTo(struct md5_state_s * md5) const
{
  FieldType::FieldTypeEnum f=GetEnum();
  md5_append(md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = GetSize();
  md5_append(md5, (const md5_byte_t *) &sz, sizeof(sz));
  bool nullable = isNullable();
  md5_append(md5, (const md5_byte_t *) &nullable, sizeof(nullable));
}

llvm::Value * FieldType::getMinValue(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("FieldType::getMinValue not implemented");
}

llvm::Value * FieldType::getMaxValue(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("FieldType::getMaxValue not implemented");
}

llvm::Value * FieldType::getZero(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("FieldType::getZero not implemented");
}

VarcharType * VarcharType::Get(DynamicRecordContext& ctxt)
{
  return Get(ctxt, std::numeric_limits<int32_t>::max(), false);
}

VarcharType * VarcharType::Get(DynamicRecordContext& ctxt, bool nullable)
{
  return Get(ctxt, std::numeric_limits<int32_t>::max(), nullable);
}

std::string VarcharType::toString() const
{
  return "VARCHAR";
}

const FieldType * VarcharType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return VarcharType::Get(getContext(), GetSize(), nullable);
}

VarcharType * VarcharType::Get(DynamicRecordContext& ctxt, int32_t sz, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::VARCHAR;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new VarcharType(ctxt,sz,nullable);
    ctxt.add(d, ft);
  }
  return (VarcharType *)ft;
}

VarcharType::~VarcharType()
{
}

llvm::Value * VarcharType::getMinValue(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("VarcharType::getMinValue not implemented");
}

llvm::Value * VarcharType::getMaxValue(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("VarcharType::getMaxValue not implemented");
}

llvm::Value * VarcharType::getZero(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("VarcharType::getZero not implemented");
}

std::string CharType::toString() const
{
  return (boost::format("CHAR(%1%)") % GetSize()).str();
}

const FieldType * CharType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return CharType::Get(getContext(), GetSize(), nullable);
}

CharType * CharType::Get(DynamicRecordContext& ctxt, int32_t sz, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::CHAR;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new CharType(ctxt,sz,nullable);
    ctxt.add(d, ft);
  }
  return (CharType *)ft;
}

CharType::~CharType()
{
}

llvm::Value * CharType::getMinValue(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("CharType::getMinValue not implemented");
}

llvm::Value * CharType::getMaxValue(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("CharType::getMaxValue not implemented");
}

llvm::Value * CharType::getZero(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("CharType::getZero not implemented");
}

std::string Int8Type::toString() const
{
  return "TINYINT";
}

const FieldType * Int8Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return Int8Type::Get(getContext(), nullable);
}

Int8Type * Int8Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::INT8;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 1;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new Int8Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (Int8Type *)ft;
}

Int8Type::~Int8Type()
{
}

llvm::Value * Int8Type::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int8_t>::min(), 
				1);
}

llvm::Value * Int8Type::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int8_t>::max(), 
				1);
}

llvm::Value * Int8Type::getZero(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				0,
				1);
}

bool Int8Type::isNumeric() const
{
  return true;
}

bool Int8Type::isIntegral() const
{
  return true;
}

std::string Int16Type::toString() const
{
  return "SMALLINT";
}

const FieldType * Int16Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return Int16Type::Get(getContext(), nullable);
}

Int16Type * Int16Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::INT16;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 2;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new Int16Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (Int16Type *)ft;
}

Int16Type::~Int16Type()
{
}

llvm::Value * Int16Type::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int16_t>::min(), 
				1);
}

llvm::Value * Int16Type::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int16_t>::max(), 
				1);
}

llvm::Value * Int16Type::getZero(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				0,
				1);
}

bool Int16Type::isNumeric() const
{
  return true;
}

bool Int16Type::isIntegral() const
{
  return true;
}

std::string Int32Type::toString() const
{
  return "INTEGER";
}

const FieldType * Int32Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return Int32Type::Get(getContext(), nullable);
}

Int32Type * Int32Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::INT32;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 4;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new Int32Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (Int32Type *)ft;
}

Int32Type::~Int32Type()
{
}

llvm::Value * Int32Type::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int32_t>::min(), 
				1);
}

llvm::Value * Int32Type::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int32_t>::max(), 
				1);
}

llvm::Value * Int32Type::getZero(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				0,
				1);
}

bool Int32Type::isNumeric() const
{
  return true;
}

bool Int32Type::isIntegral() const
{
  return true;
}

std::string Int64Type::toString() const
{
  return "BIGINT";
}

const FieldType * Int64Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return Int64Type::Get(getContext(), nullable);
}

Int64Type * Int64Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::INT64;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 8;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new Int64Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (Int64Type *)ft;
}

Int64Type::~Int64Type()
{
}

llvm::Value * Int64Type::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int64_t>::min(), 
				1);
}

llvm::Value * Int64Type::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int64_t>::max(), 
				1);
}

llvm::Value * Int64Type::getZero(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				0,
				1);
}

bool Int64Type::isNumeric() const
{
  return true;
}

bool Int64Type::isIntegral() const
{
  return true;
}

std::string FloatType::toString() const
{
  return "REAL";
}

const FieldType * FloatType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return FloatType::Get(getContext(), nullable);
}

FloatType * FloatType::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::FLOAT;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 4;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new FloatType(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (FloatType *)ft;
}

FloatType::~FloatType()
{
}

llvm::Value * FloatType::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantFP::get(LLVMGetType(ctxt),
			       std::numeric_limits<float>::min());
}

llvm::Value * FloatType::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantFP::get(LLVMGetType(ctxt),
			       std::numeric_limits<float>::max()); 
}

llvm::Value * FloatType::getZero(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantFP::get(LLVMGetType(ctxt),
			       0);
}

bool FloatType::isNumeric() const
{
  return true;
}

bool FloatType::isFloatingPoint() const
{
  return true;
}

std::string DoubleType::toString() const
{
  return "DOUBLE PRECISION";
}

const FieldType * DoubleType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return DoubleType::Get(getContext(), nullable);
}

DoubleType * DoubleType::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::DOUBLE;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 8;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new DoubleType(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (DoubleType *)ft;
}

DoubleType::~DoubleType()
{
}

llvm::Value * DoubleType::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantFP::get(LLVMGetType(ctxt),
			       std::numeric_limits<double>::min());
}

llvm::Value * DoubleType::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantFP::get(LLVMGetType(ctxt),
			       std::numeric_limits<double>::max()); 
}

llvm::Value * DoubleType::getZero(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantFP::get(LLVMGetType(ctxt),
			       0);
}

bool DoubleType::isNumeric() const
{
  return true;
}

bool DoubleType::isFloatingPoint() const
{
  return true;
}

std::string DecimalType::toString() const
{
  return "DECIMAL";
}

const FieldType * DecimalType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return DecimalType::Get(getContext(), nullable);
}

DecimalType * DecimalType::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::BIGDECIMAL;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 16;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new DecimalType(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (DecimalType *)ft;
}

DecimalType::~DecimalType()
{
}

llvm::Value * DecimalType::createGlobalValue(CodeGenerationContext * ctxt,
					     const decimal128 & dec) const
{
  llvm::LLVMContext * c = ctxt->LLVMContext;
  llvm::Module * m = ctxt->LLVMModule;
  llvm::Type * ty = ctxt->LLVMDecimal128Type;
  llvm::GlobalVariable * globalVar = 
    new llvm::GlobalVariable(*m, ty, false, 
			     llvm::GlobalValue::ExternalLinkage, 
			     0, "decGlobal");
  // The value to initialize the global
  llvm::Type * int32Ty = llvm::Type::getInt32Ty(*c);
  llvm::Type * structTypeMembers[4];
  llvm::Constant * constStructMembers[4];
  for(int i=0 ; i<4; i++) {
    structTypeMembers[i] = int32Ty;
    constStructMembers[i] = llvm::ConstantInt::get(int32Ty, 
						   ((int32_t *) &dec)[i], 
						   true);
  }
  llvm::StructType * structTy = llvm::StructType::create(*c, llvm::ArrayRef<llvm::Type*>(&structTypeMembers[0],4), "decGlobalTy", true);
  llvm::Constant * globalVal = llvm::ConstantStruct::get(structTy, 
							 llvm::ArrayRef<llvm::Constant*>(&constStructMembers[0],4));
  globalVar->setInitializer(globalVal);
  return globalVar;
}

llvm::Value * DecimalType::getMinValue(CodeGenerationContext * ctxt) const
{
  decimal128 dec;
  decContext decCtxt;
  ::decContextDefault(&decCtxt, 0);
  // Both of these values have the desired effect when we deal with non-infinite
  // decimals.  I guess I am being a bit conservative in choosing the former.
  ::decimal128FromString(&dec, "-9.999999999999999999999999999999999e+6144", &decCtxt);
  // ::decimal128FromString(&dec, "-Infinity", &decCtxt);
  return createGlobalValue(ctxt, dec);
}

llvm::Value * DecimalType::getMaxValue(CodeGenerationContext * ctxt) const
{
  decimal128 dec;
  decContext decCtxt;
  ::decContextDefault(&decCtxt, 0);
  // Both of these values have the desired effect when we deal with non-infinite
  // decimals.  I guess I am being a bit conservative in choosing the former.
  ::decimal128FromString(&dec, "9.999999999999999999999999999999999e+6144", &decCtxt);
  // ::decimal128FromString(&dec, "Infinity", &decCtxt);
  return createGlobalValue(ctxt, dec);
}

llvm::Value * DecimalType::getZero(CodeGenerationContext * ctxt) const
{
  decNumber dn;
  decimal128 dec;
  decContext decCtxt;
  ::decContextDefault(&decCtxt, 0);
  ::decNumberZero(&dn);
  ::decimal128FromNumber(&dec, &dn, &decCtxt);
  return createGlobalValue(ctxt, dec);
}

bool DecimalType::isNumeric() const
{
  return true;
}

std::string DatetimeType::toString() const
{
  return "DATETIME";
}

const FieldType * DatetimeType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return DatetimeType::Get(getContext(), nullable);
}

DatetimeType * DatetimeType::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::DATETIME;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 8;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new DatetimeType(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (DatetimeType *)ft;
}

DatetimeType::~DatetimeType()
{
}

llvm::Value * DatetimeType::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int64_t>::min(), 
				1);
}

llvm::Value * DatetimeType::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int64_t>::max(), 
				1);
}

llvm::Value * DatetimeType::getZero(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("DatetimeType::getZero not implemented");
}

std::string DateType::toString() const
{
  return "DATE";
}

const FieldType * DateType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return DateType::Get(getContext(), nullable);
}

DateType * DateType::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::DATE;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 4;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new DateType(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (DateType *)ft;
}

DateType::~DateType()
{
}

llvm::Value * DateType::getMinValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int32_t>::min(), 
				1);
}

llvm::Value * DateType::getMaxValue(CodeGenerationContext * ctxt) const
{
  return llvm::ConstantInt::get(LLVMGetType(ctxt),
				std::numeric_limits<int32_t>::max(), 
				1);
}

llvm::Value * DateType::getZero(CodeGenerationContext * ctxt) const
{
  throw std::runtime_error("DateType::getZero not implemented");
}

std::string IPv4Type::toString() const
{
  return "IPV4";
}

const FieldType * IPv4Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return IPv4Type::Get(getContext(), nullable);
}

IPv4Type * IPv4Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::IPV4;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 4;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new IPv4Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (IPv4Type *)ft;
}

IPv4Type::~IPv4Type()
{
}

std::string CIDRv4Type::toString() const
{
  return "CIDRV4";
}

const FieldType * CIDRv4Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return CIDRv4Type::Get(getContext(), nullable);
}

CIDRv4Type * CIDRv4Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::CIDRV4;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 5;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new CIDRv4Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (CIDRv4Type *)ft;
}

CIDRv4Type::~CIDRv4Type()
{
}

std::string IPv6Type::toString() const
{
  return "IPV6";
}

const FieldType * IPv6Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return IPv6Type::Get(getContext(), nullable);
}

IPv6Type * IPv6Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::IPV6;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 16;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new IPv6Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (IPv6Type *)ft;
}

IPv6Type::~IPv6Type()
{
}

std::string CIDRv6Type::toString() const
{
  return "CIDRV6";
}

const FieldType * CIDRv6Type::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return CIDRv6Type::Get(getContext(), nullable);
}

CIDRv6Type * CIDRv6Type::Get(DynamicRecordContext& ctxt, bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::CIDRV6;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 17;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new CIDRv6Type(ctxt, nullable);
    ctxt.add(d, ft);
  }
  return (CIDRv6Type *)ft;
}

CIDRv6Type::~CIDRv6Type()
{
}

void FunctionType::AppendTo(struct md5_state_s * md5) const
{
  AppendTo(mArgs, mRet, md5);
}

void FunctionType::AppendTo(const std::vector<const FieldType *>& args, 
			    const FieldType * ret,
			    struct md5_state_s * md5)
{
  FieldType::FieldTypeEnum f=FieldType::FUNCTION;
  md5_append(md5, (const md5_byte_t *) &f, sizeof(f));
  for(std::vector<const FieldType *>::const_iterator it = args.begin();
      it != args.end();
      ++it) {
    (*it)->AppendTo(md5);
  }
  ret->AppendTo(md5);
}

std::string FunctionType::toString() const
{
  return "";
}

FunctionType * FunctionType::Get(DynamicRecordContext& ctxt, 
				 const std::vector<const FieldType *>& args, 
				 const FieldType * ret)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  AppendTo(args,ret,&md5);
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new FunctionType(ctxt, args, ret);
    ctxt.add(d, ft);
  }
  return (FunctionType *)ft;
}

FunctionType * FunctionType::Get(DynamicRecordContext& ctxt, 
				 const FieldType * ret)
{
  std::vector<const FieldType *> args;
  return Get(ctxt, args, ret);
}

FunctionType * FunctionType::Get(DynamicRecordContext& ctxt, 
				 const FieldType * arg1,
				 const FieldType * ret)
{
  std::vector<const FieldType *> args;
  args.push_back(arg1);
  return Get(ctxt, args, ret);
}

FunctionType * FunctionType::Get(DynamicRecordContext& ctxt, 
				 const FieldType * arg1,
				 const FieldType * arg2,
				 const FieldType * ret)
{
  std::vector<const FieldType *> args;
  args.push_back(arg1);
  args.push_back(arg2);
  return Get(ctxt, args, ret);
}

FunctionType * FunctionType::Get(DynamicRecordContext& ctxt, 
				 const FieldType * arg1,
				 const FieldType * arg2,
				 const FieldType * arg3,
				 const FieldType * ret)
{
  std::vector<const FieldType *> args;
  args.push_back(arg1);
  args.push_back(arg2);
  args.push_back(arg3);
  return Get(ctxt, args, ret);
}

FunctionType::~FunctionType()
{
}

void FixedArrayType::AppendTo(struct md5_state_s * md5) const
{
  AppendTo(GetSize(), mElementTy, isNullable(), md5);
}

void FixedArrayType::AppendTo(int32_t sz, const FieldType * element,
			      bool nullable, struct md5_state_s * md5)
{
  FieldType::FieldTypeEnum f=FieldType::FIXED_ARRAY;
  md5_append(md5, (const md5_byte_t *) &f, sizeof(f));
  md5_append(md5, (const md5_byte_t *) &sz, sizeof(sz));
  element->AppendTo(md5);
  md5_append(md5, (const md5_byte_t *) &nullable, sizeof(nullable));
}

std::string FixedArrayType::toString() const
{
  return (boost::format("%1%[%2%]") % mElementTy->toString() % GetSize()).str();
}

const FieldType * FixedArrayType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return FixedArrayType::Get(getContext(), GetSize(), mElementTy, nullable);
}

FixedArrayType * FixedArrayType::Get(DynamicRecordContext& ctxt, 
				     int32_t sz,
				     const FieldType * element,
				     bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  AppendTo(sz, element, nullable, &md5);
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new FixedArrayType(ctxt, sz, element, nullable);
    ctxt.add(d, ft);
  }
  return (FixedArrayType *)ft;
}

llvm::Type * FixedArrayType::LLVMGetType(CodeGenerationContext * ctxt) const
{
  if (getElementType()->isNullable()) {
    llvm::Type * fixedArrayMembers[2];
    fixedArrayMembers[0] = llvm::ArrayType::get(mElementTy->LLVMGetType(ctxt), (unsigned) GetSize());
    fixedArrayMembers[1] = llvm::ArrayType::get(ctxt->LLVMBuilder->getInt8Ty(), GetNullSize());
    llvm::StructType * structTy =  llvm::StructType::get(*ctxt->LLVMContext,
                                                         llvm::makeArrayRef(&fixedArrayMembers[0], 2),
                                                         0);
    llvm::DataLayout dl("");
    const llvm::StructLayout * layout = dl.getStructLayout(structTy);
    BOOST_ASSERT(layout->getElementOffset(0) == GetDataOffset());
    BOOST_ASSERT(layout->getElementOffset(1) == GetNullOffset());
    if (layout->getSizeInBytes() != GetAllocSize()) {
      throw std::runtime_error((boost::format("layout->getSizeInBytes() != GetAllocSize(); layout->getSizeInBytes() = %1% GetAllocSize() = %2%") %
                                layout->getSizeInBytes() %
                                GetAllocSize()).str());
    }
    return structTy;
  } else {
    return llvm::ArrayType::get(mElementTy->LLVMGetType(ctxt), (unsigned) GetSize());
  }
}

FixedArrayType::~FixedArrayType()
{
}

void VariableArrayType::AppendTo(struct md5_state_s * md5) const
{
  AppendTo(mElementTy, isNullable(), md5);
}

void VariableArrayType::AppendTo(const FieldType * element,
                                 bool nullable, struct md5_state_s * md5)
{
  FieldType::FieldTypeEnum f=FieldType::VARIABLE_ARRAY;
  md5_append(md5, (const md5_byte_t *) &f, sizeof(f));
  element->AppendTo(md5);
  md5_append(md5, (const md5_byte_t *) &nullable, sizeof(nullable));
}

std::string VariableArrayType::toString() const
{
  return (boost::format("%1%[]") % mElementTy->toString()).str();
}

const FieldType * VariableArrayType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return VariableArrayType::Get(getContext(), mElementTy, nullable);
}

VariableArrayType * VariableArrayType::Get(DynamicRecordContext& ctxt, 
                                           const FieldType * element,
                                           bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  AppendTo(element, nullable, &md5);
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new VariableArrayType(ctxt, element, nullable);
    ctxt.add(d, ft);
  }
  return (VariableArrayType *)ft;
}

llvm::Type * VariableArrayType::LLVMGetType(CodeGenerationContext * ctxt) const
{
  llvm::Type * vararrayMembers[3];
  vararrayMembers[0] = llvm::Type::getInt32Ty(*ctxt->LLVMContext);
  vararrayMembers[1] = llvm::Type::getInt32Ty(*ctxt->LLVMContext);
  vararrayMembers[2] = llvm::PointerType::get(mElementTy->LLVMGetType(ctxt), 0);
  return llvm::StructType::get(*ctxt->LLVMContext,
                               llvm::makeArrayRef(&vararrayMembers[0], 3),
                               0);
}

VariableArrayType::~VariableArrayType()
{
}

const FieldType * IntervalType::getDateResultType(DynamicRecordContext& ctxt, 
						  bool nullable) const
{
  switch(mIntervalUnit) {
  case DAY:
  case MONTH:
  case YEAR:
    return DateType::Get(ctxt, nullable);
  case HOUR:
  case MINUTE:
  case SECOND:
    return DatetimeType::Get(ctxt, nullable);
  default:
    throw std::runtime_error("Internal Error: Unknown interval unit in IntervalType");
  }
}

void IntervalType::AppendTo(struct md5_state_s * md5) const
{
  FieldType::AppendTo(md5);
  md5_append(md5, (const md5_byte_t *) &mIntervalUnit, sizeof(mIntervalUnit));
}

std::string IntervalType::toString() const
{
  switch(mIntervalUnit) {
  case DAY:
    return "INTERVAL DAY";
  case HOUR:
    return "INTERVAL HOUR";
  case MINUTE:
    return "INTERVAL MINUTE";
  case MONTH:
    return "INTERVAL MONTH";
  case SECOND:
    return "INTERVAL SECOND";
  case YEAR:
    return "INTERVAL YEAR";
  default:
    throw std::runtime_error("Internal Error: Unknown interval unit in IntervalType");
  }
}

const FieldType * IntervalType::clone(bool nullable) const
{
  if (nullable == isNullable()) return this;
  return IntervalType::Get(getContext(), mIntervalUnit, nullable);
}

IntervalType * IntervalType::Get(DynamicRecordContext& ctxt, 
				 IntervalUnit intervalUnit,
				 bool nullable)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);

  md5_byte_t digest[16];
  FieldType::FieldTypeEnum f=FieldType::INTERVAL;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = native_type_size;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_append(&md5, (const md5_byte_t *) &intervalUnit, sizeof(intervalUnit));
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new IntervalType(ctxt, nullable, intervalUnit);
    ctxt.add(d, ft);
  }
  return (IntervalType *)ft;
}

IntervalType::~IntervalType()
{
}

std::string NilType::toString() const
{
  return "NULL";
}

NilType * NilType::Get(DynamicRecordContext& ctxt)
{
  FieldType * ft = NULL;
  md5_state_t md5;
  md5_init(&md5);
  FieldType::FieldTypeEnum f=FieldType::NIL;
  md5_append(&md5, (const md5_byte_t *) &f, sizeof(f));
  int32_t sz = 0;
  md5_append(&md5, (const md5_byte_t *) &sz, sizeof(sz));
  bool nullable= true;
  md5_append(&md5, (const md5_byte_t *) &nullable, sizeof(nullable));
  md5_byte_t digest[16];
  md5_finish(&md5, digest);
  Digest d(digest);
  if ((ft=ctxt.lookup(d)) == NULL) {
    ft = new NilType(ctxt);
    ctxt.add(d, ft);
  }
  return (NilType *)ft;
}

NilType::~NilType()
{
}

void BitcpyOp::coalesce(std::vector<BitcpyOp>& input, std::vector<BitcpyOp>& output)
{
  if (input.size() == 0) return;
  // Sort inputs on source offset, target offset and shift
  std::sort(input.begin(), input.end());

  // Memory of previous offset
  FieldAddress prevSourceOffset = input.front().mSourceOffset;
  FieldAddress prevTargetOffset = input.front().mTargetOffset;
  int32_t prevShift = input.front().mShift;  
  uint32_t totalMask = input.front().mSourceBitmask;
  for(std::vector<BitcpyOp>::iterator it = input.begin()+1;
      it != input.end();
      ++it) {
    if(prevSourceOffset == it->mSourceOffset && 
       prevTargetOffset == it->mTargetOffset &&
       prevShift == it->mShift) {
      totalMask |= it->mSourceBitmask;
    } else if (totalMask != 0) {
      // No more opportunity to coalese
      output.push_back(BitcpyOp(prevSourceOffset, prevTargetOffset, 
				prevShift, totalMask));
      prevSourceOffset = it->mSourceOffset;
      prevTargetOffset = it->mTargetOffset;
      prevShift = it->mShift;
      totalMask = it->mSourceBitmask;
    }
  }

  // Handle final op.
  if (totalMask != 0) {
      output.push_back(BitcpyOp(prevSourceOffset, prevTargetOffset, 
				prevShift, totalMask));
  }    
}

void BitsetOp::coalesce(std::vector<BitsetOp>& input, std::vector<BitsetOp>& output)
{
  if (input.size() == 0) return;
  // Sort inputs on source offset, target offset and shift
  std::sort(input.begin(), input.end());

  // Memory of previous offset
  FieldAddress prevTargetOffset = input.front().mTargetOffset;
  uint32_t totalMask = input.front().mTargetBitmask;
  for(std::vector<BitsetOp>::iterator it = input.begin()+1;
      it != input.end();
      ++it) {
    if(prevTargetOffset == it->mTargetOffset) {
      totalMask |= it->mTargetBitmask;
    } else if (totalMask != 0) {
      // No more opportunity to coalese
      output.push_back(BitsetOp(prevTargetOffset, totalMask));
      prevTargetOffset = it->mTargetOffset;
      totalMask = it->mTargetBitmask;
    }
  }

  // Handle final op.
  if (totalMask != 0) {
      output.push_back(BitsetOp(prevTargetOffset, totalMask));
  }    
}

void MemcpyOp::coalesce(std::vector<MemcpyOp>& input, std::vector<MemcpyOp>& output)
{
  if (input.size() == 0) return;
  // Sort inputs on source offset
  std::sort(input.begin(), input.end());

  // Memory of previous offset
  FieldAddress prevSourceOffset = input.front().mSourceOffset;
  FieldAddress prevTargetOffset = input.front().mTargetOffset;
  size_t totalSize = input.front().mSize;
  for(std::vector<MemcpyOp>::iterator it = input.begin()+1;
      it != input.end();
      ++it) {
    if(prevSourceOffset.contiguous(it->mSourceOffset, totalSize) && 
       prevTargetOffset.contiguous(it->mTargetOffset, totalSize)) {
      totalSize += it->mSize;
    } else if (totalSize > 0) {
      // No more opportunity to coalese
      output.push_back(MemcpyOp(prevSourceOffset, prevTargetOffset, totalSize));
      prevSourceOffset = it->mSourceOffset;
      prevTargetOffset = it->mTargetOffset;
      totalSize = it->mSize;
    }
  }

  // Handle final op.
  if (totalSize > 0) {
      output.push_back(MemcpyOp(prevSourceOffset, prevTargetOffset, totalSize));
  }    
}

void MemsetOp::coalesce(std::vector<MemsetOp>& input, std::vector<MemsetOp>& output)
{
  if (input.size() == 0) return;
  // Sort inputs on source offset
  std::sort(input.begin(), input.end());

  // Memory of previous offset
  FieldAddress prevSourceOffset=input.front().mSourceOffset;
  int prevValue = input.front().mValue;
  size_t totalSize = input.front().mSize;
  for(std::vector<MemsetOp>::iterator it = input.begin()+1;
      it != input.end();
      ++it) {
    if(prevSourceOffset.contiguous(it->mSourceOffset, it->mSize) && prevValue == it->mValue) {
      totalSize += it->mSize;
    } else if (totalSize > 0) {
      // No more opportunity to coalese
      output.push_back(MemsetOp(prevSourceOffset, prevValue, totalSize));
      prevSourceOffset = it->mSourceOffset;
      prevValue = it->mValue;
      totalSize = it->mSize;
    }
  }

  // Handle final op.
  if (totalSize > 0) {
      output.push_back(MemsetOp(prevSourceOffset, prevValue, totalSize));
  }    
}

RecordTypeMove::RecordTypeMove(const RecordType * source, const RecordType * target) 
{
  std::vector<MemcpyOp> fieldOps;
  std::vector<MemsetOp> fieldClearOps;
  // Right now move by matching on name.
  for(RecordType::const_member_iterator sourceIt = source->begin_members();
      sourceIt != source->end_members();
      ++sourceIt) {
    RecordType::const_member_name_iterator targetNameIt = target->mMemberNames.find(sourceIt->GetName());
    if (targetNameIt != target->mMemberNames.end()) {
      // TODO: Validate that data types match
      std::size_t sz = (sourceIt + 1) == source->end_members() ?
	sourceIt->GetType()->GetAllocSize() :
	(std::size_t) (source->mMemberOffsets[sourceIt + 1 - source->begin_members()] - 
		       source->mMemberOffsets[sourceIt - source->begin_members()]);
      fieldOps.push_back(MemcpyOp(source->mMemberOffsets[sourceIt - source->begin_members()],
				  target->mMemberOffsets[targetNameIt->second],
				  sz));
      fieldClearOps.push_back(MemsetOp(source->mMemberOffsets[sourceIt - source->begin_members()],
				       0,
				       sz));
    }    
  }

  // Coalesce into minimal number of memcpy's and memset's
  MemcpyOp::coalesce(fieldOps, mMemcpy);
  MemsetOp::coalesce(fieldClearOps, mMemset);
}

RecordTypeCopy::RecordTypeCopy(const RecordType * source, 
			       const RecordType * target,
			       const std::string& sourceRegex,
			       const std::string& targetFormat,
			       int * pos) 
{
  // These are the columns we copy
  boost::regex ex(sourceRegex);
  std::vector<BitcpyOp> fieldBitCpyOps;
  std::vector<BitsetOp> fieldBitSetOps;
  std::vector<MemcpyOp> fieldOps;
  // Assumption is that we copy the source starting at field position
  // pos in the target.  Because this is a copy we cannot memcpy Varchar
  // fields.
  for(RecordType::const_member_iterator sourceIt = source->begin_members();
      sourceIt != source->end_members();
      ++sourceIt) {
    if (!boost::regex_match(sourceIt->GetName().c_str(), ex)) 
      continue;
    std::size_t sourcePos = (std::size_t) (sourceIt - source->begin_members());
    // Copy or set NULL bits as necessary
    if (target->hasNullFields()) {
      if (sourceIt->GetType()->isNullable()) {
	int32_t shift = int32_t((*pos)%32) - int32_t(sourcePos%32);	
	fieldBitCpyOps.push_back(BitcpyOp(source->mMemberOffsets[sourcePos].getBitwordAddress(),
					  target->mMemberOffsets[*pos].getBitwordAddress(),
					  shift,
					  1U << (sourcePos%32)));
      } 
    }
    // TODO: Validate that data types match
    if (sourceIt->GetType()->GetEnum() != FieldType::VARCHAR) {
      std::size_t sz = (sourceIt + 1) == source->end_members() ?
	sourceIt->GetType()->GetAllocSize() :
	(std::size_t) (source->mMemberOffsets[sourcePos + 1] - 
		       source->mMemberOffsets[sourcePos]);
      fieldOps.push_back(MemcpyOp(source->mMemberOffsets[sourcePos],
				  target->mMemberOffsets[*pos],
				  sz));
    } else {
      // Set field : target is identified positionally, source by name
      mSet.push_back(std::make_pair(*sourceIt, *pos));
    }
    *pos += 1;
  }    

  // TODO: Replace whole word Bitcpy with shift zero by a memcpy.
  BitcpyOp::coalesce(fieldBitCpyOps, mBitcpy);
  // TODO: Replace whole word Bitset by a memset
  BitsetOp::coalesce(fieldBitSetOps, mBitset);
  // Coalesce into minimal number of memcpy's 
  MemcpyOp::coalesce(fieldOps, mMemcpy);
}

void TaggedFieldAddress::printEscaped(const char * begin, int32_t sz, 
				      char escapeChar, std::ostream& ostr)
{
  if (escapeChar != 0) {
    const char * it = begin;
    const char * end = begin + sz;
    for(; it!=end; it++) {
      switch(*it) {
      case '\n':
	ostr << escapeChar << 'n';
	break;
      case '\b':
	ostr << escapeChar << 'b';
	break;
      case '\f':
	ostr << escapeChar << 'f';
	break;
      case '\t':
	ostr << escapeChar << 't';
	break;
      case '\r':
	ostr << escapeChar << 'r';
	break;
      case '\\':
	ostr << escapeChar << '\\';
	break;
      default:
	ostr << *it;
	break;
      }
    }
  } else {
    // Raw output
    ostr << begin;
  }
}

void TaggedFieldAddress::print(RecordBuffer buf, 
			       char arrayDelimiter,
			       char escapeChar,
			       std::ostream& ostr) const
{
  // Handle NULLs 
  if (mAddress.isNull(buf)) {
    ostr << "\\N";
    return;
  }

  switch(mTag) {
  case FieldType::VARCHAR:
    {
      const char * begin = mAddress.getVarcharPtr(buf)->c_str();
      int32_t sz = mAddress.getVarcharPtr(buf)->size();
      printEscaped(begin, sz, escapeChar, ostr);
    }
    break;
  case FieldType::CHAR:
    {
      const char * begin = mAddress.getCharPtr(buf);
      // TODO: Pad to static length
      int32_t sz = ::strlen(begin);
      printEscaped(begin, sz, escapeChar, ostr);
    }
    break;
  case FieldType::BIGDECIMAL:
    {
      char buffer[DECIMAL128_String];
      if(0 == mSize) {
        decimal128ToString(mAddress.getDecimalPtr(buf), &buffer[0]);
        ostr << buffer;
      } else {
        ostr << "[";
        for(uint32_t i=0; i<mSize; ++i) {
          if (i>0) {
            ostr << ",";
          }
          decimal128ToString(mAddress.getArrayDecimalPtr(buf, i), &buffer[0]);
          ostr << buffer;
        }
        ostr << "]";
      }
      break;
    }
  case FieldType::INT8:
    if(0 == mSize) {
      ostr << (int32_t) mAddress.getInt8(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << (int32_t) mAddress.getArrayInt8(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::INT16:
    if(0 == mSize) {
      ostr << mAddress.getInt16(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayInt16(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::INT32:
    if(0 == mSize) {
      ostr << mAddress.getInt32(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayInt32(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::INT64:
    if(0 == mSize) {
      ostr << mAddress.getInt64(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayInt64(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::FLOAT:
    if(0 == mSize) {
      ostr << mAddress.getFloat(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayFloat(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::DOUBLE:
    if(0 == mSize) {
      ostr << mAddress.getDouble(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayDouble(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::DATETIME:
    if(0 == mSize) {
      ostr << mAddress.getDatetime(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayDatetime(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::DATE:
    if(0 == mSize) {
      ostr << mAddress.getDate(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayDate(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::IPV4:
    if(0 == mSize) {
      ostr << mAddress.getIPv4(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayIPv4(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::CIDRV4:
    if(0 == mSize) {
      ostr << mAddress.getCIDRv4(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayCIDRv4(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::IPV6:
    if(0 == mSize) {
      ostr << mAddress.getIPv6(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayIPv6(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::CIDRV6:
    if(0 == mSize) {
      ostr << mAddress.getCIDRv6(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayCIDRv6(buf, i);
      }
      ostr << "]";
    }
    break;
  case FieldType::NIL:
    ostr << "NULL";
    break;
  case FieldType::INTERVAL:
    if(0 == mSize) {
      ostr << mAddress.getInt32(buf);
    } else {
      ostr << "[";
      for(uint32_t i=0; i<mSize; ++i) {
        if (i>0) {
          ostr << ",";
        }
        ostr << mAddress.getArrayInt32(buf, i);
      }
      ostr << "]";
    }
    break;
  default:
    break;
  }  
}

RecordTypePrint::RecordTypePrint(const std::vector<TaggedFieldAddress>& fields)
  :
  mFields(fields),
  mFieldDelimiter('\t'),
  mRecordDelimiter('\n'),
  mArrayDelimiter(','),
  mEscapeChar('\\')
{
}

RecordTypePrint::RecordTypePrint(const std::vector<TaggedFieldAddress>& fields,
				 char fieldDelimiter, char recordDelimiter,
				 char arrayDelimiter, char escapeChar)
  :
  mFields(fields),
  mFieldDelimiter(fieldDelimiter),
  mRecordDelimiter(recordDelimiter),
  mArrayDelimiter(arrayDelimiter),
  mEscapeChar(escapeChar)
{
}

RecordTypePrint::RecordTypePrint()
  :
  mFieldDelimiter('\t'),
  mRecordDelimiter('\n'),
  mEscapeChar('\\')
{
}

RecordTypePrint::~RecordTypePrint()
{
}

void RecordTypePrint::imbue(std::ostream& ostr) const
{
  // stream takes ownership of the facet.
  boost::posix_time::time_facet * facet =
    new boost::posix_time::time_facet("%Y-%m-%d %H:%M:%S");
  boost::gregorian::date_facet * dateFacet =
    new boost::gregorian::date_facet("%Y-%m-%d");
  ostr.imbue(std::locale(std::locale(ostr.getloc(), facet), dateFacet));
  ostr << std::fixed << std::setprecision(9);
}

void RecordTypePrint::print(RecordBuffer buf, std::ostream& ostr, bool emitNewLine) const
{
  for(std::vector<TaggedFieldAddress>::const_iterator it = mFields.begin();
      it != mFields.end();
      ++it) {
    if (mFields.begin() != it) ostr << mFieldDelimiter;
    it->print(buf, mArrayDelimiter, mEscapeChar, ostr);    
  }
  if (emitNewLine)
    ostr << mRecordDelimiter;
}

RecordTypeSerialize::RecordTypeSerialize()
  :
  mSize(0)
{
}

RecordTypeSerialize::RecordTypeSerialize(std::size_t sz, const std::vector<FieldAddress>& offsets)
  :
  mSize(sz),
  mOffsets(offsets)
{
}

RecordTypeSerialize::~RecordTypeSerialize()
{
}

bool RecordTypeSerialize::doit(uint8_t * & output, uint8_t * outputEnd, RecordBufferIterator & inputPos, RecordBuffer buf) const
{
  while(inputPos.offset <= mOffsets.size()) {
    std::ptrdiff_t outputAvail = (outputEnd-output);
    std::ptrdiff_t inputAvail = 0;
    if (inputPos.offset == 0) {
      inputAvail = (buf.Ptr+mSize - inputPos.ptr);
    } else {
      Varchar * v = mOffsets[inputPos.offset-1].getVarcharPtr(buf);
      BOOST_ASSERT(v->Large.Large);
      // Be careful to handle NULL terminator.
      inputAvail = ((uint8_t *)v->Large.Ptr + v->Large.Size + 1 - inputPos.ptr);
    }
    if (inputAvail > outputAvail) {
      memcpy(output, inputPos.ptr, outputAvail);
      inputPos.ptr += outputAvail;
      output += outputAvail;
      return false;
    } else {
      memcpy(output, inputPos.ptr, inputAvail);
      inputPos.ptr += inputAvail;
      output += inputAvail;

      // Advance to next varchar, skip over any NULL
      // strings and any Small strings
      while(1) {
	inputPos.offset++;
	if (mOffsets.size()<inputPos.offset) {
	  inputPos.ptr = NULL;
	  break;
	} else if (!mOffsets[inputPos.offset-1].isNull(buf) &&
		   mOffsets[inputPos.offset-1].getVarcharPtr(buf)->Large.Large) {
	  inputPos.ptr = 
	    (uint8_t *) mOffsets[inputPos.offset-1].getVarcharPtr(buf)->Large.Ptr;
	  break;
	}
      }
    }
  }
  return true;
}

std::size_t RecordTypeSerialize::getRecordLength(RecordBuffer buf) const
{
  std::size_t sz = mSize;
  for(std::vector<FieldAddress>::const_iterator it = mOffsets.begin();
      it != mOffsets.end();
      ++it) {
    sz += std::size_t(it->getVarcharPtr(buf)->Large.Large ?
		      it->getVarcharPtr(buf)->Large.Size : 0);
  }
  return sz;
}

RecordTypeDeserialize::RecordTypeDeserialize()
  :
  mSize(0)
{
}

RecordTypeDeserialize::RecordTypeDeserialize(std::size_t sz, const std::vector<FieldAddress>& offsets)
  :
  mSize(sz),
  mOffsets(offsets)
{
}

RecordTypeDeserialize::~RecordTypeDeserialize()
{
}

// TODO: Proper constness
bool RecordTypeDeserialize::Do(uint8_t * & input, uint8_t * inputEnd, RecordBufferIterator & outputPos, RecordBuffer buf) const
{
  while(outputPos.offset <= mOffsets.size()) {
    std::ptrdiff_t inputAvail = (inputEnd - input);
    std::ptrdiff_t outputAvail = 0;
    if (outputPos.offset == 0) {
      outputAvail = (buf.Ptr+mSize-outputPos.ptr);
    } else {
      Varchar * v = mOffsets[outputPos.offset-1].getVarcharPtr(buf);
      BOOST_ASSERT(v->Large.Large);
      // Be careful to handle NULL terminator.
      outputAvail = ((uint8_t *)v->Large.Ptr + v->Large.Size + 1 - outputPos.ptr);
    }
    if (inputAvail >= outputAvail) {
      memcpy(outputPos.ptr, input, outputAvail);
      input += outputAvail;
      outputPos.ptr += outputAvail;

      // Move to next non NULL Large VARCHAR
      while(1) {
	outputPos.offset++;
	if (outputPos.offset <= mOffsets.size()) {
	  // Allocate memory and initialize pointer to it if not NULL and Large
	  // Size of string and NULL bit already copied from deserialized record.
	  if (!mOffsets[outputPos.offset-1].isNull(buf) &&
	      mOffsets[outputPos.offset-1].getVarcharPtr(buf)->Large.Large) {
	    Varchar * v = mOffsets[outputPos.offset-1].getVarcharPtr(buf);
	    char * tmp = (char *) ::malloc(v->Large.Size + 1);
	    outputPos.ptr = (uint8_t *) tmp;
	    v->Large.Ptr = tmp;
	    break;
	  }
	} else {
	  outputPos.ptr = NULL;
	  break;
	}
      }
    } else {
      memcpy(outputPos.ptr, input, inputAvail);
      input += inputAvail;
      outputPos.ptr += inputAvail;
      return false;
    }
  }
  return true;
}

RecordTypeFree::RecordTypeFree()
  :
  mSize(0)
{
}

RecordTypeFree::RecordTypeFree(std::size_t sz, const std::vector<FieldAddress>& offsets)
  :
  mOffsets(offsets),
  mSize(sz)
{
}

RecordTypeFree::~RecordTypeFree()
{
}

void RecordTypeFree::free(RecordBuffer & buf) const
{
  if (buf.Ptr == NULL) return;

  for(std::vector<FieldAddress>::const_iterator it = mOffsets.begin();
      it != mOffsets.end();
      ++it) {
    // Free
    if (it->getVarcharPtr(buf)->Large.Large) {
      ::free(const_cast<char *>(it->getVarcharPtr(buf)->Large.Ptr));
    }
  }
  RecordBuffer::free(buf);
}

RecordTypeMalloc::RecordTypeMalloc(std::size_t sz)
  :
  mSize(sz)
{
}

RecordTypeMalloc::~RecordTypeMalloc()
{
}

RecordBuffer RecordTypeMalloc::malloc() const
{
  return RecordBuffer::malloc(mSize);
}

const RecordType * RecordType::get(DynamicRecordContext & ctxt,
				   const std::vector<RecordMember>& members)
{
  const RecordType * tmp = new RecordType(members);
  ctxt.add(tmp);
  return tmp;
}

RecordType::RecordType(const std::vector<RecordMember>& members)
  :
  mMembers(members),
  mHasNullFields(false)
{
  for(const_member_iterator it = begin_members();
      it != end_members();
      ++it) {
    if (mMemberNames.end() != mMemberNames.find(it->GetName())) {
      std::string msg = (boost::format("Duplicate field name %1%"
				       " in record") % it->GetName()).str();
      throw std::runtime_error(msg);
    }
    mMemberNames[it->GetName()] = std::size_t(it - begin_members());
    if (it->GetType()->isNullable()) {
      mHasNullFields = true;
    }
  }
  // For simplicity, we allocate a NULL bit for every field
  // provided any are nullable.  We allocate our null bit field in
  // 32 bit chunks to speed up certain operations.  No space issues
  // with this for many records due to alignment.
  std::size_t sz= mHasNullFields ? ((mMembers.size()+31)/32)*sizeof(uint32_t) : 0;
  std::vector<FieldAddress> offsets;
  std::vector<TaggedFieldAddress> taggedOffsets;
  for(const_member_iterator it = begin_members();
      it != end_members();
      ++it) {
    // uint32_t pos = mHasNullFields ? (uint32_t) (it - begin_members()) : 0xffffffff;
    uint32_t pos = it->GetType()->isNullable() 
      ? (uint32_t) (it - begin_members()) : 0xffffffff;
    // Round up to alignment
    sz = it->GetType()->GetAlignment()*((sz + it->GetType()->GetAlignment() - 1)/it->GetType()->GetAlignment());
    if (sz > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error ("Record length exceeds maximum size");
    }
    uint32_t sz32 = (uint32_t) sz;
    mByteOffsetToPosition[sz] = pos;
    mMemberOffsets.push_back(FieldAddress(sz32, pos));
    taggedOffsets.push_back(TaggedFieldAddress(mMemberOffsets.back(), it->GetType()->GetEnum()));
    if (FieldType::VARCHAR == it->GetType()->GetEnum()) {
      offsets.push_back(FieldAddress(sz32, pos));
    }
    
    sz += it->GetType()->GetAllocSize();
  }

  mMalloc = boost::shared_ptr<RecordTypeMalloc>(new RecordTypeMalloc(sz));
  mFree = boost::shared_ptr<RecordTypeFree>(new RecordTypeFree(sz, offsets));
  mSerialize = boost::shared_ptr<RecordTypeSerialize>(new RecordTypeSerialize(sz, offsets));
  mDeserialize = boost::shared_ptr<RecordTypeDeserialize>(new RecordTypeDeserialize(sz, offsets));
  mPrint = boost::shared_ptr<RecordTypePrint>(new RecordTypePrint(taggedOffsets));
}

RecordType::~RecordType()
{
}

const RecordTypeMalloc * RecordType::GetMalloc() const
{
  return mMalloc.get();
}

const RecordTypeFree * RecordType::GetFree() const
{
  return mFree.get();
}

std::string RecordType::dumpTextFormat() const
{
  std::string ret;
  for(const_member_iterator it = begin_members();
      it != end_members();
      ++it) {
    if (begin_members() != it) ret += ",";
    ret += it->GetName();
    ret += " ";
    ret += it->GetType()->toString();
    if (it->GetType()->isNullable()) {
      ret += " NULL";
    } 
  }
  return ret;
}

void RecordType::dump() const
{
  std::cout << "{";
  for(const_member_iterator it = begin_members();
      it != end_members();
      ++it) {
    if (begin_members() != it) std::cout << ", ";
    std::cout << "[name=\"" << it->GetName().c_str() << "\", type=" << it->GetType()->GetEnum() << ",offset=";
    mMemberOffsets[it - begin_members()].dump();
    std::cout << "]";
  }
  std::cout << "}" << std::endl;
}

llvm::Value * RecordType::LLVMMemberGetPointer(const std::string& member, 
					       CodeGenerationContext * ctxt, 
					       llvm::Value * basePointer,
					       bool populateSymbolTable,
					       const char * prefix) const
{
  llvm::IRBuilder<> * builder = ctxt->LLVMBuilder;
  // Find the member.
  const_member_name_iterator it = mMemberNames.find(member);
  if (it == mMemberNames.end()) {
    throw std::runtime_error((boost::format("Undefined variable: %1%") % member).str());
  }
  llvm::Value * memberVal = ctxt->LLVMBuilder->CreateBitCast(mMemberOffsets[it->second].getPointer(member, 
												   ctxt, 
												   builder->CreateLoad(builder->getInt8PtrTy(), basePointer, "baseref")),
							    llvm::PointerType::get(mMembers[it->second].GetType()->LLVMGetType(ctxt), 0),
							    member.c_str());
  if (populateSymbolTable) {
    ctxt->defineFieldVariable(basePointer,
			      prefix,
			      member.c_str(),
			      this);
  }

  return memberVal;
}

// Is the LLVM value a pointer to a member of this record?
bool RecordType::isMemberPointer(llvm::Value * val,
				 llvm::Value * basePointer,
				 FieldAddress & addr) const
{
  // Members are bitcast of a untyped pointer to an offset
  // from base:
  // bitcast <i8*> getelementptr i8* load <i8**> $base $offset to <ty *>
  if (llvm::BitCastInst * bcast = llvm::dyn_cast<llvm::BitCastInst>(val)) {
    if(llvm::GetElementPtrInst * gep = 
       llvm::dyn_cast<llvm::GetElementPtrInst>(bcast->getOperand(0))) {
      // OK.  Could be a winner, look more closely.
      const llvm::Value * pointer = gep->getPointerOperand();
      if (const llvm::LoadInst * load = llvm::dyn_cast<llvm::LoadInst>(pointer)) {
	pointer = load->getOperand(0);
	bool hasConstantIndices = gep->hasAllConstantIndices();
	unsigned numIndices = gep->getNumIndices();
	if (pointer == basePointer &&
	    hasConstantIndices &&
	    numIndices == 1) {
	  if (llvm::ConstantInt * idx =
	      llvm::dyn_cast<llvm::ConstantInt>(*gep->idx_begin())) {
	    // We have a winner!  
	    // TODO: Extra sanity check that this is a valid
	    // offset and that the bitcast type convert is compatible
	    // with the type of the corresponding field.
	    std::map<uint32_t,uint32_t>::const_iterator posIt =
	      mByteOffsetToPosition.find(idx->getValue().getSExtValue());
	    if (posIt != mByteOffsetToPosition.end()) {
	      addr = FieldAddress(posIt->first, posIt->second);
	      return true;
	    }
	  }
	}
      }
    }
  }
  return false;
}

bool RecordType::operator==(const RecordType & rhs) const
{
  if (size() != rhs.size()) return false;
  // Should we worry about names or just types?
  for(std::size_t i=0; i<mMembers.size(); ++i) {
    if (mMembers[i].GetType() != rhs.mMembers[i].GetType()) return false;
  }
  return true;
}

llvm::Value * RecordType::LLVMMemberGetNull(const std::string& member, CodeGenerationContext * ctxt, llvm::Value * basePointer) const
{
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  // Find the member.
  const_member_name_iterator it = mMemberNames.find(member);
  if (it == mMemberNames.end()) {
    throw std::runtime_error((boost::format("Undefined variable: %1%") % member).str());
  }
  return mMemberOffsets[it->second].isNull(member, 
					   ctxt, 
					   b->CreateLoad(b->getInt8PtrTy(), basePointer, "baseref"));
}

void RecordType::LLVMMemberSetNull(const std::string& member, CodeGenerationContext * ctxt, llvm::Value * basePointer, bool isNull) const
{
  llvm::IRBuilder<> * b = ctxt->LLVMBuilder;
  // Find the member.
  const_member_name_iterator it = mMemberNames.find(member);
  if (it == mMemberNames.end()) {
    throw std::runtime_error((boost::format("Undefined variable: %1%") % member).str());
  }
  return mMemberOffsets[it->second].setNull(member, 
					    ctxt, 
					    b->CreateLoad(b->getInt8PtrTy(), basePointer, "baseref"),
					    isNull);
}

const RecordMember& RecordType::GetMember(int32_t idx) const
{
  return mMembers[idx];
}

void RecordType::setInt8(const std::string& field, int8_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setInt8(val, buf);
}

void RecordType::setArrayInt8(const std::string& field, int32_t idx, int8_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  mMemberOffsets[it->second].clearArrayNull(buf, dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType()), idx);
  *mMemberOffsets[it->second].getArrayInt8Ptr(buf, idx) = val;
}

void RecordType::setInt16(const std::string& field, int16_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setInt16(val, buf);
}

void RecordType::setArrayInt16(const std::string& field, int32_t idx, int16_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  mMemberOffsets[it->second].clearArrayNull(buf, dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType()), idx);
  *mMemberOffsets[it->second].getArrayInt16Ptr(buf, idx) = val;
}

void RecordType::setInt32(const std::string& field, int32_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setInt32(val, buf);
}

void RecordType::setArrayInt32(const std::string& field, int32_t idx, int32_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  mMemberOffsets[it->second].clearArrayNull(buf, dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType()), idx);
  *mMemberOffsets[it->second].getArrayInt32Ptr(buf, idx) = val;
}

void RecordType::setInt64(const std::string& field, int64_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setInt64(val,buf);
}

void RecordType::setArrayInt64(const std::string& field, int32_t idx, int64_t val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  mMemberOffsets[it->second].clearArrayNull(buf, dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType()), idx);
  *mMemberOffsets[it->second].getArrayInt64Ptr(buf, idx) = val;
}

void RecordType::setFloat(const std::string& field, float val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setFloat(val,buf);
}

void RecordType::setArrayFloat(const std::string& field, int32_t idx, float val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  mMemberOffsets[it->second].clearArrayNull(buf, dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType()), idx);
  *mMemberOffsets[it->second].getArrayFloatPtr(buf, idx) = val;
}

void RecordType::setDouble(const std::string& field, double val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setDouble(val,buf);
}

void RecordType::setArrayDouble(const std::string& field, int32_t idx, double val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  mMemberOffsets[it->second].clearArrayNull(buf, dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType()), idx);
  *mMemberOffsets[it->second].getArrayDoublePtr(buf, idx) = val;
}

void RecordType::setDatetime(const std::string& field, 
			     boost::posix_time::ptime val, 
			     RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setDatetime(val,buf);
}

void RecordType::setDate(const std::string& field, 
			 boost::gregorian::date val, 
			 RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setDate(val,buf);
}

void RecordType::setIPv4(const std::string& field, boost::asio::ip::address_v4 val, RecordBuffer buffer) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setIPv4(val,buffer);
}

void RecordType::setCIDRv4(const std::string& field, CidrV4 val, RecordBuffer buffer) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setCIDRv4(val,buffer);
}

void RecordType::setIPv6(const std::string& field, const boost::asio::ip::address_v6 & val, RecordBuffer buffer) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setIPv6(val,buffer);
}

void RecordType::setCIDRv6(const std::string& field, CidrV6 val, RecordBuffer buffer) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].setCIDRv6(val,buffer);
}

void RecordType::setVarchar(const std::string& field, const char * val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  std::size_t sz = strlen(val);
  mMemberOffsets[it->second].SetVariableLengthString(buf, val, sz);
}

void RecordType::setChar(const std::string& field, const char * val, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  mMemberOffsets[it->second].clearNull(buf);
  char * ptr = mMemberOffsets[it->second].getCharPtr(buf);
  int32_t sz = strlen(val);
  sz = std::min(sz, static_cast<const CharType *>(mMembers[it->second].GetType())->GetSize());
  memcpy(ptr, val, sz);
  ptr[sz] = 0;
  
}

int8_t RecordType::getInt8(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getInt8(buf);
}

int8_t RecordType::getArrayInt8(const std::string& field, int32_t idx, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getArrayInt8(buf, idx);
}

int16_t RecordType::getInt16(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getInt16(buf);
}

int16_t RecordType::getArrayInt16(const std::string& field, int32_t idx, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getArrayInt16(buf, idx);
}

int32_t RecordType::getInt32(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getInt32(buf);
}

int32_t RecordType::getArrayInt32(const std::string& field, int32_t idx, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getArrayInt32(buf, idx);
}

int64_t RecordType::getInt64(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getInt64(buf);
}

int64_t RecordType::getArrayInt64(const std::string& field, int32_t idx, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getArrayInt64(buf, idx);
}

float RecordType::getFloat(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getFloat(buf);
}

double RecordType::getDouble(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getDouble(buf);
}

Varchar * RecordType::getVarcharPtr(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getVarcharPtr(buf);
}

boost::asio::ip::address_v4 RecordType::getIPv4(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getIPv4(buf);
}

CidrV4 RecordType::getCIDRv4(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getCIDRv4(buf);
}

boost::asio::ip::address_v6 RecordType::getIPv6(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getIPv6(buf);
}

CidrV6 RecordType::getCIDRv6(const std::string& field, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  return mMemberOffsets[it->second].getCIDRv6(buf);
}

bool RecordType::isArrayNull(const std::string& field, int32_t idx, RecordBuffer buf) const
{
  const_member_name_iterator it = mMemberNames.find(field);
  const FixedArrayType * ft = dynamic_cast<const FixedArrayType *>(mMembers[it->second].GetType());
  return mMemberOffsets[it->second].isArrayNull(buf, ft, idx);
}
