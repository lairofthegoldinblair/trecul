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

#ifndef __TYPECHECKCONTEXT_HH__
#define __TYPECHECKCONTEXT_HH__

#include <map>
#include <stack>
#include <string>
#include <boost/dynamic_bitset.hpp>

class FieldType;
class IQLToLLVMLValue;
class RecordType;
class RecordMemberList;

class TypeCheckConfiguration
{
private:
  bool mCaseInsensitive;
public:
  TypeCheckConfiguration(bool caseInsensitive=false)
    :
    mCaseInsensitive(caseInsensitive)
  {
  }

  void caseInsensitive(bool val)
  {
    mCaseInsensitive = val;
  }

  bool caseInsensitive() const
  {
    return mCaseInsensitive;
  }

  // TODO: For the moment lets make this global.   
  static TypeCheckConfiguration & get()
  {
    static TypeCheckConfiguration config;
    return config;
  }
};

class AliasedRecordType
{
private:
  std::string mAlias;
  const RecordType * mType;
public:
  // Compiler generated copy c'tor and assignment OK.
  AliasedRecordType(const std::string& alias, const RecordType * ty)
    :
    mAlias(alias),
    mType(ty)
  {
  }
  ~AliasedRecordType()
  {
  }
  const std::string& getAlias() const { return mAlias; }
  const RecordType * getType() const { return mType; }
};

class TreculSymbolTableEntry
{
private:
  const FieldType * mType;
  IQLToLLVMLValue * mValue;
public:
  TreculSymbolTableEntry();
  TreculSymbolTableEntry(const FieldType * ft);
  TreculSymbolTableEntry(IQLToLLVMLValue * val);
  TreculSymbolTableEntry(const FieldType * ft, IQLToLLVMLValue * val);
  ~TreculSymbolTableEntry();
  const FieldType * getType() const;
  IQLToLLVMLValue * getValue() const;
};

/**
 * The symbol table allows for two part names of variables with an optional 
 * prefix.  If a variable name is unique without its prefix then it may be referenced
 * without it, otherwise there is an ambiguous reference.  In all cases it is 
 * permissible to reference a field with its fully qualified name.
 */
class TreculSymbolTable
{
public:
  typedef std::map<std::string, TreculSymbolTableEntry *>::iterator table_iterator;
  typedef std::map<std::string, TreculSymbolTableEntry *>::const_iterator table_const_iterator;
private:
  // Lookup by two part name.
  std::map<std::string, TreculSymbolTableEntry *> mNameLookup;
  // Lookup by unprefixed name. 
  std::map<std::string, TreculSymbolTableEntry *> mUnprefixedNameLookup;
  // Sentinel value for detecting ambiguous unprefixed names.
  TreculSymbolTableEntry mAmbiguous;
  // Is this symbol table case insensitive?
  bool mCaseInsensitive;

  std::string makeKey(const char * nm, const char * nm2) const;
public:
  TreculSymbolTable(const TypeCheckConfiguration & typeCheckConfig);
  ~TreculSymbolTable();
  void clear();  
  TreculSymbolTableEntry * lookup(const char * nm, const char * nm2);
  TreculSymbolTableEntry * lookup(const char * nm)
  {
    return lookup(nm, nullptr);
  }
  void add(const char * nm, const char * nm2, const FieldType * ft);
  void add(const char * nm, const char * nm2, IQLToLLVMLValue * val);
  void add(const char * nm, const char * nm2, 
	   const FieldType * ft, IQLToLLVMLValue * val);
  bool contains(const char * nm, const char * nm2) const;
};

class TypeCheckContext 
{
private:
  std::map<std::string, const RecordType *> mInputRecords;
  TreculSymbolTable mSymbolTable;
  TreculSymbolTable mAggregateTable;
  std::stack<const FieldType *> mCaseType;
public:
  typedef RecordMemberList member_list;
  class DynamicRecordContext& mContext;
private:
  const RecordType * mOutputRecord;
  TreculSymbolTable * mTypeCheckSymbolTable;
  TreculSymbolTable * mAggregateTypeCheckSymbolTable;
  TreculSymbolTable * mSaveTypeCheckSymbolTable;
  member_list * mRecordMembers;

  // One member per aggregate function.
  member_list * mAggregateMembers;
  // A record where aggregate functions
  // can be calculated.
  const RecordType * mAggregateRecord;

  /**
   * Type check and create an INTERVAL type constructor.
   * N.B.  Note use of int32_t for the unit argument. 
   * This is actually an IntervalType::IntervalUnit but I don't
   * want to include the header.
   * Someday we'll be able to forward declare the enum but right now
   * that functionality is bleeding edge  standard C++0x but not 
   * implemented in most compilers.
   */
  const FieldType * internalBuildInterval(const FieldType * ty, 
					  int32_t unit);

  bool isBuiltinFunction(const char * name);

  void init(const std::vector<AliasedRecordType>& sources,
	    const std::vector<boost::dynamic_bitset<> >& masks);

public:
  TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
		   class DynamicRecordContext & recCtxt);

  /**
   * Initialize type check context with a list of 
   * named Trecul record types (structs).
   * mask arguments indicate which fields of each input struct
   * to include in the environment.
   */
  TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
		   DynamicRecordContext & recCtxt,
		   const std::vector<AliasedRecordType>& sources);

  TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
		   DynamicRecordContext & recCtxt,
		   const std::vector<AliasedRecordType>& sources,
		   const std::vector<boost::dynamic_bitset<> >& masks);
		   
  /**
   * Initialize type check of aggregate functions.
   * When type checking the parameters of an aggregate
   * function the entire record is available to the calculation
   * but when type checking the expressions outside the scope
   * of an aggregate function only the group by keys are available.
   */
  TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
		   DynamicRecordContext & recCtxt,
		   const RecordType * input,
		   const std::vector<std::string>& groupKeys,
		   bool isOlap);

  ~TypeCheckContext();
  void loadBuiltinFunctions();

  /**
   * Retrieve the output of record construction.
   */
  const RecordType * getOutputRecord() 
  {
    return mOutputRecord;
  }

  /**
   * Retrieve the record where aggregate functions
   * may be built.
   */
  const RecordType * getAggregateRecord();

  /**
   * Set value statement
   */
  void buildSetValue(const FieldType * lhs,
		     const FieldType * rhs);

  /**
   * Switch statements
   */
  void beginSwitch(const FieldType * e);

  /**
   * Variable rvalue expression.
   */
  const FieldType * buildVariableRef(const char * nm);

  /**
   * Declare local variable.
   */
  void buildLocal(const char * nm, const FieldType * ft);

  /**
   * Array reference expression.
   */
  const FieldType * buildArray(const std::vector<const FieldType *>& e);
  const FieldType * buildArrayRef(const FieldType * arrayTy,
				  const FieldType * idx);

  /**
   * Struct reference expression.
   */
  const FieldType * buildStruct(const std::vector<const FieldType *>& e);
  const FieldType * buildStructRef(const FieldType * rowTy,
                                   const char * nm);

  /**
   * Function call 
   */
  const FieldType * buildCall(const char * f, std::vector<const FieldType *> & args);

  /**
   * Hashing and sorting
   */
  const FieldType * buildHash(std::vector<const FieldType *> & ty);
  const FieldType * buildSortPrefix(std::vector<const FieldType *> & ty);

  /**
   * Unary operations.
   */
  const FieldType * buildNegate(const FieldType * ty);

  /**
   * Binary operations.
   */
  const FieldType * buildAdd(const FieldType * lhs, 
			     const FieldType * rhs);
  const FieldType * buildSub(const FieldType * lhs, 
			     const FieldType * rhs);
  const FieldType * buildMul(const FieldType * lhs, 
			     const FieldType * rhs);
  const FieldType * buildDiv(const FieldType * lhs, 
			     const FieldType * rhs);
  const FieldType * buildModulus(const FieldType * lhs, 
				 const FieldType * rhs);
  const FieldType * buildConcat(const FieldType * lhs, 
                                const FieldType * rhs);
  const FieldType * buildBitwise(const FieldType * lhs, 
				 const FieldType * rhs);
  const FieldType * buildBitwise(const FieldType * lhs);

  /**
   * Logical operations
   */
  const FieldType * buildEquals(const FieldType * lhs, 
				const FieldType * rhs);
  const FieldType * buildCompare(const FieldType * lhs, 
				 const FieldType * rhs);
  const FieldType * buildLogicalAnd(const FieldType * lhs, 
				    const FieldType * rhs);
  const FieldType * buildLogicalNot(const FieldType * lhs);
  const FieldType * buildLike(const FieldType * lhs, 
			      const FieldType * rhs);

  /**
   * Cast/Conversion
   */
  const FieldType * buildCast(const FieldType * lhs, 
			      const FieldType * target);

  /**
   * Primitive types.
   */
  const FieldType * buildInt8Type(bool nullable = false);
  const FieldType * buildInt16Type(bool nullable = false);
  const FieldType * buildInt32Type(bool nullable = false);
  const FieldType * buildInt64Type(bool nullable = false);
  const FieldType * buildFloatType(bool nullable = false);
  const FieldType * buildDoubleType(bool nullable = false);
  const FieldType * buildDecimalType(bool nullable = false);
  const FieldType * buildDateType(bool nullable = false);
  const FieldType * buildDatetimeType(bool nullable = false);
  const FieldType * buildVarcharType();
  const FieldType * buildVarcharType(bool nullable);
  const FieldType * buildCharType(const char * sz, bool nullable = false);
  const FieldType * buildBooleanType(bool nullable = false);
  const FieldType * buildIPv4Type(const char * addr, bool nullable = false);
  const FieldType * buildIPv4Type(bool nullable = false);
  const FieldType * buildCIDRv4Type(bool nullable = false);
  const FieldType * buildIPv6Type(const char * addr, bool nullable = false);
  const FieldType * buildIPv6Type(bool nullable = false);
  const FieldType * buildCIDRv6Type(bool nullable = false);
  const FieldType * buildDecltypeType(const FieldType * expr, bool nullable = false);
  const FieldType * buildNilType();
  const FieldType * buildType(const char * typeName, bool nullable);

  /**
   * Array types
   */
  const FieldType * buildArrayType(const char * sz, const FieldType * elt, bool nullable = false);
  const FieldType * buildFixedArrayType(int32_t sz, const FieldType * elt, bool nullable = false);
  const FieldType * buildVariableArrayType(const FieldType * elt, bool nullable = false);
  
  /**
   * Struct (named and anonymous) types
   */
  const FieldType * buildStructType(const std::vector<const FieldType *> & elts, bool nullable = false);

  /**
   * Interval types.
   */
  const FieldType * buildIntervalDay(const FieldType * ty);
  const FieldType * buildIntervalHour(const FieldType * ty);
  const FieldType * buildIntervalMinute(const FieldType * ty);
  const FieldType * buildIntervalMonth(const FieldType * ty);
  const FieldType * buildIntervalSecond(const FieldType * ty);
  const FieldType * buildIntervalYear(const FieldType * ty);
  const FieldType * buildInterval(const char * intervalType, const FieldType * ty);

  /**
   * CASE expressions.
   */
  void beginCase();
  void addCondition(const FieldType * condVal);
  void addValue(const FieldType * thenVal);
  const FieldType * buildCase();

  /**
   * Ternary If/Then/Else
   */
  const FieldType * buildIfThenElse(const FieldType * condVal,
				    const FieldType * thenVal,
				    const FieldType * elseVal);

  /**
   * Record builder methods.
   */
  void beginRecord();
  void addField(const char * name, const FieldType * ty);
  void addFields(const char * recordName);
  void quotedId(const char * id, const char * format);
  void buildRecord();

  /**
   * Intrinsic functions
   */
  const FieldType * buildLeast(const std::vector<const FieldType *>& args);
  const FieldType * buildIsNull(const std::vector<const FieldType *>& args);

  /**
   * Aggregate function building.
   */
  void beginAggregateFunction();
  const FieldType * buildAggregateFunction(const FieldType * ty);

  /**
   * Automatic type promotion interface.
   */
  static const FieldType * castTo(const FieldType * lhs, 
				  const FieldType * rhs);

  static const FieldType * leastCommonType(const FieldType * e1, 
					   const FieldType * e2);

  static const FieldType * leastCommonTypeNullable(const FieldType * e1, 
						   const FieldType * e2);

  // Lookup a symbol
  const FieldType * lookupType(const char * nm);
};


#endif
