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

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/address_v6.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include "TypeCheckContext.hh"
#include "RecordType.hh"

static const char * fnPrefix = "$fn";


// This is just allowing us to avoid the RecordType.hh include.
class RecordMemberList : public std::vector<RecordMember>
{
public:
  RecordMemberList()  {}
};

TreculSymbolTableEntry::TreculSymbolTableEntry()
  :
  mType(NULL),
  mValue(NULL)
{
}

TreculSymbolTableEntry::TreculSymbolTableEntry(const FieldType * ft)
  :
  mType(ft),
  mValue(NULL)
{
}

TreculSymbolTableEntry::TreculSymbolTableEntry(IQLToLLVMLValue * val)
  :
  mType(NULL),
  mValue(val)
{
}

TreculSymbolTableEntry::TreculSymbolTableEntry(const FieldType * ft, 
					       IQLToLLVMLValue * val)
  :
  mType(ft),
  mValue(val)
{
}

TreculSymbolTableEntry::~TreculSymbolTableEntry()
{
}

const FieldType * TreculSymbolTableEntry::getType() const
{
  return mType;
}

IQLToLLVMLValue * TreculSymbolTableEntry::getValue() const
{
  return mValue;;
}

TreculSymbolTable::TreculSymbolTable(const TypeCheckConfiguration & typeCheckConfig)
  :
  mCaseInsensitive(typeCheckConfig.caseInsensitive())
{
}

TreculSymbolTable::~TreculSymbolTable()
{
  clear();
}

void TreculSymbolTable::clear()
{
  for(table_iterator it = mNameLookup.begin(),
	e = mNameLookup.end(); it != e; ++it) {
    delete it->second;
  }
  mNameLookup.clear();
  mUnprefixedNameLookup.clear();
}

TreculSymbolTableEntry * TreculSymbolTable::lookup(const char * nm, const char * nm2)
{
  std::string key(makeKey(nm, nm2));
  if (nm2) {
    table_const_iterator it = mNameLookup.find(key);
    if (it == mNameLookup.end()) {
      std::string fields;
      for(table_const_iterator it = mNameLookup.begin();
	  it != mNameLookup.end();
	  ++it) {
	if (boost::algorithm::starts_with(it->first, fnPrefix)) continue;
	if (fields.size()) fields += ",";
	fields += it->first;
      }
      throw std::runtime_error((boost::format("Undefined variable %1%.%2%/'%4%': available fields %3%") % nm % nm2 % fields % key).str());
    }    
    return it->second;
  } else {
    table_const_iterator it = mUnprefixedNameLookup.find(key);
    if (it == mUnprefixedNameLookup.end()) {
      std::string fields;
      for(table_const_iterator it = mNameLookup.begin();
	  it != mNameLookup.end();
	  ++it) {
	if (boost::algorithm::starts_with(it->first, fnPrefix)) continue;
	if (fields.size()) fields += ",";
	fields += it->first;
      }
      throw std::runtime_error((boost::format("Undefined variable %1% : available fields %2%") % nm % fields).str());
    } else if (&mAmbiguous == it->second) {
      throw std::runtime_error((boost::format("Ambiguous variable reference %1%") % nm).str());
    }
    return it->second;
  }
}

void TreculSymbolTable::add(const char * nm, const char * nm2, 
			    const FieldType * ft, IQLToLLVMLValue * val)
{
  std::string key(makeKey(nm, nm2));
  TreculSymbolTableEntry * e =  new TreculSymbolTableEntry(ft, val);
  mNameLookup[key] = e;
  std::string unprefixed(nm2 ? nm2 : nm);
  if (mCaseInsensitive) {
    boost::to_lower(unprefixed);
  }
  table_iterator it = mUnprefixedNameLookup.find(unprefixed);
  if (it != mUnprefixedNameLookup.end()) {
    it->second = &mAmbiguous;
  } else {
    mUnprefixedNameLookup[unprefixed] = e;
  }
}

void TreculSymbolTable::add(const char * nm, const char * nm2, 
			    IQLToLLVMLValue * val)
{
  add(nm, nm2, NULL, val);
}

void TreculSymbolTable::add(const char * nm, const char * nm2, 
			    const FieldType * ft)
{
  add(nm, nm2, ft, NULL);
}

bool TreculSymbolTable::contains(const char * nm, const char * nm2) const
{
  if (nm2) {
    table_const_iterator it = mNameLookup.find(makeKey(nm, nm2));
    return (it != mNameLookup.end());
  } else {
    table_const_iterator it = mUnprefixedNameLookup.find(nm);
    return (it != mUnprefixedNameLookup.end());
  }
}

std::string TreculSymbolTable::makeKey(const char * nm, const char * nm2) const
{
  std::string key(nm);
  if (nm2 != NULL) {
    key += ".";
    key += nm2;
  }
  if (mCaseInsensitive) {
    boost::to_lower(key);
  }
  return key;
}

TypeCheckContext::TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
				   DynamicRecordContext& recCtxt)
  :
  mSymbolTable(typeCheckConfig),
  mAggregateTable(typeCheckConfig),
  mContext(recCtxt),
  mOutputRecord(NULL),
  mTypeCheckSymbolTable(NULL),
  mAggregateTypeCheckSymbolTable(NULL),
  mSaveTypeCheckSymbolTable(NULL),
  mRecordMembers(NULL),
  mAggregateMembers(NULL),
  mAggregateRecord(NULL)
{
}

TypeCheckContext::TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
				   DynamicRecordContext & recCtxt,
				   const std::vector<AliasedRecordType>& sources,
				   const std::vector<boost::dynamic_bitset<> >& masks)
  :
  mSymbolTable(typeCheckConfig),
  mAggregateTable(typeCheckConfig),
  mContext(recCtxt),
  mOutputRecord(NULL),
  mTypeCheckSymbolTable(NULL),
  mAggregateTypeCheckSymbolTable(NULL),
  mSaveTypeCheckSymbolTable(NULL),
  mRecordMembers(NULL),
  mAggregateMembers(NULL),
  mAggregateRecord(NULL)
{
  init(sources, masks);
}

TypeCheckContext::TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
				   DynamicRecordContext & recCtxt,
				   const std::vector<AliasedRecordType>& sources)
  :
  mSymbolTable(typeCheckConfig),
  mAggregateTable(typeCheckConfig),
  mContext(recCtxt),
  mOutputRecord(NULL),
  mTypeCheckSymbolTable(NULL),
  mAggregateTypeCheckSymbolTable(NULL),
  mSaveTypeCheckSymbolTable(NULL),
  mRecordMembers(NULL),
  mAggregateMembers(NULL),
  mAggregateRecord(NULL)
{
  std::vector<boost::dynamic_bitset<> > masks;
  masks.resize(sources.size());
  for(std::size_t i=0; i<sources.size(); ++i) {
    masks[i].resize(sources[i].getType()->size(), true);
  }
  init(sources, masks);
}

TypeCheckContext::TypeCheckContext(const TypeCheckConfiguration & typeCheckConfig,
				   DynamicRecordContext & recCtxt,
				   const RecordType * input,
				   const std::vector<std::string>& groupKeys,
				   bool isOlap)
  :
  mSymbolTable(typeCheckConfig),
  mAggregateTable(typeCheckConfig),
  mContext(recCtxt),
  mOutputRecord(NULL),
  mTypeCheckSymbolTable(NULL),
  mAggregateTypeCheckSymbolTable(NULL),
  mSaveTypeCheckSymbolTable(NULL),
  mRecordMembers(NULL),
  mAggregateMembers(NULL),
  mAggregateRecord(NULL)
{
  mInputRecords["input"] = input;
  // Put only the group keys into the main symbol table
  // unless OLAP (running total) in which case all of
  // the input record is available in the main symbol table.
  std::set<std::string> groupKeySet;
  // To avoid O(N^2) put keys into a set (and remove dups
  // since they are abnormal but allowed).
  for(std::vector<std::string>::const_iterator it = groupKeys.begin();
      it != groupKeys.end();
      ++it) {
    if(groupKeySet.end() == groupKeySet.find(*it))
      groupKeySet.insert(*it);
  }
  for(RecordType::const_member_iterator it=input->begin_members();
      it != input->end_members();
      ++it) {
    if (isOlap || groupKeySet.end() != groupKeySet.find(it->GetName())) {
      mSymbolTable.add("input", it->GetName().c_str(), it->GetType());
    }
  }
  // Put all fields into the agg symbol table
  for(RecordType::const_member_iterator it=input->begin_members();
      it != input->end_members();
      ++it) {
    mAggregateTable.add("input", it->GetName().c_str(), it->GetType());
  }
  // TODO: Make this C wrapper stuff go away.
  mTypeCheckSymbolTable = &mSymbolTable;
  mAggregateTypeCheckSymbolTable = &mAggregateTable;
  
  // The group by fields are always in the aggregate record as well.
  // TODO: Low priority.  Here we are saying that the aggregate record
  // structure is determined by the lexical order of group by keys passed in.
  // If the select list (the transfer) has the group keys in a different order
  // then a non-trivial transfer will result.  It might be nice to figure out a
  // way of determining the structure of the aggregate record so that it mimics
  // the select list.
  mAggregateMembers = new RecordMemberList();
  for(std::vector<std::string>::const_iterator it = groupKeys.begin();
      it != groupKeys.end();
      ++it) {
    mAggregateMembers->push_back(input->getMember(*it));
  }

  loadBuiltinFunctions();
}

TypeCheckContext::~TypeCheckContext()
{
  delete mRecordMembers;
  delete mAggregateMembers;
}

void TypeCheckContext::init(const std::vector<AliasedRecordType>& sources,
			    const std::vector<boost::dynamic_bitset<> >& masks)
{
  for(std::vector<AliasedRecordType>::const_iterator it = sources.begin(); 
      it != sources.end();
      ++it) {
    std::size_t i = it - sources.begin();
    mInputRecords[it->getAlias()] = it->getType();

    for(RecordType::const_member_iterator mit=it->getType()->begin_members();
	mit != it->getType()->end_members();
	++mit) {
      std::size_t j = (std::size_t) (mit - it->getType()->begin_members());
      if (masks[i].test(j)) {
	mSymbolTable.add(it->getAlias().c_str(), mit->GetName().c_str(), mit->GetType());
      }
    }
    mSymbolTable.add(it->getAlias().c_str(), nullptr, it->getType());
  }
  mTypeCheckSymbolTable = &mSymbolTable;
  loadBuiltinFunctions();
}

bool TypeCheckContext::isBuiltinFunction(const char * name)
{
  
  if (!mTypeCheckSymbolTable->contains(fnPrefix, name)) 
    return false;
  TreculSymbolTableEntry * e = mTypeCheckSymbolTable->lookup(fnPrefix, name);
  return e->getType()->GetEnum() == FieldType::FUNCTION;
}

const FieldType * TypeCheckContext::castTo(const FieldType * lhs, 
					   const FieldType * rhs)
{
  // Equality without regards to nullability
  if (lhs->clone(true)==rhs->clone(true)) return lhs;

  // Supported conversions
  const FieldType * from_type = lhs;
  const FieldType * to_type = rhs;
  if (from_type->GetEnum() == FieldType::NIL) {
    return to_type;
  } else if (from_type->GetEnum() == FieldType::INT8) {
    if (to_type->GetEnum() == FieldType::INT8)
      return to_type;
    else if (to_type->GetEnum() == FieldType::INT16)
      return to_type;
    else if (to_type->GetEnum() == FieldType::INT32)
      return to_type;
    else if (to_type->GetEnum() == FieldType::INT64)
      return to_type;
    else if (to_type->GetEnum() == FieldType::FLOAT)
      return to_type;
    else if (to_type->GetEnum() == FieldType::DOUBLE)
      return to_type;
    else if (to_type->GetEnum() == FieldType::BIGDECIMAL)
      return to_type;
    else 
      return NULL;
  } else if (from_type->GetEnum() == FieldType::INT16) {
    if (to_type->GetEnum() == FieldType::INT16)
      return to_type;
    else if (to_type->GetEnum() == FieldType::INT32)
      return to_type;
    else if (to_type->GetEnum() == FieldType::INT64)
      return to_type;
    else if (to_type->GetEnum() == FieldType::FLOAT)
      return to_type;
    else if (to_type->GetEnum() == FieldType::DOUBLE)
      return to_type;
    else if (to_type->GetEnum() == FieldType::BIGDECIMAL)
      return to_type;
    else 
      return NULL;
  } else if (from_type->GetEnum() == FieldType::INT32) {
    if (to_type->GetEnum() == FieldType::INT32)
      return to_type;
    else if (to_type->GetEnum() == FieldType::INT64)
      return to_type;
    else if (to_type->GetEnum() == FieldType::FLOAT)
      return to_type;
    else if (to_type->GetEnum() == FieldType::DOUBLE)
      return to_type;
    else if (to_type->GetEnum() == FieldType::BIGDECIMAL)
      return to_type;
    else 
      return NULL;
  } else if (from_type->GetEnum() == FieldType::INT64) {
    if (to_type->GetEnum() == FieldType::INT64)
      return to_type;
    else if (to_type->GetEnum() == FieldType::DOUBLE)
      return to_type;
    else if (to_type->GetEnum() == FieldType::BIGDECIMAL)
      return to_type;
    else 
      return NULL;
  } else if (from_type->GetEnum() == FieldType::FLOAT) {
    if (to_type->GetEnum() == FieldType::FLOAT)
      return to_type;
    else if (to_type->GetEnum() == FieldType::DOUBLE)
      return to_type;
    else 
      return NULL;
  } else if (from_type->GetEnum() == FieldType::BIGDECIMAL) {
    if (to_type->GetEnum() == FieldType::BIGDECIMAL)
      return to_type;
    else if (to_type->GetEnum() == FieldType::DOUBLE)
      return to_type;
    else 
      return NULL;
  } else if (from_type->GetEnum() == FieldType::CHAR) {
    // TODO: What about sizes of these types?
    if (to_type->GetEnum() == FieldType::VARCHAR)
      return to_type;
    else 
      return NULL;    
  } else if (from_type->GetEnum() == FieldType::FIXED_ARRAY) {
    if (to_type->GetEnum() == FieldType::VARIABLE_ARRAY) {
      const SequentialType * from_arr_type = dynamic_cast<const SequentialType *>(from_type);
      const SequentialType * to_arr_type = dynamic_cast<const SequentialType *>(to_type);
      return nullptr != castTo(from_arr_type->getElementType(), to_arr_type->getElementType()) ? to_type : nullptr;
    } else {
      return NULL;
    }
  } else if (from_type->GetEnum() == FieldType::VARIABLE_ARRAY) {
    if (to_type->GetEnum() == FieldType::VARIABLE_ARRAY) {
      const SequentialType * from_arr_type = dynamic_cast<const SequentialType *>(from_type);
      const SequentialType * to_arr_type = dynamic_cast<const SequentialType *>(to_type);
      return nullptr != castTo(from_arr_type->getElementType(), to_arr_type->getElementType()) ? to_type : nullptr;
    } else {
      return NULL;
    }
  } else if (from_type->GetEnum() == FieldType::IPV4) {
    if (to_type->GetEnum() == FieldType::IPV6)
      return to_type;
    else if (to_type->GetEnum() == FieldType::CIDRV6)
      return to_type;
    else 
      return NULL;    
  } else if (from_type->GetEnum() == FieldType::CIDRV4) {
    if (to_type->GetEnum() == FieldType::CIDRV6)
      return to_type;
    else 
      return NULL;    
  } else if (from_type->GetEnum() == FieldType::IPV6) {
    if (to_type->GetEnum() == FieldType::CIDRV6)
      return to_type;
    else 
      return NULL;    
  } else if (from_type->GetEnum() == FieldType::STRUCT) {
    if (to_type->GetEnum() != FieldType::STRUCT)
      return NULL;

    // Either types are equal of allow casting of anonymous record to a named record
    auto from_rec_type = dynamic_cast<const RecordType *>(from_type);
    auto to_rec_type = dynamic_cast<const RecordType *>(to_type);
    if (from_rec_type == to_rec_type) {
      return to_type;
    }

    if (from_rec_type->getNumElements() != to_rec_type->getNumElements()) {
      return NULL;
    }

    for (auto from_it = from_rec_type->begin_members(), to_it = to_rec_type->begin_members();
         from_it != from_rec_type->end_members();
         ++from_it, ++to_it) {
      if (!from_it->GetName().empty() || from_it->GetType() != to_it->GetType()) {
        return NULL;
      }
    }
    return to_type;
  }
  return NULL;
}

const FieldType * TypeCheckContext::leastCommonType(const FieldType * e1, 
						    const FieldType * e2)
{
  const FieldType * ty = castTo(e1, e2);
  if (ty != NULL) return ty;
  return castTo(e2, e1);
}

const FieldType * TypeCheckContext::leastCommonTypeNullable(const FieldType * e1, 
							    const FieldType * e2)
{  
  const FieldType * ft = leastCommonType(e1,e2);
  if (ft != NULL) {
    bool isNullable = e1->isNullable() || e2->isNullable();
    ft = ft->clone(isNullable);
  } 
  return ft;
}

void TypeCheckContext::loadBuiltinFunctions()
{
  TypeCheckContext * ctxt = this;
  // Create function type for unary math operations.
  DynamicRecordContext & drc (ctxt->mContext);
  DoubleType * d = DoubleType::Get(drc);
  const FieldType * unaryDoubleOp = FunctionType::Get(drc, d, d);
  mTypeCheckSymbolTable->add(fnPrefix, "ceil", unaryDoubleOp);
  mTypeCheckSymbolTable->add(fnPrefix, "floor", unaryDoubleOp);
  mTypeCheckSymbolTable->add(fnPrefix, "sqrt", unaryDoubleOp);
  mTypeCheckSymbolTable->add(fnPrefix, "log", unaryDoubleOp);
  mTypeCheckSymbolTable->add(fnPrefix, "exp", unaryDoubleOp);
  mTypeCheckSymbolTable->add(fnPrefix, "round", 
			     FunctionType::Get(drc, 
					       DecimalType::Get(drc), 
					       Int32Type::Get(drc),
					       DecimalType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "length", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "urldecode", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "urlencode", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "replace", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "locate", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc), 
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "substr", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       Int32Type::Get(drc),
					       Int32Type::Get(drc),
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "trim", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "ltrim", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "rtrim", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "lower", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "upper", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "md5", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc), 
					       CharType::Get(drc, 32)));
  mTypeCheckSymbolTable->add(fnPrefix, "utc_timestamp", 
			     FunctionType::Get(drc, 
					       DatetimeType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "unix_timestamp", 
			     FunctionType::Get(drc, 
					       DatetimeType::Get(drc),
					       Int64Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "from_unixtime", 
			     FunctionType::Get(drc, 
					       Int64Type::Get(drc),
					       DatetimeType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "date", 
			     FunctionType::Get(drc, 
					       DatetimeType::Get(drc),
					       DateType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "dayofweek", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "dayofmonth", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "dayofyear", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "month", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "year", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "last_day", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       DateType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "julian_day", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "datediff", 
			     FunctionType::Get(drc, 
					       DateType::Get(drc),
					       DateType::Get(drc),
					       Int32Type::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "ip_address", 
			     FunctionType::Get(drc, 
					       CharType::Get(drc, 16), 
					       VarcharType::Get(drc)));
  mTypeCheckSymbolTable->add(fnPrefix, "parse_ip_address", 
			     FunctionType::Get(drc, 
					       VarcharType::Get(drc),
					       CharType::Get(drc, 16)));
  mTypeCheckSymbolTable->add(fnPrefix, "is_v4_ip_address", 
			     FunctionType::Get(drc, 
					       CharType::Get(drc, 16),
					       Int32Type::Get(drc)));
}

const RecordType * TypeCheckContext::getAggregateRecord() 
{
  if (mAggregateTypeCheckSymbolTable != NULL &&
      mAggregateRecord == NULL) {
    mAggregateRecord = RecordType::get(mContext, *mAggregateMembers);
  }  
  return mAggregateRecord;
}

void TypeCheckContext::buildSetValue(const FieldType * lhs,
				     const FieldType * rhs)
{
  if (NULL == castTo(rhs, lhs))
    throw std::runtime_error("Cannot set values of incompatible type");
  if (rhs->isNullable() && !lhs->isNullable())
    throw std::runtime_error("Cannot set nullable value into non nullable variable");
}

void TypeCheckContext::beginSwitch(const FieldType * e)
{
  if (e != Int8Type::Get(mContext) &&
      e != Int16Type::Get(mContext) &&
      e != Int32Type::Get(mContext) &&
      e != Int64Type::Get(mContext))
    throw std::runtime_error("Switch expression must be integer");
}

const FieldType * TypeCheckContext::buildVariableRef(const char * nm)
{
  TreculSymbolTableEntry * e = mTypeCheckSymbolTable->lookup(nm);
  return e->getType();
}

void TypeCheckContext::buildLocal(const char * nm, const FieldType * ty)
{
  mTypeCheckSymbolTable->add(nm, NULL, ty);
}

const FieldType * TypeCheckContext::buildArray(const std::vector<const FieldType *>& e)
{
  if (e.size() == 0) 
    throw std::runtime_error("array expression requires at least one element");
  const FieldType * elmntTy = e[0];
  for(std::size_t i=1; i<e.size(); ++i) {
    const FieldType * tmp = leastCommonTypeNullable(e[i], elmntTy);
    if (NULL == tmp) {
      throw std::runtime_error((boost::format("Can't find common type for elements of "
					      "type %1% and %2% in array") % 
				elmntTy->toString() % e[i]->toString()).str());
    }
    elmntTy = tmp;
  }
  return buildFixedArrayType(e.size(), elmntTy, false);
}

const FieldType * TypeCheckContext::buildArrayRef(const FieldType * arrayTy,
						  const FieldType * idx)
{
  if (idx != Int8Type::Get(mContext) &&
      idx != Int16Type::Get(mContext) &&
      idx != Int32Type::Get(mContext) &&
      idx != Int64Type::Get(mContext))
    throw std::runtime_error("Array index must be integer");

  if (arrayTy->GetEnum() == FieldType::FIXED_ARRAY) {
    const FieldType * elementTy = static_cast<const FixedArrayType *>(arrayTy)->getElementType();
    return elementTy;
  } else if (arrayTy->GetEnum() == FieldType::VARIABLE_ARRAY) {
    const FieldType * elementTy = static_cast<const VariableArrayType *>(arrayTy)->getElementType();
    return elementTy;
  } else {
    throw std::runtime_error("Can only reference fixed or variable length array");
  }
}

const FieldType * TypeCheckContext::buildStruct(const std::vector<const FieldType *>& e)
{
  if (e.size() == 0) 
    throw std::runtime_error("row expression requires at least one element");
  
  // Build an anonymous struct
  return buildStructType(e, false);
}

const FieldType * TypeCheckContext::buildStructRef(const FieldType * rowTy,
                                                   const char * nm)
{
  if (rowTy->GetEnum() != FieldType::STRUCT) {
    throw std::runtime_error("Member reference of non-struct type");
  }
  
  auto recTy = static_cast<const RecordType *>(rowTy);
  if (!recTy->hasMember(nm)) {
    std::stringstream fields;
    bool first=true;
    for(RecordType::const_member_iterator it = recTy->begin_members();
        it != recTy->end_members();
        ++it) {
      if (!first) fields << ",";
      fields << it->GetName();
      first = false;
    }
    throw std::runtime_error((boost::format("Undefined reference %1% in struct: available members %2%") % nm % fields.str()).str());
  }
  return recTy->getMember(nm).GetType();
}

const FieldType * TypeCheckContext::buildCall(const char * f, std::vector<const FieldType *> & args)
{
  // Check for intrinsics
  if (boost::algorithm::iequals(f, "least") ||
      boost::algorithm::iequals(f, "greatest")) {
    return buildLeast(args);
  } else if (boost::algorithm::iequals(f, "isnull") ||
	     boost::algorithm::iequals(f, "ifnull")) {
    return buildIsNull(args);
  } else if (boost::algorithm::iequals(f, "hash")) {
    return buildHash(args);
  } else if (boost::algorithm::iequals(f, "family")) {
    return buildFamily(args);
  } else if (boost::algorithm::iequals(f, "masklen")) {
    return buildMasklen(args);
  }
  // TODO: Implement operator/function overloading.
  const FieldType * fType = lookupType(f);
  if (fType == NULL || fType->GetEnum() != FieldType::FUNCTION)
    throw std::runtime_error((boost::format("Undefined function in call %1%") %
			      f).str());
  const FunctionType * funType = static_cast<const FunctionType *>(fType);
  if (funType->GetArgs().size() !=  args.size())
    throw std::runtime_error((boost::format("Function %1% takes %2% arguments") %
			      f %
			      funType->GetArgs().size()).str());
  for(std::size_t i=0; i<args.size(); ++i) {
    const FieldType * formalTy = funType->GetArgs()[i];
    const FieldType * ty = castTo(args[i], formalTy);
    if (ty == NULL)
      throw std::runtime_error((boost::format("Argument %1% of function %2% type mismatch; expected %3% received %4%") %
				i % f % formalTy->toString() %
				args[i]->toString()).str());
  }
  return funType->GetReturn();
}

const FieldType * TypeCheckContext::buildHash(std::vector<const FieldType *> & ty)
{
  // For the moment, we can hash anything.  Always returns an integer.
  return buildInt32Type();
}

const FieldType * TypeCheckContext::buildSortPrefix(std::vector<const FieldType *> & ty) 
{
  // For the moment, we can sort anything.  Always returns an integer.
  return buildInt32Type();
}

const FieldType * TypeCheckContext::buildNegate(const FieldType * ty)
{
  // Get corresponding non NULL type for checking.
  if (!ty->isNumeric())
    throw std::runtime_error("Type check error: expected numeric type");
  return ty;
}

const FieldType * TypeCheckContext::buildAdd(const FieldType * lhs,
					     const FieldType * rhs)
{
  bool nullable = lhs->isNullable() || rhs->isNullable();
  if ((lhs->GetEnum() == FieldType::DATETIME && rhs->GetEnum() == FieldType::INTERVAL) ||
      (rhs->GetEnum() == FieldType::DATETIME && lhs->GetEnum() == FieldType::INTERVAL)) {
    return buildDatetimeType(nullable);
  } else if ((lhs->GetEnum() == FieldType::DATE && rhs->GetEnum() == FieldType::INTERVAL) ||
      (rhs->GetEnum() == FieldType::DATE && lhs->GetEnum() == FieldType::INTERVAL)) {
    if (rhs->GetEnum() == FieldType::DATE) {
      std::swap(lhs, rhs);
    }
    const IntervalType * it = static_cast<const IntervalType *>(rhs);
    return it->getDateResultType(mContext, nullable);  
  } else if (lhs->GetEnum() == FieldType::CHAR && rhs->GetEnum()==FieldType::CHAR) {
    std::string retSz = boost::lexical_cast<std::string>(lhs->GetSize() + 
							 rhs->GetSize());
    return buildCharType(retSz.c_str(), nullable);
  } else {
    const FieldType * ty = leastCommonTypeNullable(lhs, rhs);
    if(ty == NULL || (!ty->isNumeric() && ty->GetEnum() != FieldType::VARCHAR && ty->GetEnum() != FieldType::CHAR)) {
      throw std::runtime_error("Can only add numeric and string fields");
    }
    return ty;
  }
}

const FieldType * TypeCheckContext::buildSub(const FieldType * lhs,
					     const FieldType * rhs)
{
  bool nullable = lhs->isNullable() || rhs->isNullable();
  if (lhs->GetEnum() == FieldType::DATE) {
    if (rhs->GetEnum() != FieldType::INTERVAL) {
      throw std::runtime_error("Can only subtract INTERVAL from DATE");
    }
    const IntervalType * it = static_cast<const IntervalType *>(rhs);
    return it->getDateResultType(mContext, nullable);  
  } else if (lhs->GetEnum() == FieldType::DATETIME) {
    if (rhs->GetEnum() != FieldType::INTERVAL) {
      throw std::runtime_error("Can only subtract INTERVAL from DATETIME");
    }
    const IntervalType * it = static_cast<const IntervalType *>(rhs);
    return buildDatetimeType(nullable);
  } else {
    const FieldType * ty = leastCommonTypeNullable(lhs, rhs);
    if(ty == NULL || !ty->isNumeric()) {
      throw std::runtime_error("Can only subtract numeric fields");
    }
    return ty;
  }
}

const FieldType * TypeCheckContext::buildMul(const FieldType * lhs,
					     const FieldType * rhs)
{
  const FieldType * ty = leastCommonTypeNullable(lhs, rhs);
  if(ty == NULL || !ty->isNumeric()) {
    throw std::runtime_error("Can only multiply numeric fields");
  }
  return ty;
}

const FieldType * TypeCheckContext::buildDiv(const FieldType * lhs,
					     const FieldType * rhs)
{
  if (lhs->GetEnum() == FieldType::IPV4 || lhs->GetEnum() == FieldType::IPV6) {
    if (!rhs->isIntegral()) {
      throw std::runtime_error("Prefix length for CIDR types must be an integer type");
    }
    bool nullable = lhs->isNullable() || rhs->isNullable();
    return lhs->GetEnum() == FieldType::IPV4 ? buildCIDRv4Type(nullable) : buildCIDRv6Type(nullable);
  } else {
    return buildMul(lhs, rhs);
  }
}

const FieldType * TypeCheckContext::buildModulus(const FieldType * lhs,
						 const FieldType * rhs)
{
  const FieldType * ret = leastCommonTypeNullable(lhs, rhs);
  if (ret == NULL || 
      (ret->clone(true) != Int8Type::Get(mContext, true) &&
       ret->clone(true) != Int16Type::Get(mContext, true) &&
       ret->clone(true) != Int32Type::Get(mContext, true) &&
       ret->clone(true) != Int64Type::Get(mContext, true)))
    throw std::runtime_error("Argument to modulus must be integer");
  return ret;
}

const FieldType * TypeCheckContext::buildConcat(const FieldType * lhs,
                                                const FieldType * rhs)
{
  bool isNullable = lhs->isNullable() || rhs->isNullable();
  const SequentialType * lhsArrTy = dynamic_cast<const SequentialType *>(lhs);
  const SequentialType * rhsArrTy = dynamic_cast<const SequentialType *>(rhs);
  if (nullptr == lhsArrTy && nullptr == rhsArrTy) {
    throw std::runtime_error("At least one argument to concatenate operator must be an array type");
  }
  int32_t lhsSize = nullptr != lhsArrTy ? lhsArrTy->GetSize() : 1;
  int32_t rhsSize = nullptr != rhsArrTy ? rhsArrTy->GetSize() : 1;
  const FieldType * lhsEltTy = nullptr != lhsArrTy ? lhsArrTy->getElementType() : lhs;
  const FieldType * rhsEltTy = nullptr != rhsArrTy ? rhsArrTy->getElementType() : rhs;
  const FieldType * retEltTy = leastCommonTypeNullable(lhsEltTy, rhsEltTy);
  if (nullptr == retEltTy) {
    throw std::runtime_error("Incompatible element types in array concatenation");
  }
  return lhs->GetEnum() == FieldType::VARIABLE_ARRAY ||
    rhs->GetEnum() == FieldType::VARIABLE_ARRAY ?
    buildVariableArrayType(retEltTy, isNullable) :
    buildFixedArrayType(lhsSize + rhsSize, retEltTy, isNullable);
}

const FieldType * TypeCheckContext::buildBitwise(const FieldType * lhs,
						 const FieldType * rhs)
{
  const FieldType * ret = leastCommonTypeNullable(lhs, rhs);
  if (ret == NULL || 
      (ret->clone(true) != Int8Type::Get(mContext, true) &&
       ret->clone(true) != Int16Type::Get(mContext, true) &&
       ret->clone(true) != Int32Type::Get(mContext, true) &&
       ret->clone(true) != Int64Type::Get(mContext, true)))
    throw std::runtime_error("Argument to bitwise operation must be integer");
  return ret;
}

const FieldType * TypeCheckContext::buildBitwise(const FieldType * lhs)
{
  const FieldType * ret = lhs;
  if (ret->clone(true) != Int8Type::Get(mContext, true) &&
      ret->clone(true) != Int16Type::Get(mContext, true) &&
      ret->clone(true) != Int32Type::Get(mContext, true) &&
      ret->clone(true) != Int64Type::Get(mContext, true))
    throw std::runtime_error("Argument to bitwise operator must be integer");
  return ret;
}

const FieldType * TypeCheckContext::buildEquals(const FieldType * lhs, 
						const FieldType * rhs)
{
  // In the future we may have types that we can compare for equality but not otherwise
  return buildCompare(lhs, rhs);
}

const FieldType * TypeCheckContext::buildCompare(const FieldType * lhs, 
						 const FieldType * rhs)
{
  const FieldType * ret = leastCommonTypeNullable(lhs, rhs);
  if (ret == NULL) 
    throw std::runtime_error((boost::format("Type check error: cannot compare "
					    "expressions of type %1% and %2%")
			      % lhs->toString() 
			      % rhs->toString()).str());
  return buildBooleanType(ret->isNullable());
}

const FieldType * TypeCheckContext::buildLogicalAnd(const FieldType * lhs, 
						    const FieldType * rhs)
{
  // Must be bool (currently int32_t for hacky reasons).
  const FieldType * bType = buildBooleanType(true);
  // Check type without regard to nullability
  if (lhs->clone(true) != bType || rhs->clone(true) != bType)
    throw std::runtime_error("Expected boolean expression");
  bool isNullable = lhs->isNullable() || rhs->isNullable();
  return bType->clone(isNullable);
}

const FieldType * TypeCheckContext::buildLogicalNot(const FieldType * lhs)
{
  const FieldType * bType = buildBooleanType(true);
  if (lhs->clone(true) != bType)
    throw std::runtime_error("Expected boolean expression");
  return lhs;
}

const FieldType * TypeCheckContext::buildLike(const FieldType * lhs, 
					      const FieldType * rhs)
{
  if (lhs->GetEnum() != FieldType::VARCHAR) {
    throw std::runtime_error("Expected varchar expression for left hand side of RLIKE expression");
  }
  if (rhs->GetEnum() != FieldType::VARCHAR) {
    throw std::runtime_error("Expected varchar expression for right hand side of RLIKE expression");
  }
  const bool isNullable = lhs->isNullable() || rhs->isNullable();
  return buildBooleanType(isNullable);
}

const FieldType * TypeCheckContext::buildNetworkSubnet(const FieldType * lhs, 
                                                       const FieldType * rhs)
{
  // Both sides should be convertible to CIDRV6
  auto cidrV6Ty = buildCIDRv6Type();
  if (nullptr == castTo(lhs, cidrV6Ty)) {
    throw std::runtime_error("Expected IPV4, IPV6, CIDR4 or CIDRV6 expression for left hand side of subnet operator");
  }
  if (nullptr == castTo(rhs, cidrV6Ty)) {
    throw std::runtime_error("Expected IPV4, IPV6, CIDR4 or CIDRV6 expression for right hand side of subnet operator");
  }
  const bool isNullable = lhs->isNullable() || rhs->isNullable();
  return buildBooleanType(isNullable);
}

const FieldType * TypeCheckContext::buildFamily(std::vector<const FieldType *> & args)
{
  if (1U != args.size()) {
    throw std::runtime_error("'family' takes one argument");
  }
  auto cidrV6Ty = buildCIDRv6Type();
  if (nullptr == castTo(args[0], cidrV6Ty)) {
    throw std::runtime_error("Expected IPV4, IPV6, CIDR4 or CIDRV6 expression for 'family' argument");
  }

  return buildInt32Type(args[0]->isNullable());
}

const FieldType * TypeCheckContext::buildMasklen(std::vector<const FieldType *> & args)
{
  if (1U != args.size()) {
    throw std::runtime_error("'masklen' takes one argument");
  }
  auto cidrV6Ty = buildCIDRv6Type();
  if (nullptr == castTo(args[0], cidrV6Ty)) {
    throw std::runtime_error("Expected IPV4, IPV6, CIDR4 or CIDRV6 expression for 'masklen' argument");
  }
  
  return buildInt32Type(args[0]->isNullable());
}

const FieldType * TypeCheckContext::buildCast(const FieldType * lhs, 
					      const FieldType * target)
{
  bool isNullable = target->isNullable() || lhs->isNullable();
  if (target->GetEnum() == FieldType::FIXED_ARRAY) {
    const FixedArrayType * targetArrayTy = dynamic_cast<const FixedArrayType *>(target);
    if (lhs->GetEnum() == FieldType::FIXED_ARRAY) {
      const FixedArrayType * lhsArrayTy = dynamic_cast<const FixedArrayType *>(lhs);
      if (target->GetSize() > lhs->GetSize() || lhsArrayTy->getElementType()->isNullable()) {
        return buildFixedArrayType(target->GetSize(),
                                   targetArrayTy->getElementType()->clone(true),
                                   isNullable);
      } else {
        return target->clone(isNullable);
      }
    } else if (lhs->GetEnum() == FieldType::VARIABLE_ARRAY) {
      const VariableArrayType * lhsArrayTy = dynamic_cast<const VariableArrayType *>(lhs);
      return buildFixedArrayType(target->GetSize(),
                                 targetArrayTy->getElementType()->clone(lhsArrayTy->getElementType()->isNullable()),
                                 isNullable);
    } else {
      throw std::runtime_error("Not supporting cast of non-array type to array type");
    }
  } else if (target->GetEnum() == FieldType::VARIABLE_ARRAY) {
    if (lhs->GetEnum() == FieldType::FIXED_ARRAY) {
      const VariableArrayType * targetArrayTy = dynamic_cast<const VariableArrayType *>(target);    
      const FixedArrayType * lhsArrayTy = dynamic_cast<const FixedArrayType *>(lhs);
      return buildVariableArrayType(targetArrayTy->getElementType()->clone(lhsArrayTy->getElementType()->isNullable()),
                                    isNullable);
    } else if (lhs->GetEnum() == FieldType::VARIABLE_ARRAY) {
      return target->clone(isNullable);
    } else {
      throw std::runtime_error("Not supporting cast of non-array type to array type");
    }
  } else {
    return target->clone(isNullable);
  }
}

const FieldType * TypeCheckContext::buildInt8Type(bool nullable)
{
  return Int8Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildInt16Type(bool nullable)
{
  return Int16Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildInt32Type(bool nullable)
{
  return Int32Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildInt64Type(bool nullable)
{
  return Int64Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildFloatType(bool nullable)
{
  return FloatType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDoubleType(bool nullable)
{
  return DoubleType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDecimalType(bool nullable)
{
  return DecimalType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDatetimeType(bool nullable)
{
  return DatetimeType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDateType(bool nullable)
{
  return DateType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildVarcharType()
{
  return VarcharType::Get(mContext);
}

const FieldType * TypeCheckContext::buildVarcharType(bool nullable)
{
  return VarcharType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildCharType(const char * sz, 
						  bool nullable)
{
  int32_t fieldSz = boost::lexical_cast<int32_t> (sz);
  return CharType::Get(mContext, fieldSz, nullable);
}

const FieldType * TypeCheckContext::buildBooleanType(bool nullable)
{
  // TODO: Currently returning int32_t for boolean (be smarter here).
  return Int32Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildIPv4Type(const char * addr, bool nullable)
{
  boost::system::error_code ec;
  boost::asio::ip::make_address_v4(addr, ec);
  if (ec) {
    throw std::runtime_error("Invalid IPv4 addresss");
  }
  return IPv4Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildIPv4Type(bool nullable)
{
  return IPv4Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildCIDRv4Type(bool nullable)
{
  return CIDRv4Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildIPv6Type(const char * addr, bool nullable)
{
  boost::system::error_code ec;
  boost::asio::ip::make_address_v6(addr, ec);
  if (ec) {
    throw std::runtime_error("Invalid IPv6 addresss");
  }
  return IPv6Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildIPv6Type(bool nullable)
{
  return IPv6Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildCIDRv6Type(bool nullable)
{
  return CIDRv6Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDecltypeType(const FieldType * ft, bool nullable)
{
  return ft->clone(nullable);
}

const FieldType * TypeCheckContext::buildNilType()
{
  // TODO: Proper NULL support
  return NilType::Get(mContext);
}

const FieldType * TypeCheckContext::buildType(const char * typeName, bool nullable)
{
  if (boost::algorithm::iequals("date", typeName)) {
    return DateType::Get(mContext, nullable);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }
}

const FieldType * TypeCheckContext::buildArrayType(const char * sz, const FieldType * elt, bool nullable)
{
  if (boost::algorithm::iequals(sz, "infinity")) {
    return buildVariableArrayType(elt, nullable);
  } else {
    int32_t fieldSz = boost::lexical_cast<int32_t> (sz);
    return buildFixedArrayType(fieldSz, elt, nullable);
  }
}

const FieldType * TypeCheckContext::buildFixedArrayType(int32_t sz, const FieldType * elt, bool nullable)
{
  return FixedArrayType::Get(mContext, sz, elt, nullable);
}

const FieldType * TypeCheckContext::buildVariableArrayType(const FieldType * elt, bool nullable)
{
  return VariableArrayType::Get(mContext, elt, nullable);
}

const FieldType * TypeCheckContext::buildStructType(const std::vector<const FieldType *> & elts, bool nullable)
{
  std::vector<RecordMember> fields;
  for(auto e : elts) {
    fields.emplace_back("", e);
  }
  return RecordType::Get(mContext, std::move(fields), nullable, true);
}

const FieldType * TypeCheckContext::internalBuildInterval(const FieldType * ty, int32_t u)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty->GetEnum() != FieldType::INT32)
    throw std::runtime_error("Expected integer expression in INTERVAL expression");
  return IntervalType::Get(mContext, (IntervalType::IntervalUnit) u, ty->isNullable());
}

const FieldType * TypeCheckContext::buildIntervalDay(const FieldType * ty)
{
  return internalBuildInterval(ty, IntervalType::DAY);
}

const FieldType * TypeCheckContext::buildIntervalHour(const FieldType * ty)
{
  return internalBuildInterval(ty, IntervalType::HOUR);
}

const FieldType * TypeCheckContext::buildIntervalMinute(const FieldType * ty)
{
  return internalBuildInterval(ty, IntervalType::MINUTE);
}

const FieldType * TypeCheckContext::buildIntervalMonth(const FieldType * ty)
{
  return internalBuildInterval(ty, IntervalType::MONTH);
}

const FieldType * TypeCheckContext::buildIntervalSecond(const FieldType * ty)
{
  return internalBuildInterval(ty, IntervalType::SECOND);
}

const FieldType * TypeCheckContext::buildIntervalYear(const FieldType * ty)
{
  return internalBuildInterval(ty, IntervalType::YEAR);
}

const FieldType * TypeCheckContext::buildInterval(const char * intervalType, const FieldType * ty)
{
  if (boost::algorithm::iequals(intervalType, "day")) {
    return buildIntervalDay(ty);
  } else if (boost::algorithm::iequals(intervalType, "hour")) {
    return buildIntervalHour(ty);
  } else if (boost::algorithm::iequals(intervalType, "minute")) {
    return buildIntervalMinute(ty);
  } else if (boost::algorithm::iequals(intervalType, "month")) {
    return buildIntervalMonth(ty);
  } else if (boost::algorithm::iequals(intervalType, "second")) {
    return buildIntervalSecond(ty);
  } else if (boost::algorithm::iequals(intervalType, "year")) {
    return buildIntervalYear(ty);
  } else {
    throw std::runtime_error("INTERVAL type must be one of DAY, HOUR, MINUTE, "
			     "MONTH, SECOND or YEAR");
  }
}

void TypeCheckContext::beginCase()
{
  mCaseType.push(NULL);
}

void TypeCheckContext::addCondition(const FieldType * condVal)
{
  const FieldType * bType = buildBooleanType(true);
  if (condVal->clone(true) != bType)
    throw std::runtime_error("Expected boolean expression in CASE expression condition");
}

void TypeCheckContext::addValue(const FieldType * thenVal)
{
  const FieldType * curType = mCaseType.top();
  if (curType != NULL) {
    thenVal = leastCommonTypeNullable(thenVal, mCaseType.top());
    if (thenVal == NULL) {
      throw std::runtime_error("Incompatible types in CASE expression");
    }
  }
  mCaseType.pop();
  mCaseType.push(thenVal);
}

const FieldType * TypeCheckContext::buildCase()
{
  // The least common super type of values.
  const FieldType * ret = mCaseType.top();
  mCaseType.pop();
  return ret;
}

const FieldType * TypeCheckContext::buildIfThenElse(const FieldType * condVal,
						    const FieldType * thenVal,
						    const FieldType * elseVal)
{
  // Must be bool (currently int32_t for hacky reasons).
  // TODO: Currently returning int32_t for boolean (be smarter here).
  const FieldType * bType = buildBooleanType();
  if (condVal != bType)
    throw std::runtime_error("Expected boolean expression");

  const FieldType * ret = leastCommonType(thenVal, elseVal);
  if (ret == NULL) 
    throw std::runtime_error("Type check failure ?:");

  return ret;
}

void TypeCheckContext::beginRecord()
{
  if (mRecordMembers)
    throw std::runtime_error("Not supporting nested records yet");
  mRecordMembers = new TypeCheckContext::member_list ();
}

void TypeCheckContext::addField(const char * name, const FieldType * ty)
{
  if (name == NULL) {
    throw std::runtime_error("Must specify an field name for expression");
  }

  if (isBuiltinFunction(name)) {
    throw std::runtime_error((boost::format("Cannot use name of built in "
					    "function '%1%' as variable name")
			      % name).str());
  }
  
  if (ty->GetEnum() == FieldType::NIL) {
    throw std::runtime_error((boost::format("NULL value in field %1% requires"
					    " CAST to specify type") %
			      name).str());
  }
  mRecordMembers->push_back(RecordMember(name, ty));
}

void TypeCheckContext::addFields(const char * recordName)
{
  // Find the record struct in the named inputs.
  std::map<std::string, const RecordType *>::const_iterator it = mInputRecords.find(recordName);
  if (it == mInputRecords.end())
    throw std::runtime_error((boost::format("Undefined input record: %1%") % recordName).str());
  // Add each of the members of this record into the target.
  for(RecordType::const_member_iterator mit = it->second->begin_members();
      mit != it->second->end_members();
      ++mit) {
    // Make sure the member is valid to reference
    buildStructRef(buildVariableRef(recordName), mit->GetName().c_str());
    mRecordMembers->push_back(*mit);
  }
}

void TypeCheckContext::quotedId(const char * id, const char * format)
{
  std::string idEx(id);
  boost::regex ex(idEx.substr(1, idEx.size() -2));
  std::string fmt(format ? format : "``");
  fmt = fmt.substr(1, fmt.size()-2);
  // Find the record struct in the named inputs.
  for(std::map<std::string, const RecordType *>::const_iterator it = mInputRecords.begin();
      it != mInputRecords.end();
      ++it) {
    // Add each of the members of this record into the target.
    for(RecordType::const_member_iterator mit = it->second->begin_members();
	mit != it->second->end_members();
	++mit) {
      if (boost::regex_match(mit->GetName().c_str(), ex)) {
	if (fmt.size()) {
	  std::string tgt = boost::regex_replace(mit->GetName(), ex, fmt, boost::format_no_copy | boost::format_first_only);
	  mRecordMembers->push_back(RecordMember(tgt, mit->GetType()));
	} else {
	  mRecordMembers->push_back(*mit);
	}
      }
    }
  }
}

void TypeCheckContext::buildRecord()
{
  const RecordType * tmp = RecordType::get(mContext, *mRecordMembers);
  delete mRecordMembers;
  mRecordMembers = NULL;
  mOutputRecord =  tmp;
}

const FieldType * TypeCheckContext::buildLeast(const std::vector<const FieldType *>& args)
{
  if (args.size() == 0) 
    throw std::runtime_error("LEAST requires at least one argument");
  const FieldType * retTy = args[0];
  for(std::size_t i=1; i<args.size(); ++i) {
    retTy = leastCommonType(args[i], retTy);
  }
  return retTy;
}

const FieldType * TypeCheckContext::buildIsNull(const std::vector<const FieldType *>& args)
{
  if (args.size() != 2) 
    throw std::runtime_error("IFNULL takes exactly two arguments");
  // Don't allow literal NULL for first argument
  if (args[0]->GetEnum() == FieldType::NIL) {
    throw std::runtime_error("IFNULL cannot take NULL as first argument");
  }
  // Allow implicit conversion of second arg to first.
  if (NULL == castTo(args[1], args[0])) {
    throw std::runtime_error((boost::format("IFNULL cannot implicitly cast "
					    "from %1% to %2%") %
			      args[1]->toString() % 
			      args[0]->toString()).str());
  }
  // Return type is type of first arg with nullability of
  // second.
  bool retNull = args[1]->isNullable() && args[0]->isNullable();
  const FieldType * retTy = args[0]->clone(retNull);
  return retTy;
}

void TypeCheckContext::beginAggregateFunction()
{
  // Make sure that we are in a context in which aggregates are
  // allowed.  If so switch to the type check context appropriate
  // for type checking the aggregate argument.
  if (mSaveTypeCheckSymbolTable != NULL) {
    throw std::runtime_error("Cannot nest aggregate functions");
  }
  if (mAggregateTypeCheckSymbolTable == NULL) {
    throw std::runtime_error("Cannot use aggregate functions outside of group by");
  }
  std::swap(mTypeCheckSymbolTable, mSaveTypeCheckSymbolTable);
  std::swap(mTypeCheckSymbolTable, mAggregateTypeCheckSymbolTable);
}

const FieldType * TypeCheckContext::buildAggregateFunction(const FieldType * ty, const char * fn)
{
  // Switch back to the top level context for the aggregate (e.g. group by
  // fields only in the symbol table).
  std::swap(mTypeCheckSymbolTable, mAggregateTypeCheckSymbolTable);
  std::swap(mTypeCheckSymbolTable, mSaveTypeCheckSymbolTable);
  // TODO: Add proper checks and conversions for ty.
  // TODO: Not all aggregate functions have nullable output
  // SUM, MIN and MAX do however (COUNT will not when it is added).
  if (boost::algorithm::iequals(fn, "array_concat")) {
    ty = buildVariableArrayType(ty, true);
  } else {
    ty = ty->clone(true);
  }
  // Add a member to the aggregate record
  std::string aggregateMember((boost::format("__AggFn%1%__") % 
			       mAggregateMembers->size()).str());
  mAggregateMembers->push_back(RecordMember(aggregateMember,
                                            ty));
  return ty;
}

const FieldType * TypeCheckContext::lookupType(const char * nm)
{
  TreculSymbolTableEntry * entry = mTypeCheckSymbolTable->lookup(nm);
  return entry->getType();
}
