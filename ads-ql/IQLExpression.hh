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

#ifndef __IQLEXPRESSION_HH
#define __IQLEXPRESSION_HH

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/iterator/transform_iterator.hpp>
#include "IQLBuildTree.h"
#include "IQLStatement.hh"
#include "RecordType.hh"

// TODO: Support constant folding of IQLExpressions
// Implement type checking and LLVM code gen off of IQLExpressions
class IQLExpression : public IQLStatement
{
public:
  typedef IQLStatement::NodeType NodeType;
  
private:
  // Type of the expression computed by type checking
  const FieldType * mFieldType;
  // Type to coerce to.  Having this as a member
  // saves a tree rewrite.
  const FieldType * mCoerceTo;
  
protected:
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, 
		const SourceLocation& loc)
    :
    IQLStatement(ctxt, nodeType, loc),
    mFieldType(NULL),
    mCoerceTo(NULL)
  {
  }
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, 
		IQLExpression * arg, 
		const SourceLocation& loc)
    :
    IQLStatement(ctxt, nodeType, arg, loc),
    mFieldType(NULL),
    mCoerceTo(NULL)
  {
  }
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, IQLExpression * arg1,
		IQLExpression * arg2, 
		const SourceLocation& loc)
    :
    IQLStatement(ctxt, nodeType, arg1, arg2, loc),
    mFieldType(NULL),
    mCoerceTo(NULL)
  {
  }
  template<typename _Iterator>
  IQLExpression(DynamicRecordContext & ctxt,
        	NodeType nodeType, _Iterator begin,
        	_Iterator end, 
        	const SourceLocation& loc)
    :
    IQLStatement(ctxt, nodeType, loc),
    mFieldType(NULL),
    mCoerceTo(NULL)
  {
    mArgs.resize(std::distance(begin, end), nullptr);
    std::copy(begin, end, &mArgs[0]);
  }
  IQLExpression(const IQLExpression & rhs)
    :
    IQLStatement(rhs),
    mFieldType(rhs.mFieldType),
    mCoerceTo(rhs.mCoerceTo)
  {
  }

  struct Downcast
  {
    typedef IQLExpression* result_type;
    typedef IQLStatement* argument_type;
    IQLExpression * operator()(IQLStatement * s) const
    {
      return static_cast<IQLExpression *>(s);
    }
  };
  
public:

  virtual ~IQLExpression();

  bool equals(const IQLExpression * rhs) const;

  void setFieldType(const FieldType * ty)
  {
    mFieldType = ty;
  }

  const FieldType * getFieldType() const
  {
    return mFieldType;
  }

  void setCoerceTo(const FieldType * coerceTo)
  {
    if (coerceTo != mFieldType) {
      mCoerceTo = coerceTo;
    } else {
      mCoerceTo = NULL;
    }
  }

  const FieldType * getCoercedFieldType() const
  {
    return mCoerceTo ? mCoerceTo : mFieldType;
  }

  // Argument interface
  std::size_t args_size() const
  {
    return mArgs.size();
  }
  typedef boost::transform_iterator<Downcast, std::vector<IQLStatement*>::const_iterator> arg_const_iterator;
  arg_const_iterator begin_args() const
  {
    return boost::make_transform_iterator(mArgs.begin(), Downcast());
  }
  arg_const_iterator end_args() const 
  {
    return boost::make_transform_iterator(mArgs.end(), Downcast());
  }

  typedef boost::transform_iterator<Downcast, std::vector<IQLStatement*>::iterator> arg_iterator;
  arg_iterator begin_args() 
  {
    return boost::make_transform_iterator(mArgs.begin(), Downcast());
  }
  arg_iterator end_args() 
  {
    return boost::make_transform_iterator(mArgs.end(), Downcast());
  }

};

class LogicalOrExpr : public IQLExpression
{
private:
  LogicalOrExpr(const LogicalOrExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LogicalOrExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LOR, left, right, loc)
  {
  }

  static LogicalOrExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				const SourceLocation& loc)
  {
    LogicalOrExpr * tmp = new LogicalOrExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LogicalOrExpr * clone() const;
};

class LogicalAndExpr : public IQLExpression
{
private:
  LogicalAndExpr(const LogicalAndExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LogicalAndExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LAND, left, right, loc)
  {
  }

  static LogicalAndExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				const SourceLocation& loc)
  {
    LogicalAndExpr * tmp = new LogicalAndExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LogicalAndExpr * clone() const;
};

class LogicalNotExpr : public IQLExpression
{
private:
  LogicalNotExpr(const LogicalNotExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LogicalNotExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LNOT, left, loc)
  {
  }

  static LogicalNotExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				const SourceLocation& loc)
  {
    LogicalNotExpr * tmp = new LogicalNotExpr(ctxt, left, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LogicalNotExpr * clone() const;
};

class IsNullExpr : public IQLExpression
{
private:
  IsNullExpr(const IsNullExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  IsNullExpr(DynamicRecordContext & ctxt,
             IQLExpression * expr,
             bool isNot,
             const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LISNULL, expr, loc)
  {
    setData(static_cast<int64_t>(isNot ? 1 : 0));
  }

  static IsNullExpr * create(DynamicRecordContext & ctxt,
                             IQLExpression * expr,
                             bool isNot,
                             const SourceLocation& loc)
  {
    IsNullExpr * tmp = new IsNullExpr(ctxt, expr, isNot, loc);
    ctxt.add(tmp);
    return tmp;
  }

  bool isNot() const
  {
    return getBooleanData();
  }

  IsNullExpr * clone() const;
};

class BitwiseAndExpr : public IQLExpression
{
private:
  BitwiseAndExpr(const BitwiseAndExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  BitwiseAndExpr(DynamicRecordContext & ctxt,
                 IQLExpression * left,
                 IQLExpression * right,
                 const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::BAND, left, right, loc)
  {
  }

  static BitwiseAndExpr * create(DynamicRecordContext & ctxt,
                                 IQLExpression * left,
                                 IQLExpression * right,
                                 const SourceLocation& loc)
  {
    BitwiseAndExpr * tmp = new BitwiseAndExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  BitwiseAndExpr * clone() const;
};

class BitwiseOrExpr : public IQLExpression
{
private:
  BitwiseOrExpr(const BitwiseOrExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  BitwiseOrExpr(DynamicRecordContext & ctxt,
                IQLExpression * left,
                IQLExpression * right,
                const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::BOR, left, right, loc)
  {
  }

  static BitwiseOrExpr * create(DynamicRecordContext & ctxt,
                                IQLExpression * left,
                                IQLExpression * right,
                                const SourceLocation& loc)
  {
    BitwiseOrExpr * tmp = new BitwiseOrExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  BitwiseOrExpr * clone() const;
};

class BitwiseXorExpr : public IQLExpression
{
private:
  BitwiseXorExpr(const BitwiseXorExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  BitwiseXorExpr(DynamicRecordContext & ctxt,
                 IQLExpression * left,
                 IQLExpression * right,
                 const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::BXOR, left, right, loc)
  {
  }

  static BitwiseXorExpr * create(DynamicRecordContext & ctxt,
                                 IQLExpression * left,
                                 IQLExpression * right,
                                 const SourceLocation& loc)
  {
    BitwiseXorExpr * tmp = new BitwiseXorExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  BitwiseXorExpr * clone() const;
};

class BitwiseNotExpr : public IQLExpression
{
private:
  BitwiseNotExpr(const BitwiseNotExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  BitwiseNotExpr(DynamicRecordContext & ctxt,
                 IQLExpression * expr,
                 const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::BNOT, expr, loc)
  {
  }

  static BitwiseNotExpr * create(DynamicRecordContext & ctxt,
                                 IQLExpression * expr,
                                 const SourceLocation& loc)
  {
    BitwiseNotExpr * tmp = new BitwiseNotExpr(ctxt, expr, loc);
    ctxt.add(tmp);
    return tmp;
  }

  BitwiseNotExpr * clone() const;
};

class PlusExpr : public IQLExpression
{
private:
  PlusExpr(const PlusExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  PlusExpr(DynamicRecordContext & ctxt,
           IQLExpression * expr,
           const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::PLUS, expr, loc)
  {
  }

  PlusExpr(DynamicRecordContext & ctxt,
           IQLExpression * left,
           IQLExpression * right,
           const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::PLUS, left, right, loc)
  {
  }

  static PlusExpr * createUnary(DynamicRecordContext & ctxt,
                                IQLExpression * expr,
                                const SourceLocation& loc)
  {
    PlusExpr * tmp = new PlusExpr(ctxt, expr, loc);
    ctxt.add(tmp);
    return tmp;
  }

  static PlusExpr * createBinary(DynamicRecordContext & ctxt,
                                 IQLExpression * left,
                                 IQLExpression * right,
                                 const SourceLocation& loc)
  {
    PlusExpr * tmp = new PlusExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  bool isUnary() const
  {
    return args_size() == 1;
  }

  PlusExpr * clone() const;
};

class MinusExpr : public IQLExpression
{
private:
  MinusExpr(const MinusExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  MinusExpr(DynamicRecordContext & ctxt,
            IQLExpression * expr,
            const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::MINUS, expr, loc)
  {
  }

  MinusExpr(DynamicRecordContext & ctxt,
            IQLExpression * left,
            IQLExpression * right,
            const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::MINUS, left, right, loc)
  {
  }

  static MinusExpr * createUnary(DynamicRecordContext & ctxt,
                                 IQLExpression * expr,
                                 const SourceLocation& loc)
  {
    MinusExpr * tmp = new MinusExpr(ctxt, expr, loc);
    ctxt.add(tmp);
    return tmp;
  }

  static MinusExpr * createBinary(DynamicRecordContext & ctxt,
                                  IQLExpression * left,
                                  IQLExpression * right,
                                  const SourceLocation& loc)
  {
    MinusExpr * tmp = new MinusExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  bool isUnary() const
  {
    return args_size() == 1;
  }

  MinusExpr * clone() const;
};

class TimesExpr : public IQLExpression
{
private:
  TimesExpr(const TimesExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  TimesExpr(DynamicRecordContext & ctxt,
            IQLExpression * left,
            IQLExpression * right,
            const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::TIMES, left, right, loc)
  {
  }

  static TimesExpr * create(DynamicRecordContext & ctxt,
                            IQLExpression * left,
                            IQLExpression * right,
                            const SourceLocation& loc)
  {
    TimesExpr * tmp = new TimesExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  TimesExpr * clone() const;
};

class DivideExpr : public IQLExpression
{
private:
  DivideExpr(const DivideExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  DivideExpr(DynamicRecordContext & ctxt,
             IQLExpression * left,
             IQLExpression * right,
             const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::DIVIDE, left, right, loc)
  {
  }

  static DivideExpr * create(DynamicRecordContext & ctxt,
                             IQLExpression * left,
                             IQLExpression * right,
                             const SourceLocation& loc)
  {
    DivideExpr * tmp = new DivideExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  DivideExpr * clone() const;
};

class ModulusExpr : public IQLExpression
{
private:
  ModulusExpr(const ModulusExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  ModulusExpr(DynamicRecordContext & ctxt,
              IQLExpression * left,
              IQLExpression * right,
              const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::MOD, left, right, loc)
  {
  }

  static ModulusExpr * create(DynamicRecordContext & ctxt,
                              IQLExpression * left,
                              IQLExpression * right,
                              const SourceLocation& loc)
  {
    ModulusExpr * tmp = new ModulusExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ModulusExpr * clone() const;
};

class ConcatenationExpr : public IQLExpression
{
private:
  ConcatenationExpr(const ConcatenationExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  ConcatenationExpr(DynamicRecordContext & ctxt,
                    IQLExpression * left,
                    IQLExpression * right,
                    const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::CONCAT, left, right, loc)
  {
  }

  static ConcatenationExpr * create(DynamicRecordContext & ctxt,
                                    IQLExpression * left,
                                    IQLExpression * right,
                                    const SourceLocation& loc)
  {
    ConcatenationExpr * tmp = new ConcatenationExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ConcatenationExpr * clone() const;
};

class EqualsExpr : public IQLExpression
{
private:
  EqualsExpr(const EqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  EqualsExpr(DynamicRecordContext & ctxt, 
	     IQLExpression * left,
	     IQLExpression * right,
	     const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::EQ, left, right, loc)
  {
  }

  static EqualsExpr * create(DynamicRecordContext & ctxt,
			     IQLExpression * left,
			     IQLExpression * right,
			     const SourceLocation& loc)
  {
    EqualsExpr * tmp = new EqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  EqualsExpr * clone() const;
};

class NotEqualsExpr : public IQLExpression
{
private:
  NotEqualsExpr(const NotEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  NotEqualsExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::NEQ, left, right, loc)
  {
  }

  static NotEqualsExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				const SourceLocation& loc)
  {
    NotEqualsExpr * tmp = new NotEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  NotEqualsExpr * clone() const;
};

class GreaterThanExpr : public IQLExpression
{
private:
  GreaterThanExpr(const GreaterThanExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  GreaterThanExpr(DynamicRecordContext & ctxt, 
		  IQLExpression * left,
		  IQLExpression * right,
		  const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::GTN, left, right, loc)
  {
  }

  static GreaterThanExpr * create(DynamicRecordContext & ctxt,
				  IQLExpression * left,
				  IQLExpression * right,
				  const SourceLocation& loc)
  {
    GreaterThanExpr * tmp = new GreaterThanExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  GreaterThanExpr * clone() const;
};

class LessThanExpr : public IQLExpression
{
private:
  LessThanExpr(const LessThanExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LessThanExpr(DynamicRecordContext & ctxt, 
	       IQLExpression * left,
	       IQLExpression * right,
	       const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LTN, left, right, loc)
  {
  }

  static LessThanExpr * create(DynamicRecordContext & ctxt,
			       IQLExpression * left,
			       IQLExpression * right,
			       const SourceLocation& loc)
  {
    LessThanExpr * tmp = new LessThanExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LessThanExpr * clone() const;
};

class GreaterThanEqualsExpr : public IQLExpression
{
private:
  GreaterThanEqualsExpr(const GreaterThanEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  GreaterThanEqualsExpr(DynamicRecordContext & ctxt, 
			IQLExpression * left,
			IQLExpression * right,
			const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::GTEQ, left, right, loc)
  {
  }

  static GreaterThanEqualsExpr * create(DynamicRecordContext & ctxt,
					IQLExpression * left,
					IQLExpression * right,
					const SourceLocation& loc)
  {
    GreaterThanEqualsExpr * tmp = new GreaterThanEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  GreaterThanEqualsExpr * clone() const;
};

class LessThanEqualsExpr : public IQLExpression
{
private:
  LessThanEqualsExpr(const LessThanEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LessThanEqualsExpr(DynamicRecordContext & ctxt, 
		     IQLExpression * left,
		     IQLExpression * right,
		     const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LTEQ, left, right, loc)
  {
  }

  static LessThanEqualsExpr * create(DynamicRecordContext & ctxt,
				     IQLExpression * left,
				     IQLExpression * right,
				     const SourceLocation& loc)
  {
    LessThanEqualsExpr * tmp = new LessThanEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LessThanEqualsExpr * clone() const;
};

class SubnetContainsExpr : public IQLExpression
{
private:
  SubnetContainsExpr(const SubnetContainsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  SubnetContainsExpr(DynamicRecordContext & ctxt, 
		     IQLExpression * left,
		     IQLExpression * right,
		     const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::SUBNET_CONTAINS, left, right, loc)
  {
  }

  static SubnetContainsExpr * create(DynamicRecordContext & ctxt,
				     IQLExpression * left,
				     IQLExpression * right,
				     const SourceLocation& loc)
  {
    SubnetContainsExpr * tmp = new SubnetContainsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  SubnetContainsExpr * clone() const;
};

class SubnetContainsEqualsExpr : public IQLExpression
{
private:
  SubnetContainsEqualsExpr(const SubnetContainsEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  SubnetContainsEqualsExpr(DynamicRecordContext & ctxt, 
                           IQLExpression * left,
                           IQLExpression * right,
                           const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::SUBNET_CONTAINSEQ, left, right, loc)
  {
  }

  static SubnetContainsEqualsExpr * create(DynamicRecordContext & ctxt,
                                           IQLExpression * left,
                                           IQLExpression * right,
                                           const SourceLocation& loc)
  {
    SubnetContainsEqualsExpr * tmp = new SubnetContainsEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  SubnetContainsEqualsExpr * clone() const;
};

class SubnetContainedByExpr : public IQLExpression
{
private:
  SubnetContainedByExpr(const SubnetContainedByExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  SubnetContainedByExpr(DynamicRecordContext & ctxt, 
                        IQLExpression * left,
                        IQLExpression * right,
                        const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::SUBNET_CONTAINED, left, right, loc)
  {
  }

  static SubnetContainedByExpr * create(DynamicRecordContext & ctxt,
                                        IQLExpression * left,
                                        IQLExpression * right,
                                        const SourceLocation& loc)
  {
    SubnetContainedByExpr * tmp = new SubnetContainedByExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  SubnetContainedByExpr * clone() const;
};

class SubnetContainedByEqualsExpr : public IQLExpression
{
private:
  SubnetContainedByEqualsExpr(const SubnetContainedByEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  SubnetContainedByEqualsExpr(DynamicRecordContext & ctxt, 
                              IQLExpression * left,
                              IQLExpression * right,
                              const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::SUBNET_CONTAINEDEQ, left, right, loc)
  {
  }

  static SubnetContainedByEqualsExpr * create(DynamicRecordContext & ctxt,
                                              IQLExpression * left,
                                              IQLExpression * right,
                                              const SourceLocation& loc)
  {
    SubnetContainedByEqualsExpr * tmp = new SubnetContainedByEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  SubnetContainedByEqualsExpr * clone() const;
};

class SubnetSymmetricContainsEqualsExpr : public IQLExpression
{
private:
  SubnetSymmetricContainsEqualsExpr(const SubnetSymmetricContainsEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  SubnetSymmetricContainsEqualsExpr(DynamicRecordContext & ctxt, 
                                    IQLExpression * left,
                                    IQLExpression * right,
                                    const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::SUBNET_SYMCONTAINSEQ, left, right, loc)
  {
  }

  static SubnetSymmetricContainsEqualsExpr * create(DynamicRecordContext & ctxt,
                                                    IQLExpression * left,
                                                    IQLExpression * right,
                                                    const SourceLocation& loc)
  {
    SubnetSymmetricContainsEqualsExpr * tmp = new SubnetSymmetricContainsEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  SubnetSymmetricContainsEqualsExpr * clone() const;
};

class CaseExpr : public IQLExpression
{
private:
  CaseExpr(const CaseExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  CaseExpr(DynamicRecordContext & ctxt,
	   const std::vector<IQLExpression *>& args,
	   const SourceLocation& loc);

  static CaseExpr * create(DynamicRecordContext & ctxt,
			   const std::vector<IQLExpression *>& args,
			   const SourceLocation& loc)
  {
    CaseExpr * tmp = new CaseExpr(ctxt, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  CaseExpr * clone() const;
};

class CallExpr : public IQLExpression
{
private:
  CallExpr(const CallExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  CallExpr(DynamicRecordContext & ctxt,
	   const char * fun,
	   const std::vector<IQLExpression *>& args,
	   const SourceLocation& loc);

  static CallExpr * create(DynamicRecordContext & ctxt,
			   const char * fun,
			   const std::vector<IQLExpression *>& args,
			   const SourceLocation& loc)
  {
    CallExpr * tmp = new CallExpr(ctxt, fun, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  CallExpr * clone() const;
};

class CastExpr : public IQLExpression
{
private:
  CastExpr(const CastExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  CastExpr(DynamicRecordContext & ctxt,
           IQLExpression * ty,
	   IQLExpression * arg,
	   const SourceLocation& loc);

  static CastExpr * create(DynamicRecordContext & ctxt,
			   IQLExpression * ty,
			   IQLExpression * arg,
			   const SourceLocation& loc)
  {
    CastExpr * tmp = new CastExpr(ctxt, ty, arg, loc);
    ctxt.add(tmp);
    return tmp;
  }

  CastExpr * clone() const;
};

class Int32Expr : public IQLExpression
{
private:
  Int32Expr(const Int32Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  Int32Expr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static Int32Expr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    Int32Expr * tmp = new Int32Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  Int32Expr * clone() const;
};

class HexInt32Expr : public IQLExpression
{
private:
  HexInt32Expr(const HexInt32Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  HexInt32Expr(DynamicRecordContext & ctxt,
               const char * text,
               const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::INT32, loc)
  {
    setData(text);
  }

  static HexInt32Expr * create(DynamicRecordContext & ctxt,
                               const char * text,
                               const SourceLocation& loc)
  {
    HexInt32Expr * tmp = new HexInt32Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  HexInt32Expr * clone() const;
};

class Int64Expr : public IQLExpression
{
private:
  Int64Expr(const Int64Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  Int64Expr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static Int64Expr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    Int64Expr * tmp = new Int64Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  Int64Expr * clone() const;
};

class IntervalExpr : public IQLExpression
{
private:
  IntervalExpr(const IntervalExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  IntervalExpr(DynamicRecordContext & ctxt, 
	       const char * text,
	       IQLExpression * arg,
	       const SourceLocation& loc);

  static IntervalExpr * create(DynamicRecordContext & ctxt,
			       const char * text,
			       IQLExpression * arg,
			       const SourceLocation& loc)
  {
    IntervalExpr * tmp = new IntervalExpr(ctxt, text, arg, loc);
    ctxt.add(tmp);
    return tmp;
  }

  IntervalExpr * clone() const;
};

class DoubleExpr : public IQLExpression
{
private:
  DoubleExpr(const DoubleExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  DoubleExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static DoubleExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    DoubleExpr * tmp = new DoubleExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  DoubleExpr * clone() const;
};

class DecimalExpr : public IQLExpression
{
private:
  DecimalExpr(const DecimalExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  DecimalExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static DecimalExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    DecimalExpr * tmp = new DecimalExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  DecimalExpr * clone() const;
};

class StringExpr : public IQLExpression
{
private:
  StringExpr(const StringExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  StringExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static StringExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    StringExpr * tmp = new StringExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  StringExpr * clone() const;
};

class BooleanExpr : public IQLExpression
{
private:
  BooleanExpr(const BooleanExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  BooleanExpr(DynamicRecordContext & ctxt, 
	      bool isTrue,
	      const SourceLocation& loc);

  static BooleanExpr * create(DynamicRecordContext & ctxt,
			      bool isTrue,
			      const SourceLocation& loc)
  {
    BooleanExpr * tmp = new BooleanExpr(ctxt, isTrue, loc);
    ctxt.add(tmp);
    return tmp;
  }

  BooleanExpr * clone() const;
};

class IPv4Expr : public IQLExpression
{
private:
  IPv4Expr(const IPv4Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  IPv4Expr(DynamicRecordContext & ctxt, 
           const char * text,
           const SourceLocation& loc);

  static IPv4Expr * create(DynamicRecordContext & ctxt,
                           const char * text,
                           const SourceLocation& loc)
  {
    IPv4Expr * tmp = new IPv4Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  IPv4Expr * clone() const;
};

class IPv6Expr : public IQLExpression
{
private:
  IPv6Expr(const IPv6Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  IPv6Expr(DynamicRecordContext & ctxt, 
           const char * text,
           const SourceLocation& loc);

  static IPv6Expr * create(DynamicRecordContext & ctxt,
                           const char * text,
                           const SourceLocation& loc)
  {
    IPv6Expr * tmp = new IPv6Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  IPv6Expr * clone() const;
};

class NilExpr : public IQLExpression
{
private:
  NilExpr(const NilExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  NilExpr(DynamicRecordContext & ctxt, 
	  const SourceLocation& loc);

  static NilExpr * create(DynamicRecordContext & ctxt,
			      const SourceLocation& loc)
  {
    NilExpr * tmp = new NilExpr(ctxt, loc);
    ctxt.add(tmp);
    return tmp;
  }

  NilExpr * clone() const;
};

class VariableExpr : public IQLExpression
{
private:
  VariableExpr(const VariableExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  VariableExpr(DynamicRecordContext & ctxt, 
	       const char * text,
	       const char * text2,
	       const SourceLocation& loc);

  static VariableExpr * create(DynamicRecordContext & ctxt,
			       const char * text,
			       const char * text2,
			       const SourceLocation& loc)
  {
    VariableExpr * tmp = new VariableExpr(ctxt, text, text2, loc);
    ctxt.add(tmp);
    return tmp;
  }

  VariableExpr * clone() const;
};

class VariableLValueExpr : public IQLExpression
{
private:
  VariableLValueExpr(const VariableLValueExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  VariableLValueExpr(DynamicRecordContext & ctxt, 
	       const char * text,
	       const SourceLocation& loc);

  static VariableLValueExpr * create(DynamicRecordContext & ctxt,
			       const char * text,
			       const SourceLocation& loc)
  {
    VariableLValueExpr * tmp = new VariableLValueExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  VariableLValueExpr * clone() const;
};

class StructMemberReferenceExpr : public IQLExpression
{
private:
  StructMemberReferenceExpr(const StructMemberReferenceExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  StructMemberReferenceExpr(DynamicRecordContext & ctxt, 
                            IQLExpression * s,
                            const char * member,
                            const SourceLocation& loc);

  static StructMemberReferenceExpr * create(DynamicRecordContext & ctxt,
                                            IQLExpression * s,
                                            const char * member,
                                            const SourceLocation& loc)
  {
    StructMemberReferenceExpr * tmp = new StructMemberReferenceExpr(ctxt, s, member, loc);
    ctxt.add(tmp);
    return tmp;
  }

  StructMemberReferenceExpr * clone() const;
};

class StructMemberLValueExpr : public IQLExpression
{
private:
  StructMemberLValueExpr(const StructMemberLValueExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  StructMemberLValueExpr(DynamicRecordContext & ctxt, 
                            IQLExpression * s,
                            const char * member,
                            const SourceLocation& loc);

  static StructMemberLValueExpr * create(DynamicRecordContext & ctxt,
                                            IQLExpression * s,
                                            const char * member,
                                            const SourceLocation& loc)
  {
    StructMemberLValueExpr * tmp = new StructMemberLValueExpr(ctxt, s, member, loc);
    ctxt.add(tmp);
    return tmp;
  }

  StructMemberLValueExpr * clone() const;
};

class ArrayReferenceExpr : public IQLExpression
{
private:
  ArrayReferenceExpr(const ArrayReferenceExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  ArrayReferenceExpr(DynamicRecordContext & ctxt, 
		     IQLExpression * arr,
		     IQLExpression * idx,
		     const SourceLocation& loc);

  static ArrayReferenceExpr * create(DynamicRecordContext & ctxt,
				     IQLExpression * arr,
				     IQLExpression * idx,
				     const SourceLocation& loc)
  {
    ArrayReferenceExpr * tmp = new ArrayReferenceExpr(ctxt, arr, idx, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ArrayReferenceExpr * clone() const;
};

class ArrayLValueExpr : public IQLExpression
{
private:
  ArrayLValueExpr(const ArrayLValueExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  ArrayLValueExpr(DynamicRecordContext & ctxt, 
		     IQLExpression * arr,
		     IQLExpression * idx,
		     const SourceLocation& loc);

  static ArrayLValueExpr * create(DynamicRecordContext & ctxt,
				     IQLExpression * arr,
				     IQLExpression * idx,
				     const SourceLocation& loc)
  {
    ArrayLValueExpr * tmp = new ArrayLValueExpr(ctxt, arr, idx, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ArrayLValueExpr * clone() const;
};

class ArrayExpr : public IQLExpression
{
private:
  ArrayExpr(const ArrayExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  ArrayExpr(DynamicRecordContext & ctxt, 
	    const std::vector<IQLExpression *>& args,
	    const SourceLocation& loc);
  
  static ArrayExpr * create(DynamicRecordContext & ctxt,
			    const std::vector<IQLExpression *>& args,
			    const SourceLocation& loc)
  {
    ArrayExpr * tmp = new ArrayExpr(ctxt, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ArrayExpr * clone() const;
};

class StructExpr : public IQLExpression
{
private:
  StructExpr(const StructExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  StructExpr(DynamicRecordContext & ctxt, 
             const std::vector<IQLExpression *>& args,
             const SourceLocation& loc);
  
  static StructExpr * create(DynamicRecordContext & ctxt,
                             const std::vector<IQLExpression *>& args,
                             const SourceLocation& loc)
  {
    StructExpr * tmp = new StructExpr(ctxt, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  StructExpr * clone() const;
};

class TypeExpr : public IQLExpression
{
private:
  TypeExpr(const TypeExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  TypeExpr(DynamicRecordContext & ctxt, 
           const FieldType * ty,
           const SourceLocation& loc);
  
  static TypeExpr * create(DynamicRecordContext & ctxt,
                           const FieldType * ty,
                             const SourceLocation& loc)
  {
    TypeExpr * tmp = new TypeExpr(ctxt, ty, loc);
    ctxt.add(tmp);
    return tmp;
  }

  TypeExpr * clone() const;
};

class DecltypeExpr : public IQLExpression
{
private:
  DecltypeExpr(const DecltypeExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  DecltypeExpr(DynamicRecordContext & ctxt, 
               IQLExpression * expr,
               const char * sz,
               const SourceLocation& loc);
  
  static DecltypeExpr * create(DynamicRecordContext & ctxt,
                               IQLExpression * expr,
                               const char * sz,
                               const SourceLocation& loc)
  {
    DecltypeExpr * tmp = new DecltypeExpr(ctxt, expr, sz, loc);
    ctxt.add(tmp);
    return tmp;
  }

  DecltypeExpr * clone() const;
};

/**
 * Walks a predicate given two input records and 
 * extracts the equijoin keys.
 */
class IQLEquiJoinDetector
{
private:
  DynamicRecordContext & mContext;
  const class RecordType * mLeft;
  const class RecordType * mRight;
  IQLExpression * mInput;

  std::vector<const std::string*> mLeftEquiJoinKeys;
  std::vector<const std::string*> mRightEquiJoinKeys;

  IQLExpression * mEquals;
  IQLExpression * mResidual;
  
  /**
   * When an equijoin pred is found, rewrite the
   * tree.
   */
  void handleEquiJoin(IQLExpression * eq, 
		      IQLExpression * p);
  /**
   * Process equality and look for equijoin pattern.
   */
  void handleEq(const class RecordType * left,
		const class RecordType * right,
		IQLExpression * eq, 
		IQLExpression * parent);
public:
  IQLEquiJoinDetector(DynamicRecordContext & ctxt,
		      const class RecordType * left,
		      const class RecordType * right,
		      IQLExpression * input);
  IQLExpression * getEquals()
  {
    return mEquals;
  }
  IQLExpression * getResidual()
  {
    return mResidual;
  }
  const std::vector<const std::string*>& getLeftEquiJoinKeys() const
  {
    return mLeftEquiJoinKeys;
  }
  const std::vector<const std::string*>& getRightEquiJoinKeys() const
  {
    return mRightEquiJoinKeys;
  }
};


class IQLFreeVariablesRule
{
private:
  // TODO: Is is worth being more efficient
  // and using a bitmap or some other kind of
  // efficient sparse set implementation?
  std::set<std::string> mVariables;
  void onExpr(IQLExpression * expr);
public:
  IQLFreeVariablesRule(IQLExpression * expr);
  const std::set<std::string>& getVariables() const;
};

/**
 * Given a pair of inputs and a predicate, splits the
 * predicate into clauses that can be evaluated entirely
 * against each input, the clauses that require both
 * inputs and the clauses that refer to variables
 * not in either input.
 */
class IQLSplitPredicateRule
{
private:
  IQLExpression * mLeft;
  IQLExpression * mRight;
  IQLExpression * mBoth;
  IQLExpression * mOther;
  static void addClause(DynamicRecordContext & ctxt,
			IQLExpression *& pred,
			IQLExpression * clause);
  void onExpr(DynamicRecordContext & ctxt,
	      const RecordType * left,
	      const RecordType * right,
	      IQLExpression * input,
	      IQLExpression * parent);
  static bool contains(const class RecordType * ty,
		       const std::set<std::string>& vars)
  {
    return contains(ty, NULL, vars);
  }
  static bool contains(const class RecordType * ty1,
		       const class RecordType * ty2,
		       const std::set<std::string>& vars);
public:
  IQLSplitPredicateRule(DynamicRecordContext & ctxt,
			const class RecordType * left,
			const class RecordType * right,
			IQLExpression * input);
  IQLExpression * getLeft() 
  {
    return mLeft; 
  }
  IQLExpression * getRight() 
  {
    return mRight; 
  }
  IQLExpression * getBoth() 
  {
    return mBoth; 
  }
  IQLExpression * getOther() 
  {
    return mOther; 
  }
};

class IQLExpression * unwrap(IQLExpressionRef r);
IQLExpressionRef wrap(class IQLExpression * r);
std::vector<class IQLStatement *> * unwrap(IQLStatementListRef r);
IQLStatementListRef wrap(std::vector<class IQLStatement *> *  r);
class DynamicRecordContext * unwrap(IQLTreeFactoryRef r);
class IQLStatement * unwrap(IQLStatementRef r);
IQLTreeFactoryRef wrap(class DynamicRecordContext * r);
class IQLGraphBuilder * unwrap(IQLGraphContextRef ctxt);
IQLGraphContextRef wrap(class IQLGraphBuilder *);
class IQLRecordTypeBuilder * unwrap(IQLRecordTypeContextRef ctxt);
IQLRecordTypeContextRef wrap(class IQLRecordTypeBuilder *);

#endif
