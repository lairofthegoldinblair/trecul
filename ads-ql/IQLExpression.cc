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

#include <stack>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>
#include "IQLExpression.hh"
#include "IQLBuildTree.h"
#include "IQLGraphBuilder.hh"
#include "RecordType.hh"

IQLExpression::~IQLExpression()
{
}

bool IQLExpression::equals(const IQLExpression * rhs) const
{
  const IQLExpression * input = this;
  const IQLExpression * pattern = rhs;

  if (!input->shallow_equals(pattern)) 
    return false;

  std::stack<std::pair<IQLExpression::arg_const_iterator, 
    IQLExpression::arg_const_iterator> > patternStk;
  std::vector<IQLStatement *> patternTmp;
  std::stack<std::pair<IQLExpression::arg_const_iterator, 
    IQLExpression::arg_const_iterator> > inputStk;
  std::vector<IQLStatement *> inputTmp;
  // Push the node with arguments on the stack.
  patternTmp.push_back(const_cast<IQLExpression *>(pattern));
  inputTmp.push_back(const_cast<IQLExpression *>(input));
  patternStk.push(std::make_pair(boost::make_transform_iterator(patternTmp.begin(), Downcast()),
                                 boost::make_transform_iterator(patternTmp.end(), Downcast())));
  inputStk.push(std::make_pair(boost::make_transform_iterator(inputTmp.begin(), Downcast()),
                               boost::make_transform_iterator(inputTmp.end(), Downcast())));

  while(patternStk.size() && inputStk.size()) {
    if (patternStk.top().first != patternStk.top().second) {      
      pattern = *patternStk.top().first;
      input = *inputStk.top().first;
      // Make sure number of args is the same
      if (false == input->shallow_equals(pattern) ||
	  input->args_size() != pattern->args_size()) {
	return false;
      }
      patternStk.push(std::make_pair(pattern->begin_args(), 
				     pattern->end_args()));
      inputStk.push(std::make_pair(input->begin_args(), 
				   input->end_args()));
    } else {
      patternStk.pop();
      inputStk.pop();
      if (patternStk.size()) {
	++patternStk.top().first;
	++inputStk.top().first;
      } 
    }    
  }

  return patternStk.size() == 0 && inputStk.size() == 0;
}

LogicalOrExpr * LogicalOrExpr::clone() const
{
  return new LogicalOrExpr(*this);
}

LogicalAndExpr * LogicalAndExpr::clone() const
{
  return new LogicalAndExpr(*this);
}

LogicalNotExpr * LogicalNotExpr::clone() const
{
  return new LogicalNotExpr(*this);
}

IsNullExpr * IsNullExpr::clone() const
{
  return new IsNullExpr(*this);
}

BitwiseAndExpr * BitwiseAndExpr::clone() const
{
  return new BitwiseAndExpr(*this);
}

BitwiseOrExpr * BitwiseOrExpr::clone() const
{
  return new BitwiseOrExpr(*this);
}

BitwiseXorExpr * BitwiseXorExpr::clone() const
{
  return new BitwiseXorExpr(*this);
}

BitwiseNotExpr * BitwiseNotExpr::clone() const
{
  return new BitwiseNotExpr(*this);
}

PlusExpr * PlusExpr::clone() const
{
  return new PlusExpr(*this);
}

MinusExpr * MinusExpr::clone() const
{
  return new MinusExpr(*this);
}

TimesExpr * TimesExpr::clone() const
{
  return new TimesExpr(*this);
}

DivideExpr * DivideExpr::clone() const
{
  return new DivideExpr(*this);
}

ModulusExpr * ModulusExpr::clone() const
{
  return new ModulusExpr(*this);
}

ConcatenationExpr * ConcatenationExpr::clone() const
{
  return new ConcatenationExpr(*this);
}

EqualsExpr * EqualsExpr::clone() const
{
  return new EqualsExpr(*this);
}

NotEqualsExpr * NotEqualsExpr::clone() const
{
  return new NotEqualsExpr(*this);
}

GreaterThanExpr * GreaterThanExpr::clone() const
{
  return new GreaterThanExpr(*this);
}

LessThanExpr * LessThanExpr::clone() const
{
  return new LessThanExpr(*this);
}

GreaterThanEqualsExpr * GreaterThanEqualsExpr::clone() const
{
  return new GreaterThanEqualsExpr(*this);
}

LessThanEqualsExpr * LessThanEqualsExpr::clone() const
{
  return new LessThanEqualsExpr(*this);
}

SubnetContainsExpr * SubnetContainsExpr::clone() const
{
  return new SubnetContainsExpr(*this);
}

SubnetContainsEqualsExpr * SubnetContainsEqualsExpr::clone() const
{
  return new SubnetContainsEqualsExpr(*this);
}

SubnetContainedByExpr * SubnetContainedByExpr::clone() const
{
  return new SubnetContainedByExpr(*this);
}

SubnetContainedByEqualsExpr * SubnetContainedByEqualsExpr::clone() const
{
  return new SubnetContainedByEqualsExpr(*this);
}

SubnetSymmetricContainsEqualsExpr * SubnetSymmetricContainsEqualsExpr::clone() const
{
  return new SubnetSymmetricContainsEqualsExpr(*this);
}

CaseExpr::CaseExpr(DynamicRecordContext & ctxt,
		   const std::vector<IQLExpression *>& args,
		   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::CASE, 
		args.begin(), args.end(), 
		loc)
{
}

CaseExpr * CaseExpr::clone() const
{
  return new CaseExpr(*this);
}

CallExpr::CallExpr(DynamicRecordContext & ctxt,
		   const char * fun,
		   const std::vector<IQLExpression *>& args,
		   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::CALL, 
		args.begin(), args.end(), 
		loc)
{
  setData(fun);
}

CallExpr * CallExpr::clone() const
{
  return new CallExpr(*this);
}

CastExpr::CastExpr(DynamicRecordContext & ctxt,
		   IQLExpression * ty,
		   IQLExpression * arg,
		   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::CAST, 
                ty,
		arg,
		loc)
{
}

CastExpr * CastExpr::clone() const
{
  return new CastExpr(*this);
}

Int32Expr::Int32Expr(DynamicRecordContext & ctxt, 
		     const char * text,
		     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::INT32, loc)
{
  // TODO: Should we check the string now or later?
  // On the one hand haven't yet constructed our object
  // nor has anyone asked for type checking.  On the
  // other hand, we'll need to convert state later.
  setData(text);
}

Int32Expr * Int32Expr::clone() const
{
  return new Int32Expr(*this);
}

HexInt32Expr * HexInt32Expr::clone() const
{
  return new HexInt32Expr(*this);
}

Int64Expr::Int64Expr(DynamicRecordContext & ctxt, 
		     const char * text,
		     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::INT64, loc)
{
  setData(text);
}

Int64Expr * Int64Expr::clone() const
{
  return new Int64Expr(*this);
}

IntervalExpr::IntervalExpr(DynamicRecordContext & ctxt, 
			   const char * text,
			   IQLExpression * arg,
			   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::INTERVAL, arg, loc)
{
  setData(text);
}

IntervalExpr * IntervalExpr::clone() const
{
  return new IntervalExpr(*this);
}

DoubleExpr::DoubleExpr(DynamicRecordContext & ctxt, 
		       const char * text,
		       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::DOUBLE, loc)
{
  setData(text);
}

DoubleExpr * DoubleExpr::clone() const
{
  return new DoubleExpr(*this);
}

DecimalExpr::DecimalExpr(DynamicRecordContext & ctxt, 
			 const char * text,
			 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::DECIMAL, loc)
{
  setData(text);
}

DecimalExpr * DecimalExpr::clone() const
{
  return new DecimalExpr(*this);
}

StringExpr::StringExpr(DynamicRecordContext & ctxt, 
		       const char * text,
		       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::STRING, loc)
{
  setData(text);
}

StringExpr * StringExpr::clone() const
{
  return new StringExpr(*this);
}

BooleanExpr::BooleanExpr(DynamicRecordContext & ctxt, 
			 bool isTrue,
			 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::BOOLEAN, loc)
{
  setData(isTrue ? 1 : 0);
}

BooleanExpr * BooleanExpr::clone() const
{
  return new BooleanExpr(*this);
}

IPv4Expr::IPv4Expr(DynamicRecordContext & ctxt, 
			 const char * text,
			 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::IPV4, loc)
{
  setData(text);
}

IPv4Expr * IPv4Expr::clone() const
{
  return new IPv4Expr(*this);
}

IPv6Expr::IPv6Expr(DynamicRecordContext & ctxt, 
			 const char * text,
			 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::IPV6, loc)
{
  setData(text);
}

IPv6Expr * IPv6Expr::clone() const
{
  return new IPv6Expr(*this);
}

NilExpr::NilExpr(DynamicRecordContext & ctxt, 
		 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::NIL, loc)
{
}

NilExpr * NilExpr::clone() const
{
  return new NilExpr(*this);
}

VariableExpr::VariableExpr(DynamicRecordContext & ctxt, 
			   const char * text,
			   const char * text2,
			   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::VARIABLE, loc)
{
  // TODO: store this parsed?
  std::string str(text);
  if (text2) {
    str += ".";
    str += text2;
  }
  setData(str.c_str());
}

VariableExpr * VariableExpr::clone() const
{
  return new VariableExpr(*this);
}

VariableLValueExpr::VariableLValueExpr(DynamicRecordContext & ctxt, 
                                       const char * text,
                                       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::VARIABLELVALUE, loc)
{
  setData(text);
}

VariableLValueExpr * VariableLValueExpr::clone() const
{
  return new VariableLValueExpr(*this);
}

StructMemberReferenceExpr::StructMemberReferenceExpr(DynamicRecordContext & ctxt, 
                                                     IQLExpression * s,
                                                     const char * member,
                                                     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::STRUCTMEMBERREF, s, loc)
{
  setData(member);
}

StructMemberReferenceExpr * StructMemberReferenceExpr::clone() const
{
  return new StructMemberReferenceExpr(*this);
}

StructMemberLValueExpr::StructMemberLValueExpr(DynamicRecordContext & ctxt, 
                                                     IQLExpression * s,
                                                     const char * member,
                                                     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::STRUCTMEMBERLVALUE, s, loc)
{
  setData(member);
}

StructMemberLValueExpr * StructMemberLValueExpr::clone() const
{
  return new StructMemberLValueExpr(*this);
}

ArrayReferenceExpr::ArrayReferenceExpr(DynamicRecordContext & ctxt, 
				       IQLExpression * arr,
				       IQLExpression * idx,
				       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::ARRAYREF, arr, idx, loc)
{
}

ArrayReferenceExpr * ArrayReferenceExpr::clone() const
{
  return new ArrayReferenceExpr(*this);
}

ArrayLValueExpr::ArrayLValueExpr(DynamicRecordContext & ctxt, 
				       IQLExpression * arr,
				       IQLExpression * idx,
				       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::ARRAYLVALUE, arr, idx, loc)
{
}

ArrayLValueExpr * ArrayLValueExpr::clone() const
{
  return new ArrayLValueExpr(*this);
}

ArrayExpr::ArrayExpr(DynamicRecordContext & ctxt,
		     const std::vector<IQLExpression *>& args,
		     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::ARR, 
		args.begin(), args.end(), 
		loc)
{
}

ArrayExpr * ArrayExpr::clone() const
{
  return new ArrayExpr(*this);
}

StructExpr::StructExpr(DynamicRecordContext & ctxt,
                       const std::vector<IQLExpression *>& args,
                       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::STRUCT, 
		args.begin(), args.end(), 
		loc)
{
}

StructExpr * StructExpr::clone() const
{
  return new StructExpr(*this);
}


TypeExpr::TypeExpr(DynamicRecordContext & ctxt, 
                   const FieldType * ty,
                   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::TYPE, loc)
{
  setFieldType(ty);
}
  
TypeExpr * TypeExpr::clone() const
{
  return new TypeExpr(*this);
}

DecltypeExpr::DecltypeExpr(DynamicRecordContext & ctxt, 
                           IQLExpression * e,
                           const char * sz,
                           const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::DECLTYPE, e, loc)
{
  if (nullptr != sz) {
    setData(sz);
  } else {
    setData("");
  }
}
  
DecltypeExpr * DecltypeExpr::clone() const
{
  return new DecltypeExpr(*this);
}

IQLEquiJoinDetector::IQLEquiJoinDetector(DynamicRecordContext& ctxt,
					 const RecordType * left,
					 const RecordType * right,
					 IQLExpression * input)
  :
  mContext(ctxt),
  mEquals(NULL),
  mResidual(input)
{
  // First lift all ANDs
  // The trees we are building are generally left associative 
  // so we do less work if we put all ANDs on the left boundary
  // of the parse tree.
  for(IQLExpression * e = mResidual; e != NULL; ) {
    if (e->getNodeType() == IQLExpression::LAND) {
      // While right child is an AND, rotate to left.
      while ((*(e->begin_args()+1))->getNodeType() == IQLExpression::LAND) {
	e->rotateRightChild();
      }
      e = *e->begin_args();
    } else {
      e = NULL;
    }
  }  
  // Scan for patterns (EQ x y) where x and y are VARIABLE 
  // from left & right (in either order).  Only process if a
  // top level conjunctive clause.
  // TODO: Should we put the predicate in conjunctive normal
  // form first?  Is that well defined with SQL ternary logic?
  // E.g. is NOT (a <> b OR c <> d) a valid equijoin predicate?
  for(IQLExpression * e = mResidual, * p=NULL; e != NULL; ) {
    switch (e->getNodeType()) {
    case IQLExpression::LAND:
      if ((*(e->begin_args()+1))->getNodeType() == IQLExpression::EQ) {
	handleEq(left, right, *(e->begin_args()+1), e);
      }
      p = e;
      e = *e->begin_args();
      break;
    case IQLExpression::EQ:
      {
	handleEq(left, right, e, p);
	e = NULL;
	break;
      }
    default:
      e = NULL;
      break;
    }
  }

  // Eliminate inserted TRUE(s) and store residual
  for(IQLExpression * e = mResidual, * p=NULL; e != NULL; ) {
    if(e->getNodeType()==IQLExpression::LAND) {
      if((*(e->begin_args()+1))->getNodeType() == IQLExpression::BOOLEAN &&
	 (*(e->begin_args()+1))->getBooleanData()) {
	if (NULL != p) {
	  IQLExpression * tmp = *e->begin_args();
	  p->replaceArg(e, tmp);
	  e = tmp;
	} else {
	  mResidual = e = *e->begin_args();
	}
      } else if((*(e->begin_args()))->getNodeType() == IQLExpression::BOOLEAN &&
		(*(e->begin_args()))->getBooleanData()) {
	if (NULL != p) {
	  IQLExpression * tmp = *(e->begin_args()+1);
	  p->replaceArg(e, tmp);
	  e = tmp;
	} else {
	  mResidual = e = *(e->begin_args()+1);
	}
      } else {
	p = e;
	e = *e->begin_args();
      }
    } else if (e->getNodeType() == IQLExpression::BOOLEAN &&
	       e->getBooleanData()) {
      if(NULL == p) {
	e = mResidual = NULL;
      }      
    } else {
      e = NULL;
    }
  }
}

void IQLEquiJoinDetector::handleEquiJoin(IQLExpression * eq, 
					 IQLExpression * p)
{
  if (p) {
    // Replace equality with TRUE
    IQLExpression * tmp = BooleanExpr::create(mContext, true, 
					      eq->getSourceLocation());
    p->replaceArg(eq,tmp);
  } else {
    mResidual = NULL;
  }
  
  // Append eq to equijoin predicate
  if (mEquals) {
    mEquals = LogicalAndExpr::create(mContext, mEquals, eq, 
				     eq->getSourceLocation());
  } else {
    mEquals = eq;
  }
}

void IQLEquiJoinDetector::handleEq(const RecordType * left,
				   const RecordType * right,
				   IQLExpression * eq,
				   IQLExpression * p)
{
  IQLExpression::arg_const_iterator it = eq->begin_args();
  IQLExpression * lexpr = *it;
  IQLExpression * rexpr = *(++it);
  if (lexpr->getNodeType() == IQLExpression::VARIABLE &&
      rexpr->getNodeType() == IQLExpression::VARIABLE) {
    const std::string & lvar(lexpr->getStringData());
    const std::string & rvar(rexpr->getStringData());
    int32_t tst = (int32_t) left->hasMember(lvar)
      + 2*(int32_t) right->hasMember(lvar) 
      + 4*(int32_t) left->hasMember(rvar) 
      + 8*(int32_t) right->hasMember(rvar) ;
    switch(tst) {
    case 6:
      mLeftEquiJoinKeys.push_back(&rvar);
      mRightEquiJoinKeys.push_back(&lvar);
      handleEquiJoin(eq, p);
      break;
    case 9:
      mLeftEquiJoinKeys.push_back(&lvar);
      mRightEquiJoinKeys.push_back(&rvar);
      handleEquiJoin(eq, p);
      break;
    case 3:
    case 12:
    case 15:
      throw std::runtime_error("Ambiguous: need to implement alias support");
      break;
    default:
      break;
    }
  }
}

IQLFreeVariablesRule::IQLFreeVariablesRule(IQLExpression * expr)
{
  // TODO: Memoization of the rule????
  onExpr(expr);
}

void IQLFreeVariablesRule::onExpr(IQLExpression * expr)
{
  switch(expr->getNodeType()) {
  case IQLExpression::VARIABLE:
    if (mVariables.find(expr->getStringData()) ==
	mVariables.end()) {
      mVariables.insert(expr->getStringData());
    }
    break;
  case IQLExpression::STRUCTMEMBERREF:
    {
      IQLExpression * base = *expr->begin_args();
      if (base->getNodeType() == IQLExpression::VARIABLE) {
	std::string qualified = base->getStringData() + "." + expr->getStringData();
	if (mVariables.find(qualified) == mVariables.end()) {
	  mVariables.insert(qualified);
	}
      } else {
	onExpr(base);
      }
    }
    break;
  case IQLExpression::ARRAYREF:
    for(IQLExpression::arg_const_iterator a = expr->begin_args(),
	e = expr->end_args(); a != e; ++a) {
      onExpr(*a);
    }
    break;
  // TODO: Handle DECLARE since these are binders.
  default:
    for(IQLExpression::arg_const_iterator a = expr->begin_args(),
	  e = expr->end_args(); a != e; ++a) {
      onExpr(*a);
    }
    break;
  }
}

const std::set<std::string>& IQLFreeVariablesRule::getVariables() const
{
  return mVariables;
}

IQLSplitPredicateRule::IQLSplitPredicateRule(DynamicRecordContext & ctxt,
					     const RecordType * left,
					     const RecordType * right,
					     IQLExpression * input)
  :
  mLeft(NULL),
  mRight(NULL),
  mBoth(NULL),
  mOther(NULL)
{
  onExpr(ctxt, left, right, input, NULL);
}

bool IQLSplitPredicateRule::contains(const class RecordType * ty1,
				     const class RecordType * ty2,
				     const std::set<std::string>& vars)
{
  for(std::set<std::string>::const_iterator v = vars.begin(),
	e = vars.end(); v != e; ++v) {
    if (!ty1->hasMember(*v) &&
	(ty2==NULL || !ty2->hasMember(*v))) {
      return false;
    }
  }
  return true;
}

void IQLSplitPredicateRule::addClause(DynamicRecordContext & ctxt,
				      IQLExpression *& pred,
				      IQLExpression * clause)
{
  if (pred) {
    pred = LogicalAndExpr::create(ctxt, pred, clause, 
				   clause->getSourceLocation());
  } else {
    pred = clause;
  }
}

void IQLSplitPredicateRule::onExpr(DynamicRecordContext & ctxt,
				   const RecordType * left,
				   const RecordType * right,
				   IQLExpression * expr,
				   IQLExpression * parent)
{
    switch(expr->getNodeType()) {
    case IQLExpression::LAND:
      {
	onExpr(ctxt, left, right, *expr->begin_args(), expr);
	onExpr(ctxt, left, right, *(expr->begin_args()+1), expr);
      }
      break;
    default:
      {
	// Get free variables and figure out where this goes
	IQLFreeVariablesRule fv(expr);
	if (contains(left, fv.getVariables())) {
	  addClause(ctxt, mLeft, expr);
	} else if (contains(right, fv.getVariables())) {
	  addClause(ctxt, mRight, expr);
	} else if (contains(left, right, fv.getVariables())) {
	  addClause(ctxt, mBoth, expr);
	} else {
	  addClause(ctxt, mOther, expr);
	}
	if (parent) {
	  parent->replaceArg(expr, BooleanExpr::create(ctxt, true, SourceLocation()));
	}
	break;
      }
    }  
}

// Implementation of the C binding
class IQLStatement * unwrap(IQLStatementRef r)
{
  return reinterpret_cast<class IQLStatement *>(r);
}

IQLStatementRef wrap(class IQLStatement * r)
{
  return reinterpret_cast<IQLStatementRef>(r);
}

std::vector<IQLStatement *> * unwrap(IQLStatementListRef r)
{
  return reinterpret_cast<std::vector<IQLStatement *> *>(r);
}

IQLStatementListRef wrap(std::vector<IQLStatement *> * r)
{
  return reinterpret_cast<IQLStatementListRef>(r);
}

class IQLExpression * unwrap(IQLExpressionRef r)
{
  return reinterpret_cast<class IQLExpression *>(r);
}

IQLExpressionRef wrap(class IQLExpression * r)
{
  return reinterpret_cast<IQLExpressionRef>(r);
}

std::vector<IQLExpression *> * unwrap(IQLExpressionListRef r)
{
  return reinterpret_cast<std::vector<IQLExpression *> *>(r);
}

IQLExpressionListRef wrap(std::vector<IQLExpression *> * r)
{
  return reinterpret_cast<IQLExpressionListRef>(r);
}

class DynamicRecordContext * unwrap(IQLTreeFactoryRef r)
{
  return reinterpret_cast<class DynamicRecordContext *>(r);
}

IQLTreeFactoryRef wrap(class DynamicRecordContext * r)
{
  return reinterpret_cast<IQLTreeFactoryRef>(r);
}

IQLGraphBuilder * unwrap(IQLGraphContextRef val)
{
  return reinterpret_cast<IQLGraphBuilder *>(val);
}

IQLGraphContextRef wrap(IQLGraphBuilder * val)
{
  return reinterpret_cast<IQLGraphContextRef>(val);
}

const FieldType * unwrap(IQLFieldTypeRef r)
{
  return reinterpret_cast<const FieldType *> (r);
}

IQLFieldTypeRef wrap(const FieldType * r)
{
  return reinterpret_cast<IQLFieldTypeRef> (r);
}

IQLExpressionListRef IQLExpressionListCreate(IQLTreeFactoryRef ctxt)
{
  return wrap(new std::vector<IQLExpression *> ());
}

void IQLExpressionListFree(IQLTreeFactoryRef ctxt,
			   IQLExpressionListRef l)
{
  return delete unwrap(l);
}

void IQLExpressionListAppend(IQLTreeFactoryRef ctxt,
			     IQLExpressionListRef l,
			     IQLExpressionRef e)
{
  unwrap(l)->push_back(unwrap(e));
}

IQLExpressionRef IQLBuildTypeExpr(IQLTreeFactoryRef ctxtRef,
                                  IQLFieldTypeRef ft,
                                  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(TypeExpr::create(ctxt, unwrap(ft),
				    SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildDecltype(IQLTreeFactoryRef ctxtRef,
                                  IQLExpressionRef e,
                                  const char * sz,
                                  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DecltypeExpr::create(ctxt, unwrap(e), sz,
                                   SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildLogicalOr(IQLTreeFactoryRef ctxtRef,				     
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalOrExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				    SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildLogicalAnd(IQLTreeFactoryRef ctxtRef,				     
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalAndExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				     SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildLogicalNot(IQLTreeFactoryRef ctxtRef,				     
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalNotExpr::create(ctxt, unwrap(leftRef),
				     SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildEquals(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef leftRef,
				IQLExpressionRef rightRef,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(EqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildNotEquals(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NotEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				    SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildGreaterThan(IQLTreeFactoryRef ctxtRef,
				     IQLExpressionRef leftRef,
				     IQLExpressionRef rightRef,
				     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(GreaterThanExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				      SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildLessThan(IQLTreeFactoryRef ctxtRef,
				  IQLExpressionRef leftRef,
				  IQLExpressionRef rightRef,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LessThanExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				   SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildGreaterThanEquals(IQLTreeFactoryRef ctxtRef,
					   IQLExpressionRef leftRef,
					   IQLExpressionRef rightRef,
					   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(GreaterThanEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					    SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildLessThanEquals(IQLTreeFactoryRef ctxtRef,
					IQLExpressionRef leftRef,
					IQLExpressionRef rightRef,
					int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LessThanEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildUnaryFun(IQLTreeFactoryRef ctxtRef,
				  const char * fun,
				  IQLExpressionRef leftRef,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> args;
  args.push_back(unwrap(leftRef));
  return wrap(CallExpr::create(ctxt, fun, args, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildBinaryFun(IQLTreeFactoryRef ctxtRef,
				   const char * fun,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> args;
  args.push_back(unwrap(leftRef));
  args.push_back(unwrap(rightRef));
  return wrap(CallExpr::create(ctxt, fun, args, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildSubnetContains(IQLTreeFactoryRef ctxtRef,
					IQLExpressionRef leftRef,
					IQLExpressionRef rightRef,
					int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(SubnetContainsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildSubnetContainsEquals(IQLTreeFactoryRef ctxtRef,
                                              IQLExpressionRef leftRef,
                                              IQLExpressionRef rightRef,
                                              int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(SubnetContainsEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
                                               SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildSubnetContainedBy(IQLTreeFactoryRef ctxtRef,
                                           IQLExpressionRef leftRef,
                                           IQLExpressionRef rightRef,
                                           int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(SubnetContainedByExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
                                            SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildSubnetContainedByEquals(IQLTreeFactoryRef ctxtRef,
                                                 IQLExpressionRef leftRef,
                                                 IQLExpressionRef rightRef,
                                                 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(SubnetContainedByEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
                                                  SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildSubnetSymmetricContainsEquals(IQLTreeFactoryRef ctxtRef,
                                                       IQLExpressionRef leftRef,
                                                       IQLExpressionRef rightRef,
                                                       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(SubnetSymmetricContainsEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
                                                        SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildLike(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionRef leftRef,
			      IQLExpressionRef rightRef,
			      int line, int column)
{
  throw std::runtime_error("LIKE not supported yet; use RLIKE instead");
}

IQLExpressionRef IQLBuildRLike(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionRef leftRef,
			       IQLExpressionRef rightRef,
			       int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "rlike", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildPlus(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionRef leftRef,
			      IQLExpressionRef rightRef,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(PlusExpr::createBinary(ctxt, unwrap(leftRef), unwrap(rightRef),
            SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildMinus(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionRef leftRef,
			       IQLExpressionRef rightRef,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(MinusExpr::createBinary(ctxt, unwrap(leftRef), unwrap(rightRef),
             SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildTimes(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionRef leftRef,
			       IQLExpressionRef rightRef,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(TimesExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
       SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildDivide(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef leftRef,
				IQLExpressionRef rightRef,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DivideExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
        SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildModulus(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef leftRef,
				 IQLExpressionRef rightRef,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ModulusExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
         SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildConcatenation(IQLTreeFactoryRef ctxtRef,
                                       IQLExpressionRef leftRef,
                                       IQLExpressionRef rightRef,
                                       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ConcatenationExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				 SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildUnaryPlus(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(PlusExpr::createUnary(ctxt, unwrap(leftRef),
			     SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildUnaryMinus(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(MinusExpr::createUnary(ctxt, unwrap(leftRef),
			      SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildBitwiseAnd(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BitwiseAndExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
            SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildBitwiseOr(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BitwiseOrExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
           SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildBitwiseXor(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BitwiseXorExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
            SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildBitwiseNot(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BitwiseNotExpr::create(ctxt, unwrap(leftRef),
			      SourceLocation(line, column)));
}


IQLExpressionRef IQLBuildCase(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionListRef argsRef,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(CaseExpr::create(ctxt, args, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildCast(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionRef ty,
			      IQLExpressionRef arg,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(CastExpr::create(ctxt, unwrap(ty), unwrap(arg), SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildLiteralCast(IQLTreeFactoryRef ctxtRef,
				     const char * typeName,
				     const char * arg,
				     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  const FieldType * ty = NULL;
  if (boost::algorithm::iequals("date", typeName)) {
    ty = DateType::Get(ctxt);
  } else if (boost::algorithm::iequals("datetime", typeName)) {
    ty = DatetimeType::Get(ctxt);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }
  return wrap(CastExpr::create(ctxt,
                               TypeExpr::create(ctxt, ty, SourceLocation(line, column)),
                               StringExpr::create(ctxt, arg, SourceLocation(line, column)),
			       SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildCall(IQLTreeFactoryRef ctxtRef,
			      const char * fun,
			      IQLExpressionListRef argsRef,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(CallExpr::create(ctxt, fun, args, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildInt32(IQLTreeFactoryRef ctxtRef,
			       const char * text,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Expr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildHexInt32(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(HexInt32Expr::create(ctxt, text, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildInt64(IQLTreeFactoryRef ctxtRef,
			       const char * text,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int64Expr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildIPv4(IQLTreeFactoryRef ctxtRef,
                              const char * text,
                              int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IPv4Expr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildIPv6(IQLTreeFactoryRef ctxtRef,
                              const char * text,
                              int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IPv6Expr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildInterval(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  IQLExpressionRef arg,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IntervalExpr::create(ctxt, text, unwrap(arg), SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildBoolean(IQLTreeFactoryRef ctxtRef,
				 int isTrue,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BooleanExpr::create(ctxt, isTrue!=0, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildNil(IQLTreeFactoryRef ctxtRef,
			     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NilExpr::create(ctxt, SourceLocation(line, column)));    
}

IQLExpressionRef IQLBuildIsNull(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef left,
				int isNot,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IsNullExpr::create(ctxt, unwrap(left), isNot != 0,
       SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildDouble(IQLTreeFactoryRef ctxtRef,
				const char * text,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DoubleExpr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildDecimal(IQLTreeFactoryRef ctxtRef,
				 const char * text,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DecimalExpr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildString(IQLTreeFactoryRef ctxtRef,
				const char * text,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(StringExpr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildWString(IQLTreeFactoryRef ctxtRef,
         const char * text,
         int line, int column)
{
  throw std::runtime_error("WSTRING_LITERAL not supported yet");
}

IQLExpressionRef IQLBuildVariable(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  const char * text2,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(VariableExpr::create(ctxt, text, text2, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildVariableLValue(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(VariableLValueExpr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildStructMemberRef(IQLTreeFactoryRef ctxtRef,
                                         IQLExpressionRef s,
                                         const char * member,
                                         int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(StructMemberReferenceExpr::create(ctxt, unwrap(s), member, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildStructMemberLValue(IQLTreeFactoryRef ctxtRef,
                                            IQLExpressionRef s,
                                            const char * member,
                                            int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(StructMemberLValueExpr::create(ctxt, unwrap(s), member, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildArrayRef(IQLTreeFactoryRef ctxtRef,
                                  IQLExpressionRef arr,
				  IQLExpressionRef idx,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ArrayReferenceExpr::create(ctxt, unwrap(arr), unwrap(idx), 
					 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildArrayLValue(IQLTreeFactoryRef ctxtRef,
                                     IQLExpressionRef arr,
                                     IQLExpressionRef idx,
                                     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ArrayLValueExpr::create(ctxt, unwrap(arr), unwrap(idx), 
                                      SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildArray(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionListRef argsRef,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(ArrayExpr::create(ctxt, args, SourceLocation(line, column)));
}

IQLFieldTypeRef IQLBuildArrayType(IQLTreeFactoryRef ctxtRef, const FieldType * eltTy, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  if (boost::algorithm::iequals("infinity", sz)) {
    return wrap(VariableArrayType::Get(ctxt, eltTy, nullable!=0));
  } else {
    int32_t fieldSz = boost::lexical_cast<int32_t> (sz);
    return wrap(FixedArrayType::Get(ctxt, fieldSz, eltTy, nullable!=0));
  }
}

IQLExpressionRef IQLBuildRow(IQLTreeFactoryRef ctxtRef,
                             IQLExpressionListRef argsRef,
                             int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(StructExpr::create(ctxt, args, SourceLocation(line, column)));
}

IQLFieldTypeRef IQLBuildInt8Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int8Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildInt8ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, Int8Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildInt16Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int16Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildInt16ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, Int16Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildInt32Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildInt32ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, Int32Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildInt64Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int64Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildInt64ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, Int64Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildFloatType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(FloatType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildFloatArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, FloatType::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildDoubleType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DoubleType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDoubleArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, DoubleType::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildDecimalType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DecimalType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDecimalArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, DecimalType::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildDateType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DateType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDateArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, DateType::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildDatetimeType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DatetimeType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDatetimeArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, DatetimeType::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildIPv4Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IPv4Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildIPv4ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, IPv4Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildCIDRv4Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(CIDRv4Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildCIDRv4ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, CIDRv4Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildIPv6Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IPv6Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildIPv6ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, IPv6Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildCIDRv6Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(CIDRv6Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildCIDRv6ArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, CIDRv6Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildNVarcharType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  throw std::runtime_error("NVARCHAR not yet implemented");
}
IQLFieldTypeRef IQLBuildVarcharType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(VarcharType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildVarcharArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, VarcharType::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildCharType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  int32_t fieldSz = boost::lexical_cast<int32_t> (sz);
  return wrap(CharType::Get(ctxt, fieldSz, nullable!=0));  
}
IQLFieldTypeRef IQLBuildBooleanType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildBooleanArrayType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return IQLBuildArrayType(ctxtRef, Int32Type::Get(ctxt, false), sz, nullable);
}
IQLFieldTypeRef IQLBuildNilType(IQLTreeFactoryRef ctxtRef)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NilType::Get(ctxt));  
}
IQLFieldTypeRef IQLBuildType(IQLTreeFactoryRef ctxtRef, const char * typeName, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  if (boost::algorithm::iequals("date", typeName)) {
    return wrap(DateType::Get(ctxt, nullable));
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }
}
IQLFieldTypeRef IQLBuildArrayType(IQLTreeFactoryRef ctxtRef, const char * typeName, const char * sz, int nullable)
{
  return IQLBuildArrayType(ctxtRef, unwrap(IQLBuildType(ctxtRef, typeName, false)), sz, nullable);
}

IQLStatementListRef IQLStatementListCreate(IQLTreeFactoryRef ctxt)
{
  return wrap(new std::vector<IQLStatement *> ());
}

void IQLStatementListFree(IQLTreeFactoryRef ctxt,
                          IQLStatementListRef l)
{
  return delete unwrap(l);
}

void IQLStatementListAppend(IQLTreeFactoryRef ctxt,
                            IQLStatementListRef l,
                            IQLStatementRef e)
{
  unwrap(l)->push_back(unwrap(e));
}

IQLStatementRef IQLBuildStatementBlock(IQLTreeFactoryRef ctxtRef,
                                       IQLStatementListRef stmts,
                                       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  auto s = unwrap(stmts);
  return wrap(StatementBlock::create(ctxt, s->begin(), s->end(), SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildBreak(IQLTreeFactoryRef ctxtRef, int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BreakStatement::create(ctxt, SourceLocation(line, column)));
}

IQLStatementRef IQLBuildContinue(IQLTreeFactoryRef ctxtRef, int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ContinueStatement::create(ctxt, SourceLocation(line, column)));
}

IQLStatementRef IQLBuildDeclare(IQLTreeFactoryRef ctxtRef, IQLExpressionRef val, const char * nm,
                                int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DeclareStatement::create(ctxt, unwrap(val), nm, SourceLocation(line, column)));
}

IQLStatementRef IQLBuildDeclareNoInit(IQLTreeFactoryRef ctxtRef,
                                      IQLFieldTypeRef ty,
                                      const char * nm,
                                      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DeclareStatement::create(ctxt, unwrap(ty), nm, SourceLocation(line, column)));
}

IQLStatementRef IQLBuildIfThenElse(IQLTreeFactoryRef ctxtRef,
                                   IQLExpressionRef cond,
                                   IQLStatementRef thenStmts,
                                   IQLStatementRef elseStmts,
                                   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IfThenElseStatement::create(ctxt, unwrap(cond), unwrap(thenStmts),
                                          unwrap(elseStmts), SourceLocation(line, column)));
}

IQLStatementRef IQLBuildReturn(IQLTreeFactoryRef ctxtRef, IQLExpressionRef val,
                               int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ReturnStatement::create(ctxt, unwrap(val), SourceLocation(line, column)));
}

IQLStatementRef IQLBuildAddFields(IQLTreeFactoryRef ctxtRef, 
					 const char * recordName,
                                 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(FieldGlobStatement::create(ctxt, recordName, SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildAddField(IQLTreeFactoryRef ctxtRef, 
					const char * fieldName,
					IQLExpressionRef expr,
                                 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(AddFieldStatement::create(ctxt, unwrap(expr), fieldName, SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildQuotedId(IQLTreeFactoryRef ctxtRef, 
					const char * pattern,
					const char * names,
                                 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(FieldPatternStatement::create(ctxt, pattern, names, SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildSet(IQLTreeFactoryRef ctxtRef,
                            IQLExpressionRef lhs,
                            IQLExpressionRef rhs,
                            int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(SetStatement::create(ctxt, unwrap(lhs), unwrap(rhs), SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildSwitch(IQLTreeFactoryRef ctxtRef,
                               IQLExpressionRef expr,
                               IQLStatementListRef cases,
                               int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  auto s = unwrap(cases);
  return wrap(SwitchStatement::create(ctxt, unwrap(expr), s->begin(), s->end(), SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildSwitchCase(IQLTreeFactoryRef ctxtRef,
                                   const char * expr,
                                   IQLStatementListRef stmts,
                                   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  auto s = unwrap(stmts);
  return wrap(SwitchCaseStatement::create(ctxt, expr, s->begin(), s->end(), SourceLocation(line, column)));  
}

IQLStatementRef IQLBuildWhile(IQLTreeFactoryRef ctxtRef,
                              IQLExpressionRef cond,
                              IQLStatementRef stmts,
                              int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(WhileStatement::create(ctxt, unwrap(cond), unwrap(stmts), SourceLocation(line, column)));  
}

void IQLGraphNodeStart(IQLGraphContextRef ctxt, const char * type, const char * name)
{
  unwrap(ctxt)->nodeStart(type, name);
}

void IQLGraphNodeBuildIntegerParam(IQLGraphContextRef ctxt, const char * name, const char * val)
{
  unwrap(ctxt)->nodeAddIntegerParam(name, val);
}

void IQLGraphNodeBuildStringParam(IQLGraphContextRef ctxt, const char * name, const char * val)
{
  unwrap(ctxt)->nodeAddStringParam(name, val);
}

void IQLGraphNodeComplete(IQLGraphContextRef ctxt)
{
  unwrap(ctxt)->nodeComplete();
}

void IQLGraphNodeBuildEdge(IQLGraphContextRef ctxt, const char * from, const char * to)
{
  unwrap(ctxt)->edgeBuild(from, to);
}

void IQLRecordTypeBuildField(IQLRecordTypeContextRef ctxt, 
			     const char * name, 
			     IQLFieldTypeRef ty)
{
  if (NULL == ty) {
    throw std::runtime_error("Invalid primitive type");
  }
  const FieldType * ft = unwrap(ty);
  unwrap(ctxt)->buildField(name, ft);
}

class IQLRecordTypeBuilder * unwrap(IQLRecordTypeContextRef val)
{
  return reinterpret_cast<IQLRecordTypeBuilder *>(val);
}

IQLRecordTypeContextRef wrap(class IQLRecordTypeBuilder * val)
{
  return reinterpret_cast<IQLRecordTypeContextRef>(val);
}

