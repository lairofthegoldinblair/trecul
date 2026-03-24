/**
 * Copyright (c) 2026, Akamai Technologies
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

#ifndef __IQLSTATEMENT_HH
#define __IQLSTATEMENT_HH

#include <stdint.h>
#include <variant>
#include <vector>
#include "RecordType.hh"

class IQLExpression;

class SourceLocation
{
private:
  int32_t mLine;
  int32_t mColumn;
public:
  SourceLocation()
    :
    mLine(0),
    mColumn(0)
  {
  }
  SourceLocation(int32_t line, int32_t column)
    :
    mLine(line),
    mColumn(column)
  {
  }

  int32_t getLine() const 
  {
    return mLine; 
  }

  int32_t getColumn() const
  {
    return mColumn;
  }
};

// TODO: Right now we are treating DynamicRecordContext
// as an owner of IQLStatement.  We probably want something
// finer grain than that.
// TODO: Support constant folding of IQLStatements
// Implement type checking and LLVM code gen off of IQLStatements
class IQLStatement
{
public:
  enum NodeType { BLOCK, BREAK, CONTINUE, DECLARE, ADDFIELD, FIELDPATTERN, FIELDGLOB, IFTHENELSE, RETURN, SET, SWITCH, SWITCHCASE, WHILE, ARRAYREF, ARRAYLVALUE, ARR, LOR, LAND, LNOT, LISNULL, CASE, BAND, BOR, BXOR, BNOT, EQ, GTN, LTN, GTEQ, LTEQ, NEQ, MINUS, PLUS, TIMES, DIVIDE, MOD, CONCAT, CAST, VARIABLE, VARIABLELVALUE, CALL, INT32, INT64, DOUBLE, DECIMAL, STRING, STRUCT, STRUCTMEMBERREF, STRUCTMEMBERLVALUE, BOOLEAN, INTERVAL, NIL, IPV4, IPV6, SUBNET_CONTAINS, SUBNET_CONTAINSEQ, SUBNET_CONTAINED, SUBNET_CONTAINEDEQ, SUBNET_SYMCONTAINSEQ, TYPE, DECLTYPE };

protected:
  DynamicRecordContext & mContext;

  NodeType mNodeType;

  // Line and column number
  SourceLocation mSourceLocation;

  typedef std::variant<int64_t, std::string, double, const FieldType *> data_type;
  data_type mData;

  std::vector<IQLStatement*> mArgs;
  
  IQLStatement(DynamicRecordContext & ctxt,
               NodeType nodeType, 
               const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mSourceLocation(loc)
  {
  }
  IQLStatement(DynamicRecordContext & ctxt,
               NodeType nodeType, 
               IQLStatement * arg, 
               const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mSourceLocation(loc)
  {
    BOOST_ASSERT(arg != NULL);
    mArgs.push_back(arg);
  }
  IQLStatement(DynamicRecordContext & ctxt,
               NodeType nodeType, IQLStatement * arg1,
               IQLStatement * arg2, 
               const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mSourceLocation(loc)
  {
    BOOST_ASSERT(arg1 != NULL && arg2 != NULL);
    mArgs.push_back(arg1);
    mArgs.push_back(arg2);
  }
  // template<typename _Iterator>
  // IQLStatement(DynamicRecordContext & ctxt,
  //              NodeType nodeType, _Iterator begin,
  //              _Iterator end, 
  //              const SourceLocation& loc)
  //   :
  //   mContext(ctxt),
  //   mNodeType(nodeType),
  //   mSourceLocation(loc),
  //   mArgs(begin, end)
  // {
  // }
  IQLStatement(const IQLStatement & rhs)
    :
    mContext(rhs.mContext),
    mNodeType(rhs.mNodeType),
    mSourceLocation(rhs.mSourceLocation),
    mArgs(rhs.mArgs),
    mData(rhs.mData)
  {
  }

  void setData(const char * s) 
  {
    mData = data_type(s);
  }
  void setData(int64_t v) 
  {
    mData = data_type(v);
  }
  void setData(int32_t v) 
  {
    mData = data_type((int64_t) v);
  }
  void setData(double v) 
  {
    mData = data_type(v);
  }
  void setData(const FieldType * ty) 
  {
    mData = data_type(ty);
  }
  const FieldType * getTypeData() const
  {
    return std::get<const FieldType *>(mData) ;
  }
public:
  virtual ~IQLStatement();
  virtual IQLStatement * clone() const =0;
  // TODO: Move into a binary op sub class
  void rotateLeftChild()
  {
    IQLStatement * left = mArgs[0];
    mArgs[0] = left->mArgs[0];
    left->mArgs[0] = left->mArgs[1];
    left->mArgs[1] = mArgs[1];
    mArgs[1] = left;
  }
  void rotateRightChild()
  {
    IQLStatement * right = mArgs[1];
    mArgs[1] = right->mArgs[1];
    right->mArgs[1] = right->mArgs[0];
    right->mArgs[0] = mArgs[0];
    mArgs[0] = right;
  }
  bool shallow_equals(const IQLStatement * rhs) const
  {
    return getNodeType() == rhs->getNodeType() &&
      mData == rhs->mData;
  }
  
  NodeType getNodeType() const
  {
    return mNodeType;
  }
  const SourceLocation& getSourceLocation() const
  {
    return mSourceLocation;
  }
  int32_t getLine() const
  {
    return mSourceLocation.getLine();
  }
  int32_t getColumn() const
  {
    return mSourceLocation.getColumn();
  }

  void replaceArg(IQLStatement * oldArg,
		  IQLStatement * newArg);
  const std::string& getStringData() const
  {
    return std::get<std::string>(mData);
  }
  const std::string * getStringDataIf() const
  {
    return std::get_if<std::string>(&mData);
  }
  bool getBooleanData() const
  {
    return std::get<int64_t>(mData) != 0;
  }
  // Children interface
  std::size_t children_size() const
  {
    return mArgs.size();
  }
  typedef std::vector<IQLStatement*>::const_iterator child_const_iterator;
  child_const_iterator begin_children() const
  {
    return mArgs.begin();
  }
  child_const_iterator end_children() const 
  {
    return mArgs.end();
  }

  typedef std::vector<IQLStatement*>::iterator child_iterator;
  child_iterator begin_children() 
  {
    return mArgs.begin();
  }
  child_iterator end_children() 
  {
    return mArgs.end();
  }
  bool isExpressionNode() const
  {
    return getNodeType() >= ARRAYREF;
  }
};

class StatementBlock : public IQLStatement
{
private:

  StatementBlock(const StatementBlock & rhs)
    :
    IQLStatement(rhs)
  {
  }
  template <typename _Iterator>
  StatementBlock(DynamicRecordContext & ctxt,
                  _Iterator stmtsBegin, _Iterator stmtsEnd,
                  const SourceLocation& loc)
    :
    IQLStatement(ctxt, IQLStatement::BLOCK, loc)
  {
    mArgs.insert(mArgs.end(), stmtsBegin, stmtsEnd);
  }


public:
  template <typename _Iterator>
  static StatementBlock * create(DynamicRecordContext & ctxt,
                                  _Iterator stmtsBegin, _Iterator stmtsEnd,
                                  const SourceLocation& loc)
  {
    auto tmp = new StatementBlock(ctxt, stmtsBegin, stmtsEnd, loc);
    ctxt.add(tmp);
    return tmp;
  }
  StatementBlock * clone() const
  {
    return new StatementBlock(*this);
  }
};

class BreakStatement : public IQLStatement
{
private:
  BreakStatement(const BreakStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
public:

  BreakStatement(DynamicRecordContext & ctxt, 
                   const SourceLocation& loc);

  static BreakStatement * create(DynamicRecordContext & ctxt,
                                   const SourceLocation& loc);

  BreakStatement * clone() const;
};

class ContinueStatement : public IQLStatement
{
private:
  ContinueStatement(const ContinueStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
public:

  ContinueStatement(DynamicRecordContext & ctxt, 
                   const SourceLocation& loc);

  static ContinueStatement * create(DynamicRecordContext & ctxt,
                                   const SourceLocation& loc);

  ContinueStatement * clone() const;
};

class DeclareStatement : public IQLStatement
{
private:
  const FieldType * mDeclaredType;
  DeclareStatement(const DeclareStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
public:

  DeclareStatement(DynamicRecordContext & ctxt, 
                   IQLExpression * val,
                   const char * nm,
                   const SourceLocation& loc);

  DeclareStatement(DynamicRecordContext & ctxt, 
                   const FieldType * ty,
                   const char * nm,
                   const SourceLocation& loc);

  static DeclareStatement * create(DynamicRecordContext & ctxt,
                                   IQLExpression * val,
                                   const char * nm,
                                   const SourceLocation& loc);

  static DeclareStatement * create(DynamicRecordContext & ctxt,
                                   const FieldType * val,
                                   const char * nm,
                                   const SourceLocation& loc);

  DeclareStatement * clone() const;

  const FieldType * getDeclaredType() const
  {
    return mDeclaredType;
  }
};

class ReturnStatement : public IQLStatement
{
private:
  ReturnStatement(const ReturnStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
public:

  ReturnStatement(DynamicRecordContext & ctxt, 
                  IQLExpression * val,
                  const SourceLocation& loc);

  static ReturnStatement * create(DynamicRecordContext & ctxt,
                                  IQLExpression * val,
                                  const SourceLocation& loc);

  ReturnStatement * clone() const;
};

/**
 * Match and output all fields from a named input record.
 */
class FieldGlobStatement : public IQLStatement
{
private:

  FieldGlobStatement(const ReturnStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  FieldGlobStatement(DynamicRecordContext & ctxt,
                     const char * recordName,
                     const SourceLocation& loc)
    :
    IQLStatement(ctxt, IQLStatement::FIELDGLOB, loc)
  {
    setData(recordName);
  }

public:
  static FieldGlobStatement * create(DynamicRecordContext & ctxt,
                                     const char * recordName,
                                     const SourceLocation& loc)
  {
    auto tmp = new FieldGlobStatement(ctxt, recordName, loc);
    ctxt.add(tmp);
    return tmp;
  }
  FieldGlobStatement * clone() const
  {
    return new FieldGlobStatement(*this);
  }
};

/**
 * A single expression with an optional name.  The name is required
 * unless the expression is a VARIABLE reference.
 */
class AddFieldStatement : public IQLStatement
{
private:
  AddFieldStatement(const AddFieldStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
public:

  AddFieldStatement(DynamicRecordContext & ctxt, 
                   IQLExpression * val,
                   const char * nm,
                   const SourceLocation& loc);

  static AddFieldStatement * create(DynamicRecordContext & ctxt,
                                   IQLExpression * val,
                                   const char * nm,
                                   const SourceLocation& loc);

  AddFieldStatement * clone() const;
  IQLExpression * getExpression();
  const std::string& getName() const;
};

/**
 * A regular expression that matches variables by name
 * and optionally use captures to rename the variables.
 */
class FieldPatternStatement : public IQLStatement
{
private:

  FieldPatternStatement(const ReturnStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  FieldPatternStatement(DynamicRecordContext & ctxt,
                        const char * nm,
                        const char * nm2,
                        const SourceLocation& loc);

public:
  static FieldPatternStatement * create(DynamicRecordContext & ctxt,
                                       const char * nm,
                                       const char * nm2,
                                     const SourceLocation& loc)
  {
    auto tmp = new FieldPatternStatement(ctxt, nm, nm2, loc);
    ctxt.add(tmp);
    return tmp;
  }
  FieldPatternStatement * clone() const
  {
    return new FieldPatternStatement(*this);
  }
};

class IfThenElseStatement : public IQLStatement
{
private:

  IfThenElseStatement(const ReturnStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  IfThenElseStatement(DynamicRecordContext & ctxt,
                      IQLExpression * cond,
                      IQLStatement * thenStmts,
                      IQLStatement * elseStmts,
                      const SourceLocation& loc);

public:
  static IfThenElseStatement * create(DynamicRecordContext & ctxt,
                                      IQLExpression * cond,
                                      IQLStatement * thenStmts,
                                      IQLStatement * elseStmts,
                                      const SourceLocation& loc)
  {
    auto tmp = new IfThenElseStatement(ctxt, cond, thenStmts, elseStmts, loc);
    ctxt.add(tmp);
    return tmp;
  }
  IfThenElseStatement * clone() const
  {
    return new IfThenElseStatement(*this);
  }
};

class SetStatement : public IQLStatement
{
private:

  SetStatement(const ReturnStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  SetStatement(DynamicRecordContext & ctxt,
               IQLExpression * lhs,
               IQLExpression * rhs,
               const SourceLocation& loc);

public:
  static SetStatement * create(DynamicRecordContext & ctxt,
                               IQLExpression * lhs,
                               IQLExpression * rhs,
                               const SourceLocation& loc)
  {
    auto tmp = new SetStatement(ctxt, lhs, rhs, loc);
    ctxt.add(tmp);
    return tmp;
  }
  SetStatement * clone() const
  {
    return new SetStatement(*this);
  }
};

class SwitchStatement : public IQLStatement
{
private:

  SwitchStatement(const SwitchStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  template <typename _Iterator>
  SwitchStatement(DynamicRecordContext & ctxt,
                  IQLExpression * expr,
                  _Iterator casesBegin, _Iterator casesEnd,
                  const SourceLocation& loc)
    :
    IQLStatement(ctxt, IQLStatement::SWITCH, expr, loc)
  {
    mArgs.insert(mArgs.end(), casesBegin, casesEnd);
  }


public:
  template <typename _Iterator>
  static SwitchStatement * create(DynamicRecordContext & ctxt,
                                  IQLExpression * expr,
                                  _Iterator casesBegin, _Iterator casesEnd,
                                  const SourceLocation& loc)
  {
    auto tmp = new SwitchStatement(ctxt, expr, casesBegin, casesEnd, loc);
    ctxt.add(tmp);
    return tmp;
  }
  SwitchStatement * clone() const
  {
    return new SwitchStatement(*this);
  }
};

class SwitchCaseStatement : public IQLStatement
{
private:

  SwitchCaseStatement(const SwitchCaseStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  template <typename _Iterator>
  SwitchCaseStatement(DynamicRecordContext & ctxt,
                      const char * expr,
                      _Iterator caseBegin, _Iterator caseEnd,
                      const SourceLocation& loc)
    :
    IQLStatement(ctxt, IQLStatement::SWITCHCASE, loc)
  {
    setData(expr);
    mArgs.insert(mArgs.end(), caseBegin, caseEnd);
  }


public:
  template <typename _Iterator>
  static SwitchCaseStatement * create(DynamicRecordContext & ctxt,
                                      const char * expr,
                                      _Iterator caseBegin, _Iterator caseEnd,
                                      const SourceLocation& loc)
  {
    auto tmp = new SwitchCaseStatement(ctxt, expr, caseBegin, caseEnd, loc);
    ctxt.add(tmp);
    return tmp;
  }
  SwitchCaseStatement * clone() const
  {
    return new SwitchCaseStatement(*this);
  }
};

class WhileStatement : public IQLStatement
{
private:

  WhileStatement(const WhileStatement & rhs)
    :
    IQLStatement(rhs)
  {
  }
  WhileStatement(DynamicRecordContext & ctxt,
                 IQLExpression * cond,
                 IQLStatement * stmts,
                 const SourceLocation& loc);

public:
  static WhileStatement * create(DynamicRecordContext & ctxt,
                                 IQLExpression * cond,
                                 IQLStatement * stmts,
                                 const SourceLocation& loc)
  {
    auto tmp = new WhileStatement(ctxt, cond, stmts, loc);
    ctxt.add(tmp);
    return tmp;
  }
  WhileStatement * clone() const
  {
    return new WhileStatement(*this);
  }
};

class IQLStatementPrinter
{
private:
  std::set<std::string> mBinaryInfix;
  std::set<std::string> mUnaryInfix;

  bool isBinaryInfix(IQLStatement * e);
  bool isUnaryPrefix(IQLStatement * e);
public:
  static const char * getExpressionSymbol(uint32_t nodeType);
  IQLStatementPrinter(std::ostream& ostr, IQLStatement * e);
};

#endif
