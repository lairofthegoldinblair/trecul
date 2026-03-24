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

#include <stdexcept>
#include "GetVariablesPass.hh"
#include "IQLExpression.hh"

GetVariablesContext::GetVariablesContext()
{
}

GetVariablesContext::~GetVariablesContext()
{
}

const std::set<std::string>& GetVariablesContext::getFreeVariables() const
{
  return mFreeVariables;
}
const std::set<std::string>& GetVariablesContext::getLocalVariables() const
{
  return mLocalVariables;
}

void GetVariablesContext::addField(const char * name)
{
  buildLocal(name);
}

void GetVariablesContext::buildLocal(const char * name)
{
  if (mLocalVariables.find(name) == mLocalVariables.end())
    mLocalVariables.insert(name);
}

void GetVariablesContext::buildVariableReference(const char * name)
{
  if (mLocalVariables.find(name) == mLocalVariables.end() &&
      mFreeVariables.find(name) == mFreeVariables.end())
    mFreeVariables.insert(name);
}

void GetVariablesContext::buildArrayReference(const char * name)
{
  buildVariableReference(name);
}

void GetVariablesContext::beginAggregateFunction()
{
  throw std::runtime_error("Not yet implemented");
}

void GetVariablesContext::buildAggregateFunction()
{
  throw std::runtime_error("Not yet implemented");
}

namespace {

void getVariablesExpr(IQLExpression * expr,
                      GetVariablesContext & ctxt)
{
  if (expr == NULL) {
    throw std::runtime_error("INTERNAL ERROR: null expression in native type check");
  }

  const auto getVariables = [&ctxt](IQLExpression * arg) {
    getVariablesExpr(arg, ctxt);
  };

  const FieldType * result = NULL;
  switch (expr->getNodeType()) {
  case IQLExpression::LOR:
  case IQLExpression::LAND:
    getVariables(*expr->begin_args());
    getVariables(*(expr->begin_args() + 1));
    break;
  case IQLExpression::LNOT:
    getVariables(*expr->begin_args());
    break;
  case IQLExpression::LISNULL:
    getVariables(*expr->begin_args());
    break;
  case IQLExpression::CASE:
    {
      const std::size_t n = expr->args_size();
      if (n < 3 || (n % 2) == 0) {
        throw std::runtime_error("INTERNAL ERROR: invalid CASE expression shape");
      }

      for (std::size_t i = 0; i + 1 < n; i += 2) {
        getVariables(*(expr->begin_args() + i));
        getVariables(*(expr->begin_args() + i + 1));
      }
      getVariables(*(expr->begin_args() + n - 1));
    }
    break;
  case IQLExpression::BAND:
  case IQLExpression::BOR:
  case IQLExpression::BXOR:
    getVariables(*expr->begin_args());
    getVariables(*(expr->begin_args() + 1));
    break;
  case IQLExpression::BNOT:
    getVariables(*expr->begin_args());
    break;
  case IQLExpression::EQ:
  case IQLExpression::NEQ:
  case IQLExpression::GTN:
  case IQLExpression::LTN:
  case IQLExpression::GTEQ:
  case IQLExpression::LTEQ:
  case IQLExpression::SUBNET_CONTAINS:
  case IQLExpression::SUBNET_CONTAINSEQ:
  case IQLExpression::SUBNET_CONTAINED:
  case IQLExpression::SUBNET_CONTAINEDEQ:
  case IQLExpression::SUBNET_SYMCONTAINSEQ:
    getVariables(*expr->begin_args());
    getVariables(*(expr->begin_args() + 1));
    break;
  case IQLExpression::MINUS:
  case IQLExpression::PLUS:
    if (expr->args_size() == 1) {
      getVariables(*expr->begin_args());
    } else {
      getVariables(*expr->begin_args());
      getVariables(*(expr->begin_args() + 1));
    }
    break;
  case IQLExpression::TIMES:
  case IQLExpression::DIVIDE:
  case IQLExpression::MOD:
  case IQLExpression::CONCAT:
  case IQLExpression::CAST:
    getVariables(*expr->begin_args());
    getVariables(*(expr->begin_args() + 1));
    break;
  case IQLExpression::VARIABLE:
  case IQLExpression::VARIABLELVALUE:
    {
      auto var = expr->getStringDataIf();
      if (nullptr != var) {
        ctxt.buildVariableReference(var->c_str());
      }
    }
    break;
  case IQLExpression::STRUCTMEMBERLVALUE:
  case IQLExpression::STRUCTMEMBERREF:
    getVariables(*expr->begin_args());
    break;
  case IQLExpression::CALL:
    {
      static const std::set<std::string> aggregateFunctions{ "ARRAY_CONCAT", "MAX", "MIN", "SUM" };
      const std::string & fun = expr->getStringData();
      if (0 == aggregateFunctions.count(fun)) {
        for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
          getVariables(*it);
        }
      } else {
        ctxt.beginAggregateFunction();
        getVariables(*expr->begin_args());
        ctxt.buildAggregateFunction();
      }
    }
    break;
  case IQLExpression::INT32:
  case IQLExpression::INT64:
  case IQLExpression::DOUBLE:
  case IQLExpression::DECIMAL:
  case IQLExpression::STRING:
  case IQLExpression::BOOLEAN:
  case IQLExpression::INTERVAL:
  case IQLExpression::NIL:
  case IQLExpression::IPV4:
  case IQLExpression::IPV6:
    break;
  case IQLExpression::ARRAYREF:
  case IQLExpression::ARRAYLVALUE:
    getVariables(*expr->begin_args());
    getVariables(*(expr->begin_args() + 1));
    break;
  case IQLExpression::ARR:
  case IQLExpression::STRUCT:
    {
      for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
        getVariables(*it);
      }
    }
    break;
  case IQLExpression::TYPE:
    break;
  case IQLExpression::DECLTYPE:
    getVariables(*expr->begin_args());
    break;
  default:
    throw std::runtime_error("INTERNAL ERROR: unsupported node in native type check");
  }
}

void getVariablesStmt(IQLStatement * stmt,
                   GetVariablesContext & ctxt)
{
  if (stmt == NULL) {
    throw std::runtime_error("INTERNAL ERROR: null statement in native type check");
  }

  const auto getStatementVariables = [&ctxt](IQLStatement * child) {
    return getVariablesStmt(child, ctxt);
  };

  const auto getExpressionVariables = [&ctxt](IQLStatement * arg) {
    if (!arg->isExpressionNode()) {
      throw std::runtime_error("INTERNAL ERROR: expression expected in native type check");
    }
    return getVariablesExpr(static_cast<IQLExpression *>(arg), ctxt);
  };

  switch (stmt->getNodeType()) {
  case IQLStatement::BLOCK:
    for(auto it = stmt->begin_children(), e = stmt->end_children(); it != e; ++it) {
      getStatementVariables(*it);
    }
    break;
  case IQLStatement::BREAK:
  case IQLStatement::CONTINUE:
    // TODO: Check if we are in a SWITCH or WHILE?
    break;
  case IQLStatement::DECLARE:
    ctxt.buildLocal(stmt->getStringData().c_str());
    if (stmt->children_size() > 0) {
       getExpressionVariables(*stmt->begin_children());
    }
    break;
  case IQLStatement::RETURN:
    getExpressionVariables(*stmt->begin_children());
    break;
  case IQLStatement::ADDFIELD:
    {      
      getExpressionVariables(*stmt->begin_children());
      auto var = stmt->getStringDataIf();
      if (nullptr != var) {
        ctxt.addField(var->c_str());
      }
      break;
    }
  case IQLStatement::FIELDPATTERN:
  case IQLStatement::FIELDGLOB:
    break;
  case IQLStatement::IFTHENELSE:
    {
      getExpressionVariables(*stmt->begin_children());
      getStatementVariables(*(stmt->begin_children() + 1));
      if (stmt->children_size() > 2) {
        getStatementVariables(*(stmt->begin_children() + 2));
      }
    }
    break;
  case IQLStatement::SET:
    {
      getExpressionVariables(*stmt->begin_children());
      getExpressionVariables(*(stmt->begin_children() + 1));
    }
    break;
  case IQLStatement::SWITCH:
    {
      getExpressionVariables(*stmt->begin_children());
      if (stmt->children_size() > 1) {
        for(auto it = stmt->begin_children()+1, e = stmt->end_children(); it != e; ++it) {
          getStatementVariables(*it);
        }
      }
    }
    break;
  case IQLStatement::SWITCHCASE:
    {
      for(auto it = stmt->begin_children(), e = stmt->end_children(); it != e; ++it) {
        getStatementVariables(*it);
      }
    }
    break;
  case IQLStatement::WHILE:
    {
      getExpressionVariables(*stmt->begin_children());
      getStatementVariables(*(stmt->begin_children() + 1));
    }
    break;
  default:
    throw std::runtime_error("INTERNAL ERROR: unsupported node in native type check");
  }
}
  
} // namespace

void IQLGetVariablesNative(const std::vector<IQLStatement *> & stmts,
                           GetVariablesContext & ctxt)
{
  for(IQLStatement * stmt : stmts) {
    getVariablesStmt(stmt, ctxt);
  }
}
