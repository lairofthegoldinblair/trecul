#include <stdexcept>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

#include "IQLExpression.hh"
#include "IQLTypeCheckNative.hh"
#include "TypeCheckContext.hh"

namespace {

const FieldType * typeCheckExpr(IQLExpression * expr,
                                TypeCheckContext & ctxt)
{
  if (expr == NULL) {
    throw std::runtime_error("INTERNAL ERROR: null expression in native type check");
  }

  const auto getArgType = [&ctxt](IQLExpression * arg) {
    return typeCheckExpr(arg, ctxt);
  };

  const FieldType * result = NULL;
  switch (expr->getNodeType()) {
  case IQLExpression::LOR:
  case IQLExpression::LAND:
    result = ctxt.buildLogicalAnd(getArgType(*expr->begin_args()),
                                  getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::LNOT:
    result = ctxt.buildLogicalNot(getArgType(*expr->begin_args()));
    break;
  case IQLExpression::LISNULL:
    getArgType(*expr->begin_args());
    result = ctxt.buildBooleanType(false);
    break;
  case IQLExpression::CASE:
    {
      const std::size_t n = expr->args_size();
      if (n < 3 || (n % 2) == 0) {
        throw std::runtime_error("INTERNAL ERROR: invalid CASE expression shape");
      }

      ctxt.beginCase();
      for (std::size_t i = 0; i + 1 < n; i += 2) {
        const auto condTy = getArgType(*(expr->begin_args() + i));
        const auto valTy = getArgType(*(expr->begin_args() + i + 1));
        ctxt.addCondition(condTy);
        ctxt.addValue(valTy);
      }
      ctxt.addValue(getArgType(*(expr->begin_args() + n - 1)));
      result = ctxt.buildCase();
    }
    break;
  case IQLExpression::BAND:
  case IQLExpression::BOR:
  case IQLExpression::BXOR:
    result = ctxt.buildBitwise(getArgType(*expr->begin_args()),
                               getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::BNOT:
    result = ctxt.buildBitwise(getArgType(*expr->begin_args()));
    break;
  case IQLExpression::EQ:
  case IQLExpression::NEQ:
    result = ctxt.buildEquals(getArgType(*expr->begin_args()),
                              getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::GTN:
  case IQLExpression::LTN:
  case IQLExpression::GTEQ:
  case IQLExpression::LTEQ:
    result = ctxt.buildCompare(getArgType(*expr->begin_args()),
                               getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::SUBNET_CONTAINS:
  case IQLExpression::SUBNET_CONTAINSEQ:
  case IQLExpression::SUBNET_CONTAINED:
  case IQLExpression::SUBNET_CONTAINEDEQ:
  case IQLExpression::SUBNET_SYMCONTAINSEQ:
    result = ctxt.buildNetworkSubnet(getArgType(*expr->begin_args()),
                                     getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::MINUS:
    if (expr->args_size() == 1) {
      result = ctxt.buildNegate(getArgType(*expr->begin_args()));
    } else {
      result = ctxt.buildSub(getArgType(*expr->begin_args()),
                             getArgType(*(expr->begin_args() + 1)));
    }
    break;
  case IQLExpression::PLUS:
    if (expr->args_size() == 1) {
      result = ctxt.buildNegate(getArgType(*expr->begin_args()));
    } else {
      result = ctxt.buildAdd(getArgType(*expr->begin_args()),
                             getArgType(*(expr->begin_args() + 1)));
    }
    break;
  case IQLExpression::TIMES:
    result = ctxt.buildMul(getArgType(*expr->begin_args()),
                           getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::DIVIDE:
    result = ctxt.buildDiv(getArgType(*expr->begin_args()),
                           getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::MOD:
    result = ctxt.buildModulus(getArgType(*expr->begin_args()),
                               getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::CONCAT:
    result = ctxt.buildConcat(getArgType(*expr->begin_args()),
                              getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::CAST:
    result = ctxt.buildCast(getArgType(*(expr->begin_args() + 1)),
                            getArgType(*expr->begin_args()));
    break;
  case IQLExpression::VARIABLE:
    result = ctxt.buildVariableRef(expr->getStringData().c_str());
    break;
  case IQLExpression::VARIABLELVALUE:
    result = ctxt.lookupType(expr->getStringData().c_str());
    break;
  case IQLExpression::STRUCTMEMBERLVALUE:
  case IQLExpression::STRUCTMEMBERREF:
    result = ctxt.buildStructRef(getArgType(*expr->begin_args()),
                                 expr->getStringData().c_str());
    break;
  case IQLExpression::CALL:
    {
      static const std::set<std::string> aggregateFunctions{ "ARRAY_CONCAT", "MAX", "MIN", "SUM" };
      const std::string & fun = expr->getStringData();
      if (0 == aggregateFunctions.count(fun)) {
        std::vector<const FieldType *> args;
        for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
          args.push_back(getArgType(*it));
        }
        if (fun == "#") {
          result = ctxt.buildHash(args);
        } else if (fun == "$") {
          result = ctxt.buildSortPrefix(args);
        } else if (fun == "rlike") {
          result = ctxt.buildLike(args[0], args[1]);
        } else if (fun == "ifnull") {
          result = ctxt.buildIsNull(args);
        } else {
          result = ctxt.buildCall(fun.c_str(), args);
        }
      } else {
        ctxt.beginAggregateFunction();
        result = ctxt.buildAggregateFunction(getArgType(*expr->begin_args()), fun.c_str());
      }
    }
    break;
  case IQLExpression::INT32:
    result = ctxt.buildInt32Type(false);
    break;
  case IQLExpression::INT64:
    result = ctxt.buildInt64Type(false);
    break;
  case IQLExpression::DOUBLE:
    result = ctxt.buildDoubleType(false);
    break;
  case IQLExpression::DECIMAL:
    result = ctxt.buildDecimalType(false);
    break;
  case IQLExpression::STRING:
    result = ctxt.buildVarcharType(false);
    break;
  case IQLExpression::BOOLEAN:
    result = ctxt.buildBooleanType(false);
    break;
  case IQLExpression::INTERVAL:
    result = ctxt.buildInterval(expr->getStringData().c_str(),
                                getArgType(*expr->begin_args()));
    break;
  case IQLExpression::NIL:
    result = ctxt.buildNilType();
    break;
  case IQLExpression::IPV4:
    result = ctxt.buildIPv4Type(expr->getStringData().c_str(), false);
    break;
  case IQLExpression::IPV6:
    result = ctxt.buildIPv6Type(expr->getStringData().c_str(), false);
    break;
  case IQLExpression::ARRAYREF:
  case IQLExpression::ARRAYLVALUE:
    result = ctxt.buildArrayRef(getArgType(*expr->begin_args()),
                                getArgType(*(expr->begin_args() + 1)));
    break;
  case IQLExpression::ARR:
    {
      std::vector<const FieldType *> args;
      for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
        args.push_back(getArgType(*it));
      }
      result = ctxt.buildArray(args);
    }
    break;
  case IQLExpression::STRUCT:
    {
      std::vector<const FieldType *> args;
      for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
        args.push_back(getArgType(*it));
      }
      result = ctxt.buildStruct(args);
    }
    break;
  case IQLExpression::TYPE:
    {
      result = expr->getFieldType();
    }
    break;
  case IQLExpression::DECLTYPE:
    {
      result = getArgType(*expr->begin_args());
      if (!expr->getStringData().empty()) {
        if (boost::algorithm::iequals("infinity", expr->getStringData())) {
          result = VariableArrayType::Get(result->getContext(), result, false);
        } else {
          int32_t fieldSz = boost::lexical_cast<int32_t> (expr->getStringData());
          result = FixedArrayType::Get(result->getContext(), fieldSz, result, false);
        }
      }
    }
    break;
  default:
    throw std::runtime_error("INTERNAL ERROR: unsupported node in native type check");
  }

  expr->setFieldType(result);
  expr->setCoerceTo(result);
  return result;
}

void typeCheckStmt(IQLStatement * stmt,
                   TypeCheckContext & ctxt)
{
  if (stmt == NULL) {
    throw std::runtime_error("INTERNAL ERROR: null statement in native type check");
  }

  const auto typeCheck = [&ctxt](IQLStatement * child) {
    return typeCheckStmt(child, ctxt);
  };

  const auto getArgType = [&ctxt](IQLStatement * arg) {
    if (!arg->isExpressionNode()) {
      throw std::runtime_error("INTERNAL ERROR: expression expected in native type check");
    }
    return typeCheckExpr(static_cast<IQLExpression *>(arg), ctxt);
  };

  switch (stmt->getNodeType()) {
  case IQLStatement::BLOCK:
    for(auto it = stmt->begin_children(), e = stmt->end_children(); it != e; ++it) {
      typeCheck(*it);
    }
    break;
  case IQLStatement::BREAK:
  case IQLStatement::CONTINUE:
    // TODO: Check if we are in a SWITCH or WHILE?
    break;
  case IQLStatement::DECLARE:
    if (stmt->children_size() > 0) {
      ctxt.buildLocal(stmt->getStringData().c_str(), getArgType(*stmt->begin_children()));
    } else {
      ctxt.buildLocal(stmt->getStringData().c_str(), static_cast<DeclareStatement*>(stmt)->getDeclaredType());
    }
    break;
  case IQLStatement::RETURN:
    getArgType(*stmt->begin_children());
    break;
  case IQLStatement::ADDFIELD:
    {
      ctxt.addField(stmt->getStringData().c_str(), getArgType(*stmt->begin_children()));
      break;
    }
  case IQLStatement::FIELDPATTERN:
    // TODO: FIXME
    ctxt.quotedId(stmt->getStringData().c_str(), nullptr);
    break;
  case IQLStatement::FIELDGLOB:
    ctxt.addFields(stmt->getStringData().c_str());
    break;
  case IQLStatement::IFTHENELSE:
    {
      auto cond = getArgType(*stmt->begin_children());
      if (cond != ctxt.buildBooleanType()) {
        throw std::runtime_error("Expected boolean expression");
      }
      typeCheck(*(stmt->begin_children() + 1));
      if (stmt->children_size() > 2) {
        typeCheck(*(stmt->begin_children() + 2));
      }
    }
    break;
  case IQLStatement::SET:
    {
      ctxt.buildSetValue(getArgType(*stmt->begin_children()), getArgType(*(stmt->begin_children() + 1)));
    }
    break;
  case IQLStatement::SWITCH:
    {
      ctxt.beginSwitch(getArgType(*stmt->begin_children()));
      if (stmt->children_size() > 1) {
        for(auto it = stmt->begin_children()+1, e = stmt->end_children(); it != e; ++it) {
          typeCheck(*it);
        }
      }
    }
    break;
  case IQLStatement::SWITCHCASE:
    {
      for(auto it = stmt->begin_children(), e = stmt->end_children(); it != e; ++it) {
        typeCheck(*it);
      }
    }
    break;
  case IQLStatement::WHILE:
    {
      auto cond = getArgType(*stmt->begin_children());
      if (cond != ctxt.buildBooleanType()) {
        throw std::runtime_error("Expected boolean expression");
      }
      typeCheck(*(stmt->begin_children() + 1));
    }
    break;
  default:
    throw std::runtime_error("INTERNAL ERROR: unsupported node in native type check");
  }
}
  
} // namespace

const FieldType * IQLTypeCheckExpressionNative(IQLExpression * expr,
                                               TypeCheckContext & ctxt)
{
  return typeCheckExpr(expr, ctxt);
}

void IQLTypeCheckStatementListNative(const std::vector<IQLStatement *> & stmts,
                                     TypeCheckContext & ctxt)
{
  for(IQLStatement * stmt : stmts) {
    typeCheckStmt(stmt, ctxt);
  }
}

const FieldType * IQLTypeCheckFunctionNative(const std::vector<IQLStatement *> & stmts,
                                             TypeCheckContext & ctxt)
{
  IQLTypeCheckStatementListNative(stmts, ctxt);
  if (IQLStatement::RETURN != stmts.back()->getNodeType()) {
    throw std::runtime_error("INTERNAL ERROR: last statement in function must be RETURN in native type check");    
  }
  return static_cast<IQLExpression *>(*stmts.back()->begin_children())->getFieldType();
}

const RecordType * IQLTypeCheckTransferNative(const std::vector<IQLStatement *> & stmts,
                                              TypeCheckContext & ctxt)
{
  ctxt.beginRecord();
  IQLTypeCheckStatementListNative(stmts, ctxt);
  ctxt.buildRecord();
  // There should be a present for us now...
  if (ctxt.getOutputRecord() == NULL)
    throw std::runtime_error("Failed to create output record");

  return ctxt.getOutputRecord();
}
