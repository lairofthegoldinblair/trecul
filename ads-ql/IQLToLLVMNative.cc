#include <stdexcept>
#include <vector>

#include "CodeGenerationContext.hh"
#include "IQLExpression.hh"
#include "IQLToLLVMNative.hh"

namespace {

struct NativeCodegenValue
{
  const IQLToLLVMValue * value;
  const FieldType * type;
};

NativeCodegenValue applyCoercion(const NativeCodegenValue & val,
                                 IQLExpression * expr,
                                 CodeGenerationContext & ctxt)
{
  const FieldType * coercedType = expr->getCoercedFieldType();
  if (coercedType != NULL && val.type != coercedType) {
    return NativeCodegenValue{ctxt.buildCast(val.value, val.type, coercedType), coercedType};
  }
  return val;
}

NativeCodegenValue codegenExpr(IQLExpression * expr,
                               CodeGenerationContext & ctxt)
{
  if (expr == NULL) {
    throw std::runtime_error("INTERNAL ERROR: null expression in native code generation");
  }

  const auto getArg = [&ctxt](IQLExpression * arg) {
    return codegenExpr(arg, ctxt);
  };

  const FieldType * resultType = expr->getFieldType();
  if (resultType == NULL) {
    throw std::runtime_error("INTERNAL ERROR: untyped expression in native code generation");
  }

  const IQLToLLVMValue * resultValue = NULL;
  switch (expr->getNodeType()) {
  case IQLExpression::LOR:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      ctxt.buildBeginOr(resultType);
      ctxt.buildAddOr(lhs.value, lhs.type, resultType);
      resultValue = ctxt.buildOr(rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::LAND:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      ctxt.buildBeginAnd(resultType);
      ctxt.buildAddAnd(lhs.value, lhs.type, resultType);
      resultValue = ctxt.buildAnd(rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::LNOT:
    {
      NativeCodegenValue arg = getArg(*expr->begin_args());
      resultValue = ctxt.buildNot(arg.value, arg.type, resultType);
    }
    break;
  case IQLExpression::LISNULL:
    {
      NativeCodegenValue arg = getArg(*expr->begin_args());
      resultValue = ctxt.buildIsNull(arg.value,
                                     arg.type,
                                     resultType,
                                     static_cast<IsNullExpr *>(expr)->isNot() ? 1 : 0);
    }
    break;
  case IQLExpression::CASE:
    {
      const std::size_t n = expr->args_size();
      if (n < 3 || (n % 2) == 0) {
        throw std::runtime_error("INTERNAL ERROR: invalid CASE expression shape");
      }

      ctxt.buildCaseBlockBegin(resultType);
      for (std::size_t i = 0; i + 1 < n; i += 2) {
        NativeCodegenValue cond = getArg(*(expr->begin_args() + i));
        NativeCodegenValue val = getArg(*(expr->begin_args() + i + 1));
        ctxt.buildCaseBlockIf(cond.value);
        ctxt.buildCaseBlockThen(val.value, val.type, resultType, false);
      }
      NativeCodegenValue elseVal = getArg(*(expr->begin_args() + n - 1));
      ctxt.buildCaseBlockThen(elseVal.value, elseVal.type, resultType, false);
      resultValue = ctxt.buildCaseBlockFinish(resultType);
    }
    break;
  case IQLExpression::BAND:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildBitwiseAnd(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::BOR:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildBitwiseOr(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::BXOR:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildBitwiseXor(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::BNOT:
    {
      NativeCodegenValue arg = getArg(*expr->begin_args());
      resultValue = ctxt.buildBitwiseNot(arg.value, arg.type, resultType);
    }
    break;
  case IQLExpression::EQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCompare(lhs.value, lhs.type, rhs.value, rhs.type, resultType, IQLToLLVMOpEQ);
    }
    break;
  case IQLExpression::NEQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCompare(lhs.value, lhs.type, rhs.value, rhs.type, resultType, IQLToLLVMOpNE);
    }
    break;
  case IQLExpression::GTN:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCompare(lhs.value, lhs.type, rhs.value, rhs.type, resultType, IQLToLLVMOpGT);
    }
    break;
  case IQLExpression::LTN:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCompare(lhs.value, lhs.type, rhs.value, rhs.type, resultType, IQLToLLVMOpLT);
    }
    break;
  case IQLExpression::GTEQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCompare(lhs.value, lhs.type, rhs.value, rhs.type, resultType, IQLToLLVMOpGE);
    }
    break;
  case IQLExpression::LTEQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCompare(lhs.value, lhs.type, rhs.value, rhs.type, resultType, IQLToLLVMOpLE);
    }
    break;
  case IQLExpression::SUBNET_CONTAINS:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildSubnetContains(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::SUBNET_CONTAINSEQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildSubnetContainsEquals(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::SUBNET_CONTAINED:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildSubnetContainedBy(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::SUBNET_CONTAINEDEQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildSubnetContainedByEquals(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::SUBNET_SYMCONTAINSEQ:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildSubnetSymmetricContainsEquals(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::MINUS:
    if (expr->args_size() == 1) {
      NativeCodegenValue arg = getArg(*expr->begin_args());
      resultValue = ctxt.buildNegate(arg.value, arg.type, resultType);
    } else {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildSub(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::PLUS:
    if (expr->args_size() == 1) {
      NativeCodegenValue arg = getArg(*expr->begin_args());
      resultValue = arg.value;
    } else {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildAdd(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::TIMES:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildMul(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::DIVIDE:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildDiv(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::MOD:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildMod(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::CONCAT:
    {
      NativeCodegenValue lhs = getArg(*expr->begin_args());
      NativeCodegenValue rhs = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildArrayConcat(lhs.value, lhs.type, rhs.value, rhs.type, resultType);
    }
    break;
  case IQLExpression::CAST:
    {
      NativeCodegenValue arg = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildCast(arg.value, arg.type, resultType);
    }
    break;
  case IQLExpression::VARIABLE:
    resultValue = ctxt.buildVariableRef(expr->getStringData().c_str(), resultType);
    break;
  case IQLExpression::VARIABLELVALUE:
    resultValue = ctxt.lookup(expr->getStringData().c_str());
    break;
  case IQLExpression::STRUCTMEMBERREF:
    {
      NativeCodegenValue s = getArg(*expr->begin_args());
      resultValue = ctxt.buildRowRef(s.value, s.type,
                                     expr->getStringData(),
                                     resultType);
    }
    break;
  case IQLExpression::STRUCTMEMBERLVALUE:
    {
      NativeCodegenValue row = getArg(*expr->begin_args());
      resultValue = ctxt.buildRowLValue(row.value, row.type, expr->getStringData().c_str(), resultType);
    }
    break;
  case IQLExpression::CALL:
    {
      static const std::set<std::string> aggregateFunctions{ "ARRAY_CONCAT", "MAX", "MIN", "SUM" };
      const std::string & fun = expr->getStringData();
      if (0 == aggregateFunctions.count(fun)) {
        std::vector<IQLToLLVMTypedValue> args;
        for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
          NativeCodegenValue v = getArg(*it);
          args.emplace_back(v.value, v.type);
        }
        if (fun == "#") {
          resultValue = ctxt.buildHash(args);
        } else if (fun == "$") {
          resultValue = ctxt.buildSortPrefix(args, resultType);
        } else if (fun == "rlike") {
          resultValue = ctxt.buildCompare(args[0].getValue(), args[0].getType(),
                                          args[1].getValue(), args[1].getType(),
                                          resultType, IQLToLLVMOpRLike);
        } else if (fun == "ifnull") {
          resultValue = ctxt.buildIsNullFunction(args, resultType);
        } else {
          resultValue = ctxt.buildCall(fun.c_str(), args, resultType);
        }
      } else {
        ctxt.beginAggregateFunction();
        NativeCodegenValue v = getArg(*expr->begin_args());
        resultValue = ctxt.buildAggregateFunction(fun.c_str(), v.value, v.type, resultType);
      }
    }
    break;
  case IQLExpression::INT32:
    {
      const std::string & s = expr->getStringData();
      if (s.size() > 1 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        throw std::runtime_error("INTERNAL ERROR: hex integer literals are not supported by native code generation");
      }
      resultValue = ctxt.buildDecimalInt32Literal(s.c_str());
    }
    break;
  case IQLExpression::INT64:
    resultValue = ctxt.buildDecimalInt64Literal(expr->getStringData().c_str());
    break;
  case IQLExpression::DOUBLE:
    resultValue = ctxt.buildDoubleLiteral(expr->getStringData().c_str());
    break;
  case IQLExpression::DECIMAL:
    resultValue = ctxt.buildDecimalLiteral(expr->getStringData().c_str());
    break;
  case IQLExpression::STRING:
    resultValue = ctxt.buildVarcharLiteral(expr->getStringData().c_str());
    break;
  case IQLExpression::BOOLEAN:
    resultValue = expr->getBooleanData() ? ctxt.buildTrue() : ctxt.buildFalse();
    break;
  case IQLExpression::INTERVAL:
    {
      NativeCodegenValue arg = getArg(*expr->begin_args());
      (void) arg;
      resultValue = ctxt.buildInterval(expr->getStringData().c_str(), arg.value);
    }
    break;
  case IQLExpression::NIL:
    resultValue = ctxt.buildNull();
    break;
  case IQLExpression::IPV4:
    resultValue = ctxt.buildIPv4Literal(expr->getStringData().c_str());
    break;
  case IQLExpression::IPV6:
    resultValue = ctxt.buildIPv6Literal(expr->getStringData().c_str());
    break;
  case IQLExpression::ARRAYREF:
    {
      NativeCodegenValue arr = getArg(*expr->begin_args());
      NativeCodegenValue idx = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildArrayRef(arr.value, arr.type, idx.value, idx.type, resultType);
    }
    break;
  case IQLExpression::ARRAYLVALUE:
    {
      NativeCodegenValue arr = getArg(*expr->begin_args());
      NativeCodegenValue idx = getArg(*(expr->begin_args() + 1));
      resultValue = ctxt.buildArrayLValue(arr.value, arr.type, idx.value, idx.type, resultType);
    }
    break;
  case IQLExpression::ARR:
    {
      std::vector<IQLToLLVMTypedValue> args;
      for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
        NativeCodegenValue v = getArg(*it);
        args.emplace_back(v.value, v.type);
      }
      resultValue = ctxt.buildArray(args, resultType);
    }
    break;
  case IQLExpression::STRUCT:
    {
      std::vector<IQLToLLVMTypedValue> args;
      for (auto it = expr->begin_args(); it != expr->end_args(); ++it) {
        NativeCodegenValue v = getArg(*it);
        args.emplace_back(v.value, v.type);
      }
      resultValue = ctxt.buildRow(args, const_cast<FieldType *>(resultType));
    }
    break;
  case IQLExpression::TYPE:
  case IQLExpression::DECLTYPE:
    // Type expressions generate no code
    break;
  default:
    throw std::runtime_error("INTERNAL ERROR: unsupported node in native code generation");
  }

  NativeCodegenValue ret{resultValue, resultType};
  return applyCoercion(ret, expr, ctxt);
}

bool codegenStmt(IQLStatement * stmt,
                 CodeGenerationContext & ctxt,
                 int & pos)
{
  bool isTerminated = false;
  
  if (stmt == NULL) {
    throw std::runtime_error("INTERNAL ERROR: null statement in native code generation");
  }

  const auto codeGen = [&ctxt, &pos](IQLStatement * arg) {
    return codegenStmt(arg, ctxt, pos);
  };

  const auto getArg = [&ctxt](IQLStatement * arg) {
    if (!arg->isExpressionNode()) {
      throw std::runtime_error("INTERNAL ERROR: expression expected in native code generation");
    }
    return codegenExpr(static_cast<IQLExpression *>(arg), ctxt);
  };

  switch (stmt->getNodeType()) {
  case IQLStatement::BLOCK:
    for(auto it = stmt->begin_children(), e = stmt->end_children(); it != e && !isTerminated; ++it) {
      isTerminated = codeGen(*it);
    }
    break;
  case IQLStatement::BREAK:
    ctxt.buildBreak();
    isTerminated = true;
    break;
  case IQLStatement::CONTINUE:
    ctxt.buildContinue();
    isTerminated = true;
    break;
  case IQLStatement::DECLARE:
    if (stmt->children_size() > 0) {
      auto val = getArg(*stmt->begin_children());
      ctxt.buildLocalVariable(stmt->getStringData().c_str(), val.value, val.type);
    } else {
      ctxt.buildDeclareLocal(stmt->getStringData().c_str(), static_cast<DeclareStatement*>(stmt)->getDeclaredType());
    }
    break;
  case IQLStatement::RETURN:
    {
      auto ret = getArg(*stmt->begin_children());
      ctxt.buildReturnValue(ret.value, ret.type);
      break;
    }
  case IQLStatement::ADDFIELD:
    {
      auto ret = getArg(*stmt->begin_children());
      ctxt.buildSetField(&pos, ret.value);
      break;
    }
  case IQLStatement::FIELDPATTERN:
    // TODO: FIXME
    ctxt.buildQuotedId(stmt->getStringData().c_str(), nullptr, &pos);
    break;
  case IQLStatement::FIELDGLOB:
    ctxt.buildSetFields(stmt->getStringData().c_str(), &pos);
    break;
  case IQLStatement::IFTHENELSE:
    {
      auto cond = getArg(*stmt->begin_children());
      ctxt.buildBeginIf();
      codeGen(*(stmt->begin_children() + 1));
      ctxt.buildBeginElse();
      if (stmt->children_size() > 2) {
        codeGen(*(stmt->begin_children() + 2));
      }
      ctxt.buildEndIf(cond.value);
    }
    break;
  case IQLStatement::SET:
    {
      auto lhs = getArg(*stmt->begin_children());
      auto rhs = getArg(*(stmt->begin_children() + 1));
      ctxt.buildSetNullableValue(static_cast<const IQLToLLVMLValue*>(lhs.value), rhs.value, rhs.type, lhs.type);
    }
    break;
  case IQLStatement::SWITCH:
    {
      auto expr = getArg(*stmt->begin_children());
      ctxt.buildBeginSwitch();      
      if (stmt->children_size() > 1) {
        for(auto it = stmt->begin_children()+1, e = stmt->end_children(); it != e; ++it) {
          codeGen(*it);
        }
      }
      ctxt.buildEndSwitch(expr.value);
    }
    break;
  case IQLStatement::SWITCHCASE:
    {
      ctxt.buildBeginSwitchCase(stmt->getStringData().c_str());
      for(auto it = stmt->begin_children(), e = stmt->end_children(); it != e; ++it) {
        codeGen(*it);
      }
      ctxt.buildEndSwitchCase();
    }
    break;
  case IQLStatement::WHILE:
    {
      ctxt.whileBegin();
      auto cond = getArg(*stmt->begin_children());
      ctxt.whileStatementBlock(cond.value, cond.type);
      codeGen(*(stmt->begin_children() + 1));
      ctxt.whileFinish();
    }
    break;
  default:
    throw std::runtime_error("INTERNAL ERROR: unsupported node in native type check");
  }
  return isTerminated;
}

} // namespace

void IQLToLLVMExpressionNative(IQLExpression * expr,
                               CodeGenerationContext & ctxt)
{
  NativeCodegenValue ret = codegenExpr(expr, ctxt);
  ctxt.buildReturnValue(ret.value, ret.type);
}

void IQLToLLVMFunctionNative(const std::vector<IQLStatement *> & stmts,
                             CodeGenerationContext & ctxt)
{
  int pos = 0;
  for(IQLStatement * stmt : stmts) {
    codegenStmt(stmt, ctxt, pos);
  }
}
