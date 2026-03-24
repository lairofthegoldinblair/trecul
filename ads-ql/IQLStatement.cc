#include "IQLExpression.hh"
#include "IQLStatement.hh"

IQLStatement::~IQLStatement()
{
}

void IQLStatement::replaceArg(IQLStatement * oldArg,
			       IQLStatement * newArg)
{
  for(std::size_t i=0, sz=mArgs.size(); i<sz; ++i) {
    if (mArgs[i] == oldArg) {
      mArgs[i] = newArg;
      return;
    }
  }
  throw std::runtime_error("Internal Error: call to IQLStatement::replaceArg"
			   " with invalid argument");
}

BreakStatement::BreakStatement(DynamicRecordContext & ctxt, 
                                     const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::BREAK, loc)
{
}

BreakStatement * BreakStatement::create(DynamicRecordContext & ctxt,
                                              const SourceLocation& loc)
{
  BreakStatement * tmp = new BreakStatement(ctxt, loc);
  ctxt.add(tmp);
  return tmp;
}

BreakStatement * BreakStatement::clone() const
{
  return new BreakStatement(*this);
}

ContinueStatement::ContinueStatement(DynamicRecordContext & ctxt, 
                                     const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::CONTINUE, loc)
{
}

ContinueStatement * ContinueStatement::create(DynamicRecordContext & ctxt,
                                              const SourceLocation& loc)
{
  ContinueStatement * tmp = new ContinueStatement(ctxt, loc);
  ctxt.add(tmp);
  return tmp;
}

ContinueStatement * ContinueStatement::clone() const
{
  return new ContinueStatement(*this);
}

DeclareStatement::DeclareStatement(DynamicRecordContext & ctxt, 
                                   IQLExpression * val,
                                   const char * nm,
                                   const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::DECLARE, val, loc),
  mDeclaredType(nullptr)
{
  setData(nm);
}

DeclareStatement::DeclareStatement(DynamicRecordContext & ctxt, 
                                   const FieldType * ty,
                                   const char * nm,
                                   const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::DECLARE, loc)
{
  mDeclaredType = ty;
  setData(nm);
}

DeclareStatement * DeclareStatement::create(DynamicRecordContext & ctxt,
                                            IQLExpression * val,
                                            const char * nm,
                                            const SourceLocation& loc)
{
  DeclareStatement * tmp = new DeclareStatement(ctxt, val, nm, loc);
  ctxt.add(tmp);
  return tmp;
}

DeclareStatement * DeclareStatement::create(DynamicRecordContext & ctxt,
                                            const FieldType * ty,
                                            const char * nm,
                                            const SourceLocation& loc)
{
  DeclareStatement * tmp = new DeclareStatement(ctxt, ty, nm, loc);
  ctxt.add(tmp);
  return tmp;
}

DeclareStatement * DeclareStatement::clone() const
{
  return new DeclareStatement(*this);
}

ReturnStatement::ReturnStatement(DynamicRecordContext & ctxt, 
                                   IQLExpression * val,
                                   const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::RETURN, val, loc)
{
}

ReturnStatement * ReturnStatement::create(DynamicRecordContext & ctxt,
                                            IQLExpression * val,
                                            const SourceLocation& loc)
{
  ReturnStatement * tmp = new ReturnStatement(ctxt, val, loc);
  ctxt.add(tmp);
  return tmp;
}

ReturnStatement * ReturnStatement::clone() const
{
  return new ReturnStatement(*this);
}

AddFieldStatement::AddFieldStatement(DynamicRecordContext & ctxt, 
                                     IQLExpression * val,
                                     const char * nm,
                                     const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::ADDFIELD, val, loc)
{
  if (nullptr != nm) {
    setData(nm);
  }
}

AddFieldStatement * AddFieldStatement::create(DynamicRecordContext & ctxt,
                                            IQLExpression * val,
                                            const char * nm,
                                            const SourceLocation& loc)
{
  AddFieldStatement * tmp = new AddFieldStatement(ctxt, val, nm, loc);
  ctxt.add(tmp);
  return tmp;
}

AddFieldStatement * AddFieldStatement::clone() const
{
  return new AddFieldStatement(*this);
}

IQLExpression * AddFieldStatement::getExpression()
{
  return static_cast<IQLExpression *>(*begin_children()); 
}

const std::string& AddFieldStatement::getName() const
{
  return getStringData();
}

FieldPatternStatement::FieldPatternStatement(DynamicRecordContext & ctxt,
                                           const char * nm,
                                           const char * nm2,
                                           const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::FIELDPATTERN, loc)
{
  if (nm2 != nullptr) {
    throw std::runtime_error("Multipart field pattern not yet supported in native AST");
  }
  setData(nm);
}

IfThenElseStatement::IfThenElseStatement(DynamicRecordContext & ctxt,
                                         IQLExpression * cond,
                                         IQLStatement * thenStmts,
                                         IQLStatement * elseStmts,
                                         const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::IFTHENELSE, cond, thenStmts, loc)
{
  if (nullptr != elseStmts) {
    mArgs.push_back(elseStmts);
  }
}

SetStatement::SetStatement(DynamicRecordContext & ctxt,
                           IQLExpression * lhs,
                           IQLExpression * rhs,
                           const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::SET, lhs, rhs, loc)
{
}

WhileStatement::WhileStatement(DynamicRecordContext & ctxt,
                               IQLExpression * cond,
                               IQLStatement * stmts,
                               const SourceLocation& loc)
  :
  IQLStatement(ctxt, IQLStatement::WHILE, cond, stmts, loc)
{
}

const char * IQLStatementPrinter::getExpressionSymbol(uint32_t nodeType)
{
  switch(nodeType) {
  case IQLStatement::LOR: return "OR";
  case IQLStatement::LAND: return "AND";
  case IQLStatement::LNOT: return "NOT";
  case IQLStatement::BAND: return "&";
  case IQLStatement::BOR: return "|";
  case IQLStatement::BXOR: return "^";
  case IQLStatement::BNOT: return "~";
  case IQLStatement::EQ: return "=";
  case IQLStatement::GTN: return ">";
  case IQLStatement::LTN: return "<";
  case IQLStatement::GTEQ: return ">=";
  case IQLStatement::LTEQ: return "<=";
  case IQLStatement::NEQ: return "<>";
  case IQLStatement::MINUS: return "-";
  case IQLStatement::PLUS: return "+";
  case IQLStatement::TIMES: return "*";
  case IQLStatement::DIVIDE: return "/";
  case IQLStatement::MOD: return "%";
  case IQLStatement::CONCAT: return "||";
  case IQLStatement::LISNULL: return "IS NULL";
  case IQLStatement::NIL: return "INTERVAL";
  default:
    throw std::runtime_error((boost::format("Unknown node type %1%") % nodeType).str());
  }
}

IQLStatementPrinter::IQLStatementPrinter(std::ostream& ostr, IQLStatement * input)
{
  mBinaryInfix.insert("+");
  mBinaryInfix.insert("-");
  mBinaryInfix.insert("*");
  mBinaryInfix.insert("/");
  mBinaryInfix.insert("%");
  mUnaryInfix.insert("+");
  mUnaryInfix.insert("-");

  // Bottom up pattern replacement.
  std::vector<std::pair<IQLStatement::child_const_iterator, 
                        IQLStatement::child_const_iterator> > stk;
  std::vector<IQLStatement *> tmp;
  tmp.push_back(input);
  // Push the node with arguments on the stack.
  stk.push_back(std::make_pair(tmp.begin(), tmp.end()));
  while(stk.size()) {
    if (stk.back().first != stk.back().second) {
      IQLStatement * s = *stk.back().first;
      if (s->isExpressionNode()) {
        IQLExpression * e = static_cast<IQLExpression *>(s);
        switch (e->getNodeType()) {
        case IQLStatement::ARRAYREF:
          ostr << "(";
          break;
        case IQLStatement::CALL:
          if (isBinaryInfix(e)) {
            ostr << "(";
          } else if (isUnaryPrefix(e)) {
            ostr << e->getStringData().c_str() << "(";
          } else {
            ostr << e->getStringData().c_str() << "(";
          }
          break;
        case IQLStatement::LISNULL:
          ostr << (e->getBooleanData() ? "_ISNOTNULL_(" : "_ISNULL_(");
          break;
        case IQLStatement::LNOT:
        case IQLStatement::BNOT:
          ostr << getExpressionSymbol(e->getNodeType()) << "(";
          break;
        case IQLStatement::PLUS:
        case IQLStatement::MINUS:
          if (e->args_size() == 1) {
            ostr << getExpressionSymbol(e->getNodeType()) << "(";
          } else {
            ostr << "(";
          }
          break;
        case IQLStatement::LAND:
        case IQLStatement::LOR:
        case IQLStatement::BAND:
        case IQLStatement::BOR:
        case IQLStatement::BXOR:
        case IQLStatement::EQ:
        case IQLStatement::NEQ:
        case IQLStatement::GTEQ:
        case IQLStatement::GTN:
        case IQLStatement::LTEQ:
        case IQLStatement::LTN:
        case IQLStatement::TIMES:
        case IQLStatement::DIVIDE:
        case IQLStatement::MOD:
        case IQLStatement::CONCAT:
	  ostr << "(";
          break;
        case IQLStatement::CASE:
          ostr << "CASE WHEN ";  
          break;
        case IQLStatement::NIL:
          ostr << "NULL";  
          break;
        case IQLStatement::VARIABLE:
        case IQLStatement::INT32:
        case IQLStatement::INT64:
        case IQLStatement::DOUBLE:
        case IQLStatement::DECIMAL:
          ostr << e->getStringData().c_str();
          break;
        case IQLStatement::STRING:
          // TODO: Do we need to quotify?
          ostr << e->getStringData().c_str();
          break;
        case IQLStatement::BOOLEAN:
          ostr << (e->getBooleanData() ? "TRUE" : "FALSE");
          break;
        default:
          break;
        }
        stk.push_back(std::make_pair(e->begin_children(), 
                                     e->end_children()));
      } else {
        std::runtime_error("Printing statement not implemented yet");
      }
    } else {
      stk.pop_back();
      if (stk.size()) {
	// Done with subtree at stk.back().first	
	IQLStatement * e = *stk.back().first;
	++stk.back().first;
	if (stk.size() > 1) {
	  IQLStatement * parent = *stk[stk.size()-2].first;
	  switch (parent->getNodeType()) {
	  case IQLStatement::STRUCTMEMBERREF:
	    ostr << "." << parent->getStringData().c_str();
	    break;
	  case IQLStatement::ARRAYREF:
            if (stk.back().first != stk.back().second) {
              ostr << "[";
            } else {
              ostr << "])";
            }
	    break;
	  case IQLStatement::CALL:
	    if (isBinaryInfix(parent)) {
	      ostr << ")";
	      if(stk.back().first != stk.back().second) {
		ostr << parent->getStringData().c_str() << "(";
	      }
	    } else {
	      if(stk.back().first != stk.back().second) {
		ostr << ",";
	      } else {
		ostr << ")";
	      }
	    }
	    break;
          case IQLStatement::LISNULL:
          case IQLStatement::LNOT:
          case IQLStatement::BNOT:
            ostr << ")";
            break;
	  case IQLStatement::CASE:
	    if (stk.back().first == stk.back().second) {
	      ostr << " END";  
	    } else if ((stk.back().first - parent->begin_children())  % 2) {
	      ostr << " THEN ";
	    } else if(stk.back().first+1 != stk.back().second) {
	      ostr << " WHEN ";
	    } else {
	      ostr << " ELSE ";
	    }
	    break;	    
	  case IQLStatement::LAND:
	  case IQLStatement::LOR:
          case IQLStatement::BAND:
          case IQLStatement::BOR:
          case IQLStatement::BXOR:
	  case IQLStatement::EQ:
	  case IQLStatement::NEQ:
	  case IQLStatement::GTEQ:
	  case IQLStatement::GTN:
	  case IQLStatement::LTEQ:
	  case IQLStatement::LTN:
          case IQLStatement::PLUS:
          case IQLStatement::MINUS:
          case IQLStatement::TIMES:
          case IQLStatement::DIVIDE:
          case IQLStatement::MOD:
          case IQLStatement::CONCAT:
            ostr << ")";
            if(stk.back().first != stk.back().second) {
              if (parent->getNodeType() == IQLStatement::PLUS && parent->children_size() == 1) {
                // Unary + is always single-arg; no delimiter between operands.
              } else if (parent->getNodeType() == IQLStatement::MINUS && parent->children_size() == 1) {
                // Unary - is always single-arg; no delimiter between operands.
              } else {
                ostr << getExpressionSymbol(parent->getNodeType()) << "(";
              }
            }
	    break;
            break;
	  default:
	    break;
	  }
	}
      }
    }    
  }
}

bool IQLStatementPrinter::isBinaryInfix(IQLStatement * e)
{
  if (e->children_size() == 2 &&
      e->getNodeType() == IQLStatement::CALL &&
      mBinaryInfix.end() != mBinaryInfix.find(e->getStringData())) {
    return true;
  }

  switch (e->getNodeType()) {
  case IQLStatement::LAND:
  case IQLStatement::LOR:
  case IQLStatement::BAND:
  case IQLStatement::BOR:
  case IQLStatement::BXOR:
  case IQLStatement::EQ:
  case IQLStatement::NEQ:
  case IQLStatement::GTEQ:
  case IQLStatement::GTN:
  case IQLStatement::LTEQ:
  case IQLStatement::LTN:
  case IQLStatement::PLUS:
  case IQLStatement::MINUS:
  case IQLStatement::TIMES:
  case IQLStatement::DIVIDE:
  case IQLStatement::MOD:
  case IQLStatement::CONCAT:
    return e->children_size() == 2;
  default:
    return false;
  }
}

bool IQLStatementPrinter::isUnaryPrefix(IQLStatement * e)
{
  if (e->children_size() == 1 &&
      e->getNodeType() == IQLStatement::CALL &&
      mUnaryInfix.end() != mUnaryInfix.find(e->getStringData())) {
    return true;
  }

  switch (e->getNodeType()) {
  case IQLStatement::LNOT:
  case IQLStatement::BNOT:
    return true;
  case IQLStatement::PLUS:
  case IQLStatement::MINUS:
    return e->children_size() == 1;
  default:
    return false;
  }
}

