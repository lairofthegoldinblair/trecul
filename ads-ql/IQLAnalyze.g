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

tree grammar IQLAnalyze;
options {
  tokenVocab=IQL;
  language=C;
  ASTLabelType    = pANTLR3_BASE_TREE;
}

@header {
#include "IQLBuildTree.h"
}

graph[IQLGraphContextRef ctxt]
    :  
        (node[$ctxt] | edge[$ctxt])*
    ;
     
node[IQLGraphContextRef ctxt]
@init {
const char * ty=NULL;
const char * nm=NULL;
}
    :
    ^('[' id1 = ID { ty = nm = (const char *)$id1.text->chars; } (id2 = ID { nm = (const char *) $id2.text->chars; })? { IQLGraphNodeStart($ctxt, ty, nm); } nodeParam[$ctxt]* {IQLGraphNodeComplete($ctxt); } )
    ;

nodeParam[IQLGraphContextRef ctxt]
    :
    ^('=' ID (DECIMAL_INTEGER_LITERAL { IQLGraphNodeBuildIntegerParam($ctxt, (const char *)$ID.text->chars, (const char *) $DECIMAL_INTEGER_LITERAL.text->chars); } | DOUBLE_QUOTED_STRING_LITERAL { IQLGraphNodeBuildStringParam($ctxt, (const char *)$ID.text->chars, (const char *) $DOUBLE_QUOTED_STRING_LITERAL.text->chars); }))
    ;

edge[IQLGraphContextRef ctxt]
    :
    ^('->' id1 = ID id2 = ID { IQLGraphNodeBuildEdge($ctxt, (const char *)$id1.text->chars, (const char *)$id2.text->chars); })
    ;

recordFormat[IQLRecordTypeContextRef ctxt, IQLTreeFactoryRef tyCtxt]
    :
    fieldFormat[$ctxt, $tyCtxt]+
    ;

fieldFormat[IQLRecordTypeContextRef ctxt, IQLTreeFactoryRef tyCtxt]
    :
    ^(ID ty = builtInType[$tyCtxt] { IQLRecordTypeBuildField($ctxt, (const char *)$ID.text->chars, ty.ty); })
    ;

singleExpression[IQLTreeFactoryRef ctxt] returns [IQLStatementListRef sl]
@init {
    IQLStatementListRef stmts = IQLStatementListCreate($ctxt);
}
  :
  (d=declareStatement[$ctxt]  { IQLStatementListAppend($ctxt, stmts, d); })* r=returnExpression[$ctxt] { IQLStatementListAppend($ctxt, stmts, r); $sl = stmts; }
  ;

declareStatement[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
    :
    ^(t=TK_DECLARE e1=expression[$ctxt] nm=ID { $s = IQLBuildDeclare($ctxt, e1.e, (const char *)$nm.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); })
    ;

returnExpression[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
    :
    ^(t=TK_RETURN e1=expression[$ctxt] { $s = IQLBuildReturn($ctxt, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    ;

program[IQLTreeFactoryRef ctxt]
  :
  ^(TK_CREATE programArgDecl[$ctxt]* returnsDecl[$ctxt]? statementBlock[$ctxt])
  ;

returnsDecl[IQLTreeFactoryRef ctxt]
        :
        ^(TK_RETURNS builtInType[$ctxt])
        ;

recordConstructor[IQLTreeFactoryRef ctxt] returns [IQLStatementListRef ctor]
@init {
    IQLStatementListRef fields = IQLStatementListCreate($ctxt);
}
    :
        (f = fieldConstructor[$ctxt] { IQLStatementListAppend($ctxt, fields, f); } )+ { $ctor = fields; }
    ;

fieldConstructor[IQLTreeFactoryRef ctxt] returns [IQLStatementRef ctor]
@init {
    const char * nm = NULL;
}
    :
    ^(t=ID '*' { $ctor = IQLBuildAddFields($ctxt, (const char *) $ID.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); })
    |
    ^(t=TK_SET e = expression[$ctxt] (ID {nm = (const char *) $ID.text->chars; })?  { $ctor = IQLBuildAddField($ctxt, nm != NULL ? nm : e.name ? (const char *) e.name->chars : NULL, e.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    |
    d = declareStatement[$ctxt] { $ctor = d; }
    |
    ^(a=QUOTED_ID (b=QUOTED_ID { nm = (const char *) $b.text->chars; })?) { $ctor = IQLBuildQuotedId($ctxt, (const char *) $a.text->chars, nm, $a->getLine($a), $a->getCharPositionInLine($a)); }
    ;

localVarOrId
    :
    ID
    ;

programArgDecl[IQLTreeFactoryRef ctxt]
    :
	^(TK_DECLARE localVarOrId builtInType[$ctxt] TK_OUTPUT?) 
	;

statement[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
	:
	(
	s1=setStatement[$ctxt] { $s = s1; }
	| s1=variableDeclaration[$ctxt] { $s = s1; }
	| s1=declareStatement[$ctxt] { $s = s1; }
	| printStatement[$ctxt]
	| s1=ifStatement[$ctxt] { $s = s1; }
	| s1=statementBlock[$ctxt] { $s = s1; }
	| ^(TK_RETURN expression[$ctxt]?)
	| t=TK_BREAK { $s = IQLBuildBreak($ctxt, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=TK_CONTINUE { $s = IQLBuildContinue($ctxt, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| ^(TK_RAISERROR expression[$ctxt] expression[$ctxt]?)
	| s1=switchStatement[$ctxt] { $s = s1; }
	| s1=whileStatement[$ctxt] { $s = s1; }
	)
	;

variableDeclaration[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
	:
	^(t=TK_DECLARE nm=localVarOrId ty=builtInType[$ctxt] { $s = IQLBuildDeclareNoInit($ctxt, ty.ty, (const char *)$nm.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); })
	;

builtInType[IQLTreeFactoryRef ctxt] returns [IQLFieldTypeRef ty, int line, int col]
@init {
nullable=0;
array=NULL;
$line = 0;
$col = 0;
}
	: ^(c=TK_INTEGER (array=arrayTypeSpec)? (nullable=typeNullability)? { $line = $c->getLine($c); $col = $c->getCharPositionInLine($c); }) { $ty = array != NULL ? IQLBuildInt32ArrayType($ctxt, array, nullable) : IQLBuildInt32Type($ctxt, nullable);  }
	  | ^(c=TK_DOUBLE (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildDoubleArrayType($ctxt, array, nullable) : IQLBuildDoubleType($ctxt, nullable); }
	  | ^(c=TK_CHAR DECIMAL_INTEGER_LITERAL (nullable=typeNullability)?) { $ty = IQLBuildCharType($ctxt, (const char *) $DECIMAL_INTEGER_LITERAL.text->chars, nullable); }
	  | ^(c=TK_VARCHAR  (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildVarcharArrayType($ctxt, array, nullable) : IQLBuildVarcharType($ctxt, nullable); }
	  | ^(c=TK_NVARCHAR (nullable=typeNullability)?) { $ty = IQLBuildNVarcharType($ctxt, nullable); }
	  | ^(c=TK_DECIMAL (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildDecimalArrayType($ctxt, array, nullable) : IQLBuildDecimalType($ctxt, nullable); } 
	  | ^(c=TK_BOOLEAN (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildBooleanArrayType($ctxt, array, nullable) : IQLBuildBooleanType($ctxt, nullable); } 
	  | ^(c=TK_DATETIME (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildDatetimeArrayType($ctxt, array, nullable) : IQLBuildDatetimeType($ctxt, nullable); } 
      | ^(c=TK_BIGINT (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildInt64ArrayType($ctxt, array, nullable) : IQLBuildInt64Type($ctxt, nullable); }
      | ^(c=TK_SMALLINT (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildInt16ArrayType($ctxt, array, nullable) : IQLBuildInt16Type($ctxt, nullable); }
      | ^(c=TK_TINYINT (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildInt8ArrayType($ctxt, array, nullable) : IQLBuildInt8Type($ctxt, nullable); }
      | ^(c=TK_REAL (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildFloatArrayType($ctxt, array, nullable) : IQLBuildFloatType($ctxt, nullable); }
      | ^(c=TK_IPV4 (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildIPv4ArrayType($ctxt, array, nullable) : IQLBuildIPv4Type($ctxt, nullable); }
      | ^(c=TK_CIDRV4 (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildCIDRv4ArrayType($ctxt, array, nullable) : IQLBuildCIDRv4Type($ctxt, nullable); }
      | ^(c=TK_IPV6 (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildIPv6ArrayType($ctxt, array, nullable) : IQLBuildIPv6Type($ctxt, nullable); }
      | ^(c=TK_CIDRV6 (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildCIDRv6ArrayType($ctxt, array, nullable) : IQLBuildCIDRv6Type($ctxt, nullable); }
	  | ^(c=ID (array=arrayTypeSpec)? (nullable=typeNullability)?) { $ty = array != NULL ? IQLBuildArrayType($ctxt, (const char *) $ID.text->chars, array, nullable) : IQLBuildType($ctxt, (const char *) $ID.text->chars, nullable); } 
	;

arrayTypeSpec returns [const char * sz]
@init {
    sz = "infinity";
}
    :
    ^(TK_ARRAY (DECIMAL_INTEGER_LITERAL { sz = (const char *) $DECIMAL_INTEGER_LITERAL.text->chars; })?)
    ;

typeNullability returns [int nullable]
    :
    TK_NULL { $nullable = 1; }
    | 
    TK_NOT { $nullable = 0; }
    ;

typeExpr[IQLTreeFactoryRef ctxt] returns [IQLExpressionRef e]
@init {
array=NULL;
}
    :
    ty = builtInType[$ctxt] { $e = IQLBuildTypeExpr($ctxt, ty.ty, ty.line, ty.col); }
    |
    ^(t=TK_DECLTYPE e1 = expression[$ctxt] (array=arrayTypeSpec)? { $e = IQLBuildDecltype($ctxt, e1.e, array, $t->getLine($t), $t->getCharPositionInLine($t)); })
    ;

setStatement[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
	:
	^(t=TK_SET e1=variableReference[$ctxt] e2=expression[$ctxt] { $s = IQLBuildSet($ctxt, e1, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
	;

variableReference[IQLTreeFactoryRef ctxt] returns [IQLExpressionRef e]
    :
    t=ID { $e = IQLBuildVariableLValue($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    |
    ^(t='[' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildArrayLValue($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    |
    ^(t='.' e1=expression[$ctxt] member=ID { $e = IQLBuildStructMemberLValue($ctxt, e1.e, (const char *)$member.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); })
    ;

switchStatement[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
@init {
    IQLStatementListRef stmts = IQLStatementListCreate($ctxt);
}
@after {
    IQLStatementListFree($ctxt, stmts);
}
    :
    ^(t=TK_SWITCH e=expression[$ctxt] (s1=switchBlock[$ctxt] { IQLStatementListAppend($ctxt, stmts, s1); })+ { $s = IQLBuildSwitch($ctxt, e.e, stmts, $t->getLine($t), $t->getCharPositionInLine($t)); })
    ;

switchBlock[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
@init {
    IQLStatementListRef stmts = IQLStatementListCreate($ctxt);
}
@after {
    IQLStatementListFree($ctxt, stmts);
}
    :
    ^(t=TK_CASE d=DECIMAL_INTEGER_LITERAL (s1 = statement[$ctxt] { IQLStatementListAppend($ctxt, stmts, s1); })+ { $s = IQLBuildSwitchCase($ctxt, (const char *)$d.text->chars, stmts, $t->getLine($t), $t->getCharPositionInLine($t)); })
    ;

printStatement[IQLTreeFactoryRef ctxt]
	:
	^(TK_PRINT expression[$ctxt])
    ;

ifStatement[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
	:
	^(t=TK_IF e=expression[$ctxt] s1=statement[$ctxt] s2=statement[$ctxt]? { $s = IQLBuildIfThenElse($ctxt, e.e, s1, s2, $t->getLine($t), $t->getCharPositionInLine($t)); })
	;

statementBlock[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
@init {
    IQLStatementListRef stmts = IQLStatementListCreate($ctxt);
}
@after {
    IQLStatementListFree($ctxt, stmts);
}
	:
	^(t=TK_BEGIN (s1=statement[$ctxt] { IQLStatementListAppend($ctxt, stmts, s1); })* { $s = IQLBuildStatementBlock($ctxt, stmts, $t->getLine($t), $t->getCharPositionInLine($t)); }) 
	;

whileStatement[IQLTreeFactoryRef ctxt] returns [IQLStatementRef s]
	:
	^(t=TK_WHILE e=expression[$ctxt] s1=statement[$ctxt] {  $s = IQLBuildWhile($ctxt, e.e, s1, $t->getLine($t), $t->getCharPositionInLine($t)); })
	;

expression[IQLTreeFactoryRef ctxt] returns [pANTLR3_STRING name, IQLExpressionRef e]
@init {
    int isBinary = 0;
    IQLExpressionListRef l = NULL;
    $name = NULL;
    $e = NULL;
}
	: 
    ^(t=TK_OR e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLogicalOr($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_AND e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLogicalAnd($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_NOT e1=expression[$ctxt] { $e = IQLBuildLogicalNot($ctxt, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_IS e1=expression[$ctxt] (TK_NOT { isBinary = 1; })?  { $e = IQLBuildIsNull($ctxt, e1.e, isBinary, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_CASE { l = IQLExpressionListCreate($ctxt); } (whenExpression[$ctxt,l])+ (elseExpression[$ctxt,l]) { $e = IQLBuildCase($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t='?' { l = IQLExpressionListCreate($ctxt); } e1=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e1.e); } e2=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e2.e); } e3=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e3.e); $e = IQLBuildCase($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); } )
    | ^(t='^' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildBitwiseXor($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='|' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildBitwiseOr($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='&' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildBitwiseAnd($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='>' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildGreaterThan($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLessThan($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='>=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildGreaterThanEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLessThanEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<>' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildNotEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='!=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildNotEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='>>' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildSubnetContains($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='>>=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildSubnetContainsEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<<' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildSubnetContainedBy($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<<=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildSubnetContainedByEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='&&' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildSubnetSymmetricContainsEquals($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t=TK_LIKE e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLike($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t=TK_RLIKE e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildRLike($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='+' e1=expression[$ctxt] (e2=expression[$ctxt] { isBinary = 1; })? { $e = isBinary ? IQLBuildPlus($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)) : IQLBuildUnaryPlus($ctxt, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='-' e1=expression[$ctxt] (e2=expression[$ctxt] { isBinary = 1; })? { $e = isBinary ? IQLBuildMinus($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)) : IQLBuildUnaryMinus($ctxt, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='*' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildTimes($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='/' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildDivide($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='%' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildModulus($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='~' e1=expression[$ctxt] { $e = IQLBuildBitwiseNot($ctxt, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='||' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildConcatenation($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='#'  { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1.e); })+ { $e = IQLBuildCall($ctxt, "#", l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t='$'  { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1.e); })+ { $e = IQLBuildCall($ctxt, "$", l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t=TK_CAST ty=typeExpr[$ctxt] e1=expression[$ctxt] { $e = IQLBuildCast($ctxt, ty, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='(' { l = IQLExpressionListCreate($ctxt); } fun=ID (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1.e); })* { $e = IQLBuildCall($ctxt, (const char *)$fun.text->chars, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t=LITERAL_CAST fun=ID lit=STRING_LITERAL) { $e = IQLBuildLiteralCast($ctxt, (const char *)$fun.text->chars, (const char *)$lit.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | ^(t=DATETIME_LITERAL lit=STRING_LITERAL) { $e = IQLBuildLiteralCast($ctxt, "DATETIME", (const char *)$lit.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=DECIMAL_INTEGER_LITERAL { $e = IQLBuildInt32($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=HEX_INTEGER_LITERAL { $e = IQLBuildHexInt32($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=DECIMAL_BIGINT_LITERAL { $e = IQLBuildInt64($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=FLOATING_POINT_LITERAL { $e = IQLBuildDouble($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=DECIMAL_LITERAL { $e = IQLBuildDecimal($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=STRING_LITERAL { $e = IQLBuildString($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=IPV4_LITERAL { $e = IQLBuildIPv4($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=IPV6_LITERAL { $e = IQLBuildIPv6($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=TK_TRUE { $e = IQLBuildBoolean($ctxt, 1, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=TK_FALSE { $e = IQLBuildBoolean($ctxt, 0, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=ID { $name = $t.text; $e = IQLBuildVariable($ctxt, (const char *)$t.text->chars, 0, $t->getLine($t), $t->getCharPositionInLine($t)); } 
	| ^(t='.' e1=expression[$ctxt] fun=ID) { $name = $fun.text; $e = IQLBuildStructMemberRef($ctxt, e1.e, (const char *)$fun.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | ^(t='[' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildArrayRef($ctxt, e1.e, e2.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | t=TK_NULL { $e = IQLBuildNil($ctxt, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | ^(t=TK_ARRAY_CONCAT e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "ARRAY_CONCAT", e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_SUM e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "SUM", e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_MAX e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "MAX", e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_MIN e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "MIN", e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_INTERVAL fun=ID e1=expression[$ctxt] { $e = IQLBuildInterval($ctxt, (const char *)$fun.text->chars, e1.e, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t=TK_ARRAY { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1.e); })* { $e = IQLBuildArray($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t=TK_ROW { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1.e); })* { $e = IQLBuildRow($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    ;    

whenExpression[IQLTreeFactoryRef ctxt, IQLExpressionListRef l]
    :
    ^(TK_WHEN e1=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e1.e); } e2=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e2.e); } )
    ;    

elseExpression[IQLTreeFactoryRef ctxt, IQLExpressionListRef l]
    :
    ^(TK_ELSE e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1.e); } )
    ;    
