/** \file
 *  This C header file was generated by $ANTLR version 3.2 Sep 23, 2009 12:02:23
 *
 *     -  From the grammar source file : IQL.g
 *     -                            On : 2022-01-03 14:45:32
 *     -                 for the lexer : IQLLexerLexer *
 * Editing it, at least manually, is not wise. 
 *
 * C language generator and runtime by Jim Idle, jimi|hereisanat|idle|dotgoeshere|ws.
 *
 *
 * The lexer IQLLexer has the callable functions (rules) shown below,
 * which will invoke the code for the associated rule in the source grammar
 * assuming that the input stream is pointing to a token/text stream that could begin
 * this rule.
 * 
 * For instance if you call the first (topmost) rule in a parser grammar, you will
 * get the results of a full parse, but calling a rule half way through the grammar will
 * allow you to pass part of a full token stream to the parser, such as for syntax checking
 * in editors and so on.
 *
 * The parser entry points are called indirectly (by function pointer to function) via
 * a parser context typedef pIQLLexer, which is returned from a call to IQLLexerNew().
 *
 * As this is a generated lexer, it is unlikely you will call it 'manually'. However
 * the methods are provided anyway.
 * * The methods in pIQLLexer are  as follows:
 *
 *  -  void      pIQLLexer->T__120(pIQLLexer)
 *  -  void      pIQLLexer->T__121(pIQLLexer)
 *  -  void      pIQLLexer->T__122(pIQLLexer)
 *  -  void      pIQLLexer->T__123(pIQLLexer)
 *  -  void      pIQLLexer->T__124(pIQLLexer)
 *  -  void      pIQLLexer->T__125(pIQLLexer)
 *  -  void      pIQLLexer->T__126(pIQLLexer)
 *  -  void      pIQLLexer->T__127(pIQLLexer)
 *  -  void      pIQLLexer->T__128(pIQLLexer)
 *  -  void      pIQLLexer->T__129(pIQLLexer)
 *  -  void      pIQLLexer->T__130(pIQLLexer)
 *  -  void      pIQLLexer->T__131(pIQLLexer)
 *  -  void      pIQLLexer->T__132(pIQLLexer)
 *  -  void      pIQLLexer->T__133(pIQLLexer)
 *  -  void      pIQLLexer->T__134(pIQLLexer)
 *  -  void      pIQLLexer->T__135(pIQLLexer)
 *  -  void      pIQLLexer->T__136(pIQLLexer)
 *  -  void      pIQLLexer->T__137(pIQLLexer)
 *  -  void      pIQLLexer->T__138(pIQLLexer)
 *  -  void      pIQLLexer->T__139(pIQLLexer)
 *  -  void      pIQLLexer->T__140(pIQLLexer)
 *  -  void      pIQLLexer->T__141(pIQLLexer)
 *  -  void      pIQLLexer->T__142(pIQLLexer)
 *  -  void      pIQLLexer->T__143(pIQLLexer)
 *  -  void      pIQLLexer->T__144(pIQLLexer)
 *  -  void      pIQLLexer->T__145(pIQLLexer)
 *  -  void      pIQLLexer->T__146(pIQLLexer)
 *  -  void      pIQLLexer->T__147(pIQLLexer)
 *  -  void      pIQLLexer->T__148(pIQLLexer)
 *  -  void      pIQLLexer->TK_ADD(pIQLLexer)
 *  -  void      pIQLLexer->TK_ALL(pIQLLexer)
 *  -  void      pIQLLexer->TK_ALTER(pIQLLexer)
 *  -  void      pIQLLexer->TK_AND(pIQLLexer)
 *  -  void      pIQLLexer->TK_ANY(pIQLLexer)
 *  -  void      pIQLLexer->TK_ARRAY(pIQLLexer)
 *  -  void      pIQLLexer->TK_AS(pIQLLexer)
 *  -  void      pIQLLexer->TK_ASC(pIQLLexer)
 *  -  void      pIQLLexer->TK_AVG(pIQLLexer)
 *  -  void      pIQLLexer->TK_BEGIN(pIQLLexer)
 *  -  void      pIQLLexer->TK_BETWEEN(pIQLLexer)
 *  -  void      pIQLLexer->TK_BIGINT(pIQLLexer)
 *  -  void      pIQLLexer->TK_BOOLEAN(pIQLLexer)
 *  -  void      pIQLLexer->TK_BREAK(pIQLLexer)
 *  -  void      pIQLLexer->TK_BY(pIQLLexer)
 *  -  void      pIQLLexer->TK_CASE(pIQLLexer)
 *  -  void      pIQLLexer->TK_CAST(pIQLLexer)
 *  -  void      pIQLLexer->TK_CHAR(pIQLLexer)
 *  -  void      pIQLLexer->TK_CIDRV4(pIQLLexer)
 *  -  void      pIQLLexer->TK_CIDRV6(pIQLLexer)
 *  -  void      pIQLLexer->TK_COALESCE(pIQLLexer)
 *  -  void      pIQLLexer->TK_CONTINUE(pIQLLexer)
 *  -  void      pIQLLexer->TK_COUNT(pIQLLexer)
 *  -  void      pIQLLexer->TK_CREATE(pIQLLexer)
 *  -  void      pIQLLexer->TK_CROSS(pIQLLexer)
 *  -  void      pIQLLexer->TK_DATETIME(pIQLLexer)
 *  -  void      pIQLLexer->TK_DECLARE(pIQLLexer)
 *  -  void      pIQLLexer->TK_DECIMAL(pIQLLexer)
 *  -  void      pIQLLexer->TK_DESC(pIQLLexer)
 *  -  void      pIQLLexer->TK_DISTINCT(pIQLLexer)
 *  -  void      pIQLLexer->TK_DOUBLE(pIQLLexer)
 *  -  void      pIQLLexer->TK_ELSE(pIQLLexer)
 *  -  void      pIQLLexer->TK_END(pIQLLexer)
 *  -  void      pIQLLexer->TK_EXISTS(pIQLLexer)
 *  -  void      pIQLLexer->TK_FALSE(pIQLLexer)
 *  -  void      pIQLLexer->TK_FROM(pIQLLexer)
 *  -  void      pIQLLexer->TK_FULL(pIQLLexer)
 *  -  void      pIQLLexer->TK_FUNCTION(pIQLLexer)
 *  -  void      pIQLLexer->TK_GROUP(pIQLLexer)
 *  -  void      pIQLLexer->TK_HAVING(pIQLLexer)
 *  -  void      pIQLLexer->TK_IF(pIQLLexer)
 *  -  void      pIQLLexer->TK_IN(pIQLLexer)
 *  -  void      pIQLLexer->TK_INDEX(pIQLLexer)
 *  -  void      pIQLLexer->TK_INNER(pIQLLexer)
 *  -  void      pIQLLexer->TK_INTO(pIQLLexer)
 *  -  void      pIQLLexer->TK_INTEGER(pIQLLexer)
 *  -  void      pIQLLexer->TK_INTERVAL(pIQLLexer)
 *  -  void      pIQLLexer->TK_IPV4(pIQLLexer)
 *  -  void      pIQLLexer->TK_IPV6(pIQLLexer)
 *  -  void      pIQLLexer->TK_IS(pIQLLexer)
 *  -  void      pIQLLexer->TK_JOIN(pIQLLexer)
 *  -  void      pIQLLexer->TK_KEY(pIQLLexer)
 *  -  void      pIQLLexer->TK_LEFT(pIQLLexer)
 *  -  void      pIQLLexer->TK_LIKE(pIQLLexer)
 *  -  void      pIQLLexer->TK_MAX(pIQLLexer)
 *  -  void      pIQLLexer->TK_MIN(pIQLLexer)
 *  -  void      pIQLLexer->TK_NOT(pIQLLexer)
 *  -  void      pIQLLexer->TK_NULL(pIQLLexer)
 *  -  void      pIQLLexer->TK_NVARCHAR(pIQLLexer)
 *  -  void      pIQLLexer->TK_ON(pIQLLexer)
 *  -  void      pIQLLexer->TK_OR(pIQLLexer)
 *  -  void      pIQLLexer->TK_ORDER(pIQLLexer)
 *  -  void      pIQLLexer->TK_OUTER(pIQLLexer)
 *  -  void      pIQLLexer->TK_OUTPUT(pIQLLexer)
 *  -  void      pIQLLexer->TK_PRECISION(pIQLLexer)
 *  -  void      pIQLLexer->TK_PRINT(pIQLLexer)
 *  -  void      pIQLLexer->TK_PROCEDURE(pIQLLexer)
 *  -  void      pIQLLexer->TK_RAISERROR(pIQLLexer)
 *  -  void      pIQLLexer->TK_REAL(pIQLLexer)
 *  -  void      pIQLLexer->TK_RETURN(pIQLLexer)
 *  -  void      pIQLLexer->TK_RETURNS(pIQLLexer)
 *  -  void      pIQLLexer->TK_RIGHT(pIQLLexer)
 *  -  void      pIQLLexer->TK_RLIKE(pIQLLexer)
 *  -  void      pIQLLexer->TK_ROW(pIQLLexer)
 *  -  void      pIQLLexer->TK_SELECT(pIQLLexer)
 *  -  void      pIQLLexer->TK_SET(pIQLLexer)
 *  -  void      pIQLLexer->TK_SMALLINT(pIQLLexer)
 *  -  void      pIQLLexer->TK_SOME(pIQLLexer)
 *  -  void      pIQLLexer->TK_SUM(pIQLLexer)
 *  -  void      pIQLLexer->TK_SWITCH(pIQLLexer)
 *  -  void      pIQLLexer->TK_THEN(pIQLLexer)
 *  -  void      pIQLLexer->TK_TINYINT(pIQLLexer)
 *  -  void      pIQLLexer->TK_TRUE(pIQLLexer)
 *  -  void      pIQLLexer->TK_UNION(pIQLLexer)
 *  -  void      pIQLLexer->TK_VARCHAR(pIQLLexer)
 *  -  void      pIQLLexer->TK_WHEN(pIQLLexer)
 *  -  void      pIQLLexer->TK_WHERE(pIQLLexer)
 *  -  void      pIQLLexer->TK_WHILE(pIQLLexer)
 *  -  void      pIQLLexer->TK_WITH(pIQLLexer)
 *  -  void      pIQLLexer->STRING_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->WSTRING_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->DOUBLE_QUOTED_STRING_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->ESCAPE_SEQUENCE(pIQLLexer)
 *  -  void      pIQLLexer->OCTAL_ESCAPE(pIQLLexer)
 *  -  void      pIQLLexer->HEX_DIGIT(pIQLLexer)
 *  -  void      pIQLLexer->UNICODE_ESCAPE(pIQLLexer)
 *  -  void      pIQLLexer->WS(pIQLLexer)
 *  -  void      pIQLLexer->ML_COMMENT(pIQLLexer)
 *  -  void      pIQLLexer->SL_COMMENT(pIQLLexer)
 *  -  void      pIQLLexer->ID(pIQLLexer)
 *  -  void      pIQLLexer->QUOTED_ID(pIQLLexer)
 *  -  void      pIQLLexer->BIGINT_SUFFIX(pIQLLexer)
 *  -  void      pIQLLexer->THREE_DIGIT_NUMBER(pIQLLexer)
 *  -  void      pIQLLexer->FOUR_DIGIT_HEX(pIQLLexer)
 *  -  void      pIQLLexer->HEX_INTEGER_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->DECIMAL_INTEGER_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->DECIMAL_BIGINT_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->FLOATING_POINT_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->IPV4_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->IPV6_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->EXPONENT(pIQLLexer)
 *  -  void      pIQLLexer->FLOAT_SUFFIX(pIQLLexer)
 *  -  void      pIQLLexer->DECIMAL_LITERAL(pIQLLexer)
 *  -  void      pIQLLexer->Tokens(pIQLLexer)
 *
 * The return type for any particular rule is of course determined by the source
 * grammar file.
 */
// [The "BSD licence"]
// Copyright (c) 2005-2009 Jim Idle, Temporal Wave LLC
// http://www.temporal-wave.com
// http://www.linkedin.com/in/jimidle
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef	_IQLLexer_H
#define _IQLLexer_H
/* =============================================================================
 * Standard antlr3 C runtime definitions
 */
#include    <antlr3.h>

/* End of standard antlr 3 runtime definitions
 * =============================================================================
 */
 
#ifdef __cplusplus
extern "C" {
#endif

// Forward declare the context typedef so that we can use it before it is
// properly defined. Delegators and delegates (from import statements) are
// interdependent and their context structures contain pointers to each other
// C only allows such things to be declared if you pre-declare the typedef.
//
typedef struct IQLLexer_Ctx_struct IQLLexer, * pIQLLexer;



#ifdef	ANTLR3_WINDOWS
// Disable: Unreferenced parameter,							- Rules with parameters that are not used
//          constant conditional,							- ANTLR realizes that a prediction is always true (synpred usually)
//          initialized but unused variable					- tree rewrite variables declared but not needed
//          Unreferenced local variable						- lexer rule declares but does not always use _type
//          potentially unitialized variable used			- retval always returned from a rule 
//			unreferenced local function has been removed	- susually getTokenNames or freeScope, they can go without warnigns
//
// These are only really displayed at warning level /W4 but that is the code ideal I am aiming at
// and the codegen must generate some of these warnings by necessity, apart from 4100, which is
// usually generated when a parser rule is given a parameter that it does not use. Mostly though
// this is a matter of orthogonality hence I disable that one.
//
#pragma warning( disable : 4100 )
#pragma warning( disable : 4101 )
#pragma warning( disable : 4127 )
#pragma warning( disable : 4189 )
#pragma warning( disable : 4505 )
#pragma warning( disable : 4701 )
#endif

/* ========================
 * BACKTRACKING IS ENABLED
 * ========================
 */

/** Context tracking structure for IQLLexer
 */
struct IQLLexer_Ctx_struct
{
    /** Built in ANTLR3 context tracker contains all the generic elements
     *  required for context tracking.
     */
    pANTLR3_LEXER    pLexer;


     void (*mT__120)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__121)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__122)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__123)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__124)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__125)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__126)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__127)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__128)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__129)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__130)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__131)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__132)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__133)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__134)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__135)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__136)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__137)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__138)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__139)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__140)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__141)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__142)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__143)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__144)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__145)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__146)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__147)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mT__148)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ADD)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ALL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ALTER)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_AND)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ANY)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ARRAY)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_AS)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ASC)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_AVG)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_BEGIN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_BETWEEN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_BIGINT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_BOOLEAN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_BREAK)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_BY)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CASE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CAST)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CHAR)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CIDRV4)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CIDRV6)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_COALESCE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CONTINUE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_COUNT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CREATE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_CROSS)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_DATETIME)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_DECLARE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_DECIMAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_DESC)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_DISTINCT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_DOUBLE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ELSE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_END)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_EXISTS)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_FALSE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_FROM)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_FULL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_FUNCTION)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_GROUP)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_HAVING)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_IF)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_IN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_INDEX)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_INNER)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_INTO)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_INTEGER)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_INTERVAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_IPV4)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_IPV6)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_IS)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_JOIN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_KEY)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_LEFT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_LIKE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_MAX)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_MIN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_NOT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_NULL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_NVARCHAR)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ON)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_OR)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ORDER)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_OUTER)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_OUTPUT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_PRECISION)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_PRINT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_PROCEDURE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_RAISERROR)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_REAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_RETURN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_RETURNS)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_RIGHT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_RLIKE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_ROW)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_SELECT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_SET)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_SMALLINT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_SOME)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_SUM)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_SWITCH)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_THEN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_TINYINT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_TRUE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_UNION)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_VARCHAR)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_WHEN)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_WHERE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_WHILE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTK_WITH)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mSTRING_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mWSTRING_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mDOUBLE_QUOTED_STRING_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mESCAPE_SEQUENCE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mOCTAL_ESCAPE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mHEX_DIGIT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mUNICODE_ESCAPE)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mWS)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mML_COMMENT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mSL_COMMENT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mID)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mQUOTED_ID)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mBIGINT_SUFFIX)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTHREE_DIGIT_NUMBER)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mFOUR_DIGIT_HEX)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mHEX_INTEGER_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mDECIMAL_INTEGER_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mDECIMAL_BIGINT_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mFLOATING_POINT_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mIPV4_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mIPV6_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mEXPONENT)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mFLOAT_SUFFIX)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mDECIMAL_LITERAL)	(struct IQLLexer_Ctx_struct * ctx);
     void (*mTokens)	(struct IQLLexer_Ctx_struct * ctx);    const char * (*getGrammarFileName)();
    void	    (*free)   (struct IQLLexer_Ctx_struct * ctx);
        
};

// Function protoypes for the constructor functions that external translation units
// such as delegators and delegates may wish to call.
//
ANTLR3_API pIQLLexer IQLLexerNew         (pANTLR3_INPUT_STREAM instream);
ANTLR3_API pIQLLexer IQLLexerNewSSD      (pANTLR3_INPUT_STREAM instream, pANTLR3_RECOGNIZER_SHARED_STATE state);

/** Symbolic definitions of all the tokens that the lexer will work with.
 * \{
 *
 * Antlr will define EOF, but we can't use that as it it is too common in
 * in C header files and that would be confusing. There is no way to filter this out at the moment
 * so we just undef it here for now. That isn't the value we get back from C recognizers
 * anyway. We are looking for ANTLR3_TOKEN_EOF.
 */
#ifdef	EOF
#undef	EOF
#endif
#ifdef	Tokens
#undef	Tokens
#endif 
#define T__144      144
#define T__143      143
#define T__146      146
#define T__145      145
#define T__140      140
#define TK_DECIMAL      24
#define TK_ALTER      70
#define T__142      142
#define T__141      141
#define TK_PRECISION      20
#define TK_DISTINCT      81
#define TK_THEN      55
#define HEX_INTEGER_LITERAL      59
#define TK_CREATE      78
#define TK_CROSS      79
#define TK_INTERVAL      56
#define TK_BIGINT      27
#define TK_GROUP      86
#define TK_JOIN      92
#define TK_WHILE      45
#define TK_CASE      41
#define DATETIME_LITERAL      6
#define ID      8
#define T__137      137
#define TK_BOOLEAN      25
#define T__136      136
#define TK_ADD      68
#define TK_ALL      69
#define T__139      139
#define T__138      138
#define T__133      133
#define T__132      132
#define IPV6_LITERAL      64
#define T__135      135
#define TK_SMALLINT      28
#define T__134      134
#define T__131      131
#define TK_IF      43
#define T__130      130
#define TK_IPV4      31
#define TK_LEFT      94
#define TK_IPV6      33
#define TK_RIGHT      101
#define TK_BREAK      15
#define TK_ASC      72
#define TK_IN      88
#define TK_IS      48
#define TK_RLIKE      50
#define T__129      129
#define TK_AS      12
#define UNICODE_ESCAPE      109
#define T__126      126
#define T__125      125
#define T__128      128
#define T__127      127
#define TK_TINYINT      29
#define TK_RETURN      14
#define TK_COALESCE      76
#define TK_FUNCTION      85
#define TK_OUTPUT      98
#define TK_AND      47
#define TK_AVG      73
#define TK_NOT      36
#define TK_FALSE      66
#define WSTRING_LITERAL      62
#define TK_SOME      104
#define TK_ROW      102
#define TK_END      40
#define CASE_NO_ELSE      4
#define TK_ARRAY      67
#define TK_INTEGER      18
#define TK_COUNT      77
#define TK_OUTER      97
#define TK_ON      95
#define TK_WITH      107
#define FLOATING_POINT_LITERAL      60
#define TK_OR      46
#define TK_INDEX      89
#define SL_COMMENT      114
#define EXPONENT      118
#define T__148      148
#define T__147      147
#define DECIMAL_LITERAL      61
#define TK_REAL      30
#define DECIMAL_BIGINT_LITERAL      7
#define TK_CIDRV6      34
#define TK_CIDRV4      32
#define TK_VARCHAR      22
#define ESCAPE_SEQUENCE      108
#define TK_FROM      83
#define TK_ORDER      96
#define DOUBLE_QUOTED_STRING_LITERAL      10
#define TK_SWITCH      38
#define TK_HAVING      87
#define TK_ANY      71
#define TK_RETURNS      100
#define TK_WHERE      106
#define TK_UNION      105
#define TK_CONTINUE      16
#define TK_NULL      35
#define TK_SELECT      103
#define TK_EXISTS      82
#define THREE_DIGIT_NUMBER      116
#define TK_FULL      84
#define TK_BETWEEN      74
#define TK_PROCEDURE      99
#define IPV4_LITERAL      63
#define TK_DESC      80
#define TK_LIKE      49
#define TK_TRUE      65
#define T__122      122
#define TK_DATETIME      26
#define T__121      121
#define T__124      124
#define T__123      123
#define DECIMAL_INTEGER_LITERAL      9
#define TK_PRINT      42
#define T__120      120
#define TK_SUM      51
#define TK_INTO      91
#define OCTAL_ESCAPE      110
#define BIGINT_SUFFIX      115
#define TK_MIN      53
#define TK_KEY      93
#define TK_MAX      52
#define HEX_DIGIT      111
#define TK_BY      75
#define WS      112
#define EOF      -1
#define FOUR_DIGIT_HEX      117
#define TK_SET      37
#define TK_NVARCHAR      23
#define TK_ELSE      44
#define TK_CAST      57
#define TK_WHEN      54
#define QUOTED_ID      13
#define TK_DECLARE      11
#define LITERAL_CAST      5
#define FLOAT_SUFFIX      119
#define TK_BEGIN      39
#define TK_INNER      90
#define ML_COMMENT      113
#define TK_RAISERROR      17
#define STRING_LITERAL      58
#define TK_DOUBLE      19
#define TK_CHAR      21
#ifdef	EOF
#undef	EOF
#define	EOF	ANTLR3_TOKEN_EOF
#endif

#ifndef TOKENSOURCE
#define TOKENSOURCE(lxr) lxr->pLexer->rec->state->tokSource
#endif

/* End of token definitions for IQLLexer
 * =============================================================================
 */
/** \} */

#ifdef __cplusplus
}
#endif

#endif

/* END - Note:Keep extra line feed to satisfy UNIX systems */
