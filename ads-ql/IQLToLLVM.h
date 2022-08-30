/** \file
 *  This C header file was generated by $ANTLR version 3.2 Sep 23, 2009 12:02:23
 *
 *     -  From the grammar source file : IQLToLLVM.g
 *     -                            On : 2022-09-02 16:50:12
 *     -           for the tree parser : IQLToLLVMTreeParser *
 * Editing it, at least manually, is not wise. 
 *
 * C language generator and runtime by Jim Idle, jimi|hereisanat|idle|dotgoeshere|ws.
 *
 *
 * The tree parser IQLToLLVM has the callable functions (rules) shown below,
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
 * a parser context typedef pIQLToLLVM, which is returned from a call to IQLToLLVMNew().
 *
 * The methods in pIQLToLLVM are  as follows:
 *
 *  - pANTLR3_STRING      pIQLToLLVM->localVarOrId(pIQLToLLVM)
 *  - void      pIQLToLLVM->statement(pIQLToLLVM)
 *  - void      pIQLToLLVM->variableDeclaration(pIQLToLLVM)
 *  - void      pIQLToLLVM->setStatement(pIQLToLLVM)
 *  - IQLToLLVMLValueRef      pIQLToLLVM->variableReference(pIQLToLLVM)
 *  - void      pIQLToLLVM->switchStatement(pIQLToLLVM)
 *  - void      pIQLToLLVM->switchBlock(pIQLToLLVM)
 *  - void      pIQLToLLVM->printStatement(pIQLToLLVM)
 *  - void      pIQLToLLVM->ifStatement(pIQLToLLVM)
 *  - void      pIQLToLLVM->statementBlock(pIQLToLLVM)
 *  - void      pIQLToLLVM->whileStatement(pIQLToLLVM)
 *  - void      pIQLToLLVM->singleExpression(pIQLToLLVM)
 *  - void      pIQLToLLVM->declareStatement(pIQLToLLVM)
 *  - void      pIQLToLLVM->returnExpression(pIQLToLLVM)
 *  - void      pIQLToLLVM->recordConstructor(pIQLToLLVM)
 *  - void      pIQLToLLVM->fieldConstructor(pIQLToLLVM)
 *  - void *      pIQLToLLVM->builtInType(pIQLToLLVM)
 *  - void      pIQLToLLVM->arrayTypeSpec(pIQLToLLVM)
 *  - void      pIQLToLLVM->typeNullability(pIQLToLLVM)
 *  - IQLToLLVM_expression_return      pIQLToLLVM->expression(pIQLToLLVM)
 *  - void      pIQLToLLVM->whenExpression(pIQLToLLVM)
 *  - void      pIQLToLLVM->elseExpression(pIQLToLLVM)
 *  - void      pIQLToLLVM->expressionNoGenerate(pIQLToLLVM)
 *  - void      pIQLToLLVM->whenExpressionNoGenerate(pIQLToLLVM)
 *  - void      pIQLToLLVM->elseExpressionNoGenerate(pIQLToLLVM)
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

#ifndef	_IQLToLLVM_H
#define _IQLToLLVM_H
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
typedef struct IQLToLLVM_Ctx_struct IQLToLLVM, * pIQLToLLVM;



#include "LLVMGen.h"


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
typedef struct IQLToLLVM_expression_return_struct
{
    pANTLR3_BASE_TREE       start;
    pANTLR3_BASE_TREE       stop;   
    IQLToLLVMValueRef llvmVal;
    int32_t dummy;
}
    IQLToLLVM_expression_return;



/** Context tracking structure for IQLToLLVM
 */
struct IQLToLLVM_Ctx_struct
{
    /** Built in ANTLR3 context tracker contains all the generic elements
     *  required for context tracking.
     */
    pANTLR3_TREE_PARSER	    pTreeParser;


     pANTLR3_STRING (*localVarOrId)	(struct IQLToLLVM_Ctx_struct * ctx);
     void (*statement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*variableDeclaration)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*setStatement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     IQLToLLVMLValueRef (*variableReference)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*switchStatement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*switchBlock)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*printStatement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*ifStatement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*statementBlock)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*whileStatement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*singleExpression)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*declareStatement)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*returnExpression)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*recordConstructor)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*fieldConstructor)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt, int32_t * fieldPos);
     void * (*builtInType)	(struct IQLToLLVM_Ctx_struct * ctx);
     void (*arrayTypeSpec)	(struct IQLToLLVM_Ctx_struct * ctx);
     void (*typeNullability)	(struct IQLToLLVM_Ctx_struct * ctx);
     IQLToLLVM_expression_return (*expression)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt);
     void (*whenExpression)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt, void * attr);
     void (*elseExpression)	(struct IQLToLLVM_Ctx_struct * ctx, IQLCodeGenerationContextRef ctxt, void * attr);
     void (*expressionNoGenerate)	(struct IQLToLLVM_Ctx_struct * ctx);
     void (*whenExpressionNoGenerate)	(struct IQLToLLVM_Ctx_struct * ctx);
     void (*elseExpressionNoGenerate)	(struct IQLToLLVM_Ctx_struct * ctx);
    // Delegated rules
    const char * (*getGrammarFileName)();
    void	    (*free)   (struct IQLToLLVM_Ctx_struct * ctx);
        
};

// Function protoypes for the constructor functions that external translation units
// such as delegators and delegates may wish to call.
//
ANTLR3_API pIQLToLLVM IQLToLLVMNew         (pANTLR3_COMMON_TREE_NODE_STREAM instream);
ANTLR3_API pIQLToLLVM IQLToLLVMNewSSD      (pANTLR3_COMMON_TREE_NODE_STREAM instream, pANTLR3_RECOGNIZER_SHARED_STATE state);

/** Symbolic definitions of all the tokens that the tree parser will work with.
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
#define TK_ALTER      72
#define T__142      142
#define T__141      141
#define TK_PRECISION      20
#define TK_DISTINCT      83
#define TK_THEN      56
#define HEX_INTEGER_LITERAL      60
#define TK_CREATE      80
#define TK_CROSS      81
#define TK_INTERVAL      57
#define TK_BIGINT      27
#define TK_GROUP      88
#define TK_JOIN      94
#define TK_WHILE      46
#define TK_CASE      42
#define DATETIME_LITERAL      6
#define ID      8
#define T__137      137
#define TK_BOOLEAN      25
#define T__136      136
#define TK_ADD      70
#define TK_ALL      71
#define T__139      139
#define T__138      138
#define T__133      133
#define T__132      132
#define IPV6_LITERAL      65
#define T__135      135
#define TK_SMALLINT      28
#define T__134      134
#define T__131      131
#define TK_IF      44
#define T__130      130
#define TK_IPV4      31
#define TK_LEFT      96
#define TK_IPV6      33
#define TK_RIGHT      103
#define TK_BREAK      15
#define TK_ASC      74
#define TK_IN      90
#define TK_IS      49
#define TK_RLIKE      51
#define T__129      129
#define TK_AS      12
#define UNICODE_ESCAPE      110
#define T__126      126
#define T__125      125
#define T__128      128
#define T__127      127
#define TK_TINYINT      29
#define TK_RETURN      14
#define TK_COALESCE      78
#define TK_FUNCTION      87
#define TK_OUTPUT      100
#define TK_AND      48
#define TK_AVG      75
#define TK_NOT      37
#define TK_FALSE      67
#define WSTRING_LITERAL      63
#define TK_SOME      105
#define TK_ROW      69
#define TK_END      41
#define CASE_NO_ELSE      4
#define TK_ARRAY      68
#define TK_INTEGER      18
#define TK_COUNT      79
#define TK_OUTER      99
#define TK_ON      97
#define TK_WITH      108
#define FLOATING_POINT_LITERAL      61
#define TK_OR      47
#define TK_INDEX      91
#define SL_COMMENT      115
#define EXPONENT      119
#define T__148      148
#define T__147      147
#define T__149      149
#define DECIMAL_LITERAL      62
#define TK_REAL      30
#define DECIMAL_BIGINT_LITERAL      7
#define TK_CIDRV6      34
#define TK_CIDRV4      32
#define TK_VARCHAR      22
#define ESCAPE_SEQUENCE      109
#define TK_FROM      85
#define TK_ORDER      98
#define DOUBLE_QUOTED_STRING_LITERAL      10
#define TK_SWITCH      39
#define TK_HAVING      89
#define TK_ANY      73
#define TK_RETURNS      102
#define TK_WHERE      107
#define TK_UNION      106
#define TK_CONTINUE      16
#define TK_NULL      36
#define TK_SELECT      104
#define TK_EXISTS      84
#define THREE_DIGIT_NUMBER      117
#define TK_FULL      86
#define TK_BETWEEN      76
#define TK_PROCEDURE      101
#define IPV4_LITERAL      64
#define TK_DESC      82
#define TK_LIKE      50
#define TK_TRUE      66
#define T__122      122
#define TK_DATETIME      26
#define T__121      121
#define T__124      124
#define T__123      123
#define DECIMAL_INTEGER_LITERAL      9
#define TK_PRINT      43
#define TK_SUM      52
#define TK_INTO      93
#define OCTAL_ESCAPE      111
#define BIGINT_SUFFIX      116
#define TK_MIN      54
#define TK_DECLTYPE      35
#define TK_KEY      95
#define TK_MAX      53
#define HEX_DIGIT      112
#define TK_BY      77
#define WS      113
#define EOF      -1
#define FOUR_DIGIT_HEX      118
#define TK_SET      38
#define TK_NVARCHAR      23
#define TK_ELSE      45
#define TK_CAST      58
#define TK_WHEN      55
#define QUOTED_ID      13
#define TK_DECLARE      11
#define LITERAL_CAST      5
#define FLOAT_SUFFIX      120
#define TK_BEGIN      40
#define TK_INNER      92
#define ML_COMMENT      114
#define TK_RAISERROR      17
#define STRING_LITERAL      59
#define TK_DOUBLE      19
#define TK_CHAR      21
#ifdef	EOF
#undef	EOF
#define	EOF	ANTLR3_TOKEN_EOF
#endif

#ifndef TOKENSOURCE
#define TOKENSOURCE(lxr) lxr->pLexer->rec->state->tokSource
#endif

/* End of token definitions for IQLToLLVM
 * =============================================================================
 */
/** \} */

#ifdef __cplusplus
}
#endif

#endif

/* END - Note:Keep extra line feed to satisfy UNIX systems */
