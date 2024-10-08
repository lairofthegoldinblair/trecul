#
# Copyright (c) 2012, Akamai Technologies
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
#   Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
# 
#   Redistributions in binary form must reproduce the above
#   copyright notice, this list of conditions and the following
#   disclaimer in the documentation and/or other materials provided
#   with the distribution.
# 
#   Neither the name of the Akamai Technologies nor the names of its
#   contributors may be used to endorse or promote products derived
#   from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
#

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set (EXTRA_LIBS ${EXTRA_LIBS} ${LIB_NCURSES})
else(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  if(LIB_TINFO)
    set (EXTRA_LIBS ${EXTRA_LIBS} ${LIB_TINFO})
  endif(LIB_TINFO)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

add_library(ads-ql CodeGenerationContext.cc GetVariablesPass.cc  IQLInterpreter.cc	LLVMGen.cc  RecordType.cc  TypeCheckContext.cc IQLAnalyze.c	IQLExpression.cc   IQLLexer.c	IQLToLLVM.c md5.c IQLGetVariables.c  IQLParser.c	IQLTypeCheck.c	SuperFastHash.c
)

target_link_libraries( ads-ql ${Boost_DATE_TIME_LIBRARY} ${Boost_REGEX_LIBRARY} ${Boost_SERIALIZATION_LIBRARY} ${LLVM_JIT_LIBS} ${ANTLR_LIBRARIES} ${LIB_PTHREAD} ${LIB_DL} ${ZLIB_LIBRARIES} ${EXTRA_LIBS} decNumber )


