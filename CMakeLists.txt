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

cmake_minimum_required (VERSION 3.2)

project (IQL)

# We have some of our own CMake Find Modules
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

######################################################
######################################################
# Packages and libraries 
######################################################
######################################################

#
# ANTLR
#
find_package(ANTLR REQUIRED)
include_directories ( ${ANTLR_INCLUDE_DIR} )

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

#
# Boost
#
find_package( Boost 1.74 COMPONENTS date_time filesystem iostreams program_options regex serialization system timer unit_test_framework REQUIRED )
include_directories ( ${Boost_INCLUDE_DIR} )

#
# Hadoop 
#
find_package(HADOOP) 
if (HADOOP_FOUND)
   include_directories(${HADOOP_INCLUDE_DIRS})

   #
   # JNI 
   # 
   # JNI is only required if we are using HDFS.  If we make
   # Hadoop integration optional, then we should make this
   # optional as well.
   # If you need to point this to a know Java install,
   # just set JAVA_HOME in your environment.
   find_package(JNI)
   # Note that FindJNI doesn't properly set JNI_FOUND or honor
   # REQUIRED
   if (JNI_FOUND)
      include_directories ( ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} )
   endif (JNI_FOUND)
   MESSAGE (STATUS "Found JNI header: ${JAVA_INCLUDE_PATH}")
   MESSAGE (STATUS "Found JVM shared library: ${JAVA_JVM_LIBRARY}")
endif (HADOOP_FOUND)

#
# LLVM
#
find_package(LLVM REQUIRED)

# What version of LLVM?  We only support LLVM 13.0
execute_process(
COMMAND ${LLVM_CONFIG} --version
OUTPUT_VARIABLE LLVM_VERSION
OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (NOT ${LLVM_VERSION} VERSION_EQUAL "15.0.7")
  MESSAGE(FATAL_ERROR "LLVM Version is ${LLVM_VERSION}; only 15.0.7 is supported")
endif (NOT ${LLVM_VERSION} VERSION_EQUAL "15.0.7")


# Definitions needed by LLVM headers
add_definitions(
    -D__STDC_LIMIT_MACROS
    -D__STDC_CONSTANT_MACROS
    -D_GNU_SOURCE)

# Now set the header and library paths:
include_directories( ${LLVM_INCLUDE_DIR} )
link_directories( ${LLVM_LIBRARY_DIR} )

# What we need for LLVM native jit
execute_process(
COMMAND ${LLVM_CONFIG} --libs analysis core bitreader bitwriter executionengine instcombine object orcjit runtimedyld scalaropts support native 
OUTPUT_VARIABLE LLVM_JIT_LIBS
OUTPUT_STRIP_TRAILING_WHITESPACE
)

#
# uriparser
#
find_package(URIPARSER REQUIRED)
include_directories ( ${URIPARSER_INCLUDE_DIRS} )

#
# zlib
#
find_package(ZLIB REQUIRED)
include_directories ( ${ZLIB_INCLUDE_DIRS} )

#
# Miscellaneous libraries builtin to Linux
#
find_library(LIB_DL dl)
find_library(LIB_PTHREAD pthread)
find_library(LIB_RT rt)
find_library(LIB_NCURSES ncurses)
find_library(LIB_TINFO tinfo)

#
# Setup CXX14 flag
#
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++14" COMPILER_SUPPORTS_CXX14)
if(COMPILER_SUPPORTS_CXX14)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++14 support. Please use a different C++ compiler.")
endif()

######################################################
######################################################
# Subdirectories of the project
######################################################
######################################################
include_directories ( "${PROJECT_SOURCE_DIR}/decNumber" )
include_directories ( "${PROJECT_SOURCE_DIR}/ads-ql" )
include_directories ( "${PROJECT_SOURCE_DIR}/ads-df" )

add_subdirectory ( decNumber )
add_subdirectory ( ads-ql )
add_subdirectory ( ads-ql/test )
add_subdirectory ( ads-df )
add_subdirectory ( ads-df/test )

######################################################
######################################################
# Unit testing support
######################################################
######################################################
enable_testing()
# It doesn't make too much sense for this to be here rather than
# in the ads-ql & ads-df subdirectories, but CMake doesn't seem 
# to be generating
# CTestTesting.cmake files in subdirectories (sigh)...
add_test(ads-ql-unit-test ${CMAKE_CURRENT_BINARY_DIR}/ads-ql/test/ads-ql-test)
add_test(ads-df-unit-test ${CMAKE_CURRENT_BINARY_DIR}/ads-df/test/ads-df-test)
add_test(ads-df-parser-test ${CMAKE_CURRENT_BINARY_DIR}/ads-df/test/parser-test)
add_test(http-operator-test ${CMAKE_CURRENT_BINARY_DIR}/ads-df/test/http-operator-test)
add_test(http-parser-test ${CMAKE_CURRENT_BINARY_DIR}/ads-df/test/http-parser-test)
add_test(ads-df-core-scripts perl ${CMAKE_CURRENT_SOURCE_DIR}/ads-df/test/testDriver.pl ${CMAKE_CURRENT_SOURCE_DIR}/ads-df/test ${CMAKE_CURRENT_BINARY_DIR}/ads-df/ads-df)

