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

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set (EXTRA_LIBS ${EXTRA_LIBS} ${LIB_RT})
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

if (HADOOP_FOUND) 
   set (EXTRA_SOURCE ${EXTRA_SOURCE} HdfsOperator.cc MapReduceJob.cc)
   set (EXTRA_LIBS ${EXTRA_LIBS} ${JAVA_JVM_LIBRARY} ${HADOOP_LIBRARIES})
   add_definitions(-DTRECUL_HAS_HADOOP)
endif (HADOOP_FOUND)

add_library(ads-df 
http_parser.c
AsyncRecordParser.cc 
CompileTimeLogicalOperator.cc 
ConstantScan.cc 
DataflowRuntime.cc 
FileService.cc 
FileSystem.cc 
FileWriteOperator.cc 
GraphBuilder.cc 
GzipOperator.cc 
HttpOperator.cc 
LogicalOperator.cc 
Merger.cc 
QueryStringOperator.cc 
QueueImport.cc 
RecordParser.cc 
RuntimeOperator.cc 
RuntimePlan.cc 
RuntimeProcess.cc 
StreamBufferBlock.cc
TableMetadata.cc
TableOperator.cc
TcpOperator.cc
${EXTRA_SOURCE}
)

target_link_libraries( ads-df ads-ql ${ZLIB_LIBRARIES} ${URIPARSER_LIBRARIES} ${Boost_FILESYSTEM_LIBRARY} ${Boost_IOSTREAMS_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_SYSTEM_LIBRARY} ${EXTRA_LIBS} )

add_executable( ads-df-exe ads-df.cc )
set_target_properties( ads-df-exe PROPERTIES OUTPUT_NAME "ads-df")
target_link_libraries( ads-df-exe ads-df )

if (HADOOP_FOUND)
   add_executable( ads-df-pipes pipes.cc )
   target_link_libraries( ads-df-pipes ads-df )
endif (HADOOP_FOUND)


