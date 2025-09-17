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

#define BOOST_PROCESS_USE_STD_FS

#include <cmath>
#include <fstream>
#include <iostream>
#include <thread>

#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zstd.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/timer/timer.hpp>
#include <boost/process/io.hpp>
#include <boost/process/system.hpp>

#include "CodeGenerationContext.hh"
#include "IQLInterpreter.hh"
#include "RecordParser.hh"
#include "DataflowRuntime.hh"
#include "RuntimeOperator.hh"
#include "RuntimeProcess.hh"
#include "RuntimePlan.hh"
#include "QueueImport.hh"
#include "SuperFastHash.h"
#include "LoserTree.hh"
#include "AsynchronousFileSystem.hh"
#include "Merger.hh"
#include "GraphBuilder.hh"
#include "ZLib.hh"
#include "Zstd.hh"
#include "FileWriteOperator.hh"
#include "TableMetadata.hh"
#include "TableOperator.hh"

#define BOOST_TEST_MODULE MyTest
#include <boost/test/data/test_case.hpp>
#include <boost/test/data/monomorphic.hpp>
#include <boost/test/unit_test.hpp>

class TestStruct {
public:
  TestStruct * mNext;
  char mPayload[96];
  TestStruct()
    :
    mNext(NULL)
  {
  }
};

class ConcurrentFifoProducerConsumer
{
private:
  ConcurrentBlockingFifo<int32_t> mFifo;
public:
  ConcurrentFifoProducerConsumer() 
    :
    mFifo(1400)
  {
  }

  void doProducer()
  {
    for(int32_t i=0; i<100000; i++) {
      mFifo.push(i);
    }
  }
  void doConsumer()
  {
    for(int32_t i=0; i<100000; i++) {
      int32_t ret = mFifo.pop();
      BOOST_CHECK_EQUAL(i, ret);
    }
  }
};

BOOST_AUTO_TEST_CASE(testImportFromStringDataBlock)
{
  std::cout << "testImportFromStringDataBlock" << std::endl;
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt)));
  const RecordType * recordType = RecordType::get(ctxt, members);
  std::vector<FieldImporter > importers;
  FieldImporter::createDefaultImport(recordType,
				     recordType,
				     '\t',
				     0,
				     '\\',
				     importers);
  {
    std::string data("82344	String data	THIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("String data", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344		");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
}

BOOST_AUTO_TEST_CASE(testImportFromStringDataBlockNullable)
{
  std::cout << "testImportFromStringDataBlockNullable" << std::endl;
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt, true)));
  const RecordType * recordType = RecordType::get(ctxt, members);
  std::vector<FieldImporter > importers;
  FieldImporter::createDefaultImport(recordType,
				     recordType,
				     '\t',
				     0,
				     '\\',
				     importers);
  {
    std::string data("82344	String data	THIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("String data", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    // This is an important subtle test case: a file that ends with
    // an empty string in a nullable field.
    std::string data("82344		");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
}

BOOST_AUTO_TEST_CASE(testImportFromStringDataBlockWithEscapes)
{
  std::cout << "testImportFromStringDataBlock" << std::endl;
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt)));
  const RecordType * recordType = RecordType::get(ctxt, members);
  std::vector<FieldImporter > importers;
  FieldImporter::createDefaultImport(recordType,
				     recordType,
				     '\t',
				     0,
				     '\\',
				     importers);
  std::vector<FieldImporter > percentImporters;
  FieldImporter::createDefaultImport(recordType,
				     recordType,
				     '\t',
				     0,
				     '%',
				     percentImporters);
  {
    std::string data("82344\tString data\\twith escaped tab\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("String data\twith escaped tab", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\t\\nLeading escaped newline\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("\nLeading escaped newline", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tHas escaped\\\\ escape char\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("Has escaped\\ escape char", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  const char * inputTokens [] = {"0", "1", "\\f", "2", "3", "\\b", "4", "5", 
				 "6", "7", "\\r", "8", "\\t", "\\n", "9"};
  const char * outputTokens [] = {"0", "1", "\f", "2", "3", "\b", "4", "5", 
				  "6", "7", "\r", "8", "\t", "\n", "9"};
  for(int32_t i=1; i<512; ++i) {
    std::stringstream data;
    data << "82344\tImport different sizes\\n";
    std::stringstream expected;
    expected << "Import different sizes\n";
    for(int32_t j=0; j<i; ++j) {
      data << inputTokens[j % 15];
      expected << outputTokens[j % 15];
    }
    data << "\tTHIRD";
    std::string tmp = data.str();
    StringDataBlock blk;
    blk.bindString(tmp);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals(expected.str(), 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\t\\nLeading escaped newline this is a loooooo"
		     "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
		     "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
		     "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
		     "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
		     "ooooooooooooooooooooooooooooooooooooooooooooong string\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("\nLeading escaped newline this is a loooooo"
					 "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
					 "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
					 "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
					 "oooooooooooooooooooooooooooooooooooooooooooooooooooooo"
					 "ooooooooooooooooooooooooooooooooooooooooooooong string",
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD",
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    // try a string that has a escaped char right over a 64-byte boundary
    std::string data("82344\ti-mobileidea3b(MRE\\[.3.00(20480) resolution\\]20480 chipset\\[T6255 touch\\] tpannel\\( camera\\)\tTHIRD");

    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("i-mobileidea3b(MRE\\[.3.00(20480) resolution\\]20480 chipset\\[T6255 touch\\] tpannel\\( camera\\)",
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD",
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
    
  }
  {
    std::string data("82344\tInvalid escaped\\achar\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    std::vector<FieldImporter >::iterator it = importers.begin();
    BOOST_CHECK(it->Import(blk, buf));
    ++it;
    BOOST_CHECK(it->Import(blk, buf));
    ++it;
    BOOST_CHECK(!it->Import(blk, buf));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tEscaped paren\\( and brace\\} string\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
 	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("Escaped paren\\( and brace\\} string",
 					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD",
 					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tEscaped CR and NL\\r\\nBack to back\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
 	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("Escaped CR and NL\r\nBack to back",
 					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD",
 					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tTrailing incomplete escape char\\\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = importers.begin();
 	it != importers.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("Trailing incomplete escape char\\",
 					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD",
 					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tString data%twith escaped tab and escaped non-control %a\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = percentImporters.begin();
	it != percentImporters.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("String data\twith escaped tab and escaped non-control %a", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tInvalid escape sequence %\a\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    std::vector<FieldImporter >::iterator it = percentImporters.begin();
    BOOST_CHECK(it->Import(blk, buf));
    ++it;
    BOOST_CHECK(it->Import(blk, buf));
    ++it;
    BOOST_CHECK(!it->Import(blk, buf));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tHas escaped%% escape char\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = percentImporters.begin();
	it != percentImporters.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("Has escaped% escape char", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
  {
    std::string data("82344\tHas escaped%( printable char\tTHIRD");
    StringDataBlock blk;
    blk.bindString(data);
    RecordBuffer buf = recordType->getMalloc().malloc();
    for(std::vector<FieldImporter >::iterator it = percentImporters.begin();
	it != percentImporters.end(); ++it) {
      BOOST_CHECK(it->Import(blk, buf));
    }
    BOOST_CHECK_EQUAL(82344, recordType->getFieldAddress("a").getInt32(buf));
    BOOST_CHECK(boost::algorithm::equals("Has escaped%( printable char", 
					 recordType->getFieldAddress("b").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK(boost::algorithm::equals("THIRD", 
					 recordType->getFieldAddress("c").getVarcharPtr(buf)->c_str()));
    recordType->getFree().free(buf);
  }
}

BOOST_AUTO_TEST_CASE(testInputQueueOperator)
{
  std::cout << "testInputQueueOperator" << std::endl;
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  const RecordType * recordType = RecordType::get(ctxt, members);
  DataflowGraphBuilder gb(ctxt);
  gb.buildGraph("a = reduce[format=\"a INTEGER, b INTEGER\"];\n"
		"b = print[numToPrint=10];\n"
		"c = devNull[];\n"
		"a -> b;\n"
		"b -> c;\n"
		);
  std::shared_ptr<RuntimeOperatorPlan> plan = gb.create(1);
  RuntimeProcess p;
  p.init(0,0,1,*plan.get());
  std::vector<NativeInputQueueOperator*> ops;
  p.getOperatorOfType<>(ops);
  BOOST_CHECK_EQUAL(1U, ops.size());
  p.runInit();
  bool ret = p.runSome();
  BOOST_CHECK(!ret);
  BOOST_CHECK_EQUAL(0U, ops.back()->getQueue().size());
  for(int32_t i=0; i<10; i++) {
    RecordBuffer buf = recordType->getMalloc().malloc();
    recordType->setInt32("a", i, buf);
    recordType->setInt32("b", i, buf);
    ops.back()->getQueue().push(buf);
  }
  ops.back()->onEvent(NULL);
  ret = p.runSome();
  BOOST_CHECK(!ret);
  BOOST_CHECK_EQUAL(0U, ops.back()->getQueue().size());

  
  ops.back()->getQueue().push(RecordBuffer());
  ops.back()->onEvent(NULL);
  ret = p.runSome(); 
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(0U, ops.back()->getQueue().size());
 
  p.runComplete();
}

BOOST_AUTO_TEST_CASE(testConcurrentFifo)
{
  ConcurrentFifoProducerConsumer pc;
  std::thread p(std::bind(&ConcurrentFifoProducerConsumer::doProducer, std::ref(pc)));
  std::thread c(std::bind(&ConcurrentFifoProducerConsumer::doConsumer, std::ref(pc)));
  p.join();
  c.join();
}

struct test_gz
{
  std::filesystem::path filename;
  test_gz(const char * _filename,
          const std::string & testdata)
    :
    filename(_filename)
  {
    namespace io = boost::iostreams;
    {
      // put ostream in a scope so its d'tor
      // is executed; that flushes everything
      // through the zlib filter.
      std::ofstream f(filename.c_str());
      io::filtering_ostream out;
      out.push(io::gzip_compressor());
      out.push(f);
      out << testdata;
    }  
  }
  
  test_gz(const char * _filename)
    :
    test_gz(_filename, "aaaaaaaaaabbbbbbbbbccccccccccccccccccccccccccccccdddddddd")
  {
  }
  
  test_gz()
    :
    test_gz("test.gz")
  {
  }
  
  ~test_gz()
  {
    std::error_code ec;
    std::filesystem::remove(filename, ec);
  }
};

struct test_zstd
{
  std::filesystem::path filename;
  static const std::string & testdata()
  {
    static std::string ret("aaaaaaaaaabbbbbbbbbccccccccccccccccccccccccccccccdddddddd\n");
    return ret;
  }
  test_zstd(const char * _filename)
    :
    filename(_filename)
  {
    namespace io = boost::iostreams;
    {
      // put ostream in a scope so its d'tor
      // is executed; that flushes everything
      // through the zlib filter.
      std::ofstream f(filename.c_str());
      io::filtering_ostream out;
      out.push(io::zstd_compressor());
      out.push(f);
      out << testdata();
    }  
  }
  
  test_zstd()
    :
    test_zstd("test.zst")
  {
  }
  
  ~test_zstd()
  {
    std::error_code ec;
    std::filesystem::remove(filename, ec);
  }
};

template<typename _Compressor>
struct IoStreamsDecompressor
{
  typedef boost::iostreams::zstd_decompressor type;
};

template<>
struct IoStreamsDecompressor<ZLibCompress>
{
  typedef boost::iostreams::gzip_decompressor type;
};

typedef boost::mpl::list<ZstdCompress,ZLibCompress> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(testCompressorOneShot, compressor_type, test_types)
{
  std::string testdata("aaaaaaaaaabbbbbbbbbccccccccccccccccccccccccccccccdddddddd");
  std::stringstream actual;
  compressor_type compressor;
  compressor.put(reinterpret_cast<const uint8_t *>(testdata.data()), testdata.size(), true);
  BOOST_CHECK(compressor.run());
  uint8_t * buf;
  std::size_t bufLen;
  compressor.consumeOutput(buf, bufLen);
  BOOST_CHECK(bufLen > 0);
  actual.write(reinterpret_cast<const char *>(buf), bufLen);
  compressor.run();
  compressor.consumeOutput(buf, bufLen);
  BOOST_CHECK_EQUAL(bufLen, 0U);
  boost::iostreams::filtering_istream decompress;
  decompress.push(typename IoStreamsDecompressor<compressor_type>::type());
  decompress.push(actual);
  std::string decompressed;
  decompress >> decompressed;
  BOOST_CHECK_EQUAL(decompressed, testdata);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(testCompressorSmallOutputBuffer, compressor_type, test_types)
{
  std::string testdata("aaaaaaaaaabbbbbbbbbccccccccccccccccccccccccccccccdddddddd");
  std::stringstream actual;
  compressor_type compressor(3);
  compressor.put(reinterpret_cast<const uint8_t *>(testdata.data()), testdata.size(), true);
  while(true) {
    compressor.run();
    uint8_t * buf;
    std::size_t bufLen;
    compressor.consumeOutput(buf, bufLen);
    BOOST_CHECK(bufLen <= 3U);
    if (bufLen > 0) {
      actual.write(reinterpret_cast<const char *>(buf), bufLen);
    } else {
      break;
    }
  }
  boost::iostreams::filtering_istream decompress;
  decompress.push(typename IoStreamsDecompressor<compressor_type>::type());
  decompress.push(actual);
  std::string decompressed;
  decompress >> decompressed;
  BOOST_CHECK_EQUAL(decompressed, testdata);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(testCompressorTrickleInput, compressor_type, test_types)
{
  std::string testdata("aaaaaaaaaabbbbbbbbbccccccccccccccccccccccccccccccdddddddd");
  std::stringstream actual;
  compressor_type compressor;
  for(std::size_t i=0; i<testdata.size(); ++i) {
    compressor.put(reinterpret_cast<const uint8_t *>(testdata.data()+i), 1, false);
    BOOST_CHECK(compressor.run());
    // Plenty of room in default output bufffer, so wait till the final flush to get it.
  }
  compressor.put(nullptr, 0, true);
  BOOST_CHECK(compressor.run());
  uint8_t * buf;
  std::size_t bufLen;
  compressor.consumeOutput(buf, bufLen);
  BOOST_CHECK(bufLen > 0);
  actual.write(reinterpret_cast<const char *>(buf), bufLen);
  compressor.run();
  compressor.consumeOutput(buf, bufLen);
  BOOST_CHECK_EQUAL(bufLen, 0U);  
  boost::iostreams::filtering_istream decompress;
  decompress.push(typename IoStreamsDecompressor<compressor_type>::type());
  decompress.push(actual);
  std::string decompressed;
  decompress >> decompressed;
  BOOST_CHECK_EQUAL(decompressed, testdata);
}

// DBTODO
typedef AsyncFileTraits<gzip_file_traits> AsyncGzip;
typedef AsyncDoubleBufferStream<AsyncGzip> AsyncGzipStream;

BOOST_AUTO_TEST_CASE(testAsyncFileSystem)
{
  // Try a relatively small block size so we exercise the block management.
  test_gz testdata;
  std::string f(testdata.filename.c_str());
  AsyncGzipStream::filesystem_type fs = 
    AsyncGzipStream::file_system_type::openFor(f.c_str());
  AsyncGzipStream stream(fs,
			 f.c_str(), 
			 20);
  BOOST_CHECK(NULL == stream.open(0));
  uint8_t * buf = stream.open(10);
  BOOST_CHECK(buf != NULL);
  BOOST_CHECK_EQUAL(0, memcmp(buf, "aaaaaaaaaa", 10));
  stream.consume(10);
  BOOST_CHECK(!stream.isEOF());
  uint8_t * buf1 = stream.open(9);
  BOOST_CHECK_EQUAL(buf+10, buf1);
  BOOST_CHECK_EQUAL(0, memcmp(buf1, "bbbbbbbbb", 9));
  stream.consume(9);
  BOOST_CHECK(!stream.isEOF());
  buf = stream.open(30);
  BOOST_CHECK_EQUAL(0, memcmp(buf, "cccccccccccccccccccccccccccccc", 30));
  stream.consume(30);
  BOOST_CHECK(!stream.isEOF());
  // This tests opening to the end of the file with a short read.
  buf = stream.open(20);
  BOOST_CHECK(buf == NULL);
  buf = stream.open(8);
  BOOST_CHECK_EQUAL(0, memcmp(buf, "dddddddd", 8));
  stream.consume(8);  
  BOOST_CHECK(stream.isEOF());
  AsyncGzipStream::file_system_type::closeFileSystem(fs);
}

BOOST_AUTO_TEST_CASE(testAsyncFileSystemOneShot)
{
  // Try a relatively small block size so we exercise the block management
  // when trying to open a window bigger than the block size
  test_gz testdata;
  std::string f(testdata.filename.c_str());
  AsyncGzipStream::filesystem_type fs = 
    AsyncGzipStream::file_system_type::openFor(f.c_str());
  AsyncGzipStream stream(fs,
			 f.c_str(), 
			 20);
  BOOST_CHECK(NULL == stream.open(0));
  uint8_t * buf = stream.open(57);
  BOOST_REQUIRE(buf != NULL);
  BOOST_CHECK_EQUAL(0, memcmp(buf, 
			      "aaaaaaaaaa"
			      "bbbbbbbbb"
			      "cccccccccccccccccccccccccccccc"
			      "dddddddd", 57));
  stream.consume(57);
  BOOST_CHECK(stream.isEOF());
  AsyncGzipStream::file_system_type::closeFileSystem(fs);
}

struct WriteWithRetryTestFixture
{
  std::map<int, std::vector<std::pair<const uint8_t *, std::size_t>>> write_calls_;
  std::vector<ssize_t> ret_ ;
  std::vector<int> errno_;
  
  WriteWithRetryTestFixture()
  {
  }

  ~WriteWithRetryTestFixture()
  {
  }

  ssize_t operator()(int fd, const uint8_t * buf, std::size_t count)
  {
    write_calls_[fd].push_back(std::make_pair(buf, count));
    errno = errno_.size() > 0 ? errno_[(write_calls_[fd].size()-1) % errno_.size()] : 0;
    return errno != 0 ? -1 : std::min(ret_[(write_calls_[fd].size()-1) % ret_.size()], (ssize_t) count);
  }
};

BOOST_FIXTURE_TEST_CASE(testWriteWithRetrySuccess, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(5);
  writeWithRetry(3, buf, 5, *this);
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(1U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
}

BOOST_FIXTURE_TEST_CASE(testWriteWithRetryEINTR, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(5);
  errno_.push_back(EINTR);
  errno_.push_back(0);
  writeWithRetry(3, buf, 5, *this);
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(2U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(1).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(1).second);
}

BOOST_FIXTURE_TEST_CASE(testWriteWithRetryTwoEINTR, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(5);
  errno_.push_back(EINTR);
  errno_.push_back(EINTR);
  try {
    writeWithRetry(3, buf, 5, *this);
    BOOST_CHECK(false);
  } catch (std::runtime_error & ex) {
  }
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(2U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(1).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(1).second);
}

BOOST_FIXTURE_TEST_CASE(testWriteWithRetryEPERM, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(5);
  errno_.push_back(EPERM);
  try {
    writeWithRetry(3, buf, 5, *this);
    BOOST_CHECK(false);
  } catch (std::runtime_error & ex) {
  }
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(1U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
}

BOOST_FIXTURE_TEST_CASE(testWriteWithRetryShortWrite, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(2);
  try {
    writeWithRetry(3, buf, 5, *this);
    BOOST_CHECK(false);
  } catch (std::runtime_error & ex) {
  }
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(2U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
  BOOST_CHECK_EQUAL(&buf[2], write_calls_.at(3).at(1).first);
  BOOST_CHECK_EQUAL(3U, write_calls_.at(3).at(1).second);
}

BOOST_FIXTURE_TEST_CASE(testWriteWithRetryPartialWriteThenSucceed, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(3);
  writeWithRetry(3, buf, 5, *this);
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(2U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
  BOOST_CHECK_EQUAL(&buf[3], write_calls_.at(3).at(1).first);
  BOOST_CHECK_EQUAL(2U, write_calls_.at(3).at(1).second);
}

BOOST_FIXTURE_TEST_CASE(testWriteWithRetryENOSPC, WriteWithRetryTestFixture)
{
  uint8_t buf [] = { 0x01, 0x02, 0x03, 0x04, 0x05 };
  ret_.push_back(3);
  errno_.push_back(0);
  errno_.push_back(ENOSPC);
  try {
    writeWithRetry(3, buf, 5, *this);
    BOOST_CHECK(false);
  } catch (std::runtime_error & ex) {
  }
  BOOST_CHECK_EQUAL(1U, write_calls_.size());
  BOOST_REQUIRE_EQUAL(2U, write_calls_.at(3).size());
  BOOST_CHECK_EQUAL(&buf[0], write_calls_.at(3).at(0).first);
  BOOST_CHECK_EQUAL(5U, write_calls_.at(3).at(0).second);
  BOOST_CHECK_EQUAL(&buf[3], write_calls_.at(3).at(1).first);
  BOOST_CHECK_EQUAL(2U, write_calls_.at(3).at(1).second);
}

BOOST_AUTO_TEST_CASE(testPortRequestList)
{
  DataflowScheduler s;
  InProcessFifo f1(s,s);
  InProcessFifo f2(s,s);
  RuntimePort & p (*f1.getSource());
  RuntimePort & q (*f1.getTarget());
  RuntimePort & r (*f2.getSource());
  BOOST_CHECK(p.request_unique());
  BOOST_CHECK_EQUAL(1U, p.request_count());
  RuntimePort::request_node_ptr n = RuntimePort::request_value_traits::to_node_ptr(p);
  BOOST_CHECK_EQUAL(n, &p.mRequestHook);
  RuntimePort * ptr = RuntimePort::request_value_traits::to_value_ptr(n);
  BOOST_CHECK_EQUAL(ptr, &p);
  BOOST_CHECK_EQUAL(p.request_next(), &p);
  // Link a few of these together.
  p.request_link_after(q);
  BOOST_CHECK(!p.request_unique());
  BOOST_CHECK_EQUAL(2U, p.request_count());
  BOOST_CHECK(!q.request_unique());
  BOOST_CHECK_EQUAL(2U, q.request_count());
  BOOST_CHECK_EQUAL(p.request_next(), &q);
  BOOST_CHECK_EQUAL(q.request_next(), &p);
  p.request_link_after(r);
  BOOST_CHECK(!p.request_unique());
  BOOST_CHECK_EQUAL(3U, p.request_count());
  BOOST_CHECK(!q.request_unique());
  BOOST_CHECK_EQUAL(3U, q.request_count());
  BOOST_CHECK(!r.request_unique());
  BOOST_CHECK_EQUAL(3U, r.request_count());
  BOOST_CHECK_EQUAL(p.request_next(), &r);
  BOOST_CHECK_EQUAL(r.request_next(), &q);
  BOOST_CHECK_EQUAL(q.request_next(), &p);
  r.request_unlink();
  BOOST_CHECK(!p.request_unique());
  BOOST_CHECK_EQUAL(2U, p.request_count());
  BOOST_CHECK(!q.request_unique());
  BOOST_CHECK_EQUAL(2U, q.request_count());
  BOOST_CHECK(r.request_unique());
  BOOST_CHECK_EQUAL(1U, r.request_count());
  BOOST_CHECK_EQUAL(p.request_next(), &q);
  BOOST_CHECK_EQUAL(q.request_next(), &p);
  BOOST_CHECK_EQUAL(r.request_next(), &r);
}

struct gzipped_test_file
{
  std::filesystem::path p;
  gzipped_test_file()
  {
    auto b = Executable::getPath().parent_path() / "parser-test";
    p = b;
    p.replace_extension("gz");
    int ret = boost::process::system("gzip", boost::process::std_in < b, boost::process::std_out > p);
    BOOST_CHECK_EQUAL(0, ret);
  }

  ~gzipped_test_file()
  {
    std::error_code ec;
    std::filesystem::remove(p, ec);
  }
};

BOOST_AUTO_TEST_CASE(testZlibDecompress)
{
  uint8_t buf[128*1024];
  gzipped_test_file tf;
  ZLibDecompress<MemoryMappedFileBuffer> d(tf.p.c_str());
  // Read a full page
  BOOST_CHECK(64*1024 == d.read(buf, 64*1024));
  // Read two full pages
  BOOST_CHECK(128*1024 == d.read(buf, 128*1024));
  // Read a partial page
  BOOST_CHECK(30000 == d.read(buf, 30000));
  // Read a couple of partial pages
  BOOST_CHECK(100000 == d.read(buf, 100000));
}

BOOST_AUTO_TEST_CASE(testZlibDecompressStdio)
{
  uint8_t buf[128*1024];
  gzipped_test_file tf;
  ZLibDecompress<BlockBufferStream<stdio_file_traits> > d(tf.p.c_str());
  // Read a full page
  BOOST_CHECK(64*1024 == d.read(buf, 64*1024));
  // Read two full pages
  BOOST_CHECK(128*1024 == d.read(buf, 128*1024));
  // Read a partial page
  BOOST_CHECK(30000 == d.read(buf, 30000));
  // Read a couple of partial pages
  BOOST_CHECK(100000 == d.read(buf, 100000));
}

BOOST_AUTO_TEST_CASE(testFileGlob)
{
  auto p = Executable::getPath();
  std::vector<std::string> files;
  Glob::expand(p.remove_filename().c_str(), files);
  BOOST_CHECK_EQUAL(1, (int) files.size());
  BOOST_CHECK_EQUAL(0, strcmp(p.c_str(),
			      files[0].c_str()));
}

void checkHdfsRelativePath(const URI& p)
{
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "hdfs"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), "myhost"));
    BOOST_CHECK(boost::algorithm::iequals(p.toString(), "hdfs://myhost:9000/this/is/a/relative/path"));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/relative/path"));
    BOOST_CHECK_EQUAL(9000, p.getPort());
}

BOOST_AUTO_TEST_CASE(testURI)
{
  {
    URI p("hdfs://myhost:9000/this/is/a/path");
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "hdfs"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), "myhost"));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/path"));
    BOOST_CHECK_EQUAL(9000, p.getPort());
  }
  {
    URI p("file:///this/is/a/path");
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "file"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
  {
    URI p("/this/is/a/path");
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), ""));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
  {
    URI a("file:///this/is/a/path");
    URI b("./relative/path");
    URI p(a,b);
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "file"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/relative/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
  {
    URI a("file:///this/is/a/");
    URI b("relative/path");
    URI p(a,b);
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "file"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::iequals(p.toString(), "file:///this/is/a/relative/path"));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/relative/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
  {
    URI a("file:///this/is/a/");
    URI b("/absolute/path");
    URI p(a,b);
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "file"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::iequals(p.toString(), "file:///absolute/path"));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/absolute/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
  {
    URI a("file:///");
    URI b("this/is/a/relative/path");
    URI p(a,b);
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "file"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::iequals(p.toString(), "file:///this/is/a/relative/path"));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/relative/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
  {
    URI a("file:///");
    URI b("/this/is/a/relative/path");
    URI p(a,b);
    BOOST_CHECK(boost::algorithm::iequals(p.getScheme(), "file"));
    BOOST_CHECK(boost::algorithm::equals(p.getHost(), ""));
    BOOST_CHECK(boost::algorithm::iequals(p.toString(), "file:///this/is/a/relative/path"));
    BOOST_CHECK(boost::algorithm::equals(p.getPath(), "/this/is/a/relative/path"));
    BOOST_CHECK_EQUAL(0, p.getPort());
  }
#if defined(TRECUL_HAS_HADOOP)
  {
    URI a("hdfs://myhost:9000/");
    URI b("this/is/a/relative/path");
    URI p(a,b);
    checkHdfsRelativePath(p);
  }
  {
    URI a("hdfs://myhost:9000/");
    URI b("/this/is/a/relative/path");
    URI p(a,b);
    checkHdfsRelativePath(p);
  }
  {
    URI a("hdfs://myhost:9000/this/is/a/");
    URI b("relative/path");
    URI p(a,b);
    checkHdfsRelativePath(p);
  }
  {
    try {
      URI a("hdfs://myhost:9000/this/is/${invalid}.txt");
      BOOST_CHECK(false);
    } catch(std::exception& e) {
      std::string err(e.what());
      std::cout << err << std::endl;
      BOOST_CHECK(boost::algorithm::starts_with(err, "Failed to parse"));      
    }
  }
#endif
}

BOOST_AUTO_TEST_CASE(testPagedHashTable)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  RecordType recTy(ctxt, members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  RecordType rhsTy(ctxt, rhsMembers);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(ctxt, emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);
  std::vector<const RecordType *> probeOnly;
  probeOnly.push_back(&rhsTy);
  probeOnly.push_back(&emptyTy);
  std::vector<const RecordType *> tableOnly;
  tableOnly.push_back(&recTy);
  tableOnly.push_back(&emptyTy);

  RecordBuffer rhs1 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs1);
  rhsTy.setChar("e", "123457", rhs1);
  rhsTy.setVarchar("f", "abcdefghijklmnoz", rhs1);
  rhsTy.setInt32("g", 9923431, rhs1);
  rhsTy.setInt64("h", 1239923433, rhs1);

  RecordBuffer rhs2 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs2);
  rhsTy.setChar("e", "123456", rhs2);
  rhsTy.setVarchar("f", "abcdefghijklmnop", rhs2);
  rhsTy.setInt32("g", 9923432, rhs2);
  rhsTy.setInt64("h", 1239923432, rhs2);

  // Use 
  {
    CodeGenerationContext codeGen;
    TreculFunction tableHash(ctxt, codeGen, "chartablehash", tableOnly, "#(a)");
    TreculFunction probeHash(ctxt, codeGen, "charprobehash", probeOnly, "#(e)");
    TreculFunction equals(ctxt, codeGen, "chareq", types, "a = e");
    TreculModule module(codeGen);
    paged_hash_table table(true, module.getFunction<TreculFunctionRuntime>(tableHash.getReference()));
    paged_hash_table::probe_predicate probe(module.getFunction<TreculFunctionRuntime>(probeHash.getReference()),
                                            module.getFunction<TreculFunctionRuntime>(equals.getReference()));

    // Test probe of an empty table
    paged_hash_table::query_iterator<paged_hash_table::probe_predicate> qit(probe);
    qit.mQueryPredicate.ProbeThis = rhs2;
    table.find(qit, &runtimeCtxt);
    BOOST_CHECK_EQUAL(false, qit.next(&runtimeCtxt));
    
    // Add a record and validate we find it.
    RecordBuffer lhs = recTy.GetMalloc()->malloc();
    recTy.setChar("a", "123456", lhs);
    recTy.setVarchar("b", "abcdefghijklmnop", lhs);
    recTy.setInt32("c", 9923432, lhs);
    recTy.setInt64("d", 1239923432, lhs);
    table.insert(lhs, &runtimeCtxt);
    table.find(qit, &runtimeCtxt);
    BOOST_CHECK_EQUAL(true, qit.next(&runtimeCtxt));
    BOOST_CHECK_EQUAL(lhs.Ptr, qit.value().Ptr);
    BOOST_CHECK_EQUAL(false, qit.next(&runtimeCtxt));

    // Now add some additional cruft to the table that doesn't match
    for(int i=0; i<100000; i++) {
      RecordBuffer tmp = recTy.GetMalloc()->malloc();
      recTy.setChar("a", (boost::format("%1%") % i).str().c_str(), tmp);
      recTy.setVarchar("b", "abcdefghijklmnop", tmp);
      recTy.setInt32("c", i, tmp);
      recTy.setInt64("d", int64_t(i)*int64_t(i), tmp);
      table.insert(tmp, &runtimeCtxt);
    }
    // Still only one.
    table.find(qit, &runtimeCtxt);
    BOOST_CHECK_EQUAL(true, qit.next(&runtimeCtxt));
    BOOST_CHECK_EQUAL(lhs.Ptr, qit.value().Ptr);
    BOOST_CHECK_EQUAL(false, qit.next(&runtimeCtxt));

    // Add a second match
    lhs = recTy.GetMalloc()->malloc();
    recTy.setChar("a", "123456", lhs);
    recTy.setVarchar("b", "zwxiherssed", lhs);
    recTy.setInt32("c", -19323, lhs);
    recTy.setInt64("d", -111, lhs);
    table.insert(lhs, &runtimeCtxt);
    table.find(qit, &runtimeCtxt);
    BOOST_CHECK_EQUAL(true, qit.next(&runtimeCtxt));
    BOOST_CHECK_EQUAL(true, qit.next(&runtimeCtxt));
    BOOST_CHECK_EQUAL(false, qit.next(&runtimeCtxt));

    // Dump contents with scan iterator
    paged_hash_table::scan_all_iterator sit(table);
    int32_t cnt=0;
    while(sit.next(&runtimeCtxt)) {
      cnt += 1;
    }
    // Pretty weak check, at least we have
    // the right number of records.
    BOOST_CHECK_EQUAL(cnt, 100002);
  }
}

BOOST_AUTO_TEST_CASE(testBlockBufferStream)
{
  test_gz testdata;
  // Try a relatively small block size so we exercise the block management.
  BlockBufferStream<gzip_file_traits> stream(testdata.filename.c_str(), 20);
  BOOST_CHECK(NULL == stream.open(0));
  uint8_t * buf = stream.open(10);
  BOOST_CHECK(buf != NULL);
  BOOST_CHECK_EQUAL(buf, stream.start());
  BOOST_CHECK_EQUAL(buf + 20, stream.end());
  BOOST_CHECK_EQUAL(0, memcmp(buf, "aaaaaaaaaa", 10));
  stream.consume(10);
  uint8_t * buf1 = stream.open(9);
  BOOST_CHECK_EQUAL(buf, stream.start());
  BOOST_CHECK_EQUAL(buf+10, buf1);
  BOOST_CHECK_EQUAL(0, memcmp(buf1, "bbbbbbbbb", 9));
  stream.consume(9);
  buf = stream.open(30);
  BOOST_CHECK_EQUAL(buf, stream.start());
  BOOST_CHECK_EQUAL(buf + 31, stream.end());
  BOOST_CHECK_EQUAL(0, memcmp(buf, "cccccccccccccccccccccccccccccc", 30));
  stream.consume(30);
  // This tests opening to the end of the file with a short read.
  buf = stream.open(20);
  BOOST_CHECK(buf == NULL);
  buf = stream.open(8);
  BOOST_CHECK_EQUAL(buf, stream.start());
  BOOST_CHECK_EQUAL(buf + 8, stream.end());
  BOOST_CHECK_EQUAL(0, memcmp(buf, "dddddddd", 8));
  stream.consume(8);
}

BOOST_AUTO_TEST_CASE(testIntrusiveFifo)
{
  IntrusiveFifo<TestStruct> fifo;
  std::vector<TestStruct> bufs(10);
  for(int i=0; i<2; i++) {
    BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
    fifo.Push(&bufs[0]);
    BOOST_CHECK(NULL==bufs[0].mNext);
    BOOST_CHECK_EQUAL(1, (int) fifo.GetSize());
    fifo.Push(&bufs[1]);
    BOOST_CHECK_EQUAL(2, (int) fifo.GetSize());
    BOOST_CHECK(NULL == bufs[1].mNext);
    BOOST_CHECK_EQUAL(&bufs[1], bufs[0].mNext);
    fifo.Push(&bufs[2]);
    BOOST_CHECK_EQUAL(3, (int) fifo.GetSize());
    BOOST_CHECK(NULL == bufs[2].mNext);
    BOOST_CHECK_EQUAL(&bufs[2], bufs[1].mNext);
    BOOST_CHECK_EQUAL(&bufs[1], bufs[0].mNext);
    TestStruct * tmp;
    fifo.Pop(tmp);
    BOOST_CHECK_EQUAL(2, (int) fifo.GetSize());
    BOOST_CHECK(tmp == &bufs[0]);
    BOOST_CHECK(NULL == bufs[2].mNext);
    BOOST_CHECK_EQUAL(&bufs[2], bufs[1].mNext);
    BOOST_CHECK(NULL == bufs[0].mNext);
    fifo.Pop(tmp);
    BOOST_CHECK_EQUAL(1, (int) fifo.GetSize());
    BOOST_CHECK(tmp == &bufs[1]);
    BOOST_CHECK(NULL == bufs[2].mNext);
    BOOST_CHECK(NULL == bufs[1].mNext);
    BOOST_CHECK(NULL == bufs[0].mNext);
    fifo.Pop(tmp);
    BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
    BOOST_CHECK(tmp== &bufs[2]);
    BOOST_CHECK(NULL == bufs[2].mNext);
    BOOST_CHECK(NULL == bufs[1].mNext);
    BOOST_CHECK(NULL == bufs[0].mNext);
  }
}

BOOST_AUTO_TEST_CASE(testCircularArrayFifo)
{
  RuntimeFifoPage<const char *, 12> page(NULL);
  BOOST_CHECK_EQUAL(0, page.GetSize());
  std::vector<char> v(12);
  const char * tmp=NULL;
  page.Push(&v[0]);
  BOOST_CHECK_EQUAL(1, page.GetSize());
  page.Pop(tmp);
  BOOST_CHECK_EQUAL(0, page.GetSize());
  BOOST_CHECK(tmp== &v[0]);

  page.Push(&v[0]);
  page.Push(&v[1]);
  BOOST_CHECK_EQUAL(2, page.GetSize());
  page.Pop(tmp);
  BOOST_CHECK_EQUAL(1, page.GetSize());
  BOOST_CHECK(tmp == &v[0]);
  page.Pop(tmp);
  BOOST_CHECK_EQUAL(0, page.GetSize());
  BOOST_CHECK(tmp == &v[1]);

  // Push all 12 elements
  for(std::size_t i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(i, page.GetSize());
    page.Push(&v[i]);
    BOOST_CHECK_EQUAL(i+1, page.GetSize());
  }
  for(std::size_t i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, page.GetSize());
    page.Pop(tmp);
    BOOST_CHECK(tmp == &v[i]);
    BOOST_CHECK_EQUAL(11-i, page.GetSize());
  }
}

BOOST_AUTO_TEST_CASE(testFifoSinglePage)
{
  RuntimeFifo<const char *, 12> fifo;
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  std::vector<char> v(12);
  const char * tmp=NULL;
  fifo.Push(&v[0]);
  BOOST_CHECK_EQUAL(1, (int) fifo.GetSize());
  fifo.Pop(tmp);
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK(tmp== &v[0]);

  fifo.Push(&v[0]);
  fifo.Push(&v[1]);
  BOOST_CHECK_EQUAL(2, (int) fifo.GetSize());
  fifo.Pop(tmp);
  BOOST_CHECK_EQUAL(1, (int) fifo.GetSize());
  BOOST_CHECK(tmp== &v[0]);
  fifo.Pop(tmp);
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK(tmp== &v[1]);

  // Push all 12 elements
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(i, (int) fifo.GetSize());
    fifo.Push(&v[i]);
    BOOST_CHECK_EQUAL(i+1, (int) fifo.GetSize());
  }
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, (int) fifo.GetSize());
    fifo.Pop(tmp);
    BOOST_CHECK(tmp == &v[i]);
    BOOST_CHECK_EQUAL(11-i, (int) fifo.GetSize());
  }
}

BOOST_AUTO_TEST_CASE(testFifoMultiPage)
{
  RuntimeFifo<const char *, 12> fifo;
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  std::vector<char> v(30);
  const char * tmp=NULL;

  // Push 12 elements
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(i, (int) fifo.GetSize());
    fifo.Push(&v[i]);
    BOOST_CHECK_EQUAL(i+1, (int) fifo.GetSize());
    BOOST_CHECK_EQUAL(1, (int) fifo.GetNumPages());
  }
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, (int) fifo.GetSize());
    fifo.Pop(tmp);
    BOOST_CHECK(tmp == &v[i]);
    BOOST_CHECK_EQUAL(11-i, (int) fifo.GetSize());
    BOOST_CHECK_EQUAL(1, (int) fifo.GetNumPages());
  }
  BOOST_CHECK(fifo.empty());

  // Push 13 elements to allocate a new page.
  // Note with this pattern of pushes and pops,
  // we create a fifo with two pages: the first full
  // and the second with only 1 element.  Then we start
  // popping.  We are popping from the full page so we
  // continue to have 2 pages until we get down to 1 element.
  for(int i=0; i<13; i++) {
    BOOST_CHECK_EQUAL(i, (int) fifo.GetSize());
    fifo.Push(&v[i]);
    BOOST_CHECK_EQUAL(i+1, (int) fifo.GetSize());
    BOOST_CHECK_EQUAL(fifo.GetSize() ? (fifo.GetSize() + 11)/12 : 1LL, fifo.GetNumPages());
  }
  for(int i=0; i<13; i++) {
    std::vector<uint8_t> pageSizes(i<12 ? 2 : 1);
    pageSizes[0] = i<12 ? 12-i : 1;
    if (i<12)
      pageSizes[1] = 1;
    BOOST_CHECK(fifo.TestPageSizes(pageSizes));
    BOOST_CHECK_EQUAL(fifo.GetSize() > 1 ? 2LL : 1LL, (int64_t ) fifo.GetNumPages());
    BOOST_CHECK_EQUAL(13-i, (int) fifo.GetSize());
    fifo.Pop(tmp);
    BOOST_CHECK(tmp == &v[i]);
    BOOST_CHECK_EQUAL(12-i, (int) fifo.GetSize());
  }
  BOOST_CHECK(fifo.empty());
}

BOOST_AUTO_TEST_CASE(testFifoSwap)
{
  RuntimeFifo<const char *, 12> fifo;
  RuntimeFifo<const char *, 12> fifo2;
  std::vector<char> v(30);
  const char * tmp;
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(0, (int) fifo2.GetSize());

  // Test swapping empty fifos
  fifo.swap(fifo2);
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(0, (int) fifo2.GetSize());
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK(fifo2.empty());
  // Fill a partial page and swap
  fifo.Push(&v[0]);
  BOOST_CHECK_EQUAL(1, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(0, (int) fifo2.GetSize());
  fifo.swap(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(1, (int) fifo2.GetSize());
  fifo2.Pop(tmp);
  BOOST_CHECK(tmp== &v[0]);
  BOOST_CHECK(fifo2.empty());

  // Fill a full page and swap
  for(int i=0; i<12; i++) {
    fifo.Push(&v[i]);
  }
  BOOST_CHECK_EQUAL(12, (int) fifo.GetSize());
  fifo.swap(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(11-i, (int) fifo2.GetSize());
    BOOST_CHECK_EQUAL(1, (int) fifo2.GetNumPages());
  }
  BOOST_CHECK(fifo2.empty());
 
  // Fill multiple pages and swap
  for(int i=0; i<15; i++) {
    fifo.Push(&v[i]);
  }
  BOOST_CHECK_EQUAL(15, (int) fifo.GetSize());
  fifo.swap(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  for(int i=0; i<15; i++) {
    BOOST_CHECK_EQUAL(15-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(14-i, (int) fifo2.GetSize());
  }
  BOOST_CHECK(fifo2.empty());
 
  // Now initialize fifo2 with some data and try
}

BOOST_AUTO_TEST_CASE(testFifoPopAllTo)
{
  RuntimeFifo<const char *, 12> fifo;
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  std::vector<char> v(120);
  const char * tmp=NULL;

  RuntimeFifo<const char *, 12> fifo2;
  // Fill a full page and push all to
  for(int i=0; i<12; i++) {
    fifo.Push(&v[i]);
  }
  BOOST_CHECK_EQUAL(12, (int) fifo.GetSize());
  fifo.popAndPushAllTo(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(11-i, (int) fifo2.GetSize());
    BOOST_CHECK_EQUAL(1, (int) fifo2.GetNumPages());
  }
  BOOST_CHECK(fifo2.empty());

  // Fill a two full pages and push all to
  for(int i=0; i<24; i++) {
    fifo.Push(&v[i]);
  }
  BOOST_CHECK_EQUAL(24, (int) fifo.GetSize());
  fifo.popAndPushAllTo(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  for(int i=0; i<24; i++) {
    BOOST_CHECK_EQUAL(24-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(23-i, (int) fifo2.GetSize());
  }
  BOOST_CHECK(fifo2.empty());
  
  // Initialize fifo2 with a full page and do same tests.
  // Fill a full page and push all to
  for(int i=0; i<12; i++) {
    fifo.Push(&v[i]);
  }
  for(int i=0; i<12; i++) {
    fifo2.Push(&v[i+60]);
  }
  BOOST_CHECK_EQUAL(12, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(12, (int) fifo2.GetSize());
  fifo.popAndPushAllTo(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(24, (int) fifo2.GetSize());
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(24-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[60+i]);
    BOOST_CHECK_EQUAL(23-i, (int) fifo2.GetSize());
  }
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(11-i, (int) fifo2.GetSize());
  }
  BOOST_CHECK(fifo2.empty());
  
  for(int i=0; i<24; i++) {
    fifo.Push(&v[i]);
  }
  for(int i=0; i<12; i++) {
    fifo2.Push(&v[i+60]);
  }
  BOOST_CHECK_EQUAL(24, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(12, (int) fifo2.GetSize());
  fifo.popAndPushAllTo(fifo2);
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(36, (int) fifo2.GetSize());
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(36-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    //    BOOST_CHECK(tmp== &v[60+i]);
    if (tmp != &v[60+i]) {
      std::cout << "tmp - &v[0] = " << (tmp - &v[0]) << "; expected " << (60+i) << std::endl;
    }
    BOOST_CHECK_EQUAL(35-i, (int) fifo2.GetSize());
  }
  for(int i=0; i<24; i++) {
    BOOST_CHECK_EQUAL(24-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(23-i, (int) fifo2.GetSize());
  }

  // Initialize fifo2 with a 2 full pages and do same tests.
  // Fill a full page and push all to
  for(int i=0; i<12; i++) {
    fifo.Push(&v[i]);
  }
  for(int i=0; i<24; i++) {
    fifo2.Push(&v[i+60]);
  }
  BOOST_CHECK_EQUAL(12, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(24, (int) fifo2.GetSize());
  fifo.popAndPushAllTo(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(36, (int) fifo2.GetSize());
  for(int i=0; i<24; i++) {
    BOOST_CHECK_EQUAL(36-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[60+i]);
    BOOST_CHECK_EQUAL(35-i, (int) fifo2.GetSize());
  }
  for(int i=0; i<12; i++) {
    BOOST_CHECK_EQUAL(12-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(11-i, (int) fifo2.GetSize());
  }
  BOOST_CHECK(fifo2.empty());

  // Initialize fifo2 with a 2 full pages and do same tests.
  // Fill a full page and push all to
  for(int i=0; i<24; i++) {
    fifo.Push(&v[i]);
  }
  for(int i=0; i<24; i++) {
    fifo2.Push(&v[i+60]);
  }
  BOOST_CHECK_EQUAL(24, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(24, (int) fifo2.GetSize());
  fifo.popAndPushAllTo(fifo2);
  BOOST_CHECK(fifo.empty());
  BOOST_CHECK_EQUAL(0, (int) fifo.GetSize());
  BOOST_CHECK_EQUAL(48, (int) fifo2.GetSize());
  for(int i=0; i<24; i++) {
    BOOST_CHECK_EQUAL(48-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[60+i]);
    BOOST_CHECK_EQUAL(47-i, (int) fifo2.GetSize());
  }
  for(int i=0; i<24; i++) {
    BOOST_CHECK_EQUAL(24-i, (int) fifo2.GetSize());
    fifo2.Pop(tmp);
    BOOST_CHECK(tmp== &v[i]);
    BOOST_CHECK_EQUAL(23-i, (int) fifo2.GetSize());
  }
  BOOST_CHECK(fifo2.empty());
}

template <int _N>
class FifoPerfTest
{
public:
  static void Run(const std::vector<TestStruct *> bufs) {
    RuntimeFifo<TestStruct *, _N> fifo;
    int numBufs = (int) bufs.size();
    for(int i=0; i<numBufs; i++) {
      fifo.Push(bufs[i]);

      // Every 100 or so pushes, pop a bunch
      if (i % 100 == 99) {
	for(int j=0; j<95; j++) {
	  TestStruct * tmp;
	  fifo.Pop(tmp);
	}
      }
    }

    TestStruct * tmp;
    while(fifo.GetSize() ) {
      fifo.Pop(tmp);
    }
  }
};

BOOST_AUTO_TEST_CASE(testFifoLargePerfTest)
{
  // Create a large working set of buffers
  int numBufs=5000000;
  std::vector<TestStruct *> bufs;
  for(int i=0; i<numBufs; i++)
    bufs.push_back(new TestStruct());
  {
    // Time to enqueue and dequeue to a fifo
    boost::timer::auto_cpu_timer t;
    FifoPerfTest<14>::Run(bufs);
  }
  {
    // Time to enqueue and dequeue to a fifo
    boost::timer::auto_cpu_timer t;
    FifoPerfTest<30>::Run(bufs);
  }
  {
    // Time to enqueue and dequeue to a fifo
    boost::timer::auto_cpu_timer t;
    FifoPerfTest<46>::Run(bufs);
  }
  {
    boost::timer::auto_cpu_timer t;
    IntrusiveFifo<TestStruct> fifo;
    int numBufs = (int) bufs.size();
    for(int i=0; i<numBufs; i++) {
      fifo.Push(bufs[i]);

      // Every 100 or so pushes, pop a bunch
      if (i % 100 == 99) {
	for(int j=0; j<95; j++) {
	  TestStruct * tmp;
	  fifo.Pop(tmp);
	}
      }
    }

    TestStruct * tmp;
    while(fifo.GetSize() ) {
      fifo.Pop(tmp);
    }
  }

  for(std::vector<TestStruct *>::iterator it = bufs.begin();
      bufs.end() != it;
      ++it)
    delete (*it);
}

BOOST_AUTO_TEST_CASE(testSchedulerReadPriority)
{
  BOOST_CHECK_EQUAL(0, DataflowScheduler::getReadPriority(0));
  BOOST_CHECK_EQUAL(1, DataflowScheduler::getReadPriority(1));
  BOOST_CHECK_EQUAL(2, DataflowScheduler::getReadPriority(2));
  BOOST_CHECK_EQUAL(2, DataflowScheduler::getReadPriority(3));
  BOOST_CHECK_EQUAL(3, DataflowScheduler::getReadPriority(4));
  BOOST_CHECK_EQUAL(10, DataflowScheduler::getReadPriority(1023));
  BOOST_CHECK_EQUAL(11, DataflowScheduler::getReadPriority(1024));
  BOOST_CHECK_EQUAL(30, DataflowScheduler::getReadPriority(1073741823));
  BOOST_CHECK_EQUAL(31, DataflowScheduler::getReadPriority(1073741824));
  BOOST_CHECK_EQUAL(31, DataflowScheduler::getReadPriority((uint64_t) std::numeric_limits<int32_t>::max()));
  BOOST_CHECK_EQUAL(31, DataflowScheduler::getReadPriority(std::numeric_limits<uint32_t>::max()));
  BOOST_CHECK_EQUAL(31, DataflowScheduler::getReadPriority(std::numeric_limits<uint64_t>::max()));
}

BOOST_AUTO_TEST_CASE(testSchedulerWritePriority)
{
  BOOST_CHECK_EQUAL(31, DataflowScheduler::getWritePriority(0));
  BOOST_CHECK_EQUAL(30, DataflowScheduler::getWritePriority(1));
  BOOST_CHECK_EQUAL(29, DataflowScheduler::getWritePriority(2));
  BOOST_CHECK_EQUAL(29, DataflowScheduler::getWritePriority(3));
  BOOST_CHECK_EQUAL(28, DataflowScheduler::getWritePriority(4));
  BOOST_CHECK_EQUAL(21, DataflowScheduler::getWritePriority(1023));
  BOOST_CHECK_EQUAL(20, DataflowScheduler::getWritePriority(1024));
  BOOST_CHECK_EQUAL(1, DataflowScheduler::getWritePriority(1073741823));
  BOOST_CHECK_EQUAL(0, DataflowScheduler::getWritePriority(1073741824));
  BOOST_CHECK_EQUAL(0, DataflowScheduler::getWritePriority((uint64_t) std::numeric_limits<int32_t>::max()));
  BOOST_CHECK_EQUAL(0, DataflowScheduler::getWritePriority(std::numeric_limits<uint32_t>::max()));
  BOOST_CHECK_EQUAL(0, DataflowScheduler::getWritePriority(std::numeric_limits<uint64_t>::max()));
}

// 31-bit byte array prefix (assuming byte array length >=4).
uint32_t byteArrayPrefix(uint8_t * a)
{
  return 
    ((((uint32_t) a[0]) << 24) |
     (((uint32_t) a[1]) << 16) |
     (((uint32_t) a[2]) << 8) |
     ((uint32_t) a[3])) >> 1; 
}

BOOST_AUTO_TEST_CASE(testKeyPrefixBuilder)
{
  // Just a sanity check endianness of machine
  uint8_t a[4] = {0x00, 0x00, 0x00, 0x02};
  uint8_t b[4] = {0x80, 0x00, 0x00, 0x00};

  BOOST_CHECK(0 > memcmp(&a[0], &b[0], 4));
  BOOST_CHECK(*((int32_t *)&a[0]) > *((int32_t *)&b[0]));
  BOOST_CHECK_EQUAL(*((int32_t *)&a[0]), 0x02000000);
  BOOST_CHECK_EQUAL(*((int32_t *)&b[0]), 0x00000080);

  uint32_t aPrefix = byteArrayPrefix(&a[0]);
  uint32_t bPrefix = byteArrayPrefix(&b[0]);
  BOOST_CHECK(aPrefix < bPrefix);
  
    KeyPrefixBuilder kpb(31);
    kpb.add(2222222222u);
    uint32_t k1 = kpb.getPrefix();
    kpb.clear();
    kpb.add(2333333333u);
    uint32_t k2 = kpb.getPrefix();
    kpb.clear();

    char foo[] = "foo";
    char bar[] = "bar";
    kpb.add(&foo[0], &foo[0]+3);
    kpb.add(7234);
    k1 = kpb.getPrefix();
    kpb.clear();

    kpb.add(&bar[0], &bar[0]+3);
    kpb.add(1234003432);
    k2 = kpb.getPrefix();
    kpb.clear();

    kpb.add(&foo[0], &foo[0]+3);
    kpb.add(7234);
    k1 = kpb.getPrefix();
    kpb.clear();

    kpb.add(&foo[0], &foo[0]+3);
    kpb.add(1234003432);
    k2 = kpb.getPrefix();
    kpb.clear();

    // On a little endian machine, these prefixes should
    // be the same.
    kpb.add(&foo[0], &foo[0]+3);
    kpb.add(0xff123456);
    k1 = kpb.getPrefix();
    kpb.clear();

    kpb.add(&foo[0], &foo[0]+3);
    kpb.add(0xff789abc);
    k2 = kpb.getPrefix();
    kpb.clear();

    kpb.add(0x12345678);
    k1 = kpb.getPrefix();
    BOOST_CHECK_EQUAL(0x12345678U/2, k1);
    kpb.clear();

    kpb.add(0x12345679);
    k2 = kpb.getPrefix();
    BOOST_CHECK_EQUAL(0x12345679U/2, k2);
    kpb.clear();
}

BOOST_AUTO_TEST_CASE(testSortKeyParse)
{
  {
    SortKey sk("a");
    BOOST_CHECK(boost::algorithm::equals(sk.getName(), "a"));
    BOOST_CHECK_EQUAL(SortKey::ASC, sk.getOrder());
  }
  {
    SortKey sk("  a\t");
    BOOST_CHECK(boost::algorithm::equals(sk.getName(), "a"));
    BOOST_CHECK_EQUAL(SortKey::ASC, sk.getOrder());
  }
  {
    SortKey sk("a ASC");
    BOOST_CHECK(boost::algorithm::equals(sk.getName(), "a"));
    BOOST_CHECK_EQUAL(SortKey::ASC, sk.getOrder());
  }
  {
    SortKey sk("a DESC");
    BOOST_CHECK(boost::algorithm::equals(sk.getName(), "a"));
    BOOST_CHECK_EQUAL(SortKey::DESC, sk.getOrder());
  }
  {
    SortKey sk("\na   \tDESC  ");
    BOOST_CHECK(boost::algorithm::equals(sk.getName(), "a"));
    BOOST_CHECK_EQUAL(SortKey::DESC, sk.getOrder());
  }
  try {
    // Test catching parse failure
    SortKey sk("\na   \tDEC  ");
    BOOST_CHECK(false);
  } catch(std::exception& ) {
  }
}

BOOST_AUTO_TEST_CASE(testLessThan1)
{
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("cre_date", DateType::Get(ctxt)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt)));
  RecordType recTy(ctxt, members);
  std::vector<std::string> fields;
  fields.push_back("akid");
  
  TreculFunction * lessThanFun = LessThanFunction::get(ctxt, &recTy, &recTy, 
                                                    fields, "testEq");
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());
  
  InterpreterContext runtimeCtxt;
  
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-04-07");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-09-09");

  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  delete lessThanFun;  
}

BOOST_AUTO_TEST_CASE(testLessThan2)
{
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("cre_date", DateType::Get(ctxt)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt)));
  RecordType recTy(ctxt, members);
  std::vector<std::string> fields;
  fields.push_back("akid");
  fields.push_back("coop_id");
  
  TreculFunction * lessThanFun = LessThanFunction::get(ctxt, &recTy, &recTy, 
                                                    fields, "testEq");
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());

  InterpreterContext runtimeCtxt;
  
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-04-07");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-09-09");

  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));  
  delete lessThanFun;
}

BOOST_AUTO_TEST_CASE(testLessThan3)
{
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("cre_date", DateType::Get(ctxt)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt)));
  RecordType recTy(ctxt, members);
  std::vector<std::string> fields;
  fields.push_back("akid");
  fields.push_back("coop_id");
  fields.push_back("cre_date");
  
  TreculFunction * lessThanFun = LessThanFunction::get(ctxt, &recTy, &recTy, 
                                                    fields, "testEq");
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());

  InterpreterContext runtimeCtxt;
  
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-04-07");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-09-09");

  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  ///////////////////////////////////////////////////////////////
  // First suite here tests when the first key is less and then
  // makes sure that the other keys don't effect the result.
  // There are a bunch of permutations to try.

  // Try where second keys are not equal; vary third key
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  // Switch order of second keys
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  // Make second key equal and muck with the third key
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  ///////////////////////////////////////////////////////////////
  // Second suite here tests when the first keys are equal.

  // Try where second keys are not equal; vary third key
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  ///////////////////////////////////////////////////////////////
  // Third suite here tests when the first two keys are equal

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setDate("cre_date", d1, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setDate("cre_date", d1, A);
  recTy.setDate("cre_date", d1, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  delete lessThanFun;
  recTy.getFree().free(A);
  recTy.getFree().free(B);
}

BOOST_AUTO_TEST_CASE(testLessThan7)
{
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  std::vector<std::string> fields;
  for(int32_t i=0; i<9; ++i) {
    std::string field = (boost::format("a%1%") % i).str();
    members.push_back(RecordMember(field, Int32Type::Get(ctxt)));
    if (0<i && i<8) {
      fields.push_back(field);
    }
  }
  RecordType recTy(ctxt, members);  
  TreculFunction * lessThanFun = LessThanFunction::get(ctxt, &recTy, &recTy, 
                                                    fields, "testEq");
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());

  InterpreterContext runtimeCtxt;  
  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  for(int32_t i=0; i<9; ++i) {
    std::string field = (boost::format("a%1%") % i).str();
    recTy.setInt32(field, 12, A);
    recTy.setInt32(field, 12, B);
  }
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setInt32("a1", 9, A);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setInt32("a6", 6, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setInt32("a1", 12, A);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setInt32("a4", 6, A);
  recTy.setInt32("a5", 20, A);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));

  delete lessThanFun;
  recTy.getFree().free(A);
  recTy.getFree().free(B);
}

BOOST_AUTO_TEST_CASE(testCompare1)
{
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("cre_date", DateType::Get(ctxt)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt)));
  RecordType recTy(ctxt, members);
  std::vector<SortKey> fields;
  fields.push_back(SortKey("akid"));
  
  TreculFunction * lessThanFun = CompareFunction::get(ctxt, &recTy, &recTy, fields, fields);
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());

  InterpreterContext runtimeCtxt;
  
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-04-07");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-09-09");

  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  delete lessThanFun;
  recTy.getFree().free(A);
  recTy.getFree().free(B);
}

BOOST_AUTO_TEST_CASE(testCompare3)
{  
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("cre_date", DateType::Get(ctxt)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt)));
  RecordType recTy(ctxt, members);
  std::vector<SortKey> fields;
  fields.push_back(SortKey("akid"));
  fields.push_back(SortKey("coop_id"));
  fields.push_back(SortKey("cre_date"));
  
  TreculFunction * lessThanFun = CompareFunction::get(ctxt, &recTy, &recTy, fields, fields);
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());
  InterpreterContext runtimeCtxt;
  
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-04-07");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-09-09");

  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  ///////////////////////////////////////////////////////////////
  // First suite here tests when the first key is less and then
  // makes sure that the other keys don't effect the result.
  // There are a bunch of permutations to try.

  // Try where second keys are not equal; vary third key
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  // Switch order of second keys
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1233, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  // Make second key equal and muck with the third key
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  ///////////////////////////////////////////////////////////////
  // Second suite here tests when the first keys are equal.

  // Try where second keys are not equal; vary third key
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(-1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(-1, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d1, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(-1, lessThan.execute(B,A,&runtimeCtxt));

  ///////////////////////////////////////////////////////////////
  // Third suite here tests when the first two keys are equal

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1231, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(-1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d2, A);
  recTy.setDate("cre_date", d1, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  recTy.setDate("cre_date", d1, A);
  recTy.setDate("cre_date", d1, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  delete lessThanFun;
  recTy.getFree().free(A);
  recTy.getFree().free(B);
}

BOOST_AUTO_TEST_CASE(testCompareNull)
{
  PlanCheckContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("cre_date", DateType::Get(ctxt, true)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22, true)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt, true)));
  RecordType recTy(ctxt, members);
  std::vector<SortKey> fields;
  fields.push_back(SortKey("akid"));
  
  TreculFunction * lessThanFun = CompareFunction::get(ctxt, &recTy, &recTy, fields, fields);
  TreculModule module(ctxt.getCodeGenerator().takeModule());
  TreculFunctionRuntime lessThan = module.getFunction<TreculFunctionRuntime>(lessThanFun->getReference());
  InterpreterContext runtimeCtxt;
  
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-04-07");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-09-09");

  RecordBuffer A = recTy.GetMalloc()->malloc();
  RecordBuffer B = recTy.GetMalloc()->malloc();

  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1fwYCKkjANwAACA8gDg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.setDate("cre_date", d1, A);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", A);
  recTy.setInt32("coop_id", 1233, A);
  recTy.setDate("cre_date", d2, B);
  recTy.setChar("akid", "vTF1NDG8m01MZwAAzAGIdg", B);
  recTy.setInt32("coop_id", 1231, B);
  BOOST_CHECK_EQUAL(0, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(0, lessThan.execute(B,A,&runtimeCtxt));
  
  recTy.getFieldAddress("akid").setNull(A);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(1, lessThan.execute(B,A,&runtimeCtxt));

  recTy.getFieldAddress("akid").setNull(B);
  BOOST_CHECK_EQUAL(-1, lessThan.execute(A,B,&runtimeCtxt));
  BOOST_CHECK_EQUAL(-1, lessThan.execute(B,A,&runtimeCtxt));

  delete lessThanFun;
  recTy.getFree().free(A);
  recTy.getFree().free(B);
}

BOOST_AUTO_TEST_CASE(testLoserTree)
{
  LoserTree<void*> t (8);

    // Inputs: Chosen so that we don't need a compare function.
    uint32_t inputs [8][4] = {{64, 62, 24, 8},
			    {32, 30, 22, 4},
			    {60, 36, 20, 10},
			    {56, 48, 34, 12},
			    {58, 46, 16, 2},
			    {66, 40, 26, 6},
			    {70, 44, 38, 8},
			    {72, 42, 28, 14}};
    std::size_t iters[8] = {0,0,0,0,0,0,0,0};

    // First 8 elements are priming the pump.
    for(int i=0; i<8; i++) {
      BOOST_CHECK(t.isHighSentinel());
      BOOST_CHECK_EQUAL(t.getInput(), 7u-i);
      t.update(7-i, inputs[7-i][iters[7-i]++], NULL);
    }
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(72u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(7u, t.getInput());
    t.update(7,inputs[7][iters[7]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(70u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(6u, t.getInput());
    t.update(6,inputs[6][iters[6]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(66u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(5u, t.getInput());
    t.update(5,inputs[5][iters[5]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(64u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(0u, t.getInput());
    t.update(0,inputs[0][iters[0]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(62u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(0u, t.getInput());
    t.update(0,inputs[0][iters[0]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(60u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(2u, t.getInput());
    t.update(2,inputs[2][iters[2]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(58u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(4u, t.getInput());
    t.update(4,inputs[4][iters[4]++], NULL);
    BOOST_CHECK(!t.isHighSentinel());
    BOOST_CHECK_EQUAL(56u, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(3u, t.getInput());
    t.update(3,inputs[3][iters[3]++], NULL);
}

void testLoserTree(uint32_t numInputs, uint32_t numElements)
{
  LoserTree<void*> t (numInputs);

    // Inputs: Key prefixes chosen unique so that we don't need a compare function.
  std::vector<std::vector<uint32_t> > inputs(numInputs);
  std::vector<std::size_t> iters(numInputs, 0);
  std::vector<std::vector<uint32_t>::const_iterator> inputIts;

  // Initialize the data by hashing values into slots
  for(uint32_t k = numElements; k>=1; --k) {
    uint32_t idx = SuperFastHash((char *) &k, sizeof(uint32_t), sizeof(uint32_t));    
    idx = idx % numInputs;
    inputs[idx].push_back(k);
  }
  for(std::size_t i=0; i<numInputs; i++) {
    inputIts.push_back(inputs[i].begin());
  }
  // First numInputs elements are priming the pump.
  for(uint32_t i=0; i<numInputs; i++) { 
    BOOST_CHECK(t.isHighSentinel()); 
    uint32_t idx = numInputs-i-1;
    BOOST_CHECK_EQUAL(t.getInput(), idx); 
    if (inputIts[idx] == inputs[idx].end()) {
      t.close(idx);
    } else {
      t.update(idx, *inputIts[idx]++, NULL);
    }
  }
  // The next numElements elements come out in order
  for(uint32_t k = numElements; k>=1; --k) {
    uint32_t idx = SuperFastHash((char *) &k, sizeof(uint32_t), sizeof(uint32_t));    
    idx = idx % numInputs;
    BOOST_CHECK(!t.isHighSentinel()); 
    BOOST_CHECK_EQUAL(k, t.getKeyPrefix());
    BOOST_CHECK_EQUAL(idx, t.getInput());    
    if (inputIts[idx] == inputs[idx].end()) {
      t.close(idx);
    } else {
      t.update(idx, *inputIts[idx]++, NULL);
    }
  }
}

BOOST_AUTO_TEST_CASE(testLoserTree2)
{
  // Try a mix of different numbers of inputs and total records to merge.
  testLoserTree(59, 1000);
  testLoserTree(64, 10000);
  testLoserTree(65, 1000);
  testLoserTree(590, 100000);
  testLoserTree(17, 1000);
  testLoserTree(4000, 100000);
  try {
    // Expected exception here.
    testLoserTree(8193, 100000);
    BOOST_CHECK(false);
  } catch(std::exception& ) {
  }
}

BOOST_AUTO_TEST_CASE(testDynamicBitsetSerialization)
{
  boost::dynamic_bitset<> a;
  a.resize(701, true);
  std::ostringstream s(std::ios_base::binary | std::ios_base::out);
  boost::archive::binary_oarchive oa(s);      
  oa << BOOST_SERIALIZATION_NVP(a);
  
  std::string buf = s.str();
  boost::iostreams::stream<boost::iostreams::array_source> archiveStream(boost::iostreams::array_source(&buf[0] ,buf.size()));
  boost::archive::binary_iarchive ia(archiveStream);
  boost::dynamic_bitset<> b;
  BOOST_CHECK_EQUAL(0u, b.size());
  ia >> BOOST_SERIALIZATION_NVP(b);
  BOOST_CHECK_EQUAL(701U, b.size());
  BOOST_CHECK_EQUAL(b.count(), b.size());
}

BOOST_AUTO_TEST_CASE(testSortRun)
{
  {
    SortRun sr(1024);
    for(int i = 0; i<7; i++) {
      // This uses 16*8 + 128*7 bytes = 1024
      // NOTE: 8 is the minimum number of sort nodes alloced.
      BOOST_CHECK(sr.push_back(SortNode(), 128));
    }
    BOOST_CHECK(!sr.push_back(SortNode(), 128));
    BOOST_CHECK_EQUAL(8, sr.capacity());
  }
  {
    SortRun sr(1024);
    for(int i = 0; i<8; i++) {
      // This uses 8*80 = 640 bytes
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(8, sr.capacity());
    }
    // Need extra room to fill up for estimate of 
    // 12 records.  For double buffer only need
    // 12*16 = 192 so this fits
    BOOST_CHECK(sr.push_back(SortNode(), 64));
    BOOST_CHECK_EQUAL(12, sr.capacity());
    for(int i = 9; i<12; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(12, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }    
    BOOST_CHECK(!sr.push_back(SortNode(), 64));
  }
  {
    SortRun sr(808);
    for(int i = 0; i<8; i++) {
      // This uses 8*80 = 640 bytes
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(8, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }
    // Need extra room to fill up for estimate of 
    // 12 records.  For double buffer we need
    // 12*16 = 192 so this doesn't fit.  We only have
    // room for 10*16=160 in the double buffer, we do
    // that then we have room for 808-672 bytes of data.
    BOOST_CHECK(sr.push_back(SortNode(), 16));
    BOOST_CHECK_EQUAL(10, sr.capacity());
    for(int i = 9; i<10; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 16));
      BOOST_CHECK_EQUAL(10, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }    
    BOOST_CHECK(!sr.push_back(SortNode(), 16));
  }
  {
    SortRun sr(1024);
    for(int i = 0; i<8; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(8, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }
    BOOST_CHECK(sr.push_back(SortNode(), 64));
    BOOST_CHECK_EQUAL(12, sr.capacity());
    // We got an underestimate of record size so
    // we waste some space on SortNodes
    BOOST_CHECK(!sr.push_back(SortNode(), 1024));
  }
  {
    SortRun sr(1024);
    for(int i = 0; i<8; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(8, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }
    BOOST_CHECK(sr.push_back(SortNode(), 64));
    BOOST_CHECK_EQUAL(12, sr.capacity());
    // We got an overestimate of record size but 
    // there isn't enough room to reallocate.
    // At the end of the following loop we have
    // 9*80 + 3*76 = 954 bytes.
    for(int i = 9; i<12; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 60));
      BOOST_CHECK_EQUAL(12, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }    
    BOOST_CHECK(!sr.push_back(SortNode(), 64));
  }
  {
    SortRun sr(1024);
    for(int i = 0; i<8; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(8, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }
    BOOST_CHECK(sr.push_back(SortNode(), 64));
    BOOST_CHECK_EQUAL(12, sr.capacity());
    // We got an overestimate of record size but 
    // there actually IS enough room to realloc.
    // At the end of the following loop we have
    // 9*80 + 3*32 = 816 bytes.   This leaves us with
    // room to realloc to 13 slots; but this isn't 
    // enough to warrant doing it so it is skipped.
    for(int i = 9; i<12; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 16));
      BOOST_CHECK_EQUAL(12, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }    
    BOOST_CHECK(!sr.push_back(SortNode(), 64));
  }
  {
    SortRun sr(1024);
    for(int i = 0; i<8; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 64));
      BOOST_CHECK_EQUAL(8, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }
    BOOST_CHECK(sr.push_back(SortNode(), 64));
    BOOST_CHECK_EQUAL(12, sr.capacity());
    // We got an overestimate of record size but 
    // there actually IS enough room to realloc.
    // At the end of the following loop we have
    // 9*80 + 3*17 = 771 bytes.   This leaves us with
    // room to realloc to 15 slots which is more
    // than a 10% increase so we do it.
    for(int i = 9; i<12; i++) {
      BOOST_CHECK(sr.push_back(SortNode(), 1));
      BOOST_CHECK_EQUAL(12, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }    
    for(int i = 12; i<15; i++) {    
      BOOST_CHECK(sr.push_back(SortNode(), 1));
      BOOST_CHECK_EQUAL(15, sr.capacity());
      BOOST_CHECK_EQUAL(i+1, sr.size());
    }
    BOOST_CHECK(!sr.push_back(SortNode(), 1));
  }
}

std::pair<RuntimeGenerateOperatorType *, const RecordType *>
make_generate_operator_type(PlanCheckContext & ctxt, const std::string & prog, uint64_t upperBound)
{
  // Create the state type here.
  std::vector<RecordMember> members;
  RecordType emptyType(ctxt, members);
  members.push_back(RecordMember("RECORDCOUNT", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITIONCOUNT", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITION", Int32Type::Get(ctxt)));
  RecordType stateType(ctxt, members);

  std::vector<AliasedRecordType> inputAndEmpty;
  inputAndEmpty.push_back(AliasedRecordType("input", &emptyType));
  inputAndEmpty.push_back(AliasedRecordType("input", &emptyType));
  TreculFunction f(ctxt, ctxt.getCodeGenerator(), "myLoopUpperBound", inputAndEmpty, 
                   boost::lexical_cast<std::string>(upperBound));

  std::vector<AliasedRecordType> inputAndState;
  inputAndState.push_back(AliasedRecordType("input", &emptyType));
  inputAndState.push_back(AliasedRecordType("state", &stateType));
  TreculTransfer2 xfer(ctxt, ctxt.getCodeGenerator(), "myGenerate", inputAndState, prog);
  auto opType = new RuntimeGenerateOperatorType("generate",
                                                TreculFreeOperation(ctxt.getCodeGenerator(), &emptyType),
                                                &stateType,
                                                TreculFreeOperation(ctxt.getCodeGenerator(), &stateType),
                                                xfer,
                                                &f);
  return std::make_pair(std::move(opType), xfer.getTarget());
}

BOOST_AUTO_TEST_CASE(testSimpleScheduler)
{
  PlanCheckContext ctxt;
  DataflowScheduler scheduler;
  RuntimeGenerateOperatorType * opType1;
  const RecordType * opType1OutputType;
  std::tie(opType1, opType1OutputType) = make_generate_operator_type(ctxt, "'this is ground control...' AS a", 24);

  RuntimeDevNullOperatorType opType2(TreculFreeOperation(ctxt.getCodeGenerator(), opType1OutputType));

  TreculModule module(ctxt.getCodeGenerator().takeModule());
  opType1->loadFunctions(module);
  opType2.loadFunctions(module);

  RuntimeGenerateOperator op1(scheduler, *opType1);
  RuntimeDevNullOperator op2(scheduler, opType2);
  
  InProcessFifo fifo(scheduler, scheduler);
  op1.addOutputPort(fifo.getSource());
  fifo.getSource()->setOperator(op1);
  op2.addInputPort(fifo.getTarget());
  fifo.getTarget()->setOperator(op2);

  // Assign the operators in the graph to scheduler.
  std::vector<RuntimeOperator *> ops;
  ops.push_back(&op1);
  ops.push_back(&op2);
  scheduler.setOperators(ops);

  // Do the work
  scheduler.run();
  scheduler.cleanup();
  delete opType1;
}

BOOST_AUTO_TEST_CASE(testSimpleSchedulerWithExpression)
{
  PlanCheckContext ctxt;
  DataflowScheduler scheduler;
  RuntimeGenerateOperatorType * opType1;
  const RecordType * opType1OutputType;
  std::tie(opType1, opType1OutputType) = make_generate_operator_type(ctxt, "'this is ground control...' AS a", 24);
  opType1OutputType->dump();
  TreculTransfer t(ctxt, ctxt.getCodeGenerator(), "xfer5", opType1OutputType, "input.*, a+a AS b, a AS c");
  t.getTarget()->dump();
  std::vector<const TreculTransfer *> transfers;
  transfers.push_back(&t);
  RuntimeCopyOperatorType opType2(TreculFreeOperation(ctxt.getCodeGenerator(), opType1OutputType), transfers);
  RuntimeDevNullOperatorType opType3(TreculFreeOperation(ctxt.getCodeGenerator(), t.getTarget()));

  TreculModule module(ctxt.getCodeGenerator().takeModule());
  opType1->loadFunctions(module);
  opType2.loadFunctions(module);
  opType3.loadFunctions(module);
  
  RuntimeGenerateOperator op1(scheduler, *opType1);
  RuntimeCopyOperator op2(scheduler, opType2);
  RuntimeDevNullOperator op3(scheduler, opType3);
  
  InProcessFifo fifo1(scheduler, scheduler);
  op1.addOutputPort(fifo1.getSource());
  fifo1.getSource()->setOperator(op1);
  op2.addInputPort(fifo1.getTarget());
  fifo1.getTarget()->setOperator(op2);
  InProcessFifo fifo2(scheduler, scheduler);
  op2.addOutputPort(fifo2.getSource());
  fifo2.getSource()->setOperator(op2);
  op3.addInputPort(fifo2.getTarget());
  fifo2.getTarget()->setOperator(op3);

  //Assign the operators in the graph to scheduler.
  std::vector<RuntimeOperator *> ops;
  ops.push_back(&op1);
  ops.push_back(&op2);
  ops.push_back(&op3);
  scheduler.setOperators(ops);

  // //Do the work
  scheduler.run();
  scheduler.cleanup();
  delete opType1;
}

// BOOST_AUTO_TEST_CASE(testSimpleSchedulerWithHashGroupBy)
// {
//   PlanCheckContext ctxt;
//   DataflowScheduler scheduler;
  // RuntimeGenerateOperatorType * opType1;
  // const RecordType * opType1OutputType;
//   std::tie(opType1, opType1OutputType) = make_generate_operator_type(ctxt, "'this is ground control...' AS a", 24);
//   opType1OutputType->dump();
//   TreculTransfer init(ctxt, ctxt.getCodeGenerator(), "xfer5init", opType1OutputType, "a AS a1, 0 AS cnt");
//   std::vector<const RecordType *> types;
//   types.push_back(opType1OutputType);
//   types.push_back(init.getTarget());
//   RecordTypeInPlaceUpdate up(ctxt, "xfer5up", types, "SET cnt = cnt + 1");
//   RuntimeHashGroupByOperatorType opType2(*opType1OutputType->GetFree(), 0, &init, &up);
//   RuntimeDevNullOperatorType opType3(init.getTarget());

//   RuntimeGenerateOperator op1(scheduler, opType1);
//   RuntimeHashGroupByOperator op2(scheduler, opType2);
//   RuntimeDevNullOperator op3(scheduler, opType3);
  
//   InProcessFifo fifo1(scheduler, scheduler);
//   op1.addOutputPort(fifo1.getSource());
//   fifo1.getSource()->setOperator(op1);
//   op2.addInputPort(fifo1.getTarget());
//   fifo1.getTarget()->setOperator(op2);
//   InProcessFifo fifo2(scheduler, scheduler);
//   op2.addOutputPort(fifo2.getSource());
//   fifo2.getSource()->setOperator(op2);
//   op3.addInputPort(fifo2.getTarget());
//   fifo2.getTarget()->setOperator(op3);

//   //Assign the operators in the graph to scheduler.
//   std::vector<RuntimeOperator *> ops;
//   ops.push_back(&op1);
//   ops.push_back(&op2);
//   ops.push_back(&op3);
//   scheduler.setOperators(ops);

//   // //Do the work
//   scheduler.run();
//   scheduler.cleanup();
// }

template <class _OpType, class _Op>
void simpleSchedulerWithGroupBy()
{
  PlanCheckContext ctxt;
  DataflowScheduler scheduler;
  RuntimeGenerateOperatorType * opType1;
  const RecordType * opType1OutputType;
  std::tie(opType1, opType1OutputType) = make_generate_operator_type(ctxt, "'this is ground control...' AS a, RECORDCOUNT/5 AS b, 77 AS c", 24);
  std::vector<std::string> groupByKeys;
  groupByKeys.push_back("b");
  std::vector<AggregateFunctionSpec> aggs;
  aggs.push_back(AggregateFunctionSpec("*", AggregateFunctionSpec::COUNT, "cnt"));
  aggs.push_back(AggregateFunctionSpec("c", AggregateFunctionSpec::SUM, "s"));
  GroupBy<_OpType> groupBy(ctxt, opType1OutputType, groupByKeys, aggs);
  RuntimePrintOperatorType opType3(groupBy.getOutputType(), TreculPrintOperation(ctxt.getCodeGenerator(), groupBy.getOutputType()), 5, 1, nullptr, nullptr, nullptr);
  RuntimeDevNullOperatorType opType4(TreculFreeOperation(ctxt.getCodeGenerator(), groupBy.getOutputType()));

  TreculModule module(ctxt.getCodeGenerator().takeModule());
  opType1->loadFunctions(module);
  groupBy.getOpType().loadFunctions(module);
  opType3.loadFunctions(module);
  opType4.loadFunctions(module);
  
  RuntimeGenerateOperator op1(scheduler, *opType1);
  _Op op2(scheduler, groupBy.getOpType());
  RuntimePrintOperator op3(scheduler, opType3);
  RuntimeDevNullOperator op4(scheduler, opType4);
  
  InProcessFifo fifo1(scheduler, scheduler);
  op1.addOutputPort(fifo1.getSource());
  fifo1.getSource()->setOperator(op1);
  op2.addInputPort(fifo1.getTarget());
  fifo1.getTarget()->setOperator(op2);
  InProcessFifo fifo2(scheduler, scheduler);
  op2.addOutputPort(fifo2.getSource());
  fifo2.getSource()->setOperator(op2);
  op3.addInputPort(fifo2.getTarget());
  fifo2.getTarget()->setOperator(op3);
  InProcessFifo fifo3(scheduler, scheduler);
  op3.addOutputPort(fifo3.getSource());
  fifo3.getSource()->setOperator(op3);
  op4.addInputPort(fifo3.getTarget());
  fifo3.getTarget()->setOperator(op4);

  //Assign the operators in the graph to scheduler.
  std::vector<RuntimeOperator *> ops;
  ops.push_back(&op1);
  ops.push_back(&op2);
  ops.push_back(&op3);
  ops.push_back(&op4);
  scheduler.setOperators(ops);

  // //Do the work
  scheduler.run();
  scheduler.cleanup();
  delete opType1;
}

BOOST_AUTO_TEST_CASE(testSimpleSchedulerWithHashGroupBy)
{
  std::cout << "testSimpleSchedulerWithHashGroupBy" << std::endl;
  simpleSchedulerWithGroupBy<RuntimeHashGroupByOperatorType, RuntimeHashGroupByOperator>();
}

BOOST_AUTO_TEST_CASE(testSimpleSchedulerWithSortGroupBy)
{
  std::cout << "testSimpleSchedulerWithSortGroupBy" << std::endl;
  simpleSchedulerWithGroupBy<RuntimeSortGroupByOperatorType, RuntimeSortGroupByOperator>();
}

BOOST_AUTO_TEST_CASE(testSimpleSchedulerWithHashJoin)
{
  PlanCheckContext ctxt;
  DataflowScheduler scheduler;
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(ctxt, emptyMembers);
  RuntimeGenerateOperatorType * opType1;
  const RecordType * opType1OutputType;
  std::tie(opType1, opType1OutputType) = make_generate_operator_type(ctxt, "RECORDCOUNT AS a", 24);
  RuntimeGenerateOperatorType * opType2;
  const RecordType * opType2OutputType;
  std::tie(opType2, opType2OutputType) = make_generate_operator_type(ctxt, "RECORDCOUNT+RECORDCOUNT AS c, RECORDCOUNT AS b", 12);
  std::vector<const RecordType *> tableOnly;
  tableOnly.push_back(opType1OutputType);
  tableOnly.push_back(&emptyTy);
  std::vector<const RecordType *> probeOnly;
  probeOnly.push_back(opType2OutputType);
  probeOnly.push_back(&emptyTy);
  std::vector<const RecordType *> tableAndProbe;
  tableAndProbe.push_back(opType1OutputType);
  tableAndProbe.push_back(opType2OutputType);
  TreculFunction tableHash(ctxt, ctxt.getCodeGenerator(), "tableHash", tableOnly, "#(a)");
  TreculFunction probeHash(ctxt, ctxt.getCodeGenerator(), "probeHash", probeOnly, "#(b)");
  //Table is first argument in equals!
  TreculFunction equals (ctxt, ctxt.getCodeGenerator(), "eq", tableAndProbe, "a = b");
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", opType1OutputType));
  types.push_back(AliasedRecordType("probe", opType2OutputType));
  TreculTransfer2 output(ctxt, ctxt.getCodeGenerator(), "makeoutput", types, "table.*, probe.*");

  RuntimeHashJoinOperatorType opType3(TreculFreeOperation(ctxt.getCodeGenerator(), opType1OutputType),
				      TreculFreeOperation(ctxt.getCodeGenerator(), opType2OutputType),
				      tableHash,
				      probeHash,
				      equals,
				      output);

  RuntimeDevNullOperatorType opType4(TreculFreeOperation(ctxt.getCodeGenerator(), output.getTarget()));

  TreculModule module(ctxt.getCodeGenerator().takeModule());
  opType1->loadFunctions(module);
  opType2->loadFunctions(module);
  opType3.loadFunctions(module);
  opType4.loadFunctions(module);
  
  RuntimeGenerateOperator op1(scheduler, *opType1);
  RuntimeGenerateOperator op2(scheduler, *opType2);
  RuntimeHashJoinOperator op3(scheduler, opType3);
  RuntimeDevNullOperator op4(scheduler, opType4);
  
  //Table is port 0
  InProcessFifo fifo1(scheduler, scheduler);
  op1.addOutputPort(fifo1.getSource());
  fifo1.getSource()->setOperator(op1);
  op3.addInputPort(fifo1.getTarget());
  fifo1.getTarget()->setOperator(op3);
  //Probe is port 1
  InProcessFifo fifo2(scheduler, scheduler);
  op2.addOutputPort(fifo2.getSource());
  fifo2.getSource()->setOperator(op2);
  op3.addInputPort(fifo2.getTarget());
  fifo2.getTarget()->setOperator(op3);
  //Output of join
  InProcessFifo fifo3(scheduler, scheduler);
  op3.addOutputPort(fifo3.getSource());
  fifo3.getSource()->setOperator(op3);
  op4.addInputPort(fifo3.getTarget());
  fifo3.getTarget()->setOperator(op4);

  //Assign the operators in the graph to scheduler.
  std::vector<RuntimeOperator *> ops;
  ops.push_back(&op1);
  ops.push_back(&op2);
  ops.push_back(&op3);
  ops.push_back(&op4);
  scheduler.setOperators(ops);

  //Do the work
  scheduler.run();
  scheduler.cleanup();
  delete opType1;
  delete opType2;
}


BOOST_AUTO_TEST_CASE(testSortMergeJoin)
{
  PlanCheckContext ctxt;
  RuntimeGenerateOperatorType * leftType;
  const RecordType * leftOutputType;
  std::tie(leftType, leftOutputType) = make_generate_operator_type(ctxt, "2*RECORDCOUNT AS a", 5);
  std::vector<SortKey> leftKeys;
  leftKeys.push_back(SortKey("a"));
  RuntimeGenerateOperatorType * rightType;
  const RecordType * rightOutputType;
  std::tie(rightType, rightOutputType) = make_generate_operator_type(ctxt, "RECORDCOUNT AS b", 10);
  std::vector<SortKey> rightKeys;
  rightKeys.push_back(SortKey("b"));
  
  SortMergeJoin smj(ctxt, 
		    SortMergeJoin::RIGHT_OUTER,
		    leftOutputType,
		    rightOutputType,
		    leftKeys,
		    rightKeys,
		    "a=b",
		    "a,b");
  RuntimeOperatorType * joinType = smj.create();
  RuntimePrintOperatorType * printType = new RuntimePrintOperatorType(smj.getOutputType(), TreculPrintOperation(ctxt.getCodeGenerator(), smj.getOutputType()), 10, 1, nullptr, nullptr, nullptr);
  RuntimeDevNullOperatorType * devNullType = new RuntimeDevNullOperatorType(TreculFreeOperation(ctxt.getCodeGenerator(), smj.getOutputType()));
  RuntimeOperatorPlan plan(1,true);
  plan.addOperatorType(leftType);
  plan.addOperatorType(rightType);				
  plan.addOperatorType(joinType);				
  plan.addOperatorType(printType);				
  plan.addOperatorType(devNullType);				
  plan.connectStraight(leftType, 0, joinType, RuntimeSortMergeJoinOperatorType::LEFT_PORT, true, true);
  plan.connectStraight(rightType, 0, joinType, RuntimeSortMergeJoinOperatorType::RIGHT_PORT, true, true);
  plan.connectStraight(joinType, 0, printType, 0, true, true);
  plan.connectStraight(printType, 0, devNullType, 0, true, true);
  plan.setModule(std::make_unique<TreculModule>(ctxt.getCodeGenerator().takeModule()));
  plan.loadFunctions();
  RuntimeProcess p;
  p.init(0,0,1,plan);
  p.run();
}

BOOST_AUTO_TEST_CASE(testSortMerge)
{
  std::cout << "testSortMerge" << std::endl;
  PlanCheckContext ctxt;
  RuntimeGenerateOperatorType * leftType;
  const RecordType * leftOutputType;
  std::tie(leftType, leftOutputType) = make_generate_operator_type(ctxt, "2*RECORDCOUNT AS a", 10);
  RuntimeGenerateOperatorType * rightType;
  const RecordType * rightOutputType;
  std::tie(rightType, rightOutputType) = make_generate_operator_type(ctxt, "RECORDCOUNT AS a", 20);
  std::vector<std::string> leftKeys;
  leftKeys.push_back("a");
  
  SortMerge sm(ctxt, 
	       leftOutputType,
	       leftKeys);
  RuntimeOperatorType * joinType = sm.create();
  RuntimePrintOperatorType * printType = new RuntimePrintOperatorType(sm.getOutputType(), TreculPrintOperation(ctxt.getCodeGenerator(), sm.getOutputType()), 30, 1, nullptr, nullptr, nullptr);
  RuntimeDevNullOperatorType * devNullType = new RuntimeDevNullOperatorType(TreculFreeOperation(ctxt.getCodeGenerator(), sm.getOutputType()));
  RuntimeOperatorPlan plan(1,true);
  plan.addOperatorType(leftType);
  plan.addOperatorType(rightType);				
  plan.addOperatorType(joinType);				
  plan.addOperatorType(printType);				
  plan.addOperatorType(devNullType);				
  plan.connectStraight(leftType, 0, joinType, 0, true, true);
  plan.connectStraight(rightType, 0, joinType, 1, true, true);
  plan.connectStraight(joinType, 0, printType, 0, true, true);
  plan.connectStraight(printType, 0, devNullType, 0, true, true);
  plan.setModule(std::make_unique<TreculModule>(ctxt.getCodeGenerator().takeModule()));
  plan.loadFunctions();
  RuntimeProcess p;
  p.init(0,0,1,plan);
  p.run();
}

BOOST_AUTO_TEST_CASE(testSort)
{
  std::cout << "testSort" << std::endl;
  PlanCheckContext ctxt;
  DataflowGraphBuilder gb(ctxt);
  gb.buildGraph("a = generate[output=\"100 - 2*RECORDCOUNT AS a\", numRecords=10];\n"
		"b = sort[key=\"a\"];\n"
		"c = print[numToPrint=10];\n"
		"d = devNull[];\n"
		"a -> b;\n"
		"b -> c;\n"
		"c -> d;\n"
		);
  std::shared_ptr<RuntimeOperatorPlan> plan = gb.create(1);
  RuntimeProcess p;
  p.init(0,0,1,*plan.get());
  p.run();
}

class RuntimeExceptionOperatorType : public RuntimeOperatorType
{
  friend class RuntimeExceptionOperator;
private:
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
  }
public:
  /**
   * NULL predicate means TRUE.
   */
  RuntimeExceptionOperatorType()
    :
    RuntimeOperatorType("RuntimeExceptionOperatorType")
  {
  }
  ~RuntimeExceptionOperatorType() 
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  void loadFunctions(TreculModule & m) override
  {
  }
};

class RuntimeExceptionOperator : public RuntimeOperatorBase<RuntimeExceptionOperatorType>
{
public:
  RuntimeExceptionOperator(RuntimeOperator::Services& services, 
                           const RuntimeExceptionOperatorType& opType)
    :
    RuntimeOperatorBase<RuntimeExceptionOperatorType>(services, opType)
  {
  }
    
  ~RuntimeExceptionOperator() {}
  void start()
  {
    throw std::runtime_error("RuntimeExceptionOperator throw");
  }
  void onEvent(RuntimePort * ) {}
  void shutdown() {}
};

RuntimeOperator * RuntimeExceptionOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeExceptionOperator(s, *this);
}

BOOST_DATA_TEST_CASE(testException, boost::unit_test::data::make({ 1, 2, 4, 16}), num_partitions)
{
  PlanCheckContext ctxt;
  RuntimeExceptionOperatorType * leftType = new RuntimeExceptionOperatorType();
  leftType->setPartitionConstraint(RuntimePartitionConstraint({0}));
  RecordType emptyTy(ctxt, {});
  TreculFunction trivialHash(ctxt, ctxt.getCodeGenerator(), "hash", std::vector<const RecordType*>({ &emptyTy, &emptyTy }), "0");
  RuntimeHashPartitionerOperatorType * partType = new RuntimeHashPartitionerOperatorType(trivialHash);
  partType->setPartitionConstraint(RuntimePartitionConstraint({0}));
  TreculFreeOperation freeOp(ctxt.getCodeGenerator(), &emptyTy);
  RuntimeDevNullOperatorType * dnType = new RuntimeDevNullOperatorType(freeOp);
  RuntimeOperatorPlan plan(num_partitions,true);
  plan.addOperatorType(leftType);
  plan.addOperatorType(partType);
  plan.addOperatorType(dnType);
  plan.connectStraight(leftType, 0, partType, 0, true, true);
  plan.connectBroadcast(partType, dnType, 0, &emptyTy, freeOp.getReference(), true, true);
  plan.setModule(std::make_unique<TreculModule>(ctxt.getCodeGenerator().takeModule()));
  plan.loadFunctions();
  RuntimeProcess p;
  p.init(0,num_partitions-1,num_partitions,plan);
  try {
    p.run();
    BOOST_CHECK(false);
  } catch (std::exception & ex) {
    BOOST_CHECK(boost::algorithm::equals("RuntimeExceptionOperator throw", ex.what()));
  }
}

struct SerialOrganizedTableTestFixtureBase
{
  std::array<std::string, 3> testdata_;
  std::vector<std::unique_ptr<test_gz>> files_;
  SerialOrganizedTableTestFixtureBase(const std::array<std::string, 3> & testdata)
    :
    testdata_(testdata)
  {
    std::filesystem::create_directory("./data", ".");
    std::filesystem::create_directory("./data/1_1", ".");
    std::filesystem::create_directory("./data/1_1/mytable", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1/1", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1/1/2024-01-01", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1/1/2024-01-01/7C4D89943F62499A9AF57475FB5057D5", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1/1/2024-01-01/13ACC820567845EB9ACB0C764F460546", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1/1/2024-01-02", ".");
    std::filesystem::create_directory("./data/1_1/mytable/1/1/2024-01-02/13ACC820567845EB9ACB0C764F460546", ".");
    files_.push_back(std::make_unique<test_gz>("./data/1_1/mytable/1/1/2024-01-01/7C4D89943F62499A9AF57475FB5057D5/serial_00000.gz", testdata_[0]));
    files_.push_back(std::make_unique<test_gz>("./data/1_1/mytable/1/1/2024-01-01/13ACC820567845EB9ACB0C764F460546/serial_00000.gz", testdata_[1]));
    files_.push_back(std::make_unique<test_gz>("./data/1_1/mytable/1/1/2024-01-02/13ACC820567845EB9ACB0C764F460546/serial_00000.gz", testdata_[2]));
  }
  SerialOrganizedTableTestFixtureBase(const std::string & testdata)
    :
    SerialOrganizedTableTestFixtureBase({ testdata, testdata, testdata })
  {
  }
  
  virtual ~SerialOrganizedTableTestFixtureBase()
  {
    std::filesystem::remove_all("./data");
  }

  std::string runTableScan(std::shared_ptr<TableMetadata> metadata, const char * predicate=nullptr)
  {
    PlanCheckContext ctxt;
    LogicalPlan plan(ctxt);
    auto tablescan = new LogicalTableParser("mytable", metadata);
    tablescan->addParam("connect", (boost::format("file://%1%") % (std::filesystem::current_path() / "data").native()).str());
    if (nullptr != predicate) {
      tablescan->addParam("where", predicate);
    }
    plan.addOperator(tablescan);
    auto filewrite = new LogicalFileWrite();
    filewrite->addParam("file", LogicalOperator::param_type((std::filesystem::current_path() / "data/output.txt").native()));
    filewrite->addParam("mode", LogicalOperator::param_type("text"));
    plan.addOperator(filewrite);
    plan.addEdge(tablescan, filewrite);
    auto runtimePlan = DataflowGraphBuilder::create(plan, 1);
    RuntimeProcess p;
    p.init(0,0,1,*runtimePlan.get());
    p.run();
    std::ifstream resultFile(std::filesystem::current_path() / "data/output.txt");
    std::stringstream result;
    std::copy(std::istreambuf_iterator<char>(resultFile),
              std::istreambuf_iterator<char>(),
              std::ostreambuf_iterator<char>(result));
    return result.str();
  }
};

struct SerialOrganizedTableTestFixture : public SerialOrganizedTableTestFixtureBase
{
  SerialOrganizedTableTestFixture()
    :
    SerialOrganizedTableTestFixtureBase("ssdfasdfa;sdkfjals\n")
  {
  }
  static std::shared_ptr<TableMetadata> getMyTableMetadata()
  {
    std::vector<SortKey> mde_log_sort; 
    std::string tableFormat("a VARCHAR");
    auto m = std::make_shared<TableMetadata>("mytable",
                                             tableFormat,
                                             mde_log_sort);

    // Create a single column group for the whole table.                                                                                                                                                             
    TableColumnGroup * cg = m->addDefaultColumnGroup();
    {
      // Version 1 of the table                                                                                                                                                                                      
      std::map<std::string, std::string> computedColumns;
      TableFileMetadata * f = new TableFileMetadata(tableFormat,
                                                    computedColumns);
      cg->add(1, f);
    }

    return m;
  }
};

BOOST_FIXTURE_TEST_CASE(testBind, SerialOrganizedTableTestFixture)
{
  SerialOrganizedTable t(1, 1, "mytable");
  std::string p = (boost::format("file://%1%") % std::filesystem::absolute(std::filesystem::path(".") / "data").generic_string()).str();
  FileSystem * fs = FileSystem::get(URI::get(p.c_str()));
  t.bind(fs);
  BOOST_REQUIRE_EQUAL(3U, t.getSerialPaths().size());
  for(auto f : t.getSerialPaths()) {
    BOOST_CHECK_EQUAL(1, f->getMinorVersion());
    BOOST_CHECK(boost::algorithm::equals("2024-01-01", f->getDate()) ||
                boost::algorithm::equals("2024-01-02", f->getDate()));
  }
  FileSystem::release(fs);
}

BOOST_FIXTURE_TEST_CASE(testBindWithPredicate, SerialOrganizedTableTestFixture)
{
  SerialOrganizedTable t(1, 1, "mytable","Date = '2024-01-01'");
  std::string p = (boost::format("file://%1%") % std::filesystem::absolute(std::filesystem::path(".") / "data").generic_string()).str();
  FileSystem * fs = FileSystem::get(URI::get(p.c_str()));
  t.bind(fs);
  BOOST_REQUIRE_EQUAL(2U, t.getSerialPaths().size());
  for(auto f : t.getSerialPaths()) {
    BOOST_CHECK_EQUAL(1, f->getMinorVersion());
    BOOST_CHECK(boost::algorithm::equals("2024-01-01", f->getDate()));
  }
  FileSystem::release(fs);
}

BOOST_FIXTURE_TEST_CASE(testTableOperator, SerialOrganizedTableTestFixture)
{
  std::stringstream expected;
  expected << testdata_[0] << testdata_[1] << testdata_[2];
  BOOST_CHECK_EQUAL(expected.str(), runTableScan(getMyTableMetadata()));
}

BOOST_FIXTURE_TEST_CASE(testTableOperatorWithDatePredicate, SerialOrganizedTableTestFixture)
{
  std::stringstream expected;
  expected << testdata_[0] << testdata_[1];
  BOOST_CHECK_EQUAL(expected.str(), runTableScan(getMyTableMetadata(), "Date = '2024-01-01'"));
}

BOOST_FIXTURE_TEST_CASE(testTableOperatorWithDatePredicate2, SerialOrganizedTableTestFixture)
{
  std::stringstream expected;
  expected << testdata_[2];
  BOOST_CHECK_EQUAL(expected.str(), runTableScan(getMyTableMetadata(), "Date = '2024-01-02'"));
}

struct SerialOrganizedSortedTableTestFixture : public SerialOrganizedTableTestFixtureBase
{
  SerialOrganizedSortedTableTestFixture()
    :
    SerialOrganizedTableTestFixtureBase({
        "1\tone\n"
        "2\ttwo\n",
        "3\tthree\n"
        "4\tfour\n",
        "5\tfive\n"
        "6\tsix\n" })
  {
  }
  static std::shared_ptr<TableMetadata> getMyTableMetadata()
  {
    std::vector<SortKey> mde_log_sort( { SortKey("a", SortKey::ASC) }); 
    std::string tableFormat("a INTEGER, b VARCHAR");
    auto m = std::make_shared<TableMetadata>("mytable",
                                             tableFormat,
                                             mde_log_sort);

    // Create a single column group for the whole table.                                                                                                                                                          
    TableColumnGroup * cg = m->addDefaultColumnGroup();
    {
      // Version 1 of the table                                                                                                                                                                                      
      std::map<std::string, std::string> computedColumns;
      TableFileMetadata * f = new TableFileMetadata(tableFormat,
                                                    computedColumns);
      cg->add(1, f);
    }

    return m;
  }
};

BOOST_FIXTURE_TEST_CASE(testTableOperatorSorted, SerialOrganizedSortedTableTestFixture)
{
  std::stringstream expected;
  expected << testdata_[0] << testdata_[1] << testdata_[2];
  BOOST_CHECK_EQUAL(expected.str(), runTableScan(getMyTableMetadata()));
}

BOOST_FIXTURE_TEST_CASE(testTableOperatorSortedWithDatePredicate, SerialOrganizedTableTestFixture)
{
  std::stringstream expected;
  expected << testdata_[0] << testdata_[1];
  BOOST_CHECK_EQUAL(expected.str(), runTableScan(getMyTableMetadata(), "Date = '2024-01-01'"));
}

BOOST_FIXTURE_TEST_CASE(testTableOperatorSortedWithDatePredicate2, SerialOrganizedTableTestFixture)
{
  std::stringstream expected;
  expected << testdata_[2];
  BOOST_CHECK_EQUAL(expected.str(), runTableScan(getMyTableMetadata(), "Date = '2024-01-02'"));
}

