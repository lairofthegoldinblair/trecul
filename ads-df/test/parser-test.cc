/**
 * Copyright (c) 2013, Akamai Technologies
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
#include <boost/date_time/posix_time/posix_time.hpp>

#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>

#include "AsyncRecordParser.hh"

BOOST_AUTO_TEST_CASE(testConsumeTerminatedString)
{
  const char * testString = "abcdefghijklmnopqrstwxyz";
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('n', *static_cast<const char *>(blk.data()));
  }
  {
    AsyncDataBlock blk((uint8_t *) testString, 5);
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    blk = AsyncDataBlock((uint8_t *) (testString + 5), strlen(testString)-5);
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('n', *static_cast<const char *>(blk.data()));
  }
  {
    AsyncDataBlock blk((uint8_t *) testString, 5);
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    blk = AsyncDataBlock((uint8_t *) (testString + 5), 6);
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    blk = AsyncDataBlock((uint8_t *) (testString + 5), strlen(testString)-5);
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('n', *static_cast<const char *>(blk.data()));
  }
  {
    AsyncDataBlock blk((uint8_t *) testString, 5);
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isError());
  }
}

BOOST_AUTO_TEST_CASE(testImportDecimalInt32)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, false)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "1392342\t";
  recTy.getFieldAddress("a").setInt32(0, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportDecimalInteger<Int32Type> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(1392342, recTy.getFieldAddress("a").getInt32(buf));
  }
  recTy.getFieldAddress("a").setInt32(0, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportDecimalInteger<Int32Type> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0, recTy.getFieldAddress("a").getInt32(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), strlen(testString)-3);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(1392342, recTy.getFieldAddress("a").getInt32(buf));
  }
  for(int32_t i=0; i<=strlen(testString); ++i) {
    std::array<AsyncDataBlock, 2> blks = { AsyncDataBlock(const_cast<char *>(testString), i), AsyncDataBlock(const_cast<char *>(testString)+i, strlen(testString) - i) };
    ImportDecimalInteger<Int32Type> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blks, buf).isSuccess());
    BOOST_CHECK(boost::asio::buffer_size(blks) > 0);
    BOOST_CHECK_EQUAL('\t', *boost::asio::buffers_begin(blks));
    BOOST_CHECK_EQUAL(1392342, recTy.getFieldAddress("a").getInt32(buf));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportDate)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DateType::Get(ctxt, false)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "2022-11-16\t";
  auto init = boost::gregorian::from_string("1970-01-01");
  auto expected = boost::gregorian::from_string("2022-11-16");
  recTy.getFieldAddress("a").setDate(init, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportDefaultDatetime<ImportDateType> importer(recTy.getFieldAddress("a"));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(expected, recTy.getFieldAddress("a").getDate(buf));
  }
  recTy.getFieldAddress("a").setDate(init, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportDefaultDatetime<ImportDateType> importer(recTy.getFieldAddress("a"));
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(init, recTy.getFieldAddress("a").getDate(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), strlen(testString)-3);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(expected, recTy.getFieldAddress("a").getDate(buf));
  }
  for(int32_t i=0; i<=strlen(testString); ++i) {
    std::array<AsyncDataBlock, 2> blks = { AsyncDataBlock(const_cast<char *>(testString), i), AsyncDataBlock(const_cast<char *>(testString)+i, strlen(testString) - i) };
    ImportDefaultDatetime<ImportDateType> importer(recTy.getFieldAddress("a"));
    BOOST_CHECK(importer.import(blks, buf).isSuccess());
    BOOST_CHECK(boost::asio::buffer_size(blks) > 0);
    BOOST_CHECK_EQUAL('\t', *boost::asio::buffers_begin(blks));
    BOOST_CHECK_EQUAL(expected, recTy.getFieldAddress("a").getDate(buf));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportDatetime)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DatetimeType::Get(ctxt, false)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "2011-02-17 20:38:33\t";
  boost::posix_time::ptime init = boost::posix_time::time_from_string("1970-01-01 00:00:00");
  boost::posix_time::ptime expected = boost::posix_time::time_from_string("2011-02-17 20:38:33");
  recTy.getFieldAddress("a").setDatetime(init, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportDefaultDatetime<ImportDatetimeType> importer(recTy.getFieldAddress("a"));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(expected, recTy.getFieldAddress("a").getDatetime(buf));
  }
  recTy.getFieldAddress("a").setDatetime(init, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportDefaultDatetime<ImportDatetimeType> importer(recTy.getFieldAddress("a"));
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(init, recTy.getFieldAddress("a").getDatetime(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), strlen(testString)-3);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(expected, recTy.getFieldAddress("a").getDatetime(buf));
  }
  for(int32_t i=0; i<=strlen(testString); ++i) {
    std::array<AsyncDataBlock, 2> blks = { AsyncDataBlock(const_cast<char *>(testString), i), AsyncDataBlock(const_cast<char *>(testString)+i, strlen(testString) - i) };
    ImportDefaultDatetime<ImportDatetimeType> importer(recTy.getFieldAddress("a"));
    BOOST_CHECK(importer.import(blks, buf).isSuccess());
    BOOST_CHECK(boost::asio::buffer_size(blks) > 0);
    BOOST_CHECK_EQUAL('\t', *boost::asio::buffers_begin(blks));
    BOOST_CHECK_EQUAL(expected, recTy.getFieldAddress("a").getDatetime(buf));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportFixedLengthString)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 14, true)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "13923429923434\t";
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  // Fast path test
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportFixedLengthString importer(recTy.getFieldAddress("a"), 14);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  // Slow path test : 2 bites
  {
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportFixedLengthString importer(recTy.getFieldAddress("a"), 14);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), strlen(testString)-3);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  // Slow path test : 3 bites
  {
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportFixedLengthString importer(recTy.getFieldAddress("a"), 14);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), 2);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 5), strlen(testString)-5);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportNullableFixedLengthString)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 14, true)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "13923429923434\t";
  const char * nullTestString = "\\N\t";
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  ImportNullableField<ImportFixedLengthString> importer(recTy.getFieldAddress("a"), 14);
  // Fast path test
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  {
    AsyncDataBlock blk((uint8_t *) nullTestString, strlen(testString));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
  }
  // Slow path test : 2 bites
  for(int32_t i=0; i<=13; ++i) {
    recTy.getFieldAddress("a").setNull(buf);
    memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
    AsyncDataBlock blk((uint8_t *) testString, i);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + i), strlen(testString)-i);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportNullableFixedLengthStringLeadingSlash)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 14, true)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "\\3923429923434\t";
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  ImportNullableField<ImportFixedLengthString> importer(recTy.getFieldAddress("a"), 14);
  // Fast path test
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("\\3923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  // Slow path test : 2 bites
  for(int32_t i=0; i<=13; ++i) {
    recTy.getFieldAddress("a").setNull(buf);
    memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
    AsyncDataBlock blk((uint8_t *) testString, i);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_MESSAGE(0 == blk.size(), "Expected 0 == blk.size(), actual " << blk.size() << " = blk.size() iteration " << i);
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + i), strlen(testString)-i);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("\\3923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportDouble)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DoubleType::Get(ctxt, false)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "1392342.2384452\t";
  // Fast path
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportOptionalBuffer<ImportDoubleType> importer(recTy.getFieldAddress("a"), '\t');
    // ImportFloatingPoint<DoubleType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(1392342.2384452, 
		      recTy.getFieldAddress("a").getDouble(buf));
  }
  // Slow path 2 bites
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportOptionalBuffer<ImportDoubleType> importer(recTy.getFieldAddress("a"), '\t');
    // ImportFloatingPoint<DoubleType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0.0, recTy.getFieldAddress("a").getDouble(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), strlen(testString)-3);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(1392342.2384452, 
		      recTy.getFieldAddress("a").getDouble(buf));
  }
  // Slow path 3 bites
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportOptionalBuffer<ImportDoubleType> importer(recTy.getFieldAddress("a"), '\t');
    // ImportFloatingPoint<DoubleType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0.0, recTy.getFieldAddress("a").getDouble(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), 4);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0.0, recTy.getFieldAddress("a").getDouble(buf));
    blk = AsyncDataBlock((uint8_t *) (testString + 7), strlen(testString)-7);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(1392342.2384452, 
		      recTy.getFieldAddress("a").getDouble(buf));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportNullableDoubleFailureLeadingSlash)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DoubleType::Get(ctxt, true)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "\\1392342.2384452\t";
  // Fast path
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportNullableField<ImportOptionalBuffer<ImportDoubleType> > importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(!importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\\', *static_cast<const char *>(blk.data()));
  }
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString, 1);
    ImportNullableField<ImportOptionalBuffer<ImportDoubleType> > importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(blk.size(),  0);
    blk = AsyncDataBlock ((uint8_t *) testString+1, strlen(testString)-1);
    BOOST_CHECK(!importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('1', *static_cast<const char *>(blk.data()));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportDecimal)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DecimalType::Get(ctxt, false)));
  RecordType recTy(ctxt, members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "1392342.2384452\t";
  decimal128 zero,expected;
  ::decimal128FromString(&zero,
			 "0", 
			 runtimeCtxt.getDecimalContext());
  ::decimal128FromString(&expected,
			 "1392342.2384452",
			 runtimeCtxt.getDecimalContext());
  // Fast path
  {
    recTy.getFieldAddress("a").setDecimal(zero, buf);
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImportWithBuffer<ImportDecimalType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(0, memcmp(&expected, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
  }
  // Slow path 2 bites
  {
    recTy.getFieldAddress("a").setDecimal(zero, buf);
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportWithBuffer<ImportDecimalType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0, memcmp(&zero, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), strlen(testString)-3);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(0, memcmp(&expected, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
  }
  // Slow path 3 bites
  {
    recTy.getFieldAddress("a").setDecimal(zero, buf);
    AsyncDataBlock blk((uint8_t *) testString, 3);
    ImportWithBuffer<ImportDecimalType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0, memcmp(&zero, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
    blk = AsyncDataBlock((uint8_t *) (testString + 3), 4);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    BOOST_CHECK_EQUAL(0, memcmp(&zero, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
    blk = AsyncDataBlock((uint8_t *) (testString + 7), strlen(testString)-7);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('\t', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(0, memcmp(&expected, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
  }
  for(int32_t i=0; i<=strlen(testString); ++i) {
    std::array<AsyncDataBlock, 2> blks = { AsyncDataBlock(const_cast<char *>(testString), i), AsyncDataBlock(const_cast<char *>(testString)+i, strlen(testString) - i) };
    ImportWithBuffer<ImportDecimalType> importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blks, buf).isSuccess());
    BOOST_CHECK(boost::asio::buffer_size(blks) > 0);
    BOOST_CHECK_EQUAL('\t', *boost::asio::buffers_begin(blks));
    BOOST_CHECK_EQUAL(0, memcmp(&expected, recTy.getFieldAddress("a").getDecimalPtr(buf), 16));
  }
  recTy.getFree().free(buf);
}

class ImporterIterator
{
private:
  enum State { START, AGAIN };
  State mState;
  ParserState mPS;
  GenericRecordImporter mImporter;
  GenericRecordImporter::iterator mIt;
public:
  ImporterIterator(std::vector<ImporterSpec*>::const_iterator begin,
			std::vector<ImporterSpec*>::const_iterator end)
  :
    mState(START),
    mImporter(begin, end)
  {
  }
  ParserState import(AsyncDataBlock & block, 
		     RecordBuffer buf);
};

ParserState ImporterIterator::import(AsyncDataBlock & block, 
				     RecordBuffer buf)
{
  switch(mState) {
    while(true) {
      for(mIt = mImporter.begin();
	    mIt != mImporter.end(); ++mIt) {
	do {
	  mPS = (*mIt)(block, buf);
	  if (mPS.isSuccess()) {
	    break;
	  }
	  mState = AGAIN;
	  return mPS;
        case AGAIN:;
	} while(true);
      }
      mState = START;
      return ParserState::success();
    case START:;
    }
  }
  return ParserState::error(-2);
}

BOOST_AUTO_TEST_CASE(testCustomImporter)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> baseMembers;
  baseMembers.push_back(RecordMember("file_id", Int32Type::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("id", Int64Type::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("akid", CharType::Get(ctxt, 22, false)));
  baseMembers.push_back(RecordMember("cre_date", DatetimeType::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("cre_hour", Int32Type::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("campaign_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("creative_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("lineitem_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("member_page_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("advert_type_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("imp_cost", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cost_model", VarcharType::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("cost", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpm_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpc_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("vc_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cc_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpm_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpc_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("vc_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cc_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("view_ts", DatetimeType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("click_ts", DatetimeType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("conv_ts", DatetimeType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("tid", VarcharType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("sale", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("rev_calc", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("coop_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("dummy", VarcharType::Get(ctxt, true)));
  RecordType baseTy(ctxt, baseMembers);
  std::vector<RecordMember> members;
  members.push_back(RecordMember("id", Int64Type::Get(ctxt, false)));
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22, false)));
  members.push_back(RecordMember("cre_date", DatetimeType::Get(ctxt, false)));
  members.push_back(RecordMember("cost_model", VarcharType::Get(ctxt, false)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt, false)));

  RecordType recTy(ctxt, members);
  std::vector<ImporterSpec*> specs;
  ImporterSpec::createDefaultImport(&recTy, &baseTy, '\t', '\n', specs);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "3007174\t2518829283\tAAA-AP_7AU46SAAA0BMmLg\t2011-08-02 13:59:39\t13\t16191\t68404\t15624\t\\N\t1\t200.00\tCPM\t0.002000\t\\N\t\\N\t\\N\t\\N\t0.000000\t0.00\t0.00\t0.00\t2011-08-02 13:59:39\t\\N\t\\N\t\\N\t\\N\t1\t1159\t232\t17060\tRMKT\t\\N\t\\N\t\\N\n"
    "4568685\t3985370792\tAAA-AWOj8E-HHgAAYBFMbA\t2012-07-02 08:50:47\t8\t37074\t1209848\t16707\t\\N\t1\t3.44\tDYNA\t0.000034\t\\N\t\\N\t\\N\t\\N\t0.000000\t0.00\t0.00\t0.00\t2012-07-02 08:50:47\t\\N\t\\N\t\\N\t\\N\t1\t304\t266\t887371\tACQ\t\\N\t\\N\t3718402554839679057\n"
    "4590336\t4129420988\tAAA-AWOj8E-HHgAAYBFMbA\t2012-07-05 23:25:06\t23\t44652\t1119241\t16707\t\\N\t1\t29.40\tDYNA\t0.000294\t0.003000\t\\N\t\\N\t\\N\t0.003000\t0.00\t0.00\t0.00\t2012-07-05 23:25:06\t\\N\t\\N\t\\N\t\\N\t1\t1452\t266\t803732\tRMKT\t\\N\t\\N\t7228796745580141893\n"
    "3791703\t3296814760\tAAA-Ad9j_06HBQAAuFT0Dg\t2012-01-22 14:52:15\t14\t35938\t733766\t16707\t\\N\t1\t69.18\tDYNA\t0.000692\t0.002450\t\\N\t\\N\t\\N\t0.000000\t0.00\t0.00\t0.00\t2012-01-22 14:52:15\t\\N\t\\N\t\\N\t\\N\t1\t1385\t266\t507103\tATMG\t\\N\t\\N\t3206783830354137187\n";
  // Fast path
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    ImporterIterator importer(specs.begin(), specs.end());
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('4', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(2518829283, 
		      recTy.getFieldAddress("id").getInt64(buf));
    BOOST_CHECK(boost::algorithm::equals("AAA-AP_7AU46SAAA0BMmLg", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-08-02 13:59:39"), recTy.getFieldAddress("cre_date").getDatetime(buf));
    BOOST_CHECK(boost::algorithm::equals("CPM", 
					 recTy.getFieldAddress("cost_model").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK_EQUAL(1159, 
		      recTy.getFieldAddress("coop_id").getInt32(buf));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('4', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(3985370792, 
		      recTy.getFieldAddress("id").getInt64(buf));
    BOOST_CHECK(boost::algorithm::equals("AAA-AWOj8E-HHgAAYBFMbA", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2012-07-02 08:50:47"), recTy.getFieldAddress("cre_date").getDatetime(buf));
    BOOST_CHECK(boost::algorithm::equals("DYNA", 
					 recTy.getFieldAddress("cost_model").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK_EQUAL(304, 
		      recTy.getFieldAddress("coop_id").getInt32(buf));
  }
  // Slow path : 2 bites
  for(int32_t i=0; i< 100; i++) {
    AsyncDataBlock blk((uint8_t *) testString, i + 1);
    ImporterIterator importer(specs.begin(), specs.end());
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK_EQUAL(0, blk.size());
    blk = AsyncDataBlock((uint8_t *) (testString + i + 1), strlen(testString) - i - 1);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('4', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(2518829283, 
		      recTy.getFieldAddress("id").getInt64(buf));
    BOOST_CHECK(boost::algorithm::equals("AAA-AP_7AU46SAAA0BMmLg", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-08-02 13:59:39"), recTy.getFieldAddress("cre_date").getDatetime(buf));
    BOOST_CHECK(boost::algorithm::equals("CPM", 
					 recTy.getFieldAddress("cost_model").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK_EQUAL(1159, 
		      recTy.getFieldAddress("coop_id").getInt32(buf));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('4', *static_cast<const char *>(blk.data()));
    BOOST_CHECK_EQUAL(3985370792, 
		      recTy.getFieldAddress("id").getInt64(buf));
    BOOST_CHECK(boost::algorithm::equals("AAA-AWOj8E-HHgAAYBFMbA", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2012-07-02 08:50:47"), recTy.getFieldAddress("cre_date").getDatetime(buf));
    BOOST_CHECK(boost::algorithm::equals("DYNA", 
					 recTy.getFieldAddress("cost_model").getVarcharPtr(buf)->c_str()));
    BOOST_CHECK_EQUAL(304, 
		      recTy.getFieldAddress("coop_id").getInt32(buf));
  }
}

