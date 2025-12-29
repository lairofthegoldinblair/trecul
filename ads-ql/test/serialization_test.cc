/**
 * Copyright (c) 2026, Akamai Technologies
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

#include "CodeGenerationContext.hh"
#include "IQLInterpreter.hh"
#include "SuperFastHash.h"

#define BOOST_TEST_MODULE TreculRecordSerializeTest
#include <boost/test/unit_test.hpp>

struct SyncSerializeAsyncDeserializeTestFixture
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  const RecordType * outputType;
  RecordBuffer outputBuf;
  CodeGenerationContext codeGenCtxt;
  TreculSerializationState::pointer state;
  std::unique_ptr<TreculModule> treculModule;
  decimal128 decimalVal;

  SyncSerializeAsyncDeserializeTestFixture()
    :
    outputType(nullptr)
  {
    ::decimal128FromString(&decimalVal, "23434.333", runtimeCtxt.getDecimalContext());
  }
  
  ~SyncSerializeAsyncDeserializeTestFixture()
  {
    if (nullptr != outputType) {
      outputType->getFree().free(outputBuf);
    }
  }

  void makeRecord(const std::string & xfer)
  {
    std::vector<RecordMember> members;
    RecordType recTy(ctxt, members);
    RecordBuffer inputBuf;
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, xfer);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    outputType = t1.getTarget();
    state = TreculSerializationState::get(outputType);
  }
  std::string serializeSynchronous()
  {
    RecordTypePrintOperation op(outputType, true);
    auto m = std::unique_ptr<IQLRecordTypeOperationModule>(op.create());
    std::stringstream sstr;
    m->execute(outputBuf, sstr, true);
    return sstr.str();
  }
  std::string makeAndSerializeRecord(const std::string & xfer)
  {
    makeRecord(xfer);
    return serializeSynchronous();
  }
  TreculRecordSerializeRuntime makeSerializer()
  {
    TreculRecordSerialize d(codeGenCtxt, outputType);
    treculModule = std::make_unique<TreculModule>(codeGenCtxt);
    return treculModule->getFunction<TreculRecordSerializeRuntime>(d.getReference());
  }
  TreculRecordDeserializeRuntime makeDeserializer()
  {
    TreculRecordDeserialize d(codeGenCtxt, outputType);
    treculModule = std::make_unique<TreculModule>(codeGenCtxt);
    return treculModule->getFunction<TreculRecordDeserializeRuntime>(d.getReference());
  }
  std::size_t serializedLength()
  {
    TreculRecordSerializedLength len(codeGenCtxt, outputType);
    treculModule = std::make_unique<TreculModule>(codeGenCtxt);
    auto f = treculModule->getFunction<TreculRecordSerializedLengthRuntime>(len.getReference());
    return f.length(outputBuf, *state, &runtimeCtxt);
  }
  bool checkFieldEqual(const std::string & field, RecordBuffer lhs, RecordBuffer rhs)
  {
    DynamicRecordContext local;
    RecordTypeFunction equals(local, "equals",
                              { AliasedRecordType("lhs", outputType), AliasedRecordType("rhs", outputType) },
                              (boost::format("lhs.%1% = rhs.%1%") % field).str());
    int32_t val = equals.execute(lhs, rhs, &runtimeCtxt);
    return val == 1;
  }
};

BOOST_FIXTURE_TEST_CASE(DepthZero, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  BOOST_CHECK_EQUAL(0U, outputType->getDepth());
}

BOOST_FIXTURE_TEST_CASE(DepthZeroWithNestedRecord, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ROW(12 , 23434.333) AS b, CAST('small' AS CHAR(5)) AS c");
  BOOST_CHECK_EQUAL(0U, outputType->getDepth());
}

BOOST_FIXTURE_TEST_CASE(DepthOne, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ARRAY[12,13] AS a, 14 AS b");
  BOOST_CHECK_EQUAL(1U, outputType->getDepth());
}

BOOST_FIXTURE_TEST_CASE(DepthOneWithNestedRecord, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ROW(ARRAY[12,13]) AS a, 14 AS b");
  BOOST_CHECK_EQUAL(1U, outputType->getDepth());
}

BOOST_FIXTURE_TEST_CASE(DepthOneVarArray, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CAST(ARRAY[12,13] AS INTEGER[]) AS a, 14 AS b");
  BOOST_CHECK_EQUAL(1U, outputType->getDepth());
}

BOOST_FIXTURE_TEST_CASE(DepthOneVarArrayWithNestedRecord, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ROW(CAST(ARRAY[12,13] AS INTEGER[])) AS a, 14 AS b");
  BOOST_CHECK_EQUAL(1U, outputType->getDepth());
}

BOOST_FIXTURE_TEST_CASE(SerializeSimpleFlatBuffer, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeSimpleFlatBufferOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeSimpleFlatBufferTwoBites, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+(serializeBuf.size()/2), *state.get(), &runtimeCtxt);
  BOOST_CHECK(!ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+(serializeBuf.size()/2));
  ret = serializer.serialize(outputBuf, output, serializeBuf.data()+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeSimpleFlatBufferOneByteAtATime, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  for(std::size_t i=0; i<serializeBuf.size(); ++i) {
    bool ret = serializer.serialize(outputBuf, output, output+1, *state.get(), &runtimeCtxt);
    BOOST_CHECK_EQUAL(ret, i+1 == serializeBuf.size());
    BOOST_CHECK_EQUAL(output, serializeBuf.data()+i+1);
  }
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeSimpleFlatBufferOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getCharPtr(deserializeBuf)));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(DeserializeSimpleFlatBufferTwoBites, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data()+(str.size()/2), *state.get(), &runtimeCtxt);
  BOOST_CHECK(!ret);
  BOOST_CHECK_EQUAL(input, str.data()+(str.size()/2));
  ret = deserializer.deserialize(deserializeBuf, input, str.data()+str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getCharPtr(deserializeBuf)));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(DeserializeSimpleFlatBufferOneByteAtATime, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  for(std::size_t i=0; i<str.size(); ++i) {
    bool ret = deserializer.deserialize(deserializeBuf, input, input+1, *state.get(), &runtimeCtxt);
    BOOST_CHECK(ret == (i+1 == str.size()));
  }
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getCharPtr(deserializeBuf)));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeSimpleFlatBufferNullable, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CAST(NULL AS INTEGER) AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeSimpleFlatBufferNullableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CAST(NULL AS INTEGER) AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeSimpleFlatBufferNullableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CAST(NULL AS INTEGER) AS a, 23434.333 AS b, CAST('small' AS CHAR(5)) AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(outputType->isNull("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getCharPtr(deserializeBuf)));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeSmallVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, 'small' AS c");
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeSmallVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, 'small' AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeSmallVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, 23434.333 AS b, 'small' AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeSmallNullableVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CASE WHEN 1=1 THEN 'small' ELSE NULL END AS c");
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeSmallNullableVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CASE WHEN 1=1 THEN 'small' ELSE NULL END AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeSmallNullableVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, 23434.333 AS b, CASE WHEN 1=1 THEN 'small' ELSE NULL END AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK(!outputType->getFieldAddress("c").isNull(deserializeBuf));
  BOOST_CHECK_EQUAL(0, strcmp("small",outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CASE WHEN 1=2 THEN 'it does not matter if this is large or small' ELSE NULL END AS c");
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE(outputType->getFieldAddress("c").isNull(outputBuf));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, 23434.333 AS b, CASE WHEN 1=2 THEN 'it does not matter if this is large or small' ELSE NULL END AS c");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, 23434.333 AS b, CASE WHEN 1=2 THEN 'it does not matter if this is large or small' ELSE NULL END AS c");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK(outputType->getFieldAddress("c").isNull(deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeLargeVarcharFirstPosition, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "'" << large << "' AS c, 12 AS a, 23434.333 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeLargeVarcharFirstPositionOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "'" << large << "' AS c, 12 AS a, 23434.333 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeLargeVarcharFirstPositionOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "'" << large << "' AS c, 12 AS a, 23434.333 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeLargeVarcharMiddlePosition, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large << "' AS c, 23434.333 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeLargeVarcharMiddlePositionOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large << "' AS c, 23434.333 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeLargeVarcharMiddlePositionOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large << "' AS c, 23434.333 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeLargeVarcharLastPosition, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, 23434.333 AS b, '" << large << "' AS c";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeLargeVarcharLastPositionOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, 23434.333 AS b, '" << large << "' AS c";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeLargeVarcharLastPositionOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, 23434.333 AS b, '" << large << "' AS c";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableLargeVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN '" << large << "' ELSE NULL END AS c, 12 AS a, 23434.333 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableLargeVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN '" << large << "' ELSE NULL END AS c, 12 AS a, 23434.333 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableLargeVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN '" << large << "' ELSE NULL END AS c, 12 AS a, 23434.333 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_CHECK(!outputType->getFieldAddress("c").isNull(deserializeBuf));
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeTwoLargeVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large1 = "this is a large model string";
  std::string large2 = "this is also a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large1 << "' AS c, 23434.333 AS b, '" << large2 << "' AS d";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", outputBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(outputBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large1.c_str(),outputType->getFieldAddress("c").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(0, strcmp(large2.c_str(),outputType->getFieldAddress("d").getVarcharPtr(outputBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large1.size()+large2.size()+2, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large1.size()+large2.size()+2);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large1[0]));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()+large1.size()+1], &large2[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeTwoLargeVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large1 = "this is a large model string";
  std::string large2 = "this is also a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large1 << "' AS c, 23434.333 AS b, '" << large2 << "' AS d";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large1.size()+large2.size()+2);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large1[0]));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+large1.size()+1], &large2[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeTwoLargeVarcharThreeBites, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large1 = "this is a large model string";
  std::string large2 = "this is also a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large1 << "' AS c, 23434.333 AS b, '" << large2 << "' AS d";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large1.size()+large2.size()+2);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, serializeBuf.data() + outputType->GetAllocSize() + 5, *state.get(), &runtimeCtxt);
  BOOST_CHECK(!ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data() + outputType->GetAllocSize() + 5);
  ret = serializer.serialize(outputBuf, output, serializeBuf.data() + outputType->GetAllocSize() + large1.size() + 5, *state.get(), &runtimeCtxt);
  BOOST_CHECK(!ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data() + outputType->GetAllocSize() + large1.size() + 5);
  ret = serializer.serialize(outputBuf, output, serializeBuf.data() + serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data() + serializeBuf.size()); 
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large1[0]));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+large1.size()+1], &large2[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeTwoLargeVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large1 = "this is a large model string";
  std::string large2 = "this is also a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large1 << "' AS c, 23434.333 AS b, '" << large2 << "' AS d";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large1.c_str(),outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(0, strcmp(large2.c_str(),outputType->getFieldAddress("d").getVarcharPtr(deserializeBuf)->c_str()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeTwoLargeVarcharThreeBites, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large1 = "this is a large model string";
  std::string large2 = "this is also a large model string";
  std::stringstream prog;
  prog << "12 AS a, '" << large1 << "' AS c, 23434.333 AS b, '" << large2 << "' AS d";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + outputType->GetAllocSize() + 5, *state.get(), &runtimeCtxt);
  BOOST_CHECK(!ret);
  BOOST_CHECK(input == str.data() + outputType->GetAllocSize() + 5);
  ret = deserializer.deserialize(deserializeBuf, input, str.data() + outputType->GetAllocSize() + large1.size() + 5, *state.get(), &runtimeCtxt);
  BOOST_CHECK(!ret);
  BOOST_CHECK(input == str.data() + outputType->GetAllocSize() + large1.size() + 5);
  ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_REQUIRE_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  BOOST_REQUIRE_EQUAL(0, memcmp(outputType->getFieldAddress("b").getDecimalPtr(deserializeBuf),
                              &decimalVal, 16));
  BOOST_REQUIRE_EQUAL(0, strcmp(large1.c_str(),outputType->getFieldAddress("c").getVarcharPtr(deserializeBuf)->c_str()));
  BOOST_REQUIRE_EQUAL(0, strcmp(large2.c_str(),outputType->getFieldAddress("d").getVarcharPtr(deserializeBuf)->c_str()));
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ARRAY[12,13] AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Binary print of fixed array of copyable type
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ARRAY[12,13] AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("ARRAY[12,13] AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getArrayInt32("a", 0, deserializeBuf));
  BOOST_CHECK_EQUAL(13, outputType->getArrayInt32("a", 1, deserializeBuf));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableFixedArrayCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CASE WHEN 1=1 THEN ARRAY[12,13] ELSE NULL END AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Binary print of fixed array of copyable type
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableFixedArrayCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CASE WHEN 1=1 THEN ARRAY[12,13] ELSE NULL END AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableFixedArrayCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CASE WHEN 1=1 THEN ARRAY[12,13] ELSE NULL END AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->getFieldAddress("a").isNull(deserializeBuf));
  BOOST_CHECK_EQUAL(12, outputType->getArrayInt32("a", 0, deserializeBuf));
  BOOST_CHECK_EQUAL(13, outputType->getArrayInt32("a", 1, deserializeBuf));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayNullableCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ARRAY[12,NULL,13] AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Binary print of fixed array of copyable type
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayNullableCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("ARRAY[12,NULL,13] AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayNullableCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("ARRAY[12,NULL,13] AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getArrayInt32("a", 0, deserializeBuf));
  BOOST_CHECK(outputType->isArrayNull("a", 1, deserializeBuf));
  BOOST_CHECK_EQUAL(13, outputType->getArrayInt32("a", 2, deserializeBuf));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableFixedArrayNullableCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CASE WHEN 1=1 THEN ARRAY[12,NULL,13] ELSE NULL END AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Binary print of fixed array of copyable type
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableFixedArrayNullableCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CASE WHEN 1=1 THEN ARRAY[12,NULL,13] ELSE NULL END AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableFixedArrayNullableCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CASE WHEN 1=1 THEN ARRAY[12,NULL,13] ELSE NULL END AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->getFieldAddress("a").isNull(deserializeBuf));
  BOOST_CHECK_EQUAL(12, outputType->getArrayInt32("a", 0, deserializeBuf));
  BOOST_CHECK(outputType->isArrayNull("a", 1, deserializeBuf));
  BOOST_CHECK_EQUAL(13, outputType->getArrayInt32("a", 2, deserializeBuf));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayNonCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  // Binary print of fixed array of non-copyable type
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "ARRAY['small','" << large << "'] AS a, 14 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  std::string str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayNonCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "ARRAY['small','" << large << "'] AS a, 14 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayNonCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "ARRAY['small','" << large << "'] AS a, 14 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", outputType->getArrayVarcharPtr("a", 0, deserializeBuf)->c_str()));
  BOOST_CHECK_EQUAL(0, ::strcmp(large.c_str(), outputType->getArrayVarcharPtr("a", 1, deserializeBuf)->c_str()));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullFixedArrayCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CASE WHEN 1=2 THEN ARRAY[12,13] ELSE NULL END AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Binary print of fixed array of copyable type
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullFixedArrayCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("CASE WHEN 1=2 THEN ARRAY[12,13] ELSE NULL END AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullFixedArrayCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CASE WHEN 1=2 THEN ARRAY[12,13] ELSE NULL END AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(outputType->isNull("a", deserializeBuf));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data{12,13};
  makeRecord("CAST(ARRAY[12,13] AS INTEGER[]) AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+sizeof(data), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+sizeof(data));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeVariableArrayCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data{12,13};
  makeRecord("CAST(ARRAY[12,13] AS INTEGER[]) AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+sizeof(data));
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CAST(ARRAY[12,13] AS INTEGER[]) AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 0));
  BOOST_CHECK_EQUAL(13, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 1));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableVariableArrayCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data{12,13};
  makeRecord("CASE WHEN 1=1 THEN CAST(ARRAY[12,13] AS INTEGER[]) ELSE NULL END AS a, 14 AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+sizeof(data), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+sizeof(data));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableVariableArrayCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data{12,13};
  makeRecord("CASE WHEN 1=1 THEN CAST(ARRAY[12,13] AS INTEGER[]) ELSE NULL END AS a, 14 AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+sizeof(data));
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableVariableArrayCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CASE WHEN 1=1 THEN CAST(ARRAY[12,13] AS INTEGER[]) ELSE NULL END AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->isNull("a", deserializeBuf));
  BOOST_CHECK_EQUAL(12, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 0));
  BOOST_CHECK_EQUAL(13, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 1));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayNullableCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 3> data{12,0,13};
  makeRecord("CAST(ARRAY[12,NULL,13] AS INTEGER[]) AS a, 14 AS b");
  // Extra byte at end for null bits of the array
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+sizeof(data)+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+sizeof(data)+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeVariableArrayNullableCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 3> data{12,0,13};
  makeRecord("CAST(ARRAY[12,NULL,13] AS INTEGER[]) AS a, 14 AS b");
  auto serializer = makeSerializer();
  // Extra byte at end for null bits of the array
  std::vector<char> serializeBuf(outputType->GetAllocSize()+sizeof(data)+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayNullableCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CAST(ARRAY[12,NULL,13] AS INTEGER[]) AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->isArrayNull("a", 0, deserializeBuf));
  BOOST_CHECK_EQUAL(12, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 0));
  BOOST_CHECK(outputType->isArrayNull("a", 1, deserializeBuf));
  BOOST_CHECK(!outputType->isArrayNull("a", 2, deserializeBuf));
  BOOST_CHECK_EQUAL(13, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 2));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableVariableArrayNullableCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 3> data{12,0,13};
  makeRecord("CASE WHEN 1=1 THEN CAST(ARRAY[12,NULL,13] AS INTEGER[]) ELSE NULL END AS a, 14 AS b");
  // Extra byte at end for null bits of the array
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+sizeof(data)+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+sizeof(data)+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableVariableArrayNullableCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 3> data{12,0,13};
  makeRecord("CASE WHEN 1=1 THEN CAST(ARRAY[12,NULL,13] AS INTEGER[]) ELSE NULL END AS a, 14 AS b");
  auto serializer = makeSerializer();
  // Extra byte at end for null bits of the array
  std::vector<char> serializeBuf(outputType->GetAllocSize()+sizeof(data)+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], &data[0], sizeof(data)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableVariableArrayNullableCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("CASE WHEN 1=1 THEN CAST(ARRAY[12,NULL,13] AS INTEGER[]) ELSE NULL END AS a, 14 AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->isArrayNull("a", 0, deserializeBuf));
  BOOST_CHECK_EQUAL(12, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 0));
  BOOST_CHECK(outputType->isArrayNull("a", 1, deserializeBuf));
  BOOST_CHECK(!outputType->isArrayNull("a", 2, deserializeBuf));
  BOOST_CHECK_EQUAL(13, outputType->getFieldAddress("a").getVarArrayInt32(deserializeBuf, 2));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayNonCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  // Binary print of fixed array of non-copyable type
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CAST(ARRAY['small','" << large << "'] AS VARCHAR[]) AS a, 14 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+2*sizeof(Varchar)+large.size()+1, serializedLength());
  std::string str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+2*sizeof(Varchar)+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()+sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()+2*sizeof(Varchar)], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeVariableArrayNonCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CAST(ARRAY['small','" << large << "'] AS VARCHAR[]) AS a, 14 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+2*sizeof(Varchar)+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()+sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+2*sizeof(Varchar)], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayNonCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CAST(ARRAY['small','" << large << "'] AS VARCHAR[]) AS a, 14 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 0)->c_str()));
  BOOST_CHECK_EQUAL(0, ::strcmp(large.c_str(), outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 1)->c_str()));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableVariableArrayNonCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  // Binary print of fixed array of non-copyable type
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN CAST(ARRAY['small','" << large << "'] AS VARCHAR[]) ELSE NULL END AS a, 14 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+2*sizeof(Varchar)+large.size()+1, serializedLength());
  std::string str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+2*sizeof(Varchar)+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()+sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()+2*sizeof(Varchar)], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableVariableArrayNonCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN CAST(ARRAY['small','" << large << "'] AS VARCHAR[]) ELSE NULL END AS a, 14 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+2*sizeof(Varchar)+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()+sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+2*sizeof(Varchar)], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableVariableArrayNonCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN CAST(ARRAY['small','" << large << "'] AS VARCHAR[]) ELSE NULL END AS a, 14 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->isNull("a", deserializeBuf));
  BOOST_CHECK_EQUAL(0, ::strcmp("small", outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 0)->c_str()));
  BOOST_CHECK_EQUAL(0, ::strcmp(large.c_str(), outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 1)->c_str()));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayNullableNonCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  // Binary print of fixed array of non-copyable type
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CAST(ARRAY['small',NULL,'" << large << "'] AS VARCHAR[]) AS a, 14 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+3*sizeof(Varchar)+large.size()+2, serializedLength());
  std::string str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+3*sizeof(Varchar)+large.size()+2);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()+2*sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()+3*sizeof(Varchar)+1], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeVariableArrayNullableNonCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CAST(ARRAY['small',NULL,'" << large << "'] AS VARCHAR[]) AS a, 14 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+3*sizeof(Varchar)+large.size()+2);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()+2*sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+3*sizeof(Varchar)+1], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayNullableNonCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CAST(ARRAY['small',NULL,'" << large << "'] AS VARCHAR[]) AS a, 14 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->isArrayNull("a", 0, deserializeBuf));
  BOOST_CHECK_EQUAL(0, ::strcmp("small", outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 0)->c_str()));
  BOOST_CHECK(outputType->isArrayNull("a", 1, deserializeBuf));
  BOOST_CHECK(!outputType->isArrayNull("a", 2, deserializeBuf));
  BOOST_CHECK_EQUAL(0, ::strcmp(large.c_str(), outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 2)->c_str()));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNullableVariableArrayNullableNonCopyableType, SyncSerializeAsyncDeserializeTestFixture)
{
  // Binary print of fixed array of non-copyable type
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN CAST(ARRAY['small',NULL,'" << large << "'] AS VARCHAR[]) ELSE NULL END AS a, 14 AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+3*sizeof(Varchar)+large.size()+2, serializedLength());
  std::string str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+3*sizeof(Varchar)+large.size()+2);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&str[outputType->GetAllocSize()+2*sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()+3*sizeof(Varchar)+1], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNullableVariableArrayNullableNonCopyableTypeOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN CAST(ARRAY['small',NULL,'" << large << "'] AS VARCHAR[]) ELSE NULL END AS a, 14 AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+3*sizeof(Varchar)+large.size()+2);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(5, reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp("small", reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()])->c_str()));
  BOOST_CHECK_EQUAL(int32_t(large.size()), reinterpret_cast<const Varchar *>(&serializeBuf[outputType->GetAllocSize()+2*sizeof(Varchar)])->size());
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+3*sizeof(Varchar)+1], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNullableVariableArrayNullableNonCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "CASE WHEN 1=1 THEN CAST(ARRAY['small',NULL,'" << large << "'] AS VARCHAR[]) ELSE NULL END AS a, 14 AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(!outputType->isArrayNull("a", 0, deserializeBuf));
  BOOST_CHECK_EQUAL(0, ::strcmp("small", outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 0)->c_str()));
  BOOST_CHECK(outputType->isArrayNull("a", 1, deserializeBuf));
  BOOST_CHECK(!outputType->isArrayNull("a", 2, deserializeBuf));
  BOOST_CHECK_EQUAL(0, ::strcmp(large.c_str(), outputType->getFieldAddress("a").getVarArrayVarcharPtr(deserializeBuf, 2)->c_str()));
  BOOST_CHECK_EQUAL(14, outputType->getInt32("b", deserializeBuf));
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordCopyableTypes, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, ROW(23434.333, CAST('small' AS CHAR(5))) AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordCopyableTypesOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, ROW(23434.333, CAST('small' AS CHAR(5))) AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, ROW(23434.333, CAST('small' AS CHAR(5))) AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getCharPtr(innerDeserializeBuf)));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordNullableCopyableTypes, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, ROW(CASE WHEN 1=1 THEN 23434.333 ELSE NULL END, CAST('small' AS CHAR(5))) AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordNullableCopyableTypesOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, ROW(CASE WHEN 1=1 THEN 23434.333 ELSE NULL END, CAST('small' AS CHAR(5))) AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordNullableCopyableOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, ROW(CASE WHEN 1=1 THEN 23434.333 ELSE NULL END, CAST('small' AS CHAR(5))) AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerOutputBuf = outputType->getStructPtr("b", outputBuf);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK(!innerAddress->isNull(innerDeserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getCharPtr(innerOutputBuf)));
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getCharPtr(innerDeserializeBuf)));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordSmallVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data{12,13};
  makeRecord("12 AS a, ROW(23434.333, 'small') AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordSmallVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, ROW(23434.333, 'small') AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordSmallVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, ROW(23434.333, 'small') AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordSmallNullableVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data{12,13};
  makeRecord("12 AS a, ROW(23434.333, CASE WHEN 1=1 THEN 'small' ELSE NULL END) AS b");
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordSmallNullableVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  makeRecord("12 AS a, ROW(23434.333, CASE WHEN 1=1 THEN 'small' ELSE NULL END) AS b");
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordSmallNullableVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  auto str = makeAndSerializeRecord("12 AS a, ROW(23434.333, CASE WHEN 1=1 THEN 'small' ELSE NULL END) AS b");
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK(!innerAddress->isNull(innerDeserializeBuf));
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordLargeVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, '" << large << "') AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordLargeVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, '" << large << "') AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordLargeVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  // Nested record with large model string
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, '" << large << "') AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordLargeNullableVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, CASE WHEN 1=1 THEN '" << large << "' ELSE NULL END) AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordLargeNullableVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, CASE WHEN 1=1 THEN '" << large << "' ELSE NULL END) AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordLargeNullableVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  // Nested record with large model string
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, CASE WHEN 1=1 THEN '" << large << "' ELSE NULL END) AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK(!innerAddress->isNull(innerDeserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK(!innerAddress->isNull(innerDeserializeBuf));
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeNestedRecordNullVarchar, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, CASE WHEN 1=2 THEN '" << large << "' ELSE NULL END) AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeNestedRecordNullVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, CASE WHEN 1=2 THEN '" << large << "' ELSE NULL END) AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeNestedRecordNullVarcharOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  // Nested record with large model string
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ROW(23434.333, CASE WHEN 1=2 THEN '" << large << "' ELSE NULL END) AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getStructPtr("b", deserializeBuf);
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK(!innerAddress->isNull(innerDeserializeBuf));
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK(innerAddress->isNull(innerDeserializeBuf));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayOfNestedRecord, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ARRAY[ROW(23434.333, 'small'), ROW(23434.333, '" << large << "')] AS b";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayOfNestedRecordOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ARRAY[ROW(23434.333, 'small'), ROW(23434.333, '" << large << "')] AS b";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayOfNestedRecordOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "12 AS a, ARRAY[ROW(23434.333, 'small'), ROW(23434.333, '" << large << "')] AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::FIXED_ARRAY, ty->GetEnum());
  ty = dynamic_cast<const SequentialType *>(ty)->getElementType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getFieldAddress("b").getArrayStructPtr(deserializeBuf, 0, innerTy->GetAllocSize());
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  innerDeserializeBuf = outputType->getFieldAddress("b").getArrayStructPtr(deserializeBuf, 1, innerTy->GetAllocSize());
  innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayOfNestedRecord, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[ROW(23434.333, 'small'), ROW(23434.333, '" << large << "')], 12 AS a, CAST(tmp AS DECLTYPE(tmp[0])[])  AS b";
  makeRecord(prog.str());
  const FieldType * rowTy = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::VARIABLE_ARRAY, rowTy->GetEnum());
  rowTy = dynamic_cast<const SequentialType *>(rowTy)->getElementType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, rowTy->GetEnum());  
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize()+2*rowTy->GetAllocSize()+large.size()+1, serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+2*rowTy->GetAllocSize()+large.size()+1);
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Yeah, this says get getVarcharPtr but it also gets a pointer to the VarArray...
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], outputType->getFieldAddress("b").getVarcharPtr(outputBuf)->c_str(), 2*rowTy->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&str[outputType->GetAllocSize()+2*rowTy->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeVariableArrayOfNestedRecordOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[ROW(23434.333, 'small'), ROW(23434.333, '" << large << "')], 12 AS a, CAST(tmp AS DECLTYPE(tmp[0])[])  AS b";
  makeRecord(prog.str());
  const FieldType * rowTy = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::VARIABLE_ARRAY, rowTy->GetEnum());
  rowTy = dynamic_cast<const SequentialType *>(rowTy)->getElementType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, rowTy->GetEnum());  
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+2*rowTy->GetAllocSize()+large.size()+1);
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Yeah, this says get getVarcharPtr but it also gets a pointer to the VarArray...
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], outputType->getFieldAddress("b").getVarcharPtr(outputBuf)->c_str(), 2*rowTy->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::strcmp(&serializeBuf[outputType->GetAllocSize()+2*rowTy->GetAllocSize()], &large[0]));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayOfNestedRecordOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::string large = "this is a large model string";
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[ROW(23434.333, 'small'), ROW(23434.333, '" << large << "')], 12 AS a, CAST(tmp AS DECLTYPE(tmp[0])[])  AS b";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK_EQUAL(12, outputType->getInt32("a", deserializeBuf));
  const FieldType * ty = outputType->getMember("b").GetType();
  BOOST_REQUIRE_EQUAL(FieldType::VARIABLE_ARRAY, ty->GetEnum());
  ty = dynamic_cast<const SequentialType *>(ty)->getElementType();
  BOOST_REQUIRE_EQUAL(FieldType::STRUCT, ty->GetEnum());
  BOOST_REQUIRE_EQUAL(2, ty->GetSize());
  const RecordType * innerTy = static_cast<const RecordType *>(ty);
  auto innerDeserializeBuf = outputType->getFieldAddress("b").getVarArrayStructPtr(deserializeBuf, 0, innerTy->GetAllocSize());
  auto innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp("small",innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  innerDeserializeBuf = outputType->getFieldAddress("b").getVarArrayStructPtr(deserializeBuf, 1, innerTy->GetAllocSize());
  innerAddress = innerTy->begin_offsets();
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, memcmp(innerAddress->getDecimalPtr(innerDeserializeBuf),
                              &decimalVal, 16));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() != innerAddress);
  BOOST_CHECK_EQUAL(0, strcmp(large.c_str(),innerAddress->getVarcharPtr(innerDeserializeBuf)->c_str()));
  ++innerAddress;
  BOOST_REQUIRE(innerTy->end_offsets() == innerAddress);
  outputType->getFree().free(deserializeBuf);
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayOfFixedArrays, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[ARRAY[1,2], ARRAY[3,4]] AS b, 23434.333 AS c";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayOfFixedArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[ARRAY[1,2], ARRAY[3,4]] AS b, 23434.333 AS c";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayOfFixedArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[ARRAY[1,2], ARRAY[3,4]] AS b, 23434.333 AS c";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(checkFieldEqual("a", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("b", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("c", outputBuf, deserializeBuf));
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayOfNullableFixedArrays, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[ARRAY[1,2], NULL, ARRAY[3,4]] AS b, 23434.333 AS c";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize(), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize());
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayOfNullableFixedArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[ARRAY[1,2], NULL, ARRAY[3,4]] AS b, 23434.333 AS c";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize());
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(output, serializeBuf.data()+serializeBuf.size());
  BOOST_CHECK_EQUAL(0, ::memcmp(serializeBuf.data(), outputBuf.Ptr, outputType->GetAllocSize()));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayOfNullableFixedArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[ARRAY[1,2], NULL, ARRAY[3,4]] AS b, 23434.333 AS c";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(checkFieldEqual("a", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("b", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("c", outputBuf, deserializeBuf));
}

BOOST_FIXTURE_TEST_CASE(SerializeFixedArrayOfVariableArrays, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data1{1,2};
  std::array<int32_t, 2> data2{3,4};
  std::stringstream prog;
  prog << "823 AS a, ARRAY[CAST(ARRAY[1,2] AS INTEGER[]), CAST(ARRAY[3,4] AS INTEGER[])] AS b, 23434.333 AS c";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize() + sizeof(data1) + sizeof(data2), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize() + sizeof(data1) + sizeof(data2));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], data1.data(), sizeof(data1)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()+sizeof(data1)], data2.data(), sizeof(data2)));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeFixedArrayOfVariableArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data1{1,2};
  std::array<int32_t, 2> data2{3,4};
  std::stringstream prog;
  prog << "823 AS a, ARRAY[CAST(ARRAY[1,2] AS INTEGER[]), CAST(ARRAY[3,4] AS INTEGER[])] AS b, 23434.333 AS c";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize() + sizeof(data1) + sizeof(data2));
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], data1.data(), sizeof(data1)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()+sizeof(data1)], data2.data(), sizeof(data2)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeFixedArrayOfVariableArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "823 AS a, ARRAY[CAST(ARRAY[1,2] AS INTEGER[]), CAST(ARRAY[3,4] AS INTEGER[])] AS b, 23434.333 AS c";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(checkFieldEqual("a", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("b", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("c", outputBuf, deserializeBuf));
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayOfFixedArrays, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data1{1,2};
  std::array<int32_t, 2> data2{3,4};
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[ARRAY[1,2], ARRAY[3,4]], 823 AS a, CAST(tmp AS DECLTYPE(tmp[0])[]) AS b, 23434.333 AS c";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize() + sizeof(data1) + sizeof(data2), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+sizeof(data1)+sizeof(data2));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], data1.data(), sizeof(data1)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()+sizeof(data1)], data2.data(), sizeof(data2)));
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayOfFixedArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data1{1,2};
  std::array<int32_t, 2> data2{3,4};
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[ARRAY[1,2], ARRAY[3,4]], 823 AS a, CAST(tmp AS DECLTYPE(tmp[0])[]) AS b, 23434.333 AS c";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize() + sizeof(data1) + sizeof(data2));
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], data1.data(), sizeof(data1)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()+sizeof(data1)], data2.data(), sizeof(data2)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayOfFixedArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[ARRAY[1,2], ARRAY[3,4]], 823 AS a, CAST(tmp AS DECLTYPE(tmp[0])[]) AS b, 23434.333 AS c";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(checkFieldEqual("a", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("b", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("c", outputBuf, deserializeBuf));
}

BOOST_FIXTURE_TEST_CASE(SerializeVariableArrayOfVariableArrays, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data1{1,2};
  std::array<int32_t, 2> data2{3,4};
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[CAST(ARRAY[1,2] AS INTEGER[]), CAST(ARRAY[3,4] AS INTEGER[])], 823 AS a, CAST(tmp AS DECLTYPE(tmp[0])[]) AS b, 23434.333 AS c";
  makeRecord(prog.str());
  BOOST_REQUIRE_EQUAL(outputType->GetAllocSize() + 2*sizeof(Vararray)+ sizeof(data1) + sizeof(data2), serializedLength());
  auto str = serializeSynchronous();
  BOOST_REQUIRE_EQUAL(str.size(), outputType->GetAllocSize()+2*sizeof(Vararray)+sizeof(data1)+sizeof(data2));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Yeah, this says get getVarcharPtr but it also gets a pointer to the VarArray...
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()], outputType->getFieldAddress("b").getVarcharPtr(outputBuf)->c_str(), 2*sizeof(Vararray)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()+2*sizeof(Vararray)], data1.data(), sizeof(data1)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&str[outputType->GetAllocSize()+2*sizeof(Vararray)+sizeof(data1)], data2.data(), sizeof(data2)));
}

BOOST_FIXTURE_TEST_CASE(AsyncSerializeVariableArrayOfVariableArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::array<int32_t, 2> data1{1,2};
  std::array<int32_t, 2> data2{3,4};
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[CAST(ARRAY[1,2] AS INTEGER[]), CAST(ARRAY[3,4] AS INTEGER[])], 823 AS a, CAST(tmp AS DECLTYPE(tmp[0])[]) AS b, 23434.333 AS c";
  makeRecord(prog.str());
  auto serializer = makeSerializer();
  std::vector<char> serializeBuf(outputType->GetAllocSize()+2*sizeof(Vararray)+sizeof(data1)+sizeof(data2));
  char * output = serializeBuf.data();
  bool ret = serializer.serialize(outputBuf, output, output+serializeBuf.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[0], outputBuf.Ptr, outputType->GetAllocSize()));
  // Yeah, this says get getVarcharPtr but it also gets a pointer to the VarArray...
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()], outputType->getFieldAddress("b").getVarcharPtr(outputBuf)->c_str(), 2*sizeof(Vararray)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()+2*sizeof(Vararray)], data1.data(), sizeof(data1)));
  BOOST_CHECK_EQUAL(0, ::memcmp(&serializeBuf[outputType->GetAllocSize()+2*sizeof(Vararray)+sizeof(data1)], data2.data(), sizeof(data2)));
}

BOOST_FIXTURE_TEST_CASE(DeserializeVariableArrayOfVariableArraysOneShot, SyncSerializeAsyncDeserializeTestFixture)
{
  std::stringstream prog;
  prog << "DECLARE tmp = ARRAY[CAST(ARRAY[1,2] AS INTEGER[]), CAST(ARRAY[3,4] AS INTEGER[])], 823 AS a, CAST(tmp AS DECLTYPE(tmp[0])[]) AS b, 23434.333 AS c";
  auto str = makeAndSerializeRecord(prog.str());
  auto deserializer = makeDeserializer();
  RecordBuffer deserializeBuf = outputType->getMalloc().malloc();
  const char * input = str.data();
  bool ret = deserializer.deserialize(deserializeBuf, input, str.data() + str.size(), *state.get(), &runtimeCtxt);
  BOOST_CHECK(ret);
  BOOST_CHECK(input == str.data() + str.size());
  BOOST_CHECK(checkFieldEqual("a", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("b", outputBuf, deserializeBuf));
  BOOST_CHECK(checkFieldEqual("c", outputBuf, deserializeBuf));
}

