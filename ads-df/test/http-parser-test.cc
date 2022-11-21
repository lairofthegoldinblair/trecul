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

#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>

#include "HttpParser.hh"

BOOST_AUTO_TEST_CASE(testRequestLineParser)
{
  const char * testString = "GET /my/simple/uri HTTP/1.1\r\nWon'tGetHere";
  {
    AsyncDataBlock blk((uint8_t *) testString, strlen(testString));
    HttpRequestLineParser p;
    BOOST_CHECK(p.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('W', *static_cast<const char *>(blk.data()));
  }
  // Two bites
  for(std::size_t i = 1; i<29; i++) {
    AsyncDataBlock blk((uint8_t *) testString, i);
    HttpRequestLineParser p;
    BOOST_CHECK(p.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK(0 ==blk.size());
    BOOST_CHECK_EQUAL(testString[i], *static_cast<const char *>(blk.data()));
    blk = AsyncDataBlock((uint8_t *) (testString + i), strlen(testString) - i);
    BOOST_CHECK(p.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(blk.size() > 0);
    BOOST_CHECK_EQUAL('W', *static_cast<const char *>(blk.data()));
  }
}
