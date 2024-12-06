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

#include <arpa/inet.h>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <boost/utility.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/find.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time_adjustor.hpp>
#include <antlr3defs.h>

// LLVM Includes
#include "llvm/ExecutionEngine/JITEventListener.h"
#include "llvm/ExecutionEngine/GenericValue.h"
#include "llvm/ADT/iterator_range.h" // For make_range
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/RTDyldMemoryManager.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Bitstream/BitstreamWriter.h"
#include "llvm/Support/Memory.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Mangler.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/Object/ObjectFile.h"

#include "md5.h"
#include "IQLInterpreter.hh"
#include "LLVMGen.h"
#include "CodeGenerationContext.hh"
#include "TypeCheckContext.hh"
#include "GetVariablesPass.h"
#include "IQLLexer.h"
#include "IQLParser.h"
#include "IQLAnalyze.h"
#include "IQLTypeCheck.h"
#include "IQLGetVariables.h"
#include "IQLToLLVM.h"
#include "RecordType.hh"
#include "IQLExpression.hh"

extern "C" {
#include "decNumberLocal.h"
}

class OrcJit
{
private:
  std::unique_ptr<llvm::orc::LLJIT> mJIT;

public:
  OrcJit(std::function<void(llvm::orc::MaterializationResponsibility &, const llvm::object::ObjectFile &Obj,
			    const llvm::RuntimeDyld::LoadedObjectInfo &)> notifyLoaded,
	 std::function<void(llvm::orc::MaterializationResponsibility &, const llvm::object::ObjectFile &Obj,
			    const llvm::RuntimeDyld::LoadedObjectInfo &)> notifyFinalized)
  {
    auto creator = [notifyLoaded](llvm::orc::ExecutionSession & es, const llvm::Triple & tt) -> std::unique_ptr<llvm::orc::ObjectLayer> {
	  auto mem = []() { return std::make_unique<llvm::SectionMemoryManager>(); };	  
	  auto layer = std::make_unique<llvm::orc::RTDyldObjectLinkingLayer>(es, std::move(mem));
	  layer->setNotifyLoaded(notifyLoaded);
	  return layer;
    };
    llvm::ExitOnError exitOnErr;
    mJIT = exitOnErr(llvm::orc::LLJITBuilder().setObjectLinkingLayerCreator(std::move(creator)).create());
    // TODO: Best to limit this search to the external functions we define, but this works
    auto searchGen = exitOnErr(llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(mJIT->getDataLayout().getGlobalPrefix()));
    mJIT->getExecutionSession().getJITDylibByName("main")->addGenerator(std::move(searchGen));
  }

  llvm::Error addModule(llvm::orc::ThreadSafeModule module) {
    return mJIT->addIRModule(std::move(module));
  }

  llvm::Error addObject(std::unique_ptr<llvm::MemoryBuffer> obj) {
    return mJIT->addObjectFile(std::move(obj));
  }

  llvm::Expected<llvm::orc::ExecutorAddr> findSymbol(const std::string Name) {
    return mJIT->lookup(Name);
  }
};

// Forward decls
extern "C" void InternalInt32FromDate(boost::gregorian::date arg,
				      int32_t * ret,
				      InterpreterContext * ctxt);
extern "C" void InternalInt64FromDatetime(boost::posix_time::ptime arg,
					  int64_t * ret,
					  InterpreterContext * ctxt);


InterpreterContext::InterpreterContext() {
  decContextDefault(&mDecimalContext, DEC_INIT_DECIMAL128);
}

InterpreterContext::~InterpreterContext() {
  clear();
}

void * InterpreterContext::malloc(size_t sz) {
  void * tmp = ::malloc(sz);
  mToFree.insert(tmp);
  return tmp;
}

void InterpreterContext::erase(void * ptr) {
  mToFree.erase(ptr);
}

void InterpreterContext::clear() {
  for(std::set<void *>::iterator it = mToFree.begin();
      it != mToFree.end();
      ++it) {
    void * ptr = *it;
    ::free(ptr);
  }
  mToFree.clear();
}

bool InterpreterContext::regex_match(const char* regex_source_c, const char* string) {
  std::string regex_source(regex_source_c);
  regex_cache_type::iterator it = mRegexCache.find(regex_source);
  if (it == mRegexCache.end()) {
    // when it comes time to add a new element to the cache
    if (mRegexCache.size() >= MAX_REGEX_CACHE) {
      // we drop an existing element chosen at random
      // the result is like LRU, but cheaper to implement
      it = mRegexCache.begin();
      std::advance(it, rand() % mRegexCache.size());
      mRegexCache.erase(it);
    }
    mRegexCache[regex_source] = boost::regex(regex_source);
    it = mRegexCache.find(regex_source);
  }
  boost::regex& regex = it->second;
  return boost::regex_match(string, regex);
}

char * Varchar::allocateLarge(int32_t len, 
			      InterpreterContext * ctxt)
{
  return (char *) ctxt->malloc(len);
}

void copyFromString(const char * lhs,
		    int32_t len,
		    Varchar * result,
		    InterpreterContext * ctxt)
{
  result->assign(lhs, len, ctxt);
}

void copyFromString(const char * lhs,
		    Varchar * result,
		    InterpreterContext * ctxt)
{
  copyFromString(lhs, ::strlen(lhs), result, ctxt);
}

void copyFromString(boost::iterator_range<const char *> rng,
		    Varchar * result,
		    InterpreterContext * ctxt)
{
  int32_t len = (int32_t) std::distance(rng.begin(), rng.end());
  copyFromString(rng.begin(), len, result, ctxt);
}

char * allocateVarcharBytes(Varchar * result,
			    int32_t sz,
			    InterpreterContext * ctxt)
{
  if (sz < Varchar::MIN_LARGE_STRING_SIZE) {
    result->Small.Size = sz;
    result->Small.Large = 0;
    return &result->Small.Data[0];
  } else {
    result->Large.Size = sz;
    char * buf = (char *) ctxt->malloc(result->Large.Size + 1);
    result->Large.Ptr = buf;
    result->Large.Large = 1;
    return buf;
  }
}

void getVarcharRange(const Varchar * in,
		     const char * & ptr,
		     unsigned & sz)
{
  if (!in->Large.Large) {
    ptr = &in->Small.Data[0];
    sz = in->Small.Size;
  } else {
    ptr = in->Large.Ptr;
    sz = in->Large.Size;
  }
}

// TODO: Add the Int64 stuff to decNumber.c
/* ------------------------------------------------------------------ */
/* decGetDigits -- count digits in a Units array                      */
/*                                                                    */
/*   uar is the Unit array holding the number (this is often an       */
/*          accumulator of some sort)                                 */
/*   len is the length of the array in units [>=1]                    */
/*                                                                    */
/*   returns the number of (significant) digits in the array          */
/*                                                                    */
/* All leading zeros are excluded, except the last if the array has   */
/* only zero Units.                                                   */
/* ------------------------------------------------------------------ */
// This may be called twice during some operations.
static Int decGetDigits(Unit *uar, Int len) {
  Unit *up=uar+(len-1);            // -> msu
  Int  digits=(len-1)*DECDPUN+1;   // possible digits excluding msu
  #if DECDPUN>4
  uInt const *pow;                 // work
  #endif
                                   // (at least 1 in final msu)
  #if DECCHECK
  if (len<1) printf("decGetDigits called with len<1 [%ld]\n", (LI)len);
  #endif

  for (; up>=uar; up--) {
    if (*up==0) {                  // unit is all 0s
      if (digits==1) break;        // a zero has one digit
      digits-=DECDPUN;             // adjust for 0 unit
      continue;}
    // found the first (most significant) non-zero Unit
    #if DECDPUN>1                  // not done yet
    if (*up<10) break;             // is 1-9
    digits++;
    #if DECDPUN>2                  // not done yet
    if (*up<100) break;            // is 10-99
    digits++;
    #if DECDPUN>3                  // not done yet
    if (*up<1000) break;           // is 100-999
    digits++;
    #if DECDPUN>4                  // count the rest ...
    for (pow=&powers[4]; *up>=*pow; pow++) digits++;
    #endif
    #endif
    #endif
    #endif
    break;
    } // up
  return digits;
  } // decGetDigits

decNumber * decNumberFromUInt64(decNumber *dn, uint64_t uin) {
  Unit *up;                             // work pointer
  decNumberZero(dn);                    // clean
  if (uin==0) return dn;                // [or decGetDigits bad call]
  for (up=dn->lsu; uin>0; up++) {
    *up=(Unit)(uin%(DECDPUNMAX+1));
    uin=uin/(DECDPUNMAX+1);
    }
  dn->digits=decGetDigits(dn->lsu, up-dn->lsu);
  return dn;
  } // decNumberFromUInt64

decNumber * decNumberFromInt64(decNumber *dn, int64_t in) {
  uint64_t unsig;
  if (in>=0) unsig=in;
  else unsig=-in;                    // invert
  // in is now positive
  decNumberFromUInt64(dn, unsig);
  if (in<0) dn->bits=DECNEG;            // sign needed
  return dn;
  } // decNumberFromInt64

int64_t decNumberToInt64(const decNumber *dn, decContext *set) {
#if DECCHECK
  if (decCheckOperands(DECUNRESU, DECUNUSED, dn, set)) return 0;
#endif

  // special or too many digits, or bad exponent
  if (dn->bits&DECSPECIAL || dn->digits>20 || dn->exponent!=0) ; // bad
  else { // is a finite integer with 20 or fewer digits
    int64_t d;                     // work
    const Unit *up;                // ..
    uint64_t hi=0, lo;             // ..
    up=dn->lsu;                    // -> lsu
    lo=*up;                        // get 1 to 19 digits
#if DECDPUN>1                  // split to higher
    hi=lo/10;
    lo=lo%10;
#endif
    up++;
    // collect remaining Units, if any, into hi
    for (d=DECDPUN; d<dn->digits; up++, d+=DECDPUN) hi+=*up*DECPOWERS[d-1];
    // now low has the lsd, hi the remainder
    if (hi>922337203685477580LL || (hi==922337203685477580LL && lo>7)) { // out of range?
      // most-negative is a reprieve
      if (dn->bits&DECNEG && hi==922337203685477580LL && lo==8) return 0x80000000;
      // bad -- drop through
    }
    else { // in-range always
      Int i=X10(hi)+lo;
      if (dn->bits&DECNEG) return -i;
      return i;
    }
  } // integer
  decContextSetStatus(set, DEC_Invalid_operation); // [may not return]
  return 0;
} // decNumberToInt32

template<typename IntTraits>
typename IntTraits::int_type decNumberToInt(const decNumber *dn, decContext *set) {
#if DECCHECK
  if (decCheckOperands(DECUNRESU, DECUNUSED, dn, set)) return 0;
#endif

  // special or too many digits, or bad exponent
  if (dn->bits&DECSPECIAL || dn->digits>IntTraits::max_digits || dn->exponent!=0) ; // bad
  else { // is a finite integer with 20 or fewer digits
    typename IntTraits::int_type d;    // work
    const Unit *up;// ..
    typename IntTraits::uint_type hi=0, lo; // ..
    up=dn->lsu;                    // -> lsu
    lo=*up;                        // get 1 to 19 digits
#if DECDPUN>1                  // split to higher
    hi=lo/10;
    lo=lo%10;
#endif
    up++;
    // collect remaining Units, if any, into hi
    for (d=DECDPUN; d<dn->digits; up++, d+=DECDPUN) hi+=*up*DECPOWERS[d-1];
    // now low has the lsd, hi the remainder
    if (hi>IntTraits::max_value || (hi==IntTraits::max_value && lo>7)) { // out of range?
      // most-negative is a reprieve
      if (dn->bits&DECNEG && hi==922337203685477580LL && lo==8) return IntTraits::min_value;
      // bad -- drop through
    }
    else { // in-range alwaysp
      typename IntTraits::int_type i=X10(hi)+lo;
      if (dn->bits&DECNEG) return -i;
      return i;
    }
  } // integer
  decContextSetStatus(set, DEC_Invalid_operation); // [may not return]
  return 0;
} // decNumberToInt

struct Int8Traits
{
  typedef int8_t int_type;
  typedef uint8_t uint_type;
  static const int max_digits = 3;
  static const int_type max_value = std::numeric_limits<int8_t>::max();
  static const int_type min_value = std::numeric_limits<int8_t>::min();
};

struct Int16Traits
{
  typedef int16_t int_type;
  typedef uint16_t uint_type;
  static const int max_digits = 5;
  static const int_type max_value = std::numeric_limits<int16_t>::max();
  static const int_type min_value = std::numeric_limits<int16_t>::min();
};

struct Int64Traits
{
  typedef int64_t int_type;
  typedef uint64_t uint_type;
  static const int max_digits = 20;
  static const int_type max_value = std::numeric_limits<int64_t>::max();
  static const int_type min_value = std::numeric_limits<int64_t>::min();
};

double decNumberToDouble(const decNumber *dn, decContext *set) {
#if DECCHECK
  if (decCheckOperands(DECUNRESU, DECUNUSED, dn, set)) return 0;
#endif

  // special or too many digits, or bad exponent
  if (dn->bits&DECSPECIAL || dn->digits>20) ; // bad
  else { // is a finite integer with 20 or fewer digits
    int64_t d;                     // work
    const Unit *up;                // ..
    uint64_t hi=0, lo;             // ..
    up=dn->lsu;                    // -> lsu
    lo=*up;                        // get 1 to 19 digits
#if DECDPUN>1                  // split to higher
    hi=lo/10;
    lo=lo%10;
#endif
    up++;
    // collect remaining Units, if any, into hi
    for (d=DECDPUN; d<dn->digits; up++, d+=DECDPUN) hi+=*up*DECPOWERS[d-1];
    // now low has the lsd, hi the remainder
    if (hi>922337203685477580LL || (hi==922337203685477580LL && lo>7)) { // out of range?
      // most-negative is a reprieve
      if (dn->bits&DECNEG && hi==922337203685477580LL && lo==8) return 0x80000000;
      // bad -- drop through
    }
    else { // in-range always
      Int i=X10(hi)+lo;
      double ret = pow(10, dn->exponent)*i;
      if (dn->bits&DECNEG) return -ret;
      return ret;
    }
  } // integer
  decContextSetStatus(set, DEC_Invalid_operation); // [may not return]
  return 0;
} // decNumberToDouble

std::ostream& operator<<(std::ostream& str, decimal128& val) {
  char buf[DECIMAL128_String];
  decimal128ToString(&val, buf);
  str << buf;
  return str;
}

extern "C" void InternalDecimalAdd(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberAdd(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalSub(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberSubtract(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalMul(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberMultiply(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalDiv(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberDivide(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalNeg(decimal128 * lhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b;
  decimal128ToNumber(lhs, &a);
  decNumberCopyNegate(&b, &a);
  decimal128FromNumber(result, &b, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromVarchar(const Varchar * arg,
					   decimal128 * ret,
					   InterpreterContext * ctxt) 
{
  ::decimal128FromString(ret, arg->c_str(), ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromChar(const char * arg,
				       decimal128 * ret,
				       InterpreterContext * ctxt) 
{
  ::decimal128FromString(ret, arg, ctxt->getDecimalContext());
}

static void printEscaped(const char * begin, int32_t sz, 
                         char escapeChar, std::ostream& ostr)
{
  if (escapeChar != 0) {
    const char * it = begin;
    const char * end = begin + sz;
    for(; it!=end; it++) {
      switch(*it) {
      case '\n':
	ostr << escapeChar << 'n';
	break;
      case '\b':
	ostr << escapeChar << 'b';
	break;
      case '\f':
	ostr << escapeChar << 'f';
	break;
      case '\t':
	ostr << escapeChar << 't';
	break;
      case '\r':
	ostr << escapeChar << 'r';
	break;
      case '\\':
	ostr << escapeChar << '\\';
	break;
      default:
	ostr << *it;
	break;
      }
    }
  } else {
    // Raw output
    ostr << begin;
  }
}

extern "C" void InternalPrintVarchar(const Varchar * arg,
                                     char escapeChar,
                                     uint8_t * ostr)
{
  const char * begin = arg->c_str();
  int32_t sz = arg->size();
  printEscaped(begin, sz, escapeChar, *(reinterpret_cast<std::ostream *>(ostr)));
}

extern "C" void InternalPrintChar(const char * arg,
                                  char escapeChar,
                                  uint8_t * ostr)
{
  const char * begin = arg;
  // TODO: Pad to static length
  int32_t sz = ::strlen(begin);
  printEscaped(begin, sz, escapeChar, *reinterpret_cast<std::ostream *>(ostr));
}

extern "C" void InternalPrintCharRaw(const char * arg,
                                     uint8_t * ostr)
{
  *reinterpret_cast<std::ostream *>(ostr) << arg;
}

extern "C" void InternalPrintDecimal(decimal128 * arg,
                                     uint8_t * ostr)
{
  char buffer[DECIMAL128_String];
  decimal128ToString(arg, &buffer[0]);
  *reinterpret_cast<std::ostream *>(ostr) << buffer;
}

extern "C" void InternalPrintInt64(int64_t arg,
                                   uint8_t * ostr)
{
  *reinterpret_cast<std::ostream *>(ostr) << arg;
}

extern "C" void InternalPrintDouble(double arg,
                                    uint8_t * ostr)
{
  *reinterpret_cast<std::ostream *>(ostr) << arg;
}

extern "C" void InternalPrintDatetime(boost::posix_time::ptime arg,
                                      uint8_t * ostr)
{
  *reinterpret_cast<std::ostream *>(ostr) << arg;
}

extern "C" void InternalPrintDate(boost::gregorian::date arg,
                                      uint8_t * ostr)
{
  *reinterpret_cast<std::ostream *>(ostr) << arg;
}

extern "C" void InternalPrintIPv4(boost::asio::ip::address_v4::bytes_type network_order,
                                  uint8_t * ostr)
{
  boost::asio::ip::address_v4 addr(network_order);
  *reinterpret_cast<std::ostream *>(ostr) << addr;
}

extern "C" void InternalPrintCIDRv4(int64_t arg,
                                    uint8_t * ostr)
{
  const CidrV4Runtime * cidr = (const CidrV4Runtime *) &arg;
  boost::asio::ip::address_v4 addr(cidr->prefix);
  *reinterpret_cast<std::ostream *>(ostr) << addr;
  *reinterpret_cast<std::ostream *>(ostr) <<  "/";
  *reinterpret_cast<std::ostream *>(ostr) <<  boost::lexical_cast<std::string>((int) cidr->prefix_length);
}

static const unsigned char v4MappedPrefix[12] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF };

extern "C" void InternalPrintIPv6(const char * lhs, 
                                  uint8_t * ostr)
{
  const uint8_t * octets = (const uint8_t *) lhs;
  char buf[INET6_ADDRSTRLEN];
  if (memcmp(lhs, v4MappedPrefix, sizeof(v4MappedPrefix)) == 0) {
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u", octets[12], octets[13], octets[14], octets[15]);
  } else {
    const char * result = ::inet_ntop(AF_INET6, (struct in6_addr *) lhs, buf, sizeof(buf));
    if (result == 0) {
      buf[0] = 0;
    }
  }
  *reinterpret_cast<std::ostream *>(ostr) << buf;
}

extern "C" void InternalPrintCIDRv6(const char * lhs, 
                                    uint8_t * ostr)
{
  const uint8_t * octets = (const uint8_t *) lhs;
  char buf[INET6_ADDRSTRLEN+4];
  if (memcmp(lhs, v4MappedPrefix, sizeof(v4MappedPrefix)) == 0 && octets[16]>=96) {
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u/%u", octets[12], octets[13], octets[14], octets[15], octets[16]-96);
  } else {
    const char * result = ::inet_ntop(AF_INET6, (struct in6_addr *) lhs, buf, sizeof(buf));
    if (result == 0) {
      buf[0] = 0;
    } else {
      auto len = ::strlen(result);
      snprintf(&buf[len], sizeof(buf)-len, "/%u", octets[16]);      
    }
  }
  
  *reinterpret_cast<std::ostream *>(ostr) << buf;
}

extern "C" void InternalDecimalFromInt8(int8_t val, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a;
  decNumberFromInt32(&a, val);
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromInt16(int16_t val, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a;
  decNumberFromInt32(&a, val);
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromInt32(int32_t val, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a;
  decNumberFromInt32(&a, val);
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromInt64(int64_t val, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a;
  decNumberFromInt64(&a, val);
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromDouble(double arg,
					  decimal128 * ret,
					  InterpreterContext * ctxt) 
{
  char buf[32];
  snprintf(buf, 32, "%.15e", arg);
  ::decimal128FromString(ret, buf, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromFloat(float arg,
                                         decimal128 * ret,
                                         InterpreterContext * ctxt) 
{
  InternalDecimalFromFloat(arg, ret, ctxt);
}

extern "C" void InternalDecimalFromDate(boost::gregorian::date arg,
					decimal128 * ret,
					InterpreterContext * ctxt) 
{
  int32_t tmp;
  InternalInt32FromDate(arg, &tmp, ctxt);
  return InternalDecimalFromInt32(tmp, ret, ctxt);
}

extern "C" void InternalDecimalFromDatetime(boost::posix_time::ptime arg,
					    decimal128 * ret,
					    InterpreterContext * ctxt) 
{
  int64_t tmp;
  InternalInt64FromDatetime(arg, &tmp, ctxt);
  return InternalDecimalFromInt64(tmp, ret, ctxt);
}

extern "C" void InternalDecimalCmp(decimal128 * lhs, decimal128 * rhs, int32_t * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberCompare(&c, &b, &a, ctxt->getDecimalContext());
  *result = c.lsu[0];
  if (c.bits & DECNEG)
    *result *= -1;
}

extern "C" void InternalDecimalRound(decimal128 * lhs, int32_t precision, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b;
  decimal128ToNumber(lhs, &a);
  decNumberFromInt32(&b, -precision);
  decNumberRescale(&a, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalVarcharAdd(Varchar* lhs, Varchar* rhs, Varchar* result, InterpreterContext * ctxt) {
  char * buf = NULL;
  switch(lhs->Large.Large | (rhs->Large.Large << 1)) {
  case 0:
    buf = allocateVarcharBytes(result, lhs->Small.Size+rhs->Small.Size, ctxt);
    memcpy(buf, &lhs->Small.Data[0], lhs->Small.Size);
    memcpy(buf+lhs->Small.Size, &rhs->Small.Data[0], rhs->Small.Size);
    buf[lhs->Small.Size + rhs->Small.Size] = 0;
    break;
  case 1:
    result->Large.Size = lhs->Large.Size + rhs->Small.Size;
    buf = (char *) ctxt->malloc(result->Large.Size + 1);
    memcpy(buf, lhs->Large.Ptr, lhs->Large.Size);
    memcpy(buf+lhs->Large.Size, &rhs->Small.Data[0], rhs->Small.Size);
    buf[lhs->Large.Size + rhs->Small.Size] = 0;
    result->Large.Ptr = buf;
    result->Large.Large = 1;
    break;
  case 2:
    result->Large.Size = lhs->Small.Size + rhs->Large.Size;
    buf = (char *) ctxt->malloc(result->Large.Size + 1);
    memcpy(buf, &lhs->Small.Data[0], lhs->Small.Size);
    memcpy(buf+lhs->Small.Size, rhs->Large.Ptr, rhs->Large.Size);
    buf[lhs->Small.Size + rhs->Large.Size] = 0;
    result->Large.Ptr = buf;
    result->Large.Large = 1;
    break;
  case 3:
    result->Large.Size = lhs->Large.Size + rhs->Large.Size;
    buf = (char *) ctxt->malloc(result->Large.Size + 1);
    memcpy(buf, lhs->Large.Ptr, lhs->Large.Size);
    memcpy(buf+lhs->Large.Size, rhs->Large.Ptr, rhs->Large.Size);
    buf[lhs->Large.Size + rhs->Large.Size] = 0;
    result->Large.Ptr = buf;
    result->Large.Large = 1;
    break;
  }
}

extern "C" void InternalVarcharCopy(Varchar * lhs, Varchar * result, int32_t trackForDelete, InterpreterContext * ctxt) {
  if (!lhs->Large.Large) {
    result->Small = lhs->Small;
  } else {
    result->Large.Large = lhs->Large.Large;
    result->Large.Size = lhs->Large.Size;
    char * buf = trackForDelete ? 
      (char *) ctxt->malloc(result->Large.Size + 1) : 
      (char*) ::malloc(result->Large.Size + 1);
    memcpy(buf, lhs->Large.Ptr, lhs->Large.Size);
    buf[lhs->Large.Size] = 0;
    result->Large.Ptr = buf;
  }
}

extern "C" void InternalVarcharAllocate(int32_t len, int32_t sz, Varchar * result, int32_t trackForDelete, InterpreterContext * ctxt) {
  result->Large.Large = 1;
  result->Large.Size = len;
  char * buf = trackForDelete ? 
    (char *) ctxt->malloc(sz) : 
    (char*) ::malloc(sz);
  ::memset(buf, 0, sz);
  result->Large.Ptr = buf;
}

extern "C" void InternalVarcharErase(Varchar * lhs, InterpreterContext * ctxt) {
  // Remove varchar pointer from internal heap tracking
  if (lhs->Large.Large) {
    ctxt->erase(const_cast<char *>(lhs->Large.Ptr));
  }
}

extern "C" int32_t InternalVarcharEquals(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  if (lhs->Large.Large) {
    return rhs->Large.Large && lhs->Large.Size == rhs->Large.Size && 
      0 == memcmp(lhs->Large.Ptr, rhs->Large.Ptr, lhs->Large.Size);
  } else {
    return 0 == rhs->Large.Large && lhs->Small.Size == rhs->Small.Size && 
      0 == memcmp(&lhs->Small.Data[0], &rhs->Small.Data[0], lhs->Small.Size);
  }
}

extern "C" int32_t InternalVarcharNE(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  if (lhs->Large.Large) {
    return !rhs->Large.Large || lhs->Large.Size != rhs->Large.Size || 
      0 != memcmp(lhs->Large.Ptr, rhs->Large.Ptr, lhs->Large.Size);
  } else {
    return rhs->Large.Large || lhs->Small.Size != rhs->Small.Size || 
      0 != memcmp(&lhs->Small.Data[0], &rhs->Small.Data[0], lhs->Small.Size);
  }
}

extern "C" int32_t InternalVarcharLT(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->c_str(), rhs->c_str());
  return cmp < 0;
}

extern "C" int32_t InternalVarcharLE(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->c_str(), rhs->c_str());
  return cmp <= 0;
}

extern "C" int32_t InternalVarcharGT(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->c_str(), rhs->c_str());
  return cmp > 0;
}

extern "C" int32_t InternalVarcharGE(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->c_str(), rhs->c_str());
  return cmp >= 0;
}

extern "C" int32_t InternalVarcharRLike(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  return ctxt->regex_match(rhs->c_str(), lhs->c_str());
}

/**
 * convert a hex char to its nibble value.
 * return 127 if not valid hex char.
 */ 
static char hexNibble(char c)
{
  if (c >= '0' && c <= '9') {
    return c - '0';
  } else {
    char lower = c | 0x20;
    if (lower >= 'a' && lower <= 'f') {
      return lower - 'a' + 10;
    }
  }
  return 127;
}

extern "C" void InternalVarcharUrlDecode(Varchar* lhs, Varchar* result, InterpreterContext * ctxt) {
  std::string s;
  int32_t sz = lhs->size();
  s.reserve(sz);
  const char * p = lhs->c_str();
  const char * e = p + sz;
  for(; p != e; ++p) {
    switch(*p) {
    case '%':
      {
	if(++p != e) {
	  char decode1 = hexNibble(*p);
	  if(decode1 != 127 && ++p != e) {
	    char decode2 = hexNibble(*p);
	    if (decode2 != 127) {
	      s += (decode1 << 4) | decode2;
	    }
	    break;
	  }
	}
	// TODO: Error if we get here.
	break;
      }
    case '+':
      s += ' ';
      break;
    default:
      s += *p;
      break;
    }
  }
  copyFromString(s.c_str(), (int32_t) s.size(), result, ctxt);
}

extern "C" void InternalVarcharUrlEncode(Varchar* lhs, Varchar* result, InterpreterContext * ctxt) {
  static const char * hexLut = "0123456789ABCDEF";
  std::string s;
  int32_t sz = lhs->size();
  s.reserve(2*sz);
  const char * p = lhs->c_str();
  const char * e = p + sz;
  for(; p != e; ++p) {
    switch(*p) {
    case '0': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9':
    case 'a': case 'b': case 'c': case 'd': case 'e':
    case 'f': case 'g': case 'h': case 'i': case 'j':
    case 'k': case 'l': case 'm': case 'n': case 'o':
    case 'p': case 'q': case 'r': case 's': case 't':
    case 'u': case 'v': case 'w': case 'x': case 'y': case 'z':
    case 'A': case 'B': case 'C': case 'D': case 'E':
    case 'F': case 'G': case 'H': case 'I': case 'J':
    case 'K': case 'L': case 'M': case 'N': case 'O':
    case 'P': case 'Q': case 'R': case 'S': case 'T':
    case 'U': case 'V': case 'W': case 'X': case 'Y': case 'Z':
    case '.': case '-': case '*': case '_': 
      s += *p;
      break;
    case ' ':
      s += '+';
      break;
    default:
      s += '%';
      s += hexLut[*reinterpret_cast<const uint8_t *>(p) >> 4];
      s += hexLut[*p & 0x0f];
      break;
    }
  }
  copyFromString(s.c_str(), (int32_t) s.size(), result, ctxt);
}

extern "C" void InternalCharFromVarchar(Varchar* in, char * out, int32_t outSz) {
  if (in->Large.Large) {
    if (outSz <= in->Large.Size) {
      memcpy(out, in->Large.Ptr, outSz);
    } else {
      memcpy(out, in->Large.Ptr, in->Large.Size);
      memset(out+in->Large.Size, ' ', outSz-in->Large.Size);
    }
  } else {
    if (outSz <= in->Small.Size) {
      memcpy(out, &in->Small.Data[0], outSz);
    } else {
      memcpy(out, &in->Small.Data[0], in->Small.Size);
      memset(out+in->Small.Size, ' ', outSz-in->Small.Size);
    }
  }
  // Null terminate.
  out[outSz] = 0;
}

extern "C" void InternalVarcharReplace(Varchar* lhs, 
				       Varchar* pattern,
				       Varchar* format,
				       Varchar * result,
				       InterpreterContext * ctxt) {
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  std::string s(ptr, sz);
  boost::replace_all(s, pattern->c_str(), format->c_str());
  copyFromString(s.c_str(), (int32_t) s.size(), result, ctxt);
}

extern "C" int32_t InternalVarcharLocate(Varchar* pattern, 
					 Varchar* lhs) {
  unsigned patSz;
  const char * patPtr;
  getVarcharRange(pattern, patPtr, patSz);  
  // MySQL Behavior: empty space always returns 1.
  if (0 == patSz) return 1;
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  boost::iterator_range<const char*> rng(ptr, ptr+sz);
  boost::iterator_range<const char*> patRng(patPtr, patPtr+patSz);
  boost::iterator_range<const char*> result 
    = boost::find_first(rng, patRng);
  return result.begin()== rng.end() ? 0 : 
    (int32_t) (result.begin()-rng.begin() + 1);
}

extern "C" void substr(Varchar* lhs, 
		       int32_t start, 
		       int32_t len,
		       Varchar * result,
		       InterpreterContext * ctxt) {
  // Size of the result.
  if (len < 0) len = std::numeric_limits<int32_t>::max();
  // If start is too big then return empty string.
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  if (sz <= start) {
    start = sz;
    len = 0;
  }
  unsigned resultSz  = std::min((unsigned) len, sz - start);
  copyFromString(ptr+start, resultSz, result, ctxt);
}

extern "C" void trim(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  boost::iterator_range<const char*> rng(ptr, ptr+sz);
  rng = boost::trim_copy(rng);
  copyFromString(rng, result, ctxt);
}

extern "C" void ltrim(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  boost::iterator_range<const char*> rng(ptr, ptr+sz);
  rng = boost::trim_left_copy(rng);
  copyFromString(rng, result, ctxt);
}

extern "C" void rtrim(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  boost::iterator_range<const char*> rng(ptr, ptr+sz);
  rng = boost::trim_right_copy(rng);
  copyFromString(rng, result, ctxt);
}

extern "C" void lower(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  if (lhs->Large.Large) {
    result->Large.Size = lhs->Large.Size;
    char * buf = (char *) ctxt->malloc(result->Large.Size + 1);
    boost::iterator_range<const char*> rng(lhs->Large.Ptr, 
					   lhs->Large.Ptr+lhs->Large.Size);
    boost::to_lower_copy(buf, rng);
    buf[result->Large.Size] = 0;
    result->Large.Ptr = buf;  
    result->Large.Large = 1;
  } else {
    result->Small.Size = lhs->Small.Size;
    boost::iterator_range<const char*> rng(&lhs->Small.Data[0], 
					   &lhs->Small.Data[0]+lhs->Small.Size);
    boost::to_lower_copy(&result->Small.Data[0], rng);
    result->Small.Data[result->Small.Size] = 0;
    result->Small.Large = 0;
  }
}

extern "C" void upper(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  if (lhs->Large.Large) {
    result->Large.Size = lhs->Large.Size;
    char * buf = (char *) ctxt->malloc(result->Large.Size + 1);
    boost::iterator_range<const char*> rng(lhs->Large.Ptr, 
					   lhs->Large.Ptr+lhs->Large.Size);
    boost::to_upper_copy(buf, rng);
    buf[result->Large.Size] = 0;
    result->Large.Ptr = buf;  
    result->Large.Large = 1;
  } else {
    result->Small.Size = lhs->Small.Size;
    boost::iterator_range<const char*> rng(&lhs->Small.Data[0], 
					   &lhs->Small.Data[0]+lhs->Small.Size);
    boost::to_upper_copy(&result->Small.Data[0], rng);
    result->Small.Data[result->Small.Size] = 0;
    result->Small.Large = 0;
  }
}

extern "C" int32_t length(Varchar* lhs) {
  return lhs->size();
}

extern "C" void InternalVarcharFromChar(const char * lhs, 
					Varchar * result,
					InterpreterContext * ctxt) {
  copyFromString(lhs, result, ctxt);
}

extern "C" void InternalVarcharFromInt32(int32_t val,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  // 10 digits + trailing EOS + optional - 
  if (11 < Varchar::MIN_LARGE_STRING_SIZE) {
    snprintf(&result->Small.Data[0], sizeof(result->Small.Data), "%d", val);
    result->Small.Size = strlen(&result->Small.Data[0]);
    result->Small.Large = 0;
  } else {
    char buf[12];
    snprintf(buf, 12, "%d", val);
    copyFromString(&buf[0], result, ctxt);
  }
}

extern "C" void InternalVarcharFromInt8(int8_t val,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  InternalVarcharFromInt32(val, result, ctxt);
}

extern "C" void InternalVarcharFromInt16(int16_t val,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  InternalVarcharFromInt32(val, result, ctxt);
}

extern "C" void InternalVarcharFromInt64(int64_t val,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  // 20 digits + trailing EOS + optional - 
  char buf[22];
  // Cast to hush warnings from gcc
  snprintf(buf, 22, "%lld", (long long int) val);
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromDecimal(decimal128 * val,
					   Varchar * result,
					   InterpreterContext * ctxt) 
{
  char buf[DECIMAL128_String + 1];
  ::decimal128ToString(val, buf);
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromDouble(double val,
					  Varchar * result,
					  InterpreterContext * ctxt) 
{
  char buf[64];
  snprintf(buf, 64, "%.15g", val);
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromFloat(float val,
                                         Varchar * result,
                                         InterpreterContext * ctxt) 
{
  InternalVarcharFromDouble(val, result, ctxt);
}

extern "C" void InternalVarcharFromDate(boost::gregorian::date d,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  boost::gregorian::greg_year_month_day parts = d.year_month_day();
  if (10 < Varchar::MIN_LARGE_STRING_SIZE && parts.year <= 9999U) {
    char * buf = &result->Small.Data[0];
    snprintf(buf, sizeof(result->Small.Data), "%04d-%02d-%02d", (int32_t) parts.year, 
	    (int32_t) parts.month, (int32_t) parts.day);
    result->Small.Size = 10;
    result->Small.Large = 0;
  } else {
    char buf[64];
    snprintf(buf, 64, "%04d-%02d-%02d", (int32_t) parts.year, 
	    (int32_t) parts.month, (int32_t) parts.day);
    copyFromString(&buf[0], result, ctxt);
  }
}

extern "C" void InternalVarcharFromDatetime(boost::posix_time::ptime t,
					    Varchar * result,
					    InterpreterContext * ctxt) 
{
  // We should only use 20 bytes here but some compilers warn about truncation
  char buf[128];
  boost::gregorian::greg_year_month_day parts = t.date().year_month_day();
  boost::posix_time::time_duration dur = t.time_of_day();
  snprintf(buf, 128, "%04d-%02d-%02d %02d:%02d:%02d", (int32_t) parts.year, (int32_t) parts.month,
	  (int32_t) parts.day, (int32_t) dur.hours(), (int32_t) dur.minutes(), (int32_t) dur.seconds());
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromIPv4(boost::asio::ip::address_v4::bytes_type network_order,
                                        Varchar * result,
                                        InterpreterContext * ctxt) 
{
  boost::asio::ip::address_v4 addr(network_order);
  std::string buf = addr.to_string();
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromCIDRv4(int64_t arg,
                                          Varchar * result,
                                          InterpreterContext * ctxt) 
{
  const CidrV4Runtime * cidr = (const CidrV4Runtime *) &arg;
  boost::asio::ip::address_v4 addr(cidr->prefix);
  std::string buf = addr.to_string();
  buf += "/";
  buf += boost::lexical_cast<std::string>((int) cidr->prefix_length);
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromIPv6(const char * lhs, 
                                        Varchar * result,
                                        InterpreterContext * ctxt) {
  const uint8_t * octets = (const uint8_t *) lhs;
  char buf[INET6_ADDRSTRLEN];
  if (memcmp(lhs, v4MappedPrefix, sizeof(v4MappedPrefix)) == 0) {
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u", octets[12], octets[13], octets[14], octets[15]);
  } else {
    const char * result = ::inet_ntop(AF_INET6, (struct in6_addr *) lhs, buf, sizeof(buf));
    if (result == 0) {
      buf[0] = 0;
    }
  }
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalVarcharFromCIDRv6(const char * lhs, 
                                        Varchar * result,
                                        InterpreterContext * ctxt) {
  const uint8_t * octets = (const uint8_t *) lhs;
  char buf[INET6_ADDRSTRLEN+4];
  if (memcmp(lhs, v4MappedPrefix, sizeof(v4MappedPrefix)) == 0) {
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u/%u", octets[12], octets[13], octets[14], octets[15], octets[16]);
  } else {
    const char * result = ::inet_ntop(AF_INET6, (struct in6_addr *) lhs, buf, sizeof(buf));
    if (result == 0) {
      buf[0] = 0;
    } else {
      auto len = ::strlen(result);
      snprintf(&buf[len], sizeof(buf)-len, "/%u", octets[16]);      
    }
  }
  
  copyFromString(&buf[0], result, ctxt);
}

extern "C" void InternalInt8FromVarchar(const Varchar * arg,
					 int8_t * ret,
					 InterpreterContext * ctxt) 
{
  try {
    *ret = boost::lexical_cast<int>(arg->c_str());
  } catch(std::exception & ex) {
    std::cerr << "Failed casting VARCHAR to TINYINT : " << arg->c_str() << std::endl;
    *ret = 0;
  }
}

extern "C" void InternalInt8FromChar(const char * arg,
				      int8_t * ret,
				      InterpreterContext * ctxt) 
{
  try {
    *ret = boost::lexical_cast<int>(arg);
  } catch(std::exception & ex) {
    std::cerr << "Failed casting CHAR to TINYINT : " << arg << std::endl;
    *ret = 0;
  }
}

extern "C" void InternalInt8FromDecimal(decimal128 * arg,
					 int64_t * ret,
					 InterpreterContext * ctxt) 
{
  decNumber a;
  decimal128ToNumber(arg, &a);
  char buf[100];
  decNumber one;
  decNumberFromInt32(&one, 1);
  decNumber rounded;
  decNumberQuantize(&rounded, &a, &one, ctxt->getDecimalContext());
  *ret = decNumberToInt<Int8Traits>(&rounded, ctxt->getDecimalContext());
}

extern "C" void InternalInt8FromDate(boost::gregorian::date arg,
				      int8_t * ret,
				      InterpreterContext * ctxt) 
{
  boost::gregorian::greg_year_month_day ymd = arg.year_month_day();
  *ret = ymd.year*10000 + ymd.month*100 + ymd.day;
}

extern "C" void InternalInt8FromDatetime(boost::posix_time::ptime arg,
					  int8_t * ret,
					  InterpreterContext * ctxt) 
{
  InternalInt8FromDate(arg.date(), ret, ctxt);
}

extern "C" void InternalInt16FromVarchar(const Varchar * arg,
					 int16_t * ret,
					 InterpreterContext * ctxt) 
{
  *ret = boost::lexical_cast<int16_t>(arg->c_str());
}

extern "C" void InternalInt16FromChar(const char * arg,
				      int16_t * ret,
				      InterpreterContext * ctxt) 
{
  *ret = boost::lexical_cast<int16_t>(arg);
}

extern "C" void InternalInt16FromDecimal(decimal128 * arg,
					 int64_t * ret,
					 InterpreterContext * ctxt) 
{
  decNumber a;
  decimal128ToNumber(arg, &a);
  char buf[100];
  decNumber one;
  decNumberFromInt32(&one, 1);
  decNumber rounded;
  decNumberQuantize(&rounded, &a, &one, ctxt->getDecimalContext());
  *ret = decNumberToInt<Int16Traits>(&rounded, ctxt->getDecimalContext());
}

extern "C" void InternalInt16FromDate(boost::gregorian::date arg,
				      int16_t * ret,
				      InterpreterContext * ctxt) 
{
  boost::gregorian::greg_year_month_day ymd = arg.year_month_day();
  *ret = ymd.year*10000 + ymd.month*100 + ymd.day;
}

extern "C" void InternalInt16FromDatetime(boost::posix_time::ptime arg,
					  int16_t * ret,
					  InterpreterContext * ctxt) 
{
  InternalInt16FromDate(arg.date(), ret, ctxt);
}

extern "C" void InternalInt32FromVarchar(const Varchar * arg,
					 int32_t * ret,
					 InterpreterContext * ctxt) 
{
  *ret = atoi(arg->c_str());
}

extern "C" void InternalInt32FromChar(const char * arg,
				      int32_t * ret,
				      InterpreterContext * ctxt) 
{
  *ret = atoi(arg);
}

extern "C" void InternalInt32FromDecimal(decimal128 * arg,
					 int32_t * ret,
					 InterpreterContext * ctxt) 
{
  decNumber a;
  decimal128ToNumber(arg, &a);
  char buf[100];
  decNumber one;
  decNumberFromInt32(&one, 1);
  decNumber rounded;
  decNumberQuantize(&rounded, &a, &one, ctxt->getDecimalContext());
  *ret = decNumberToInt32(&rounded, ctxt->getDecimalContext());
}

extern "C" void InternalInt32FromDate(boost::gregorian::date arg,
				      int32_t * ret,
				      InterpreterContext * ctxt) 
{
  boost::gregorian::greg_year_month_day ymd = arg.year_month_day();
  *ret = ymd.year*10000 + ymd.month*100 + ymd.day;
}

extern "C" void InternalInt32FromDatetime(boost::posix_time::ptime arg,
					  int32_t * ret,
					  InterpreterContext * ctxt) 
{
  InternalInt32FromDate(arg.date(), ret, ctxt);
}

extern "C" void InternalInt64FromVarchar(const Varchar * arg,
					 int64_t * ret,
					 InterpreterContext * ctxt) 
{
  *ret = std::strtoll(arg->c_str(), NULL, 10);
}

extern "C" void InternalInt64FromChar(const char * arg,
				      int64_t * ret,
				      InterpreterContext * ctxt) 
{
  *ret = std::strtoll(arg, NULL, 10);
}

extern "C" void InternalInt64FromDecimal(decimal128 * arg,
					 int64_t * ret,
					 InterpreterContext * ctxt) 
{
  decNumber a;
  decimal128ToNumber(arg, &a);
  char buf[100];
  decNumber one;
  decNumberFromInt32(&one, 1);
  decNumber rounded;
  decNumberQuantize(&rounded, &a, &one, ctxt->getDecimalContext());
  // *ret = decNumberToInt64(&rounded, ctxt->getDecimalContext());
  *ret = decNumberToInt<Int64Traits>(&rounded, ctxt->getDecimalContext());
}

extern "C" void InternalInt64FromDate(boost::gregorian::date arg,
				      int64_t * ret,
				      InterpreterContext * ctxt) 
{
  int32_t tmp;
  InternalInt32FromDate(arg, &tmp, ctxt);
  *ret = tmp;
}

extern "C" void InternalInt64FromDatetime(boost::posix_time::ptime arg,
					  int64_t * ret,
					  InterpreterContext * ctxt) 
{
  boost::gregorian::greg_year_month_day ymd = arg.date().year_month_day();
  boost::posix_time::time_duration td = arg.time_of_day();
  *ret = ymd.year*10000000000LL + ymd.month*100000000LL + ymd.day*1000000LL +
    td.hours()*10000 + td.minutes()*100 + td.seconds();
}

extern "C" void InternalDoubleFromVarchar(const Varchar * arg,
					  double * ret,
					  InterpreterContext * ctxt) 
{
  *ret = atof(arg->c_str());
}

extern "C" void InternalDoubleFromChar(const char * arg,
				       double * ret,
				       InterpreterContext * ctxt) 
{
  *ret = atof(arg);
}

extern "C" void InternalDoubleFromDecimal(decimal128 * arg,
					  double * ret,
					  InterpreterContext * ctxt) 
{
  char buf[DECIMAL128_String];  
  decimal128ToString(arg, &buf[0]);
  *ret = atof(buf);
  // decNumber a;
  // decimal128ToNumber(arg, &a);
  // *ret = decNumberToDouble(&a, ctxt->getDecimalContext());
}

extern "C" void InternalDoubleFromDate(boost::gregorian::date arg,
				       double * ret,
				       InterpreterContext * ctxt) 
{
  int32_t tmp;
  InternalInt32FromDate(arg, &tmp, ctxt);
  *ret = tmp;
}

extern "C" void InternalDoubleFromDatetime(boost::posix_time::ptime arg,
					   double * ret,
					   InterpreterContext * ctxt) 
{
  int64_t tmp;
  InternalInt64FromDatetime(arg, &tmp, ctxt);
  *ret = tmp;
}

extern "C" boost::gregorian::date InternalDateFromVarchar(Varchar* arg) {
  return boost::gregorian::from_string(arg->c_str());
}

extern "C" boost::gregorian::date date(boost::posix_time::ptime t) {
  return t.date();
}
/**
 * ODBC standard dayofweek with Sunday=1
 */
extern "C" int32_t dayofweek(boost::gregorian::date t) {
  return 1+t.day_of_week();
}

extern "C" int32_t dayofmonth(boost::gregorian::date t) {
  return t.day();
}

extern "C" int32_t dayofyear(boost::gregorian::date t) {
  return t.day_of_year();
}

extern "C" int32_t month(boost::gregorian::date t) {
  return t.month();
}

extern "C" int32_t year(boost::gregorian::date t) {
  return t.year();
}

extern "C" boost::gregorian::date last_day(boost::gregorian::date t) {
  return t.end_of_month();
}

extern "C" int32_t datediff(boost::gregorian::date a, boost::gregorian::date b) {
  return (int32_t) (a-b).days();
}

extern "C" int32_t julian_day(boost::gregorian::date a) {
  return (int32_t) a.julian_day();
}

extern "C" boost::posix_time::ptime InternalDatetimeFromVarchar(Varchar* lhs) {
  unsigned sz;
  const char * ptr;
  getVarcharRange(lhs, ptr, sz);
  return sz == 10 ?
    boost::posix_time::ptime(boost::gregorian::from_string(ptr)) :
    boost::posix_time::time_from_string(ptr);
}

extern "C" boost::posix_time::ptime InternalDatetimeFromDate(boost::gregorian::date lhs) {
  return boost::posix_time::ptime(lhs, boost::posix_time::time_duration(0,0,0,0));
}

extern "C" boost::posix_time::ptime utc_timestamp() {
  return boost::posix_time::microsec_clock::universal_time();
}

extern "C" int64_t unix_timestamp(boost::posix_time::ptime t)
{
  // // TODO: Should make a way of setting the ambient time zone
  // typedef boost::date_time::local_adjustor<boost::posix_time::ptime, -5, 
  //   boost::posix_time::us_dst> us_eastern;
  boost::posix_time::ptime unixZero = boost::posix_time::from_time_t(0); 
  // boost::posix_time::ptime utcTime = us_eastern::local_to_utc(t);
  return (t - unixZero).total_seconds();
  // return (t - unixZero).total_seconds();
}

extern "C" boost::posix_time::ptime from_unixtime(int64_t unixTime)
{
  // TODO: Should make a way of setting the ambient time zone
  // typedef boost::date_time::local_adjustor<boost::posix_time::ptime, -5, 
  //   boost::posix_time::us_dst> us_eastern;
  boost::posix_time::ptime utcTime = boost::posix_time::from_time_t(unixTime);
  // return us_eastern::utc_to_local(utcTime);
  return utcTime;
}

extern "C" boost::posix_time::ptime datetime_add_second(boost::posix_time::ptime t,
						    int32_t units) {
  return t + boost::posix_time::seconds(units);
}

extern "C" boost::posix_time::ptime datetime_add_hour(boost::posix_time::ptime t,
						    int32_t units) {
  return t + boost::posix_time::hours(units);
}

extern "C" boost::posix_time::ptime datetime_add_minute(boost::posix_time::ptime t,
						    int32_t units) {
  return t + boost::posix_time::minutes(units);
}

extern "C" boost::posix_time::ptime datetime_add_day(boost::posix_time::ptime t,
						 int32_t units) {
  return t + boost::gregorian::days(units);
}

extern "C" boost::posix_time::ptime datetime_add_month(boost::posix_time::ptime t,
						 int32_t units) {
  return t + boost::gregorian::months(units);
}

extern "C" boost::posix_time::ptime datetime_add_year(boost::posix_time::ptime t,
						 int32_t units) {
  return t + boost::gregorian::years(units);
}

extern "C" boost::posix_time::ptime date_add_second(boost::gregorian::date t,
						    int32_t units) {
  return boost::posix_time::ptime(t, boost::posix_time::seconds(units));
}

extern "C" boost::posix_time::ptime date_add_hour(boost::gregorian::date t,
						  int32_t units) {
  return boost::posix_time::ptime(t, boost::posix_time::hours(units));
}

extern "C" boost::posix_time::ptime date_add_minute(boost::gregorian::date t,
						    int32_t units) {
  return boost::posix_time::ptime(t, boost::posix_time::minutes(units));
}

extern "C" boost::gregorian::date date_add_day(boost::gregorian::date t,
					       int32_t units) {
  return t + boost::gregorian::days(units);
}

extern "C" boost::gregorian::date date_add_month(boost::gregorian::date t,
						 int32_t units) {
  return t + boost::gregorian::months(units);
}

extern "C" boost::gregorian::date date_add_year(boost::gregorian::date t,
						int32_t units) {
  return t + boost::gregorian::years(units);
}

extern "C" void InternalIPAddress(const char * lhs, 
				  Varchar * result,
				  InterpreterContext * ctxt) {
  InternalVarcharFromIPv6(lhs, result, ctxt);
}

extern "C" void InternalParseIPAddress(const Varchar * lhs,
				       char * result,
				       InterpreterContext * ctxt) {
  struct in_addr in4;
  int ret = ::inet_pton(AF_INET, lhs->c_str(), &in4);
  if (ret == 1) {
    memcpy(&result[0], &v4MappedPrefix[0], 12);
    memcpy(&result[12], &in4, 4);
  } else {
    ret = ::inet_pton(AF_INET6, lhs->c_str(), result);
    if (1 != ret) {
      memset(result, 0, 16);
    }
  }
}

extern "C" void InternalTruncateIPAddress(const char * ip, 
					  int32_t prefix_length,
					  char * result,
					  InterpreterContext * ctxt) {
  memcpy(result, ip, 16);
  // Early bytes can stay as they are  
  int i = prefix_length / 8;
  if (prefix_length % 8 != 0) {
    // Middle partial byte has to be dealt with
    result[i] = result[i] & 0xFF << (8 - (prefix_length % 8));
    i++;
  }
  
  // Later bytes get zeroed
  if (i < 16) memset(&result[i], 0, 16-i);
}

extern "C" int32_t InternalIPAddressAddrBlockMatch(const CidrV6Runtime * lhs,
						   const CidrV6Runtime * rhs) {
  char zeroed_rhs[16];
  char zeroed_lhs[16];
  if (lhs->prefix_length >= rhs->prefix_length) {
    return 0;
  }
  InternalTruncateIPAddress((const char *) &lhs->prefix[0], lhs->prefix_length, zeroed_lhs, nullptr);
  InternalTruncateIPAddress((const char *) &rhs->prefix[0], lhs->prefix_length, zeroed_rhs, nullptr);
  return 0 == memcmp(zeroed_lhs, zeroed_rhs, 16);
}

extern "C" int32_t InternalIsV4IPAddress(const char * prefix_ip) {
  return memcmp(prefix_ip, v4MappedPrefix, sizeof(v4MappedPrefix)) == 0;
}

extern "C" void InternalArrayException() {
  throw std::runtime_error("Array Bounds Exception");
}

template <class _T>
class ANTLR3AutoPtr : boost::noncopyable {
private:
  _T * mPtr;

public:
  ANTLR3AutoPtr(_T * ptr)
    :
    mPtr(ptr)
  {
  }

  ~ANTLR3AutoPtr() {
    if (mPtr != NULL)
      mPtr->free(mPtr);
    mPtr = NULL;
  }

  operator bool () const {
    return mPtr != NULL;
  }

  _T * get() {
    return mPtr;
  }

  _T * operator-> () {
    return mPtr;
  }
};

LLVMBase::LLVMBase()
  :
  mContext(NULL)
{
  // Compile into an LLVM program
  mContext = new CodeGenerationContext();

}

LLVMBase::~LLVMBase()
{
  if (mContext) {
    delete mContext;
    mContext = NULL;
  }

}

void LLVMBase::InitializeLLVM()
{
}

class IQLRecordBufferMethodHandle 
{
private:
  std::unique_ptr<OrcJit> mJIT;

  void initialize(std::unique_ptr<llvm::Module> module, 
		  std::string & objectFile);
public:
  IQLRecordBufferMethodHandle(std::unique_ptr<llvm::Module> module, 
			      std::string & objectFile);
  IQLRecordBufferMethodHandle(const std::string& objectFile);
  ~IQLRecordBufferMethodHandle();
  template<typename T>
  T getFunPtr(const std::string& nm)
  {
    // TODO: Prepend __????
    auto ExprSymbol = mJIT->findSymbol(nm);

    // return reinterpret_cast<void *>(llvm::cantFail(ExprSymbol.getAddress()));
    llvm::ExitOnError ExitOnErr;
    return ExitOnErr(std::move(ExprSymbol)).toPtr<T>();
  }

  static void onObjectLoaded(llvm::orc::MaterializationResponsibility & mr, const llvm::object::ObjectFile &obj,
			     const llvm::RuntimeDyld::LoadedObjectInfo & objInfo,
			     std::string & objectFile)
  {
    objectFile.assign(obj.getMemoryBufferRef().getBufferStart(), obj.getMemoryBufferRef().getBufferEnd());
  }
  void onObjectFinalized(llvm::orc::MaterializationResponsibility & mr, const llvm::object::ObjectFile &obj,
			 const llvm::RuntimeDyld::LoadedObjectInfo & objInfo)
  {
  }
};

IQLRecordBufferMethodHandle::IQLRecordBufferMethodHandle(std::unique_ptr<llvm::Module> module, 
							 std::string & objectFile)
{
  initialize(std::move(module), objectFile);
}

IQLRecordBufferMethodHandle::IQLRecordBufferMethodHandle(const std::string& objectFile)
{
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
  // Can't create the JIT until after LLVM is initialized
  mJIT = std::make_unique<OrcJit>([](llvm::orc::MaterializationResponsibility & mr, const llvm::object::ObjectFile &obj,
					 const llvm::RuntimeDyld::LoadedObjectInfo & objInfo) {
				  },
				  [](llvm::orc::MaterializationResponsibility & mr, const llvm::object::ObjectFile &obj,
					 const llvm::RuntimeDyld::LoadedObjectInfo & objInfo) {
				  });

  if (!objectFile.empty()) {
    llvm::cantFail(mJIT->addObject(llvm::MemoryBuffer::getMemBuffer(objectFile)));
  }
}

IQLRecordBufferMethodHandle::~IQLRecordBufferMethodHandle()
{  
}

void IQLRecordBufferMethodHandle::initialize(std::unique_ptr<llvm::Module> module,
					     std::string & objectFile)
{
  std::unique_ptr<llvm::LLVMContext> context(new llvm::LLVMContext());
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  // Can't create the JIT until after LLVM is initialized
  mJIT = std::make_unique<OrcJit>([&objectFile](llvm::orc::MaterializationResponsibility & mr,
                                                const llvm::object::ObjectFile &obj,
                                                const llvm::RuntimeDyld::LoadedObjectInfo & objInfo) {
				    IQLRecordBufferMethodHandle::onObjectLoaded(mr, obj, objInfo, objectFile);
				  },
				  [this](llvm::orc::MaterializationResponsibility & mr,
                                         const llvm::object::ObjectFile &obj,
					 const llvm::RuntimeDyld::LoadedObjectInfo & objInfo) {
				    this->onObjectFinalized(mr, obj, objInfo);
				  });
  llvm::cantFail(mJIT->addModule(llvm::orc::ThreadSafeModule(std::move(module), std::move(context))));
}

class IQLParserStuff
{
private:
  pANTLR3_INPUT_STREAM mInput;
  pANTLR3_COMMON_TOKEN_STREAM mStream;
  pIQLLexer mLexer;
  pIQLParser mParser;
  pANTLR3_COMMON_TREE_NODE_STREAM mNodes;
  IQLExpression * mNativeAST;

  void initParse(const std::string& transfer);
  void cleanup();
public:
  IQLParserStuff();
  ~IQLParserStuff();
  void parseFunction(const std::string& transfer);
  void parseTransfer(const std::string& transfer);
  void parseUpdate(const std::string& transfer);
  void getFreeVariables(std::set<std::string>& freeVariables);
  const RecordType * typeCheckTransfer(const TypeCheckConfiguration & typeCheckConfig,
				       DynamicRecordContext & recCtxt,
				       const RecordType * source);
  const RecordType * typeCheckTransfer(const TypeCheckConfiguration & typeCheckConfig,
				       DynamicRecordContext & recCtxt,
				       const std::vector<AliasedRecordType>& sources);
  const RecordType * typeCheckTransfer(const TypeCheckConfiguration & typeCheckConfig,
				       DynamicRecordContext & recCtxt,
				       const std::vector<AliasedRecordType>& sources,
				       const std::vector<boost::dynamic_bitset<> >& masks);
  void typeCheckUpdate(const TypeCheckConfiguration & typeCheckConfig,
		       DynamicRecordContext & recCtxt,
		       const std::vector<const RecordType *>& sources,
		       const std::vector<boost::dynamic_bitset<> >& masks);
  pANTLR3_COMMON_TREE_NODE_STREAM getNodes() { return mNodes; }
  pANTLR3_BASE_TREE getAST() { return mNodes->root; }
  IQLRecordConstructor * generateTransferAST(DynamicRecordContext & recCtxt);
  IQLExpression * generateFunctionAST(DynamicRecordContext & recCtxt);
};

IQLParserStuff::IQLParserStuff()
  :
  mInput(NULL),
  mStream(NULL),
  mLexer(NULL),
  mParser(NULL),
  mNodes(NULL),
  mNativeAST(NULL)
{
}

IQLParserStuff::~IQLParserStuff()
{
  cleanup();
}

void IQLParserStuff::cleanup()
{
  if(mInput)
    mInput->free(mInput);
  if(mLexer)
    mLexer->free(mLexer);
  if(mStream)
    mStream->free(mStream);
  if(mParser)
    mParser->free(mParser);
  if(mNodes)
    mNodes->free(mNodes);
  mInput = NULL;
  mStream = NULL;
  mLexer = NULL;
  mParser = NULL;
  mNodes = NULL;
}
static int streamID=0;

void IQLParserStuff::initParse(const std::string& transfer)
{
  // Erase any state that has accumulated.
  cleanup();
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  mInput = antlr3StringStreamNew((pANTLR3_UINT8) transfer.c_str(),
                                 ANTLR3_ENC_UTF8,
                                 transfer.size(),
                                 (pANTLR3_UINT8) (boost::format("My Program %1%") % (streamID++)).str().c_str());
  if (!mInput)
    throw std::runtime_error("Antlr out of memory");

  // SQL is case insensitive
  mInput->setUcaseLA(mInput, ANTLR3_TRUE);
  
  mLexer = IQLLexerNew(mInput);
  if (!mLexer)
    throw std::runtime_error("Antlr out of memory");

  mStream = antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, TOKENSOURCE(mLexer));
  mParser = IQLParserNew(mStream);
  if (!mParser)
    throw std::runtime_error("Antlr out of memory");  
}

void IQLParserStuff::parseFunction(const std::string& f)
{  
  initParse(f);
  IQLParser_singleExpression_return parserRet = mParser->singleExpression(mParser);
  if (mParser->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % f).str());
  mNodes = antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT);
}

void IQLParserStuff::parseTransfer(const std::string& transfer)
{  
  initParse(transfer);
  IQLParser_recordConstructor_return parserRet = mParser->recordConstructor(mParser);
  if (mParser->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % transfer).str());
  mNodes = antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT);
}

void IQLParserStuff::parseUpdate(const std::string& transfer)
{  
  initParse(transfer);
  IQLParser_statementList_return parserRet = mParser->statementList(mParser);
  if (mParser->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % transfer).str());
  mNodes = antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT);
}

void IQLParserStuff::getFreeVariables(std::set<std::string>& freeVariables)
{
  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  GetVariablesContext getVariablesContext;

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLGetVariables> alz(IQLGetVariablesNew(mNodes));
  alz->recordConstructor(alz.get(), wrap(&getVariablesContext));
  // We should only get a failure here if there is a bug in our
  // tree grammar.  Not much a user can do about this.
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("INTERNAL ERROR: Failed to get free variables.");

  for(std::set<std::string>::const_iterator 
	it = getVariablesContext.getFreeVariables().begin(), 
	end = getVariablesContext.getFreeVariables().end();
      it != end;
      ++it) {
    if (freeVariables.find(*it) == freeVariables.end()) 
      freeVariables.insert(*it);
  }
}

const RecordType * IQLParserStuff::typeCheckTransfer(const TypeCheckConfiguration & typeCheckConfig,
						     DynamicRecordContext & recCtxt,
						     const RecordType * source)
{
  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  std::vector<AliasedRecordType> aliased;
  aliased.push_back(AliasedRecordType("input", source));
  std::vector<boost::dynamic_bitset<> > masks;
  masks.resize(1);
  masks[0].resize(source->size(), true);
  TypeCheckContext typeCheckContext(typeCheckConfig, recCtxt, aliased, masks);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(mNodes));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL)
    throw std::runtime_error("Failed to create output record");

  return typeCheckContext.getOutputRecord();
}

const RecordType * 
IQLParserStuff::typeCheckTransfer(const TypeCheckConfiguration & typeCheckConfig,
				  DynamicRecordContext & recCtxt,
				  const std::vector<AliasedRecordType>& sources)

{
  std::vector<boost::dynamic_bitset<>> masks;
  for(const auto & s : sources) {
    masks.emplace_back(s.getType()->size());
    masks.back().set();
  }
  return typeCheckTransfer(typeCheckConfig, recCtxt, sources, masks);
}

const RecordType * 
IQLParserStuff::typeCheckTransfer(const TypeCheckConfiguration & typeCheckConfig,
				  DynamicRecordContext & recCtxt,
				  const std::vector<AliasedRecordType>& sources,
				  const std::vector<boost::dynamic_bitset<> >& masks)

{
  // Create an appropriate context for type checking.  This requires associating the
  // input records with a name and then inserting all the members of the record type
  // with a symbol table.
  TypeCheckContext typeCheckContext(typeCheckConfig, recCtxt, sources, masks);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(mNodes));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL)
    throw std::runtime_error("Failed to create output record");

  return typeCheckContext.getOutputRecord();
}

void IQLParserStuff::typeCheckUpdate(const TypeCheckConfiguration & typeCheckConfig,
				     DynamicRecordContext & recCtxt,
				     const std::vector<const RecordType *>& sources,
				     const std::vector<boost::dynamic_bitset<> >& masks)
{
  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  std::vector<AliasedRecordType> aliased; 
  for(std::size_t i=0; i<sources.size(); ++i) {
    std::string nm ((boost::format("input%1%") % i).str());
    aliased.push_back(AliasedRecordType(nm, sources[i]));
  } 
  TypeCheckContext typeCheckContext(typeCheckConfig, recCtxt, aliased, masks);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(mNodes));
  alz->statementBlock(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");
}

IQLRecordConstructor * IQLParserStuff::generateTransferAST(DynamicRecordContext & recCtxt)
{
  // Generate native AST.  We'll eventually move all analysis
  // to this.
  ANTLR3AutoPtr<IQLAnalyze> nativeASTGenerator(IQLAnalyzeNew(mNodes));
  IQLRecordConstructor * nativeAST = unwrap(nativeASTGenerator->recordConstructor(nativeASTGenerator.get(), 
							   wrap(&recCtxt)));
  if (nativeASTGenerator->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("AST generation failed");
  return nativeAST;
}

IQLExpression * IQLParserStuff::generateFunctionAST(DynamicRecordContext & recCtxt)
{
  // Generate native AST.  We'll eventually move all analysis
  // to this.
  ANTLR3AutoPtr<IQLAnalyze> nativeASTGenerator(IQLAnalyzeNew(mNodes));
  IQLExpression * nativeAST = unwrap(nativeASTGenerator->singleExpression(nativeASTGenerator.get(), 
							   wrap(&recCtxt)));
  if (nativeASTGenerator->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("AST generation failed");
  return nativeAST;
}

TreculRecordOperations::TreculRecordOperations(CodeGenerationContext & codeGen,
                                               const RecordType * recordType,
                                               const std::string& funName,
                                               std::function<void()> op)
  :
  mFunName(funName)
{
  // Create the LLVM function and populate variables in
  // symbol table to prepare for code gen.
  codeGen.createRecordTypeOperation(recordType, mFunName);

  op();
  
  codeGen.LLVMBuilder->CreateRetVoid();

  llvm::verifyFunction(*codeGen.LLVMFunction);
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *mContext->LLVMModule;
  // // Now run optimizer over the IR
  codeGen.mFPM->run(*codeGen.LLVMFunction);
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *mContext->LLVMModule;
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();
}

TreculRecordOperations::~TreculRecordOperations()
{
}

TreculFreeOperation::TreculFreeOperation(CodeGenerationContext & codeGen, const RecordType * recordType)
  :
  TreculRecordOperations(codeGen, recordType, "RecordTypeFree", [&codeGen, recordType]() { codeGen.buildRecordTypeFree(recordType); })
{
}

TreculPrintOperation::TreculPrintOperation(CodeGenerationContext & codeGen, const RecordType * recordType)
  :
  TreculRecordOperations(codeGen, recordType, "RecordTypePrint", [&codeGen, recordType]() { codeGen.buildRecordTypePrint(recordType, '\t', '\n', '\\'); })
{
}

void TreculRecordPrintRuntime::imbue(std::ostream& ostr)
{
  // stream takes ownership of the facet.
  boost::posix_time::time_facet * facet =
    new boost::posix_time::time_facet("%Y-%m-%d %H:%M:%S");
  boost::gregorian::date_facet * dateFacet =
    new boost::gregorian::date_facet("%Y-%m-%d");
  ostr.imbue(std::locale(std::locale(ostr.getloc(), facet), dateFacet));
  ostr << std::fixed << std::setprecision(9);
}

void TreculTransferRuntime::execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mMalloc.malloc();
  if (isSourceMove) {
    (*mMoveFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);  
  } else {
    (*mCopyFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);  
  }
  ctxt->clear();
}

void TreculAggregateRuntime::executeInit(RecordBuffer & source, 
				     RecordBuffer & target, 
				     class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mAggregateMalloc.malloc();
  (*mInitFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);      
  ctxt->clear();
}

void TreculAggregateRuntime::executeUpdate(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt) const
{
  (*mUpdateFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);      
  ctxt->clear();
}

void TreculAggregateRuntime::executeTransfer(RecordBuffer & source, 
					 RecordBuffer & target, 
					 class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mTransferMalloc.malloc();
  (*mTransferFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);      
  ctxt->clear();
}

void TreculAggregateRuntime::executeTransfer(RecordBuffer & source1, 
					 RecordBuffer & source2, 
					 RecordBuffer & target, 
					 class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mTransferMalloc.malloc();  
  (*((LLVMFuncType2) mTransferFunction))((char *) source1.Ptr, 
					 (char *) source2.Ptr, 
					 (char *) target.Ptr, ctxt);      
  ctxt->clear();
}

IQLRecordConstructor * TreculTransfer::getAST(class DynamicRecordContext& recCtxt,
                                              const std::string& xfer)
{
  IQLParserStuff p;
  p.parseTransfer(xfer);
  return p.generateTransferAST(recCtxt);
}

void TreculTransfer::getFreeVariables(const std::string& xfer,
                                      std::set<std::string>& freeVariables)
{
  IQLParserStuff p;
  p.parseTransfer(xfer);
  p.getFreeVariables(freeVariables);
}

TreculTransfer::TreculTransfer(DynamicRecordContext& recCtxt,
                               CodeGenerationContext & codeGen,
                               const std::string & funName,
                               const RecordType * source,
                               const std::string& transfer)
  :
  mSource(source),
  mTarget(NULL),
  mFunName(funName),
  mCopyFunName(mFunName + "&copy"),
  mMoveFunName(mFunName + "&move"),
  mTransfer(transfer),
  mIsIdentity(false)
{
  IQLParserStuff p;
  const TypeCheckConfiguration & typeCheckConfig(TypeCheckConfiguration::get());
  p.parseTransfer(transfer);
  mTarget = p.typeCheckTransfer(typeCheckConfig, recCtxt, source);

  // Create two variants of the function: one for move and one for copy.
  // They have the same signature.
  for(int i=0; i<2; i++) {
    // Clean up the symbol table from the last code gen
    codeGen.reinitializeForTransfer(typeCheckConfig);
    codeGen.createTransferFunction(mSource, mTarget, i ? mMoveFunName : mCopyFunName);
    // Set state about whether we want move or copy semantics
    codeGen.IQLMoveSemantics = i;
    // Code generate
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(&codeGen));
    codeGen.LLVMBuilder->CreateRetVoid();
    
    // If doing copy find out if this was an identity transfer
    if (0 == i)
      mIsIdentity = 1==codeGen.IsIdentity;

    llvm::verifyFunction(*codeGen.LLVMFunction);
    // llvm::outs() << "We just constructed this LLVM module:\n\n" << *codeGen.LLVMModule;
    // // Now run optimizer over the IR
    codeGen.mFPM->run(*codeGen.LLVMFunction);
    // llvm::outs() << "We just optimized this LLVM module:\n\n" << *codeGen.LLVMModule;
    // llvm::outs() << "\n\nRunning foo: ";
    // llvm::outs().flush();
  }
}

TreculTransfer::~TreculTransfer()
{
}

void TreculTransfer2Runtime::execute(RecordBuffer * sources, 
                                     bool * isSourceMove, 
                                     int32_t numSources,
                                     RecordBuffer & target, 
                                     class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mMalloc.malloc();
  if (numSources != 2) 
    throw std::runtime_error("IQLTransferModule2::execute : Number of sources must be 2");
  // TODO: Handle move semantics
  BOOST_ASSERT(!isSourceMove[0] && !isSourceMove[1]);
  // TODO: Handle arbitrary number of inputs.
  (*mCopyFunction)((char *) sources[0].Ptr, (char *) sources[1].Ptr, (char *) target.Ptr, ctxt);  
  ctxt->clear();  
}

TreculTransfer2::TreculTransfer2(DynamicRecordContext& recCtxt, 
                                 CodeGenerationContext & codeGen,
                                 const std::string & funName, 
                                 const std::vector<AliasedRecordType>& sources, 
                                 const std::string& transfer)
  :
  mSources(sources),
  mTarget(NULL),
  mFunName(funName),
  mCopyFunName(mFunName + "&copy"),
  mMoveFunName(mFunName + "&move"),
  mTransfer(transfer)
{
  IQLParserStuff p;
  const TypeCheckConfiguration & typeCheckConfig(TypeCheckConfiguration::get());
  p.parseTransfer(transfer);
  mTarget = p.typeCheckTransfer(typeCheckConfig, recCtxt, mSources);

  // Create two variants of the function: one for move and one for copy.
  // They have the same signature.
  for(int i=0; i<2; i++) {
    // Clean up the symbol table from the last code gen
    codeGen.reinitializeForTransfer(typeCheckConfig);
    codeGen.createTransferFunction(mSources, mTarget, i ? mMoveFunName : mCopyFunName);
    // Set state about whether we want move or copy semantics
    codeGen.IQLMoveSemantics = i;
    // Code generate
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(&codeGen));
    codeGen.LLVMBuilder->CreateRetVoid();
    
    llvm::verifyFunction(*codeGen.LLVMFunction);
    // llvm::outs() << "We just constructed this LLVM module:\n\n" << *codeGen.LLVMModule;
    // // Now run optimizer over the IR
    codeGen.mFPM->run(*codeGen.LLVMFunction);
    // llvm::outs() << "We just optimized this LLVM module:\n\n" << *codeGen.LLVMModule;
    // llvm::outs() << "\n\nRunning foo: ";
    // llvm::outs().flush();
  }
}

TreculTransfer2::~TreculTransfer2()
{
}

RecordTypeTransfer::RecordTypeTransfer(DynamicRecordContext& recCtxt, const std::string & funName, const RecordType * source, const std::string& transfer)
  :
  LLVMBase(),
  TreculTransfer(recCtxt, *mContext, funName, source, transfer)
{
}

RecordTypeTransfer::~RecordTypeTransfer()
{
}

void RecordTypeTransfer::execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove)
{
  if (!mModule) {
    mModule.reset(create());
  }
  mModule->execute(source, target, ctxt, isSourceMove);
}

IQLTransferModule * RecordTypeTransfer::create() const
{
  return new IQLTransferModule(getTarget()->getMalloc(), getCopyFunName(), getMoveFunName(), llvm::CloneModule(*mContext->LLVMModule));
}

IQLTransferModule::IQLTransferModule(const RecordTypeMalloc& recordMalloc,
				     const std::string& copyFunName, 
				     const std::string& moveFunName, 
				     std::unique_ptr<llvm::Module> module)
  :
  mFunName(copyFunName, moveFunName, recordMalloc),
  mModule(std::move(module)),
  mFunction(mModule.getTransfer<TreculTransferRuntime>(mFunName))
{
}

IQLTransferModule::~IQLTransferModule()
{
}

void IQLTransferModule::execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove) const
{
  mFunction.execute(source, target, ctxt, isSourceMove);
}

RecordTypeTransfer2::RecordTypeTransfer2(DynamicRecordContext& recCtxt, 
					 const std::string & funName, 
					 const std::vector<AliasedRecordType>& sources, 
					 const std::string& transfer)
  :
  LLVMBase(),
  TreculTransfer2(recCtxt, *mContext, funName, sources, transfer)
{
}

RecordTypeTransfer2::~RecordTypeTransfer2()
{
}

void RecordTypeTransfer2::execute(RecordBuffer * sources, 
				  bool * isSourceMove, 
				  int32_t numSources,
				  RecordBuffer & target, 
				  class InterpreterContext * ctxt)
{
  if(!mModule) {
    mModule.reset(create());
  }
  mModule->execute(sources, isSourceMove, numSources, target, ctxt);
}

IQLTransferModule2 * RecordTypeTransfer2::create() const
{
  return new IQLTransferModule2(getTarget()->getMalloc(), getCopyFunName(), getMoveFunName(), llvm::CloneModule(*mContext->LLVMModule));
}

IQLTransferModule2::IQLTransferModule2(const RecordTypeMalloc& recordMalloc,
                                       const std::string& copyFunName, 
                                       const std::string& moveFunName, 
                                       std::unique_ptr<llvm::Module> module)
  :
  mFunName(copyFunName, moveFunName, recordMalloc),
  mModule(std::move(module)),
  mFunction(mModule.getTransfer<TreculTransfer2Runtime>(mFunName))
{
}

IQLTransferModule2::~IQLTransferModule2()
{
}

void IQLTransferModule2::execute(RecordBuffer * sources, 
				  bool * isSourceMove, 
				  int32_t numSources,
				  RecordBuffer & target, 
				  class InterpreterContext * ctxt) const
{
  mFunction.execute(sources, isSourceMove, numSources, target, ctxt);
}

IQLUpdateModule::IQLUpdateModule(const std::string& funName, 
				 std::unique_ptr<llvm::Module> module)
  :
  mFunName(funName),
  mModule(std::move(module)),
  mFunction(mModule.getFunction<TreculUpdateRuntime>(mFunName))
{
}

IQLUpdateModule::~IQLUpdateModule()
{
}

void IQLUpdateModule::execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const
{
  mFunction.execute(source, target, ctxt);
}

RecordTypeInPlaceUpdate::RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
						 const std::string & funName, 
						 const std::vector<const RecordType *>& sources, 
						 const std::string& statements)
  :
  LLVMBase(),
  TreculInPlaceUpdate(recCtxt, *mContext, funName, sources, statements)
{
}

RecordTypeInPlaceUpdate::RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
						 const std::string & funName, 
						 const std::vector<const RecordType *>& sources, 
						 const std::vector<boost::dynamic_bitset<> >& masks,
						 const std::string& statements)
  :
  LLVMBase(),
  TreculInPlaceUpdate(recCtxt, *mContext, funName, sources, masks, statements)
{
}

RecordTypeInPlaceUpdate::~RecordTypeInPlaceUpdate()
{
}

void RecordTypeInPlaceUpdate::execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt)
{
  if (!mModule) {
    mModule.reset(create());
  }
  mModule->execute(source, target, ctxt);
}

IQLUpdateModule * RecordTypeInPlaceUpdate::create() const
{
  return new IQLUpdateModule(getFunName(), llvm::CloneModule(*mContext->LLVMModule));
}

RecordTypeOperations::RecordTypeOperations(const RecordType * recordType, const std::string& funName, std::function<void()> op)
  :
  mFunName(funName)
{
  init(recordType, op);
}

RecordTypeOperations::~RecordTypeOperations()
{
}

void RecordTypeOperations::init(const RecordType * recordType, std::function<void()> op)
{
  InitializeLLVM();

  // Create the LLVM function and populate variables in
  // symbol table to prepare for code gen.
  mContext->createRecordTypeOperation(recordType, mFunName);

  op();
  
  mContext->LLVMBuilder->CreateRetVoid();

  llvm::verifyFunction(*mContext->LLVMFunction);
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *mContext->LLVMModule;
  // // Now run optimizer over the IR
  mContext->mFPM->run(*mContext->LLVMFunction);
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *mContext->LLVMModule;
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();
}

IQLRecordTypeOperationModule * RecordTypeOperations::create() const
{
  return new IQLRecordTypeOperationModule(mFunName, llvm::CloneModule(*mContext->LLVMModule));
}

RecordTypeFreeOperation::RecordTypeFreeOperation(const RecordType * recordType)
  :
  RecordTypeOperations(recordType, "RecordTypeFree", [this, recordType]() { this->mContext->buildRecordTypeFree(recordType); })
{
}

RecordTypePrintOperation::RecordTypePrintOperation(const RecordType * recordType)
  :
  RecordTypeOperations(recordType, "RecordTypePrint", [this, recordType]() { this->mContext->buildRecordTypePrint(recordType, '\t', '\n', '\\'); })
{
}

IQLRecordTypeOperationModule::IQLRecordTypeOperationModule(const std::string& funName, 
                                                           std::unique_ptr<llvm::Module> module)
  :
  mFunName(funName),
  mFunction(NULL),
  mImpl(NULL)
{
  initImpl(std::move(module));
}

IQLRecordTypeOperationModule::~IQLRecordTypeOperationModule()
{
  if (mImpl) {
    delete mImpl;
  }
}

void IQLRecordTypeOperationModule::initImpl(std::unique_ptr<llvm::Module> module)
{
  mImpl = new IQLRecordBufferMethodHandle(std::move(module), mObjectFile);
  mFunction = mImpl->getFunPtr<LLVMFuncType>(mFunName);
}

void IQLRecordTypeOperationModule::initImpl()
{
  mImpl = new IQLRecordBufferMethodHandle(mObjectFile);
  mFunction = mImpl->getFunPtr<LLVMFuncType>(mFunName);
}

IQLRecordTypeOperationModule & IQLRecordTypeOperationModule::operator=(const IQLRecordTypeOperationModule & rhs)
{
  if (mImpl != nullptr) {
    delete mImpl;
  }
  mFunName = rhs.mFunName;
  mObjectFile = rhs.mObjectFile;
  initImpl();
  return *this;
}

void IQLRecordTypeOperationModule::execute(RecordBuffer & source) const
{
  (*mFunction)((char *) source.Ptr, nullptr, 0);
}

void IQLRecordTypeOperationModule::execute(RecordBuffer & source, std::ostream & ostr, bool outputRecordDelimiter) const
{
  (*mFunction)((char *) source.Ptr, (char *)&ostr, outputRecordDelimiter);
}

IQLFunctionModule::IQLFunctionModule(const std::string& funName, 
				     std::unique_ptr<llvm::Module> module)
  :
  mFunName(funName),
  mModule(std::move(module)),
  mFunction(mModule.getFunction<TreculFunctionRuntime>(mFunName))
{
}

IQLFunctionModule::~IQLFunctionModule()
{
}

int32_t IQLFunctionModule::execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt) const
{
  return mFunction.execute(sourceA, sourceB, ctxt);
}

TreculAggregate::TreculAggregate(DynamicRecordContext& recCtxt, 
            class CodeGenerationContext & codeGen,
					 const std::string & funName, 
					 const RecordType * source, 
					 const std::string& transfer,
					 const std::vector<std::string>& groupKeys,
					 bool isOlap)
  :
  mSource(source),
  mAggregate(NULL),
  mTarget(NULL),
  mIsIdentity(false)
{
  mInitializeFun = funName + "$init";
  mUpdateFun = funName + "$update";
  mTransferFun = funName + "$transfer";

  IQLParserStuff p;
  p.parseTransfer(transfer);

  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  const TypeCheckConfiguration & typeCheckConfig(TypeCheckConfiguration::get());
  TypeCheckContext typeCheckContext(typeCheckConfig, recCtxt, mSource, groupKeys, isOlap);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(p.getNodes()));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL ||
      typeCheckContext.getAggregateRecord() == NULL)
    throw std::runtime_error("Failed to create output record");
  
 mTarget = typeCheckContext.getOutputRecord();
 mAggregate = typeCheckContext.getAggregateRecord();

 // Create a valid code generation context based on the input and output record formats.

 // Create update function on top of source
 // and aggregate. Mask out group by keys in
 // aggregate to avoid conflicts.
 codeGen.reinitialize();
 createUpdateFunction(codeGen, groupKeys);

 // Save stuff into Update specific variables.
 codeGen.saveAggregateContext(&codeGen.Update);

 // Reinitialize and create initializer.
 codeGen.createFunctionContext(typeCheckConfig);
 codeGen.createTransferFunction(mSource, mAggregate, mInitializeFun);

 // Generate code to initialize group by keys (nothing
 // about this exists in the aggregate functions we have).
 for(std::vector<std::string>::const_iterator it = groupKeys.begin();
     it != groupKeys.end();
     ++it) {
   codeGen.buildSetAggregateField(codeGen.buildVariableRef(it->c_str(), mSource->getMember(*it).GetType()));

 }
 // We know that aggregate initialization isn't
 // identity.  Reset the flag so we can find out
 // about the top level transfer.
 codeGen.IsIdentity = 1;

 // Save stuff into Initialize specific variables.
 codeGen.saveAggregateContext(&codeGen.Initialize);

 // Reinitialize and create transfer
 codeGen.createFunctionContext(typeCheckConfig);
 if (!isOlap) {
   codeGen.createTransferFunction(mAggregate, mTarget, mTransferFun);
 } else {
   // In the OLAP case, we have a Transfer2 going on in which
   // we have the source record and the aggregate to transfer 
   // from.  Mask out the group keys from aggregate record to
   // avoid name conflicts.
   // TODO: In the sort running total case we don't need
   // group keys in the aggregate record.  In the hash case we
   // do (so we can put the aggregate record into a hash table).
   std::vector<const RecordType *> updateSources;
   updateSources.push_back(mSource);
   updateSources.push_back(mAggregate);
   std::vector<boost::dynamic_bitset<> > masks(2);
   masks[0].resize(updateSources[0]->size(), true);
   masks[1].resize(updateSources[1]->size(), true);
   for(std::size_t i=0; i<groupKeys.size(); i++) {
     masks[1].set(i, false);
   }
   codeGen.createTransferFunction(updateSources, masks, mTarget, mTransferFun);
   codeGen.IQLMoveSemantics = 0;
 }
 codeGen.saveAggregateContext(&codeGen.Transfer);

 // Code generate
 ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
 toLLVM->recordConstructor(toLLVM.get(), wrap(&codeGen));

 // Complete all builders
 codeGen.Update.Builder->CreateRetVoid();
 codeGen.Initialize.Builder->CreateRetVoid();
 codeGen.Transfer.Builder->CreateRetVoid();
    
 mIsIdentity = 1==codeGen.IsIdentity;

 llvm::verifyFunction(*codeGen.Update.Function);
 llvm::verifyFunction(*codeGen.Initialize.Function);
 llvm::verifyFunction(*codeGen.Transfer.Function);

 // // Now run optimizer over the IR
 codeGen.mFPM->run(*codeGen.Update.Function);
 codeGen.mFPM->run(*codeGen.Initialize.Function);
 codeGen.mFPM->run(*codeGen.Transfer.Function);
}

TreculAggregate::TreculAggregate(class DynamicRecordContext& recCtxt, 
            class CodeGenerationContext & codeGen,
					 const std::string & funName, 
					 const RecordType * source, 
					 const std::string& initializer,
					 const std::string& update,
					 const std::vector<std::string>& groupKeys,
					 bool isOlap)
  :
  mSource(source),
  mAggregate(NULL),
  mTarget(NULL),
  mIsIdentity(false)
{
  init(recCtxt, codeGen, funName, source, initializer, update, groupKeys, isOlap);
}

TreculAggregate::~TreculAggregate()
{
}

void TreculAggregate::init(class DynamicRecordContext& recCtxt, 
            class CodeGenerationContext & codeGen,
			       const std::string & funName, 
			       const RecordType * source, 
			       const std::string& initializer,
			       const std::string& update,
			       const std::vector<std::string>& groupKeys,
			       bool isOlap)
{
  mInitializeFun = funName + "$init";
  mUpdateFun = funName + "$update";
  mTransferFun = funName + "$transfer";

  // First parse all into AST
  IQLParserStuff initParser;
  initParser.parseTransfer(initializer);
  mAggregate = initParser.typeCheckTransfer(TypeCheckConfiguration::get(), recCtxt, source);
  // TODO: Add type checking phase to update
  IQLParserStuff updateParser;
  updateParser.parseUpdate(update);
  std::vector<const RecordType *> updateSources;
  updateSources.push_back(mSource);
  updateSources.push_back(mAggregate);
  std::vector<boost::dynamic_bitset<> > masks(2);
  masks[0].resize(updateSources[0]->size(), true);
  masks[1].resize(updateSources[1]->size(), true);
  for(std::size_t i=0; i<groupKeys.size(); i++) {
    masks[1].set(i, false);
  }
  updateParser.typeCheckUpdate(TypeCheckConfiguration::get(), recCtxt, updateSources, masks);
  //
  // Code gen time
  //
  std::vector<llvm::Function *> funs;

  //
  // Update function
  //

  // Create the LLVM function and populate variables in
  // symbol table to prepare for code gen.
  codeGen.reinitialize();
  createUpdateFunction(codeGen, groupKeys);
  {
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(updateParser.getNodes()));  
    toLLVM->statementBlock(toLLVM.get(), wrap(&codeGen));
    codeGen.LLVMBuilder->CreateRetVoid();
    funs.push_back(codeGen.LLVMFunction);
  }

  // 
  // Init function
  //
  // Reinitialize and create transfer
  codeGen.reinitialize();

  codeGen.createTransferFunction(mSource, mAggregate, mInitializeFun);
  {
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(initParser.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(&codeGen));
    codeGen.LLVMBuilder->CreateRetVoid();
    funs.push_back(codeGen.LLVMFunction);
  }
  
  // 
  // Transfer function
  //
  codeGen.reinitialize();

  // Subtle point, the string argument to parser
  // needs to be at same scope because we are not
  // copying the string inside of the parser and the
  // constructed trees have pointers into the string.
  std::string defaultTransfer;
  IQLParserStuff transferParser;
  // createTransferFunction(mTransferFun, mAggregate, mTarget);
  if (!isOlap) {
    defaultTransfer = "input.*";
    transferParser.parseTransfer(defaultTransfer);
    mTarget = transferParser.typeCheckTransfer(TypeCheckConfiguration::get(), recCtxt, mAggregate);
    codeGen.createTransferFunction(mAggregate, mTarget, mTransferFun);
  } else {
    std::stringstream xfer;
    xfer << "input0.*";
    // In the OLAP case, we have a Transfer2 going on in which
    // we have the source record and the aggregate to transfer 
    // from.  Mask out the group keys from aggregate record to
    // avoid name conflicts.
    // TODO: In the sort running total case we don't need
    // group keys in the aggregate record.  In the hash case we
    // do (so we can put the aggregate record into a hash table).
    std::vector<const RecordType *> updateSources;
    updateSources.push_back(mSource);
    updateSources.push_back(mAggregate);
    std::vector<boost::dynamic_bitset<> > masks(2);
    masks[0].resize(updateSources[0]->size(), true);
    masks[1].resize(updateSources[1]->size(), true);
    for(std::size_t i=0; i<groupKeys.size(); i++) {
      masks[1].set(i, false);
    }
    for(std::size_t i=groupKeys.size(); i < mAggregate->size(); i++) {
      xfer << ", " << mAggregate->GetMember(i).GetName().c_str();
    }
    std::vector<AliasedRecordType> types;
    types.push_back(AliasedRecordType("input0", mSource));
    types.push_back(AliasedRecordType("input1", mAggregate));

    defaultTransfer = xfer.str();
    transferParser.parseTransfer(defaultTransfer);
    mTarget = transferParser.typeCheckTransfer(TypeCheckConfiguration::get(), recCtxt, types, masks);
    codeGen.createTransferFunction(updateSources, masks, mTarget, mTransferFun);
    codeGen.IQLMoveSemantics = 0;
  }
  codeGen.IsIdentity = 1;
  {
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(transferParser.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(&codeGen));
    codeGen.LLVMBuilder->CreateRetVoid();
    funs.push_back(codeGen.LLVMFunction);
  }
  mIsIdentity = 1==codeGen.IsIdentity;
  BOOST_ASSERT(isOlap || mIsIdentity);

 // Verify and optimize
 for(std::vector<llvm::Function*>::iterator it=funs.begin();
     it != funs.end();
     ++it) {
   llvm::verifyFunction(**it);
   codeGen.mFPM->run(**it);
 }
}

void TreculAggregate::createUpdateFunction(CodeGenerationContext & codeGen,
                                           const std::vector<std::string>& groupKeys)
{
  // Create update function on top of source
  // and aggregate. Mask out group by keys in
  // aggregate to avoid conflicts.
  std::vector<const RecordType *> updateSources;
  updateSources.push_back(mSource);
  updateSources.push_back(mAggregate);
  std::vector<boost::dynamic_bitset<> > masks(2);
  masks[0].resize(updateSources[0]->size(), true);
  masks[1].resize(updateSources[1]->size(), true);
  for(std::size_t i=0; i<groupKeys.size(); i++) {
    masks[1].set(i, false);
  }
  codeGen.createUpdate(updateSources, masks, mUpdateFun);
}

TreculAggregateReference TreculAggregate::getReference() const
{
  return TreculAggregateReference(mInitializeFun, mUpdateFun, mTransferFun, mAggregate, mTarget, mIsIdentity);
}

TreculModule::TreculModule(std::unique_ptr<llvm::Module> module)
  :
  mImpl(NULL)
{
  initImpl(std::move(module));
}

TreculModule::TreculModule(CodeGenerationContext & ctxt)
  :
  mImpl(NULL)
{
  initImpl(ctxt.takeModule());
}

TreculModule::~TreculModule()
{
  delete mImpl;
}

void TreculModule::initImpl(std::unique_ptr<llvm::Module> module)
{
  mImpl = new IQLRecordBufferMethodHandle(std::move(module), mObjectFile);
}

void TreculModule::initImpl()
{
  mImpl = new IQLRecordBufferMethodHandle(mObjectFile);
}

void * TreculModule::getFunctionPointer(const TreculFunctionReference & fun)
{
  return mImpl->getFunPtr<void *>(fun.getName());
}

void TreculUpdateRuntime::execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const
{
  (*mFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);    
  ctxt->clear();
}

TreculFunctionRuntime::~TreculFunctionRuntime()
{
}

int32_t TreculFunctionRuntime::execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt) const
{
  int32_t ret;
  (*mFunction)((char *) sourceA.Ptr, (char *) sourceB.Ptr, &ret, ctxt);    
  ctxt->clear();
  return ret;
}

TreculInPlaceUpdate::TreculInPlaceUpdate(class DynamicRecordContext& recCtxt, 
                          class CodeGenerationContext & codeGen,
						 const std::string & funName, 
						 const std::vector<const RecordType *>& sources, 
						 const std::string& statements)
{
  // By default include all fields in all sources.
  std::vector<boost::dynamic_bitset<> > masks;
  masks.resize(sources.size());
  for(std::size_t i=0; i<masks.size(); ++i) {
    masks[i].resize(sources[i]->size(), true);
  }
  init(recCtxt, codeGen, funName, sources, masks, statements);
}

TreculInPlaceUpdate::TreculInPlaceUpdate(class DynamicRecordContext& recCtxt, 
                          class CodeGenerationContext & codeGen,
						 const std::string & funName, 
						 const std::vector<const RecordType *>& sources, 
						 const std::vector<boost::dynamic_bitset<> >& masks,
						 const std::string& statements)
{
  init(recCtxt, codeGen, funName, sources, masks, statements);
}

TreculInPlaceUpdate::~TreculInPlaceUpdate()
{
}

void TreculInPlaceUpdate::init(class DynamicRecordContext& recCtxt, 
                          class CodeGenerationContext & codeGen,
				   const std::string & funName, 
				   const std::vector<const RecordType *>& sources, 
				   const std::vector<boost::dynamic_bitset<> >& masks,
				   const std::string& statements)
{
  mSources = sources;
  mFunName = funName;
  mStatements = statements;

  IQLParserStuff p;
  p.parseUpdate(statements);
  p.typeCheckUpdate(TypeCheckConfiguration::get(), recCtxt, sources, masks);

  // Create the LLVM function and populate variables in
  // symbol table to prepare for code gen.
  codeGen.createUpdate(mSources, masks, mFunName);

  ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
  toLLVM->statementBlock(toLLVM.get(), wrap(&codeGen));
  codeGen.LLVMBuilder->CreateRetVoid();

  llvm::verifyFunction(*codeGen.LLVMFunction);
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *codeGen.LLVMModule;
  // // Now run optimizer over the IR
  codeGen.mFPM->run(*codeGen.LLVMFunction);
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *codeGen.LLVMModule;
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();
}

IQLExpression * TreculFunction::getAST(class DynamicRecordContext& recCtxt,
						  const std::string& f)
{
  IQLParserStuff p;
  p.parseFunction(f);
  return p.generateFunctionAST(recCtxt);
}

TreculFunction::TreculFunction(class DynamicRecordContext& recCtxt, 
                               class CodeGenerationContext & codeGen,
                               const std::string & funName, 
                               const std::vector<const RecordType *> & sources, 
                               const std::string& statements)
  :
  mFunName(funName),
  mStatements(statements)
{
  for(std::vector<const RecordType *>::const_iterator rit = sources.begin();
      rit != sources.end();
      ++rit) {
    mSources.push_back(AliasedRecordType(sources.size() == 1 ? 
					"input" : 
					(boost::format("input%1%") % (rit-sources.begin())).str().c_str(), 
					*rit));
  }
  init(recCtxt, codeGen);
}

TreculFunction::TreculFunction(class DynamicRecordContext& recCtxt, 
                               class CodeGenerationContext & codeGen,
                               const std::string & funName, 
                               const std::vector<AliasedRecordType>& sources, 
                               const std::string& statements)
  :
  mSources(sources),
  mFunName(funName),
  mStatements(statements)
{
  init(recCtxt, codeGen);
}

TreculFunction::~TreculFunction()
{
}

void TreculFunction::init(DynamicRecordContext& recCtxt,
                          CodeGenerationContext & codeGen)
{
  // Right now we assmue 2 input sources (one may be empty).
  if (mSources.size() != 2)
    throw std::runtime_error("TreculFunction requires 2 source record types (the second may be empty)");

  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(antlr3StringStreamNew((pANTLR3_UINT8) mStatements.c_str(),
                                                                 ANTLR3_ENC_UTF8,
                                                                 mStatements.size(),
                                                                 (pANTLR3_UINT8) "My Program"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  // SQL is case insensitive
  input->setUcaseLA(input.get(), ANTLR3_TRUE);

  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_singleExpression_return parserRet = psr->singleExpression(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % mStatements).str());

  // std::cout << parserRet.tree->toStringTree(parserRet.tree)->chars << std::endl;

  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  // TODO: check for name ambiguity and resolve.
  TypeCheckContext typeCheckContext(TypeCheckConfiguration::get(), recCtxt, mSources);

  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  
  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  IQLFieldTypeRef retTy = alz->singleExpression(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (unwrap(retTy)->clone(true) != Int32Type::Get(recCtxt, true))
    throw std::runtime_error("Only supporting int32_t return type on functions right now");

  // Setup LLVM access to our external structure(s).  
  codeGen.createFunction(mSources, codeGen.LLVMInt32Type, mFunName);

  ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(nodes.get()));  
  toLLVM->singleExpression(toLLVM.get(), wrap(&codeGen));
  codeGen.LLVMBuilder->CreateRetVoid();

  llvm::verifyFunction(*codeGen.LLVMFunction);
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *codeGen.LLVMModule;
  // Now run optimizer over the IR
  codeGen.mFPM->run(*codeGen.LLVMFunction);
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *codeGen.LLVMModule;
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();
}

RecordTypeFunction::RecordTypeFunction(class DynamicRecordContext& recCtxt, 
				       const std::string & funName, 
				       const std::vector<const RecordType *> & sources, 
				       const std::string& statements)
  :
  LLVMBase(),
  TreculFunction(recCtxt, *mContext, funName, sources, statements)
{
}

RecordTypeFunction::RecordTypeFunction(class DynamicRecordContext& recCtxt, 
				       const std::string & funName, 
				       const std::vector<AliasedRecordType>& sources, 
				       const std::string& statements)
  :
  LLVMBase(),
  TreculFunction(recCtxt, *mContext, funName, sources, statements)
{
}

RecordTypeFunction::~RecordTypeFunction()
{
}

int32_t RecordTypeFunction::execute(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt)
{
  if (!mModule) {
    mModule.reset(create());
  }
  return mModule->execute(source, target, ctxt);
}

IQLFunctionModule * RecordTypeFunction::create() const
{
  return new IQLFunctionModule(getFunName(), llvm::CloneModule(*mContext->LLVMModule));
}


// TODO: Put this backin GraphBuilder.cc and figure out what is
// goofy with headers that caused the compilation issue that lead
// me to put this in here.
#include "IQLGraphBuilder.hh"

void IQLGraphBuilder::buildGraph(const std::string& graphSpec, bool isFile)
{
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(isFile ?
                                           antlr3FileStreamNew((pANTLR3_UINT8) graphSpec.c_str(), ANTLR3_ENC_UTF8) :
                                           antlr3StringStreamNew((pANTLR3_UINT8) graphSpec.c_str(),
                                                                 ANTLR3_ENC_UTF8,
                                                                 graphSpec.size(),
                                                                 (pANTLR3_UINT8) "My Program"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, 
										     TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_graph_return parserRet = psr->graph(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error("Parse failed");

  // std::cout << parserRet.tree->toStringTree(parserRet.tree)->chars << std::endl;

  // Now pass through the type checker
  IQLGraphContextRef gc = wrap(this);
  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  alz->graph(alz.get(), gc);
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");
}

IQLRecordTypeBuilder::IQLRecordTypeBuilder(DynamicRecordContext& ctxt,
					   const std::string& spec, 
					   bool isFile)
  :
  mContext(ctxt)
{
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(isFile ?
                                           antlr3FileStreamNew((pANTLR3_UINT8) spec.c_str(), ANTLR3_ENC_UTF8) :
                                           antlr3StringStreamNew((pANTLR3_UINT8) spec.c_str(),
                                                                 ANTLR3_ENC_UTF8,
                                                                 spec.size(),
                                                                 (pANTLR3_UINT8) "RecordTypeParser"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  // SQL is case insensitive
  input->setUcaseLA(input.get(), ANTLR3_TRUE);

  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, 
										     TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_recordFormat_return parserRet = psr->recordFormat(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error("Parse failed");

  // std::cout << parserRet.tree->toStringTree(parserRet.tree)->chars << std::endl;

  // Now pass through builder
  TypeCheckContext typeCheckContext(TypeCheckConfiguration::get(), ctxt);
  IQLRecordTypeContextRef gc = wrap(this);
  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  alz->recordFormat(alz.get(), gc, wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");
}

IQLRecordTypeBuilder::~IQLRecordTypeBuilder()
{
}

RecordTypeAggregate::RecordTypeAggregate(DynamicRecordContext& recCtxt, 
					 const std::string & funName, 
					 const RecordType * source, 
					 const std::string& transfer,
					 const std::vector<std::string>& groupKeys,
					 bool isOlap)
  :
  LLVMBase(),
  TreculAggregate(recCtxt, *mContext, funName, source, transfer, groupKeys, isOlap)
{
}

RecordTypeAggregate::RecordTypeAggregate(class DynamicRecordContext& recCtxt, 
					 const std::string & funName, 
					 const RecordType * source, 
					 const std::string& initializer,
					 const std::string& update,
					 const std::vector<std::string>& groupKeys,
					 bool isOlap)
  :
  LLVMBase(),
  TreculAggregate(recCtxt, *mContext, funName, source, initializer, update, groupKeys, isOlap)
{
}

RecordTypeAggregate::~RecordTypeAggregate()
{
}

IQLAggregateModule * RecordTypeAggregate::create() const
{
  return new IQLAggregateModule(getAggregate()->getMalloc(),
				getTarget()->getMalloc(),
				getInitializeFunName(),
				getUpdateFunName(),
				getTransferFunName(),
				llvm::CloneModule(*mContext->LLVMModule),
				getIsTransferIdentity());
}

IQLAggregateModule::IQLAggregateModule(const RecordTypeMalloc& aggregateMalloc,
				       const RecordTypeMalloc& targetMalloc,
				       const std::string& initName, 
				       const std::string& updateName,
				       const std::string& transferName,
				       std::unique_ptr<llvm::Module> module,
				       bool isTransferIdentity)
  :
  mFunName(initName, updateName, transferName, aggregateMalloc, targetMalloc, isTransferIdentity),
  mModule(std::move(module)),
  mFunction(mModule.getAggregate(mFunName))
{
}

IQLAggregateModule::~IQLAggregateModule()
{
}

void IQLAggregateModule::executeInit(RecordBuffer & source, 
				     RecordBuffer & target, 
				     class InterpreterContext * ctxt) const
{
  mFunction.executeInit(source, target, ctxt);
}

void IQLAggregateModule::executeUpdate(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt) const
{
  mFunction.executeUpdate(source, target, ctxt);
}

void IQLAggregateModule::executeTransfer(RecordBuffer & source, 
					 RecordBuffer & target, 
					 class InterpreterContext * ctxt) const
{
  mFunction.executeTransfer(source, target, ctxt);
}

void IQLAggregateModule::executeTransfer(RecordBuffer & source1, 
					 RecordBuffer & source2, 
					 RecordBuffer & target, 
					 class InterpreterContext * ctxt) const
{
  mFunction.executeTransfer(source1, source2, target, ctxt);
}

RecordTypeFree::RecordTypeFree(const RecordType * recordType)
{
  RecordTypeFreeOperation op(recordType);
  mModule = op.create();
}

RecordTypeFree::RecordTypeFree(const RecordTypeFree & rhs)
  :
  mModule(nullptr)
{
  if(rhs.mModule != nullptr) {
    mModule = new IQLRecordTypeOperationModule(*rhs.mModule);
  }
}

RecordTypeFree & RecordTypeFree::operator=(const RecordTypeFree & rhs)
{
  delete mModule;
  mModule;
  if(rhs.mModule != nullptr) {
    mModule = new IQLRecordTypeOperationModule(*rhs.mModule);
  }
  return *this;
}

RecordTypePrint::RecordTypePrint(const RecordType * recordType)
{
  RecordTypePrintOperation op(recordType);
  mModule = op.create();
}

RecordTypePrint::RecordTypePrint(const RecordType * recordType,
                                 char fieldDelimter, char recordDelimiter, 
                                 char arrayDelimiter, char escapeChar)
{
  RecordTypePrintOperation op(recordType);
  mModule = op.create();
}

RecordTypePrint::RecordTypePrint(const RecordTypePrint & rhs)
  :
  mModule(nullptr)
{
  if(rhs.mModule != nullptr) {
    mModule = new IQLRecordTypeOperationModule(*rhs.mModule);
  }
}

RecordTypePrint & RecordTypePrint::operator=(const RecordTypePrint & rhs)
{
  delete mModule;
  mModule;
  if(rhs.mModule != nullptr) {
    mModule = new IQLRecordTypeOperationModule(*rhs.mModule);
  }
  return *this;
}

void RecordTypePrint::imbue(std::ostream& ostr) const
{
  // stream takes ownership of the facet.
  boost::posix_time::time_facet * facet =
    new boost::posix_time::time_facet("%Y-%m-%d %H:%M:%S");
  boost::gregorian::date_facet * dateFacet =
    new boost::gregorian::date_facet("%Y-%m-%d");
  ostr.imbue(std::locale(std::locale(ostr.getloc(), facet), dateFacet));
  ostr << std::fixed << std::setprecision(9);
}
