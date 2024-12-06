#ifndef __ASYNCRECORDPARSER_H__
#define __ASYNCRECORDPARSER_H__

#include <stdint.h>

#include <array>
#include <stdexcept>
#include <iostream>

#include <boost/array.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/serialization/serialization.hpp>

#include "RecordBuffer.hh"
#include "RecordType.hh"
#include "RuntimeOperator.hh"
#include "FileSystem.hh"
#include "FileService.hh"
#include "RecordParser.hh"
#include "StreamBufferBlock.hh"

typedef boost::asio::mutable_buffer AsyncDataBlock;

// Coroutine-ish parser classes

class ParserState
{
private:
  int32_t mResult;
public:
  static ParserState exhausted()
  {
    return ParserState(1);
  }
  static ParserState success()
  {
    return ParserState(0);
  }
  static ParserState error(int32_t errCode)
  {
    return ParserState(errCode);
  }
  ParserState()
    :
    mResult(1)
  {
  }
  ParserState(int32_t result)
    :
    mResult(result)
  {
  }
  bool isError() const 
  {
    return mResult < 0;
  }
  bool isExhausted() const 
  {
    return mResult == 1;
  }
  bool isSuccess() const 
  {
    return mResult == 0;
  }
  int32_t result() const
  {
    return mResult;
  }
};

class ImporterDelegate
{
private:
  typedef ParserState (*ImporterStub)(void *, AsyncDataBlock&, RecordBuffer);
  void * mObject;
  ImporterStub mMethod;

  template <class _T, ParserState (_T::*_TMethod)(AsyncDataBlock&, RecordBuffer)>
  static ParserState Stub(void * obj, AsyncDataBlock& source, RecordBuffer target)
  {
    _T* p = static_cast<_T*>(obj);
    return (p->*_TMethod)(source, target);
  }
public:
  ImporterDelegate()
    :
    mObject(NULL),
    mMethod(NULL)
  {
  }
  
  template <class _T, ParserState (_T::*_TMethod)(AsyncDataBlock&, RecordBuffer)>
  static ImporterDelegate fromMethod(_T * obj)
  {
    ImporterDelegate d;
    d.mObject = obj;
    d.mMethod = &Stub<_T, _TMethod>;
    return d;
  }

  ParserState operator()(AsyncDataBlock& targetOffset, RecordBuffer size)
  {
    return (*mMethod)(mObject, targetOffset, size);
  }
};

class ImporterSpec
{
private:
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
  }
protected:
  ImporterSpec()
  {
  }
public:
  virtual ~ImporterSpec() {}
  virtual ImporterDelegate makeObject(void * buf) const =0;
  virtual std::size_t objectSize() const =0;
  virtual std::size_t objectAlignment() const =0;
  virtual bool isConsumeOnly() const { return false; }
  static void createDefaultImport(const RecordType * recordType,
				  const RecordType * baseRecordType,
				  char fieldDelim,
				  char recordDelim,
				  std::vector<ImporterSpec*>& importers);  
};

class ConsumeTerminatedString
{
private:
  enum State { START, READ };
  State mState;
  uint8_t mTerm;

  bool importInternal(AsyncDataBlock& source, RecordBuffer target) 
  {
    auto start = static_cast<const char *>(source.data());
    auto found = static_cast<const char *>(memchr(source.data(), mTerm, boost::asio::buffer_size(source)));
    if(found) {
      source += std::size_t(found - start) + 1;
      return true;
    } else {
      source += boost::asio::buffer_size(source);
      return false;
    }
  }

public:
  ConsumeTerminatedString(uint8_t term);

  ParserState import(AsyncDataBlock& source, RecordBuffer target);
};

class ConsumeTerminatedStringSpec : public ImporterSpec
{
private:
  uint8_t mTerm;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
  }
  ConsumeTerminatedStringSpec()
    :
    mTerm(0)
  {
  }
public:
  ConsumeTerminatedStringSpec(uint8_t term)
    :
    mTerm(term)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    ConsumeTerminatedString * obj = new (buf) ConsumeTerminatedString(mTerm);
    return ImporterDelegate::fromMethod<ConsumeTerminatedString, 
      &ConsumeTerminatedString::import>(obj);
  }
  std::size_t objectSize() const
  {
    return sizeof(ConsumeTerminatedString);
  }
  std::size_t objectAlignment() const
  {
    return boost::alignment_of<ConsumeTerminatedString>::value;
  }
  bool isConsumeOnly() const { return true; }
};

template<typename _ImporterType>
class ImportNullableField
{
private:
  _ImporterType mImporter;
  enum State { START, READ, READ_2, READ_3, READ_4 };
  State mState;
  char mLookAheadBuffer;

  template<typename BuffersType>
  static void consume(BuffersType & bufs, std::size_t sz)
  {
    // First first buffer with non-zero size
    for(auto & buf : bufs) {
      if (buf.size() > 0) {
        auto to_consume = std::min(buf.size(), sz);
        buf += to_consume;
        sz -= to_consume;
        if (0 == sz) {
          return;
        }
      }
    }
  }

  template<typename BuffersType>
  bool isNullMarker(BuffersType& sources)
  {
    auto it = boost::asio::buffers_begin(sources);
    return '\\' == *it++  && 'N' == *it;
  }

public:
  template<typename... Args>
  ImportNullableField(Args&&... args)
    :
    mImporter(std::forward<Args>(args)...),
    mState(START),
    mLookAheadBuffer('\\')
  {
  }

  ParserState import(AsyncDataBlock& source, RecordBuffer target)
  {
    std::array<AsyncDataBlock, 1> sources = { source };
    auto ret = import(sources, target);
    source = sources[0];
    return ret;
  }

  template<typename BuffersType>
  ParserState import(BuffersType& sources, RecordBuffer target)
  {
    switch(mState) {
      while(true) {
      case START:
        // Fast path : we have the potential null marker available
        if (boost::asio::buffer_size(sources) >= 2) {
          if (isNullMarker(sources)) {
            mImporter.setNull(target);
            consume(sources, 2U);
            mState = START;
            return ParserState::success();
          } else {
            while(true) {
            case READ:
              auto ret = mImporter.import(sources, target);
              mState = ret.isExhausted() ? READ : START;
              return ret;
            }
          }
        }

        if (boost::asio::buffer_size(sources) == 1) {
          if ('\\' != *boost::asio::buffers_begin(sources)) {
            while(true) {
            case READ_2:
              auto ret = mImporter.import(sources, target);
              mState = ret.isExhausted() ? READ_2 : START;
              return ret;
            }
          } else {
            consume(sources, 1);
            mState = READ_3;
            return ParserState::exhausted();
            {
              case READ_3:
                boost::array<AsyncDataBlock, std::tuple_size<BuffersType>::value+1> tmp;
                tmp[0] = AsyncDataBlock(&mLookAheadBuffer, 1);
                for(std::size_t i=0; i<std::tuple_size<BuffersType>::value; ++i) {
                  tmp[i+1] = sources[i];
                }
                auto ret = mImporter.import(tmp, target);
                for(std::size_t i=0; i<std::tuple_size<BuffersType>::value; ++i) {
                  sources[i] = tmp[i+1];
                }
                // If exhausted then we must have consumed the '\' so we don't use it a second time
                BOOST_ASSERT(tmp[0].size() > 0 || !ret.isExhausted());
                mState = ret.isExhausted() ? READ_4 : START;
                return ret;              
            }
            while(true) {
            case READ_4:
              auto ret = mImporter.import(sources, target);
              mState = ret.isExhausted() ? READ_4 : START;
              return ret;              
            }
          }
        }

        mState = START;
        return ParserState::exhausted();
      }
    }
    return ParserState::error(-2);
  }
};

template<typename _TreculType>
class ImportDecimalInteger
{
public:
  typedef typename TreculNativeType<_TreculType>::type integer_type;
private:
  FieldAddress mTargetOffset;
  enum State { START, READ_FIRST, READ_DIGITS };
  State mState;
  integer_type mValue;
  uint8_t mTerm;
  bool mNeg;

  template<typename BuffersType>
  bool importInternal(BuffersType& sources, RecordBuffer target)
  {
    integer_type val = mValue;
    for(auto & source : sources) {
      uint8_t * start = static_cast<uint8_t *>(source.data());
      uint8_t * end = start + boost::asio::buffer_size(source);
      for(uint8_t * s = start; s != end; ++s) {
        if (*s > '9' || *s < '0')  {
          // TODO: Right now assuming and not validating a single delimiter character
          // TODO: Protect against overflow	
          mTargetOffset.set<_TreculType>(mNeg ? -val : val, target);
          source += std::size_t(s - start);
          mValue = 0;
          mNeg = false;
          return true;
        }
        val = val * 10 + (*s - '0');
      }
      mValue = val;
      source += boost::asio::buffer_size(source);
    }
    return false;
  }

  template<typename BuffersType>
  static void consume(BuffersType & bufs, std::size_t sz)
  {
    // First first buffer with non-zero size
    for(auto & buf : bufs) {
      if (buf.size() > 0) {
        auto to_consume = std::min(buf.size(), sz);
        buf += to_consume;
        sz -= to_consume;
        if (0 == sz) {
          return;
        }
      }
    }
  }

public:
  ImportDecimalInteger(const FieldAddress& targetOffset,
		     uint8_t term)
    :
    mTargetOffset(targetOffset),
    mState(START),
    mValue(0),
    mTerm(term),
    mNeg(false)
  {
  }

  ParserState import(AsyncDataBlock& source, RecordBuffer target)
  {
    std::array<AsyncDataBlock, 1> sources = { source };
    auto ret = import(sources, target);
    source = sources[0];
    return ret;
  }

  template<typename BuffersType>
  ParserState import(BuffersType& sources, RecordBuffer target)
  {
    switch(mState) {
      while(true) {
        BOOST_ASSERT(0 == mValue);
        BOOST_ASSERT(!mNeg);
        if (0 == boost::asio::buffer_size(sources)) {
          mState = READ_FIRST;
          return ParserState::exhausted();
        case READ_FIRST:
          if (0 == boost::asio::buffer_size(sources)) {
            return ParserState::error(-1);
          }
        }
        // if (*static_cast<const char *>(source.data()) == '-') {
        if (*boost::asio::buffers_begin(sources) == '-') {
          mNeg = true;
          consume(sources, 1);
        // } else if (*static_cast<const char *>(source.data()) == '+') {
        } else if (*boost::asio::buffers_begin(sources) == '+') {
          consume(sources, 1);
        }
        while(true) {
          if (0 == boost::asio::buffer_size(sources)) {
            mState = READ_DIGITS;
            return ParserState::exhausted();
          case READ_DIGITS:
            if (0 == boost::asio::buffer_size(sources)) {
              return ParserState::error(-1);
            }
          }
          if (importInternal(sources, target)) {
            mState = START;
            return ParserState(ParserState::success());
          case START:
            break;
          }
        }
      }
    }
    return ParserState::error(-2);
  }

  void setNull(RecordBuffer buf)
  {
    mTargetOffset.setNull(buf);
  }
};

template<typename _TreculType>
class ImportDecimalIntegerSpec : public ImporterSpec
{
public:
  typedef ImportDecimalInteger<_TreculType> importer_type;
  typedef ImportNullableField<importer_type> nullable_importer_type;
private:
  FieldAddress mTargetOffset;
  uint8_t mTerm;
  bool mNullable;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
    ar & BOOST_SERIALIZATION_NVP(mNullable);
  }
  ImportDecimalIntegerSpec()
    :
    mTerm(0)
  {
  }
public:
  ImportDecimalIntegerSpec(const FieldAddress & targetOffset,
                           uint8_t term,
                           bool nullable)
    :
    mTargetOffset(targetOffset),
    mTerm(term),
    mNullable(nullable)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    if (mNullable) {
      nullable_importer_type * obj = new (buf) nullable_importer_type(mTargetOffset, mTerm);
      return ImporterDelegate::fromMethod<nullable_importer_type, &nullable_importer_type::import>(obj);
    } else {
      importer_type * obj = new (buf) importer_type(mTargetOffset, mTerm);
      return ImporterDelegate::fromMethod<importer_type, &importer_type::import>(obj);
    }
  }

  std::size_t objectSize() const
  {
    return mNullable ? sizeof(nullable_importer_type) : sizeof(importer_type);
  }
  std::size_t objectAlignment() const
  {
    return mNullable ? boost::alignment_of<nullable_importer_type >::value : boost::alignment_of<importer_type >::value;
  }
};

struct ImportDateType
{
  typedef std::array<uint8_t, 3> array_type;
  typedef std::array<unsigned short, 3> nums_type;
  static array_type sizes;
  static void createAndSet(const nums_type & nums, FieldAddress addr, RecordBuffer buf)
  {
    typedef boost::gregorian::date date;
    // This call profiles to be pretty slow.
    // Can this be improved without losing too much
    // safety?
    date t(date(nums[0], nums[1], nums[2]));
    addr.setDate(t, buf);
  }
};

struct ImportDatetimeType
{
  typedef std::array<uint8_t, 6> array_type;
  typedef std::array<unsigned short, 6> nums_type;
  static array_type sizes;
  static void createAndSet(const nums_type & nums, FieldAddress addr, RecordBuffer buf)
  {
    typedef boost::posix_time::ptime ptime;
    typedef boost::posix_time::time_duration time_duration;
    typedef boost::gregorian::date date;
    // This call profiles to be pretty slow.
    // Can this be improved without losing too much
    // safety?
    ptime t(date(nums[0], nums[1], nums[2]),
    	    time_duration(nums[3],nums[4],nums[5]));
    addr.setDatetime(t, buf);
  }
};

template <typename _DatetimeType>
class ImportDefaultDatetime
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ_DELIM, READ_DIGITS };
  State mState = START;
  typename _DatetimeType::nums_type mNums;
  uint8_t mOuterIndex = 0;
  uint8_t mInnerIndex = 0;

  template<typename BuffersType>
  static void consume(BuffersType & bufs, std::size_t sz)
  {
    // First first buffer with non-zero size
    for(auto & buf : bufs) {
      if (buf.size() > 0) {
        auto to_consume = std::min(buf.size(), sz);
        buf += to_consume;
        sz -= to_consume;
        if (0 == sz) {
          return;
        }
      }
    }
  }

public:
  ImportDefaultDatetime(const FieldAddress& targetOffset)
    :
    mTargetOffset(targetOffset)
  {
    for(std::size_t i=0; i<std::tuple_size<typename _DatetimeType::array_type>::value; ++i) {
      mNums[i] = 0;
    }
  }

  ParserState import(AsyncDataBlock& source, RecordBuffer target) 
  {
    std::array<AsyncDataBlock, 1> sources = { source };
    auto ret = import(sources, target);
    source = sources[0];
    return ret;
  }

  template<typename BuffersType>
  ParserState import(BuffersType& sources, RecordBuffer target)
  {
    // Importing a fixed format of 10 chars
    // YYYY-MM-DD
    switch(mState) {
      while(true) {
      case START:
       for(mOuterIndex=0; mOuterIndex<std::tuple_size<typename _DatetimeType::array_type>::value; ++mOuterIndex) {
          mNums[mOuterIndex] = 0;
          for(mInnerIndex=0; mInnerIndex<_DatetimeType::sizes[mOuterIndex]; mInnerIndex++) {
            if (0 == boost::asio::buffer_size(sources)) {
              mState = READ_DIGITS;
              return ParserState::exhausted();
            case READ_DIGITS:
              if (0 == boost::asio::buffer_size(sources)) {
                return ParserState::error(-1);
              }
            }
            auto s = *boost::asio::buffers_begin(sources);
            if (s > '9' || s < '0')  {
              return ParserState::error(-1);
            }
            mNums[mOuterIndex] = mNums[mOuterIndex] * 10 + (s - '0');
            consume(sources, 1);
          }
          if (mOuterIndex+1 != std::tuple_size<typename _DatetimeType::array_type>::value) {
            if (0 == boost::asio::buffer_size(sources)) {
              mState = READ_DELIM;
              return ParserState::exhausted();
            case READ_DELIM:
              if (0 == boost::asio::buffer_size(sources)) {
                return ParserState::error(-1);
              }
            }
            consume(sources, 1);
          }
        }
       _DatetimeType::createAndSet(mNums, mTargetOffset, target);
       mState = START;
       return ParserState::success();
      }
    }
    return ParserState::error(-2);
  }

  void setNull(RecordBuffer buf)
  {
    mTargetOffset.setNull(buf);
  }
};

template <typename _DatetimeType>
class ImportDefaultDatetimeSpec : public ImporterSpec
{
public:
  typedef ImportDefaultDatetime<_DatetimeType> importer_type;
  typedef ImportNullableField<importer_type> nullable_importer_type;
private:
  FieldAddress mTargetOffset;
  bool mNullable = false;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mNullable);
  }
  ImportDefaultDatetimeSpec() {}
public:
  ImportDefaultDatetimeSpec(const FieldAddress & targetOffset,
                           bool nullable)
    :
    mTargetOffset(targetOffset),
    mNullable(nullable)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    if (mNullable) {
      nullable_importer_type * obj = new (buf) nullable_importer_type(mTargetOffset);
      return ImporterDelegate::fromMethod<nullable_importer_type, &nullable_importer_type::import>(obj);
    } else {
      importer_type * obj = new (buf) importer_type(mTargetOffset);
      return ImporterDelegate::fromMethod<importer_type, &importer_type::import>(obj);
    }
  }

  std::size_t objectSize() const
  {
    return mNullable ? sizeof(nullable_importer_type) : sizeof(importer_type);
  }
  std::size_t objectAlignment() const
  {
    return mNullable ? boost::alignment_of<nullable_importer_type >::value : boost::alignment_of<importer_type >::value;
  }
};

struct ImportVarcharType
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    addr.SetVariableLengthString(buf, (const char *) begin, std::distance(begin, end));
    return true;
  }
};

struct ImportDoubleType
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    char * endptr;
    addr.setDouble(::strtod((const char *) begin, &endptr), buf);
    return reinterpret_cast<const char *>(end) == endptr;
  }
};

struct ImportFloatType
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    char * endptr;
    addr.setDouble(::strtod((const char *) begin, &endptr), buf);
    return reinterpret_cast<const char *>(end) == endptr;
  }
};

struct ImportIPv4Type
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    // TODO: When we require c++17 then we can use string_view and avoid copy (or we should not use
    // boost and just call the underlying c API to parse
    addr.setIPv4(boost::asio::ip::make_address_v4(std::string((const char *) begin, (const char *) end)), buf);
    return true;
  }
};

struct ImportCIDRv4Type
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    // TODO: When we require c++17 then we can use string_view and avoid copy (or we should not use
    // boost and just call the underlying c API to parse
    std::array<char, 1> toks = { '/' };
    const char * sep = std::search((const char *) begin, (const char *) end,
                                   &toks[0], &toks[1]);
    auto prefix = boost::asio::ip::make_address_v4(std::string((const char *) begin, sep));
    uint8_t prefix_length = sep == (const char *) end || sep+1 == (const char *) end ? 32 : atoi(sep+1);
    addr.setCIDRv4({ prefix,  prefix_length}, buf);
    return true;
  }
};

struct ImportIPv6Type
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    // TODO: When we require c++17 then we can use string_view and avoid copy (or we should not use
    // boost and just call the underlying c API to parse
    std::string tmp((const char *) begin, (const char *) end);
    // Try to parse as v4 first and use that if it succeeds.
    boost::system::error_code ec;
    auto v4addr = boost::asio::ip::make_address_v4(tmp, ec);
    if (!ec) {
      addr.setIPv6(boost::asio::ip::make_address_v6(boost::asio::ip::v4_mapped, v4addr), buf);
      return true;    
    } else {
      auto v6addr = boost::asio::ip::make_address_v6(tmp, ec);
      if (!ec) {
        addr.setIPv6(v6addr, buf);
        return true;
      } else {
        return false;
      }
    }
  }
};

struct ImportCIDRv6Type
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    // TODO: When we require c++17 then we can use string_view and avoid copy (or we should not use
    // boost and just call the underlying c API to parse
    std::array<char, 1> toks = { '/' };
    const char * sep = std::search((const char *) begin, (const char *) end,
                                   &toks[0], &toks[1]);
    // Try to parse as v4 first and use that if it succeeds.
    std::string tmp((const char *) begin, sep);
    boost::system::error_code ec;
    auto v4addr = boost::asio::ip::make_address_v4(tmp, ec);
    if (!ec) {
      int32_t prefix_length = sep == (const char *) end || sep+1 == (const char *) end ? 32 : atoi(sep+1);
      if (prefix_length < 0 || prefix_length > 32) {
        return false;
      }
      addr.setCIDRv6({ boost::asio::ip::make_address_v6(boost::asio::ip::v4_mapped, v4addr), (uint8_t) (prefix_length + 96) },
                     buf);
      return true;    
    } else {
      auto v6addr = boost::asio::ip::make_address_v6(tmp, ec);
      if (!ec) {
        int32_t prefix_length = sep == (const char *) end || sep+1 == (const char *) end ? 128 : atoi(sep+1);
        if (prefix_length < 0 || prefix_length > 128) {
          return false;
        }
        addr.setCIDRv6({ v6addr, (uint8_t) prefix_length }, buf);
        return true;
      } else {
        return false;
      }
    }
  }
};

struct ImportDecimalType
{
  static bool parseAndSet(const uint8_t * begin, const uint8_t * end, FieldAddress addr, RecordBuffer buf)
  {
    decContext decCtxt;
    decContextDefault(&decCtxt, DEC_INIT_DECIMAL128); // no traps, please
    decimal128 * dec = addr.getDecimalPtr(buf);
    decimal128FromString(dec, (char *) begin, &decCtxt);
    addr.clearNull(buf);
    return 0 == decCtxt.status;
  }
};

template<typename _ImportFieldType>
class ImportOptionalBuffer
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ };
  State mState;
  std::vector<uint8_t> * mLocal;
  uint8_t mTerm;

public:
  ImportOptionalBuffer(const FieldAddress& targetOffset,
                      uint8_t term)
    :
    mTargetOffset(targetOffset),
    mState(START),
    mLocal(NULL),
    mTerm(term)
  {
  }
  
  ParserState import(AsyncDataBlock& source, RecordBuffer target)
  {
    std::array<AsyncDataBlock, 1> sources = { source };
    auto ret = import(sources, target);
    source = sources[0];
    return ret;
  }

  template<typename BuffersType>
  ParserState import(BuffersType& sources, RecordBuffer target)
  {
    switch(mState) {
      while(true) {      
      case START:
        // Fast path; we find a terminator in the input buffer and
        // just call atof.
        if (1 == std::distance(boost::asio::buffer_sequence_begin(sources), boost::asio::buffer_sequence_end(sources))) {
          uint8_t term = mTerm;
          for(auto & source : sources) {
            for(uint8_t * s = static_cast<uint8_t *>(source.data()), * e = static_cast<uint8_t *>(source.data()) + boost::asio::buffer_size(source);
                s != e; ++s) {
              if (*s == term) {
                bool ret = _ImportFieldType::parseAndSet(static_cast<uint8_t *>(source.data()), s, mTargetOffset, target);
                source += int(ret)*(s - static_cast<uint8_t *>(source.data()));
                mState = START;
                return ret ? ParserState::success() : ParserState::error(-1);
              }
            }
          }
          // Already know that there is no terminator, so copy everything then head to slow path with a read
          mLocal = new std::vector<uint8_t>();
          for(auto & source : sources) {
            mLocal->insert(mLocal->end(), static_cast<uint8_t *>(source.data()),
                           static_cast<uint8_t *>(source.data()) + boost::asio::buffer_size(source));
            source += boost::asio::buffer_size(source);
          }
          mState = READ;
          return ParserState::exhausted();
        }  

        // Slow path; the field crosses the block boundary.  Copy into
        // private memory and call out to parse routine from there.
        do {
          case READ:
            if (nullptr == mLocal) {
              mLocal = new std::vector<uint8_t>();
            }
            {
              uint8_t term = mTerm;
              for(auto & source : sources) {
                for(uint8_t * s = static_cast<uint8_t *>(source.data()), * e = static_cast<uint8_t *>(source.data()) + boost::asio::buffer_size(source);
                    s != e; ++s) {
                  if (*s == term) {
                    mLocal->insert(mLocal->end(), static_cast<uint8_t *>(source.data()), s);
                    mLocal->push_back(0);
                    // Set end to point to the trailing null terminator
                    bool ret = _ImportFieldType::parseAndSet(&mLocal->operator[](0), &mLocal->operator[](0) + mLocal->size() - 1, mTargetOffset, target);
                    source += int(ret)*(s - static_cast<uint8_t *>(source.data()));
                    delete mLocal;
                    mLocal = NULL;
                    mState = START;
                    return ret ? ParserState::success() : ParserState::error(-1);
                  } 
                }
                mLocal->insert(mLocal->end(), static_cast<uint8_t *>(source.data()), static_cast<uint8_t *>(source.data()) + boost::asio::buffer_size(source));
                source += boost::asio::buffer_size(source);
              }
              mState = READ;
              return ParserState::exhausted();
            }
        } while(true);
      }
    }
    return ParserState::error(-2);
  }

  void setNull(RecordBuffer buf)
  {
    mTargetOffset.setNull(buf);
  }
};

template<typename _ImportFieldType>
class ImportOptionalBufferSpec : public ImporterSpec
{
public:
  typedef ImportOptionalBuffer<_ImportFieldType> importer_type;
  typedef ImportNullableField<importer_type> nullable_importer_type;
private:
  FieldAddress mTargetOffset;
  uint8_t mTerm;
  bool mNullable;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
    ar & BOOST_SERIALIZATION_NVP(mNullable);
  }
  ImportOptionalBufferSpec()
    :
    mTerm(0)
  {
  }
public:
  ImportOptionalBufferSpec(const FieldAddress & targetOffset,
                           uint8_t term,
                           bool nullable)
    :
    mTargetOffset(targetOffset),
    mTerm(term),
    mNullable(nullable)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    if (mNullable) {
      nullable_importer_type * obj = new (buf) nullable_importer_type(mTargetOffset, mTerm);
      return ImporterDelegate::fromMethod<nullable_importer_type, &nullable_importer_type::import>(obj);
    } else {
      importer_type * obj = new (buf) importer_type(mTargetOffset, mTerm);
      return ImporterDelegate::fromMethod<importer_type, &importer_type::import>(obj);
    }
  }

  std::size_t objectSize() const
  {
    return mNullable ? sizeof(nullable_importer_type) : sizeof(importer_type);
  }
  std::size_t objectAlignment() const
  {
    return mNullable ? boost::alignment_of<nullable_importer_type >::value : boost::alignment_of<importer_type >::value;
  }
};

template<typename _ImportFieldType>
class ImportWithBuffer
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ };
  State mState;
  std::vector<uint8_t> mLocal;
  uint8_t mTerm;

public:
  ImportWithBuffer(const FieldAddress& targetOffset,
                      uint8_t term)
    :
    mTargetOffset(targetOffset),
    mState(START),
    mTerm(term)
  {
  }

  ParserState import(AsyncDataBlock& source, RecordBuffer target)
  {
    std::array<AsyncDataBlock, 1> sources = { source };
    auto ret = import(sources, target);
    source = sources[0];
    return ret;
  }

  template<typename BuffersType>
  ParserState import(BuffersType& sources, RecordBuffer target)
  {
    switch(mState) {
      while(true) {      
      case START:
        // Slow path; the field crosses the block boundary.  Copy into
        // private memory and call atof from there.
        mLocal.resize(0);
        do {
            case READ:
            {
              uint8_t term = mTerm;
              for(auto & source : sources) {
                for(uint8_t * s = static_cast<uint8_t *>(source.data()), * e = static_cast<uint8_t *>(source.data()) + boost::asio::buffer_size(source);
                    s != e; ++s) {
                  if (*s == term) {
                    mLocal.insert(mLocal.end(), static_cast<uint8_t *>(source.data()), s);
                    mLocal.push_back(0);
                    bool ret = _ImportFieldType::parseAndSet(&mLocal[0], &mLocal[0] + mLocal.size(), mTargetOffset, target);
                    source += int(ret)*(s - static_cast<uint8_t *>(source.data()));
                    mLocal.resize(0);
                    mState = START;
                    return ret ? ParserState::success() : ParserState::error(-1);
                  } 
                }
                mLocal.insert(mLocal.end(), static_cast<uint8_t *>(source.data()), static_cast<uint8_t *>(source.data()) + boost::asio::buffer_size(source));
                source += boost::asio::buffer_size(source);
              }
            }
            mState = READ;
            return ParserState::exhausted();
        } while(true);
      }
    }
    return ParserState::error(-2);
  }

  void setNull(RecordBuffer buf)
  {
    mTargetOffset.setNull(buf);
  }
};

template<typename _ImportFieldType>
class ImportWithBufferSpec : public ImporterSpec
{
  typedef ImportWithBuffer<_ImportFieldType> importer_type;
  typedef ImportNullableField<importer_type> nullable_importer_type;
private:
  FieldAddress mTargetOffset;
  uint8_t mTerm;
  bool mNullable;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
    ar & BOOST_SERIALIZATION_NVP(mNullable);
  }
  ImportWithBufferSpec()
    :
    mTerm(0)
  {
  }
public:
  ImportWithBufferSpec(const FieldAddress & targetOffset,
                       uint8_t term,
                       bool nullable)
    :
    mTargetOffset(targetOffset),
    mTerm(term),
    mNullable(nullable)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    if (mNullable) {
      nullable_importer_type * obj = new (buf) nullable_importer_type(mTargetOffset, mTerm);
      return ImporterDelegate::fromMethod<nullable_importer_type, &nullable_importer_type::import>(obj);
    } else {
      importer_type * obj = new (buf) importer_type(mTargetOffset, mTerm);
      return ImporterDelegate::fromMethod<importer_type, &importer_type::import>(obj);
    }
  }

  std::size_t objectSize() const
  {
    return mNullable ? sizeof(nullable_importer_type) : sizeof(importer_type);
  }
  std::size_t objectAlignment() const
  {
    return mNullable ? boost::alignment_of<nullable_importer_type >::value : boost::alignment_of<importer_type >::value;
  }
};

class ImportFixedLengthString
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ };
  State mState;
  int32_t mSize;
  // For slow path (partial reads); where in the target
  // are we.
  int32_t mRead;
public:
  ImportFixedLengthString(const FieldAddress& targetOffset,
			  int32_t size);

  ParserState import(AsyncDataBlock& source, RecordBuffer target) 
  {
    std::array<AsyncDataBlock, 1> sources = { source };
    auto ret = import(sources, target);
    source = sources[0];
    return ret;
  }

  template<typename BuffersType>
  ParserState import(BuffersType& sources, RecordBuffer target)
  {
    switch(mState) {
      while(true) { 
      case START:
        // Fast path
        if (mSize <= sources[0].size()) {
          mTargetOffset.SetFixedLengthString(target, 
                                             static_cast<const char *>(sources[0].data()), 
                                             mSize);
          sources[0] += mSize;
          mState = START;
          return ParserState::success();
        }

        // Slow path : not enough buffer to copy in one
        // shot.  Read the remainder of the buffer and try
        // again.
        mRead = 0;
        while(true) {
          if (0 == boost::asio::buffer_size(sources)) {
            mState = READ;
            return ParserState::exhausted();
          case READ:
            if (0 == boost::asio::buffer_size(sources)) {
              return ParserState::error(-1);
            }
          }
          for(auto & source : sources) {
            if (mSize - mRead <= source.size()) {	  
              // We're done
              int32_t toRead = mSize - mRead;
              memcpy(mTargetOffset.getCharPtr(target) + mRead,
                     source.data(),
                     toRead);
              mTargetOffset.clearNull(target);
              source += toRead;
              mState = START;
              mRead = 0;
              return ParserState::success();
            } else {
              // Partial read
              int32_t toRead = (int32_t) source.size();
              memcpy(mTargetOffset.getCharPtr(target) + mRead,
                     source.data(),
                     toRead);
              mRead += toRead;
              source += source.size();
            }
          }
        }      
      }
    }
    return ParserState::error(-2);
  }

  void setNull(RecordBuffer buf)
  {
    mTargetOffset.setNull(buf);
  }
};

class ImportFixedLengthStringSpec : public ImporterSpec
{
public:
  typedef ImportFixedLengthString importer_type;
  typedef ImportNullableField<importer_type> nullable_importer_type;
private:
  FieldAddress mTargetOffset;
  int32_t mSize;
  uint8_t mTerm;
  bool mNullable;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mSize);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
    ar & BOOST_SERIALIZATION_NVP(mNullable);
  }
  ImportFixedLengthStringSpec()
    :
    mSize(0),
    mTerm(0),
    mNullable(false)
  {
  }
public:
  ImportFixedLengthStringSpec(const FieldAddress & targetOffset,
			      int32_t sz,
			      uint8_t term,
                              bool nullable)
    :
    mTargetOffset(targetOffset),
    mSize(sz),
    mTerm(term),
    mNullable(nullable)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    // ImportFixedLengthString * obj = new (buf) ImportFixedLengthString(mTargetOffset, mSize);
    // return ImporterDelegate::fromMethod<ImportFixedLengthString, &ImportFixedLengthString::import>(obj);
    if (mNullable) {
      nullable_importer_type * obj = new (buf) nullable_importer_type(mTargetOffset, mSize);
      return ImporterDelegate::fromMethod<nullable_importer_type, &nullable_importer_type::import>(obj);
    } else {
      importer_type * obj = new (buf) importer_type(mTargetOffset, mSize);
      return ImporterDelegate::fromMethod<importer_type, &importer_type::import>(obj);
    }
  }

  std::size_t objectSize() const
  {
    // return sizeof(ImportFixedLengthString);
    return mNullable ? sizeof(nullable_importer_type) : sizeof(importer_type);
  }
  std::size_t objectAlignment() const
  {
    return boost::alignment_of<ImportFixedLengthString>::value;
    // return mNullable ? boost::alignment_of<nullable_importer_type >::value : boost::alignment_of<importer_type >::value;
  }
};

class LogicalAsyncParser : public LogicalOperator
{
private:
  TreculFreeOperation * mInputStreamFree;
  const RecordType * mFormat;
  std::string mStringFormat;
  std::string mMode;
  bool mSkipHeader;
  char mFieldSeparator;
  char mRecordSeparator;
  std::string mCommentLine;
public:
  LogicalAsyncParser();
  ~LogicalAsyncParser();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class GenericAsyncParserOperatorType : public RuntimeOperatorType
{
  friend class GenericAsyncParserOperator;
private:
  typedef ImporterSpec* field_importer_type;
  typedef std::vector<ImporterSpec*>::const_iterator field_importer_const_iterator;
  // Importer instructions
  std::vector<field_importer_type> mImporters;
  // Importer to read to end of line (when skipping over non "r" log lines).
  field_importer_type mSkipImporter;
  // Access to stream buffer
  StreamBufferBlock mStreamBlock;
  // Create new records
  RecordTypeMalloc mStreamMalloc;
  RecordTypeMalloc mMalloc;
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  // What am I importing
  const RecordType * mRecordType;
  // Is there a header to skip?
  bool mSkipHeader;
  // Skip lines starting with this.
  std::string mCommentLine;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mImporters);
    ar & BOOST_SERIALIZATION_NVP(mSkipImporter);
    ar & BOOST_SERIALIZATION_NVP(mStreamBlock);
    ar & BOOST_SERIALIZATION_NVP(mStreamMalloc);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mSkipHeader);
    ar & BOOST_SERIALIZATION_NVP(mCommentLine);
  }
  GenericAsyncParserOperatorType()
  {
  }  

public:
  GenericAsyncParserOperatorType(char fieldSeparator,
				 char recordSeparator,
				 const RecordType * inputStreamType,
                                 const TreculFreeOperation & inputStreamTypeFreeFunctor,
				 const RecordType * recordType,
				 const RecordType * baseRecordType=NULL,
				 const char * commentLine = "");

  ~GenericAsyncParserOperatorType();

  void setSkipHeader(bool value) 
  {
    mSkipHeader = value;
  }

  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
  }
  
  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

class GenericRecordImporter
{
public:
  typedef std::vector<ImporterDelegate>::iterator iterator;
private:
  // The importer objects themselves
  uint8_t * mImporterObjects;
  // Importer delegates
  std::vector<ImporterDelegate> mImporters;
public:
  GenericRecordImporter(ImporterSpec * spec);
  GenericRecordImporter(std::vector<ImporterSpec*>::const_iterator begin,
			std::vector<ImporterSpec*>::const_iterator end);
  ~GenericRecordImporter();
  iterator begin() 
  {
    return mImporters.begin();
  }
  iterator end() 
  {
    return mImporters.end();
  }
};

class LogicalBlockRead : public LogicalOperator
{
private:
  const RecordType * mStreamBlock;
  TreculFreeOperation * mStreamBlockFree;
  std::string mFile;
  int32_t mBufferCapacity;
  bool mBucketed;
public:
  LogicalBlockRead();
  ~LogicalBlockRead();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

template<class _OpType>
class GenericAsyncReadOperator : public RuntimeOperator
{
private:
  typedef _OpType operator_type;

  enum State { START, OPEN_FILE, READ_BLOCK, WRITE_BLOCK, WRITE_EOF };
  State mState;

  // The list of files from which I read; retrieved
  // by calling my operator type.
  std::vector<std::shared_ptr<FileChunk> > mFiles;
  // The file am I working on
  std::vector<std::shared_ptr<FileChunk> >::const_iterator mFileIt;
  // File Service for async IO
  FileService * mFileService;
  // File handle that is currently open
  FileServiceFile * mFileHandle;
  // Record buffer I am importing into
  RecordBuffer mOutput;

  const operator_type & getLogParserType()
  {
    return *static_cast<const operator_type *>(&getOperatorType());
  }

public:
  GenericAsyncReadOperator(RuntimeOperator::Services& services, 
			     const RuntimeOperatorType& opType)
    :
    RuntimeOperator(services, opType),
    mFileService(NULL),
    mFileHandle(NULL)
  {
  }

  ~GenericAsyncReadOperator()
  {
    if (mFileService) {
      FileService::release(mFileService);
    }
  }

  /**
   * intialize.
   */
  void start()
  {
    mFiles.clear();
    // What file(s) am I parsing?
    typename _OpType::chunk_strategy_type chunkFiles;
    // Expand file name globbing, then get files for this
    // partition.
    chunkFiles.expand(getLogParserType().mFileInput, getNumPartitions());
    chunkFiles.getFilesForPartition(getPartition(), mFiles);
    mState = START;
    mFileService = FileService::get();
    
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      for(mFileIt = mFiles.begin();
	  mFileIt != mFiles.end();
	  ++mFileIt) {
	BOOST_ASSERT(mFileHandle == NULL);
	// Allocate a new input buffer for the file in question.
	if ((*mFileIt)->getBegin() > 0) {
	  throw std::runtime_error("Not implemented yet");
	  // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
	  // 				64*1024,
	  // 				(*mFileIt)->getBegin()-1,
	  // 				(*mFileIt)->getEnd());
	  // RecordBuffer nullRecord;
	  // getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
	} else {
	  // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
	  // 				64*1024,
	  // 				(*mFileIt)->getBegin(),
	  // 				(*mFileIt)->getEnd());
	  mFileService->requestOpenForRead((*mFileIt)->getFilename().c_str(), 
					   (*mFileIt)->getBegin(),
					   (*mFileIt)->getEnd(),
					   getCompletionPorts()[0]);
	  requestCompletion(0);
	  mState = OPEN_FILE;
	  return;
	case OPEN_FILE:
	  {
	    RecordBuffer buf;
	    read(port, buf);
	    mFileHandle = mFileService->getOpenResponse(buf);
	  }
	}
	
	// Read all of the record in the file.
	while(true) {
	  mOutput = getLogParserType().mMalloc.malloc();
	  // If empty read a block; it is OK to exhaust a file
	  // here but not while in the middle of a record, so 
	  // we make a separate read attempt here.
	  mFileService->requestRead(mFileHandle, 
				    (uint8_t *) getLogParserType().mBufferAddress.getCharPtr(mOutput), 
				    getLogParserType().mBufferCapacity, 
				    getCompletionPorts()[0]);
	  requestCompletion(0);
	  mState = READ_BLOCK;
	  return;
	case READ_BLOCK:
	  {
	    RecordBuffer buf;
	    read(port, buf);
	    int32_t bytesRead = mFileService->getReadBytes(buf);
	    if (0 == bytesRead) {
	      getLogParserType().mFree.free(mOutput);
	      mOutput = RecordBuffer();
	      // TODO: Send a message indicating file boundary potentially
	      // Decompressors may need this to resync state (or maybe not).
	      break;
	    }
	    getLogParserType().mBufferSize.setInt32(bytesRead, mOutput);
	  }
	  // Done cause we had good record
	  // Flush always and write through to
	  // avoid local queue; these are big chunks of memory
	  requestWriteThrough(0);
	  mState = WRITE_BLOCK;
	  return;
	case WRITE_BLOCK:
	  write(port, mOutput, true);
	}
	// Either EOF or parse failure.  In either
	// case done with this file.
	// TODO: FIXME
	// delete mFileHandle;
	mFileHandle = NULL;
      }
      // Done with the last file so output EOS.
      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(), true);
      return;
    }
  }

  void shutdown()
  {
  }
};

template <class _ChunkStrategy = ExplicitChunkStrategy >
class GenericAsyncReadOperatorType : public RuntimeOperatorType
{
  // Don't really know how to do friends between templates.
public:
  typedef _ChunkStrategy chunk_strategy_type;
  typedef typename _ChunkStrategy::file_input file_input_type;
  typedef ImporterSpec* field_importer_type;
  typedef std::vector<ImporterSpec*>::const_iterator field_importer_const_iterator;

  // What file(s) am I parsing?
  file_input_type mFileInput;
  // Create new records
  RecordTypeMalloc mMalloc;
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  // Accessors into buffer (size INTEGER, buffer CHAR(N))
  FieldAddress mBufferSize;
  FieldAddress mBufferAddress;
  // Size of buffer to allocate
  int32_t mBufferCapacity;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFileInput);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mBufferSize);
    ar & BOOST_SERIALIZATION_NVP(mBufferAddress);
    ar & BOOST_SERIALIZATION_NVP(mBufferCapacity);
  }
  GenericAsyncReadOperatorType()
  {
  }  

public:
  GenericAsyncReadOperatorType(const typename _ChunkStrategy::file_input& file,
                               const RecordType * streamBlockType,
                               const TreculFreeOperation & streamBlockFreeFunctor)
    :
    RuntimeOperatorType("GenericAsyncReadOperatorType"),
    mFileInput(file),
    mMalloc(streamBlockType->getMalloc()),
    mFreeRef(streamBlockFreeFunctor.getReference()),
    mBufferSize(streamBlockType->getFieldAddress("size")),
    mBufferAddress(streamBlockType->getFieldAddress("buffer")),
    mBufferCapacity(streamBlockType->getMember("buffer").GetType()->GetSize())
  {
  }

  ~GenericAsyncReadOperatorType()
  {
  }

  int32_t numServiceCompletionPorts() const 
  {
    return 1;
  }

  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
  }
  
  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

template <class _ChunkStrategy>
RuntimeOperator * GenericAsyncReadOperatorType<_ChunkStrategy>::create(RuntimeOperator::Services & services) const
{
  return new GenericAsyncReadOperator<GenericAsyncReadOperatorType<_ChunkStrategy> >(services, *this);
}

#endif
