#ifndef __TRECUL_ZSTD_HH__
#define __TRECUL_ZSTD_HH__

#include <cstdint>
#include <stdexcept>
#include <string>

#include <boost/format.hpp>

#include "zstd.h"

class ZstdCompress
{
private:
  ZSTD_CStream * mStream;
  ZSTD_inBuffer mInputBuffer;
  ZSTD_outBuffer mOutputBuffer;
  ZSTD_EndDirective mFlush;
  uint8_t * mOutputStart;
  uint8_t * mOutputEnd;
  bool mDone;
  
  void resetOutputBuffer();
public:
  ZstdCompress();
  ZstdCompress(std::size_t outputBufferSize);
  ~ZstdCompress();

  void put(const uint8_t * buf_start, std::size_t len, bool isEOS);
  bool run();
  void consumeOutput(uint8_t * & output, std::size_t & len);

  static const std::string & extension()
  {
    static const std::string ret(".zst");
    return ret;
  }
};

template <class _InputBuffer>
class ZstdDecompress
{
private:
  _InputBuffer mInput;
  ZSTD_DStream * mStream;
  ZSTD_inBuffer mInputBuffer;

public:
  ZstdDecompress(const char * filename)
    :
    mInput(filename, 64*1024)
  {
    mInputBuffer.src = nullptr;
    mInputBuffer.size = 0;
    mInputBuffer.pos = 0;
    mStream = ::ZSTD_createDStream();
    ::ZSTD_initDStream(mStream);
  }
  ~ZstdDecompress()
  {
    if (nullptr != mStream) {
      ::ZSTD_freeDStream(mStream);
    }
  }
  std::size_t read(uint8_t * buf, std::size_t bufSize)
  {
    ZSTD_outBuffer outputBuffer;
    outputBuffer.dst = buf;
    outputBuffer.size = bufSize;
    outputBuffer.pos = 0;
    do {
      if (mInputBuffer.size == mInputBuffer.pos) {
        mInputBuffer.pos = 0;
	// Try to open a full window but no worry if we get a short read.
	mInputBuffer.size = 64*1024;
        uint8_t * tmp;
	mInput.open(mInputBuffer.size, tmp);
        mInputBuffer.src = tmp;
	if (mInputBuffer.size == 0) {
	  if (mInput.isEOF()) {
            break;
          } else {
	    // We actually got a zero read without being at EOF
	    throw std::runtime_error("Error reading compressed file");
	  }
	}
      }
      int ret = ::ZSTD_decompressStream(mStream, &outputBuffer, &mInputBuffer);
      if (mInputBuffer.size == mInputBuffer.pos) {
	// We've exhausted input.  Try to get more next time
	// around.
	mInput.consume(mInputBuffer.size);
      }
      if (outputBuffer.pos == outputBuffer.size) {
	break;
      } else {
	// We should eitehr be blocked due to output being full or
        // input being exhausted
	if (mInputBuffer.size != mInputBuffer.pos) {
	  throw std::runtime_error((boost::format("Error decompressing file: inflate returned %1%") %
				    ret).str());
	}
      }
    } while(true);

    // Done trying to read.
    return outputBuffer.pos;
  }
  bool isEOF()
  {
    return mInputBuffer.size == mInputBuffer.pos && mInput.isEOF();
  }
};

#endif
