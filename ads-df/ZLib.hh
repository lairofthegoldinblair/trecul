#ifndef __TRECUL_ZLIB_HH__
#define __TRECUL_ZLIB_HH__

#include <cstdint>
#include <stdexcept>
#include <string>
#include <boost/format.hpp>
#include <zlib.h>

// TODO: Refactor this into an operator
class ZLibCompress
{
private:
  z_stream mStream;
  uint8_t * mOutputStart;
  uint8_t * mOutputEnd;
  int mFlush;
public:
  ZLibCompress(std::size_t outputBufferSz=64*1024);
  ~ZLibCompress();

  void put(const uint8_t * buf_start, std::size_t len, bool isEOS);
  bool run();
  void consumeOutput(uint8_t * & output, std::size_t & len);
  static const std::string & extension()
  {
    static const std::string ret(".gz");
    return ret;
  }
};

// As I am thinking about buffer abstractions it seems that
// I want to factor out into some kind of policy objects the
// behavior where buffer memory comes from.
// For example we want the ZLib interface to play nicely as a
// filter in the buffer model but we don't want to force any
// buffer/memory management policy on the zlib interface.
// I think the _InputBuffer concept works fine for this but
// I don't think I have a workable _OutputBuffer concept that
// works as well.  Here we punt by using the 
// read() interface (that is to say we don't abstract the notion
// of _OutputBuffer rather we create a notion of how an output
// buffer would be filled in a pull fashion).  Note that we DON'T
// want the read() interface to actually be an _InputBuffer because
// input buffers must provide memory!
template <class _InputBuffer>
class ZLibDecompress
{
private:
  _InputBuffer mInput;
  z_stream mStream;
  std::size_t mSize;

public:
  ZLibDecompress(const char * filename)
    :
    mInput(filename, 64*1024),
    mSize(0)
  {
    mStream.zalloc = Z_NULL;
    mStream.zfree = Z_NULL;
    mStream.opaque = Z_NULL;
    ::inflateInit2(&mStream, 31);
    mStream.avail_in = 0;
    mStream.next_in = NULL;
  }
  ~ZLibDecompress()
  {
    ::inflateEnd(&mStream);
  }
  int32_t read(uint8_t * buf, int32_t bufSize)
  {
    mStream.avail_out = bufSize;
    mStream.next_out = buf;
    do {
      if (mStream.avail_in == 0) {
	// Try to open a full window but no worry if we get a short read.
	mSize = 64*1024;
	mInput.open(mSize, mStream.next_in);
	mStream.avail_in = (uInt) mSize;
	if (mSize == 0) {
	  if (mInput.isEOF()) {
	    break;
	  } else {
	    // We actually got a zero read without being at EOF
	    throw std::runtime_error("Error reading compressed file");
	  }
	}
      }
      int ret = ::inflate(&mStream, 1);
      if (mStream.avail_in == 0) {
	// We've exhausted input.  Try to get more next time
	// around.
	mInput.consume(mSize);
	mSize = 0;
      }
      if (mStream.avail_out == 0) {
	break;
      } else {
	// If there wasn't a problem, the either mStream.avail_in == 0 ||
	// mStream.avail_out == 0.
	if (mStream.avail_in != 0) {
	  throw std::runtime_error((boost::format("Error decompressing file: inflate returned %1%") %
				    ret).str());
	}
      }
    } while(true);

    // Done trying to read.
    return mStream.next_out ? mStream.next_out - buf : bufSize;
  }
  bool isEOF()
  {
    return mStream.avail_in == 0 && mInput.isEOF();
  }
};

#endif
