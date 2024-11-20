#include "ZLib.hh"

ZLibCompress::ZLibCompress(std::size_t outputBufferSz)
  :
  mOutputStart(NULL),
  mOutputEnd(NULL)
{
  mOutputStart = new uint8_t [outputBufferSz];
  mOutputEnd = mOutputStart + outputBufferSz;
  mStream.zalloc = Z_NULL;
  mStream.zfree = Z_NULL;
  mStream.opaque = Z_NULL;
  // We use deflateInit2 so we can request a gzip header.  Other
  // params are set to defaults.
  int ret = ::deflateInit2(&mStream, 
			   Z_DEFAULT_COMPRESSION, 
			   Z_DEFLATED,
			   31, // Greater than 15 indicates gzip format
			   8,
			   Z_DEFAULT_STRATEGY);
  if (ret != Z_OK)
    throw std::runtime_error("Error initializing compression library");

  // Bind the buffer to the gzip stream
  mStream.avail_out = (mOutputEnd - mOutputStart);
  mStream.next_out = mOutputStart;
}

ZLibCompress::~ZLibCompress()
{
  deflateEnd(&mStream);
  delete [] mOutputStart;
}

void ZLibCompress::put(const uint8_t * bufStart, std::size_t len, bool isEOS)
{
  mFlush = isEOS ? Z_FINISH : Z_NO_FLUSH;
  mStream.avail_in = (int) len;
  mStream.next_in = const_cast<uint8_t *>(bufStart);
}

bool ZLibCompress::run()
{
  // Try to consume the input.
  int ret=Z_OK;
  do {
    ret = ::deflate(&mStream, mFlush);    
  } while(ret == Z_OK);
  return mStream.avail_in == 0;
}

void ZLibCompress::consumeOutput(uint8_t * & output, std::size_t & len)
{
  // How much data in the compressor state?
  output = mOutputStart;
  len = mStream.next_out - mOutputStart;
  // Reset for more output.
  mStream.avail_out = (mOutputEnd - mOutputStart);
  mStream.next_out = mOutputStart;  
}

