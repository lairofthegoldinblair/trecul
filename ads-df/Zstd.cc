#include "Zstd.hh"

ZstdCompress::ZstdCompress(std::size_t outputBufferSize)
  :
  mStream(ZSTD_createCStream()),
  mOutputStart(new uint8_t [outputBufferSize]),
  mOutputEnd(mOutputStart + outputBufferSize),
  mDone(false)
{
  // We use deflateInit2 so we can request a gzip header.  Other
  // params are set to defaults.
  std::size_t ret = ::ZSTD_initCStream(mStream, 3);
  if (ZSTD_isError(ret))
    throw std::runtime_error((boost::format("Error initializing zstd compression library: %1%") % ZSTD_getErrorName(ret)).str());

  resetOutputBuffer();
}

ZstdCompress::ZstdCompress()
  :
  ZstdCompress(ZSTD_CStreamOutSize())
{
}

ZstdCompress::~ZstdCompress()
{
  delete [] mOutputStart;
  if (nullptr != mStream) {
    ::ZSTD_freeCStream(mStream);
  }
}

void ZstdCompress::put(const uint8_t * bufStart, std::size_t len, bool isEOS)
{
  mInputBuffer.src = bufStart;
  mInputBuffer.size = len;
  mInputBuffer.pos = 0;  
  mFlush = isEOS ? ZSTD_e_end : ZSTD_e_continue;
}

bool ZstdCompress::run()
{
  if (mDone) {
    return true;
  }
  // Try to consume the input.
  std::size_t ret=0;
  do {
    ret = ::ZSTD_compressStream2(mStream, &mOutputBuffer, &mInputBuffer, mFlush);    
  } while(ret > 0 && !::ZSTD_isError(ret) && mOutputBuffer.pos < mOutputBuffer.size);
  mDone = 0 == ret && ZSTD_e_end == mFlush;
  return mInputBuffer.pos == mInputBuffer.size;
}

void ZstdCompress::consumeOutput(uint8_t * & output, std::size_t & len)
{
  // How much data in the compressor state?
  output = reinterpret_cast<uint8_t *>(mOutputBuffer.dst);
  len = mOutputBuffer.pos;
  resetOutputBuffer();
}

void ZstdCompress::resetOutputBuffer()
{
  mOutputBuffer.dst = mOutputStart;
  mOutputBuffer.size = std::distance(mOutputStart, mOutputEnd);
  mOutputBuffer.pos = 0;
}
