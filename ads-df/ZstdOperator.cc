#include "ZstdOperator.hh"

RuntimeZstdcatOperator::RuntimeZstdcatOperator(RuntimeOperator::Services& services, 
                                               const RuntimeGunzipOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeGunzipOperatorType>(services, opType),
  mStream(nullptr),
  mLastRet(0)
{
}

RuntimeZstdcatOperator::~RuntimeZstdcatOperator()
{
  if (nullptr != mStream) {
    ::ZSTD_freeDStream(mStream);
  }
}

/**
 * intialize.
 */
void RuntimeZstdcatOperator::start()
{
  mInputBuffer.src = nullptr;
  mInputBuffer.size = 0;
  mInputBuffer.pos = 0;
  mStream = ::ZSTD_createDStream();
  ::ZSTD_initDStream(mStream);
  mState = START;
  onEvent(NULL);
}

void RuntimeZstdcatOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      // Fill up a buffer with uncompressed data
      BOOST_ASSERT(mOutput == RecordBuffer());
      // if (usesExternalBuffers()) {
      //   // In this mode we get a buffer from an input;
      //   // often fed by the consumer of this operator's output
      //   requestRead(1);
      //   mState = GET_BUFFER;
      //   return;
      // case GET_BUFFER:
      //   read(port, mOutput);
      // } else {
      // In this mode, we create the buffer
      mOutput = getMyOperatorType().mMalloc.malloc();
      BOOST_ASSERT(0 == getMyOperatorType().mStreamBlock.getSize(mOutput));
      BOOST_ASSERT(getMyOperatorType().mStreamBlock.begin(mOutput) == getMyOperatorType().mStreamBlock.end(mOutput));
      // }
      mOutputBuffer.dst = getMyOperatorType().mStreamBlock.end(mOutput);
      mOutputBuffer.size = getMyOperatorType().mStreamBlock.capacity() -
        getMyOperatorType().mStreamBlock.getSize(mOutput);
      mOutputBuffer.pos = 0;
      
      do {
        if (mInputBuffer.size == mInputBuffer.pos) {
          if (mInput != RecordBuffer()) {
            getMyOperatorType().mFree.free(mInput);
            mInput = RecordBuffer();
          }
          requestRead(0);
          mState = READ;
          return;
        case READ:
          BOOST_ASSERT(mInput == RecordBuffer());
          read(port, mInput);
          if (mInput == RecordBuffer()) {
            mInputBuffer.src = nullptr;
            mInputBuffer.size = 0;
            mInputBuffer.pos = 0;
            if (0 != mLastRet) {
              mLastRet = ::ZSTD_decompressStream(mStream, &mOutputBuffer, &mInputBuffer);
              if (ZSTD_isError(mLastRet)) {
                throw std::runtime_error((boost::format("Error decompressing zst data: %1%") % ZSTD_getErrorName(mLastRet)).str());
              }
            }
            // Send out any decompressed data
            if (0 != mOutputBuffer.pos) {
              getMyOperatorType().mStreamBlock.setSize(mOutputBuffer.pos, mOutput);
              mState = WRITE_LAST;
              requestWrite(0);
              return;
            case WRITE_LAST:
              // Flush always; these are big chunks of memory
              write(port, mOutput, true);
              mOutput = RecordBuffer();
            } else {
              getMyOperatorType().mFree.free(mOutput);
            }
            // Close up shop
            mState = WRITE_EOF;
            requestWrite(0);
            return;
          case WRITE_EOF:
            // Flush always; these are big chunks of memory
            write(port, RecordBuffer(), true);
            return;
          }
          mInputBuffer.size = getMyOperatorType().mStreamBlock.getSize(mInput);
          mInputBuffer.src = getMyOperatorType().mStreamBlock.begin(mInput);
          mInputBuffer.pos = 0;
          if (mInputBuffer.size == mInputBuffer.pos) {
            // We actually got a zero read without being at EOF
            throw std::runtime_error("Error reading compressed file");
          }
        }
        {
          mLastRet = ::ZSTD_decompressStream(mStream, &mOutputBuffer, &mInputBuffer);
          if (ZSTD_isError(mLastRet)) {
            throw std::runtime_error((boost::format("Error decompressing zst data: %1%") % ZSTD_getErrorName(mLastRet)).str());
          }
          if (mInputBuffer.size == mInputBuffer.pos) {
            // We've exhausted input.  Try to get more next time
            // around.
            // TODO: Send buffer back to producer if we must
            getMyOperatorType().mFree.free(mInput);
            mInput = RecordBuffer();
          }
          if (mOutputBuffer.size == mOutputBuffer.pos) {
            break;
          } else {
            // If there wasn't a problem, then either mStream.avail_in == 0 ||
            // mStream.avail_out == 0.
            if (mInputBuffer.size != mInputBuffer.pos) {
              throw std::runtime_error((boost::format("Error decompressing file: ZSTD_decompressStream returned %1%") %
                                        ZSTD_getErrorName(mLastRet)).str());
            }
            // Exhausted input but still have some space 
            // output; not a problem just keep going.
          }
        }

      } while(true);
      // Done trying to read.
      BOOST_ASSERT (mOutputBuffer.size == mOutputBuffer.pos);
      getMyOperatorType().mStreamBlock.setSize(mOutputBuffer.pos, mOutput);
      // Flush always and write through to
      // avoid local queue; these are big chunks of memory
      mState = WRITE;
      requestWriteThrough(0);
      return;
    case WRITE:
      write(port, mOutput, true);
      mOutput = RecordBuffer();
    }
  }
}

void RuntimeZstdcatOperator::shutdown()
{
  if (nullptr != mStream) {
    ::ZSTD_freeDStream(mStream);
    mStream = nullptr;
  }
}
