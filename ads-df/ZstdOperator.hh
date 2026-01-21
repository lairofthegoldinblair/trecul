#ifndef __ZSTDOPERATOR_H__
#define __ZSTDOPERATOR_H__

#include "zstd.h"

#include "GzipOperator.hh"

class RuntimeZstdcatOperator : public RuntimeOperatorBase<RuntimeGunzipOperatorType>
{
private:
  typedef RuntimeGunzipOperatorType operator_type;

  enum State { START, GET_BUFFER, READ, WRITE, WRITE_LAST, WRITE_EOF };
  State mState;
  RecordBuffer mInput;
  RecordBuffer mOutput;
  ZSTD_DStream * mStream;
  ZSTD_inBuffer mInputBuffer;
  ZSTD_outBuffer mOutputBuffer;
  std::size_t mLastRet;

  bool usesExternalBuffers() const
  {
    return getNumInputs()==2;
  }

public:
  RuntimeZstdcatOperator(RuntimeOperator::Services& services, 
                         const RuntimeGunzipOperatorType& opType);

  ~RuntimeZstdcatOperator();

  /**
   * intialize.
   */
  void start();

  void onEvent(RuntimePort * port);

  void shutdown();
};


#endif
