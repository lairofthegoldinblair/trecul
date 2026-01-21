#ifndef __GZIPOPERATOR_H__
#define __GZIPOPERATOR_H__

#include "CompressionType.hh"
#include "LogicalOperator.hh"
#include "RuntimePlan.hh"
#include "RuntimeOperator.hh"
#include "StreamBufferBlock.hh"

class LogicalGunzip : public LogicalOperator
{
private:
  const RecordType * mStreamBlock;
  TreculFreeOperation * mFree;
  CompressionType mCompressionType;
  int32_t mBufferCapacity;
public:
  LogicalGunzip(const CompressionType & compressionType);
  ~LogicalGunzip();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeGunzipOperatorType : public RuntimeOperatorType
{
  friend class RuntimeGunzipOperator;
  friend class RuntimeZstdcatOperator;
private:
  RecordTypeMalloc mMalloc;
  TreculFunctionReference mFreeRef;
  TreculRecordFreeRuntime mFree;
  CompressionType mCompressionType;
  StreamBufferBlock mStreamBlock;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mCompressionType);
    ar & BOOST_SERIALIZATION_NVP(mStreamBlock);    
  }
  RuntimeGunzipOperatorType()
  {
  }
public:
  RuntimeGunzipOperatorType(const RecordType * bufferTy,
                            const TreculFreeOperation & freeFunctor,
                            const CompressionType & compressionType);
  ~RuntimeGunzipOperatorType();
  void loadFunctions(TreculModule & m) override
  {
    mFree = m.getFunction<TreculRecordFreeRuntime>(mFreeRef);
  }  
  RuntimeOperator * create(RuntimeOperator::Services & s) const;  
};



#endif
