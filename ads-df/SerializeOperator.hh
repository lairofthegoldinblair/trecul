#ifndef __SERIALIZEOPERATOR_H__
#define __SERIALIZEOPERATOR_H__

#include "LogicalOperator.hh"
#include "RuntimePlan.hh"
#include "RuntimeOperator.hh"
#include "StreamBufferBlock.hh"

class LogicalDeserialize : public LogicalOperator
{
private:
  TreculFreeOperation * mInputFree;
  const RecordType * mRecordType;
  TreculRecordDeserialize * mDeserialization;
  TreculFreeOperation * mOutputFree;
public:
  LogicalDeserialize();
  ~LogicalDeserialize();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeDeserializeOperatorType : public RuntimeOperatorType
{
  friend class RuntimeDeserializeOperator;
private:
  TreculSerializationStateFactory mSerializationStateFactory;
  TreculFunctionReference mDeserializeRef;
  TreculRecordDeserializeRuntime mDeserialize;
  RecordTypeMalloc mOutputMalloc;
  TreculFunctionReference mOutputFreeRef;
  TreculRecordFreeRuntime mOutputFree;
  TreculFunctionReference mStreamBufferFreeRef;
  TreculRecordFreeRuntime mStreamBufferFree;
  StreamBufferBlock mStreamBlock;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mSerializationStateFactory);
    ar & BOOST_SERIALIZATION_NVP(mDeserializeRef);
    ar & BOOST_SERIALIZATION_NVP(mOutputMalloc);
    ar & BOOST_SERIALIZATION_NVP(mOutputFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mStreamBufferFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mStreamBlock);    
  }
  RuntimeDeserializeOperatorType()
  {
  }
public:
  RuntimeDeserializeOperatorType(const RecordType * streamBufferTy,
                                 const TreculFreeOperation & streamBufferFreeFunctor,
                                 const RecordType * recordType,
                                 const TreculRecordDeserialize & deserialize,
                                 const TreculFreeOperation & outputFreeFunctor);
  ~RuntimeDeserializeOperatorType();
  void loadFunctions(TreculModule & m) override
  {
    mDeserialize = m.getFunction<TreculRecordDeserializeRuntime>(mDeserializeRef);
    mOutputFree = m.getFunction<TreculRecordFreeRuntime>(mOutputFreeRef);
    mStreamBufferFree = m.getFunction<TreculRecordFreeRuntime>(mStreamBufferFreeRef);
  }  
  RuntimeOperator * create(RuntimeOperator::Services & s) const;  
};



#endif
