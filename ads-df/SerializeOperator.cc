#include "FileSystem.hh"
#include "SerializeOperator.hh"

LogicalDeserialize::LogicalDeserialize()
  :
  LogicalOperator(1,1,1,1),
  mInputFree(nullptr),
  mRecordType(nullptr),
  mDeserialization(nullptr),
  mOutputFree(nullptr)
{
}

LogicalDeserialize::~LogicalDeserialize()
{
  delete mDeserialization;
  delete mOutputFree;
  delete mInputFree;
}

void LogicalDeserialize::check(PlanCheckContext& ctxt)
{
  std::string stringFormat;
  const LogicalOperatorParam * formatParam=NULL;
  
  if (!StreamBufferBlock::isStreamBufferType(getInput(0)->getRecordType())) {
    ctxt.logError(*this, "Invalid type on input; must be stream type");
  }

  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("format")) {
	stringFormat = getStringValue(ctxt, *it);
	formatParam = &*it;
      } else if (it->equals("formatfile")) {
	std::string filename(getStringValue(ctxt, *it));
        stringFormat = FileSystem::readFile(filename);
	formatParam = &*it;
      } else {
	checkDefaultParam(ctxt, *it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }
  // We must have format parameter.
  if (NULL == formatParam || 0==stringFormat.size()) {
    ctxt.logError(*this, "Must specify format or formatfile argument");
  } else {
    try {
      IQLRecordTypeBuilder bld(ctxt, stringFormat, false);
      mRecordType = bld.getProduct();
      mDeserialization = new TreculRecordDeserialize(ctxt.getCodeGenerator(), mRecordType);
      mOutputFree = new TreculFreeOperation(ctxt.getCodeGenerator(), mRecordType);
      getOutput(0)->setRecordType(mRecordType);
    } catch(std::exception& ex) {
      ctxt.logError(*this, *formatParam, ex.what());
    }
  }
  mInputFree = new TreculFreeOperation(ctxt.getCodeGenerator(), getInput(0)->getRecordType());
}

void LogicalDeserialize::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeDeserializeOperatorType(getInput(0)->getRecordType(), *mInputFree, mRecordType, *mDeserialization, *mOutputFree);
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

class RuntimeDeserializeOperator : public RuntimeOperatorBase<RuntimeDeserializeOperatorType>
{
private:
  typedef RuntimeDeserializeOperatorType operator_type;

  enum State { START, /*GET_BUFFER,*/ READ, WRITE, WRITE_EOF };
  State mState;
  RecordBuffer mInput;
  RecordBuffer mOutput;
  uint8_t * mBufferIt;
  uint8_t * mBufferEnd;
  InterpreterContext * mRuntimeContext;
  TreculSerializationStateFactory::pointer mSerializationState;  

  bool deserialize()
  {
    const char * tmp = reinterpret_cast<char *>(mBufferIt);
    bool ret = getMyOperatorType().mDeserialize.deserialize(mOutput, tmp, reinterpret_cast<char *>(mBufferEnd), *mSerializationState, mRuntimeContext);
    mBufferIt = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(tmp));
    return ret;
  }

public:
  RuntimeDeserializeOperator(RuntimeOperator::Services& services, 
                             const RuntimeDeserializeOperatorType& opType)
    :
    RuntimeOperatorBase<RuntimeDeserializeOperatorType>(services, opType),
    mRuntimeContext(new InterpreterContext()),
    mSerializationState(opType.mSerializationStateFactory.create())
  {
  }

  ~RuntimeDeserializeOperator()
  {
    delete mRuntimeContext;
  }

  /**
   * intialize.
   */
  void start()
  {
    mState = START;
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      while(true) {

        requestRead(0);
        mState = READ;
        return;
      case READ:
        read(port, mInput);

        if (RecordBuffer::isEOS(mInput)) {
          break;
        }

        mBufferIt = getMyOperatorType().mStreamBlock.begin(mInput);
        mBufferEnd = getMyOperatorType().mStreamBlock.end(mInput);
        // Deserialize records, write them and return buffer
        // TODO: Handling EOS
        while(mBufferIt < mBufferEnd) {
          if (mOutput.Ptr == NULL) {
            mOutput = getMyOperatorType().mOutputMalloc.malloc();
            mSerializationState->state = 0;
          }
          if(deserialize()) {
            requestWrite(0);
            mState = WRITE;
            return;
          case WRITE:
            write(port, mOutput, false);
            mOutput = RecordBuffer(NULL);
            mSerializationState->state = 0;
          } 
        }

        getMyOperatorType().mStreamBufferFree.free(mInput);
        mInput = RecordBuffer();
      }
      // Close up shop
      if (mOutput != RecordBuffer()) {
        // This shouldn't happen should probably fail here...
        // In any case we shouldn't leak memory even if this fails.
        getMyOperatorType().mOutputFree.free(mOutput);
        mOutput = RecordBuffer();
      }
      mState = WRITE_EOF;
      requestWrite(0);
      return;
    case WRITE_EOF:
      // Flush always; these are big chunks of memory
      write(port, RecordBuffer(), true);
      return;
    }
  }

  void shutdown()
  {
  }
};

RuntimeDeserializeOperatorType::RuntimeDeserializeOperatorType(const RecordType * streamBufferTy,
                                                               const TreculFreeOperation & streamBufferFreeFunctor,
                                                               const RecordType * recordType,
                                                               const TreculRecordDeserialize & deserialize,
                                                               const TreculFreeOperation & outputFreeFunctor)
  :
  mSerializationStateFactory(recordType),
  mDeserializeRef(deserialize.getReference()),
  mOutputMalloc(recordType->getMalloc()),
  mOutputFreeRef(outputFreeFunctor.getReference()),
  mStreamBufferFreeRef(streamBufferFreeFunctor.getReference()),
  mStreamBlock(streamBufferTy)
{
}

RuntimeDeserializeOperatorType::~RuntimeDeserializeOperatorType()
{
}

RuntimeOperator * RuntimeDeserializeOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeDeserializeOperator(s, *this);    
}
