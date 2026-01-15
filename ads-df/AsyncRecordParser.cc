#include <boost/algorithm/string/trim.hpp>
#include <boost/tokenizer.hpp>
#include "AsyncRecordParser.hh"
#include "FileSystem.hh"

ImportDateType::array_type ImportDateType::sizes = { 4, 2, 2 };

ImportDatetimeType::array_type ImportDatetimeType::sizes = { 4, 2, 2, 2, 2, 2 };

void ImporterSpec::createDefaultImport(const RecordType * recordType,
				       const RecordType * baseRecordType,
				       char fieldDelim,
				       char recordDelim,
				       std::vector<ImporterSpec*>& importers)
{
  typedef FieldImporter field_importer_type;
  // Apply default rules for inferring physical layout from
  // logical record definition.
  for(RecordType::const_member_iterator mit = baseRecordType->begin_members();
      mit != baseRecordType->end_members();
      ++mit) {
    // TODO: Last field allow either record or field terminator so that
    // we can support ignoring trailing fields.
    char delim = mit+1 == baseRecordType->end_members() ? recordDelim : fieldDelim;
    if (recordType->hasMember(mit->GetName())) {
      const RecordMember& member(recordType->getMember(mit->GetName()));
      FieldAddress offset = recordType->getMemberOffset(mit->GetName());
      // Get the importer to set up.
      ImporterSpec * spec;
      // Dispatch on in-memory type.
      switch(member.GetType()->GetEnum()) {
      case FieldType::VARCHAR:
	spec = new ImportOptionalBufferSpec<ImportVarcharType>(offset,
                                                               delim,
                                                               member.GetType()->isNullable());
	break;
      case FieldType::CHAR:
	spec = new ImportFixedLengthStringSpec(offset, 
					       member.GetType()->GetSize(),
                                               delim,
					       member.GetType()->isNullable());
	break;
      case FieldType::BIGDECIMAL:
	spec = new ImportWithBufferSpec<ImportDecimalType>(offset,
                                                           delim,
                                                            member.GetType()->isNullable());
	break;
      case FieldType::INT8:
	spec = new ImportDecimalIntegerSpec<Int8Type>(offset,
                                                      delim,
                                                      member.GetType()->isNullable());
	break;
      case FieldType::INT16:
	spec = new ImportDecimalIntegerSpec<Int16Type>(offset,
                                                       delim,
                                                       member.GetType()->isNullable());
	break;
      case FieldType::INT32:
	spec = new ImportDecimalIntegerSpec<Int32Type>(offset,
                                                       delim,
                                                       member.GetType()->isNullable());
	break;
      case FieldType::INT64:
	spec = new ImportDecimalIntegerSpec<Int64Type>(offset,
                                                       delim,
                                                       member.GetType()->isNullable());
	break;
      case FieldType::FLOAT:
	spec = new ImportOptionalBufferSpec<ImportFloatType>(offset,
                                                             delim,
                                                             member.GetType()->isNullable());
      	break;
      case FieldType::DOUBLE:
	spec = new ImportOptionalBufferSpec<ImportDoubleType>(offset,
                                                              delim,
                                                              member.GetType()->isNullable());
      	break;
      case FieldType::IPV4:
	spec = new ImportOptionalBufferSpec<ImportIPv4Type>(offset,
                                                            delim,
                                                            member.GetType()->isNullable());
      	break;
      case FieldType::CIDRV4:
	spec = new ImportOptionalBufferSpec<ImportCIDRv4Type>(offset,
                                                              delim,
                                                              member.GetType()->isNullable());
      	break;
      case FieldType::IPV6:
	spec = new ImportOptionalBufferSpec<ImportIPv6Type>(offset,
                                                            delim,
                                                             member.GetType()->isNullable());
      	break;
      case FieldType::CIDRV6:
	spec = new ImportOptionalBufferSpec<ImportCIDRv6Type>(offset,
                                                              delim,
                                                              member.GetType()->isNullable());
      	break;
      case FieldType::DATETIME:
	spec = new ImportDefaultDatetimeSpec<ImportDatetimeType>(offset,
                                                                 member.GetType()->isNullable());
	break;
      case FieldType::DATE:
	spec = new ImportDefaultDatetimeSpec<ImportDateType>(offset,
                                                             member.GetType()->isNullable());
	break;
      case FieldType::FUNCTION:
	throw std::runtime_error("Importing function types not supported");
	break;
      default:
	throw std::runtime_error("Importing unknown field type");
      }
      importers.push_back(spec);
    }
    importers.push_back(new ConsumeTerminatedStringSpec(delim));
  }
  // Optimize this a bit.  Iterate backward to find the
  // last importer that generates a value (not just consume).
  // Make one more importer consuming till the end of the line.
  std::size_t sz = importers.size();
  for(std::size_t i=1; i<=sz; ++i) {
    if (!importers[sz-i]->isConsumeOnly()) {
      if (i > 2) {
	// Last generator is at sz - i.  So make the last
	// element a consumer at sz - i + 1.  Thus new size
	// is sz-i+2.
	importers.resize(sz - i + 2);
	delete importers[sz - i + 1];
	importers[sz - i + 1] = new ConsumeTerminatedStringSpec(recordDelim);
      }
      break;
    }
  }
}

ConsumeTerminatedString::ConsumeTerminatedString(uint8_t term)
  :
  mState(START),
  mTerm(term)
{
}

ParserState ConsumeTerminatedString::import(AsyncDataBlock& source, RecordBuffer target) 
{
  switch(mState) {
    while(true) {      
      if (0 == source.size()) {
	mState = READ;
	return ParserState::exhausted();
      case READ:
	if (0 == source.size()) {
	  return ParserState::error(-1);
	}
      }
      if (importInternal(source, target)) {
	mState = START;
	return ParserState::success();
      case START:
	;
      }
    }
  }
  return ParserState::error(-2);
}

ImportFixedLengthString::ImportFixedLengthString(const FieldAddress& targetOffset,
						 int32_t sz)
  :
  mTargetOffset(targetOffset),
  mState(START),
  mSize(sz)
{
}

LogicalAsyncParser::LogicalAsyncParser()
  :
  LogicalOperator(1,1,1,1),
  mInputStreamFree(nullptr),
  mFormat(nullptr),
  mMode("text"),
  mSkipHeader(false),
  mFieldSeparator('\t'),
  mRecordSeparator('\n')
{
}

LogicalAsyncParser::~LogicalAsyncParser()
{
  delete mInputStreamFree;
}

void LogicalAsyncParser::check(PlanCheckContext& ctxt)
{
  const LogicalOperatorParam * formatParam=NULL;
  std::vector<std::string> referenced;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("comment")) {
	mCommentLine = getStringValue(ctxt, *it);
      } else if (it->equals("fieldseparator")) {
	std::string tmp = getStringValue(ctxt, *it);
	if (tmp.size() == 1) {
	  mFieldSeparator = tmp[0];
	} else if (boost::algorithm::equals(tmp, "\\t")) {
	  mFieldSeparator = '\t';
	} else if (boost::algorithm::equals(tmp, "\\n")) {
	  mFieldSeparator = '\n';
	} else {
	  ctxt.logError(*this, "unsupported field separator");
	}
      } else if (it->equals("recordseparator")) {
	std::string tmp = getStringValue(ctxt, *it);
	if (tmp.size() == 1) {
	  mRecordSeparator = tmp[0];
	} else if (boost::algorithm::equals(tmp, "\\t")) {
	  mRecordSeparator = '\t';
	} else if (boost::algorithm::equals(tmp, "\\n")) {
	  mRecordSeparator = '\n';
	} else {
	  ctxt.logError(*this, "unsupported record separator");
	}
      } else if (it->equals("format")) {
	mStringFormat = getStringValue(ctxt, *it);
	formatParam = &*it;
      } else if (it->equals("formatfile")) {
	std::string filename(getStringValue(ctxt, *it));
	mStringFormat = FileSystem::readFile(filename);
	formatParam = &*it;
      } else if (it->equals("mode")) {
	mMode = getStringValue(ctxt, *it);
      } else if (it->equals("output")) {
	std::string str = boost::get<std::string>(it->Value);
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(",");
	tokenizer tok(str, sep);
	for(tokenizer::iterator tokIt = tok.begin();
	    tokIt != tok.end();
	    ++tokIt) {
	  // TODO: Validate that these are valid field names.
	 referenced.push_back(boost::trim_copy(*tokIt));
	}
      } else if (it->equals("skipheader")) {
	mSkipHeader = getBooleanValue(ctxt, *it);
      } else {
	checkDefaultParam(ctxt, *it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }

  if (!boost::algorithm::iequals("binary", mMode) &&
      !boost::algorithm::iequals("text", mMode)) {
    ctxt.logError(*this, "mode parameter must be \"text\" or \"binary\"");
  }

  if (boost::algorithm::iequals("binary", mMode) &&
      mCommentLine.size()) {
    ctxt.logError(*this, "comment only supported when mode is \"text\"");
  }

  if (boost::algorithm::iequals("binary", mMode) &&
      referenced.size()) {
    ctxt.logError(*this, "output only supported when mode is \"text\"");
  }

  // We must have format parameter.
  if (NULL == formatParam || 0==mStringFormat.size()) {
    ctxt.logError(*this, "Must specify format argument");
  } else {
    try {
      IQLRecordTypeBuilder bld(ctxt, mStringFormat, false);
      mFormat = bld.getProduct();
      // Default referenced is all columns.
      if (0 == referenced.size()) {
	for(RecordType::const_member_iterator m = mFormat->begin_members(),
	      e = mFormat->end_members(); m != e; ++m) {
	  referenced.push_back(m->GetName());
	}
      }
      
      getOutput(0)->setRecordType(RecordType::get(ctxt, mFormat, 
						  referenced.begin(), 
						  referenced.end()));
    } catch(std::exception& ex) {
      ctxt.logError(*this, *formatParam, ex.what());
    }
  }

  mInputStreamFree = new TreculFreeOperation(ctxt.getCodeGenerator(), getInput(0)->getRecordType());
}

void LogicalAsyncParser::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = NULL;
  if(boost::algorithm::iequals("binary", mMode)) {
    throw std::runtime_error("Binary mode not supported yet");
  } else {
    GenericAsyncParserOperatorType * tot = 
      new GenericAsyncParserOperatorType(mFieldSeparator,
					 mRecordSeparator,
					 getInput(0)->getRecordType(),
                                         *mInputStreamFree,
					 getOutput(0)->getRecordType(),
					 mFormat,
					 mCommentLine.c_str());
    tot->setSkipHeader(mSkipHeader);
    opType = tot;
  }
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

GenericRecordImporter::GenericRecordImporter(ImporterSpec * spec)
  :
  mImporterObjects(NULL)
{
  // Convert the import specs into objects and delegates
  // for fast invocation (that avoids virtual functions/vtable
  // lookup).
    
  // We want all importer state to be packed into a small
  // region of memory.
  mImporterObjects = new uint8_t [spec->objectSize()];
  mImporters.push_back(spec->makeObject(mImporterObjects));
}

GenericRecordImporter::GenericRecordImporter(std::vector<ImporterSpec*>::const_iterator begin,
					     std::vector<ImporterSpec*>::const_iterator end)
  :
  mImporterObjects(NULL)
{
  // Convert the import specs into objects and delegates
  // for fast invocation (that avoids virtual functions/vtable
  // lookup).
    
  // We want all importer state to be packed into a small
  // region of memory.
  std::size_t sz = 0;
  for(std::vector<ImporterSpec*>::const_iterator it = begin;
      it != end; ++it) {
    sz += sz % (*it)->objectAlignment();
    sz += (*it)->objectSize();      
  }
  mImporterObjects = new uint8_t [sz];
  sz = 0;
  for(std::vector<ImporterSpec*>::const_iterator it = begin;
      it != end; ++it) {
    sz += sz % (*it)->objectAlignment();
    mImporters.push_back((*it)->makeObject(mImporterObjects + sz));
    sz += (*it)->objectSize();      
  }
}

GenericRecordImporter::~GenericRecordImporter()
{
  delete [] mImporterObjects;
}

class GenericAsyncParserOperator : public RuntimeOperator
{
private:
  typedef GenericAsyncParserOperatorType operator_type;

  enum State { START, READ, READ_NEW_RECORD, READ_SKIP, WRITE, WRITE_EOF };
  State mState;

  // // The importer objects themselves
  // uint8_t * mImporterObjects;
  // // Importer delegates
  // std::vector<ImporterDelegate> mImporters;
  // // The field am I currently importing
  // std::vector<ImporterDelegate>::iterator mIt;
  // Importer objects and delegates
  GenericRecordImporter mImporters;
  // The field I am currently importing
  GenericRecordImporter::iterator mIt;
  // Skip importer for reading and throwing away lines
  GenericRecordImporter mSkipImporter;
  // Input buffer for the file.
  AsyncDataBlock mInputBuffer;
  // Status of last call to parser
  ParserState mParserState;
  // Stream block buffer containing data
  RecordBuffer mInput;
  // Record buffer I am importing into
  RecordBuffer mOutput;
  // Records imported
  uint64_t mRecordsImported;
  // Have we run the skip importer
  bool mSkipped;

  const operator_type & getLogParserType()
  {
    return *static_cast<const operator_type *>(&getOperatorType());
  }

public:
  GenericAsyncParserOperator(RuntimeOperator::Services& services, 
			     const RuntimeOperatorType& opType)
    :
    RuntimeOperator(services, opType),
    mImporters(getLogParserType().mImporters.begin(),
	       getLogParserType().mImporters.end()),
    mSkipImporter(getLogParserType().mSkipImporter),
    mRecordsImported(0),
    mSkipped(false)
  {
    // std::size_t sz = getLogParserType().mCommentLine.size();
    // if (sz > std::numeric_limits<int32_t>::max()) {
    //   throw std::runtime_error("Comment line too large");
    // }
    // mCommentLineSz = (int32_t) sz;

    // // Convert the import specs into objects and delegates
    // // for fast invocation (that avoids virtual functions/vtable
    // // lookup).
    
    // // We want all importer state to be packed into a small
    // // region of memory.
    // std::size_t sz = 0;
    // for(operator_type::field_importer_const_iterator it = 
    // 	  getLogParserType().mImporters.begin();
    // 	it != getLogParserType().mImporters.end(); ++it) {
    //   sz += sz % (*it)->objectAlignment();
    //   sz += (*it)->objectSize();      
    // }
    // mImporterObjects = new uint8_t [sz];
    // sz = 0;
    // for(operator_type::field_importer_const_iterator it = 
    // 	  getLogParserType().mImporters.begin();
    // 	it != getLogParserType().mImporters.end(); ++it) {
    //   sz += sz % (*it)->objectAlignment();
    //   mImporters.push_back((*it)->makeObject(mImporterObjects + sz));
    //   sz += (*it)->objectSize();      
    // }
  }

  ~GenericAsyncParserOperator()
  {
    // delete [] mImporterObjects;
  }

  /**
   * intialize.
   */
  void start()
  {
    mState = START;
    mRecordsImported = 0;
    mSkipped = false;
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      // Read all of the record in the file.
      while(true) {
	// If empty read a block; it is OK to exhaust a file
	// here but not while in the middle of a record, so 
	// we make a separate read attempt here.
	if (0 == mInputBuffer.size()) {
	  requestRead(0);
	  mState = READ_NEW_RECORD;
	  return;
	case READ_NEW_RECORD:
	  {
	    if (mInput != RecordBuffer()) {
	      getLogParserType().mFree.free(mInput);
	    }
	    read(port, mInput);
	    if (mInput == RecordBuffer()) {
	      // Done
	      break;
	    }
	    int32_t bytesRead = getLogParserType().mStreamBlock.getSize(mInput);
	    if (0 == bytesRead) {
	      // ASSERT here instead of trying again?
	      getLogParserType().mFree.free(mInput);
	      mInput = RecordBuffer();
	      continue;
	    }
	    uint8_t * imp = 
	      (uint8_t *) getLogParserType().mStreamBlock.begin(mInput);
	    mInputBuffer = AsyncDataBlock(imp, bytesRead);
	  }
	}

        if (!mSkipped && getLogParserType().mSkipHeader) {
          for(mIt = mSkipImporter.begin();
              mIt != mSkipImporter.end();
              ++mIt) {
            while(true) {
              mParserState = (*mIt)(mInputBuffer, mOutput);
              if (mParserState.isSuccess()) {
                // Successful parse
                break;
              } else if (mParserState.isExhausted()) {
                BOOST_ASSERT(0 == mInputBuffer.size());
                do {
                  requestRead(0);
                  mState = READ_SKIP;
                  return;
                  case READ_SKIP:
                    {
                      if (mInput != RecordBuffer()) {
                        getLogParserType().mFree.free(mInput);
                      }
                      read(port, mInput);
                      if (mInput == RecordBuffer()) {
                        throw std::runtime_error("Parse Error in record: "
                                                 "end of file reached");
                      }
                      int32_t bytesRead = getLogParserType().mStreamBlock.getSize(mInput);
                      if (0 == bytesRead) {
                        // ASSERT here instead of trying again?
                        getLogParserType().mFree.free(mInput);
                        mInput = RecordBuffer();
                        continue;
                      }
                      uint8_t * imp = 
                        (uint8_t *) getLogParserType().mStreamBlock.begin(mInput);
                      mInputBuffer = AsyncDataBlock(imp, bytesRead);
                    }
                } while(false);	      
              } else {
                // Bad record
                throw std::runtime_error("Parse Error in record");
              }
            }
            mSkipped = true;
          }
        }
        
	// This is our actual record.
	mOutput = getLogParserType().mMalloc.malloc();
	for(mIt = mImporters.begin();
	    mIt != mImporters.end();
	    ++mIt) {
	  while(true) {
	    mParserState = (*mIt)(mInputBuffer, mOutput);
	    if (mParserState.isSuccess()) {
	      // Successful parse
	      break;
	    } else if (mParserState.isExhausted()) {
	      BOOST_ASSERT(0 == mInputBuffer.size());
	      do {
		requestRead(0);
		mState = READ;
		return;
	      case READ:
		{
		  if (mInput != RecordBuffer()) {
		    getLogParserType().mFree.free(mInput);
		  }
		  read(port, mInput);
		  if (mInput == RecordBuffer()) {
		    throw std::runtime_error("Parse Error in record: "
					     "end of file reached");
		  }
		  int32_t bytesRead = getLogParserType().mStreamBlock.getSize(mInput);
		  if (0 == bytesRead) {
		    // ASSERT here instead of trying again?
		    getLogParserType().mFree.free(mInput);
		    mInput = RecordBuffer();
		    continue;
		  }
		  uint8_t * imp = 
		    (uint8_t *) getLogParserType().mStreamBlock.begin(mInput);
		  mInputBuffer = AsyncDataBlock(imp, bytesRead);
		}
	      } while(false);	      
	    } else {
	      // Bad record
	      throw std::runtime_error("Parse Error in record");
	    }
	  }
	}
	// Done cause we had good record
	mRecordsImported += 1;
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mOutput, false);
      }
      // Done with the last file so output EOS.
      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer::create(), true);
      return;
    }
  }

  void shutdown()
  {
  }
};

GenericAsyncParserOperatorType::GenericAsyncParserOperatorType(char fieldSeparator,
							       char recordSeparator,
							       const RecordType * inputStreamType,
                                                               const TreculFreeOperation & inputStreamTypeFreeFunctor,
							       const RecordType * recordType,
							       const RecordType * baseRecordType,
							       const char * commentLine)
  :
  RuntimeOperatorType("GenericAsyncParserOperatorType"),
  mSkipImporter(NULL),
  mStreamBlock(inputStreamType),
  mStreamMalloc(inputStreamType->getMalloc()),
  mFreeRef(inputStreamTypeFreeFunctor.getReference()),
  mRecordType(recordType),
  mSkipHeader(false),
  mCommentLine(commentLine)
{
  mMalloc = mRecordType->getMalloc();

  // Records have tab delimited fields and newline delimited records
  ImporterSpec::createDefaultImport(recordType, 
				    baseRecordType ? baseRecordType : recordType,
				    fieldSeparator, 
				    recordSeparator, mImporters);
    
  // To skip a line we just parse to newline and discard.
  // We need this when syncing to the middle of a file.
  mSkipImporter = new ConsumeTerminatedStringSpec(recordSeparator);
}

GenericAsyncParserOperatorType::~GenericAsyncParserOperatorType()
{
  for(field_importer_const_iterator it = mImporters.begin(); 
      it != mImporters.end(); ++it) {
    delete *it;
  }
  delete mSkipImporter;
}

RuntimeOperator * GenericAsyncParserOperatorType::create(RuntimeOperator::Services & services) const
{
  return new GenericAsyncParserOperator(services, *this);
}

LogicalBlockRead::LogicalBlockRead()
  :
  LogicalOperator(0,0,1,1),
  mCompressionType(CompressionType::Gzip()),
  mFileFormat(TableFileFormat::Delimited()),
  mStreamBlock(nullptr),
  mStreamBlockFree(nullptr),
  mBufferCapacity(64*1024),
  mBucketed(false)
{
}

LogicalBlockRead::~LogicalBlockRead()
{
  delete mStreamBlockFree;
}

void LogicalBlockRead::check(PlanCheckContext& ctxt)
{
  const LogicalOperatorParam * formatParam=NULL;
  std::vector<std::string> referenced;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("file")) {
	mFile = getStringValue(ctxt, *it);
      } else if (it->equals("bucketed")) {
	mBucketed = getBooleanValue(ctxt, *it);
      } else if (it->equals("blocksize")) {
	mBufferCapacity = getInt32Value(ctxt, *it);
      } else if (it->equals("compression")) {
        std::error_code ec;
        auto val = getStringValue(ctxt, *it);
        mCompressionType = CompressionType::fromString(val, ec);
        if (ec) {
          ctxt.logError(*this, "compression parameter must be \"none\", \"gzip\" or \"zstd\"");
        }
      } else if (it->equals("mode")) {
        std::string val = getStringValue(ctxt, *it);
        if (boost::algorithm::iequals("binary", val))  {
          mFileFormat = TableFileFormat::Binary();
        } else if (boost::algorithm::iequals("text", val))  {
          mFileFormat = TableFileFormat::Delimited();
        } else {
          ctxt.logError(*this, "mode parameter must be \"text\" or \"binary\"");
        }
      } else {
	checkDefaultParam(ctxt, *it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }

  std::vector<RecordMember> members;
  members.push_back(RecordMember("size", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("buffer", CharType::Get(ctxt, mBufferCapacity)));  
  mStreamBlock = RecordType::get(ctxt, members);
  mStreamBlockFree = new TreculFreeOperation(ctxt.getCodeGenerator(), mStreamBlock);
  getOutput(0)->setRecordType(mStreamBlock);
}

void LogicalBlockRead::create(class RuntimePlanBuilder& plan)
{
  typedef GenericAsyncReadOperatorType<ExplicitChunkStrategy> text_op_type;
  typedef GenericAsyncReadOperatorType<SerialChunkStrategy> serial_op_type;

  RuntimeOperatorType * opType = NULL;
  if (mBucketed) {
    PathPtr p = Path::get(mFile);
    // Default to a local file URI.
    if (p->getUri()->getScheme().size() == 0)
      p = Path::get("file://" + mFile);
    serial_op_type * sot = new serial_op_type(p,
                                              serial_op_type::chunk_strategy_type(mCompressionType, mFileFormat),
    					      getOutput(0)->getRecordType(),
                                              *mStreamBlockFree);
    opType = sot;
  } else {
    text_op_type * tot = new text_op_type(mFile,
                                          text_op_type::chunk_strategy_type(),
					  getOutput(0)->getRecordType(),
                                          *mStreamBlockFree);
    opType = tot;
  }
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);  
}
