#ifndef __TRECUL_FILE_CREATION_POLICY_HH__
#define __TRECUL_FILE_CREATION_POLICY_HH__

#include <iomanip>

#include <boost/asio.hpp>

#include "CompressionType.hh"
#include "RuntimePort.hh"
#include "RuntimeOperator.hh"
#include "ServiceCompletionPort.hh"

class MultiFileCreationPolicy;
class StreamingFileCreationPolicy;

/**
 * We write data to HDFS by first writing to a temporary
 * file and then renaming that temporary file to a permanent
 * file name.
 * In a program that writes multiple files, it is crucial that
 * the files be renamed in a deterministic order.  The reason for
 * this is that multiple copies of the program may be running
 * (e.g. due to Hadoop speculative execution) and we demand that
 * exactly one of the copies succeeds.  If we do not have a deterministic
 * order of file renames then it is possible for a "deadlock"-like
 * scenario to occur in which all copies fail (think of renaming
 * a file as being equivalent to taking a write lock on a resource
 * identified by the file name).
 */
class HdfsFileCommitter
{
private:
  std::vector<std::shared_ptr<class HdfsFileRename> >mActions;
  std::string mError;
  /**
   * Number of actions that have requested commit
   */
  std::size_t mCommits;

  /*
   * Mutex to protect all member variables/methods
   */
  std::mutex mMutex;

  // TODO: Don't use a singleton here, have the dataflow
  // manage the lifetime.
  static HdfsFileCommitter * sCommitter;
  static int32_t sRefCount;
  static std::mutex sGuard;
public:  
  static HdfsFileCommitter * get();
  static void release(HdfsFileCommitter *);
  HdfsFileCommitter();
  ~HdfsFileCommitter();
  void track (PathPtr from, PathPtr to,
	      FileSystem * fileSystem);
  bool commit();
  const std::string& getError() const 
  {
    return mError;
  }
};


/**
 * This policy supports keeping multiple files open
 * for writes and a close policy that defers to the file
 * committer.
 */
template<typename _Factory>
class MultiFileCreation
{
public:
  typedef _Factory factory_type;
  typedef typename factory_type::output_file_type output_file_type;
private:
  const MultiFileCreationPolicy& mPolicy;
  PathPtr mRootUri;
  FileSystem * mGenericFileSystem;
  InterpreterContext * mRuntimeContext;
  std::map<std::string, output_file_type *> mFile;
  HdfsFileCommitter * mCommitter;
  int32_t mPartition;

  output_file_type * createFile(const std::string& filePath,
                                _Factory * factory);

  void add(const std::string& filePath, output_file_type * of)
  {
    mFile[filePath] = of;
  }
public:
  MultiFileCreation(const MultiFileCreationPolicy& policy, 
		    RuntimeOperator::Services& services);
    
  ~MultiFileCreation();

  void start(FileSystem * genericFileSystem, PathPtr rootUri,
	     _Factory * fileFactory);

  output_file_type * onRecord(RecordBuffer input,
                              _Factory * fileFactory);
    
  void close(bool flush);
    
  void commit(std::string& err);

  void setCompletionPort(ServiceCompletionFifo * )
  {
  }
};

class MultiFileCreationPolicy
{
public:
  template<typename _Factory> friend class MultiFileCreation;
  template<typename _Factory>
  struct creation_type
  {
    typedef MultiFileCreation<_Factory> type;
  };
private:
  std::string mHdfsFile;
  // Transfer to calculate any expressions in the
  // file string.
  TreculTransferReference mTransferRef;
  TreculTransferRuntime mTransfer;
  TreculFunctionReference mTransferFreeRef;
  TreculRecordFreeRuntime mTransferFree;
  FieldAddress * mTransferOutput;
  CompressionType mCompressionType;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mHdfsFile);
    ar & BOOST_SERIALIZATION_NVP(mTransferRef);
    ar & BOOST_SERIALIZATION_NVP(mTransferFreeRef);
    ar & BOOST_SERIALIZATION_NVP(mTransferOutput);
    ar & BOOST_SERIALIZATION_NVP(mCompressionType);
  }
public:
  MultiFileCreationPolicy()
    :
    mTransferOutput(NULL)
  {
  }
  MultiFileCreationPolicy(const std::string& hdfsFile,
			  const TreculTransfer * argTransfer,
                          const TreculFreeOperation * argTransferFree,
                          const CompressionType & compressionType)
    :
    mHdfsFile(hdfsFile),
    mTransferRef(nullptr != argTransfer ? argTransfer->getReference() : TreculTransferReference()),
    mTransferFreeRef(nullptr != argTransferFree ? argTransferFree->getReference() : TreculFunctionReference()),
    mTransferOutput(nullptr != argTransfer ? new FieldAddress(*argTransfer->getTarget()->begin_offsets()) : NULL),
    mCompressionType(compressionType)
  {
  }
  ~MultiFileCreationPolicy()
  {
    delete mTransferOutput;
  }
    
  bool requiresServiceCompletionPort() const
  {
    return false;
  }

  void loadFunctions(TreculModule & m)
  {
    if (!mTransferRef.empty()) {
      mTransfer = m.getTransfer<TreculTransferRuntime>(mTransferRef);
    }
    if (!mTransferFreeRef.empty()) {
      mTransferFree = m.getFunction<TreculRecordFreeRuntime>(mTransferFreeRef);
    }
  }
  const CompressionType & getCompressionType() const
  {
    return mCompressionType;
  }
};

template<typename _Factory>
typename MultiFileCreation<_Factory>::output_file_type *
MultiFileCreation<_Factory>::createFile(const std::string& filePath,
                                        _Factory * factory)
{
  // We create a temporary file name and write to that.  When
  // complete we'll rename the file.  This should make things safe
  // in case multiple copies of the operator are trying to write
  // to the same file (e.g. running in Hadoop with speculative execution).
  // The temporary file name must have the pattern serial_ddddd
  // so that it uses the appropriate block placement policy.
  std::string tmpStr = FileSystem::getTempFileName();

  std::stringstream str;
  str << filePath << "/" << tmpStr << "_serial_" << 
    std::setw(5) << std::setfill('0') << mPartition <<
    factory_type::extension();
  PathPtr tempPath = Path::get(mRootUri, str.str());
  std::stringstream permFile;
  permFile << filePath + "/serial_" << 
    std::setw(5) << std::setfill('0') << mPartition <<
    factory_type::extension();  
  if (mCommitter == NULL) {
    mCommitter = HdfsFileCommitter::get();
  }
  mCommitter->track(tempPath, 
                    Path::get(mRootUri, permFile.str()), 
                    mGenericFileSystem);
  // Call back to the factory to actually create the file
  output_file_type * of = factory->createFile(tempPath);
  add(filePath, of);
  return of;
}

template<typename _Factory>
MultiFileCreation<_Factory>::MultiFileCreation(const MultiFileCreationPolicy& policy, 
                                               RuntimeOperator::Services& services)
  :
  mPolicy(policy),
  mGenericFileSystem(NULL),
  mRuntimeContext(NULL),
  mCommitter(NULL),
  mPartition(services.getPartition())
{
  if (!!policy.mTransfer) {
    mRuntimeContext = new InterpreterContext();
  }
}
    
template<typename _Factory>
MultiFileCreation<_Factory>::~MultiFileCreation()
{
  for(typename std::map<std::string, output_file_type*>::iterator it = mFile.begin(),
        end = mFile.end(); it != end; ++it) {
    if (it->second->File) {
      // Abnormal shutdown
      it->second->File->close();
    }
  }

  if (mCommitter) {
    HdfsFileCommitter::release(mCommitter);
    mCommitter = NULL;

  }
}

template<typename _Factory>
void MultiFileCreation<_Factory>::start(FileSystem * genericFileSystem, PathPtr rootUri,
                                        _Factory * fileFactory)
{
  mGenericFileSystem = genericFileSystem;
  mRootUri = rootUri;
  // If statically defined path, create and open file here.
  if (0 != mPolicy.mHdfsFile.size()) {
    createFile(mPolicy.mHdfsFile, fileFactory);
  }
}

template<typename _Factory>
typename MultiFileCreation<_Factory>::output_file_type *
MultiFileCreation<_Factory>::onRecord(RecordBuffer input,
                                      _Factory * fileFactory)
{
  if (NULL == mRuntimeContext) {
    return mFile.begin()->second;
  } else {
    RecordBuffer output;
    mPolicy.mTransfer.execute(input, output, mRuntimeContext, false);
    std::string fileName(mPolicy.mTransferOutput->getVarcharPtr(output)->c_str());
    mPolicy.mTransferFree.free(output);
    mRuntimeContext->clear();
    typename std::map<std::string, output_file_type *>::iterator it = mFile.find(fileName);
    if (mFile.end() == it) {
      std::cout << "Creating file: " << fileName.c_str() << std::endl;
      return createFile(fileName, fileFactory);
    }
    return it->second;
  }
}
    
template<typename _Factory>
void MultiFileCreation<_Factory>::close(bool flush)
{
  for(typename std::map<std::string, output_file_type*>::iterator it = mFile.begin(),
        end = mFile.end(); it != end; ++it) {
    if (it->second->File != NULL) {
      if (flush) {
        it->second->flush();
      }
      it->second->close();
      it->second->File = NULL;
    }
  }
}
    
template<typename _Factory>
void MultiFileCreation<_Factory>::commit(std::string& err)
{
  // Put the file(s) in its final place.
  for(typename std::map<std::string, output_file_type*>::iterator it = mFile.begin(),
        end = mFile.end(); it != end; ++it) {    
    if(!mCommitter->commit()) {
      err = mCommitter->getError();
      break;
    } 
  }
}

template<typename _Factory>
class StreamingFileCreation
{
public:
  typedef _Factory factory_type;
  typedef typename factory_type::output_file_type output_file_type;
private:
  const StreamingFileCreationPolicy & mPolicy;
  PathPtr mRootUri;
  FileSystem * mGenericFileSystem;
  std::size_t mNumRecords;
  boost::asio::deadline_timer mTimer;
  ServiceCompletionFifo * mCompletionPort;
  // Path of current file as we are writing to it
  PathPtr mTempPath;
  // Final path name of file.
  PathPtr mFinalPath;
  output_file_type * mCurrentFile;

  output_file_type * createFile(const std::string& filePath,
                                factory_type * factory);

  // Timer callback
  void timeout(const boost::system::error_code& err);
public:
  StreamingFileCreation(const StreamingFileCreationPolicy& policy,
			RuntimeOperator::Services& services);
  ~StreamingFileCreation();
  void start(FileSystem * genericFileSystem, PathPtr rootUri,
	     factory_type * fileFactory);
  output_file_type * onRecord(RecordBuffer input,
			factory_type * fileFactory);
  void close(bool flush);
  void commit(std::string& err);
  void setCompletionPort(ServiceCompletionFifo * f)
  {
    mCompletionPort = f;
  }
};

/**
 * Controls the creation of write file(s).  
 * For example, do we create a single file for the entire
 * run of the operator or do we periodically cut files based on
 * time or record counts.
 */
class StreamingFileCreationPolicy
{
public:
  template <typename _Factory> friend class StreamingFileCreation;
  template<typename _Factory>
  struct creation_type
  {
    typedef StreamingFileCreation<_Factory> type;
  };
private:
  /**
   * Directory in which to create files.
   */
  std::string mBaseDir;

  /**
     il   * Max amount of time to write to a file in seconds.
     * If this is 0 then there is no time limit.
     */
  std::size_t mFileSeconds;
  
  /**
   * Max number of records to write to a file..
   * If this is 0 then there is no record limit.
   */
  std::size_t mFileRecords;

  /**
   * Compression type of files to create
   */
  CompressionType mCompressionType;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mBaseDir);
    ar & BOOST_SERIALIZATION_NVP(mFileSeconds);
    ar & BOOST_SERIALIZATION_NVP(mFileRecords);
    ar & BOOST_SERIALIZATION_NVP(mCompressionType);
  }

  StreamingFileCreationPolicy()
    :
    mFileSeconds(0),
    mFileRecords(0)
  {
  }
public:
  StreamingFileCreationPolicy(const std::string& baseDir,
                              const CompressionType & compressionType,
			      std::size_t fileSeconds=0,
			      std::size_t fileRecords=0)
    :
    mBaseDir(baseDir),
    mFileSeconds(fileSeconds),
    mFileRecords(fileRecords),
    mCompressionType(compressionType)
  {
  }
  ~StreamingFileCreationPolicy()
  {
  }
  bool requiresServiceCompletionPort() const { return true; }
  void loadFunctions(TreculModule & m)
  {
  }
  const CompressionType & getCompressionType() const
  {
    return mCompressionType;
  }
};

template<typename _Factory>
StreamingFileCreation<_Factory>::StreamingFileCreation(const StreamingFileCreationPolicy& policy,
					     RuntimeOperator::Services& services)
  :
  mPolicy(policy),
  mGenericFileSystem(NULL),
  mNumRecords(0),
  mCurrentFile(NULL),
  mTimer(services.getIOService()),
  mCompletionPort(NULL)
{
}

template<typename _Factory>
StreamingFileCreation<_Factory>::~StreamingFileCreation()
{
}

template<typename _Factory>
void StreamingFileCreation<_Factory>::timeout(const boost::system::error_code& err)
{
  // Post to operator completion port.
  if(err != boost::asio::error::operation_aborted) {
    RecordBuffer buf;
    mCompletionPort->write(buf);
  }
}

template<typename _Factory>
typename StreamingFileCreation<_Factory>::output_file_type *
StreamingFileCreation<_Factory>::createFile(const std::string& filePath,
                                            _Factory * factory)
{
  BOOST_ASSERT(mCurrentFile == NULL);

  // We create a temporary file name and write to that.  
  // Here we are not using a block placement naming scheme 
  // since we are likely executing on a single partition at this point.
  // This assumes we have a downstream splitter process that partitions
  // data.
  std::string tmpStr = FileSystem::getTempFileName();

  std::stringstream str;
  str << filePath << "/" << tmpStr << ".tmp";
  mTempPath = Path::get(mRootUri, str.str());
  std::stringstream finalStr;
  finalStr << filePath << "/" << tmpStr << factory_type::extension();
  mFinalPath = Path::get(mRootUri, finalStr.str());
  // Call back to the factory to actually create the file
  mCurrentFile = factory->createFile(mTempPath);
  
  if (mPolicy.mFileSeconds > 0) {
    mTimer.expires_from_now(boost::posix_time::seconds(mPolicy.mFileSeconds));
    // mTimer.async_wait(boost::bind(&StreamingFileCreation<_Factory>::timeout, 
    //     			  this,
    //     			  boost::asio::placeholders::error));
    mTimer.async_wait([this](const boost::system::error_code & err) {
      this->timeout(err);
    });
  }

  return mCurrentFile;
}

template<typename _Factory>
void StreamingFileCreation<_Factory>::start(FileSystem * genericFileSystem,
                                            PathPtr rootUri,
                                            _Factory * factory)
{
  mGenericFileSystem = genericFileSystem;
  mRootUri = rootUri;
}

template<typename _Factory>
typename StreamingFileCreation<_Factory>::output_file_type *
StreamingFileCreation<_Factory>::onRecord(RecordBuffer input,
                                          _Factory * factory)
{
  if ((0 != mPolicy.mFileRecords && 
       mPolicy.mFileRecords <= mNumRecords) ||
      (0 != mPolicy.mFileSeconds &&
       RecordBuffer() == input)) {
    close(true);
  }
  // Check if we have an input to record
  if (input != RecordBuffer()) {
    // Check if we have an open file
    if (mCurrentFile == NULL)
      createFile(mPolicy.mBaseDir, factory);
    mNumRecords += 1;
  }
  return mCurrentFile;
}

template<typename _Factory>
void StreamingFileCreation<_Factory>::close(bool flush)
{
  if (mCurrentFile != NULL) {
    if (flush) {
      mCurrentFile->flush();
    }
    mCurrentFile->close();
    delete mCurrentFile;
    mCurrentFile = NULL;
    mNumRecords = 0;
    if (flush && NULL != mTempPath.get() && 
	NULL != mFinalPath.get()) {
      // Put the file in its final place; don't wait for commit
      mGenericFileSystem->rename(mTempPath, mFinalPath);
      // TODO: Error!!!!  Queue this up for a retry.
      mTempPath = mFinalPath = PathPtr();
    }
    // Delete any file on unclean shutdown...
    if (mPolicy.mFileSeconds > 0) {
      // May not be necessary if we closing a file as a result
      // of a timeout
      mTimer.cancel();
    }
  }
}

template<typename _Factory>
void StreamingFileCreation<_Factory>::commit(std::string& err)
{
  // Streaming files don't wait for a commit signal.
}

#endif
