/**
 * Copyright (c) 2012, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <sys/types.h>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/core/ignore_unused.hpp>

#include "FileSystem.hh"
#include "FileWriteOperator.hh"
#include "RuntimeProcess.hh"
#include "ZLib.hh"
#include "Zstd.hh"

class NullCompress
{
private:
  uint8_t * mBuffer;
  std::size_t mLength;
public:
  NullCompress()
    :
    mBuffer(nullptr),
    mLength(0)
  {
  }

  ~NullCompress()
  {
  }

  void put(const uint8_t * buf_start, std::size_t len, bool isEOS)
  {
    boost::ignore_unused(isEOS);
    mBuffer = const_cast<uint8_t *>(buf_start);
    mLength = len;
  }
  bool run()
  {
    return mBuffer == nullptr;
  }
  void consumeOutput(uint8_t * & output, std::size_t & len)
  {
    output = mBuffer;
    len = mLength;
    mBuffer = nullptr;
    mLength = 0;
  }
  static const std::string & extension()
  {
    static const std::string ret(".txt");
    return ret;
  }
};



template<typename _Compressor>
class RuntimeWriteOperator : public RuntimeOperator
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  

private:
  enum State { START, READ };
  State mState;
  const RuntimeWriteOperatorType &  getWriteType()
  {
    return *reinterpret_cast<const RuntimeWriteOperatorType *>(&getOperatorType());
  }

  int mFile;
  RuntimePrinter mPrinter;
  _Compressor mCompressor;
  AsyncWriter<RuntimeWriteOperator> mWriter;
  std::thread * mWriterThread;
  int64_t mRead;

  void shutdownWriter()
  {
    if (mWriterThread != nullptr) {
      mWriter.enqueue(RecordBuffer());
      mWriterThread->join();
      delete mWriterThread;
      mWriterThread = nullptr;
    }
  }
public:
  RuntimeWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimeWriteOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

template<typename _Compressor>
RuntimeWriteOperator<_Compressor>::RuntimeWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mFile(-1),
  mPrinter(getWriteType().mPrint),
  mWriter(*this),
  mWriterThread(NULL),
  mRead(0)
{
}

template<typename _Compressor>
RuntimeWriteOperator<_Compressor>::~RuntimeWriteOperator()
{
  shutdownWriter();
}

template<typename _Compressor>
void RuntimeWriteOperator<_Compressor>::start()
{
  if (boost::algorithm::equals(getWriteType().mFile, "-")) {
    mFile = STDOUT_FILENO;
  } else {
    // Are we compressing?
    std::filesystem::path p (getWriteType().mFile);
    // Create directories if necessary before opening file.
    if (!p.parent_path().empty()) {
      std::filesystem::create_directories(p.parent_path());
    }
    mFile = ::open(getWriteType().mFile.c_str(),
		   O_WRONLY|O_CREAT|O_TRUNC,
		   S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP);
    if (mFile == -1) {
      auto err = errno;
      throw std::runtime_error((boost::format("Couldn't create file %1%: %2% (errno %3%)") % getWriteType().mFile %
                                ::strerror(err) % err).str());
    }
  }
  // Start a thread that will write
  mWriterThread = new std::thread(std::bind(&AsyncWriter<RuntimeWriteOperator>::run, 
                                            std::ref(mWriter)));
  mState = START;
  onEvent(NULL);
}

struct NullCompressor
{
};

static void writeWithRetry(int fd, const uint8_t * buf, std::size_t count)
{
  writeWithRetry<>(fd, buf, count, &::write);
}

template<typename _Compressor>
void writeCompressor(_Compressor & compressor, int fd, const uint8_t * buf, std::size_t bufSize)
{
  compressor.put(buf, bufSize, false);
  while(!compressor.run()) {
    uint8_t * output;
    std::size_t outputLen;
    compressor.consumeOutput(output, outputLen);
    writeWithRetry(fd, (const uint8_t *) output, outputLen);
  }
}

template<typename _Compressor>
void flushCompressor(_Compressor & compressor, int fd)
{
  // Flush data through the compressor.
  compressor.put(nullptr, 0, true);
  while(true) {
    compressor.run();
    uint8_t * output;
    std::size_t outputLen;
    compressor.consumeOutput(output, outputLen);
    if (outputLen > 0) {
      writeWithRetry(fd, (const uint8_t *) output, outputLen);
    } else {
      break;
    }
  }    
}

template<>
void writeCompressor<NullCompressor>(NullCompressor & , int fd, const uint8_t * buf, std::size_t bufSize)
{
  writeWithRetry(fd, buf, bufSize);
}

template<>
void flushCompressor<NullCompressor>(NullCompressor & , int )
{
}

template<typename _Compressor>
void RuntimeWriteOperator<_Compressor>::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (mRead == 0 && getWriteType().mHeader.size() && 
      getWriteType().mHeaderFile.size() == 0) {
    writeCompressor(mCompressor, mFile, (const uint8_t *) getWriteType().mHeader.c_str(), 
                    getWriteType().mHeader.size());
  }
  if (!isEOS) {
    mPrinter.print(input, true);
    getWriteType().mFree.free(input);
    writeCompressor(mCompressor, mFile, (const uint8_t *) mPrinter.c_str(), mPrinter.size());
    mPrinter.clear();
    mRead += 1;
  } else {
    flushCompressor(mCompressor, mFile);
    // Clean close of file and file system
    if (mFile != STDOUT_FILENO) {
      ::close(mFile);
    }
    mFile = -1;
  }
}

template<typename _Compressor>
void RuntimeWriteOperator<_Compressor>::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    if(getWriteType().mHeader.size() && 
       getWriteType().mHeaderFile.size()) {
      std::filesystem::path p (getWriteType().mHeaderFile);
      if (boost::algorithm::iequals(ZLibCompress::extension(), p.extension().string()) ||
          boost::algorithm::iequals(ZstdCompress::extension(), p.extension().string())) {
	// TODO: Support compressed header file.
	throw std::runtime_error("Writing compressed header file not supported yet");
      }
      std::ofstream headerFile(getWriteType().mHeaderFile.c_str(),
			       std::ios_base::out);
      headerFile << getWriteType().mHeader.c_str();
      headerFile.flush();
    }
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
	bool isEOS = RecordBuffer::isEOS(input);
	//writeToHdfs(input, isEOS);
	if (!isEOS) {
          mWriter.enqueue(input);
        } else {
          shutdownWriter();
	  break;
	}
      }
    }
  }
}

template<typename _Compressor>
void RuntimeWriteOperator<_Compressor>::shutdown()
{
  if (mFile != STDOUT_FILENO && mFile >= 0) {
    ::close(mFile);
  }
  mFile = -1;
  shutdownWriter();
}

RuntimeOperator * RuntimeWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  std::filesystem::path p(mFile);
  if (boost::algorithm::iequals(ZLibCompress::extension(), p.extension().string())) {
    return new RuntimeWriteOperator<ZLibCompress>(services, *this);
  } else if (boost::algorithm::iequals(ZstdCompress::extension(), p.extension().string())) {
    return new RuntimeWriteOperator<ZstdCompress>(services, *this);
  } else {
    return new RuntimeWriteOperator<NullCompressor>(services, *this);
  }
}


/**
 * Implements a 2-phase commit like protocol for committing
 * a rename.
 */
class HdfsFileRename
{
private:
  PathPtr mFrom;
  PathPtr mTo;
  FileSystem * mFileSystem;
public:
  HdfsFileRename(PathPtr from,
		 PathPtr to,
		 FileSystem * fileSystem);
  ~HdfsFileRename();
  bool prepare(std::string& err);
  void commit();
  void rollback();
  void dispose();
  static bool renameLessThan (std::shared_ptr<HdfsFileRename> lhs, 
			      std::shared_ptr<HdfsFileRename> rhs);
};


HdfsFileCommitter * HdfsFileCommitter::sCommitter = NULL;
int32_t HdfsFileCommitter::sRefCount = 0;
std::mutex HdfsFileCommitter::sGuard;

HdfsFileCommitter * HdfsFileCommitter::get()
{
  std::unique_lock<std::mutex> lock(sGuard);
  if (sRefCount++ == 0) {
    sCommitter = new HdfsFileCommitter();
  }
  return sCommitter;
}

void HdfsFileCommitter::release(HdfsFileCommitter *)
{
  std::unique_lock<std::mutex> lock(sGuard);
  if(--sRefCount == 0) {
    delete sCommitter;
    sCommitter = NULL;
  }
}

HdfsFileCommitter::HdfsFileCommitter()
  :
  mCommits(0)
{
}

HdfsFileCommitter::~HdfsFileCommitter()
{
}

void HdfsFileCommitter::track (PathPtr from, 
			       PathPtr to,
			       FileSystem * fileSystem)
{
  std::unique_lock<std::mutex> lock(mMutex);
  mActions.push_back(std::shared_ptr<HdfsFileRename>(new HdfsFileRename(from, to, fileSystem)));
}

bool HdfsFileCommitter::commit()
{
  std::unique_lock<std::mutex> lock(mMutex);
  std::cout << "HdfsFileCommitter::commit; mCommits = " << mCommits << std::endl;
  if (++mCommits == mActions.size()) {      
    // Sort the actions to make a deterministic order.
    std::sort(mActions.begin(), mActions.end(), 
	      HdfsFileRename::renameLessThan);
    for(std::size_t i = 0; i<mActions.size(); ++i) {
      if (!mActions[i]->prepare(mError)) {
	// Failed to commit delete temp files that haven't
	// been dealt with.  Note that we don't rollback
	// since we are no longer assuming a single process
	// writes all of the files.  If we did rollback then we
	// might be undoing work that another process that has
	// succeeded is assuming is in the filesystem. That would
	// appear to the user as job success when some files
	// have not been written.
	// See CR 1459063 for more details.
	for(std::size_t j = 0; j<mActions.size(); ++j) {
	  mActions[j]->dispose();
	}
	mActions.clear();
	mCommits = 0;
	return false;
      }
    }
    // Everyone voted YES, so commit (essentially a noop).
    for(std::size_t i = 0; i<mActions.size(); ++i) {
      mActions[i]->commit();
    }
    mActions.clear();
    mCommits = 0;
  }
  return true;
}

bool HdfsFileRename::renameLessThan (std::shared_ptr<HdfsFileRename> lhs, 
				     std::shared_ptr<HdfsFileRename> rhs)
{
  return strcmp(lhs->mTo->toString().c_str(), 
		rhs->mTo->toString().c_str()) < 0;
}

HdfsFileRename::HdfsFileRename(PathPtr from,
			       PathPtr to,
			       FileSystem * fileSystem)
  :
  mFrom(from),
  mTo(to),
  mFileSystem(fileSystem)
{
  if (!mFrom || mFrom->toString().size() == 0 || 
      !mTo || mTo->toString().size() == 0)
    throw std::runtime_error("HdfsFileRename::HdfsFileRename "
			     "expects non-empty filenames");
}

HdfsFileRename::~HdfsFileRename()
{
}

bool HdfsFileRename::prepare(std::string& err)
{
  if (mFrom && mFrom->toString().size()) {
    BOOST_ASSERT(mTo && mTo->toString().size() != 0);
    std::cout << "HdfsFileRename::prepare renaming " << 
      (*mFrom) << " to " <<
      (*mTo) << std::endl;
    if (!mFileSystem->rename(mFrom, mTo)) {
      std::string msg = (boost::format("Failed to rename HDFS file %1% to %2%") %
			 (*mFrom) % (*mTo)).str();
      std::cout << msg.c_str() << std::endl;
      // Check whether the file already exists.  If so and it is
      // the same size as what we just wrote, then assume idempotence
      // and return success.
      std::shared_ptr<FileStatus> toStatus, fromStatus;
      try {
	toStatus = 
	  mFileSystem->getStatus(mTo);
      } catch(std::exception & ex) {
	  err = (boost::format("Rename failed and target file %1% does "
			       "not exist") %
		 (*mTo)).str();      
	  std::cout << err.c_str() << std::endl;
	  return false;
      }
      try {
	fromStatus = 
	  mFileSystem->getStatus(mFrom);
      } catch(std::exception & ex) {
	err = (boost::format("Rename failed, target file %1% exists "
			     "but failed to "
			     "get status of temporary file %2%") %
	       (*mTo) % (*mFrom)).str();      
	std::cout << err.c_str() << std::endl;
	return false;
      }
      
      // This is an interesting check but with compression enabled
      // it isn't guaranteed to hold.  In particular, in a reducer
      // to which we have emitted with a non-unique key there is
      // non-determinism in the order of the resulting stream (depending
      // on the order in which map files are processed for example).
      // If the order winds up being sufficiently different between two
      // files, then the compression ratio may differ and the resulting
      // file sizes won't match.
      if (toStatus->size() != fromStatus->size()) {
	msg = (boost::format("Rename failed: target file %1% already "
			     "exists and has size %2%, "
			     "temporary file %3% has size %4%; "
			     "ignoring rename failure and continuing") %
	       (*mTo) % toStatus->size() % 
	       (*mFrom) % fromStatus->size()).str();      
      } else {
	msg = (boost::format("Both %1% and %2% have the same size; ignoring "
			     "rename failure and continuing") %
	       (*mFrom) % (*mTo)).str();
      }
      std::cout << msg.c_str() << std::endl;
    } 
    mFrom = PathPtr();      
    return true;
  } else {
    return false;
  }
}

void HdfsFileRename::commit()
{
  if (!mFrom && mTo) {
    std::cout << "HdfsFileRename::commit " << (*mTo) << std::endl;
    // Only commit if we prepared.
    mTo = PathPtr();
  }
}

void HdfsFileRename::rollback()
{
  if (!mFrom && mTo) {
    // Only rollback if we prepared.
    std::cout << "Rolling back permanent file: " << (*mTo) << std::endl;
    mFileSystem->remove(mTo);
    mTo = PathPtr();
  }
}

void HdfsFileRename::dispose()
{
  if (mFrom) {
    std::cout << "Removing temporary file: " << (*mFrom) << std::endl;
    mFileSystem->remove(mFrom);
    mFrom = PathPtr();
  }
}

template<typename _Compressor>
class OutputFile
{
public:
  WritableFile * File;
  _Compressor Compressor;
  OutputFile(WritableFile * f) 
    :
    File(f)
  {
  }
  ~OutputFile()
  {
    close();
  }
  void flush()
  {
    // Flush data through the compressor.
    this->Compressor.put(NULL, 0, true);
    while(true) {
      this->Compressor.run();
      uint8_t * output;
      std::size_t outputLen;
      this->Compressor.consumeOutput(output, outputLen);
      if (outputLen > 0) {
	this->File->write(output, outputLen);
      } else {
	break;
      }
    }    
    // Flush data to disk
    this->File->flush();
  }
  bool close() {
    // Clean close of file 
    bool ret = true;
    if (File != NULL) {
      ret = File->close();
      delete File;
      File = NULL;
    }
    return ret;
  }
};

template<typename _FileCreationPolicy, typename _Compressor>
class RuntimeHdfsWriteOperator : public RuntimeOperatorBase<RuntimeHdfsWriteOperatorType<_FileCreationPolicy>>
{
public:
  typedef RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor> this_type;
  typedef OutputFile<_Compressor> output_file_type;
  typedef typename _FileCreationPolicy::template creation_type<this_type>::type file_creation_type;
  void writeToHdfs(RecordBuffer input, bool isEOS);  
private:
  enum State { START, READ };
  State mState;

  PathPtr mRootUri;
  RuntimePrinter mPrinter;
  AsyncWriter<RuntimeHdfsWriteOperator> mWriter;
  std::thread * mWriterThread;
  HdfsFileCommitter * mCommitter;
  std::string mError;
  file_creation_type mCreationPolicy;

  void renameTempFile();
  /**
   * Is this operator writing an inline header?
   */
  bool hasInlineHeader() 
  {
    return this->getMyOperatorType().mHeader.size() != 0 && 
      this->getMyOperatorType().mHeaderFile.size()==0;
  }
  /**
   * Is this operator writing a header file?
   */
  bool hasHeaderFile()
  {
    return this->getPartition() == 0 && 
      this->getMyOperatorType().mHeader.size() != 0 && 
      this->getMyOperatorType().mHeaderFile.size()!=0;  
  }
  
public:
  RuntimeHdfsWriteOperator(RuntimeOperator::Services& services,
                           const RuntimeHdfsWriteOperatorType<_FileCreationPolicy>& opType);
  ~RuntimeHdfsWriteOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();

  /**
   * Create a file
   */
  output_file_type * createFile(PathPtr filePath);

  /**
   * default extension of the files
   */
  static const std::string & extension()
  {
    return _Compressor::extension();
  }
};

template<typename _FileCreationPolicy, typename _Compressor>
RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::RuntimeHdfsWriteOperator(RuntimeOperator::Services& services, 
                                                                                     const RuntimeHdfsWriteOperatorType<_FileCreationPolicy>& opType)
  :
  RuntimeOperatorBase<RuntimeHdfsWriteOperatorType<_FileCreationPolicy>>(services, opType),
  mState(START),
  mPrinter(this->getMyOperatorType().mPrint),
  mWriter(*this),
  mWriterThread(NULL),
  mCommitter(NULL),
  mCreationPolicy(*opType.mCreationPolicy, services)
{
}

template<typename _FileCreationPolicy, typename _Compressor>
RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::~RuntimeHdfsWriteOperator()
{
  delete mWriterThread;

  if (mCommitter) {
    HdfsFileCommitter::release(mCommitter);
    mCommitter = NULL;
  }
}

template<typename _FileCreationPolicy, typename _Compressor>
typename RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::output_file_type *
RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::createFile(PathPtr filePath)
{
  // TODO: Create all ancestor directories
  PathPtr parent = Path::resolveReference(filePath, Path::get("."));
  if (!this->getMyOperatorType().mFileFactory->getFileSystem()->exists(parent)) {
    std::cout << "Creating parent directory " << parent->toString().c_str() << " of file path " << filePath->toString().c_str() << std::endl;
    this->getMyOperatorType().mFileFactory->mkdir(parent);
  }
  
  // TODO: Check if file exists
  // TODO: Make sure file is cleaned up in case of failure.
  WritableFile * f = this->getMyOperatorType().mFileFactory->openForWrite(filePath);
  output_file_type * of = new output_file_type(f);
  if (hasInlineHeader()) {
    // We write in-file header for every partition
    of->Compressor.put((const uint8_t *) this->getMyOperatorType().mHeader.c_str(), 
		       this->getMyOperatorType().mHeader.size(), 
		       false);
    while(!of->Compressor.run()) {
      uint8_t * output;
      std::size_t outputLen;
      of->Compressor.consumeOutput(output, outputLen);
      of->File->write(output, outputLen);
    }
  } 
  return of;
}

template<typename _FileCreationPolicy, typename _Compressor>
void RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::start()
{
  WritableFileFactory * ff = this->getMyOperatorType().mFileFactory;
  mRootUri = ff->getFileSystem()->getRoot();
  if (this->getMyOperatorType().mCreationPolicy->requiresServiceCompletionPort()) {
    mCreationPolicy.setCompletionPort(&(dynamic_cast<ServiceCompletionPort*>(this->getCompletionPorts()[0])->getFifo()));
  }
  mCreationPolicy.start(ff->getFileSystem(), mRootUri, this);

  if (hasHeaderFile()) {
    PathPtr headerPath = Path::get(this->getMyOperatorType().mHeaderFile);
    std::string tmpHeaderStr = FileSystem::getTempFileName();
    PathPtr tmpHeaderPath = Path::get(mRootUri, 
				      "/tmp/headers/" + tmpHeaderStr);
    if (mCommitter == NULL) {
      mCommitter = HdfsFileCommitter::get();
    }
    mCommitter->track(tmpHeaderPath, headerPath, 
		      this->getMyOperatorType().mFileFactory->getFileSystem());
    WritableFile * headerFile = ff->openForWrite(tmpHeaderPath);
    if (headerFile == NULL) {
      throw std::runtime_error("Couldn't create header file");
    }
    headerFile->write((const uint8_t *) &this->getMyOperatorType().mHeader[0], 
		      this->getMyOperatorType().mHeader.size());
    headerFile->close();
    delete headerFile;
  }
  // Start a thread that will write
  // mWriterThread = 
  //   new std::thread(std::bind(&AsyncWriter<RuntimeHdfsWriteOperator>::run, 
  // 				  std::ref(mWriter)));
  mState = START;
  onEvent(NULL);
}

template<typename _FileCreationPolicy, typename _Compressor>
void RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::renameTempFile()
{
  mCreationPolicy.commit(mError);

  if (0 == mError.size() && hasHeaderFile()) {
    // Commit the header file as well.
    if(!mCommitter->commit()) {
      mError = mCommitter->getError();
    } 
  }
}

template<typename _FileCreationPolicy, typename _Compressor>
void RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (!isEOS) {
    output_file_type * of = mCreationPolicy.onRecord(input, this);
    mPrinter.print(input);
    this->getMyOperatorType().mFree.free(input);
    of->Compressor.put((const uint8_t *) mPrinter.c_str(), mPrinter.size(), false);
    while(!of->Compressor.run()) {
      uint8_t * output;
      std::size_t outputLen;
      of->Compressor.consumeOutput(output, outputLen);
      of->File->write(output, outputLen);
    }
    mPrinter.clear();
  } else {
    mCreationPolicy.close(true);
  }
}

template<typename _FileCreationPolicy, typename _Compressor>
void RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      if (this->getCompletionPorts().size()) {
	this->getCompletionPorts()[0]->request_unlink();
	this->getCompletionPorts()[0]->request_link_after(*this->getInputPorts()[0]);
	this->requestRead(*this->getCompletionPorts()[0]);
      } else {
	this->requestRead(0);
      }
      mState = READ;
      return;
    case READ:
      RecordBuffer input;
      this->read(port, input);
      if (port == this->getInputPorts()[0]) {
	bool isEOS = RecordBuffer::isEOS(input);
	writeToHdfs(input, isEOS);
	// mWriter.enqueue(input);
	if (isEOS) {
	  // Wait for writers to flush; perhaps this should be done is shutdown
	  // mWriterThread->join();
	  // See if there was an error within the writer that we need
	  // to throw out.  Errors here are thow that would have happened
	  // after we enqueued EOS.
	  // std::string err;
	  // mWriter.getError(err);
	  // if (err.size()) {
	  //   throw std::runtime_error(err);
	  // }
	  // Do the rename of the temp file in the main dataflow
	  // thread because this makes the order in which files are renamed
	  // across the entire dataflow deterministic (at least for the map-reduce
	  // case where there is only a single dataflow thread and all collectors/reducers
	  // are sort-merge).
	  renameTempFile();
	  break;
	}
      } else {
	BOOST_ASSERT(port == this->getCompletionPorts()[0]);
	mCreationPolicy.onRecord(RecordBuffer(), this);
      }
    }
  }
}

template<typename _FileCreationPolicy, typename _Compressor>
void RuntimeHdfsWriteOperator<_FileCreationPolicy, _Compressor>::shutdown()
{
  mCreationPolicy.close(false);
  if (mError.size() != 0) {
    throw std::runtime_error(mError);
  }
}

RuntimeOperator * HdfsWriteOperatorFactory::create(RuntimeOperator::Services& services,
                                                   const RuntimeHdfsWriteOperatorType<MultiFileCreationPolicy> & opType)
{
  if (CompressionType::Gzip() == opType.getCompressionType()) {
    return new RuntimeHdfsWriteOperator<MultiFileCreationPolicy, ZLibCompress>(services, opType);
  } else if (CompressionType::Zstandard() == opType.getCompressionType()) {
    return new RuntimeHdfsWriteOperator<MultiFileCreationPolicy, ZstdCompress>(services, opType);
  } else if (CompressionType::Uncompressed() == opType.getCompressionType()) {
    return new RuntimeHdfsWriteOperator<MultiFileCreationPolicy, NullCompress>(services, opType);
  } else {
    return nullptr;
  }
}

RuntimeOperator * HdfsWriteOperatorFactory::create(RuntimeOperator::Services& services,
                                                   const RuntimeHdfsWriteOperatorType<StreamingFileCreationPolicy> & opType)
{
  if (CompressionType::Gzip() == opType.getCompressionType()) {
    return new RuntimeHdfsWriteOperator<StreamingFileCreationPolicy, ZLibCompress>(services, opType);
  } else if (CompressionType::Zstandard() == opType.getCompressionType()) {
    return new RuntimeHdfsWriteOperator<StreamingFileCreationPolicy, ZstdCompress>(services, opType);
  } else if (CompressionType::Uncompressed() == opType.getCompressionType()) {
    return new RuntimeHdfsWriteOperator<StreamingFileCreationPolicy, NullCompress>(services, opType);
  } else {
    return nullptr;
  }
}

