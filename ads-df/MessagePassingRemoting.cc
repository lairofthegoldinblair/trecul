// TODO: replace with Boost.Log
#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include "GraphBuilder.hh"
#include "MessagePassing.hh"
#include "MessagePassingRemoting.hh"

static void checkRegularFileExists(const std::string& filename)
{
  std::filesystem::path p (filename);
  if (!std::filesystem::exists(p)) {
    throw std::runtime_error((boost::format("%1% does not exist") %
			      filename).str());
  }
  if (!std::filesystem::is_regular_file(p)) {
    throw std::runtime_error((boost::format("%1% is not a regular file") %
			      filename).str());
  }  
}

class MPIInit
{
private:
  bool mInit;
public:
  MPIInit()
    :
    mInit(false)
  {
    int provided;
    mInit = MPI_SUCCESS == ::MPI_Init_thread(nullptr, nullptr, MPI_THREAD_MULTIPLE, &provided);
    if (!mInit || provided != MPI_THREAD_MULTIPLE)
      throw std::runtime_error("Required multithread MPI support");
  }


  ~MPIInit()
  {
    if (mInit) {
      ::MPI_Finalize();
    }
  }


};

MessagePassingRemoting::MessagePassingRemoting()
  :
  RuntimeProcess(),
  mDemuxType(new RuntimeMessageDemuxOperatorType()),
  mDemux(NULL),
  mMPIScheduler(NULL),
  mBufferPoolType(new RuntimeBufferPoolOperatorType())
{
}

MessagePassingRemoting::~MessagePassingRemoting()
{
  delete mDemux;
  delete mMPIScheduler;
  delete mBufferPoolType;
  delete mDemuxType;
  for(auto t : mDataAvailableTypes) {
    delete t;
  }
  for(auto t : mReceiverTypes) {
    delete t;
  }
  for(auto t : mSendAvailableTypes) {
    delete t;
  }
  for(auto t : mSenderTypes) {
    delete t;
  }
  for(auto c : mRemoteReceiveChannels) {
    delete c;
  }
}

DataflowScheduler & MessagePassingRemoting::getMPIScheduler()
{
  // If we haven't already create the scheduler and demux operator.
  if(mMPIScheduler == NULL) {
    mMPIScheduler = new DataflowScheduler();
    mDemux = mDemuxType->create(*mMPIScheduler);
  }
  return *mMPIScheduler;
}

RuntimeOperator& MessagePassingRemoting::getBufferPool(int32_t partition)
{
  if (mBufferPool.find(partition) == mBufferPool.end()) {
    mBufferPool[partition] = createOperator(mBufferPoolType, partition);
  }

  return *mBufferPool.find(partition)->second;
}

void MessagePassingRemoting::connectFromBufferPool(RuntimeOperator & op, int32_t inputPort, int32_t partition)
{
  connectInProcess(getBufferPool(partition), getBufferPool(partition).getNumOutputs(), partition,
                   op, inputPort, partition,
                   true);
}

void MessagePassingRemoting::connectToBufferPool(RuntimeOperator & op, int32_t outputPort, int32_t partition)
{
  // Have to add ports in order right now
  // Bad API decision.                                                                                                                                                                                              // Op sends requests to demux on this
  connectInProcess(op, outputPort, partition,
                   getBufferPool(partition), getBufferPool(partition).getNumInputs(), partition,
                   true);
}

void MessagePassingRemoting::connectFromDemux(RuntimeOperator & op, int32_t inputPort, int32_t partition)
{
  // Op receives completion
  connectInProcess(*mDemux, mDemux->getNumOutputs(), getMPIScheduler(),
                   op, inputPort, getScheduler(partition),
                   true);
}

void MessagePassingRemoting::connectToDemux(RuntimeOperator & op, int32_t outputPort, int32_t partition)
{
  // Have to add ports in order right now
  // Bad API decision.
  // Op sends requests to demux on this
  connectInProcess(op, outputPort, getScheduler(partition),
                   *mDemux, mDemux->getNumInputs(), getMPIScheduler(),
                   true);
}

void MessagePassingRemoting::connectRemoteReceive(RuntimeOperator & sourceA, int32_t outputPortA,
                                                  RuntimeOperator & sourceB, int32_t outputPortB,
                                                  RuntimeOperator & target, int32_t inputPort,
                                                  int32_t partition)
{
  mRemoteReceiveChannels.push_back(new RemoteReceiveFifo(getScheduler(partition), getScheduler(partition)));
  sourceA.setOutputPort(mRemoteReceiveChannels.back()->getAvailableSource(), outputPortA);
  mRemoteReceiveChannels.back()->getAvailableSource()->setOperator(sourceA);
  sourceB.setOutputPort(mRemoteReceiveChannels.back()->getDataSource(), outputPortB);
  mRemoteReceiveChannels.back()->getDataSource()->setOperator(sourceB);
  target.setInputPort(mRemoteReceiveChannels.back()->getTarget(), inputPort);
  mRemoteReceiveChannels.back()->getTarget()->setOperator(target);
}

void MessagePassingRemoting::addRemoteSource(const InterProcessFifoSpec& spec,
                                             RuntimeOperator & sourceOp,
                                             int32_t sourcePartition,
                                             int32_t sourcePartitionConstraintIndex,
                                             int32_t targetPartition,
                                             int32_t targetPartitionConstraintIndex,
                                             int32_t tag)
{
  // The rank is the position of the set bit in the iterator.
  // The tag is calculated as the base tag set in the spec + sourcePosition*#targetOperators + target position within
  // the set bits.
  mSendAvailableTypes.push_back(new RuntimeSendAvailableOperatorType(targetPartition,
                                                                     1000000 + tag));
  RuntimeOperator * sendAvailable = createOperator(mSendAvailableTypes.back(), sourcePartition);

  mSenderTypes.push_back(new RuntimeMessageSendOperatorType(spec.getSerialize(),
                                                            spec.getFreeRef(),
                                                            spec.getFree(),
                                                            targetPartition,
                                                            tag));
  RuntimeOperator * sender = createOperator(mSenderTypes.back(), sourcePartition);
  // Connect everyone up in the magic order.
  // This is REALLY brittle right now!
  connectToDemux(*sendAvailable, RuntimeMessageSendAvailableOperator::DATA_AVAILABLE_REQUEST, sourcePartition);
  connectFromDemux(*sendAvailable, RuntimeMessageSendAvailableOperator::DATA_AVAILABLE_COMPLETE, sourcePartition);
  connectToBufferPool(*sender, RuntimeMessageSendOperator::BUFFER_ALLOC_REQUEST, sourcePartition);
  connectFromBufferPool(*sender, RuntimeMessageSendOperator::BUFFER_ALLOC_COMPLETE, sourcePartition);
  connectToDemux(*sender, RuntimeMessageSendOperator::DATA_SEND_REQUEST, sourcePartition);
  connectFromDemux(*sender, RuntimeMessageSendOperator::DATA_SEND_COMPLETE, sourcePartition);
  connectToBufferPool(*sender, RuntimeMessageSendOperator::BUFFER_FREE_REQUEST, sourcePartition);
  connectInProcess(*sendAvailable, RuntimeMessageSendAvailableOperator::RECORD_OUTPUT, sourcePartition,
                   *sender, RuntimeMessageSendOperator::RECORD_INPUT, sourcePartition, true);

  // Finally can connect the external operator to the input of the send available operator
  connectInProcess(sourceOp, targetPartitionConstraintIndex, sourcePartition,
                   *sendAvailable, RuntimeMessageSendAvailableOperator::RECORD_INPUT, sourcePartition, true);

}

void MessagePassingRemoting::addRemoteTarget(const InterProcessFifoSpec& spec,
                                             RuntimeOperator & targetOp,
                                             int32_t targetPartition,
                                             int32_t targetPartitionConstraintIndex,
                                             int32_t sourcePartition,
                                             int32_t sourcePartitionConstraintIndex,
                                             int32_t tag)
{
  int dataAvailableTag = 1000000 + tag;
  mDataAvailableTypes.push_back(new RuntimeDataAvailableOperatorType(sourcePartition,
                                                                     dataAvailableTag));
  RuntimeOperator * dataAvailable = createOperator(mDataAvailableTypes.back(), targetPartition);

  mReceiverTypes.push_back(new RuntimeMessageReceiverOperatorType(spec.getDeserialize(),
                                                                  spec.getMalloc(),
                                                                  sourcePartition,
                                                                  tag));
  RuntimeOperator * receiver = createOperator(mReceiverTypes.back(), targetPartition);

  // Connect everyone up in the magic order.
  // This is REALLY brittle right now!
  connectToDemux(*dataAvailable, RuntimeDataAvailableOperator::DATA_RECV_REQUEST, targetPartition);
  connectFromDemux(*dataAvailable, RuntimeDataAvailableOperator::DATA_RECV_COMPLETE, targetPartition);
  connectToBufferPool(*receiver, RuntimeMessageReceiverOperator::BUFFER_ALLOC_REQUEST, targetPartition);
  connectFromBufferPool(*receiver, RuntimeMessageReceiverOperator::BUFFER_ALLOC_COMPLETE, targetPartition);
  connectToDemux(*receiver, RuntimeMessageReceiverOperator::DATA_RECV_REQUEST, targetPartition);
  connectFromDemux(*receiver, RuntimeMessageReceiverOperator::DATA_RECV_COMPLETE, targetPartition);
  connectToBufferPool(*receiver, RuntimeMessageReceiverOperator::BUFFER_FREE_REQUEST, targetPartition);

  // Connect output
  connectRemoteReceive(*dataAvailable, RuntimeDataAvailableOperator::RECORD_OUTPUT,
                       *receiver, RuntimeMessageReceiverOperator::RECORD_OUTPUT,
                       targetOp, sourcePartitionConstraintIndex,
                       targetPartition);

}

void MessagePassingRemoting::runRemote(std::vector<std::shared_ptr<std::thread> >& threads)
{
  // If required start the scheduler for the message service.
  if (NULL != mDemux) {
    std::vector<RuntimeOperator *> ops;
    ops.push_back(mDemux);
    mMPIScheduler->setOperators(ops);
    threads.push_back(std::make_shared<std::thread>([this](){ RuntimeProcess::run(*this->mMPIScheduler); }));
  }
}

bool MessagePassingRemoting::isMPI()
{
  return nullptr != getenv("PMI_RANK") || nullptr != getenv("OMPI_COMM_WORLD_RANK");
}

int MessagePassingRemoting::runMPI(boost::program_options::variables_map & vm)
{
  std::vector<std::string> JobServers;
  std::map<std::string, std::vector<std::size_t> > JobServerIndex;
  
    // By default we are one process of one.
  int rank=0;
  int sz=1;

  // Test to see if we are running under MPI; if so initialize
  // and figure out what the cluster looks like.
  std::shared_ptr<MPIInit> mpiInit;
  if (isMPI()) {
    mpiInit = std::make_shared<MPIInit>();
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &sz);
  }

  try {
    if (rank == 0) {
      std::cout << "Starting process from pid=" << getpid() << std::endl;
      // bool wait = true;
      // while(wait) {
      //   std::this_thread::sleep_for(std::chrono::seconds(1));
      // }
      char host[256];
      gethostname(&host[0], 256);
      JobServerIndex[host].push_back(JobServers.size());
      JobServers.push_back(host);
      std::cout << "Job root: " << host << std::endl;
      // Collect info about servers in the job.
      for(int partition=1; partition<sz; partition++) {
        MPI_Status status;
        ::MPI_Recv(&host[0], 256, MPI_BYTE, partition, 0, MPI_COMM_WORLD, &status);
        JobServerIndex[host].push_back(JobServers.size());
        JobServers.push_back(host);
        std::cout << "Job server[" << partition << "]: " << host << std::endl;
      }

      std::string inputFile(vm["file"].as<std::string>());
    if (!boost::algorithm::equals(inputFile, "-")) {
      checkRegularFileExists(inputFile);
    }
    // Make sure partitions is at least as large as partition+1;
    PlanCheckContext ctxt;
    DataflowGraphBuilder gb(ctxt);
    gb.buildGraphFromFile(inputFile);
    std::shared_ptr<RuntimeOperatorPlan> plan = gb.create(sz);
      if (sz > 1) {
        std::ostringstream s(std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(s);
        RuntimeOperatorPlan * tmp = plan.get();
        oa << BOOST_SERIALIZATION_NVP(tmp);

        std::string buf = s.str();
	std::size_t bufSz = buf.size() + 1;
        for(int partition=1; partition<sz; ++partition) {
          std::cout << "Sending plan of length " << bufSz << " to partition " << partition << std::endl;
          ::MPI_Send(const_cast<char*>(buf.c_str()), bufSz, MPI_BYTE, partition, 0, MPI_COMM_WORLD);
        }
      }

      MessagePassingRemoting p;
      p.init(0,0,sz,*plan.get());
      p.run();
    } else {
      // Tell root who I am
      char host[256];
      gethostname(&host[0], 256);
      ::MPI_Send(&host[0], 256, MPI_BYTE, 0, 0, MPI_COMM_WORLD);

      // Get my instructions from root
      MPI_Status status;
      int ret = ::MPI_Probe(0,0,MPI_COMM_WORLD,&status);
      int bufSz;
      ret = ::MPI_Get_count(&status, MPI_BYTE, &bufSz);
      char * buf = new char [bufSz];
      ret = ::MPI_Recv(buf, bufSz, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &status);
      // Subtle point : we want the plan to outlive the runtime because the latter
      // refers to former.
      RuntimeOperatorPlan * tmp=nullptr;
      if (bufSz > 1) {
        std::cout << "Partition " << rank << " received plan of length " << bufSz << " from root" << std::endl;
        boost::iostreams::stream<boost::iostreams::array_source> archiveStream(boost::iostreams::array_source(buf,bufSz));
        boost::archive::binary_iarchive ia(archiveStream);
        ia >> BOOST_SERIALIZATION_NVP(tmp);
        if (tmp->getNumPartitions() != sz) {
          throw std::runtime_error((boost::format("INTERNAL ERROR: Partition %1% received plan with %2% partitions but MPI size is %3") %
                                    rank % tmp->getNumPartitions() % sz).str());
        }
        tmp->loadFunctions();
        MessagePassingRemoting p;
        p.init(rank,rank,sz,*tmp);
        std::cout << "Starting process with rank=" << rank << " from pid=" << getpid() << std::endl;
        p.run();
      }
      delete tmp;
      delete [] buf;
    }
  } catch(std::exception & e) {
    std::cerr << e.what() << std::endl;
    if (mpiInit.get())
      ::MPI_Abort(MPI_COMM_WORLD, -1);
    return -1;
  }
  return 0;
}
