#include "DataflowRuntime.hh"
#include "ServiceCompletionPort.hh"

ServiceCompletionFifo::ServiceCompletionFifo(DataflowScheduler & targetScheduler)
:
  mRecordsRead(0),
  mTarget(NULL),
  mTargetScheduler(targetScheduler)
{
  mTarget = new ServiceCompletionPort(*this);
}

ServiceCompletionFifo::~ServiceCompletionFifo()
{
  delete mTarget;
}

void ServiceCompletionFifo::write(RecordBuffer buf)
{
  bool wakeUpTarget;
  {
    std::lock_guard<std::mutex> channelGuard(mLock);
    DataflowSchedulerScopedLock schedGuard(mTargetScheduler);
    mQueue.Push(buf);
    mRecordsRead += 1;
    wakeUpTarget = mTargetScheduler.reprioritizeReadRequest(*mTarget);
  }
  if (wakeUpTarget) {
    mTargetScheduler.unblocked();
  }
}

void ServiceCompletionFifo::sync(ServiceCompletionPort & port)
{
  writeSomeToPort();
}

void ServiceCompletionFifo::writeSomeToPort()
{
  // Move data into the target port.
  // Signal target that read request is complete.
  std::lock_guard<std::mutex> channelGuard(mLock);
  DataflowSchedulerScopedLock schedGuard(mTargetScheduler);
  mQueue.popAndPushSomeTo(mTarget->getLocalBuffer());
  mTargetScheduler.readComplete(*mTarget);
}

