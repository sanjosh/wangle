
#include <wangle/concurrent/FutureExecutor.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/BoundThreadFactory.h>


using namespace folly;
using namespace wangle;

void threadFunc() {

}

int main()
{
  size_t numThreads = 1;
  int8_t numPriorities = 2;
  size_t maxQueueSize = 100;
  size_t numCores = 4;

  typedef FutureExecutor<CPUThreadPoolExecutor> MyExec;

  std::vector<std::shared_ptr<MyExec>> executors;

  auto namedFactory = std::make_shared<NamedThreadFactory>("me");

  for (int coreId = 0; coreId < numCores; coreId ++) {
    auto factory = std::make_shared<BoundThreadFactory>(namedFactory, 
      coreId);

    auto executor = std::make_shared<MyExec>(numThreads, 
      numPriorities, 
      maxQueueSize, 
      factory);

    executors.push_back(executor);
  }

  std::vector<folly::Future<folly::Unit>> futVec;

  for (int i = 0; i < 1000; i ++)
  {
    bool repeat;
    do {
      try {
        auto fut = executors[i % numCores]->addFuture(threadFunc);
        futVec.push_back(std::move(fut));
        std::cout << "submitted job=" << i << std::endl;
        repeat = false;
      } catch (const wangle::QueueFullException& e) {
        std::cout << "caught exception" << std::endl;
        repeat = true;
      }
    } while (repeat);
  }

  for (auto& fut : futVec)
  {
    fut.wait();
  }
  futVec.clear();

  for (auto& executor : executors) {

    auto stats = executor->getPoolStats();

    std::cout 
      << stats.threadCount << ","
      << stats.activeThreadCount << ","
      << stats.idleThreadCount << ","
      << stats.totalTaskCount << "," 
      << stats.pendingTaskCount 
      << std::endl;

    executor->stop();

    executor->join();
  }
  executors.clear();

}
