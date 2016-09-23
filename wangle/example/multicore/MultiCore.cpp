
#include <wangle/concurrent/FutureExecutor.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/BoundThreadFactory.h>

#include <sys/types.h> // open
#include <fcntl.h> // open
#include <sys/stat.h> // stat
#include <unistd.h> // stat

#include <glog/logging.h> // LOG

using namespace folly;
using namespace wangle;

char* filename = nullptr;

size_t numCores = 40;
size_t numTasks = 1024;
size_t fileSize = 0;
size_t blkSize = 0;
int fd = -1;
int newfd = -1;

void workerFunc(int idx) {

  char* buf = (char*)malloc(blkSize);

  ssize_t readSz = pread(fd, buf, blkSize, idx * blkSize);
  if (readSz != blkSize) {
    LOG(ERROR) << "failed to read offset=" << idx * blkSize << " error=" << errno;
  }

  for (size_t c = 0; c < blkSize; c++)
  {
    buf[c] = 'a' + (idx % 26);
  }

  ssize_t writeSz = pwrite(newfd, buf, blkSize, idx * blkSize);
  if (writeSz != blkSize) {
    LOG(ERROR) << "failed to write offset=" << idx * blkSize << " error=" << errno;
  }

  free(buf);
}

int main(int argc, char* argv[])
{
  size_t numThreads = 1;
  int8_t numPriorities = 2;
  size_t maxQueueSize = 100;

  if (argc > 1) {
    filename = strdup(argv[1]);
  }
  fd = open(filename, O_RDONLY);
  assert(fd >= 0);

  newfd = open("./abc.new", O_CREAT | O_TRUNC | O_WRONLY, S_IRWXU | S_IRWXG);
  assert(newfd >= 0);

  struct stat statbuf;
  int err = fstat(fd, &statbuf);
  assert(err == 0);
  fileSize = statbuf.st_size;

  blkSize = fileSize/numTasks;

  typedef FutureExecutor<CPUThreadPoolExecutor> MyExec;

  std::vector<std::shared_ptr<MyExec>> executors;

  auto namedFactory = std::make_shared<NamedThreadFactory>("me");

  // Start an executor on each core
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

  // Fire off the tasks
  for (int i = 0; i < numTasks; i ++)
  {
    bool repeat;
    do {
      try {
        // set expiration time of 1 sec
        // std::chrono::milliseconds expiration(10000000);
        // int8_t priority = 1;
        auto fut = executors[i % numCores]->addFuture(std::bind(workerFunc, i));
        futVec.push_back(std::move(fut));
        repeat = false;
      } catch (const wangle::QueueFullException& e) {
        usleep(1000);
        repeat = true;
      }
    } while (repeat);
  }

  // wait for the tasks to finish
  for (auto& fut : futVec)
  {
    fut.wait();
  }
  futVec.clear();

  // shutdown all executors
  for (auto& executor : executors) {

    auto stats = executor->getPoolStats();

    std::cout 
      << stats.activeThreadCount << ","
      << stats.idleThreadCount << ","
      << stats.threadCount << ","
      << stats.pendingTaskCount << ","
      << stats.totalTaskCount 
      << std::endl;

    executor->stop();

    executor->join();
  }
  executors.clear();

  close(fd);
  fsync(newfd);
  close(newfd);

}
