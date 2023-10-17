#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->numThreads = num_threads;
    this->threadPool = new std::thread[this->numThreads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

//
// Allow each thread to pick the next, or ith, task when they are done.
//
void TaskSystemParallelSpawn::runSingleThread(IRunnable *runnable, int num_total_tasks, std::mutex *lock, int *curTask)
{
    int nextTask = -1;
    while (nextTask < num_total_tasks)
    {
        // Have threads claim the next task, use lock to ensure that there's no race condition
        lock->lock();
        nextTask = *curTask;
        *curTask += 1;
        lock->unlock();
        if (nextTask < num_total_tasks)
        {
            runnable->runTask(nextTask, num_total_tasks);
        }
    }
}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::mutex *lock = new std::mutex();
    int *curr_task = new int;
    *curr_task = 0;
    for (int i = 0; i < this->numThreads; i++)
    {
        this->threadPool[i] = std::thread(&TaskSystemParallelSpawn::runSingleThread, this, runnable, num_total_tasks, lock, curr_task);
    }
    for (int i = 0; i < this->numThreads; i++)
    {
        this->threadPool[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

ProgramState::ProgramState() {
    this->thread_pool_lock = new std::mutex();
    this->finish_lock = new std::mutex();
    this->finish_cv = new std::condition_variable();
    this->runnable = nullptr;
    this->finished_tasks = -1;
    this->remaining_tasks = -1;
    this->total_tasks = -1;
}

ProgramState::~ProgramState() {}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->progState = new ProgramState;
    this->killed = false;
    this->num_threads = num_threads;
    this->thread_pool = std::vector<std::thread>(num_threads);
    for (int i = 0; i < num_threads; i++)
    {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    killed = true;
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> ulock(*(this->progState->finish_lock));
    this->progState->thread_pool_lock->lock();
    this->progState->finished_tasks = 0;
    this->progState->remaining_tasks = num_total_tasks;
    this->progState->total_tasks = num_total_tasks;
    this->progState->runnable = runnable;
    this->progState->thread_pool_lock->unlock();
    this->progState->finish_cv->wait(ulock);
    ulock.unlock();
}

void TaskSystemParallelThreadPoolSpinning::spinningThread()
{
    int id;
    int total;
    while (true)
    {
        if (this->killed)
            break;
        this->progState->thread_pool_lock->lock();
        total = this->progState->total_tasks;
        id = total - this->progState->remaining_tasks;
        if (id < total)
            this->progState->remaining_tasks--;
        this->progState->thread_pool_lock->unlock();
        if (id < total)
        {
            this->progState->runnable->runTask(id, total);
            this->progState->thread_pool_lock->lock();
            this->progState->finished_tasks++;
            if (this->progState->finished_tasks == total)
            {
                this->progState->thread_pool_lock->unlock();
                // lock so we put the main thread to sleep
                this->progState->finish_lock->lock();
                this->progState->finish_lock->unlock();
                this->progState->finish_cv->notify_all();
            }
            else
            {
                this->progState->thread_pool_lock->unlock();
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
