#include "tasksys.h"
#include <thread>
#include <deque>
#include <mutex>
#include <condition_variable>

std::mutex lock;
std::condition_variable_any cv;
std::condition_variable_any cv_thread;


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


void threadTask(IRunnable* runnable, int num_total_tasks, TaskSystemParallelSpawn* task) {
    while (true) {
        lock.lock();
        if (task->q.empty()) {
            lock.unlock();
            break;
        }
        int cur_task = task->q.front();
        task->q.pop_front();
        lock.unlock();
        runnable->runTask(cur_task, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::thread threads[this->num_threads];

    for (int i = 0; i < num_total_tasks; i++) {
        this->q.push_back(i);
    }
    for (int i = 0; i < this->num_threads; i++) {
        threads[i] = std::thread(threadTask, runnable, num_total_tasks, this);
    }
    for (int i = 0; i < this->num_threads; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void threadTaskSpin(TaskSystemParallelThreadPoolSpinning* task) {
    while (true) {
        lock.lock();
        if (task->num_tasks_completed == task->num_total_tasks) {
            //printf("notifying\n");
            cv.notify_all();
        }
        if (task->q.empty()) {
            bool done = task->done;
            lock.unlock();
            if (done) break;
            continue;
        }
        int cur_task = task->q.front();
        task->q.pop_front();
        //printf("size of queue is %ld\n", task->q.size());
        lock.unlock();
        //printf("calling runTask with %d and %d\n", cur_task, task->num_total_tasks);
        task->runnable->runTask(cur_task, task->num_total_tasks);
        lock.lock();
        task->num_tasks_completed += 1;
        //printf("num tasks completed is %d\n", task->num_tasks_completed);
        lock.unlock();
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->done = false;
    this->num_threads = num_threads;
    this->threads = std::vector<std::thread>();
    for (int i = 0; i < this->num_threads; i++) {
        this->threads.push_back(std::thread(threadTaskSpin, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    lock.lock();
    this->done = true;
    lock.unlock();
    for (int i = 0; i < this->num_threads; i++) {
        //printf("joining threads\n");
        this->threads[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    lock.lock();
    this->num_tasks_completed = 0;
    this->num_total_tasks = num_total_tasks;
    this->runnable = runnable;
    //printf("run called\n");
    for (int i = 0; i < num_total_tasks; i++) {
        this->q.push_back(i);
    }
    lock.unlock();

    while(true) {
        lock.lock();
        cv.wait(lock);
        bool done = this->num_tasks_completed == this->num_total_tasks;
        //printf("done condition checked\n");
        lock.unlock();
        if (done) break;
    }
    //printf("returning\n");

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void threadTaskSleep(TaskSystemParallelThreadPoolSleeping* task) {
    while (true) {
        lock.lock();
        if (task->num_tasks_completed == task->num_total_tasks) {
            //printf("notifying\n");
            cv.notify_all();
        }
        while (task->q.empty()) {
            //printf("thread checking done\n");
            if (task->done) {
                lock.unlock();
                return;
            }
            cv_thread.wait(lock);
        }
        int cur_task = task->q.front();
        task->q.pop_front();
        //printf("size of queue is %ld\n", task->q.size());
        lock.unlock();
        //printf("calling runTask with %d and %d\n", cur_task, task->num_total_tasks);
        task->runnable->runTask(cur_task, task->num_total_tasks);
        lock.lock();
        task->num_tasks_completed += 1;
        //printf("num tasks completed is %d\n", task->num_tasks_completed);
        lock.unlock();
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->done = false;
    this->num_threads = num_threads;
    this->threads = std::vector<std::thread>();
    for (int i = 0; i < this->num_threads; i++) {
        this->threads.push_back(std::thread(threadTaskSleep, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    lock.lock();
    this->done = true;
    cv_thread.notify_all();
    lock.unlock();
    for (int i = 0; i < this->num_threads; i++) {
        //printf("joining threads\n");
        this->threads[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    lock.lock();
    this->num_tasks_completed = 0;
    this->num_total_tasks = num_total_tasks;
    this->runnable = runnable;
    //printf("run called\n");
    for (int i = 0; i < num_total_tasks; i++) {
        this->q.push_back(i);
    }
    cv_thread.notify_all();
    lock.unlock();

    while(true) {
        lock.lock();
        cv.wait(lock);
        bool done = this->num_tasks_completed == this->num_total_tasks;
        //printf("done condition checked\n");
        lock.unlock();
        if (done) break;
    }
    //printf("returning\n");
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
