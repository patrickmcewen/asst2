#include "tasksys.h"
#include <thread>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <map>
#include "CycleTimer.h"

std::mutex lock;
std::mutex inc_lock;
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
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync()
{
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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


void taskAdd(TaskID task_id, int num_total_tasks, TaskSystemParallelThreadPoolSleeping *task) {
    for (int i = 0; i < num_total_tasks; i++)
    {
        task->q.push_back(std::make_pair(i, task_id));
    }
}

void threadTaskSleep(TaskSystemParallelThreadPoolSleeping *task)
{
    //std::vector<TaskID> add_to_roots;
    while (true)
    {
        lock.lock();

        while (task->q.empty())
        {
            if (task->done)
            {
                lock.unlock();
                return;
            }
            //task->waiting++;
            //printf("thread waiting for work\n");
            cv_thread.wait(lock);
            //task->waiting--;
        }
        std::pair<int, TaskID> task_info = task->q.front();
        task->q.pop_front();
        int cur_task = task_info.first;
        TaskID cur_task_id = task_info.second;
        lock.unlock();
        //printf("thread retrieved task number %d for task id %d\n", cur_task, cur_task_id);
        task->runnables[cur_task_id].runnable->runTask(cur_task, task->runnables[cur_task_id].num_total_tasks);
        //printf("task number %d completed for task id %d\n", cur_task, cur_task_id);
        inc_lock.lock();
        task->runnables[cur_task_id].num_tasks_completed += 1;
        //printf("tasks for id %d completed are %d versus %d total\n", cur_task_id, task->runnables[cur_task_id].num_tasks_completed, task->runnables[cur_task_id].num_total_tasks);
        bool done_with_task = task->runnables[cur_task_id].num_tasks_completed == task->runnables[cur_task_id].num_total_tasks;
        inc_lock.unlock();
        if (done_with_task)
        {
            //double start_time = CycleTimer::currentSeconds();
            lock.lock();
            task->cur_tasks.erase(cur_task_id);
            for (TaskID t: task->runnables[cur_task_id].outgoing) {
                if (--task->runnables[t].in_degree == 0) {
                    taskAdd(t, task->runnables[t].num_total_tasks, task);
                    cv_thread.notify_all();
                }
            }
            //printf("erasing task from set\n");
            if (task->waiting_for_sync && task->cur_tasks.empty())
            {
                //printf("all tasks done for now\n");
                cv.notify_all();
            }
            //task->total_time += CycleTimer::currentSeconds() - start_time;
            lock.unlock();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->done = false;
    this->num_threads = num_threads;
    this->waiting_for_sync = false;
    this->threads = std::vector<std::thread>();
    this->total_time = 0;
    // this->threads.push_back(std::thread(threadTaskDistribute, this));
    for (int i = 0; i < this->num_threads; i++)
    {
        this->threads.push_back(std::thread(threadTaskSleep, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    lock.lock();
    //printf("marking done\n");
    this->done = true;
    cv_thread.notify_all();
    // cv_work.notify_one();
    lock.unlock();
    for (int i = 0; i < this->num_threads; i++)
    {
        //printf("joining threads\n");
        this->threads[i].join();
    }
    //printf("total time spent in crit area is %f\n", this->total_time);
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CS149 students will implement this method in Part B.
    //
    TaskID task_id = this->cur_task_id;
    this->cur_task_id += 1;
    lock.lock();
    int in_degree = 0;
    for (TaskID t: deps) {
        if (this->cur_tasks.count(t)) {
            this->runnables[t].outgoing.insert(task_id);
            in_degree++;
        }
    }
    this->cur_tasks.insert(task_id);
    runnableInfo runnable_info = {
        runnable,
        deps,
        num_total_tasks,
        0, 
        {},
        in_degree
    };
    this->runnables[task_id] = runnable_info;
    if (!in_degree) {
        //printf("adding to queue from runAsync\n");
        taskAdd(task_id, num_total_tasks, this);
        cv_thread.notify_all();
    }

    //printf("task_id %d assigned\n", task_id);
    lock.unlock();
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    lock.lock();
    this->waiting_for_sync = true;
    while (!this->cur_tasks.empty())
    {
        //printf("waiting for threads to finish tasks\n");
        // cv_thread.notify_all();
        cv.wait(lock);
    }
    this->waiting_for_sync = false;
    lock.unlock();
    //printf("threads finished tasks, returning sync\n");
}
