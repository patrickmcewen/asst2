#include "tasksys.h"
#include <thread>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <map>
#include "CycleTimer.h"

std::mutex lock;
std::mutex task_finished_lock;
std::mutex q_lock;
std::condition_variable_any cv;
std::condition_variable_any cv_thread;
std::condition_variable_any cv_work;

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

void addToQueue(std::deque<std::pair<int, TaskID>> &q, TaskSystemParallelThreadPoolSleeping *task)
{
    // bool added_work = false;
    std::vector<TaskID> tasks_to_remove;
    int ind = 0;
    while (ind < (int)task->waiting_tasks.size()) {
        TaskID cur_task_id = task->waiting_tasks.front();
        task->waiting_tasks.pop_front();
        runnableInfo runnable_info = task->runnables[cur_task_id];
        bool can_add = true;
        for (TaskID taskID : runnable_info.deps)
        {
            if (task->cur_tasks.count(taskID))
            {
                can_add = false;
            }
        }
        // printf("can add value is %d\n", can_add);
        if (can_add)
        {
            // added_work = true;
            for (int i = 0; i < runnable_info.num_total_tasks; i++)
            {
                q.push_back(std::make_pair(i, cur_task_id));
            }
            tasks_to_remove.push_back(cur_task_id);
            break;
        }
        task->waiting_tasks.push_back(cur_task_id);
        ind++;
    }
    /*for (TaskID cur_task_id : task->waiting_tasks)
    {
        runnableInfo runnable_info = task->runnables[cur_task_id];
        bool can_add = true;
        for (TaskID taskID : runnable_info.deps)
        {
            if (task->cur_tasks.count(taskID))
            {
                can_add = false;
            }
        }
        // printf("can add value is %d\n", can_add);
        if (can_add)
        {
            // added_work = true;
            for (int i = 0; i < runnable_info.num_total_tasks; i++)
            {
                q.push_back(std::make_pair(i, cur_task_id));
            }
            tasks_to_remove.push_back(cur_task_id);
            break;
        }
    }
    for (TaskID task_id : tasks_to_remove)
    {
        task->waiting_tasks.erase(task_id);
    }*/
    /*if (!added_work) {
        printf("didnt add any work to queue\n");
    }*/
}

/*void threadTaskDistribute(TaskSystemParallelThreadPoolSleeping* task) {
    while (true) {
        lock.lock();
        while(!task->work_to_add) {
            if (task->done) {
                lock.unlock();
                return;
            }
            cv_work.wait(lock);
        }
        //printf("adding work to queue\n");
        addToQueue(task->q, task);
        //printf("size of queue is %ld\n", task->q.size());
        task->work_to_add = false;
        cv_thread.notify_all();
        lock.unlock();
    }
}*/

void threadTaskSleep(TaskSystemParallelThreadPoolSleeping *task)
{
    while (true)
    {
        lock.lock();
        while (task->q.empty() || task->work_to_add)
        {
            if (task->done)
            {
                lock.unlock();
                return;
            }
            if (task->work_to_add)
            {
                task->work_to_add = false;
                //q_lock.lock();
                //double start_time = CycleTimer::currentSeconds();
                addToQueue(task->q, task);
                //double end_time = CycleTimer::currentSeconds();
                //task->total_time += end_time - start_time;
                //q_lock.unlock();
            }
            if (task->q.empty())
            {
                // printf("thread waiting for work\n");
                cv_thread.wait(lock);
            }
        }
        //lock.unlock();
        // q_lock.lock();
        std::pair<int, TaskID> task_info = task->q.front();
        task->q.pop_front();
        // q_lock.unlock();
        int cur_task = task_info.first;
        TaskID cur_task_id = task_info.second;
        lock.unlock();
        // printf("thread retrieved task number %d for task id %d\n", cur_task, cur_task_id);
        task->runnables[cur_task_id].runnable->runTask(cur_task, task->runnables[cur_task_id].num_total_tasks);
        task_finished_lock.lock();
        // printf("task number %d completed for task id %d\n", cur_task, cur_task_id);
        task->runnables[cur_task_id].num_tasks_completed += 1;
        // printf("tasks for id %d completed are %d versus %d total\n", cur_task_id, task->runnables[cur_task_id].num_tasks_completed, task->runnables[cur_task_id].num_total_tasks);
        if (task->runnables[cur_task_id].num_tasks_completed == task->runnables[cur_task_id].num_total_tasks)
        {
            task->cur_tasks.erase(cur_task_id);
            // printf("erasing task from set\n");
            task->work_to_add = true;
            cv_thread.notify_all();
            if (task->waiting_for_sync && task->q.empty())
            {
                // printf("all tasks done for now\n");
                cv.notify_all();
            }
        }
        task_finished_lock.unlock();
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
    this->work_to_add = false;
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
    // printf("marking done\n");
    this->done = true;
    cv_thread.notify_all();
    // cv_work.notify_one();
    lock.unlock();
    for (int i = 0; i < this->num_threads; i++)
    {
        // printf("joining threads\n");
        this->threads[i].join();
    }
    //printf("total time spend in addToQueue is %f\n", this->total_time);
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
    lock.lock();
    TaskID task_id = this->cur_task_id;
    this->cur_task_id += 1;
    //q_lock.lock();
    this->waiting_tasks.push_back(task_id);
    //q_lock.unlock();
    runnableInfo runnable_info = {
        runnable,
        deps,
        num_total_tasks,
        0
    };
    this->runnables[task_id] = runnable_info;
    task_finished_lock.lock();
    if (this->cur_tasks.empty() || deps.size() == 0) 
    {
        this->work_to_add = true;
        cv_thread.notify_all();
    }
    this->cur_tasks.insert(task_id);
    task_finished_lock.unlock();
    // addToQueue(this->q, this);
    // printf("task_id %d assigned\n", task_id);
    lock.unlock();
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    task_finished_lock.lock();
    this->waiting_for_sync = true;
    while (!this->cur_tasks.empty())
    {
        // printf("waiting for threads to finish tasks\n");
        // cv_thread.notify_all();
        cv.wait(task_finished_lock);
    }
    this->waiting_for_sync = false;
    task_finished_lock.unlock();
    // printf("threads finished tasks, returning sync\n");
}
