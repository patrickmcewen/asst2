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

// Add an entry with completed/no dependecies to the worker queue
void addToQueue(std::deque<std::pair<int, TaskID>> &q, TaskSystemParallelThreadPoolSleeping *task, TaskID cur_task_id)
{
    runnableInfo runnable_info = task->runnables[cur_task_id];
    // Add individual tasks from the bulk
    for (int i = 0; i < runnable_info.num_total_tasks; i++)
    {
        q.push_back(std::make_pair(i, cur_task_id));
    }
}

void threadTaskSleep(TaskSystemParallelThreadPoolSleeping *task)
{
    while (true)
    {
        lock.lock();
        while (task->q.empty() || task->work_to_add)
        {
            if (task->done)
            {
                // If the thread pool has been killed in destructor
                lock.unlock();
                return;
            }
            if (task->work_to_add)
            {
                task->work_to_add = false;
            }
            // if (task->q.empty())
            // {
            //     cv_thread.wait(lock);
            // }
        }
        std::pair<int, TaskID> task_info = task->q.front();
        task->q.pop_front();
        int cur_task = task_info.first;
        TaskID cur_task_id = task_info.second;
        lock.unlock();

        // Run the individual tasks within the bulk task
        task->runnables[cur_task_id].runnable->runTask(cur_task, task->runnables[cur_task_id].num_total_tasks);
        
        task_finished_lock.lock();
        task->runnables[cur_task_id].num_tasks_completed += 1;
        // TODO: If all tasks in the bulk task are finished, decrease in-degree
        // of adjacent dependencies and push them to ready queue.
        if (task->runnables[cur_task_id].num_tasks_completed == task->runnables[cur_task_id].num_total_tasks)
        {
            lock.lock();
            task->cur_tasks.erase(cur_task_id);
            for (TaskID nextTaskID : task->runnables[cur_task_id].outgoing_edges) {
                runnableInfo next_runnable = task->runnables[nextTaskID];
                next_runnable.deps.erase(cur_task_id); // Decrease the in-degree
                if (next_runnable.deps.size() == 0) {
                    task->cur_tasks.insert(nextTaskID);
                    addToQueue(task->q, task, nextTaskID);
                }
                // There's more work to do IFF there are more nextTaskIDs
                task->work_to_add = true;
            }
            lock.unlock();
            cv_thread.notify_all();
            if (task->waiting_for_sync && task->q.empty())
            {
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
    // Create our thread pool
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
    this->done = true;
    cv_thread.notify_all();
    lock.unlock();
    for (int i = 0; i < this->num_threads; i++)
    {
        this->threads[i].join();
    }
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

/*
    We are guaranteed for TaskIDs for tasks this one is dependent on are lower.
*/
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    lock.lock();
    TaskID task_id = this->cur_task_id;
    this->cur_task_id += 1;

    for (TaskID dependency: deps) {
        // TODO: Construct adjacency list; get the latest dependency of our current
        // task_id and append our task_id there.
        this->runnables[dependency].outgoing_edges.push_back(task_id);
    }

    std::set<TaskID> dependencies (deps.begin(), deps.end()); // Dependencies should only contain adjacent incoming edges
    std::vector<TaskID> outgoing_edges; // Initialize to empty
    runnableInfo runnable_info = {
        runnable,
        dependencies,
        outgoing_edges,
        num_total_tasks,
        0
    };

    this->runnables[task_id] = runnable_info;
    task_finished_lock.lock();
    // Insert into our topological BFS queue
    if (this->cur_tasks.empty() || deps.size() == 0)
    {
        this->work_to_add = true;
        this->cur_tasks.insert(task_id); 
        cv_thread.notify_all();
    }
    task_finished_lock.unlock();
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
        cv.wait(task_finished_lock);
    }
    this->waiting_for_sync = false;
    task_finished_lock.unlock();
}