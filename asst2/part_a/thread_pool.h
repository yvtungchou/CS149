#ifndef _THREADPOOL_SPIN_H
#define _THREADPOOL_SPIN_H

#include "itasksys.h"
#include <mutex>
#include <thread>
#include <queue>
#include <unordered_set>
#include <unordered_map>

/*
    To avoid ambiguity, here I redefine the terms. Task refers to the bigger granularity task, while work is a part of the task that was split out
    to some thread.
*/
struct Work {
    IRunnable* runnable;
    int work_id;
    TaskID task_id;
    int num_total_works;
    std::vector<int> deps;

    Work() {};

    Work(IRunnable* runnable_in, int i_in, int num_total_works_in) 
    : runnable(runnable_in), work_id(i_in), num_total_works(num_total_works_in), deps({}) {}

    Work(IRunnable* runnable_in, int i_in, TaskID task_id_in, int num_total_works_in, std::vector<int> deps_in) 
    : runnable(runnable_in), work_id(i_in), task_id(task_id_in), num_total_works(num_total_works_in), deps(deps_in) {}
};

class ThreadPool {
public:
    ThreadPool(int num_threads_in);

    void push(Work task);

    void run();

    void sync();

    ~ThreadPool();

    int num_threads;
    int num_tasks;
    int num_tasks_finished;
    std::thread *thread_pool;
    std::queue<Work> q;
    std::mutex q_mutex;
    std::mutex t_mutex;
    std::condition_variable t_cv;
};

class ThreadPoolSleep {
public:
    ThreadPoolSleep(int num_threads_in);

    void push(Work work);

    void run();

    void sync();

    ~ThreadPoolSleep();

    int num_threads;
    int num_tasks;
    int num_tasks_finished;
    std::thread *thread_pool;
    std::queue<Work> q;
    std::mutex q_mutex;
    std::mutex t_mutex;
    std::condition_variable q_cv;
    std::condition_variable t_cv;
};

class ThreadPool_async {
public:
    ThreadPool_async(int num_threads_in);

    void thread_run();

    void push(Work work);

    TaskID run_task(IRunnable* runnable, int num_total_works, const std::vector<TaskID>& deps={});

    void sync(int task_id);

    void sync_all();

    ~ThreadPool_async();

    int num_threads;
    TaskID task_counter;
    std::unordered_set<TaskID> tasks;
    std::unordered_set<TaskID> finished_tasks;
    std::unordered_map<TaskID, int> num_works;
    std::unordered_map<TaskID, int> num_works_finished;
    std::thread *thread_pool;
    std::queue<Work> q;
    std::mutex q_mutex;
    std::mutex t_mutex;
    std::condition_variable q_cv;
    std::unordered_map<TaskID, std::condition_variable> t_cv;
};

#endif
