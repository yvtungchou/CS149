#ifndef _THREADPOOL_SPIN_H
#define _THREADPOOL_SPIN_H

#include "itasksys.h"
#include <mutex>
#include <thread>
#include <queue>
#include <unordered_set>
#include <unordered_map>

struct Task {
    IRunnable* runnable;
    int i;
    int num_total_tasks;
    std::vector<int> deps;

    Task() {};

    Task(IRunnable* runnable_in, int i_in, int num_total_tasks_in) 
    : runnable(runnable_in), i(i_in), num_total_tasks(num_total_tasks_in), deps({}) {}
    
    Task(IRunnable* runnable_in, int i_in, int num_total_tasks_in, std::vector<int> deps_in) 
    : runnable(runnable_in), i(i_in), num_total_tasks(num_total_tasks_in), deps(deps_in) {}
};

class ThreadPool {
public:
    ThreadPool(int num_threads_in);

    void push(Task task);

    void run();

    void sync();

    ~ThreadPool();

    int num_threads;
    int num_tasks;
    int num_tasks_finished;
    std::thread *thread_pool;
    std::queue<Task> q;
    std::mutex q_mutex;
    std::mutex t_mutex;
    std::condition_variable t_cv;
};

class ThreadPoolSleep {
public:
    ThreadPoolSleep(int num_threads_in);

    void push(Task task);

    void run();

    void sync();

    ~ThreadPoolSleep();

    int num_threads;
    int num_tasks;
    int num_tasks_finished;
    std::thread *thread_pool;
    std::queue<Task> q;
    std::mutex q_mutex;
    std::mutex t_mutex;
    std::condition_variable q_cv;
    std::condition_variable t_cv;
};

class ThreadPool_async {
public:
    ThreadPool_async(int num_threads_in);

    void push(Task task);

    void run();

    void sync(int task_id);

    void sync_all();

    ~ThreadPool_async();

    int num_threads;
    std::unordered_set<int> tasks;
    std::unordered_map<int, bool> tasks_finished;
    std::thread *thread_pool;
    std::queue<Task> q;
    std::mutex q_mutex;
    std::mutex t_mutex;
    std::condition_variable q_cv;
    std::unordered_map<int, std::condition_variable> t_cv;
};

#endif
