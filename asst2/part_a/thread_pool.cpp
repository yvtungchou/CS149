#include "thread_pool.h"

ThreadPool::ThreadPool(int num_threads_in) {
    num_threads = num_threads_in;
    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&ThreadPool::run, this);
    }
}

void ThreadPool::run() {
    while (true) {
        Task curr_task;
        bool do_task = false;
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            if (!q.empty()) {
                curr_task = q.front();
                q.pop();
                do_task = true;
            }
        }
        if (do_task) {
            if (curr_task.i == -1) break;
            curr_task.runnable->runTask(curr_task.i, curr_task.num_total_tasks);
            std::unique_lock<std::mutex> lk(t_mutex);
            num_tasks_finished++;
            t_cv.notify_one();
        }
    }
}

void ThreadPool::push(Task task) {
    std::unique_lock<std::mutex> lk(q_mutex);
    q.push(task);
    num_tasks++;
}

void ThreadPool::sync() {
    std::unique_lock<std::mutex> lk(t_mutex);
    while (num_tasks_finished < num_tasks) {
        t_cv.wait(lk);
    }
    num_tasks = 0;
    num_tasks_finished = 0;
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lk(q_mutex);
        for (int i = 0; i < num_threads; i++) {
            q.push(Task(nullptr, -1, -1));
        }
    }

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }
    delete[] thread_pool;
}

ThreadPoolSleep::ThreadPoolSleep(int num_threads_in) {
    num_threads = num_threads_in;
    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&ThreadPoolSleep::run, this);
    }
}

void ThreadPoolSleep::run() {
    while (true) {
        Task curr_task;
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            while (q.empty()) {
                q_cv.wait(lk);
            }
            curr_task = q.front();
            q.pop();
        }
        if (curr_task.i == -1) break;
        curr_task.runnable->runTask(curr_task.i, curr_task.num_total_tasks);
        {
            std::unique_lock<std::mutex> lk(t_mutex);
            num_tasks_finished++;
        }
        t_cv.notify_one();
    }
}

void ThreadPoolSleep::push(Task task) {
    {
        std::unique_lock<std::mutex> lk(q_mutex);
        q.push(task);
        num_tasks++;
    }
    q_cv.notify_one();
}

void ThreadPoolSleep::sync() {
    std::unique_lock<std::mutex> lk(t_mutex);
    while (num_tasks_finished < num_tasks) {
        t_cv.wait(lk);
    }
    num_tasks = 0;
    num_tasks_finished = 0;
}

ThreadPoolSleep::~ThreadPoolSleep() {
    {
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            for (int i = 0; i < num_threads; i++) {
                q.push(Task(nullptr, -1, -1));
            }
        }
        q_cv.notify_all();
    }
    
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }
    delete[] thread_pool;
}

/*
    ThreadPool_async
*/

ThreadPool_async::ThreadPool_async(int num_threads_in) {
    num_threads = num_threads_in;
    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&ThreadPool_async::run, this);
    }
}

void ThreadPool_async::run() {
    while (true) {
        Task curr_task;
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            while (q.empty()) {
                q_cv.wait(lk);
            }
            curr_task = q.front();
            q.pop();
        }
        if (curr_task.i == -1) break;
        curr_task.runnable->runTask(curr_task.i, curr_task.num_total_tasks);
        {
            std::unique_lock<std::mutex> lk(t_mutex);
            tasks_finished[curr_task.i] = true;
        }
        t_cv[curr_task.i].notify_one();
    }
}

void ThreadPool_async::push(Task task) {
    {
        std::unique_lock<std::mutex> lk_q(q_mutex);
        q.push(task);
        std::unique_lock<std::mutex> lk_t(t_mutex);
        tasks.insert(task.i);
        tasks_finished[task.i] = false;
        t_cv[task.i];
    }
    q_cv.notify_one();
}

void ThreadPool_async::sync(int task_id) {
    std::unique_lock<std::mutex> lk(t_mutex);
    while (!tasks_finished[task_id]) {
        t_cv[task_id].wait(lk);
    }
    tasks.erase(task_id);
}

void ThreadPool_async::sync_all() {
    std::vector<int> to_sync(tasks.begin(), tasks.end());
    for (auto id : to_sync) {
        sync(id);
    }
}

ThreadPool_async::~ThreadPool_async() {
    {
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            for (int i = 0; i < num_threads; i++) {
                q.push(Task(nullptr, -1, -1));
            }
        }
        q_cv.notify_all();
    }
    
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }
    delete[] thread_pool;
}