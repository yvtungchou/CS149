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
        Work curr_task;
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
            if (curr_task.work_id == -1) break;
            curr_task.runnable->runTask(curr_task.work_id, curr_task.num_total_works);
            std::unique_lock<std::mutex> lk(t_mutex);
            num_tasks_finished++;
            t_cv.notify_one();
        }
    }
}

void ThreadPool::push(Work task) {
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
            q.push(Work(nullptr, -1, -1));
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
        Work curr_task;
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            while (q.empty()) {
                q_cv.wait(lk);
            }
            curr_task = q.front();
            q.pop();
        }
        if (curr_task.work_id == -1) break;
        curr_task.runnable->runTask(curr_task.work_id, curr_task.num_total_works);
        {
            std::unique_lock<std::mutex> lk(t_mutex);
            num_tasks_finished++;
        }
        t_cv.notify_one();
    }
}

void ThreadPoolSleep::push(Work task) {
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
                q.push(Work(nullptr, -1, -1));
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
    task_counter = 0;
    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&ThreadPool_async::thread_run, this);
    }
}

void ThreadPool_async::thread_run() {
    while (true) {
        Work curr_work;
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            while (q.empty()) {
                q_cv.wait(lk);
            }
            curr_work = q.front();
            q.pop();
        }
        if (curr_work.work_id == -1) break;
        for (auto prereq : curr_work.deps) sync(prereq);
        curr_work.runnable->runTask(curr_work.work_id, curr_work.num_total_works);
        {
            std::unique_lock<std::mutex> lk(t_mutex);
            num_works_finished[curr_work.task_id]++;
        }
        t_cv[curr_work.task_id].notify_one();
    }
}

TaskID ThreadPool_async::run_task(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    tasks.insert(task_counter);
    num_works[task_counter] = 0;
    num_works_finished[task_counter] = 0;
    for (int i = 0; i < num_total_tasks; i++) {
        push(Work(runnable, i, task_counter, num_total_tasks, deps));
    }
    return task_counter++;
}

void ThreadPool_async::push(Work work) {
    {
        std::unique_lock<std::mutex> lk_q(q_mutex);
        q.push(work);
        std::unique_lock<std::mutex> lk_t(t_mutex);
        num_works[work.task_id]++;
    }
    q_cv.notify_one();
}

void ThreadPool_async::sync(TaskID task_id) {
    std::unique_lock<std::mutex> lk(t_mutex);
    while ((finished_tasks.find(task_id) == finished_tasks.end()) && (num_works_finished[task_id] < num_works[task_id])) {
        t_cv[task_id].wait(lk);
    }
    finished_tasks.insert(task_id);
    tasks.erase(task_id);
    num_works.erase(task_id);
    num_works_finished.erase(task_id);
}

void ThreadPool_async::sync_all() {
    std::vector<TaskID> to_sync(tasks.begin(), tasks.end());
    for (auto id : to_sync) {
        sync(id);
    }
}

ThreadPool_async::~ThreadPool_async() {
    {
        {
            std::unique_lock<std::mutex> lk(q_mutex);
            for (int i = 0; i < num_threads; i++) {
                q.push(Work(nullptr, -1, -1));
            }
        }
        q_cv.notify_all();
    }
    
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }
    delete[] thread_pool;
}