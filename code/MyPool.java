package com.huahuadan.test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liuyichen
 * @version 1.0
 */
@Slf4j
public class MyPool {
    public static void main(String[] args) {
        ThreadPool threadPool = new ThreadPool(2,
                1000, TimeUnit.MILLISECONDS, 8, (queue, task)->{
            // 1. 死等
            queue.put(task);
            // 2) 带超时等待
//            queue.offer(task, 1500, TimeUnit.MILLISECONDS);
            // 3) 让调用者放弃任务执行
//            log.debug("放弃{}", task);
            // 4) 让调用者抛出异常
//            throw new RuntimeException("任务执行失败 " + task);
            // 5) 让调用者自己执行任务
//            task.run();
        });
        for (int i = 0; i < 8; i++) {
            int j = i;
            threadPool.execute(() -> {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.debug("{}", j);
            });
        }


    }


}
@Slf4j
class ThreadPool{
    // 任务队列
    private BlockedQueue<Runnable> taskQueue;

    // 线程集合
    private HashSet<Worker> workers = new HashSet<>();

    // 核心线程数
    private int coreSize;

    // 获取任务时的超时时间
    private long timeout;

    private TimeUnit timeUnit;

    private RejectPolicy<Runnable> rejectPolicy;

    public ThreadPool(int coreSize, long timeout, TimeUnit timeUnit, int queueCapacity, RejectPolicy<Runnable> rejectPolicy) {
        this.coreSize = coreSize;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.taskQueue = new BlockedQueue<>(queueCapacity);
        this.rejectPolicy = rejectPolicy;
    }

    public void execute(Runnable task) {
        synchronized (workers) {
            if ( workers.size()<coreSize) {
                Worker worker = new Worker(task);
                log.info("新增工作线程 worker{}, {}", worker, task);
                workers.add(worker);
                worker.start();
            }else {
                taskQueue.tryPut(rejectPolicy, task);
            }

        }
    }


    class Worker extends Thread{
       private Runnable task;
       public Worker(Runnable task){
           this.task = task;
       }
       @Override
        public void run() {
           while(task != null || (task = taskQueue.poll(timeout, timeUnit)) != null) {
               try {
                   log.debug("正在执行...{}", task);
                   task.run();
               } catch (Exception e) {
                   e.printStackTrace();
               } finally {
                   task = null;
               }
           }
           synchronized (workers) {
               log.debug("worker 被移除{}", this);
               workers.remove(this);
           }
       }
    }



}
@FunctionalInterface//拒绝策略
interface RejectPolicy<T>{

    void reject(BlockedQueue<T> queue, T t);
}
@Slf4j
class BlockedQueue<T>{
    //阻塞队列
    private Deque<T> taskQueue = new ArrayDeque<>();
    //阻塞队列的大小
    private int capacity;
    //锁
    ReentrantLock lock = new ReentrantLock();
    //生产者条件变量
    Condition fullWaitSet = lock.newCondition();
    //消费者条件变量
    Condition emptyWaitSet = lock.newCondition();
    public BlockedQueue(int capacity){
        this.capacity = capacity;
    }

    //阻塞获取
    public T take(){
        lock.lock();
        try {
            while (taskQueue.isEmpty()){
                try {
                    log.info("Waiting for full wait.");
                    emptyWaitSet.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            //获取到任务

            T t = taskQueue.removeFirst();
            log.info("获取一个任务,{}",t);
            fullWaitSet.signal();
            return t;
        }finally {
            lock.unlock();
        }
    }

    //待时间的阻塞获取
    public T poll(long timeout, TimeUnit unit){
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (taskQueue.isEmpty()){
                if (nanos <= 0){
                    log.info("taskQueue is empty,time out");
                    return null;
                }
                try {
                    log.info("Waiting for empty wait(timeout={})",nanos);
                    nanos = emptyWaitSet.awaitNanos(nanos);//会返回剩余的等待时间
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            T t = taskQueue.removeFirst();
            log.info("获取到一个任务{}",t);
            fullWaitSet.signal();
            return t;
        }finally {
            lock.unlock();
        }

    }

    //阻塞添加
    public void put(T t){
        lock.lock();
        try {
            while (taskQueue.size() == capacity){
                try {
                    fullWaitSet.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("添加一个任务{}",t);
            taskQueue.addLast(t);
            emptyWaitSet.signal();
        }finally {
            lock.unlock();
        }
    }

    //带超时时间的阻塞添加
    public boolean offer(T t, long timeout, TimeUnit unit){
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (taskQueue.size() == capacity){
                if (nanos <= 0){
                    log.info("taskQueue is full,timeout");
                    return false;
                }
                try {
                    log.info("Waiting for full wait(timeout={}),task{}",nanos,t);
                    nanos = fullWaitSet.awaitNanos(nanos);//会返回剩余的等待时间
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("添加一个任务{}",t);
            taskQueue.addLast(t);
            emptyWaitSet.signal();
        }finally {
            lock.unlock();
        }
        return true;
    }
    public int size() {
        lock.lock();
        try {
            return taskQueue.size();
        } finally {
            lock.unlock();
        }
    }

    //用户自定义的阻塞添加
    public void tryPut(RejectPolicy<T> rejectPolicy, T t){
        lock.lock();
        try {
            if(taskQueue.size() == capacity){
                rejectPolicy.reject( this,t);
            }else {
                log.info("添加任务到队,{}",t);
                taskQueue.addLast(t);
                emptyWaitSet.signal();
            }

        }finally {
            lock.unlock();
        }
    }






}
