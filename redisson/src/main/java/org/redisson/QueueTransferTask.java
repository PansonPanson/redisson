/**
 * Copyright (c) 2013-2022 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.RFuture;
import org.redisson.api.RTopic;
import org.redisson.api.listener.BaseStatusListener;
import org.redisson.api.listener.MessageListener;
import org.redisson.connection.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public abstract class QueueTransferTask {
    
    private static final Logger log = LoggerFactory.getLogger(QueueTransferTask.class);

    public static class TimeoutTask {
        
        private final long startTime;
        private final Timeout task;
        
        public TimeoutTask(long startTime, Timeout task) {
            super();
            this.startTime = startTime;
            this.task = task;
        }
        
        public long getStartTime() {
            return startTime;
        }
        
        public Timeout getTask() {
            return task;
        }
        
    }
    
    private int usage = 1;
    private final AtomicReference<TimeoutTask> lastTimeout = new AtomicReference<TimeoutTask>();
    private final ServiceManager serviceManager;
    
    public QueueTransferTask(ServiceManager serviceManager) {
        super();
        this.serviceManager = serviceManager;
    }

    public void incUsage() {
        usage++;
    }
    
    public int decUsage() {
        usage--;
        return usage;
    }
    
    private int messageListenerId;
    private int statusListenerId;
    
    public void start() {
        // 调用的就是RedissonDelayedQueue构造方法里面定义的getTopic()方法
        // 注册两个Listener监听事件：1.onSubscribe(订阅监听) 2.onMessage(消息监听)
        RTopic schedulerTopic = getTopic();
        statusListenerId = schedulerTopic.addListener(new BaseStatusListener() {
            @Override
            public void onSubscribe(String channel) {
                pushTask();
            }
        });
        
        messageListenerId = schedulerTopic.addListener(Long.class, new MessageListener<Long>() {
            @Override
            public void onMessage(CharSequence channel, Long startTime) {
                scheduleTask(startTime);
            }
        });
    }
    
    public void stop() {
        RTopic schedulerTopic = getTopic();
        schedulerTopic.removeListener(messageListenerId);
        schedulerTopic.removeListener(statusListenerId);
    }

    /**
     *
     * @param startTime pushTaskAsync 方法返回的时间
     */
    private void scheduleTask(final Long startTime) {
        TimeoutTask oldTimeout = lastTimeout.get();
        if (startTime == null) {
            return;
        }

        // 取消上一个定时任务
        if (oldTimeout != null) {
            oldTimeout.getTask().cancel();
        }

        // 如果任务过期时间减去系统当前时间大于 10 毫秒，说明任务还没有过期
        long delay = startTime - System.currentTimeMillis();
        if (delay > 10) {
            // 定时时间 = 剩余的过期时间
            Timeout timeout = serviceManager.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    pushTask();
                    
                    TimeoutTask currentTimeout = lastTimeout.get();
                    if (currentTimeout.getTask() == timeout) {
                        lastTimeout.compareAndSet(currentTimeout, null);
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
            if (!lastTimeout.compareAndSet(oldTimeout, new TimeoutTask(startTime, timeout))) {
                timeout.cancel();
            }
        } else {
            // 如果任务过期时间减去系统当前时间小于等于 10 毫秒，则将过期数据放入目标延迟队列里面
            pushTask();
        }
    }
    
    protected abstract RTopic getTopic();
    
    protected abstract RFuture<Long> pushTaskAsync();
    
    private void pushTask() {
        // 调用的就是RedissonDelayedQueue构造方法里面定义的 pushTaskAsync 方法
        RFuture<Long> startTimeFuture = pushTaskAsync();
        // 返回最先过期的任务的时间
        startTimeFuture.whenComplete((res, e) -> {
            if (e != null) {
                if (e instanceof RedissonShutdownException) {
                    return;
                }
                log.error(e.getMessage(), e);
                scheduleTask(System.currentTimeMillis() + 5 * 1000L);
                return;
            }

            // res 为返回值
            if (res != null) {
                scheduleTask(res);
            }
        });
    }

}
