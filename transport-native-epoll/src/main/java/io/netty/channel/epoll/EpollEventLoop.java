/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.epoll;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * {@link EventLoop} which uses epoll under the covers. Only works on Linux!
 */
final class EpollEventLoop extends SingleThreadEventLoop {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(EpollEventLoop.class);

    private final int epfd;
    private final int evfd;
    private int id;
    @SuppressWarnings("unused")
    private volatile int wakenUp;
    private static final AtomicIntegerFieldUpdater<EpollEventLoop> WAKEN_UP_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(EpollEventLoop.class, "wakenUp");
    private int oldWakenUp;
    private boolean overflown;

    private static final int IO_RATIO = 50;
    private final Map<Integer, AbstractEpollChannel> ids = new HashMap<Integer, AbstractEpollChannel>();
    private final long[] events;

    EpollEventLoop(EventLoopGroup parent, ThreadFactory threadFactory, int maxEvents) {
        super(parent, threadFactory, false);
        events = new long[maxEvents];
        boolean success = false;
        int epfd = -1;
        int evfd = -1;
        try {
            this.epfd = epfd = Native.epollCreate();
            this.evfd = evfd = Native.eventFd();
            Native.epollCtlAdd(epfd, evfd, Native.EPOLLIN, 0);
            success = true;
        } finally {
            if (!success) {
                if (epfd != -1) {
                    try {
                        Native.close(epfd);
                    } catch (Exception e) {
                        // ignore
                    }
                }
                if (evfd != -1) {
                    try {
                        Native.close(evfd);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    private int nextId() {
        int id = this.id;
        if (id == Integer.MAX_VALUE) {
            overflown = true;
            id = 0;
        }
        if (overflown) {
            // the ids had an overflow before so we need to make sure the id is not in use atm before assign
            // it.
            for (;;) {
                if (!ids.containsKey(++id)) {
                    this.id = id;
                    break;
                }
            }
        } else {
            this.id = ++id;
        }
        return id;
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && WAKEN_UP_UPDATER.compareAndSet(this, 0, 1)) {
            // write to the evfd which will then wake-up epoll_wait(...)
            Native.eventFdWrite(evfd, 1L);
        }
    }

    /**
     * Register the given epoll with this {@link io.netty.channel.EventLoop}.
     */
    void add(AbstractEpollChannel ch) {
        assert inEventLoop();
        int id = nextId();
        Native.epollCtlAdd(epfd, ch.fd, ch.flags, id);
        ch.id = id;
        ids.put(id, ch);
    }

    /**
     * The flags of the given epoll was modified so update the registration
     */
    void modify(AbstractEpollChannel ch) {
        assert inEventLoop();
        Native.epollCtlMod(epfd, ch.fd, ch.flags, ch.id);
    }

    /**
     * Deregister the given epoll from this {@link io.netty.channel.EventLoop}.
     */
    void remove(AbstractEpollChannel ch) {
        assert inEventLoop();
        if (ids.remove(ch.id) != null && ch.isOpen()) {
            // Remove the epoll. This is only needed if it's still open as otherwise it will be automatically
            // removed once the file-descriptor is closed.
            Native.epollCtlDel(epfd, ch.fd);
        }
    }

    @Override
    protected Queue<Runnable> newTaskQueue() {
        // This event loop never calls takeTask()
        return new ConcurrentLinkedQueue<Runnable>();
    }

    private int epollWait() {
        int selectCnt = 0;
        long currentTimeNanos = System.nanoTime();
        long selectDeadLineNanos = currentTimeNanos + delayNanos(currentTimeNanos);
        for (;;) {
            long timeoutMillis = (selectDeadLineNanos - currentTimeNanos + 500000L) / 1000000L;
            if (timeoutMillis <= 0) {
                if (selectCnt == 0) {
                    int ready = Native.epollWait(epfd, events, 0);
                    if (ready > 0) {
                        return ready;
                    }
                }
                break;
            }

            int selectedKeys = Native.epollWait(epfd, events, (int) timeoutMillis);
            selectCnt ++;

            if (selectedKeys != 0 || oldWakenUp == 1 || wakenUp == 1 || hasTasks()) {
                // Selected something,
                // waken up by user, or
                // the task queue has a pending task.
                return selectedKeys;
            }
            currentTimeNanos = System.nanoTime();
        }
        return 0;
    }

    @Override
    protected void run() {
        for (;;) {
            oldWakenUp = WAKEN_UP_UPDATER.getAndSet(this, 0);
            try {
                int ready;
                if (hasTasks()) {
                    // Non blocking just return what is ready directly without block
                    ready = Native.epollWait(epfd, events, 0);
                } else {
                    ready = epollWait();

                    // 'wakenUp.compareAndSet(false, true)' is always evaluated
                    // before calling 'selector.wakeup()' to reduce the wake-up
                    // overhead. (Selector.wakeup() is an expensive operation.)
                    //
                    // However, there is a race condition in this approach.
                    // The race condition is triggered when 'wakenUp' is set to
                    // true too early.
                    //
                    // 'wakenUp' is set to true too early if:
                    // 1) Selector is waken up between 'wakenUp.set(false)' and
                    //    'selector.select(...)'. (BAD)
                    // 2) Selector is waken up between 'selector.select(...)' and
                    //    'if (wakenUp.get()) { ... }'. (OK)
                    //
                    // In the first case, 'wakenUp' is set to true and the
                    // following 'selector.select(...)' will wake up immediately.
                    // Until 'wakenUp' is set to false again in the next round,
                    // 'wakenUp.compareAndSet(false, true)' will fail, and therefore
                    // any attempt to wake up the Selector will fail, too, causing
                    // the following 'selector.select(...)' call to block
                    // unnecessarily.
                    //
                    // To fix this problem, we wake up the selector again if wakenUp
                    // is true immediately after selector.select(...).
                    // It is inefficient in that it wakes up the selector for both
                    // the first case (BAD - wake-up required) and the second case
                    // (OK - no wake-up required).

                    if (wakenUp == 1) {
                        Native.eventFdWrite(evfd, 1L);
                    }
                }

                final long ioStartTime = System.nanoTime();
                if (ready > 0) {
                    processReady(events, ready);
                }

                final long ioTime = System.nanoTime() - ioStartTime;

                final int ioRatio = IO_RATIO;
                runAllTasks(ioTime * (100 - ioRatio) / ioRatio);

                if (isShuttingDown()) {
                    closeAll();
                    if (confirmShutdown()) {
                        break;
                    }
                }
            } catch (Throwable t) {
                logger.warn("Unexpected exception in the selector loop.", t);

                // Prevent possible consecutive immediate failures that lead to
                // excessive CPU consumption.
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
        }
    }

    private void closeAll() {
        int ready = Native.epollWait(epfd, events, 0);
        Collection<AbstractEpollChannel> channels = new ArrayList<AbstractEpollChannel>(ready);

        for (int i = 0; i < ready; i++) {
            final long ev = events[i];

            int id = (int) (ev >> 32L);
            AbstractEpollChannel ch = ids.get(id);
            if (ch != null) {
                channels.add(ids.get(id));
            }
        }

        for (AbstractEpollChannel ch: channels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    private void processReady(long[] events, int ready) {
        for (int i = 0; i < ready; i ++) {
            final long ev = events[i];

            int id = (int) (ev >> 32L);
            if (id == 0) {
                // consume wakeup event
                Native.eventFdRead(evfd);
            } else {
                boolean read = (ev & Native.EPOLLIN) != 0;
                boolean write = (ev & Native.EPOLLOUT) != 0;
                boolean close = (ev & Native.EPOLLRDHUP) != 0;

                AbstractEpollChannel ch = ids.get(id);
                if (ch != null) {
                    AbstractEpollChannel.NativeUnsafe unsafe = (AbstractEpollChannel.NativeUnsafe) ch.unsafe();
                    if (write) {
                        // force flush of data as the epoll is writable again
                        unsafe.epollOutReady();
                    }
                    if (read) {
                        // Something is ready to read, so consume it now
                        unsafe.epollInReady();
                    }
                    if (close) {
                        unsafe.epollRdHupReady();
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup() {
        try {
            Native.close(epfd);
        } catch (IOException e) {
            logger.warn("Failed to close the epoll fd.", e);
        }
        try {
            Native.close(evfd);
        } catch (IOException e) {
            logger.warn("Failed to close the event fd.", e);
        }
    }
}
