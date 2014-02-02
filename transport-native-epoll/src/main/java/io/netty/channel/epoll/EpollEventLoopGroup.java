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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

import java.util.concurrent.ThreadFactory;

/**
 * {@link EventLoopGroup} which uses epoll under the covers. Because of this
 * it only works on linux.
 */
public final class EpollEventLoopGroup extends MultithreadEventLoopGroup {

    public EpollEventLoopGroup() {
        super(0, null);
    }

    public EpollEventLoopGroup(int nThreads) {
        super(nThreads, null);
    }

    public EpollEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        super(nThreads, threadFactory);
    }

    public EpollEventLoopGroup(int nThreads, ThreadFactory threadFactory, int maxEvents) {
        super(nThreads, threadFactory, maxEvents);
    }

    @Override
    protected EventExecutor newChild(ThreadFactory threadFactory, Object... args) throws Exception {
        int maxEvents;
        if (args.length == 1) {
            maxEvents = (Integer) args[0];
        } else {
            maxEvents = 128;
        }
        return new EpollEventLoop(this, threadFactory, maxEvents);
    }
}
