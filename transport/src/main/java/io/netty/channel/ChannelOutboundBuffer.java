/*
 * Copyright 2013 The Netty Project
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
/*
 * Written by Josh Bloch of Google Inc. and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * (Transport implementors only) an internal data structure used by {@link AbstractChannel} to store its pending
 * outbound write requests.
 */
public final class ChannelOutboundBuffer {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);

    private static final int INITIAL_CAPACITY = 32;

    private final AbstractChannel channel;
    private Entry first;
    private Entry last;
    private int flushed;
    private int size;

    private ByteBuffer[] nioBuffers;
    private int nioBufferCount;
    private long nioBufferSize;

    private boolean inFail;

    private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");

    @SuppressWarnings("unused")
    private volatile long totalPendingSize;

    private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> WRITABLE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "writable");

    private volatile int writable = 1;

    ChannelOutboundBuffer(AbstractChannel channel) {
        this.channel = channel;
    }

    void addMessage(Object msg, ChannelPromise promise) {
        int msgSize = channel.estimatorHandle().size(msg);
        if (msgSize < 0) {
            msgSize = 0;
        }

        Entry entry;
        if (promise instanceof Entry) {
            entry = (Entry) promise;
        } else {
            entry = RecyclableEntry.newInstance(promise);
        }
        entry.init(msg, msgSize, total(msg));

        if (last == null) {
            first = last = entry;
        } else {
            last.next(entry);
            last = entry;
        }
        size++;
        // increment pending bytes after adding message to the unflushed arrays.
        // See https://github.com/netty/netty/issues/1619
        incrementPendingOutboundBytes(msgSize);
    }

    void addFlush() {
        flushed = size;
    }

    /**
     * Increment the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void incrementPendingOutboundBytes(int size) {
        // Cache the channel and check for null to make sure we not produce a NPE in case of the Channel gets
        // recycled while process this method.
        Channel channel = this.channel;
        if (size == 0 || channel == null) {
            return;
        }

        long oldValue = totalPendingSize;
        long newWriteBufferSize = oldValue + size;
        while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
            oldValue = totalPendingSize;
            newWriteBufferSize = oldValue + size;
        }

        int highWaterMark = channel.config().getWriteBufferHighWaterMark();

        if (newWriteBufferSize > highWaterMark) {
            if (WRITABLE_UPDATER.compareAndSet(this, 1, 0)) {
                channel.pipeline().fireChannelWritabilityChanged();
            }
        }
    }

    /**
     * Decrement the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void decrementPendingOutboundBytes(int size) {
        // Cache the channel and check for null to make sure we not produce a NPE in case of the Channel gets
        // recycled while process this method.
        Channel channel = this.channel;
        if (size == 0 || channel == null) {
            return;
        }

        long oldValue = totalPendingSize;
        long newWriteBufferSize = oldValue - size;
        while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
            oldValue = totalPendingSize;
            newWriteBufferSize = oldValue - size;
        }

        int lowWaterMark = channel.config().getWriteBufferLowWaterMark();

        if (newWriteBufferSize == 0 || newWriteBufferSize < lowWaterMark) {
            if (WRITABLE_UPDATER.compareAndSet(this, 0, 1)) {
                channel.pipeline().fireChannelWritabilityChanged();
            }
        }
    }

    private static long total(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        if (msg instanceof ByteBufHolder) {
            return ((ByteBufHolder) msg).content().readableBytes();
        }
        return -1;
    }

    public Object current() {
        if (isEmpty()) {
            return null;
        } else {
            return first.msg();
        }
    }

    /**
     * Replace the current msg with the given one.
     * The replaced msg will automatically be released
     */
    public void current(Object msg) {
        Entry entry =  first;
        safeRelease(entry.msg());
        entry.init(msg, entry.pendingSize(), entry.total());
    }

    public void progress(long amount) {
        Entry e = first;
        ChannelPromise p = e.promise();
        if (p instanceof ChannelProgressivePromise) {
            long progress = e.progress(amount);
            ((ChannelProgressivePromise) p).tryProgress(progress, e.total());
        }
    }

    public boolean remove() {
        if (isEmpty()) {
            return false;
        }

        Entry e = first;
        first = e.next();
        if (first == null) {
            last = null;
        }

        ChannelPromise promise = e.promise();
        int pendingSize = e.pendingSize();
        flushed--;
        size--;

        safeRelease(e.msg());
        e.clear();

        promise.trySuccess();
        decrementPendingOutboundBytes(pendingSize);

        return true;
    }

    public boolean remove(Throwable cause) {
        if (isEmpty()) {
            return false;
        }

        Entry e = first;
        first = e.next();
        if (first == null) {
            last = null;
        }
        ChannelPromise promise = e.promise();
        int pendingSize = e.pendingSize();

        flushed--;
        size--;

        safeRelease(e.msg());
        e.clear();

        safeFail(promise, cause);
        decrementPendingOutboundBytes(pendingSize);

        return true;
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@code null} is returned otherwise.  If this method returns a non-null array, {@link #nioBufferCount()} and
     * {@link #nioBufferSize()} will return the number of NIO buffers in the returned array and the total number
     * of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     */
    public ByteBuffer[] nioBuffers() {
        long nioBufferSize = 0;
        int nioBufferCount = 0;
        final ByteBufAllocator alloc = channel.alloc();
        ByteBuffer[] nioBuffers = this.nioBuffers;
        if (nioBuffers == null) {
            nioBuffers = this.nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
        }
        Entry entry = first;
        for (int i = 0; i < flushed; i++) {
            if (!(entry.msg() instanceof ByteBuf)) {
                this.nioBufferCount = 0;
                this.nioBufferSize = 0;
                return null;
            }

            ByteBuf buf = (ByteBuf) entry.msg();
            final int readerIndex = buf.readerIndex();
            final int readableBytes = buf.writerIndex() - readerIndex;

            if (readableBytes > 0) {
                nioBufferSize += readableBytes;
                int count = entry.bufferCount();
                int neededSpace = nioBufferCount + count;
                if (neededSpace > nioBuffers.length) {
                    this.nioBuffers = nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                }
                if (buf.isDirect() || !alloc.isDirectBufferPooled()) {
                    if (count == 1) {
                        ByteBuffer nioBuf = entry.buffer();
                        nioBuffers[nioBufferCount ++] = nioBuf;
                    } else {
                        ByteBuffer[] nioBufs = entry.buffers();
                        nioBufferCount = fillBufferArray(nioBufs, nioBuffers, nioBufferCount);
                    }
                } else {
                    nioBufferCount = fillBufferArrayNonDirect(entry, buf, readerIndex,
                            readableBytes, alloc, nioBuffers, nioBufferCount);
                }
            }
            entry = entry.next();
        }
        this.nioBufferCount = nioBufferCount;
        this.nioBufferSize = nioBufferSize;

        return nioBuffers;
    }

    private static int fillBufferArray(ByteBuffer[] nioBufs, ByteBuffer[] nioBuffers, int nioBufferCount) {
        for (ByteBuffer nioBuf: nioBufs) {
            if (nioBuf == null) {
                break;
            }
            nioBuffers[nioBufferCount ++] = nioBuf;
        }
        return nioBufferCount;
    }

    private static int fillBufferArrayNonDirect(Entry entry, ByteBuf buf, int readerIndex, int readableBytes,
                                      ByteBufAllocator alloc, ByteBuffer[] nioBuffers, int nioBufferCount) {
        ByteBuf directBuf = alloc.directBuffer(readableBytes);
        directBuf.writeBytes(buf, readerIndex, readableBytes);
        buf.release();
        entry.init(directBuf, entry.pendingSize(), entry.total());
        nioBuffers[nioBufferCount ++] = entry.buffer();
        return nioBufferCount;
    }

    private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
        int newCapacity = array.length;
        do {
            // double capacity until it is big enough
            // See https://github.com/netty/netty/issues/1890
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (neededSpace > newCapacity);

        ByteBuffer[] newArray = new ByteBuffer[newCapacity];
        System.arraycopy(array, 0, newArray, 0, size);

        return newArray;
    }

    public int nioBufferCount() {
        return nioBufferCount;
    }

    public long nioBufferSize() {
        return nioBufferSize;
    }

    boolean getWritable() {
        return writable != 0;
    }

    public int size() {
        return flushed;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    void failFlushed(Throwable cause) {
        // Make sure that this method does not reenter.  A listener added to the current promise can be notified by the
        // current thread in the tryFailure() call of the loop below, and the listener can trigger another fail() call
        // indirectly (usually by closing the channel.)
        //
        // See https://github.com/netty/netty/issues/1501
        if (inFail) {
            return;
        }

        try {
            inFail = true;
            for (;;) {
                if (!remove(cause)) {
                    break;
                }
            }
        } finally {
            inFail = false;
        }
    }

    void close(final ClosedChannelException cause) {
        if (inFail) {
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    close(cause);
                }
            });
            return;
        }

        inFail = true;

        if (channel.isOpen()) {
            throw new IllegalStateException("close() must be invoked after the channel is closed.");
        }

        if (!isEmpty()) {
            throw new IllegalStateException("close() must be invoked after all flushed writes are handled.");
        }

        // Release all unflushed messages.
        try {
            Entry e = first;
            int flushed = this.flushed;
            for (int i = 0;; i++) {
                if (e == null) {
                    break;
                }
                if (i <= flushed) {
                    e = e.next();
                    continue;
                }

                safeRelease(e.msg());
                safeFail(e.promise(), cause);

                // Just decrease; do not trigger any events via decrementPendingOutboundBytes()
                int size = e.pendingSize();
                long oldValue = totalPendingSize;
                long newWriteBufferSize = oldValue - size;
                while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
                    oldValue = totalPendingSize;
                    newWriteBufferSize = oldValue - size;
                }

                e.clear();
                e = e.next();
            }
        } finally {
            inFail = false;
        }
    }

    private static void safeRelease(Object message) {
        try {
            ReferenceCountUtil.release(message);
        } catch (Throwable t) {
            logger.warn("Failed to release a message.", t);
        }
    }

    private static void safeFail(ChannelPromise promise, Throwable cause) {
        if (!(promise instanceof VoidChannelPromise) && !promise.tryFailure(cause)) {
            logger.warn("Promise done already: {} - new exception is:", promise, cause);
        }
    }

    public long totalPendingWriteBytes() {
        return totalPendingSize;
    }

    private static final class RecyclableEntry implements Entry {
        private static final Recycler<RecyclableEntry> RECYCLER = new Recycler<RecyclableEntry>() {
            @Override
            protected RecyclableEntry newObject(Handle handle) {
                return new RecyclableEntry(handle);
            }
        };

        private final Handle handle;
        private ChannelPromise promise;
        private Entry next;
        private int count;
        private ByteBuffer buffer;
        private ByteBuffer[] buffers;
        private int pendingSize;
        private long total;
        private long progress;
        private Object msg;

        static Entry newInstance(ChannelPromise promise) {
            RecyclableEntry entry = RECYCLER.get();
            entry.promise = promise;
            return entry;
        }

        private RecyclableEntry(Handle handle) {
            this.handle = handle;
        }

        @Override
        public int pendingSize() {
            return pendingSize;
        }

        @Override
        public long total() {
            return total;
        }

        @Override
        public long progress(long bytes) {
            progress += bytes;
            return progress;
        }

        @Override
        public void clear() {
            buffers = null;
            buffer = null;
            msg = null;
            promise = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            count = 0;
            next = null;
            RECYCLER.recycle(this, handle);
        }

        @Override
        public void init(Object msg, int pendingSize, long total) {
            this.msg = msg;
            this.pendingSize = pendingSize;
            this.total = total;
            buffer = null;
            buffers = null;
            count = 0;
        }

        @Override
        public void next(Entry entry) {
            next = entry;
        }

        @Override
        public Entry next() {
            return next;
        }

        @Override
        public ChannelPromise promise() {
            return promise;
        }

        @Override
        public ByteBuffer buffer() {
            initBuffers();
            return buffer;
        }

        @Override
        public ByteBuffer[] buffers() {
            initBuffers();
            return buffers;
        }

        @Override
        public int bufferCount() {
            initBuffers();
            return count;
        }

        @Override
        public Object msg() {
            return msg;
        }

        private void initBuffers() {
            if (count == 0 && msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                count = buf.nioBufferCount();
                if (count == 1) {
                    buffer = buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes());
                } else {
                    buffers = buf.nioBuffers(buf.readerIndex(), buf.readableBytes());
                }
            }
        }
    }

    /**
     * Entry in this {@link ChannelOutboundBuffer} which maps a message to a {@link ChannelPromise}
     */
    interface Entry {
        void init(Object msg, int pendingSize, long total);
        void next(Entry entry);
        int pendingSize();
        long total();
        long progress(long progress);
        Entry next();
        ChannelPromise promise();
        ByteBuffer buffer();
        ByteBuffer[] buffers();
        int bufferCount();
        Object msg();
        void clear();
    }
}
