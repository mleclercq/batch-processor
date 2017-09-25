package com.foo;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class BatchIOStreamProcessor<T>
        extends RecursiveAction {

    private final Stream<T> data;
    private final int       batchSize;

    private final ConcurrentLinkedQueue<Sink<?>> sinks =
            new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<SinkJob<?>> sinkJobs =
            new ConcurrentLinkedQueue<>();

    private final ReentrantLock sinkJobsLock     = new ReentrantLock();
    private final Condition     sinkJobsNonEmpty = sinkJobsLock.newCondition();

    private final ConcurrentLinkedDeque<ForkJoinTask<?>> pendingTasks =
            new ConcurrentLinkedDeque<>();

    private Throwable error = null;

    protected BatchIOStreamProcessor(
            final Stream<T> data,
            final int batchSize) {
        this.data = data;
        this.batchSize = batchSize;
    }

    protected abstract void process(T[] batch);

    @Override
    protected void compute() {
        final ForkJoinTask<Void> reader =
                new BatchReader(data, batchSize).fork();
        final Sinker sinker = new Sinker();
        sinker.fork();
        // keep track of the first error to occurs.
        // in any case, make sure to close all sinks before returning.
        try {
            // wait for the reader to finnish reading
            try {
                reader.get();
            } catch (final InterruptedException e) {
                error = e;
            } catch (final ExecutionException e) {
                error = e.getCause();
            }

            if (error != null) {
                // now join pending tasks;
                while (!pendingTasks.isEmpty()) {
                    final ForkJoinTask<?> task = pendingTasks.poll();
                    // poll() may return null if cleanPendingTasks() is being
                    // executed concurrently by the sinker
                    if (task != null) {
                        try {
                            task.get();
                        } catch (final InterruptedException e) {
                            error = e;
                            break;
                        } catch (final ExecutionException e) {
                            error = e.getCause();
                            break;
                        }
                    }
                }
            }

            if (error != null) {
                // now join the sinker task
                sinkJobsLock.lock();
                try {
                    // notify the sinker that it should stop once there is no more
                    // sinkJob.
                    sinker.shouldStop = true;
                    sinkJobsNonEmpty.signal();
                } finally {
                    sinkJobsLock.unlock();
                }
                try {
                    sinker.get();
                } catch (final InterruptedException e) {
                    error = e;
                } catch (final ExecutionException e) {
                    error = e.getCause();
                }
            }
        } finally {
            // processing is finished. Close the sinks
            for (final Sink<?> sink : sinks) {
                try {
                    sink.close();
                } catch (final IOException e) {
                    if (error == null) {
                        error = e;
                    }
                }
            }
        }
        if (error != null) {
            throw new CompletionException(error);
        }
    }

    private void schedule(final ForkJoinTask<?> task) {
        task.fork();
        pendingTasks.add(task);
    }

    private void cleanPendingTasks() {
        ForkJoinTask<?> task;
        // clear pending tasks in the front of the list that are done.
        // TODO what if a task in the front is much longer that the others?
        while ((task = pendingTasks.poll()) != null) {
            if (!task.isDone()) {
                // task is not finished, put if back in the queue.
                pendingTasks.addFirst(task);
                return;
            }
            if (task.isCompletedAbnormally() && error == null) {
                error = task.getException();
            }
        }
    }

    public abstract class Sink<J>
            implements Closeable {

        protected Sink() {
            sinks.add(this);
        }

        protected abstract void perform(J sinkJob);

        public void sink(final J job) {
            sinkJobsLock.lock();
            try {
                sinkJobs.add(new SinkJob<>(this, job));
                sinkJobsNonEmpty.notify();
            } finally {
                sinkJobsLock.unlock();
            }
        }
    }

    public abstract class BufferedSink<E>
            extends Sink<Iterable<E>>
            implements Consumer<E> {
        private final ConcurrentBuffer<E> buffer =
                new ConcurrentBuffer<>(batchSize);

        @Override
        public void accept(final E elem) {
            final Iterable<E> toDrain = this.buffer.add(elem);
            if (toDrain != null) {
                sink(toDrain);
            }
        }

        protected abstract void doClose() throws IOException;

        @Override
        public final void close() throws IOException {
            final Iterable<E> toDrain = buffer.clear();
            try {
                perform(toDrain);
            } finally {
                doClose();
            }
        }
    }

    public final class FileSync<E>
            extends BufferedSink<E> {

        private BufferedWriter writer;

        public FileSync(final Path file, final Charset charset)
                throws IOException {
            writer = Files.newBufferedWriter(file, charset);
        }

        @Override
        protected void perform(final Iterable<E> sinkJob) {
            try {
                for (final E e : sinkJob) {
                    writer.append(e.toString()).append('\n');
                }
            } catch (final IOException e) {
                try {
                    writer.close();
                } catch (final IOException ignore) {
                } finally {
                    writer = null;
                }
                throw new CompletionException(e);
            }
        }

        @Override
        protected void doClose() throws IOException {
            writer.close();
        }
    }

    private final class SinkJob<J> {
        private final Sink<J> sink;
        private final J       job;

        private SinkJob(final Sink<J> sink, final J job) {
            this.sink = sink;
            this.job = job;
        }

        private void perform() {
            sink.perform(job);
        }
    }

    private final class BatchReader
            extends RecursiveAction {

        private final Stream<T> source;
        private final int       batchSize;

        private BatchReader(final Stream<T> source, final int batchSize) {
            this.source = source;
            this.batchSize = batchSize;
        }

        @Override
        protected void compute() {
            final ForkJoinPool pool = getPool();
            final ReaderFlowControl flowControl = new ReaderFlowControl(pool);
            try {
                final Iterator<T> iterator = source.iterator();
                T[] batch = createBatch(batchSize);
                int i = 0;
                while (iterator.hasNext()) {
                    batch[i++] = iterator.next();
                    if (i == batchSize) {
                        // end of batch
                        schedule(new ProcessingTask(batch));
                        if (flowControl.shouldWait()) {
                            // set batch array to null to allow GC while waiting
                            //noinspection UnusedAssignment
                            batch = null;
                            try {
                                ForkJoinPool.managedBlock(flowControl);
                            } catch (final InterruptedException e) {
                                if (error == null) {
                                    error = e;
                                }
                                return;
                            }
                        }
                        batch = createBatch(batchSize);
                        i = 0;
                    }
                    cleanPendingTasks();
                }
                if (i > 0) {
                    // process last batch
                    // copy last batch in an array of the right size
                    System.arraycopy(batch, 0, batch = createBatch(i), 0,
                            i);
                    schedule(new ProcessingTask(batch));
                }
            } finally {
                source.close();
            }
        }

        @SuppressWarnings("unchecked")
        private T[] createBatch(final int batchSize) {
            return (T[]) new Object[batchSize];
        }

        private final class ReaderFlowControl
                implements ForkJoinPool.ManagedBlocker {
            private final ForkJoinPool pool;

            public ReaderFlowControl(final ForkJoinPool pool) {
                this.pool = pool;
            }

            @Override
            public boolean block() throws InterruptedException {
                Thread.sleep(100);
                return isReleasable();
            }

            // wait if the number of waiting task in the pool + the number of
            // sinkJobs is greater than the parallelism of the pool.
            @Override
            public boolean isReleasable() {
                // access the size of sinkJobs list outside a synchronized block
                // This is safe because only an estimate of that size is needed.
                return pool.getQueuedSubmissionCount() + sinkJobs.size()
                        <= pool.getParallelism();
            }

            public boolean shouldWait() {
                return !isReleasable();
            }
        }
    }

    private final class ProcessingTask
            extends RecursiveAction {

        private final T[] batch;

        private ProcessingTask(final T[] batch) {
            this.batch = batch;
        }

        @Override
        protected void compute() {
            process(batch);
        }
    }

    private final class Sinker
            extends RecursiveAction {

        private volatile boolean shouldStop = false;

        @Override
        protected void compute() {
            final ForkJoinPool pool = getPool();
            final SinkerBlocker sinkerBlocker = new SinkerBlocker();
            while (true) {
                SinkJob<?> sinkJob;
                sinkJobsLock.lock();
                try {
                    while ((sinkJob = sinkJobs.poll()) == null
                            && !shouldStop) {
                        try {
                            ForkJoinPool.managedBlock(sinkerBlocker);
                        } catch (final InterruptedException e) {
                            if (error == null) {
                                error = e;
                            }
                            return;
                        }
                    }
                } finally {
                    sinkJobsLock.unlock();
                }
                if (sinkJob == null) {
                    assert shouldStop;
                    return;
                }
                try {
                    sinkJob.perform();
                } catch (final CompletionException e) {
                    if (error == null) {
                        error = e.getCause();
                    }
                }
                cleanPendingTasks();
            }
        }

        private final class SinkerBlocker
                implements ForkJoinPool.ManagedBlocker {
            @Override
            public boolean block() throws InterruptedException {
                sinkJobsNonEmpty.await();
                return isReleasable();
            }

            @Override
            public boolean isReleasable() {
                return !sinkJobs.isEmpty() || shouldStop;
            }
        }
    }

    /**
     * Modified version of a Treiber stack where the add() method may clear the
     * buffer when it is full, in which case it returns the previous content of
     * the buffer as a linked-list.
     */
    private static final class ConcurrentBuffer<E> {

        private final int maxSize;
        private final AtomicReference<Node<E>> head =
                new AtomicReference<>(null);

        private ConcurrentBuffer(final int maxSize) {
            this.maxSize = maxSize;
        }

        private Iterable<E> add(final E e) {
            final Node<E> newHead = new Node<>(e);
            Node<E> oldHead;
            while (true) {
                oldHead = head.get();
                newHead.setNext(oldHead);
                if (newHead.lenght >= maxSize) {
                    if (head.compareAndSet(oldHead, null)) {
                        return newHead;
                    }
                } else if (head.compareAndSet(oldHead, newHead)) {
                    return null;
                }
            }
        }

        private Iterable<E> clear() {
            Node<E> oldHead;
            do {
                oldHead = head.get();
            } while (!head.compareAndSet(oldHead, null));
            return oldHead;
        }

        private static final class Node<E>
                implements Iterable<E> {
            private final E       value;
            private       Node<E> next;
            private       int     lenght;

            private Node(final E value) {
                this.value = value;
            }

            private void setNext(final Node<E> next) {
                this.next = next;
                this.lenght = next == null ? 1 : next.lenght + 1;
            }

            @Override
            public Iterator<E> iterator() {
                return new Iterator<E>() {
                    private Node<E> top = Node.this;

                    @Override
                    public boolean hasNext() {
                        return top != null;
                    }

                    @Override
                    public E next() {
                        final Node<E> oldTop = top;
                        top = top.next;
                        return oldTop.value;
                    }
                };
            }
        }
    }
}
