/*
 * MIT License.
 *
 * Copyright (c) 2025-2026 Rubenicos
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An object that executes submitted Runnable tasks and return itself as cancellable objects.<br>
 * Unlike {@link Executor}, this kind of object allows to execute delayed and repeatable tasks.
 *
 * @author Rubenicos
 *
 * @param <T> the cancellable object type
 */
public interface TaskExecutor<T> extends Executor, AutoCloseable {

    /**
     * Task executor object that use Java methods to execute tasks.<br>
     * Is NOT suggested to use this object due is not scalable and doesn't
     * use any thread pool, make a better implementation instead.
     */
    TaskExecutor<Thread> JAVA = new TaskExecutor<Thread>() {
        @Override
        public @NotNull Thread run(@NotNull Runnable command) {
            final Thread thread = new Thread(command);
            thread.start();
            return thread;
        }

        @Override
        public @NotNull Thread runLater(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
            final Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(unit.toMillis(delay));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (!Thread.interrupted()) {
                    command.run();
                }
            });
            thread.start();
            return thread;
        }

        @Override
        public @NotNull Thread runTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
            return runLinkedTimer(command, delay, period, unit);
        }

        @Override
        public @NotNull Thread runLinkedTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
            final Thread thread = new Thread(() -> {
                if (delay > 0) {
                    try {
                        Thread.sleep(unit.toMillis(delay));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                while (!Thread.interrupted()) {
                    command.run();
                    try {
                        Thread.sleep(unit.toMillis(period));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            thread.start();
            return thread;
        }

        @Override
        public void cancel(@NotNull Thread thread) {
            thread.interrupt();
        }
    };

    /**
     * Creates a TaskExecutor from a ScheduledExecutorService.
     *
     * @param scheduledExecutor the scheduled executor service
     * @return                  a newly generated TaskExecutor
     */
    @NotNull
    static TaskExecutor<Future<?>> from(@NotNull ScheduledExecutorService scheduledExecutor) {
        return new TaskExecutor<Future<?>>() {
            @Override
            public @NotNull Future<?> run(@NotNull Runnable command) {
                command.run();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public @NotNull Future<?> runLater(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
                return scheduledExecutor.schedule(command, delay, unit);
            }

            @Override
            public @NotNull Future<?> runTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
                return scheduledExecutor.scheduleAtFixedRate(command, delay, period, unit);
            }

            @Override
            public @NotNull Future<?> runLinkedTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
                return scheduledExecutor.scheduleWithFixedDelay(command, delay, period, unit);
            }

            @Override
            public void cancel(@NotNull Future<?> future) {
                future.cancel(false);
            }

            @Override
            public void close() throws IOException {
                try {
                    scheduledExecutor.shutdown();
                } catch (Throwable t) {
                    throw new IOException(t);
                }
            }
        };
    }

    /**
     * Creates a TaskExecutor from an ExecutorService and a ScheduledExecutorService.
     *
     * @param executor          the executor service
     * @param scheduledExecutor the scheduled executor service
     * @return                  a newly generated TaskExecutor
     */
    @NotNull
    static TaskExecutor<Future<?>> from(@NotNull ExecutorService executor, @NotNull ScheduledExecutorService scheduledExecutor) {
        return new TaskExecutor<Future<?>>() {
            @Override
            public @NotNull Future<?> run(@NotNull Runnable command) {
                return executor.submit(command);
            }

            @Override
            public @NotNull Future<?> runLater(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
                return scheduledExecutor.schedule(() -> executor.execute(command), delay, unit);
            }

            @Override
            public @NotNull Future<?> runTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
                return scheduledExecutor.scheduleAtFixedRate(() -> executor.execute(command), delay, period, unit);
            }

            @Override
            public @NotNull Future<?> runLinkedTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
                return scheduledExecutor.scheduleWithFixedDelay(() -> executor.execute(command), delay, period, unit);
            }

            @Override
            public void cancel(@NotNull Future<?> future) {
                future.cancel(false);
            }

            @Override
            public void close() throws IOException {
                try {
                    executor.shutdown();
                    scheduledExecutor.shutdown();
                } catch (Throwable t) {
                    throw new IOException(t);
                }
            }
        };
    }

    @Override
    default void execute(@NotNull Runnable command) {
        run(command);
    }

    /**
     * Executes the given command at some time in the future.<br>
     * Unlike {@link Executor#execute(Runnable)}, this method return the
     * task itself, that can be canceled at some time in the future.<br>
     * For example, a recursive call that locks the thread can be canceled if
     * it's executed using this method.
     *
     * @param command the runnable task
     * @return        a task type that can be canceled
     */
    @NotNull
    T run(@NotNull Runnable command);

    /**
     * Executes the given command after the time delay has passed.
     *
     * @param command the runnable task
     * @param delay   the time delay to pass before the task should be executed
     * @param unit    the time unit for the time delay
     * @return        a task type that can be canceled
     */
    @NotNull
    T runLater(@NotNull Runnable command, long delay, @NotNull TimeUnit unit);

    /**
     * Executes the given command after the time delay has passed.
     *
     * @param command the runnable task
     * @param delay   the duration delay to pass before the task should be executed
     * @return        a task type that can be canceled
     */
    @NotNull
    default T runLater(@NotNull Runnable command, @NotNull Duration delay) {
        return runLater(command, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.
     *
     * @param command the runnable task
     * @param delay   the time delay to pass before the first execution of the task
     * @param period  the time between task executions after the first execution of the task
     * @param unit    the time unit for the initial delay and period
     * @return        a task type that can be canceled
     */
    @NotNull
    T runTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit);

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.
     *
     * @param command the runnable task
     * @param delay   the duration delay to pass before the first execution of the task
     * @param period  the duration between task executions after the first execution of the task
     * @return        a task type that can be canceled
     */
    @NotNull
    default T runTimer(@NotNull Runnable command, @NotNull Duration delay, @NotNull Duration period) {
        return runTimer(command, delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.<br>
     * This method is similar to {@link TaskExecutor#runTimer(Runnable, long, long, TimeUnit)},
     * but this method is used to link a task to another task, so if the first task is canceled,
     * the linked task will be canceled too.
     *
     * @param command the runnable task
     * @param delay   the time delay to pass before the first execution of the task
     * @param period  the time between task executions after the first execution of the task
     * @param unit    the time unit for the initial delay and period
     * @return        a task type that can be canceled
     */
    @NotNull
    default T runLinkedTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
        return runTimer(command, delay, period, unit);
    }

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.<br>
     * This method is similar to {@link TaskExecutor#runTimer(Runnable, Duration, Duration)},
     * but this method is used to link a task to another task, so if the first task is canceled,
     * the linked task will be canceled too.
     *
     * @param command the runnable task
     * @param delay   the duration delay to pass before the first execution of the task
     * @param period  the duration between task executions after the first execution of the task
     * @return        a task type that can be canceled
     */
    @NotNull
    default T runLinkedTimer(@NotNull Runnable command, @NotNull Duration delay, @NotNull Duration period) {
        return runLinkedTimer(command, delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.<br>
     * This method is similar to {@link TaskExecutor#runTimer(Runnable, long, long, TimeUnit)},
     * but this method is used to lock the task execution, so if the task is already running,
     * the next execution will be skipped.
     *
     * @param command the runnable task
     * @param delay   the time delay to pass before the first execution of the task
     * @param period  the time between task executions after the first execution of the task
     * @param unit    the time unit for the initial delay and period
     * @return        a task type that can be canceled
     */
    @NotNull
    default T runLockedTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
        final AtomicBoolean running = new AtomicBoolean(false);
        final Runnable lockedCommand = () -> {
            if (!running.compareAndSet(false, true)) {
                return;
            }
            try {
                command.run();
            } finally {
                running.set(false);
            }
        };
        return runTimer(lockedCommand, delay, period, unit);
    }

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.<br>
     * This method is similar to {@link TaskExecutor#runTimer(Runnable, Duration, Duration)},
     * but this method is used to lock the task execution, so if the task is already running,
     * the next execution will be skipped.
     *
     * @param command the runnable task
     * @param delay   the duration delay to pass before the first execution of the task
     * @param period  the duration between task executions after the first execution of the task
     * @return        a task type that can be canceled
     */
    @NotNull
    default T runLockedTimer(@NotNull Runnable command, @NotNull Duration delay, @NotNull Duration period) {
        return runLockedTimer(command, delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Cancel a task type that was created by this executor.
     *
     * @param t a task type that can be canceled
     */
    void cancel(@NotNull T t);

    @Override
    default void close() throws IOException {
        // empty default method
    }
}
