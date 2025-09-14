/*
 * This file is part of gama, licensed under the MIT License
 *
 * Copyright (c) 2025 Rubenicos
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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * An object that executes submitted Runnable tasks and return itself as cancellable objects.<br>
 * Unlike {@link Executor}, this kind of object allows to execute delayed and repeatable tasks.
 *
 * @author Rubenicos
 *
 * @param <T> the cancellable object type.
 */
public interface TaskExecutor<T> extends Closeable {

    /**
     * Task executor object that use Java methods to execute tasks.<br>
     * Is NOT suggested to use this object due is not scalable and doesn't
     * use any thread pool, make a better implementation instead.
     */
    TaskExecutor<Thread> JAVA = new TaskExecutor<Thread>() {
        @Override
        public @NotNull Thread execute(@NotNull Runnable command) {
            final Thread thread = new Thread(command);
            thread.start();
            return thread;
        }

        @Override
        public @NotNull Thread execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
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
        public @NotNull Thread execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
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
     * Executes the given command at some time in the future.<br>
     * Unlike {@link Executor#execute(Runnable)}, this method return the
     * task itself, that can be cancelled at some time in the future.<br>
     * For example, a recursive call that locks the thread can be cancelled if
     * it's executed using this method.
     *
     * @param command the runnable task.
     * @return        a task type that can be cancelled.
     */
    @NotNull
    T execute(@NotNull Runnable command);

    /**
     * Executes the given command after the time delay has passed.
     *
     * @param command the runnable task.
     * @param delay   the time delay to pass before the task should be executed.
     * @param unit    the time unit for the time delay.
     * @return        a task type that can be cancelled.
     */
    @NotNull
    T execute(@NotNull Runnable command, long delay, @NotNull TimeUnit unit);

    /**
     * Executes the given command after the initial delay has passed,
     * and then periodically executed with the specified period.
     *
     * @param command the runnable task.
     * @param delay   the time delay to pass before the first execution of the task.
     * @param period  the time between task executions after the first execution of the task.
     * @param unit    the time unit for the initial delay and period.
     * @return        a task type that can be cancelled.
     */
    @NotNull
    T execute(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit);

    /**
     * Cancel a task type that was created by this executor.
     *
     * @param t a task type that can be cancelled.
     */
    void cancel(@NotNull T t);

    @Override
    default void close() throws IOException {
        // empty default method
    }

    /**
     * Return the current executor as Java {@link Executor}.
     *
     * @return an {@link Executor} that represent the current object.
     */
    @NotNull
    default Executor executor() {
        return this::execute;
    }
}
