/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogCaptureAppender extends AppenderSkeleton implements AutoCloseable {
    private final List<LoggingEvent> events = new LinkedList<>();
    private final List<LogLevelChange> logLevelChanges = new LinkedList<>();

    public static class LogLevelChange {

        public LogLevelChange(final Level originalLevel, final Class<?> clazz) {
            this.originalLevel = originalLevel;
            this.clazz = clazz;
        }

        private final Level originalLevel;

        private final Class<?> clazz;

    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class Event {
        private final String level;
        private final String message;
        private final Optional<String> throwableInfo;
        private final Optional<String> throwableClassName;

        Event(final String level, final String message, final Optional<String> throwableInfo, final Optional<String> throwableClassName) {
            this.level = level;
            this.message = message;
            this.throwableInfo = throwableInfo;
            this.throwableClassName = throwableClassName;
        }

        public String getLevel() {
            return level;
        }

        public String getMessage() {
            return message;
        }

        public Optional<String> getThrowableInfo() {
            return throwableInfo;
        }

        public Optional<String> getThrowableClassName() {
            return throwableClassName;
        }
    }

    public static LogCaptureAppender createAndRegister() {
        final LogCaptureAppender logCaptureAppender = new LogCaptureAppender();
        Logger.getRootLogger().addAppender(logCaptureAppender);
        return logCaptureAppender;
    }

    public static LogCaptureAppender createAndRegister(final Class<?> clazz) {
        final LogCaptureAppender logCaptureAppender = new LogCaptureAppender();
        Logger.getLogger(clazz).addAppender(logCaptureAppender);
        return logCaptureAppender;
    }

    public void setClassLogger(final Class<?> clazz, Level level) {
        logLevelChanges.add(new LogLevelChange(Logger.getLogger(clazz).getLevel(), clazz));
        Logger.getLogger(clazz).setLevel(level);
    }

    public static void unregister(final LogCaptureAppender logCaptureAppender) {
        Logger.getRootLogger().removeAppender(logCaptureAppender);
    }

    @Override
    protected void append(final LoggingEvent event) {
        synchronized (events) {
            events.add(event);
        }
    }

    public List<String> getMessages(String level) {
        return getEvents().stream()
                .filter(e -> level.equals(e.getLevel()))
                .map(Event::getMessage)
                .collect(Collectors.toList());
    }

    public List<String> getMessages() {
        final LinkedList<String> result = new LinkedList<>();
        synchronized (events) {
            for (final LoggingEvent event : events) {
                result.add(event.getRenderedMessage());
            }
        }
        return result;
    }

    public List<Event> getEvents() {
        final LinkedList<Event> result = new LinkedList<>();
        synchronized (events) {
            for (final LoggingEvent event : events) {
                final String[] throwableStrRep = event.getThrowableStrRep();
                final Optional<String> throwableString;
                final Optional<String> throwableClassName;
                if (throwableStrRep == null) {
                    throwableString = Optional.empty();
                    throwableClassName = Optional.empty();
                } else {
                    final StringBuilder throwableStringBuilder = new StringBuilder();

                    for (final String s : throwableStrRep) {
                        throwableStringBuilder.append(s);
                    }

                    throwableString = Optional.of(throwableStringBuilder.toString());
                    throwableClassName = Optional.of(event.getThrowableInformation().getThrowable().getClass().getName());
                }

                result.add(new Event(event.getLevel().toString(), event.getRenderedMessage(), throwableString, throwableClassName));
            }
        }
        return result;
    }

    @Override
    public void close() {
        for (final LogLevelChange logLevelChange : logLevelChanges) {
            Logger.getLogger(logLevelChange.clazz).setLevel(logLevelChange.originalLevel);
        }
        logLevelChanges.clear();
        unregister(this);
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
