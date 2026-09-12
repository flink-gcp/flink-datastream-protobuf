/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.protobuf.runtimeapp;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A single checkpointed input cursor; the filesystem carries commands, never recovered values. */
public final class ControlledSource implements Source<Integer, ControlledSource.Cursor, Integer> {
    private final String directory;

    public ControlledSource(String directory) {
        this.directory = directory;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Integer, Cursor> createReader(SourceReaderContext context) {
        return new SourceReader<>() {
            private Integer next;

            @Override
            public void start() {
                context.sendSplitRequest();
            }

            @Override
            public InputStatus pollNext(ReaderOutput<Integer> output) throws Exception {
                Path root = Path.of(directory);
                if (Files.exists(root.resolve("fail")) && !Files.exists(root.resolve("failed"))) {
                    Files.createFile(root.resolve("failed"));
                    throw new IOException("Injected failure after completed checkpoint");
                }
                Path limit = root.resolve("limit");
                if (next != null
                        && Files.exists(limit)
                        && next < Integer.parseInt(Files.readString(limit).trim())) {
                    output.collect(next++);
                    return InputStatus.MORE_AVAILABLE;
                }
                return InputStatus.NOTHING_AVAILABLE;
            }

            @Override
            public List<Cursor> snapshotState(long checkpointId) {
                return next == null ? List.of() : List.of(new Cursor(next));
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return CompletableFuture.runAsync(
                        () -> {}, CompletableFuture.delayedExecutor(20, TimeUnit.MILLISECONDS));
            }

            @Override
            public void addSplits(List<Cursor> splits) {
                if (splits.size() != 1 || next != null) {
                    throw new IllegalStateException("Expected exactly one input cursor");
                }
                next = splits.get(0).next;
            }

            @Override
            public void notifyNoMoreSplits() {}

            @Override
            public void close() {}
        };
    }

    @Override
    public SplitEnumerator<Cursor, Integer> createEnumerator(
            SplitEnumeratorContext<Cursor> context) {
        return restoreEnumerator(context, 0);
    }

    @Override
    public SplitEnumerator<Cursor, Integer> restoreEnumerator(
            SplitEnumeratorContext<Cursor> context, Integer checkpoint) {
        return new SplitEnumerator<>() {
            private Cursor pending = checkpoint < 0 ? null : new Cursor(checkpoint);

            @Override
            public void start() {}

            @Override
            public void handleSplitRequest(int subtaskId, String requesterHostname) {
                if (pending != null && context.registeredReaders().containsKey(subtaskId)) {
                    context.assignSplit(pending, subtaskId);
                    pending = null;
                }
            }

            @Override
            public void addSplitsBack(List<Cursor> splits, int subtaskId) {
                if (!splits.isEmpty()) {
                    pending = splits.get(0);
                }
            }

            @Override
            public void addReader(int subtaskId) {
                handleSplitRequest(subtaskId, null);
            }

            @Override
            public Integer snapshotState(long checkpointId) {
                return pending == null ? -1 : pending.next;
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public SimpleVersionedSerializer<Cursor> getSplitSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Cursor cursor) {
                return integerBytes(cursor.next);
            }

            @Override
            public Cursor deserialize(int version, byte[] bytes) throws IOException {
                return new Cursor(readInteger(version, bytes));
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Integer value) {
                return integerBytes(value);
            }

            @Override
            public Integer deserialize(int version, byte[] bytes) throws IOException {
                return readInteger(version, bytes);
            }
        };
    }

    private static byte[] integerBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static int readInteger(int version, byte[] bytes) throws IOException {
        if (version != 1 || bytes.length != 4) {
            throw new IOException("Unsupported input cursor");
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static final class Cursor implements SourceSplit {
        private final int next;

        Cursor(int next) {
            this.next = next;
        }

        @Override
        public String splitId() {
            return "input";
        }
    }
}
