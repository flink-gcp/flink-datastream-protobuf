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

package io.github.flink.gcp.protobuf.spike;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/** Test instrument for factory routing and transport; it has no state compatibility contract. */
public class ProbeTypeInfoFactory extends TypeInfoFactory<Message> {
    @Override
    public TypeInformation<Message> createTypeInfo(
            Type type, Map<String, TypeInformation<?>> parameters) {
        if (!(type instanceof Class<?>)) {
            throw new IllegalArgumentException("Expected a concrete generated class: " + type);
        }
        return new ProbeTypeInformation<>(((Class<?>) type).asSubclass(Message.class));
    }

    public static final class ProbeTypeInformation<T extends Message> extends TypeInformation<T> {
        private final Class<T> type;

        public ProbeTypeInformation(Class<T> type) {
            this.type = type;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<T> getTypeClass() {
            return type;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<T> createSerializer(SerializerConfig config) {
            return new ProbeSerializer<>(type);
        }

        @Override
        public String toString() {
            return "Probe(" + type.getName() + ")";
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ProbeTypeInformation<?>
                    && type.equals(((ProbeTypeInformation<?>) other).type);
        }

        @Override
        public int hashCode() {
            return type.hashCode();
        }

        @Override
        public boolean canEqual(Object other) {
            return other instanceof ProbeTypeInformation<?>;
        }
    }

    public static final class ProbeSerializer<T extends Message> extends TypeSerializer<T> {
        private final Class<T> type;
        private transient Parser<? extends Message> parser;

        public ProbeSerializer(Class<T> type) {
            this.type = type;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<T> duplicate() {
            return new ProbeSerializer<>(type);
        }

        @Override
        public T createInstance() {
            try {
                return type.cast(type.getMethod("getDefaultInstance").invoke(null));
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public T copy(T value) {
            return value;
        }

        @Override
        public T copy(T value, T reuse) {
            return value;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(T value, DataOutputView out) throws IOException {
            byte[] bytes = value.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        public T deserialize(DataInputView in) throws IOException {
            int size = in.readInt();
            if (size < 0 || size > 1048576) {
                throw new IOException("Invalid probe frame size: " + size);
            }
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            if (parser == null) {
                parser = createInstance().getParserForType();
            }
            return type.cast(parser.parseFrom(bytes));
        }

        @Override
        public T deserialize(T reuse, DataInputView in) throws IOException {
            return deserialize(in);
        }

        @Override
        public void copy(DataInputView in, DataOutputView out) throws IOException {
            serialize(deserialize(in), out);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ProbeSerializer<?>
                    && type.equals(((ProbeSerializer<?>) other).type);
        }

        @Override
        public int hashCode() {
            return type.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<T> snapshotConfiguration() {
            throw new UnsupportedOperationException("The spike does not test managed state");
        }
    }
}
