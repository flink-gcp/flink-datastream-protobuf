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

package io.github.flink.gcp.protobuf.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import com.google.protobuf.ByteString;
import com.twitter.chill.protobuf.ProtobufSerializer;
import com.twitter.chill.thrift.TBaseSerializer;
import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.benchmark.generated.BenchmarkProtos;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Explicit representation and Flink serializer selection; no registry mutation. */
public enum Lane {
    POJO,
    TUPLE,
    AVRO_SPECIFIC,
    AVRO_GENERIC,
    PROTOBUF_NATIVE,
    PROTOBUF_CHILL,
    THRIFT_CHILL,
    KRYO;

    public boolean generic() {
        return this == PROTOBUF_CHILL || this == THRIFT_CHILL || this == KRYO;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeSerializer<Object> serializer(boolean deterministic)
            throws ReflectiveOperationException {
        SerializerConfigImpl config =
                (SerializerConfigImpl) new ExecutionConfig().getSerializerConfig();
        config.setGenericTypes(generic());
        TypeInformation<?> info;
        Class<?> expected;
        switch (this) {
            case POJO -> {
                info = pojoType();
                expected = PojoSerializer.class;
            }
            case TUPLE -> {
                info =
                        Types.TUPLE(
                                Types.INT,
                                Types.LONG,
                                Types.BOOLEAN,
                                Types.STRING,
                                Types.TUPLE(Types.BOOLEAN, Types.STRING),
                                Types.PRIMITIVE_ARRAY(Types.BYTE),
                                Types.TUPLE(Types.INT, Types.STRING),
                                Types.LIST(Types.LONG),
                                Types.MAP(Types.STRING, Types.LONG));
                expected = TupleSerializer.class;
            }
            case AVRO_SPECIFIC -> {
                info =
                        new AvroTypeInfo<>(
                                io.github.flink.gcp.protobuf.benchmark.generated.avro.Record.class);
                expected = AvroSerializer.class;
            }
            case AVRO_GENERIC -> {
                info = new GenericRecordAvroTypeInfo(schema());
                expected = AvroSerializer.class;
            }
            case PROTOBUF_NATIVE -> {
                info =
                        ProtobufTypeInformation.newBuilder(BenchmarkProtos.Record.class)
                                .deterministicSerialization(deterministic)
                                .build();
                expected = Class.forName("io.github.flink.gcp.protobuf.ProtobufTypeSerializer");
            }
            case PROTOBUF_CHILL -> {
                config.registerTypeWithKryoSerializer(
                        BenchmarkProtos.Record.class, ProtobufSerializer.class);
                info = new GenericTypeInfo<>(BenchmarkProtos.Record.class);
                expected = KryoSerializer.class;
            }
            case THRIFT_CHILL -> {
                config.addDefaultKryoSerializer(
                        org.apache.thrift.TBase.class, TBaseSerializer.class);
                config.registerKryoType(
                        io.github.flink.gcp.protobuf.benchmark.generated.thrift.Record.class);
                info =
                        new GenericTypeInfo<>(
                                io.github.flink.gcp.protobuf.benchmark.generated.thrift.Record
                                        .class);
                expected = KryoSerializer.class;
            }
            case KRYO -> {
                for (Class<?> type :
                        List.of(
                                GenericValue.class,
                                PojoValue.Child.class,
                                ArrayList.class,
                                LinkedHashMap.class,
                                byte[].class)) {
                    config.registerKryoType(type);
                }
                info = TypeInformation.of(GenericValue.class);
                expected = KryoSerializer.class;
            }
            default -> throw new IllegalStateException(name());
        }
        TypeSerializer<Object> serializer = (TypeSerializer<Object>) info.createSerializer(config);
        require(
                expected.isInstance(serializer),
                "Unexpected serializer for " + this + ": " + serializer.getClass());
        if (generic()) {
            KryoSerializer<?> kryo = (KryoSerializer<?>) serializer;
            if (this == PROTOBUF_CHILL) {
                require(
                        kryo.getKryo().getSerializer(BenchmarkProtos.Record.class).getClass()
                                == ProtobufSerializer.class,
                        "Missing Protobuf adapter");
            }
            if (this == THRIFT_CHILL) {
                require(
                        kryo.getKryo()
                                        .getSerializer(
                                                io.github.flink.gcp.protobuf.benchmark.generated
                                                        .thrift.Record.class)
                                        .getClass()
                                == TBaseSerializer.class,
                        "Missing Thrift adapter");
            }
        } else {
            inspect(serializer, new IdentityHashMap<>(), true);
        }
        return serializer;
    }

    private static PojoTypeInfo<PojoValue> pojoType() throws NoSuchFieldException {
        PojoTypeInfo<PojoValue.Child> child =
                new PojoTypeInfo<>(
                        PojoValue.Child.class,
                        List.of(
                                new PojoField(PojoValue.Child.class.getField("code"), Types.INT),
                                new PojoField(
                                        PojoValue.Child.class.getField("name"), Types.STRING)));
        String[] names = {
            "id", "timestamp", "active", "name", "note", "payload", "child", "values", "attributes"
        };
        TypeInformation<?>[] types = {
            Types.INT,
            Types.LONG,
            Types.BOOLEAN,
            Types.STRING,
            Types.STRING,
            Types.PRIMITIVE_ARRAY(Types.BYTE),
            child,
            Types.LIST(Types.LONG),
            Types.MAP(Types.STRING, Types.LONG)
        };
        List<PojoField> fields = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            fields.add(new PojoField(PojoValue.class.getField(names[i]), types[i]));
        }
        return new PojoTypeInfo<>(PojoValue.class, fields);
    }

    static String inspect(
            TypeSerializer<?> serializer, IdentityHashMap<Object, Boolean> seen, boolean rejectKryo)
            throws IllegalAccessException {
        if (seen.put(serializer, true) != null) {
            return "";
        }
        if (rejectKryo) {
            require(
                    !(serializer instanceof KryoSerializer<?>),
                    "Nested Kryo serializer: " + serializer);
        }
        StringBuilder result = new StringBuilder(serializer.getClass().getName());
        for (Class<?> type = serializer.getClass(); type != null; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                if (TypeSerializer.class.isAssignableFrom(field.getType())
                        || field.getType() == TypeSerializer[].class) {
                    field.setAccessible(true);
                    Object value = field.get(serializer);
                    if (value instanceof TypeSerializer<?> child) {
                        result.append(" [")
                                .append(field.getName())
                                .append("=")
                                .append(inspect(child, seen, rejectKryo))
                                .append("]");
                    }
                    if (value instanceof TypeSerializer<?>[] children) {
                        for (TypeSerializer<?> child : children) {
                            result.append(" [")
                                    .append(inspect(child, seen, rejectKryo))
                                    .append("]");
                        }
                    }
                }
            }
        }
        return result.toString();
    }

    public Object encode(Corpus.Value v) {
        return switch (this) {
            case POJO -> pojo(v);
            case KRYO -> new GenericValue(pojo(v));
            case TUPLE ->
                    Tuple9.of(
                            v.id(),
                            v.timestamp(),
                            v.active(),
                            v.name(),
                            Tuple2.of(v.note() != null, v.note() == null ? "" : v.note()),
                            v.bytes(),
                            Tuple2.of(v.child().code(), v.child().name()),
                            new ArrayList<>(v.values()),
                            new LinkedHashMap<>(v.attributes()));
            case PROTOBUF_NATIVE, PROTOBUF_CHILL -> {
                BenchmarkProtos.Record.Builder b =
                        BenchmarkProtos.Record.newBuilder()
                                .setId(v.id())
                                .setTimestamp(v.timestamp())
                                .setActive(v.active())
                                .setName(v.name())
                                .setPayload(ByteString.copyFrom(v.payload()))
                                .setChild(
                                        BenchmarkProtos.Child.newBuilder()
                                                .setCode(v.child().code())
                                                .setName(v.child().name()))
                                .addAllValues(v.values())
                                .putAllAttributes(v.attributes());
                if (v.note() != null) {
                    b.setNote(v.note());
                }
                yield b.build();
            }
            case AVRO_SPECIFIC, AVRO_GENERIC -> avro(v);
            case THRIFT_CHILL -> {
                var t = new io.github.flink.gcp.protobuf.benchmark.generated.thrift.Record();
                t.setId(v.id())
                        .setTimestamp(v.timestamp())
                        .setActive(v.active())
                        .setName(v.name())
                        .setPayload(v.payload())
                        .setChild(
                                new io.github.flink.gcp.protobuf.benchmark.generated.thrift.Child(
                                        v.child().code(), v.child().name()))
                        .setValues(new ArrayList<>(v.values()))
                        .setAttributes(new LinkedHashMap<>(v.attributes()));
                if (v.note() != null) {
                    t.setNote(v.note());
                }
                yield t;
            }
        };
    }

    private static PojoValue pojo(Corpus.Value v) {
        PojoValue p = new PojoValue();
        p.id = v.id();
        p.timestamp = v.timestamp();
        p.active = v.active();
        p.name = v.name();
        p.note = v.note();
        p.payload = v.bytes();
        p.child = new PojoValue.Child(v.child().code(), v.child().name());
        p.values = new ArrayList<>(v.values());
        p.attributes = new LinkedHashMap<>(v.attributes());
        return p;
    }

    private static Schema schema() {
        return io.github.flink.gcp.protobuf.benchmark.generated.avro.Record.getClassSchema();
    }

    private IndexedRecord avro(Corpus.Value v) {
        IndexedRecord r =
                this == AVRO_SPECIFIC
                        ? new io.github.flink.gcp.protobuf.benchmark.generated.avro.Record()
                        : new GenericData.Record(schema());
        IndexedRecord child =
                this == AVRO_SPECIFIC
                        ? new io.github.flink.gcp.protobuf.benchmark.generated.avro.Child()
                        : new GenericData.Record(schema().getField("child").schema());
        child.put(0, v.child().code());
        child.put(1, v.child().name());
        Object[] fields = {
            v.id(),
            v.timestamp(),
            v.active(),
            v.name(),
            v.note(),
            ByteBuffer.wrap(v.bytes()),
            child,
            new ArrayList<>(v.values()),
            new LinkedHashMap<>(v.attributes())
        };
        for (int i = 0; i < fields.length; i++) {
            r.put(i, fields[i]);
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    public Corpus.Value decode(Object object) {
        if (object instanceof PojoValue p) {
            return Corpus.decoded(
                    p.id,
                    p.timestamp,
                    p.active,
                    p.name,
                    p.note,
                    p.payload,
                    new Corpus.Child(p.child.code, p.child.name),
                    p.values,
                    p.attributes);
        }
        if (object instanceof GenericValue p) {
            return Corpus.decoded(
                    p.id,
                    p.timestamp,
                    p.active,
                    p.name,
                    p.note,
                    p.payload,
                    new Corpus.Child(p.child.code, p.child.name),
                    p.values,
                    p.attributes);
        }
        if (object instanceof BenchmarkProtos.Record p) {
            return Corpus.decoded(
                    p.getId(),
                    p.getTimestamp(),
                    p.getActive(),
                    p.getName(),
                    p.hasNote() ? p.getNote() : null,
                    p.getPayload().toByteArray(),
                    new Corpus.Child(p.getChild().getCode(), p.getChild().getName()),
                    p.getValuesList(),
                    p.getAttributesMap());
        }
        if (object instanceof io.github.flink.gcp.protobuf.benchmark.generated.thrift.Record p) {
            return Corpus.decoded(
                    p.id,
                    p.timestamp,
                    p.active,
                    p.name,
                    p.isSetNote() ? p.note : null,
                    p.getPayload(),
                    new Corpus.Child(p.child.code, p.child.name),
                    p.values,
                    p.attributes);
        }
        if (object instanceof Tuple9<?, ?, ?, ?, ?, ?, ?, ?, ?> p) {
            Tuple2<Boolean, String> note = (Tuple2<Boolean, String>) p.f4;
            Tuple2<Integer, String> child = (Tuple2<Integer, String>) p.f6;
            return Corpus.decoded(
                    (int) p.f0,
                    (long) p.f1,
                    (boolean) p.f2,
                    (String) p.f3,
                    note.f0 ? note.f1 : null,
                    (byte[]) p.f5,
                    new Corpus.Child(child.f0, child.f1),
                    (List<Long>) p.f7,
                    (Map<String, Long>) p.f8);
        }
        IndexedRecord p = (IndexedRecord) object;
        IndexedRecord child = (IndexedRecord) p.get(6);
        ByteBuffer bytes = ((ByteBuffer) p.get(5)).duplicate();
        byte[] payload = new byte[bytes.remaining()];
        bytes.get(payload);
        Map<String, Long> attrs = new LinkedHashMap<>();
        ((Map<?, ?>) p.get(8)).forEach((k, v) -> attrs.put(k.toString(), (Long) v));
        return Corpus.decoded(
                (int) p.get(0),
                (long) p.get(1),
                (boolean) p.get(2),
                p.get(3).toString(),
                p.get(4) == null ? null : p.get(4).toString(),
                payload,
                new Corpus.Child((int) child.get(0), child.get(1).toString()),
                (List<Long>) p.get(7),
                attrs);
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
