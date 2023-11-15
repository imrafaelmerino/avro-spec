package jsonvalues.spec;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Builder class for creating instances of {@link SpecDeserializer}.
 * <p>
 * This builder provides a convenient way to configure and create {@code SpecDeserializer} instances for deserializing
 * JSON values based on reader and writer specifications.
 * </p>
 */
public final class SpecDeserializerBuilder {
    private final JsSpec readerSpec;
    private final JsSpec writerSpec;
    private GenericRecord reusedRecord;
    private DecoderFactory decoderFactory = DecoderFactory.get();
    private BinaryDecoder reusedDecoder;

    private boolean enableDebug = false;
    private String deserializerName;



    private SpecDeserializerBuilder(JsSpec readerSpec, JsSpec writerSpec) {
        this.readerSpec = Objects.requireNonNull(readerSpec);
        this.writerSpec = Objects.requireNonNull(writerSpec);
    }
    /**
     * Creates a new instance of {@code SpecDeserializerBuilder} with the given reader and writer specifications.
     *
     * @param readerSpec The specification for reading JSON values.
     * @param writerSpec The specification for writing JSON values.
     * @return A new instance of {@code SpecDeserializerBuilder}.
     */
    public static SpecDeserializerBuilder of(JsSpec readerSpec, JsSpec writerSpec) {
        return new SpecDeserializerBuilder(readerSpec, writerSpec);
    }

    /**
     * Creates a new instance of {@code SpecDeserializerBuilder} with the given spec as the reader and writer specifications.
     *
     * @param spec The specification for writing and reading JSON values.
     * @return A new instance of {@code SpecDeserializerBuilder}.
     *
     * @see #of(JsSpec, JsSpec)
     */
    public static SpecDeserializerBuilder of(JsSpec spec) {
        return new SpecDeserializerBuilder(spec, spec);
    }

    /**
     * Sets the reused record for the deserializer.
     *
     * @param reusedRecord The reused {@link GenericRecord} for deserialization.
     * @return This builder instance.
     */
    public SpecDeserializerBuilder withReusedRecord(GenericRecord reusedRecord) {
        this.reusedRecord = Objects.requireNonNull(reusedRecord);
        return this;
    }
    /**
     * Sets the {@link DecoderFactory} for the deserializer.
     *
     * @param decoderFactory The {@code DecoderFactory} to be used for decoding.
     * @return This builder instance.
     */
    public SpecDeserializerBuilder withDecoderFactory(DecoderFactory decoderFactory) {
        this.decoderFactory = Objects.requireNonNull(decoderFactory);
        return this;
    }

    /**
     * Sets the reused decoder for the deserializer.
     *
     * @param reusedDecoder The reused {@link BinaryDecoder} for deserialization.
     * @return This builder instance.
     */
    public SpecDeserializerBuilder withReusedDecoder(BinaryDecoder reusedDecoder) {
        this.reusedDecoder = Objects.requireNonNull(reusedDecoder);
        return this;
    }

    /**
     * Enables debugging for Avro Spec Deserialization by capturing Java Flight Recorder (JFR) events.
     * When debug mode is enabled, the library generates events using the {@link SpecDeserializerEvent} class,
     * providing insights into the deserialization process. The specified deserializer name is used to identify
     * the events and distinguish between different deserialization processes.
     *
     * @param deserializerName The name to identify the deserializer in JFR events.
     * @return The updated {@link SpecDeserializerBuilder} instance with debugging enabled.
     */
    public SpecDeserializerBuilder enableDebug(final String deserializerName) {
        this.enableDebug = true;
        this.deserializerName = requireNonNull(deserializerName);
        return this;
    }

    /**
     * Builds and returns a new instance of {@link SpecDeserializer} based on the configured settings.
     *
     * @return A new instance of {@code SpecDeserializer}.
     */
    public SpecDeserializer build() {
        return new SpecDeserializer(deserializerName,readerSpec, writerSpec, reusedRecord, decoderFactory, reusedDecoder,enableDebug);
    }
}