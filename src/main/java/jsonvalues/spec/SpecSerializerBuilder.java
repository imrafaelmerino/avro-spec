package jsonvalues.spec;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import static java.util.Objects.requireNonNull;

/**
 * Builder class for constructing instances of {@link SpecSerializer}.
 */
public final class SpecSerializerBuilder {

    private final JsSpec spec;
    private boolean enableDebug = false;
    private String serializerName;
    private BinaryEncoder reused = null;
    private EncoderFactory factory = EncoderFactory.get();

    private SpecSerializerBuilder(final JsSpec spec) {
        this.spec = requireNonNull(spec);
    }

    /**
     * Creates a new SpecSerializerBuilder instance with the provided JsSpec.
     *
     * @param spec The JsSpec representing the JSON validation rules.
     * @return A SpecSerializerBuilder instance.
     */
    public static SpecSerializerBuilder of(final JsSpec spec) {
        return new SpecSerializerBuilder(spec);
    }


    /**
     * Enables debugging for Avro Spec Serialization by capturing Java Flight Recorder (JFR) events. When debug mode is
     * enabled, the library generates events using the {@link AvroSpecSerializerEvent} class, providing insights into
     * the serialization process. The specified serializer name is used to identify the events and distinguish between
     * different serialization processes.
     *
     * @param serializerName The name to identify the serializer in JFR events.
     * @return The updated {@link SpecSerializerBuilder} instance with debugging enabled.
     */
    public SpecSerializerBuilder enableDebug(final String serializerName) {
        this.enableDebug = true;
        this.serializerName = requireNonNull(serializerName);
        return this;
    }

    /**
     * Sets the BinaryEncoder to be reused during serialization.
     *
     * @param reused The BinaryEncoder to be reused.
     * @return This SpecSerializerBuilder instance.
     */
    public SpecSerializerBuilder withReusedEncoder(final BinaryEncoder reused) {
        this.reused = requireNonNull(reused);
        return this;
    }

    /**
     * Sets the EncoderFactory for creating binary or JSON encoders. If {@code null}, the default EncoderFactory is
     * used.
     *
     * @param factory The EncoderFactory to be used.
     * @return This SpecSerializerBuilder instance.
     */
    public SpecSerializerBuilder withEncoderFactory(final EncoderFactory factory) {
        this.factory = requireNonNull(factory);
        return this;
    }

    /**
     * Builds a new instance of SpecSerializer based on the configured settings.
     *
     * @return A SpecSerializer instance.
     */
    public SpecSerializer build() {
        return new SpecSerializer(serializerName, spec, reused, factory, enableDebug);
    }
}