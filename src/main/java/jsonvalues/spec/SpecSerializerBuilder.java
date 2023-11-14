package jsonvalues.spec;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import static java.util.Objects.requireNonNull;

/**
 * Builder class for constructing instances of {@link SpecSerializer}.
 */
public final class SpecSerializerBuilder {
    private final JsSpec spec;
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
     * Sets the EncoderFactory for creating binary or JSON encoders. If {@code null}, the default EncoderFactory is used.
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
        return new SpecSerializer(spec, reused, factory);
    }
}