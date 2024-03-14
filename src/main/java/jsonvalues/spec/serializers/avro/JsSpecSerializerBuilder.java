package jsonvalues.spec.serializers.avro;

import static java.util.Objects.requireNonNull;

import jsonvalues.spec.JsSpec;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Builder class for constructing instances of {@link JsSpecSerializer}.
 */
public final class JsSpecSerializerBuilder {

  private boolean isJFREnabled = true;

  private final JsSpec spec;
  private BinaryEncoder reused = null;
  private EncoderFactory factory = EncoderFactory.get();

  private JsSpecSerializerBuilder(final JsSpec spec) {
    this.spec = requireNonNull(spec);
  }

  /**
   * Creates a new SpecSerializerBuilder instance with the provided JsSpec.
   *
   * @param spec The JsSpec representing the JSON validation rules.
   * @return A SpecSerializerBuilder instance.
   */
  public static JsSpecSerializerBuilder of(final JsSpec spec) {
    return new JsSpecSerializerBuilder(spec);
  }


  /**
   * Sets the BinaryEncoder to be reused during serialization.
   *
   * @param reused The BinaryEncoder to be reused.
   * @return This SpecSerializerBuilder instance.
   */
  public JsSpecSerializerBuilder withReusedEncoder(final BinaryEncoder reused) {
    this.reused = requireNonNull(reused);
    return this;
  }

  /**
   * Sets the EncoderFactory for creating binary or JSON encoders. If {@code null}, the default EncoderFactory is used.
   *
   * @param factory The EncoderFactory to be used.
   * @return This SpecSerializerBuilder instance.
   */
  public JsSpecSerializerBuilder withEncoderFactory(final EncoderFactory factory) {
    this.factory = requireNonNull(factory);
    return this;
  }

  public JsSpecSerializerBuilder withoutJFREvents() {
    this.isJFREnabled = false;
    return this;
  }

  /**
   * Builds a new instance of SpecSerializer based on the configured settings.
   *
   * @return A SpecSerializer instance.
   */
  public JsSpecSerializer build() {
    return new JsSpecSerializer(isJFREnabled,
                                spec,
                                reused,
                                factory);
  }
}