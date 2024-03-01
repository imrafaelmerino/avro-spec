package jsonvalues.spec.deserializers.avro;

import java.util.Objects;
import jsonvalues.spec.JsSpec;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Builder class for creating instances of {@link JsObjSpecDeserializer}.
 * <p>
 * This builder provides a convenient way to configure and create {@code SpecDeserializer} instances for deserializing
 * JSON values based on reader and writer specifications.
 * </p>
 */
public final class JsonSpecDeserializerBuilder {

  private final JsSpec spec;
  private DecoderFactory decoderFactory = DecoderFactory.get();
  private BinaryDecoder reusedDecoder;
  private boolean isJFREnabled = true;


  private JsonSpecDeserializerBuilder(JsSpec spec) {
    this.spec = Objects.requireNonNull(spec);
  }


  /**
   * Creates a new instance of {@code SpecDeserializerBuilder} with the given spec as the reader and writer
   * specifications.
   *
   * @param spec The specification for writing and reading JSON values.
   * @return A new instance of {@code SpecDeserializerBuilder}.
   */
  public static JsonSpecDeserializerBuilder of(JsSpec spec) {
    return new JsonSpecDeserializerBuilder(spec);
  }


  /**
   * Sets the {@link DecoderFactory} for the deserializer.
   *
   * @param decoderFactory The {@code DecoderFactory} to be used for decoding.
   * @return This builder instance.
   */
  public JsonSpecDeserializerBuilder withDecoderFactory(DecoderFactory decoderFactory) {
    this.decoderFactory = Objects.requireNonNull(decoderFactory);
    return this;
  }

  /**
   * Sets the reused decoder for the deserializer.
   *
   * @param reusedDecoder The reused {@link BinaryDecoder} for deserialization.
   * @return This builder instance.
   */
  public JsonSpecDeserializerBuilder withReusedDecoder(BinaryDecoder reusedDecoder) {
    this.reusedDecoder = Objects.requireNonNull(reusedDecoder);
    return this;
  }

  public JsonSpecDeserializerBuilder withoutJFREvents() {
    this.isJFREnabled = false;
    return this;
  }


  /**
   * Builds and returns a new instance of {@link JsObjSpecDeserializer} based on the configured settings.
   *
   * @return A new instance of {@code SpecDeserializer}.
   */
  public JsonSpecDeserializer build() {
    return new JsonSpecDeserializer(spec,
                                    decoderFactory,
                                    reusedDecoder,
                                    isJFREnabled);
  }
}