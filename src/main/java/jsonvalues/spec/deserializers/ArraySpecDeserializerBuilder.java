package jsonvalues.spec.deserializers;

import java.util.Objects;
import jsonvalues.spec.JsArraySpec;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Builder class for creating instances of {@link ObjSpecDeserializer}.
 * <p>
 * This builder provides a convenient way to configure and create {@code SpecDeserializer} instances for deserializing
 * JSON values based on reader and writer specifications.
 * </p>
 */
public final class ArraySpecDeserializerBuilder {

  private final JsArraySpec spec;
  private GenericArray<?> reusedArray;
  private DecoderFactory decoderFactory = DecoderFactory.get();
  private BinaryDecoder reusedDecoder;
  private boolean isJFREnabled = true;


  private ArraySpecDeserializerBuilder(JsArraySpec spec) {
    this.spec = Objects.requireNonNull(spec);
  }


  /**
   * Creates a new instance of {@code SpecDeserializerBuilder} with the given spec as the reader and writer
   * specifications.
   *
   * @param spec The specification for writing and reading JSON values.
   * @return A new instance of {@code SpecDeserializerBuilder}.
   */
  public static ArraySpecDeserializerBuilder of(JsArraySpec spec) {
    return new ArraySpecDeserializerBuilder(spec);
  }

  /**
   * Sets the reused record for the deserializer.
   *
   * @param reusedArray The reused {@link GenericRecord} for deserialization.
   * @return This builder instance.
   */
  public ArraySpecDeserializerBuilder withReusedArray(GenericArray<?> reusedArray) {
    this.reusedArray = Objects.requireNonNull(reusedArray);
    return this;
  }

  /**
   * Disables Java Flight Recorder (JFR) events for the deserializer.
   *
   * @return This builder instance.
   */
  public ArraySpecDeserializerBuilder withoutJFREvents() {
    this.isJFREnabled = false;
    return this;
  }

  /**
   * Sets the {@link DecoderFactory} for the deserializer.
   *
   * @param decoderFactory The {@code DecoderFactory} to be used for decoding.
   * @return This builder instance.
   */
  public ArraySpecDeserializerBuilder withDecoderFactory(DecoderFactory decoderFactory) {
    this.decoderFactory = Objects.requireNonNull(decoderFactory);
    return this;
  }

  /**
   * Sets the reused decoder for the deserializer.
   *
   * @param reusedDecoder The reused {@link BinaryDecoder} for deserialization.
   * @return This builder instance.
   */
  public ArraySpecDeserializerBuilder withReusedDecoder(BinaryDecoder reusedDecoder) {
    this.reusedDecoder = Objects.requireNonNull(reusedDecoder);
    return this;
  }


  /**
   * Builds and returns a new instance of {@link ObjSpecDeserializer} based on the configured settings.
   *
   * @return A new instance of {@code SpecDeserializer}.
   */
  public ArraySpecDeserializer build() {
    return new ArraySpecDeserializer(spec,
                                     reusedArray,
                                     decoderFactory,
                                     reusedDecoder,
                                     isJFREnabled);
  }
}