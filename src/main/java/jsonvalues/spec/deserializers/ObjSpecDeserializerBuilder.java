package jsonvalues.spec.deserializers;

import java.util.Objects;
import jsonvalues.spec.JsSpec;
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
public final class ObjSpecDeserializerBuilder {

  private ObjSpecDeserializerBuilder(final JsSpec spec) {
    this.spec = spec;
  }

  private final JsSpec spec;
  private GenericRecord reusedRecord;
  private DecoderFactory decoderFactory = DecoderFactory.get();
  private BinaryDecoder reusedDecoder;
  private boolean isJFREnabled = true;


  /**
   * Creates a new instance of {@code SpecDeserializerBuilder} with the given spec as the reader and writer
   * specifications.
   *
   * @param spec The specification for writing and reading JSON values.
   * @return A new instance of {@code SpecDeserializerBuilder}.
   */
  public static ObjSpecDeserializerBuilder of(JsSpec spec) {
    return new ObjSpecDeserializerBuilder(spec);
  }

  /**
   * Sets the reused record for the deserializer.
   *
   * @param reusedRecord The reused {@link GenericRecord} for deserialization.
   * @return This builder instance.
   */
  public ObjSpecDeserializerBuilder withReusedRecord(GenericRecord reusedRecord) {
    this.reusedRecord = Objects.requireNonNull(reusedRecord);
    return this;
  }

  /**
   * Sets the {@link DecoderFactory} for the deserializer.
   *
   * @param decoderFactory The {@code DecoderFactory} to be used for decoding.
   * @return This builder instance.
   */
  public ObjSpecDeserializerBuilder withDecoderFactory(DecoderFactory decoderFactory) {
    this.decoderFactory = Objects.requireNonNull(decoderFactory);
    return this;
  }

  /**
   * Sets the reused decoder for the deserializer.
   *
   * @param reusedDecoder The reused {@link BinaryDecoder} for deserialization.
   * @return This builder instance.
   */
  public ObjSpecDeserializerBuilder withReusedDecoder(BinaryDecoder reusedDecoder) {
    this.reusedDecoder = Objects.requireNonNull(reusedDecoder);
    return this;
  }

  /**
   * Disables Java Flight Recorder (JFR) events for the deserializer.
   *
   * @return This builder instance.
   */
  public ObjSpecDeserializerBuilder withoutJFREvents() {
    this.isJFREnabled = false;
    return this;
  }


  /**
   * Builds and returns a new instance of {@link ObjSpecDeserializer} based on the configured settings.
   *
   * @return A new instance of {@code SpecDeserializer}.
   */
  public ObjSpecDeserializer build() {
    return new ObjSpecDeserializer(spec,
                                   reusedRecord,
                                   decoderFactory,
                                   reusedDecoder,
                                   isJFREnabled);
  }
}