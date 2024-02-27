package jsonvalues.spec;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
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

  private final JsArraySpec readerSpec;
  private final JsArraySpec writerSpec;
  private GenericArray<?> reusedRecord;
  private DecoderFactory decoderFactory = DecoderFactory.get();
  private BinaryDecoder reusedDecoder;

  private boolean enableDebug = false;
  private String deserializerName;


  private ArraySpecDeserializerBuilder(JsArraySpec readerSpec,
                                       JsArraySpec writerSpec) {
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
  public static ArraySpecDeserializerBuilder of(JsArraySpec readerSpec,
                                                JsArraySpec writerSpec) {
    return new ArraySpecDeserializerBuilder(readerSpec,
                                            writerSpec);
  }

  /**
   * Creates a new instance of {@code SpecDeserializerBuilder} with the given spec as the reader and writer
   * specifications.
   *
   * @param spec The specification for writing and reading JSON values.
   * @return A new instance of {@code SpecDeserializerBuilder}.
   * @see #of(JsArraySpec, JsArraySpec)
   */
  public static ArraySpecDeserializerBuilder of(JsArraySpec spec) {
    return new ArraySpecDeserializerBuilder(spec,
                                            spec);
  }

  /**
   * Sets the reused record for the deserializer.
   *
   * @param reusedRecord The reused {@link GenericRecord} for deserialization.
   * @return This builder instance.
   */
  public ArraySpecDeserializerBuilder withReusedRecord(GenericArray<?> reusedRecord) {
    this.reusedRecord = Objects.requireNonNull(reusedRecord);
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
    return new ArraySpecDeserializer(deserializerName,
                                     readerSpec,
                                     writerSpec,
                                     reusedRecord,
                                     decoderFactory,
                                     reusedDecoder);
  }
}