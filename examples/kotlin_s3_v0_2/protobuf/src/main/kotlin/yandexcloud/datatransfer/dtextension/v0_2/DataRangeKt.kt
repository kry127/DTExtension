//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: api/v0_2/common.proto

package yandexcloud.datatransfer.dtextension.v0_2;

@kotlin.jvm.JvmName("-initializedataRange")
public inline fun dataRange(block: yandexcloud.datatransfer.dtextension.v0_2.DataRangeKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange =
  yandexcloud.datatransfer.dtextension.v0_2.DataRangeKt.Dsl._create(yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange.newBuilder()).apply { block() }._build()
public object DataRangeKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange = _builder.build()

    /**
     * <pre>
     * absence of 'from' field means -oo
     * </pre>
     *
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ColumnValue from_exclusive = 1;</code>
     */
    public var fromExclusive: yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnValue
      @JvmName("getFromExclusive")
      get() = _builder.getFromExclusive()
      @JvmName("setFromExclusive")
      set(value) {
        _builder.setFromExclusive(value)
      }
    /**
     * <pre>
     * absence of 'from' field means -oo
     * </pre>
     *
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ColumnValue from_exclusive = 1;</code>
     */
    public fun clearFromExclusive() {
      _builder.clearFromExclusive()
    }
    /**
     * <pre>
     * absence of 'from' field means -oo
     * </pre>
     *
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ColumnValue from_exclusive = 1;</code>
     * @return Whether the fromExclusive field is set.
     */
    public fun hasFromExclusive(): kotlin.Boolean {
      return _builder.hasFromExclusive()
    }

    /**
     * <pre>
     * absence of 'to' field means +oo
     * </pre>
     *
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ColumnValue to_inclusive = 2;</code>
     */
    public var toInclusive: yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnValue
      @JvmName("getToInclusive")
      get() = _builder.getToInclusive()
      @JvmName("setToInclusive")
      set(value) {
        _builder.setToInclusive(value)
      }
    /**
     * <pre>
     * absence of 'to' field means +oo
     * </pre>
     *
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ColumnValue to_inclusive = 2;</code>
     */
    public fun clearToInclusive() {
      _builder.clearToInclusive()
    }
    /**
     * <pre>
     * absence of 'to' field means +oo
     * </pre>
     *
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ColumnValue to_inclusive = 2;</code>
     * @return Whether the toInclusive field is set.
     */
    public fun hasToInclusive(): kotlin.Boolean {
      return _builder.hasToInclusive()
    }
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange.copy(block: yandexcloud.datatransfer.dtextension.v0_2.DataRangeKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange =
  yandexcloud.datatransfer.dtextension.v0_2.DataRangeKt.Dsl._create(this.toBuilder()).apply { block() }._build()

val yandexcloud.datatransfer.dtextension.v0_2.Common.DataRangeOrBuilder.fromExclusiveOrNull: yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnValue?
  get() = if (hasFromExclusive()) getFromExclusive() else null

val yandexcloud.datatransfer.dtextension.v0_2.Common.DataRangeOrBuilder.toInclusiveOrNull: yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnValue?
  get() = if (hasToInclusive()) getToInclusive() else null

