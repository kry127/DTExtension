//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: api/v0_2/source/control.proto

package yandexcloud.datatransfer.dtextension.v0_2.source;

@kotlin.jvm.JvmName("-initializesplitReq")
public inline fun splitReq(block: yandexcloud.datatransfer.dtextension.v0_2.source.SplitReqKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq =
  yandexcloud.datatransfer.dtextension.v0_2.source.SplitReqKt.Dsl._create(yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq.newBuilder()).apply { block() }._build()
public object SplitReqKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq = _builder.build()

    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.DataRange data_range = 1;</code>
     */
    public var dataRange: yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange
      @JvmName("getDataRange")
      get() = _builder.getDataRange()
      @JvmName("setDataRange")
      set(value) {
        _builder.setDataRange(value)
      }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.DataRange data_range = 1;</code>
     */
    public fun clearDataRange() {
      _builder.clearDataRange()
    }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.DataRange data_range = 1;</code>
     * @return Whether the dataRange field is set.
     */
    public fun hasDataRange(): kotlin.Boolean {
      return _builder.hasDataRange()
    }
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq.copy(block: yandexcloud.datatransfer.dtextension.v0_2.source.SplitReqKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReq =
  yandexcloud.datatransfer.dtextension.v0_2.source.SplitReqKt.Dsl._create(this.toBuilder()).apply { block() }._build()

val yandexcloud.datatransfer.dtextension.v0_2.source.Control.SplitReqOrBuilder.dataRangeOrNull: yandexcloud.datatransfer.dtextension.v0_2.Common.DataRange?
  get() = if (hasDataRange()) getDataRange() else null

