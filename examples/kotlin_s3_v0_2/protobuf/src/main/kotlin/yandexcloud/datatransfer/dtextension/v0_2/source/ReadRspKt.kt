//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: api/v0_2/source/source_service.proto

package yandexcloud.datatransfer.dtextension.v0_2.source;

@kotlin.jvm.JvmName("-initializereadRsp")
public inline fun readRsp(block: yandexcloud.datatransfer.dtextension.v0_2.source.ReadRspKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp =
  yandexcloud.datatransfer.dtextension.v0_2.source.ReadRspKt.Dsl._create(yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp.newBuilder()).apply { block() }._build()
public object ReadRspKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp = _builder.build()

    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.Result result = 1;</code>
     */
    public var result: yandexcloud.datatransfer.dtextension.v0_2.Common.Result
      @JvmName("getResult")
      get() = _builder.getResult()
      @JvmName("setResult")
      set(value) {
        _builder.setResult(value)
      }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.Result result = 1;</code>
     */
    public fun clearResult() {
      _builder.clearResult()
    }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.Result result = 1;</code>
     * @return Whether the result field is set.
     */
    public fun hasResult(): kotlin.Boolean {
      return _builder.hasResult()
    }

    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.Cursor cursor = 2;</code>
     */
    public var cursor: yandexcloud.datatransfer.dtextension.v0_2.Common.Cursor
      @JvmName("getCursor")
      get() = _builder.getCursor()
      @JvmName("setCursor")
      set(value) {
        _builder.setCursor(value)
      }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.Cursor cursor = 2;</code>
     */
    public fun clearCursor() {
      _builder.clearCursor()
    }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.Cursor cursor = 2;</code>
     * @return Whether the cursor field is set.
     */
    public fun hasCursor(): kotlin.Boolean {
      return _builder.hasCursor()
    }

    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.source.ReadControlItemRsp control_item_rsp = 3;</code>
     */
    public var controlItemRsp: yandexcloud.datatransfer.dtextension.v0_2.source.Control.ReadControlItemRsp
      @JvmName("getControlItemRsp")
      get() = _builder.getControlItemRsp()
      @JvmName("setControlItemRsp")
      set(value) {
        _builder.setControlItemRsp(value)
      }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.source.ReadControlItemRsp control_item_rsp = 3;</code>
     */
    public fun clearControlItemRsp() {
      _builder.clearControlItemRsp()
    }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.source.ReadControlItemRsp control_item_rsp = 3;</code>
     * @return Whether the controlItemRsp field is set.
     */
    public fun hasControlItemRsp(): kotlin.Boolean {
      return _builder.hasControlItemRsp()
    }
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp.copy(block: yandexcloud.datatransfer.dtextension.v0_2.source.ReadRspKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp =
  yandexcloud.datatransfer.dtextension.v0_2.source.ReadRspKt.Dsl._create(this.toBuilder()).apply { block() }._build()

val yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRspOrBuilder.resultOrNull: yandexcloud.datatransfer.dtextension.v0_2.Common.Result?
  get() = if (hasResult()) getResult() else null

val yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRspOrBuilder.cursorOrNull: yandexcloud.datatransfer.dtextension.v0_2.Common.Cursor?
  get() = if (hasCursor()) getCursor() else null

val yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRspOrBuilder.controlItemRspOrNull: yandexcloud.datatransfer.dtextension.v0_2.source.Control.ReadControlItemRsp?
  get() = if (hasControlItemRsp()) getControlItemRsp() else null

