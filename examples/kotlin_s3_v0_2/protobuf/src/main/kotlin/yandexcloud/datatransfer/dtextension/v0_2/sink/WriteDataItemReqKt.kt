//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: api/v0_2/sink/control.proto

package yandexcloud.datatransfer.dtextension.v0_2.sink;

@kotlin.jvm.JvmName("-initializewriteDataItemReq")
public inline fun writeDataItemReq(block: yandexcloud.datatransfer.dtextension.v0_2.sink.WriteDataItemReqKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq =
  yandexcloud.datatransfer.dtextension.v0_2.sink.WriteDataItemReqKt.Dsl._create(yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq.newBuilder()).apply { block() }._build()
public object WriteDataItemReqKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq = _builder.build()

    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ChangeItem change_item = 2;</code>
     */
    public var changeItem: yandexcloud.datatransfer.dtextension.v0_2.Data.ChangeItem
      @JvmName("getChangeItem")
      get() = _builder.getChangeItem()
      @JvmName("setChangeItem")
      set(value) {
        _builder.setChangeItem(value)
      }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ChangeItem change_item = 2;</code>
     */
    public fun clearChangeItem() {
      _builder.clearChangeItem()
    }
    /**
     * <code>.yandexcloud.datatransfer.dtextension.v0_2.ChangeItem change_item = 2;</code>
     * @return Whether the changeItem field is set.
     */
    public fun hasChangeItem(): kotlin.Boolean {
      return _builder.hasChangeItem()
    }
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq.copy(block: yandexcloud.datatransfer.dtextension.v0_2.sink.WriteDataItemReqKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReq =
  yandexcloud.datatransfer.dtextension.v0_2.sink.WriteDataItemReqKt.Dsl._create(this.toBuilder()).apply { block() }._build()

val yandexcloud.datatransfer.dtextension.v0_2.sink.Control.WriteDataItemReqOrBuilder.changeItemOrNull: yandexcloud.datatransfer.dtextension.v0_2.Data.ChangeItem?
  get() = if (hasChangeItem()) getChangeItem() else null

