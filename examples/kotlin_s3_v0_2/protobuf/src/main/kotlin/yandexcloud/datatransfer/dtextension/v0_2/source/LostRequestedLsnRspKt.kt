//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: api/v0_2/source/control.proto

package yandexcloud.datatransfer.dtextension.v0_2.source;

@kotlin.jvm.JvmName("-initializelostRequestedLsnRsp")
public inline fun lostRequestedLsnRsp(block: yandexcloud.datatransfer.dtextension.v0_2.source.LostRequestedLsnRspKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp =
  yandexcloud.datatransfer.dtextension.v0_2.source.LostRequestedLsnRspKt.Dsl._create(yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp.newBuilder()).apply { block() }._build()
public object LostRequestedLsnRspKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp = _builder.build()
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp.copy(block: yandexcloud.datatransfer.dtextension.v0_2.source.LostRequestedLsnRspKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.Control.LostRequestedLsnRsp =
  yandexcloud.datatransfer.dtextension.v0_2.source.LostRequestedLsnRspKt.Dsl._create(this.toBuilder()).apply { block() }._build()

