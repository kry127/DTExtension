//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: api/v0_2/source/control.proto

package yandexcloud.datatransfer.dtextension.v0_2.source;

@kotlin.jvm.JvmName("-initializebeginSnapshotReq")
public inline fun beginSnapshotReq(block: yandexcloud.datatransfer.dtextension.v0_2.source.BeginSnapshotReqKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq =
  yandexcloud.datatransfer.dtextension.v0_2.source.BeginSnapshotReqKt.Dsl._create(yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq.newBuilder()).apply { block() }._build()
public object BeginSnapshotReqKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq = _builder.build()
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq.copy(block: yandexcloud.datatransfer.dtextension.v0_2.source.BeginSnapshotReqKt.Dsl.() -> kotlin.Unit): yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotReq =
  yandexcloud.datatransfer.dtextension.v0_2.source.BeginSnapshotReqKt.Dsl._create(this.toBuilder()).apply { block() }._build()

