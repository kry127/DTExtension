package yandexcloud.datatransfer.dtextension.v0_2.sink

import io.grpc.CallOptions
import io.grpc.CallOptions.DEFAULT
import io.grpc.Channel
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.ServerServiceDefinition
import io.grpc.ServerServiceDefinition.builder
import io.grpc.ServiceDescriptor
import io.grpc.Status
import io.grpc.Status.UNIMPLEMENTED
import io.grpc.StatusException
import io.grpc.kotlin.AbstractCoroutineServerImpl
import io.grpc.kotlin.AbstractCoroutineStub
import io.grpc.kotlin.ClientCalls
import io.grpc.kotlin.ClientCalls.bidiStreamingRpc
import io.grpc.kotlin.ClientCalls.unaryRpc
import io.grpc.kotlin.ServerCalls
import io.grpc.kotlin.ServerCalls.bidiStreamingServerMethodDefinition
import io.grpc.kotlin.ServerCalls.unaryServerMethodDefinition
import io.grpc.kotlin.StubFor
import kotlin.String
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.jvm.JvmOverloads
import kotlin.jvm.JvmStatic
import kotlinx.coroutines.flow.Flow
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.sink.SinkServiceGrpc.getServiceDescriptor

/**
 * Holder for Kotlin coroutine-based client and server APIs for
 * yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.
 */
object SinkServiceGrpcKt {
  const val SERVICE_NAME: String = SinkServiceGrpc.SERVICE_NAME

  @JvmStatic
  val serviceDescriptor: ServiceDescriptor
    get() = SinkServiceGrpc.getServiceDescriptor()

  val specMethod: MethodDescriptor<Common.SpecReq, Common.SpecRsp>
    @JvmStatic
    get() = SinkServiceGrpc.getSpecMethod()

  val checkMethod: MethodDescriptor<Common.CheckReq, Common.CheckRsp>
    @JvmStatic
    get() = SinkServiceGrpc.getCheckMethod()

  val writeMethod: MethodDescriptor<SinkServiceOuterClass.WriteReq, SinkServiceOuterClass.WriteRsp>
    @JvmStatic
    get() = SinkServiceGrpc.getWriteMethod()

  /**
   * A stub for issuing RPCs to a(n) yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService
   * service as suspending coroutines.
   */
  @StubFor(SinkServiceGrpc::class)
  class SinkServiceCoroutineStub @JvmOverloads constructor(
    channel: Channel,
    callOptions: CallOptions = DEFAULT
  ) : AbstractCoroutineStub<SinkServiceCoroutineStub>(channel, callOptions) {
    override fun build(channel: Channel, callOptions: CallOptions): SinkServiceCoroutineStub =
        SinkServiceCoroutineStub(channel, callOptions)

    /**
     * Executes this RPC and returns the response message, suspending until the RPC completes
     * with [`Status.OK`][Status].  If the RPC completes with another status, a corresponding
     * [StatusException] is thrown.  If this coroutine is cancelled, the RPC is also cancelled
     * with the corresponding exception as a cause.
     *
     * @param request The request message to send to the server.
     *
     * @param headers Metadata to attach to the request.  Most users will not need this.
     *
     * @return The single response from the server.
     */
    suspend fun spec(request: Common.SpecReq, headers: Metadata = Metadata()): Common.SpecRsp =
        unaryRpc(
      channel,
      SinkServiceGrpc.getSpecMethod(),
      request,
      callOptions,
      headers
    )
    /**
     * Executes this RPC and returns the response message, suspending until the RPC completes
     * with [`Status.OK`][Status].  If the RPC completes with another status, a corresponding
     * [StatusException] is thrown.  If this coroutine is cancelled, the RPC is also cancelled
     * with the corresponding exception as a cause.
     *
     * @param request The request message to send to the server.
     *
     * @param headers Metadata to attach to the request.  Most users will not need this.
     *
     * @return The single response from the server.
     */
    suspend fun check(request: Common.CheckReq, headers: Metadata = Metadata()): Common.CheckRsp =
        unaryRpc(
      channel,
      SinkServiceGrpc.getCheckMethod(),
      request,
      callOptions,
      headers
    )
    /**
     * Returns a [Flow] that, when collected, executes this RPC and emits responses from the
     * server as they arrive.  That flow finishes normally if the server closes its response with
     * [`Status.OK`][Status], and fails by throwing a [StatusException] otherwise.  If
     * collecting the flow downstream fails exceptionally (including via cancellation), the RPC
     * is cancelled with that exception as a cause.
     *
     * The [Flow] of requests is collected once each time the [Flow] of responses is
     * collected. If collection of the [Flow] of responses completes normally or
     * exceptionally before collection of `requests` completes, the collection of
     * `requests` is cancelled.  If the collection of `requests` completes
     * exceptionally for any other reason, then the collection of the [Flow] of responses
     * completes exceptionally for the same reason and the RPC is cancelled with that reason.
     *
     * @param requests A [Flow] of request messages.
     *
     * @param headers Metadata to attach to the request.  Most users will not need this.
     *
     * @return A flow that, when collected, emits the responses from the server.
     */
    fun write(requests: Flow<SinkServiceOuterClass.WriteReq>, headers: Metadata = Metadata()):
        Flow<SinkServiceOuterClass.WriteRsp> = bidiStreamingRpc(
      channel,
      SinkServiceGrpc.getWriteMethod(),
      requests,
      callOptions,
      headers
    )}

  /**
   * Skeletal implementation of the yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService
   * service based on Kotlin coroutines.
   */
  abstract class SinkServiceCoroutineImplBase(
    coroutineContext: CoroutineContext = EmptyCoroutineContext
  ) : AbstractCoroutineServerImpl(coroutineContext) {
    /**
     * Returns the response to an RPC for
     * yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.Spec.
     *
     * If this method fails with a [StatusException], the RPC will fail with the corresponding
     * [Status].  If this method fails with a [java.util.concurrent.CancellationException], the RPC
     * will fail
     * with status `Status.CANCELLED`.  If this method fails for any other reason, the RPC will
     * fail with `Status.UNKNOWN` with the exception as a cause.
     *
     * @param request The request from the client.
     */
    open suspend fun spec(request: Common.SpecReq): Common.SpecRsp = throw
        StatusException(UNIMPLEMENTED.withDescription("Method yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.Spec is unimplemented"))

    /**
     * Returns the response to an RPC for
     * yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.Check.
     *
     * If this method fails with a [StatusException], the RPC will fail with the corresponding
     * [Status].  If this method fails with a [java.util.concurrent.CancellationException], the RPC
     * will fail
     * with status `Status.CANCELLED`.  If this method fails for any other reason, the RPC will
     * fail with `Status.UNKNOWN` with the exception as a cause.
     *
     * @param request The request from the client.
     */
    open suspend fun check(request: Common.CheckReq): Common.CheckRsp = throw
        StatusException(UNIMPLEMENTED.withDescription("Method yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.Check is unimplemented"))

    /**
     * Returns a [Flow] of responses to an RPC for
     * yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.Write.
     *
     * If creating or collecting the returned flow fails with a [StatusException], the RPC
     * will fail with the corresponding [Status].  If it fails with a
     * [java.util.concurrent.CancellationException], the RPC will fail with status
     * `Status.CANCELLED`.  If creating
     * or collecting the returned flow fails for any other reason, the RPC will fail with
     * `Status.UNKNOWN` with the exception as a cause.
     *
     * @param requests A [Flow] of requests from the client.  This flow can be
     *        collected only once and throws [java.lang.IllegalStateException] on attempts to
     * collect
     *        it more than once.
     */
    open fun write(requests: Flow<SinkServiceOuterClass.WriteReq>):
        Flow<SinkServiceOuterClass.WriteRsp> = throw
        StatusException(UNIMPLEMENTED.withDescription("Method yandexcloud.datatransfer.dtextension.v0_2.sink.SinkService.Write is unimplemented"))

    final override fun bindService(): ServerServiceDefinition = builder(getServiceDescriptor())
      .addMethod(unaryServerMethodDefinition(
      context = this.context,
      descriptor = SinkServiceGrpc.getSpecMethod(),
      implementation = ::spec
    ))
      .addMethod(unaryServerMethodDefinition(
      context = this.context,
      descriptor = SinkServiceGrpc.getCheckMethod(),
      implementation = ::check
    ))
      .addMethod(bidiStreamingServerMethodDefinition(
      context = this.context,
      descriptor = SinkServiceGrpc.getWriteMethod(),
      implementation = ::write
    )).build()
  }
}
