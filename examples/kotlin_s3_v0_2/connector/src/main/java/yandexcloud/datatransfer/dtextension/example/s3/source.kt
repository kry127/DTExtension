package yandexcloud.datatransfer.dtextension.example.s3

import com.beust.klaxon.Klaxon
import kotlinx.coroutines.flow.Flow
import net.pwall.json.schema.JSONSchema
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceGrpcKt
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass

object S3Boilerplate {
    val resultOk = Common.Result.newBuilder().setOk(true).build()
    fun resultError(error: String) : Common.Result {
        return Common.Result.newBuilder().setError(error).build()
    }
}

data class S3Parameters(
    val pg_connection_string: String,
)

class S3 : SourceServiceGrpcKt.SourceServiceCoroutineImplBase() {
    private val specificationPath = "/spec.json";

    override suspend fun spec(request: Common.SpecReq): Common.SpecRsp {
        val specPath = javaClass.getResource(specificationPath)
            ?: return Common.SpecRsp.newBuilder()
                .setResult(S3Boilerplate.resultError("Spec file not found"))
                .build()
        return Common.SpecRsp.newBuilder()
            .setResult(S3Boilerplate.resultOk)
            .setJsonSpec(specPath.readText())
            .build()
    }

    override suspend fun check(request: Common.CheckReq): Common.CheckRsp {
        val specPath = javaClass.getResource(specificationPath)
            ?: return Common.CheckRsp.newBuilder()
                .setResult(S3Boilerplate.resultError("Spec file not found"))
                .build()
        val specSchema = JSONSchema.parseFile(specPath.path)
        val output = specSchema.validateBasic(request.jsonSettings)
        if (output.errors != null) {
            val err = output.errors?.map { it.instanceLocation + "(" + it.keywordLocation+ "):" + it.error }
                ?.joinToString(separator = "\n") ?: ""
            return Common.CheckRsp.newBuilder()
                .setResult(S3Boilerplate.resultError(err))
                .build()
        }
        // TODO add extra validation if needed
        return Common.CheckRsp.newBuilder()
            .setResult(S3Boilerplate.resultOk)
            .build()
    }

    override suspend fun discover(request: SourceServiceOuterClass.DiscoverReq): SourceServiceOuterClass.DiscoverRsp {
        val specPath = javaClass.getResource(specificationPath)
            ?: return SourceServiceOuterClass.DiscoverRsp.newBuilder()
                .setResult(S3Boilerplate.resultError("Spec file not found"))
                .build()
        val specSchema = JSONSchema.parseFile(specPath.path)
        val output = specSchema.validateBasic(request.jsonSettings)
        if (output.errors != null) {
            val err = output.errors?.map { it.instanceLocation + "(" + it.keywordLocation+ "):" + it.error }
                ?.joinToString(separator = "\n") ?: ""
            return SourceServiceOuterClass.DiscoverRsp.newBuilder()
                .setResult(S3Boilerplate.resultError(err))
                .build()
        }
        // TODO add extra validation if needed
        val parameters = Klaxon().parse<S3Parameters>(request.jsonSettings)
    }

    override fun read(requests: Flow<SourceServiceOuterClass.ReadReq>): Flow<SourceServiceOuterClass.ReadRsp> {
        return super.read(requests)
    }

    override fun stream(requests: Flow<SourceServiceOuterClass.StreamReq>): Flow<SourceServiceOuterClass.StreamRsp> {
        return super.stream(requests)
    }
}