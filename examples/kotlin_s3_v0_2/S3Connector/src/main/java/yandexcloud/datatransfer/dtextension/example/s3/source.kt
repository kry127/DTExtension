package yandexcloud.datatransfer.dtextension.example.s3

import kotlinx.coroutines.flow.Flow
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceGrpcKt
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass

class S3 : SourceServiceGrpcKt.SourceServiceCoroutineImplBase() {
    override suspend fun spec(request: Common.SpecReq): Common.SpecRsp {
        return super.spec(request)
    }

    override suspend fun check(request: Common.CheckReq): Common.CheckRsp {
        return super.check(request)
    }

    override suspend fun discover(request: SourceServiceOuterClass.DiscoverReq): SourceServiceOuterClass.DiscoverRsp {
        return super.discover(request)
    }

    override fun read(requests: Flow<SourceServiceOuterClass.ReadReq>): Flow<SourceServiceOuterClass.ReadRsp> {
        return super.read(requests)
    }

    override fun stream(requests: Flow<SourceServiceOuterClass.StreamReq>): Flow<SourceServiceOuterClass.StreamRsp> {
        return super.stream(requests)
    }
}