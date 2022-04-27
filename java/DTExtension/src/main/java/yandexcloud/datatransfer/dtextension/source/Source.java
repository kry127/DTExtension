package yandexcloud.datatransfer.dtextension.source;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import yandexcloud.datatransfer.dtextension.v1.Common;
import yandexcloud.datatransfer.dtextension.v1.TaskServiceGrpc;
import yandexcloud.datatransfer.dtextension.v1.TaskServiceOuterClass;
import yandexcloud.datatransfer.dtextension.v1.Tasks;
import yandexcloud.datatransfer.dtextension.v1.source.SourceInterfaceGrpc;

import java.io.IOException;

public final class Source {
    private final int port;
    private final Server server;
    private final TasksFactory factory;
    private final GrpcSourceService sourceService;
    private final GrpcTaskService taskService;

    /**
     * In order to create source endpoint you should provide open port
     * for endpoint to connect and endpoint factory
     * @param port for gRPC service
     * @param factory for user defined code
     */
    public Source(int port, TasksFactory factory) throws IOException {
        this.port = port;
        this.factory = factory;
        this.sourceService = new GrpcSourceService();
        this.taskService = new GrpcTaskService();

        this.server = ServerBuilder
                .forPort(port)
                .addService(this.sourceService)
                .addService(this.taskService)
                .build();
        this.server.start();
    }

    private static class GrpcSourceService extends SourceInterfaceGrpc.SourceInterfaceImplBase {
        @Override
        public void configureEndpoint(yandexcloud.datatransfer.dtextension.v1.source.SourceService.ConfigureEndpointRequest request, StreamObserver<yandexcloud.datatransfer.dtextension.v1.source.SourceService.ConfigureEndpointResponse> responseObserver) {
            super.configureEndpoint(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void restoreEndpoint(yandexcloud.datatransfer.dtextension.v1.source.SourceService.RestoreEndpointRequest request, StreamObserver<yandexcloud.datatransfer.dtextension.v1.source.SourceService.ConfigureEndpointResponse> responseObserver) {
            super.restoreEndpoint(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void activate(yandexcloud.datatransfer.dtextension.v1.source.SourceService.ActivateRequest request, StreamObserver<Common.ErrorResponse> responseObserver) {
            super.activate(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void deactivate(yandexcloud.datatransfer.dtextension.v1.source.SourceService.DeactivateRequest request, StreamObserver<Common.ErrorResponse> responseObserver) {
            super.deactivate(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void healthcheck(yandexcloud.datatransfer.dtextension.v1.source.SourceService.HealthcheckRequest request, StreamObserver<yandexcloud.datatransfer.dtextension.v1.source.SourceService.HealthcheckResponse> responseObserver) {
            super.healthcheck(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }
    }

    private static class GrpcTaskService extends TaskServiceGrpc.TaskServiceImplBase {
        @Override
        public void getTask(TaskServiceOuterClass.TaskId request, StreamObserver<Tasks.Task> responseObserver) {
            super.getTask(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void persistTask(TaskServiceOuterClass.TaskId request, StreamObserver<Tasks.PersistedTask> responseObserver) {
            super.persistTask(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void getParentTasks(TaskServiceOuterClass.TaskId request, StreamObserver<TaskServiceOuterClass.TaskIds> responseObserver) {
            super.getParentTasks(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void getChildTasks(TaskServiceOuterClass.TaskId request, StreamObserver<TaskServiceOuterClass.TaskIds> responseObserver) {
            super.getChildTasks(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void splitTask(TaskServiceOuterClass.TaskId request, StreamObserver<TaskServiceOuterClass.SplitTaskResponse> responseObserver) {
            super.splitTask(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public StreamObserver<TaskServiceOuterClass.GetChangeItemsRequest> getChangeItems(StreamObserver<TaskServiceOuterClass.GetChangeItemsResponse> responseObserver) {
            throw new UnsupportedOperationException("Implement me");
            // return super.getChangeItems(responseObserver);
        }

        @Override
        public StreamObserver<TaskServiceOuterClass.PutChangeItemsRequest> putChangeItems(StreamObserver<TaskServiceOuterClass.PutChangeItemsResponse> responseObserver) {
            throw new UnsupportedOperationException("Implement me");
            // return super.putChangeItems(responseObserver);
        }
    }

}