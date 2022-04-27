package yandexcloud.datatransfer.dtextension.sink;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import yandexcloud.datatransfer.dtextension.v0_1.Common;
import yandexcloud.datatransfer.dtextension.v0_1.TaskServiceGrpc;
import yandexcloud.datatransfer.dtextension.v0_1.TaskServiceOuterClass;
import yandexcloud.datatransfer.dtextension.v0_1.Tasks;
import yandexcloud.datatransfer.dtextension.v0_1.sink.SinkInterfaceGrpc;
import yandexcloud.datatransfer.dtextension.v0_1.sink.TargetService;

import java.io.IOException;

public final class Sink {
    private final int port;
    private final Server server;
    private final TasksFactory factory;
    private final GrpcSinkService sinkService;
    private final GrpcTaskService taskService;

    /**
     * In order to create source endpoint you should provide open port
     * for endpoint to connect and endpoint factory
     * @param port for gRPC service
     * @param factory for user defined code
     */
    public Sink(int port, TasksFactory factory) throws IOException {
        this.port = port;
        this.factory = factory;
        this.sinkService = new GrpcSinkService();
        this.taskService = new GrpcTaskService();

        this.server = ServerBuilder
                .forPort(port)
                .addService(this.sinkService)
                .addService(this.taskService)
                .build();
        this.server.start();
    }

    private static class GrpcSinkService extends SinkInterfaceGrpc.SinkInterfaceImplBase {
        @Override
        public void configureEndpoint(TargetService.ConfigureEndpointRequest request, StreamObserver<TargetService.ConfigureEndpointResponse> responseObserver) {
            super.configureEndpoint(request, responseObserver);
        }

        @Override
        public void restoreEndpoint(TargetService.RestoreEndpointRequest request, StreamObserver<TargetService.ConfigureEndpointResponse> responseObserver) {
            super.restoreEndpoint(request, responseObserver);
        }

        @Override
        public void activate(TargetService.ActivateRequest request, StreamObserver<Common.ErrorResponse> responseObserver) {
            super.activate(request, responseObserver);
        }

        @Override
        public void deactivate(TargetService.DeactivateRequest request, StreamObserver<Common.ErrorResponse> responseObserver) {
            super.deactivate(request, responseObserver);
        }

        @Override
        public void healthcheck(TargetService.HealthcheckRequest request, StreamObserver<TargetService.HealthcheckResponse> responseObserver) {
            super.healthcheck(request, responseObserver);
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