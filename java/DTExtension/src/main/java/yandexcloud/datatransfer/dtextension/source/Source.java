package yandexcloud.datatransfer.dtextension.source;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import yandexcloud.datatransfer.dtextension.cdc.ChangeItem;
import yandexcloud.datatransfer.dtextension.cdc.Table;
import yandexcloud.datatransfer.dtextension.guaranties.MonoColumnDescription;
import yandexcloud.datatransfer.dtextension.guaranties.SourceTaskFailoverType;
import yandexcloud.datatransfer.dtextension.guaranties.TableGuarantee;
import yandexcloud.datatransfer.dtextension.task.*;
import yandexcloud.datatransfer.dtextension.v1.Common;
import yandexcloud.datatransfer.dtextension.v1.TaskServiceGrpc;
import yandexcloud.datatransfer.dtextension.v1.TaskServiceOuterClass;
import yandexcloud.datatransfer.dtextension.v1.Tasks;
import yandexcloud.datatransfer.dtextension.v1.source.SourceInterfaceGrpc;
import yandexcloud.datatransfer.dtextension.v1.source.SourceService;
import yandexcloud.datatransfer.dtextension.v1.source.TableGuaranteeOuterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class Source {
    private final int port;
    private final Server server;
    private final TasksFactory factory;
    private final GrpcSourceService sourceService;
    private final GrpcTaskService taskService;

    /**
     * In order to create source endpoint you should provide open port
     * for endpoint to connect and endpoint factory
     *
     * @param port    for gRPC service
     * @param factory for user defined code
     */
    public Source(int port, TasksFactory factory) throws IOException {
        this.port = port;
        this.factory = factory;
        this.sourceService = new GrpcSourceService();
        this.taskService = new GrpcTaskService();

        this.transferToRegistrar = new ConcurrentHashMap<>();

        this.server = ServerBuilder
                .forPort(port)
                .addService(this.sourceService)
                .addService(this.taskService)
                .build();
        this.server.start();
    }

    private ConcurrentHashMap<String, TaskRegistrarImpl> transferToRegistrar;

    private TaskRegistrarImpl getRegistrar(String transferId) {
        if (!transferToRegistrar.containsKey(transferId)) {
            transferToRegistrar.put(transferId, new TaskRegistrarImpl());
        }
        return transferToRegistrar.get(transferId);
    }

    private class TaskRegistrarImpl implements TaskRegistrar {

        /**
         * This private class preserves simultaneous activation and deactivation
         * in concurrent execution and restricts emitting of change items while not activated
         */
        private class SourceTaskWrapper implements SourceTask {
            private final SourceTask task;
            private final SourceTaskFailoverType failoverType;
            private volatile boolean activated;

            public SourceTaskWrapper(SourceTask task, SourceTaskFailoverType failoverType) {
                this.task = task;
                this.failoverType = failoverType;
            }

            @Override
            public void emitChangeItems(Consumer<ChangeItem> changeItemsConsumer) {
                this.task.emitChangeItems(changeItemsConsumer);
            }

            @Override
            public synchronized void Activate(String transferId) {
                this.task.Activate(transferId);
            }

            @Override
            public synchronized void Deactivate(String transferId) {
                this.task.Deactivate(transferId);
            }
        }

        private TransferLifecycle overrideTaskActivation = null;

        private int taskCount;
        private ArrayList<SourceTask> tasks;
        private ArrayList<SourceTaskFailoverType> failoverTypes;
        private ArrayList<Integer> snapshotTasks; // indices of snapshot tasks
        private ArrayList<Integer> streamTasks;   // indices of stream tasks

        TaskRegistrarImpl() {
            tasks = new ArrayList<>();
            snapshotTasks = new ArrayList<>();
            streamTasks = new ArrayList<>();
        }

        @Override
        public void RegisterSourceEndpoint(TransferLifecycle sourceEndpoint) {
            this.overrideTaskActivation = sourceEndpoint;
        }

        @Override
        public void RegisterTableSnapshotTask(TableSnapshotTask snapshotTask, SourceTaskFailoverType failoverType) {
            this.tasks.add(snapshotTask);
            this.failoverTypes.add(failoverType);
            this.snapshotTasks.add(this.taskCount);
            this.taskCount++;
        }

        @Override
        public void RegisterStreamTask(StreamTask streamTask, SourceTaskFailoverType failoverType) {
            this.tasks.add(streamTask);
            this.failoverTypes.add(failoverType);
            this.streamTasks.add(this.taskCount);
            this.taskCount++;
        }

        public void activate(String transferId) {
            if (this.overrideTaskActivation != null) {
                this.overrideTaskActivation.Activate(transferId);
                return;
            }
            // default activation way otherwise
            for (Integer streamId : this.streamTasks) {
                StreamTask stream = (StreamTask) this.tasks.get(streamId);
                stream.Activate(transferId);
            }
            for (Integer streamId : this.snapshotTasks) {
                SnapshotTask snapshot = (SnapshotTask) this.tasks.get(streamId);
                snapshot.Activate(transferId);
            }
        }

        public void deactivate(String transferId) {
            if (this.overrideTaskActivation != null) {
                this.overrideTaskActivation.Deactivate(transferId);
                return;
            }
            // default activation way otherwise
            for (Integer streamId : this.streamTasks) {
                StreamTask stream = (StreamTask) this.tasks.get(streamId);
                stream.Deactivate(transferId);
            }
            for (Integer streamId : this.snapshotTasks) {
                SnapshotTask snapshot = (SnapshotTask) this.tasks.get(streamId);
                snapshot.Deactivate(transferId);
            }
        }

        public SourceService.ConfigureEndpointResponse makeResponse() {
            SourceService.ConfigureEndpointResponse.Ok.Builder responseBuilder =
                    SourceService.ConfigureEndpointResponse.Ok.newBuilder();
            responseBuilder.
        }
    }

    private class GrpcSourceService extends SourceInterfaceGrpc.SourceInterfaceImplBase {
        @Override
        public void configureEndpoint(SourceService.ConfigureEndpointRequest request, StreamObserver<SourceService.ConfigureEndpointResponse> responseObserver) {
            super.configureEndpoint(request, responseObserver);
            final String transferId = request.getTransferId();
            final TaskRegistrarImpl registrar = getRegistrar(transferId);

            try {
                EndpointConfigParameters params = EndpointConfigParameters.convertFromProtobuf(request);
                Source.this.factory.RegisterTasks(params, registrar);
            } catch (Exception e) {

            }
            responseObserver.onNext(new SourceService.ConfigureEndpointResponse());
            responseObserver.onCompleted();
        }

        @Override
        public void restoreEndpoint(SourceService.RestoreEndpointRequest request, StreamObserver<SourceService.ConfigureEndpointResponse> responseObserver) {
            super.restoreEndpoint(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void activate(SourceService.ActivateRequest request, StreamObserver<Common.ErrorResponse> responseObserver) {
            super.activate(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void deactivate(SourceService.DeactivateRequest request, StreamObserver<Common.ErrorResponse> responseObserver) {
            super.deactivate(request, responseObserver);
            throw new UnsupportedOperationException("Implement me");
        }

        @Override
        public void healthcheck(SourceService.HealthcheckRequest request, StreamObserver<SourceService.HealthcheckResponse> responseObserver) {
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