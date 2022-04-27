package yandexcloud.datatransfer.dtextension.source;

import yandexcloud.datatransfer.dtextension.cdc.Table;
import yandexcloud.datatransfer.dtextension.guaranties.MonoColumnDescription;
import yandexcloud.datatransfer.dtextension.guaranties.TableGuarantee;
import yandexcloud.datatransfer.dtextension.v1.Common;
import yandexcloud.datatransfer.dtextension.v1.source.SourceService;
import yandexcloud.datatransfer.dtextension.v1.source.TableGuaranteeOuterClass;

import java.util.stream.Collectors;

public class EndpointConfigParameters {
    private final String transferId;
    private final String endpointConfiguration;
    private final TableGuarantee[] tableGuarantees;

    private EndpointConfigParameters(String transferId, String endpointConfiguration, TableGuarantee[] tableGuarantees) {
        this.transferId = transferId;
        this.endpointConfiguration = endpointConfiguration;
        this.tableGuarantees = tableGuarantees;
    }

    public String getTransferId() {
        return transferId;
    }

    public String getEndpointConfiguration() {
        return endpointConfiguration;
    }

    public TableGuarantee[] getTableGuarantees() {
        return tableGuarantees;
    }

    public static EndpointConfigParameters convertFromProtobuf(SourceService.ConfigureEndpointRequest request) {
        TableGuarantee[] guarantees = new TableGuarantee[0];
        EndpointConfigParameters params = new EndpointConfigParameters(
                request.getTransferId(),
                request.getEndpointConfig(),
                request.getTableGuaranteesList().stream().map(tableGuarantee -> {
                            Common.Table table = tableGuarantee.getTable();
                            TableGuaranteeOuterClass.TableGuarantee.MonoColumnDescription monoColDescr = tableGuarantee.getMonoColumn();
                            return new TableGuarantee(
                                    new Table(table.getDatabase(), table.getName()), tableGuarantee.getAdditionOnly(),
                                    new MonoColumnDescription(monoColDescr.getColumnName(),
                                            monoColDescr.getStrict(), monoColDescr.getDescending()));
                        }
                ).collect(Collectors.toList()).toArray(guarantees)
        );
        return params;
    }
}
