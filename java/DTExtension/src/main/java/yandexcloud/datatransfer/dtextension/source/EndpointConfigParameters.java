package yandexcloud.datatransfer.dtextension.source;

import yandexcloud.datatransfer.dtextension.guaranties.TableGuarantee;

public class EndpointConfigParameters {
    private final String transferId;
    private final String endpointConfiguration;
    private final TableGuarantee[] tableGuarantees;

    public EndpointConfigParameters(String transferId, String endpointConfiguration, TableGuarantee[] tableGuarantees) {
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
}
