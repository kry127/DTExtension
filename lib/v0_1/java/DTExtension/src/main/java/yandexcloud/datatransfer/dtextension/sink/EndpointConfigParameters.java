package yandexcloud.datatransfer.dtextension.sink;

import yandexcloud.datatransfer.dtextension.guaranties.TableGuarantee;

public class EndpointConfigParameters {
    private final String transferId;
    private final String endpointConfiguration;

    public EndpointConfigParameters(String transferId, String endpointConfiguration) {
        this.transferId = transferId;
        this.endpointConfiguration = endpointConfiguration;
    }

    public String getTransferId() {
        return transferId;
    }

    public String getEndpointConfiguration() {
        return endpointConfiguration;
    }
}
