package uz.mpp.deliveryreconciliation.grpcclient;

import org.junit.jupiter.api.Test;
import uz.mpp.deliveryreconciliation.core.Evidence;
import uz.mpp.platformcontracts.grpc.v1.QuerySmResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QuerySmClientTest {

    @Test
    void foundMapsToConfirmedDelivered() {
        QuerySmResponse resp = QuerySmResponse.newBuilder().setFound(true).setRawStatus("DELIVRD").build();
        assertEquals(Evidence.QuerySmOutcome.CONFIRMED_DELIVERED, QuerySmClient.mapResponse(resp));
    }

    @Test
    void notFoundMapsToConfirmedNotFound() {
        QuerySmResponse resp = QuerySmResponse.newBuilder().setFound(false).setRawStatus("NOT_FOUND").build();
        assertEquals(Evidence.QuerySmOutcome.CONFIRMED_NOT_FOUND, QuerySmClient.mapResponse(resp));
    }

    @Test
    void deferredMapsToInconclusiveNotConfirmedNotFound() {
        QuerySmResponse resp = QuerySmResponse.newBuilder().setFound(false).setRawStatus("DEFERRED_SUBMIT_PRIORITY").build();
        assertEquals(Evidence.QuerySmOutcome.INCONCLUSIVE, QuerySmClient.mapResponse(resp));
    }
}