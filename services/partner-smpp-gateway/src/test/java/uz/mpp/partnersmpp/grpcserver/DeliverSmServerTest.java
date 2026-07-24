package uz.mpp.partnersmpp.grpcserver;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import uz.mpp.partnersmpp.codec.CommandId;
import uz.mpp.partnersmpp.codec.Pdu;
import uz.mpp.partnersmpp.codec.PduCodec;
import uz.mpp.partnersmpp.server.ChannelRegistry;
import uz.mpp.platformcontracts.grpc.v1.DeliverSmRequest;
import uz.mpp.platformcontracts.grpc.v1.DeliverSmResponse;
import uz.mpp.platformcontracts.grpc.v1.DeliverSmStatus;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DeliverSmServerTest {

    private static class CapturingObserver implements StreamObserver<DeliverSmResponse> {
        final CompletableFuture<DeliverSmResponse> future = new CompletableFuture<>();

        @Override
        public void onNext(DeliverSmResponse value) {
            future.complete(value);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
        }
    }

    @Test
    void deliversWhenSessionActiveAndEpochMatches() throws Exception {
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();
        long epoch = registry.register("acme", "click_uz_main", channel);

        DeliverSmServer server = new DeliverSmServer(registry);
        CapturingObserver observer = new CapturingObserver();

        server.deliverSm(DeliverSmRequest.newBuilder()
            .setMessageId("msg-1").setPartnerId("acme").setSystemId("click_uz_main")
            .setSessionEpoch(epoch).setStatusText("DELIVRD")
            .setPayload(ByteString.copyFromUtf8("id:1 stat:DELIVRD"))
            .build(), observer);

        DeliverSmResponse resp = observer.future.get();
        assertEquals(DeliverSmStatus.DELIVER_SM_STATUS_DELIVERED, resp.getStatus());

        ByteBuf written = channel.readOutbound();
        assertNotNull(written, "ожидали, что deliver_sm PDU реально записан в канал");
        Pdu pdu = PduCodec.decode(written);
        assertEquals(CommandId.DELIVER_SM, pdu.header().commandId());
    }

    @Test
    void rejectsWhenNoActiveSession() throws Exception {
        ChannelRegistry registry = new ChannelRegistry();
        DeliverSmServer server = new DeliverSmServer(registry);
        CapturingObserver observer = new CapturingObserver();

        server.deliverSm(DeliverSmRequest.newBuilder()
            .setMessageId("msg-1").setPartnerId("acme").setSystemId("no_such_system")
            .setSessionEpoch(1).build(), observer);

        assertEquals(DeliverSmStatus.DELIVER_SM_STATUS_NO_ACTIVE_SESSION, observer.future.get().getStatus());
    }

    @Test
    void rejectsOnStaleEpoch() throws Exception {
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();
        long epoch = registry.register("acme", "click_uz_main", channel);

        DeliverSmServer server = new DeliverSmServer(registry);
        CapturingObserver observer = new CapturingObserver();

        server.deliverSm(DeliverSmRequest.newBuilder()
            .setMessageId("msg-1").setPartnerId("acme").setSystemId("click_uz_main")
            .setSessionEpoch(epoch - 1) // устаревший epoch
            .build(), observer);

        assertEquals(DeliverSmStatus.DELIVER_SM_STATUS_STALE_EPOCH, observer.future.get().getStatus());
    }
}