package uz.mpp.partnersmpp.grpcserver;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import uz.mpp.partnersmpp.codec.*;
import uz.mpp.partnersmpp.server.ChannelRegistry;
import uz.mpp.partnersmpp.session.SequenceNumberGenerator;
import uz.mpp.platformcontracts.grpc.v1.DeliverSmRequest;
import uz.mpp.platformcontracts.grpc.v1.DeliverSmResponse;
import uz.mpp.platformcontracts.grpc.v1.DeliverSmStatus;
import uz.mpp.platformcontracts.grpc.v1.PartnerDeliverSmServiceGrpc;

/**
 * handle_deliver_sm_command + send_deliver_sm (service_internal_methods.md
 * §1.2) — instance-addressed gRPC от Partner Notification Service
 * (platform-contracts/grpc/partner_gateway.proto). Проверяет session_epoch
 * перед отправкой (HLD §11.1).
 *
 * <p><b>Известное упрощение</b>: push сейчас fire-and-forget — DELIVERED
 * возвращается сразу после успешной записи в канал, не после реального
 * deliver_sm_resp от партнёра (корреляция по sequence_number с ожидающим
 * gRPC-вызовом не реализована в этом срезе, см. README "Что НЕ реализовано").
 *
 * <p><b>Кодревью (CODE_REVIEW.md, "partner-smpp-gateway" #1, CRITICAL) —
 * расследовано, см. README "Проверено кодревью".</b> Нет interceptor'а/TLS
 * здесь в коде — сервер полагается на mTLS + identity-аутентификацию Istio
 * mesh (namespace-wide {@code PeerAuthentication STRICT},
 * {@code infra/istio/peer-authentication-strict.yaml}) в сочетании с
 * {@code NetworkPolicy}, сгенерированной из явного
 * {@code CALL_GRAPH}-ребра {@code partner-notification-service ->
 * partner-smpp-gateway} ({@code k8s/network_policies.py}) — на сетевом
 * уровне порт 9000 доступен ТОЛЬКО подам Partner Notification Service, и
 * каждое такое соединение обязано пройти mTLS с проверкой identity пода на
 * уровне mesh. "Любой сосед по namespace" из находки кодревью не
 * подтверждается: без соответствующего NetworkPolicy-правила подключиться
 * к этому порту не может ни один под, кроме явно разрешённого.
 */
public final class DeliverSmServer extends PartnerDeliverSmServiceGrpc.PartnerDeliverSmServiceImplBase {

    private final ChannelRegistry channelRegistry;
    private final SequenceNumberGenerator sequenceNumberGenerator = new SequenceNumberGenerator();

    public DeliverSmServer(ChannelRegistry channelRegistry) {
        this.channelRegistry = channelRegistry;
    }

    @Override
    public void deliverSm(DeliverSmRequest request, StreamObserver<DeliverSmResponse> responseObserver) {
        ChannelRegistry.ActiveSession session = channelRegistry.lookup(request.getPartnerId(), request.getSystemId());

        if (session == null) {
            reply(responseObserver, DeliverSmStatus.DELIVER_SM_STATUS_NO_ACTIVE_SESSION);
            return;
        }
        if (session.sessionEpoch() != request.getSessionEpoch()) {
            reply(responseObserver, DeliverSmStatus.DELIVER_SM_STATUS_STALE_EPOCH);
            return;
        }

        ShortMessagePdu body = new ShortMessagePdu(
            "", (byte) 0, (byte) 1, "", (byte) 0, (byte) 1, "",
            (byte) 0x04 /* SMSC delivery receipt */, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0,
            request.getPayload().toByteArray()
        );
        Pdu pdu = Pdu.withBody(CommandId.DELIVER_SM, CommandStatus.ESME_ROK, sequenceNumberGenerator.next(), body);

        Channel channel = session.channel();
        if (!channel.isActive()) {
            reply(responseObserver, DeliverSmStatus.DELIVER_SM_STATUS_NO_ACTIVE_SESSION);
            return;
        }

        ByteBuf out = Unpooled.buffer();
        PduCodec.encode(pdu, out);
        channel.writeAndFlush(out);

        reply(responseObserver, DeliverSmStatus.DELIVER_SM_STATUS_DELIVERED);
    }

    private void reply(StreamObserver<DeliverSmResponse> observer, DeliverSmStatus status) {
        observer.onNext(DeliverSmResponse.newBuilder().setStatus(status).build());
        observer.onCompleted();
    }
}