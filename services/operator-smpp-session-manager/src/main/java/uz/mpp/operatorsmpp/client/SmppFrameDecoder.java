package uz.mpp.operatorsmpp.client;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * SMPP framing: command_length (первые 4 байта, big-endian) — размер
 * всего PDU, включая сами эти 4 байта. initialBytesToStrip=0, потому что
 * {@link uz.mpp.operatorsmpp.codec.PduCodec#decode} читает command_length
 * заново из начала фрейма.
 */
public final class SmppFrameDecoder extends LengthFieldBasedFrameDecoder {

    private static final int MAX_FRAME_LENGTH = 64 * 1024;

    public SmppFrameDecoder() {
        super(MAX_FRAME_LENGTH, 0, 4, -4, 0);
    }
}