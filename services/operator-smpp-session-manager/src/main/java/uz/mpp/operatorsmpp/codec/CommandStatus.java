package uz.mpp.operatorsmpp.codec;

/** Подмножество SMPP 3.4 command_status, используемое этим сервисом. */
public final class CommandStatus {
    private CommandStatus() {
    }

    public static final int ESME_ROK = 0x00000000;
    public static final int ESME_RINVMSGLEN = 0x00000001;
    public static final int ESME_RINVCMDLEN = 0x00000002;
    public static final int ESME_RINVCMDID = 0x00000003;
    public static final int ESME_RINVBNDSTS = 0x00000004;
    public static final int ESME_RALYBND = 0x00000005;
    public static final int ESME_RSYSERR = 0x00000008;
    public static final int ESME_RINVPASWD = 0x0000000E;
    public static final int ESME_RINVSYSID = 0x0000000F;
    public static final int ESME_RTHROTTLED = 0x00000058;
    public static final int ESME_RMSGQFUL = 0x00000014;
}
