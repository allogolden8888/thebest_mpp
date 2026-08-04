package uz.mpp.operatorsmpp.codec;

/** Тело bind_transceiver (SMPP 3.4 §4.1.1) — TLV-параметры (sc_interface_version) не поддержаны в этом срезе. */
public record BindTransceiver(
    String systemId,
    String password,
    String systemType,
    byte interfaceVersion,
    byte addrTon,
    byte addrNpi,
    String addressRange
) {
}