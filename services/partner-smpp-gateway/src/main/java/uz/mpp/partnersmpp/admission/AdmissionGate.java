package uz.mpp.partnersmpp.admission;

/** Hot-path admission decision for an authenticated SMPP partner. */
@FunctionalInterface
public interface AdmissionGate {
    boolean admit(String partnerId);
}
