package uz.mpp.partnersmpp.core;

import uz.mpp.partnersmpp.codec.ShortMessagePdu;

import java.util.ArrayList;
import java.util.List;

/**
 * validate_submit_pdu (service_internal_methods.md §1.2): SubmitSmPdu ->
 * ValidatedSubmit | PduError.
 */
public final class SubmitValidator {

    private SubmitValidator() {
    }

    public record ValidationResult(boolean valid, List<String> errors) {
        public static ValidationResult ok() {
            return new ValidationResult(true, List.of());
        }
    }

    public static ValidationResult validate(ShortMessagePdu pdu) {
        List<String> errors = new ArrayList<>();

        if (pdu.destinationAddr() == null || pdu.destinationAddr().isEmpty()) {
            errors.add("destination_addr пуст");
        }
        if (pdu.shortMessage() != null && pdu.shortMessage().length > 254) {
            errors.add("short_message превышает 254 октета (sm_length — один байт по SMPP 3.4)");
        }
        if (pdu.destinationAddr() != null && pdu.destinationAddr().length() > 21) {
            errors.add("destination_addr длиннее 21 символа (SMPP 3.4 §4.4.1 максимум)");
        }

        return errors.isEmpty() ? ValidationResult.ok() : new ValidationResult(false, errors);
    }
}