package uz.mpp.deliveryreconciliation.core;

import uz.mpp.platformcontracts.common.v1.Protocol;

/**
 * check_query_sm_policy (service_internal_methods.md §1.9): operator_id,
 * local config snapshot -&gt; Enabled | Disabled. Только protocol=SMPP — HTTP
 * не имеет query_sm-эквивалента (services_specifictaion.md §2.9).
 * "Выключен по умолчанию" (services_specifictaion.md §2.9).
 */
public final class QuerySmPolicy {

    private QuerySmPolicy() {
    }

    public enum Decision { ENABLED, DISABLED }

    public static Decision check(Protocol protocol, boolean operatorConfigEnablesQuerySm) {
        if (protocol != Protocol.PROTOCOL_SMPP) {
            return Decision.DISABLED;
        }
        return operatorConfigEnablesQuerySm ? Decision.ENABLED : Decision.DISABLED;
    }
}
