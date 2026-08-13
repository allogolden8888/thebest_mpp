package uz.mpp.operatorsmpp.core;

/**
 * Внутренняя раскладка приоритета туннеля (dynamic-seeking-russell.md
 * "Priority-tier scheduler") — не то же самое, что wire-значение
 * {@code SubmitRequest.priority_flag} (SMPP priority_flag, 0-3, 3=наивысший,
 * прокидывается на провод без изменений). Раскладка на 3 tier'а — отдельная
 * scheduler-policy, специально держится отдельно от wire-значения, чтобы
 * 70/20/10-сплит можно было менять без proto-миграции.
 */
public enum PriorityTier {
    HIGH,
    MEDIUM,
    LOW;

    /**
     * {@code 3 -> HIGH}, {@code 1|2 -> MEDIUM}, {@code 0} или
     * out-of-range (partner-rest-receiver's {@code DEFAULT_PRIORITY_FLAG}
     * не отправлен/битый клиент) {@code -> LOW}.
     */
    public static PriorityTier forPriorityFlag(int priorityFlag) {
        if (priorityFlag == 3) {
            return HIGH;
        }
        if (priorityFlag == 1 || priorityFlag == 2) {
            return MEDIUM;
        }
        return LOW;
    }
}
