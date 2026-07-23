package uz.mpp.scheduler.standard.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import uz.mpp.scheduler.standard.core.ControlSnapshot;

/**
 * Полная топология Standard Lane (services_specifictaion.md §3.1):
 *
 * <pre>
 * scheduler.standard.commands --> [hold-processor] --(dynamic sink)--> stage.*
 *                                       |
 *                                  standard-holds-store (persistent, changelog scheduler.standard.state.changelog)
 *
 * execution.control (global) --> [control-update-processor] --> control-snapshot-store (global)
 *                                       |
 *                                  ControlSnapshot (in-memory, read by hold-processor)
 * </pre>
 */
public final class StandardLaneTopology {

    private StandardLaneTopology() {
    }

    public static Topology build() {
        return build(new ControlSnapshot());
    }

    /** Overload для тестов — позволяет инспектировать снапшот извне. */
    public static Topology build(ControlSnapshot snapshot) {
        Topology topology = new Topology();

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, uz.mpp.scheduler.standard.core.HeldItem>> holdsStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(HoldCommandProcessor.STORE_NAME),
                HeldItemSerde.keySerde(),
                new HeldItemSerde()
            );

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, byte[]>> controlStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ControlUpdateProcessor.STORE_NAME),
                Serdes.String(),
                Serdes.ByteArray()
            );

        topology.addSource("hold-source", Serdes.String().deserializer(), Serdes.ByteArray().deserializer(), Topics.HOLD_COMMANDS);
        topology.addProcessor("hold-processor", HoldCommandProcessor.supplier(snapshot), "hold-source");
        topology.addStateStore(holdsStoreBuilder, "hold-processor");

        topology.addGlobalStore(
            controlStoreBuilder,
            "control-source",
            Serdes.String().deserializer(),
            Serdes.ByteArray().deserializer(),
            Topics.EXECUTION_CONTROL,
            "control-update-processor",
            ControlUpdateProcessor.supplier(snapshot)
        );

        topology.addSink("release-sink", new StageTopicNameExtractor(),
            Serdes.String().serializer(), Serdes.ByteArray().serializer(), "hold-processor");

        return topology;
    }
}
