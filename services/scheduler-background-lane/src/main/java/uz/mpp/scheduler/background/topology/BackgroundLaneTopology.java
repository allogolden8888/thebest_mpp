package uz.mpp.scheduler.background.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import uz.mpp.scheduler.background.core.BackgroundTask;

/**
 * Топология Background Lane (services_specifictaion.md §3.1):
 *
 * <pre>
 * scheduler.background.commands --> [background-processor] --> operator.dlr.unresolved (dlr-sink)
 *                                          |                --> notification.retry (notification-sink)
 *                                     background-tasks-store
 *                                     (persistent, changelog scheduler.background.state.changelog)
 * </pre>
 */
public final class BackgroundLaneTopology {

    private BackgroundLaneTopology() {
    }

    public static Topology build() {
        Topology topology = new Topology();

        StoreBuilder<KeyValueStore<String, BackgroundTask>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(BackgroundCommandProcessor.STORE_NAME),
            BackgroundTaskSerde.keySerde(),
            new BackgroundTaskSerde()
        );

        topology.addSource("background-source", Serdes.String().deserializer(), Serdes.ByteArray().deserializer(), Topics.BACKGROUND_COMMANDS);
        topology.addProcessor("background-processor", BackgroundCommandProcessor.supplier(), "background-source");
        topology.addStateStore(storeBuilder, "background-processor");

        topology.addSink(BackgroundCommandProcessor.DLR_SINK, Topics.OPERATOR_DLR_UNRESOLVED,
            Serdes.String().serializer(), Serdes.ByteArray().serializer(), "background-processor");
        topology.addSink(BackgroundCommandProcessor.NOTIFICATION_SINK, Topics.NOTIFICATION_RETRY,
            Serdes.String().serializer(), Serdes.ByteArray().serializer(), "background-processor");

        return topology;
    }
}
