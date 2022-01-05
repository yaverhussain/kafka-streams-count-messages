package kafka.streams.count;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Basically keeps track of current offsets of consumers consuming off post processing kafka topics. One off : Gets initial offsets and starts kafka stream per
 * post processing kafka topic. This will keep getting new data and storing in local cache. On going scheduled: updates the offsets map to latest and kicks
 * local cache method to flush out of date messages.
 * 
 * @author yhpw09
 *
 */

@Service
public class KafkaMessageCountService {
    
    private KafkaStreams streams;

    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration;

    /**
     * Windowed state store
     */
    public void startCountOnlyStreams(String topicName, String stateStoreName) {

        StreamsBuilder builder = new StreamsBuilder();
        
        builder.stream(topicName, Consumed.with(Serdes.String(), Serdes.String()))
        .selectKey((k,v) -> topicName)
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
        .count(Materialized
                .<String, Long> as(Stores.persistentWindowStore(stateStoreName, Duration.ofDays(45), Duration.ofMinutes(60), false))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));


        streams = new KafkaStreams(builder.build(), kafkaStreamsConfiguration.asProperties());

        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
    
    @SuppressWarnings("deprecation")
    public long getMessageCount(String topicName, String stateStoreName) {
        ReadOnlyWindowStore<String, Long> windowStore = streams.store(stateStoreName, QueryableStoreTypes.windowStore());
        Instant timeTo = Instant.now();
        
        // All the times
        Instant timeFrom = Instant.ofEpochMilli(0);

        // get all windows and count messages in each window
        WindowStoreIterator<Long> iterator = null;
        long count = 0;
        try {
            iterator = windowStore.fetch(topicName, timeFrom, timeTo);
            while (iterator.hasNext()) {
                KeyValue<Long, Long> next = iterator.next();
                Long value = next.value;
                if (value != null) {
                    count += value;
                }
            } // close the iterator to release resources
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }

        return count;
    }
    
    public void getMessageCount() {
        ReadOnlyKeyValueStore<String, String> store =  streams.store("topic_test" + "-count1", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, String> iter = store.all();
        while (iter.hasNext()) {
            KeyValue<String, String> entry = iter.next();
            System.out.println("  Processed key: "+entry.key+" and value: "+entry.value+" and sending to processed-topic topic");
        }
        iter.close();
    }
}
