package ca.kouse.kafka.streams;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import ca.kousel.kafka.constants.IKafkaConstants;
import ca.kousel.kafka.partitioner.CustomPartitioner;

public class StreamConfiguration {
	public static StreamsConfig streamProperties() {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, IKafkaConstants.ZOOKEEPER_CONFIG);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.STREAM_ID);
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		return new StreamsConfig(props);
	}
}
