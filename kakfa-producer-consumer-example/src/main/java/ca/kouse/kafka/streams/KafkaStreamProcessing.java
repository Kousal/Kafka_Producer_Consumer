package ca.kouse.kafka.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import ca.kousel.kafka.deserializer.VolvoDeserializer;
import ca.kousel.kafka.serializer.VolvoSerializer;


import org.apache.kafka.common.serialization.Serde;


public class KafkaStreamProcessing {
	public static void main(String args[])
	{
		KStreamBuilder kStreamBuilder = new KStreamBuilder();
		
		
		final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<Integer> intSerde = Serdes.Integer();
		
		final Serializer<VolvoUsageMessage> volvoSerializer = new VolvoSerializer();
		final Deserializer<VolvoUsageMessage> volvoDeserializer = new VolvoDeserializer();
		
		final Serde<VolvoUsageMessage> countryMessageSerd = Serdes.serdeFrom(volvoSerializer, volvoDeserializer);
		
		KStream<String, VolvoUsageMessage> volvoStream = kStreamBuilder.stream(stringSerde, countryMessageSerd, "streaminput");
		
		
				
		KTable<String,Integer> runningVolvoCountPerApp = volvoStream.aggregateByKey(
				new Initializer<Integer>() {
					public Integer apply() {
						System.out.println(0);
						return new Integer(0);}
				},
				new Aggregator<String, VolvoUsageMessage, Integer>() {

					@Override
					public Integer apply(String aggKey, VolvoUsageMessage value, Integer aggregate) {
						aggregate = aggregate+value.getCount();
						System.out.println(aggKey + " " + aggregate);
						return aggregate;
					}
					
				}, 
				stringSerde,
				intSerde,
				"streamoutput"
				);
		
		runningVolvoCountPerApp.toStream().to(stringSerde, intSerde, "streamOutput");;
		
		
		StreamsConfig sc = StreamConfiguration.streamProperties();
		
		KafkaStreams ks = new KafkaStreams(kStreamBuilder, sc);
		
		ks.start();
		
		
		
	}
}
