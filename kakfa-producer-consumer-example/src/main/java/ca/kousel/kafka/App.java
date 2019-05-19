package ca.kousel.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import ca.kouse.kafka.streams.VolvoUsageMessage;
import ca.kousel.kafka.constants.IKafkaConstants;
import ca.kousel.kafka.consumer.ConsumerCreator;
import ca.kousel.kafka.producer.ProducerCreator;

public class App {
	public static void main(String[] args) {
		//runProducer();
		//runConsumer();
		runProducerForStreamVolvo();
		runConsumerForStreamVolvo();
	}

	static void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
		consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
		// User either below code or subscribe method call 
		/*TopicPartition tp = new TopicPartition("3partition_2replication", 0);
		ArrayList al = new ArrayList();
		al.add(tp);
		consumer.assign(al);*/
		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}
		
			consumerRecords.forEach((record) -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer() {
		Producer<Long, String> producer = ProducerCreator.createProducer();

		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			
			//Record without key
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>
			(IKafkaConstants.TOPIC_NAME,
					"This is record " + index);
			
			//Record with key being passed
			final ProducerRecord<Long,String> rec1 = new ProducerRecord<Long,String>(IKafkaConstants.TOPIC_NAME,
					new Long(index), "With key "+index);
			try {
				//RecordMetadata metadata = producer.send(record).get();
				RecordMetadata metadata = producer.send(rec1).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}
	
	static void runProducerForStreamVolvo() {
		Producer<String, VolvoUsageMessage> producer = ProducerCreator.createVolvoProducer();
		String[] appName = new String[] {
				"MARKET-99","MARKET-800"
		};
		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			VolvoUsageMessage vum = new VolvoUsageMessage();
			vum.setAppName(appName[index%2]);
			vum.setAppId(appName[index%2]);
			vum.setLob(appName[index%2]);
			vum.setNumberOfDevelopers(index);
			vum.setNumberOfUsers(index);
			vum.setCount(index);
			
			//Record without key
			final ProducerRecord<String, VolvoUsageMessage> record = new ProducerRecord<String, VolvoUsageMessage>
			("streaminput", vum.getAppName(),
					vum);
			
			//Record with key being passed
			final ProducerRecord<Long,String> rec1 = new ProducerRecord<Long,String>(IKafkaConstants.TOPIC_NAME,
					new Long(index), "With key "+index);
			try {
				//RecordMetadata metadata = producer.send(record).get();
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}
	static void runConsumerForStreamVolvo() {
		Consumer<String, Long> consumer = ConsumerCreator.createConsumerForVolvo();
		consumer.subscribe(Collections.singletonList("streamoutput"));
		// User either below code or subscribe method call 
		/*TopicPartition tp = new TopicPartition("3partition_2replication", 0);
		ArrayList al = new ArrayList();
		al.add(tp);
		consumer.assign(al);*/
		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<String, Long> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}
		
			consumerRecords.forEach((record) -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

}
