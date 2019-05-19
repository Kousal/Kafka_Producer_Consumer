package ca.kousel.kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import ca.kouse.kafka.streams.VolvoUsageMessage;
import ca.kousel.kafka.pojo.CustomObject;

public class VolvoSerializer implements Serializer<VolvoUsageMessage> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, VolvoUsageMessage data) {
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retVal = objectMapper.writeValueAsString(data).getBytes();
		} catch (Exception exception) {
			System.out.println("Error in serializing object" + data);
		}
		return retVal;
	}

	@Override
	public void close() {

	}

}