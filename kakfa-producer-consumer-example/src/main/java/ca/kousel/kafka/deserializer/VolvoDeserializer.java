package ca.kousel.kafka.deserializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import ca.kouse.kafka.streams.VolvoUsageMessage;
import ca.kousel.kafka.pojo.CustomObject;

public class VolvoDeserializer implements Deserializer<VolvoUsageMessage> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public VolvoUsageMessage deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		VolvoUsageMessage object = null;
		try {
			object = mapper.readValue(data, VolvoUsageMessage.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	@Override
	public void close() {
	}
}