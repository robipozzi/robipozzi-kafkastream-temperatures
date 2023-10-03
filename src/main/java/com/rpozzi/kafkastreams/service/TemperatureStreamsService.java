package com.rpozzi.kafkastreams.service;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rpozzi.kafkastreams.dto.Sensor;

@Service
public class TemperatureStreamsService {
	private static final Logger logger = LoggerFactory.getLogger(TemperatureStreamsService.class);
	@Value(value = "${spring.kafka.bootstrap-servers}")
	private String kafkaBootstrapServers;
	@Value(value = "${kafkastreams.application.id}")
	private String kafkaStreamsAppId;
	@Value(value = "${kafka.topic.temperatures}")
	private String temperatureKafkaTopic;
	private int highTemperatureThreshold = 25;

	public void process() {
		// ################################################################################
		// ############### Kafka Streams - Temperature handling sample code ###############
		// ################################################################################
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamsAppId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// Initialize StreamsBuilder
		final StreamsBuilder builder = new StreamsBuilder();
		
		// Read Stream from input Kafka Topic ${kafka.topic.temperatures} (see application.properties for mapping)
		logger.info("Streaming from '" + temperatureKafkaTopic + "' Kafka topic ...");
		KStream<String, String> sensorData = builder.stream(temperatureKafkaTopic);
		
		/* ##### Stream Transformations - START ##### */
		// ===== Print messages from input Kafka Topic ${kafka.topic.temperatures} 
		sensorData.foreach((key, value) -> logger.debug(key + " => " + value));
		
		// ===== Apply mapValues transformation
		KStream<String, Integer> temperatures = sensorData.mapValues(value -> consumeMsg(value).getTemperature());
		temperatures.foreach((key, value) -> logger.debug("mapValues --> TEMPERATURE = " + value));
		
		// ===== Apply filter transformation
		KStream<String, Integer> highTemperatures = temperatures.filter((key, value) -> value > highTemperatureThreshold);
		highTemperatures.foreach((key, value) -> logger.debug("filter (higher than " + highTemperatureThreshold + " degrees) --> !!! HIGH TEMPERATURE !!! -- " + value));
		
		
		
		/* ##### Stream Transformations - END ##### */

		final Topology topology = builder.build();
		logger.debug("Printing Topology ...");
		logger.debug(topology.describe().toString());
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("temperature-streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
	
	private Sensor consumeMsg(String in) {
		logger.debug("===> running consumeMsg(String in) method ...");
		logger.info("Reading from '" + temperatureKafkaTopic + "' Kafka topic (using SpringBoot Kafka APIs) ...");
		Sensor sensor = null;
		ObjectMapper mapper = new ObjectMapper();
		try {
			logger.debug("Message read : " + in);
			sensor = mapper.readValue(in, Sensor.class);
			logger.info("Temperature = " + sensor.getTemperature() + " - Humidity = " + sensor.getHumidity());
			
		} catch (JsonMappingException e) {
			logger.error(e.getLocalizedMessage());
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			logger.error(e.getLocalizedMessage());
			e.printStackTrace();
		}
		logger.debug("<=== returning Sensor data ...");
		return sensor;
	}

}