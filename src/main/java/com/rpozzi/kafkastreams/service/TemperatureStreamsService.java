package com.rpozzi.kafkastreams.service;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
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
	private static final String SSL_SECURITY_PROTOCOL = "SSL";
	@Value(value = "${spring.kafka.bootstrap-servers}")
	private String kafkaBootstrapServers;
	@Value(value = "${kafkastreams.application.id}")
	private String kafkaStreamsAppId;
	@Value(value = "${kafka.topic.temperatures}")
	private String temperatureKafkaTopic;
	private int windowSizeMinutes = 1;
	private int gracePeriodMinutes = 1;
	private int highTemperatureThreshold = 23;
	// SSL configuration properties
	@Value(value = "${spring.kafka.security.protocol}")
	private String securityProtocol;
	@Value(value = "${spring.kafka.ssl.trust-store-location}")
	private String truststore;
	@Value(value = "${spring.kafka.ssl.trust-store-password}")
	private String truststorePassword;

	public void process() {
		// ################################################################################
		// ############### Kafka Streams - Temperature handling sample code ###############
		// ################################################################################
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamsAppId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		/* ==== If SSL is enabled, set SSL properties ==== */
		if (securityProtocol.equals(TemperatureStreamsService.SSL_SECURITY_PROTOCOL)) {
			props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
			props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
			props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
		}

		// Initialize StreamsBuilder
		final StreamsBuilder builder = new StreamsBuilder();
		
		// Read Stream from input Kafka Topic ${kafka.topic.temperatures} (see application.properties for mapping)
		logger.info("Streaming from '" + temperatureKafkaTopic + "' Kafka topic ...");
		KStream<String, String> sensorData = builder.stream(temperatureKafkaTopic);
		// ===== Print messages from sensorData Stream (i.e.: messages published to input Kafka Topic ${kafka.topic.temperatures}) 
		sensorData.foreach((key, value) -> logger.debug(key + " => " + value));
				
		/* ******************************************************************************************** */
		/* ***** Stream Transformations to calculate Average Temperature in a Time Window - START ***** */
		/* ******************************************************************************************** */
		
		// ===== Apply mapValues transformation to extract temperature data from input messages
		KStream<String, Integer> temperatures = sensorData.mapValues(value -> consumeMsg(value).getTemperature());
		temperatures.foreach((key, value) -> logger.debug("apply mapValues() to extract temperature --> TEMPERATURE = " + value));
		
		// A tumbling time window with a size of 1 minute (and, by definition, an implicit advance interval of 1 minute), and grace period of 1 minute.
		Duration windowSize = Duration.ofMinutes(windowSizeMinutes);
		Duration gracePeriod = Duration.ofMinutes(gracePeriodMinutes);
		
		// Calculate the average temperature in a 1-minute tumbling window
		//   - apply groupByKey to group temperature data (since the key is always the same, it groups every temperature in the defined time window)
		//   - aggregate temperature data in the time window (uses TemperatureAggregate custom class, that exposes 2 convenient methods
		//		--> add(): which sums temperature data and increment the count of temperature data points coming in
		//		--> getAverage(): which returns the average temperature (calculated as (sum of temperatures) / (count of temperature data points) in time window)
		//   - apply mapValues to produce a record with the same key and average temperature as the value (as calculated by TemperatureAggregate.getAverage()) 
		KStream<Windowed<String>, Double> averageTemperatureStream = temperatures
			.groupByKey() /* Key in the original message is always the same, so we can group on it directly */
			.windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gracePeriod))
			.aggregate(
				() -> new TemperatureAggregate(0, 0), /* initializer */
				(key, temperature, temperatureAggregate) -> temperatureAggregate.add(temperature), /* adder */
				Materialized.with(Serdes.String(), new TemperatureAggregateSerde())
			)
			.mapValues((windowedKey, aggregate) -> aggregate.getAverage())
			.toStream();
		averageTemperatureStream.foreach((key, value) -> logger.info("===> AVERAGE TEMPERATURE = " + value));
		
		/* ****************************************************************************************** */
		/* ***** Stream Transformations to calculate Average Temperature in a Time Window - END ***** */
		/* ****************************************************************************************** */
		
		// ===== Apply filter transformation to select average temperatures higher than 22°
		KStream<Windowed<String>, Double> highTemperatures = averageTemperatureStream.filter((key, value) -> value > highTemperatureThreshold);
		// ===== Print messages from highTemperatures Stream 
		highTemperatures.foreach((key, value) -> logger.debug(key + " => " + value));
		highTemperatures.foreach((key, value) -> logger.info("--> !!! HIGH TEMPERATURE !!! -- Average temperature is " + value + ", higher than " + highTemperatureThreshold + "°"));
		
		/* **************************************** */
		/* ***** Run Streams Topology - START ***** */
		/* **************************************** */
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
		/* ************************************** */
		/* ***** Run Streams Topology - END ***** */
		/* ************************************** */
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