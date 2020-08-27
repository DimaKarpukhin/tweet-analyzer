package com.handson.twitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.twitter.config.KafkaEmbeddedConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.rule.KafkaEmbedded;

@SpringBootApplication
public class TwitterApplication {

	public static void main(String[] args) {
		SpringApplication.run(TwitterApplication.class, args);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

	public static final KafkaEmbedded KAFKA_EMBEDDED = createKafkaEmbedded();
	private static KafkaEmbedded createKafkaEmbedded() {
		AnnotationConfigApplicationContext context =
				new AnnotationConfigApplicationContext(KafkaEmbeddedConfig.class);
		KafkaEmbedded kafkaEmbedded = context.getBean(KafkaEmbedded.class);
		Runtime.getRuntime().addShutdownHook(new Thread(context::close));
		return kafkaEmbedded;
	}
}
