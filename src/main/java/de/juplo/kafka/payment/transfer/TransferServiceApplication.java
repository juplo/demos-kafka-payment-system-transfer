package de.juplo.kafka.payment.transfer;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.adapter.KafkaMessagingService;
import de.juplo.kafka.payment.transfer.adapter.TransferConsumer;
import de.juplo.kafka.payment.transfer.domain.TransferService;
import de.juplo.kafka.payment.transfer.ports.MessagingService;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@SpringBootApplication
@EnableConfigurationProperties(TransferServiceProperties.class)
@Slf4j
public class TransferServiceApplication
{
  @Bean(destroyMethod = "close")
  KafkaProducer<String, String> producer(TransferServiceProperties properties)
  {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    return new KafkaProducer<>(props);
  }

  @Bean
  KafkaConsumer<String, String> consumer(TransferServiceProperties properties)
  {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return new KafkaConsumer<>(props);
  }

  @Bean(destroyMethod = "shutdown")
  ExecutorService executorService()
  {
    return Executors.newFixedThreadPool(1);
  }

  @Bean(destroyMethod = "shutdown")
  TransferConsumer transferConsumer(
      TransferServiceProperties properties,
      KafkaConsumer<String, String> consumer,
      ExecutorService executorService,
      ObjectMapper mapper,
      TransferService transferService)
  {
    TransferConsumer transferConsumer =
        new TransferConsumer(
            properties.topic,
            consumer,
            executorService,
            mapper,
            transferService,
            transferService,
            transferService);
    transferConsumer.start();
    return transferConsumer;
  }

  @Bean
  MessagingService kafkaMessagingService(
      KafkaProducer<String, String> producer,
      ObjectMapper mapper,
      TransferServiceProperties properties)
  {
    return new KafkaMessagingService(producer, mapper, properties.topic);
  }

  @Bean
  TransferService transferService(
      TransferRepository repository,
      MessagingService messagingService)
  {
    return new TransferService(repository, messagingService);
  }


  public static void main(String[] args)
  {
    SpringApplication.run(TransferServiceApplication.class, args);
  }
}
