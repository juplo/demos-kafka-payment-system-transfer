package de.juplo.kafka.payment.transfer;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.adapter.KafkaMessagingService;
import de.juplo.kafka.payment.transfer.adapter.NoOpMessageService;
import de.juplo.kafka.payment.transfer.adapter.TransferConsumer;
import de.juplo.kafka.payment.transfer.adapter.TransferController;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import de.juplo.kafka.payment.transfer.ports.TransferService;
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

import java.util.Optional;
import java.util.Properties;


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
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return new KafkaConsumer<>(props);
  }

  @Bean(destroyMethod = "shutdown")
  TransferConsumer transferConsumer(
      TransferServiceProperties properties,
      KafkaConsumer<String, String> consumer,
      ObjectMapper mapper,
      TransferService productionTransferService,
      TransferService restoreTransferService)
  {
    return
        new TransferConsumer(
            properties.topic,
            consumer,
            mapper,
            new TransferConsumer.ConsumerUseCases() {
              @Override
              public void create(Long id, Long payer, Long payee, Integer amount)
              {
                productionTransferService.create(id, payer, payee, amount);
              }

              @Override
              public Optional<Transfer> get(Long id)
              {
                return productionTransferService.get(id);
              }

              @Override
              public void handleStateChange(Long id, Transfer.State state)
              {
                productionTransferService.handleStateChange(id, state);
              }
            },
            new TransferConsumer.ConsumerUseCases() {
              @Override
              public void create(Long id, Long payer, Long payee, Integer amount)
              {
                restoreTransferService.create(id, payer, payee, amount);
              }

              @Override
              public Optional<Transfer> get(Long id)
              {
                return restoreTransferService.get(id);
              }

              @Override
              public void handleStateChange(Long id, Transfer.State state)
              {
                restoreTransferService.handleStateChange(id, state);
              }
            });
  }

  @Bean
  KafkaMessagingService kafkaMessagingService(
      KafkaProducer<String, String> producer,
      ObjectMapper mapper,
      TransferServiceProperties properties)
  {
    return new KafkaMessagingService(producer, mapper, properties.topic);
  }

  @Bean
  TransferService productionTransferService(
      TransferRepository repository,
      KafkaMessagingService kafkaMessagingService)
  {
    return new TransferService(repository, kafkaMessagingService);
  }

  @Bean
  TransferService restoreTransferService(
      TransferRepository repository,
      NoOpMessageService noOpMessageService)
  {
    return new TransferService(repository, noOpMessageService);
  }

  @Bean
  TransferController transferController(
      TransferService productionTransferService,
      KafkaMessagingService kafkaMessagingService)
  {
    return new TransferController(productionTransferService, kafkaMessagingService);
  }


  public static void main(String[] args)
  {
    SpringApplication.run(TransferServiceApplication.class, args);
  }
}
