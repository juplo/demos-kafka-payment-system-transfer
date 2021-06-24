package de.juplo.kafka.payment.transfer;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.adapter.*;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.persistence.InMemoryTransferRepository;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import de.juplo.kafka.payment.transfer.ports.TransferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;
import java.util.Properties;


@SpringBootApplication
@EnableConfigurationProperties(TransferServiceProperties.class)
@Slf4j
public class TransferServiceApplication
{
  @Bean(destroyMethod = "close")
  AdminClient adminClient(TransferServiceProperties properties)
  {
    Assert.hasText(properties.getBootstrapServers(), "juplo.transfer.bootstrap-servers must be set");

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());

    return AdminClient.create(props);
  }

  @Bean(destroyMethod = "close")
  KafkaProducer<String, String> producer(TransferServiceProperties properties)
  {
    Assert.hasText(properties.getBootstrapServers(), "juplo.transfer.bootstrap-servers must be set");
    Assert.hasText(properties.getTopic(), "juplo.transfer.topic must be set");
    Assert.notNull(properties.getNumPartitions(), "juplo.transfer.num-partitions must be set");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TransferPartitioner.class);
    props.put(TransferPartitioner.TOPIC, properties.getTopic());
    props.put(TransferPartitioner.NUM_PARTITIONS, properties.getNumPartitions());

    return new KafkaProducer<>(props);
  }

  @Bean
  KafkaConsumer<String, String> consumer(TransferServiceProperties properties)
  {
    Assert.hasText(properties.getBootstrapServers(), "juplo.transfer.bootstrap-servers must be set");
    Assert.hasText(properties.getGroupId(), "juplo.transfer.group-id must be set");
    Assert.hasText(properties.getGroupInstanceId(), "juplo.transfer.group-instance-id must be set");

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
    props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, properties.getGroupInstanceId());
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getCanonicalName());
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
      AdminClient adminClient,
      TransferRepository repository,
      LocalStateStoreSettings localStateStoreSettings,
      ObjectMapper mapper,
      TransferService productionTransferService,
      TransferService restoreTransferService)
  {
    return
        new TransferConsumer(
            properties.getTopic(),
            properties.getNumPartitions(),
            properties.getInstanceIdUriMapping(),
            consumer,
            adminClient,
            repository,
            Clock.systemDefaultZone(),
            localStateStoreSettings.interval,
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
    return new KafkaMessagingService(producer, mapper, properties.getTopic());
  }

  @RequiredArgsConstructor
  static class LocalStateStoreSettings
  {
    final Optional<File> file;
    final int interval;
  }

  @Bean
  LocalStateStoreSettings localStateStoreSettings(TransferServiceProperties properties)
  {
    if (properties.getStateStoreInterval() < 1)
    {
      log.info("juplo.transfer.state-store-interval is < 1: local storage of state is deactivated");
      return new LocalStateStoreSettings(Optional.empty(), 0);
    }

    if (!StringUtils.hasText(properties.getLocalStateStorePath()))
    {
      log.info("juplo.transfer.local-state-store-path is not set: local storage of state is deactivated!");
      return new LocalStateStoreSettings(Optional.empty(), 0);
    }

    Path path = Path.of(properties.getLocalStateStorePath());
    log.info("using {} as local state store", path.toAbsolutePath());

    if (Files.notExists(path))
    {
      try
      {
        Files.createFile(path);
      }
      catch (IOException e)
      {
        throw new IllegalArgumentException("Could not create local state store: " + path.toAbsolutePath());
      }
    }

    if (!(Files.isReadable(path) && Files.isWritable(path)))
    {
      throw new IllegalArgumentException("No R/W-access on local state store: " + path.toAbsolutePath());
    }

    return new LocalStateStoreSettings(Optional.of(path.toFile()), properties.getStateStoreInterval());
  }

  @Bean
  InMemoryTransferRepository inMemoryTransferRepository(
      LocalStateStoreSettings localStateStoreSettings,
      TransferServiceProperties properties,
      ObjectMapper mapper)
  {
    return new InMemoryTransferRepository(localStateStoreSettings.file, properties.getNumPartitions(), mapper);
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
      KafkaMessagingService kafkaMessagingService,
      TransferConsumer transferConsumer)
  {
    return new TransferController(productionTransferService, kafkaMessagingService, transferConsumer);
  }


  public static void main(String[] args)
  {
    SpringApplication.run(TransferServiceApplication.class, args);
  }
}
