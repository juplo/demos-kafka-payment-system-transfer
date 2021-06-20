package de.juplo.kafka.payment.transfer.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.ports.CreateTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.GetTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.HandleStateChangeUseCase;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@RequestMapping("/consumer")
@ResponseBody
@Slf4j
public class TransferConsumer implements Runnable, ConsumerRebalanceListener
{
  private final String topic;
  private final int numPartitions;
  private final KafkaConsumer<String, String> consumer;
  private final AdminClient adminClient;
  private final TransferRepository repository;
  private final ObjectMapper mapper;
  private final ConsumerUseCases productionUseCases, restoreUseCases;

  private boolean running = false;
  private boolean shutdown = false;
  private Future<?> future = null;

  private final String groupId;
  private final String groupInstanceId;
  private final Map<String, String> instanceIdUriMapping;
  private final String[] instanceIdByPartition;

  private volatile boolean partitionOwnershipUnknown = true;


  public TransferConsumer(
      String topic,
      int numPartitions,
      Map<String, String> instanceIdUriMapping,
      KafkaConsumer<String, String> consumer,
      AdminClient adminClient,
      TransferRepository repository,
      ObjectMapper mapper,
      ConsumerUseCases productionUseCases,
      ConsumerUseCases restoreUseCases)
  {
    this.topic = topic;
    this.numPartitions = numPartitions;
    this.groupId = consumer.groupMetadata().groupId();
    this.groupInstanceId = consumer.groupMetadata().groupInstanceId().get();
    this.instanceIdByPartition = new String[numPartitions];
    this.instanceIdUriMapping = new HashMap<>(instanceIdUriMapping.size());
    for (String instanceId : instanceIdUriMapping.keySet())
    {
      // Requests are not redirected for the instance itself
      String uri = instanceId.equals(groupInstanceId)
          ? null
          : instanceIdUriMapping.get(instanceId);
      this.instanceIdUriMapping.put(instanceId, uri);
    }
    this.consumer = consumer;
    this.adminClient = adminClient;
    this.repository = repository;
    this.mapper = mapper;
    this.productionUseCases = productionUseCases;
    this.restoreUseCases = restoreUseCases;
  }


  @Override
  public void run()
  {
    while (running)
    {
      try
      {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        if (records.count() == 0)
          continue;

        log.debug("polled {} records", records.count());
        records.forEach(record -> handleRecord(record, productionUseCases));
      }
      catch (WakeupException e)
      {
        log.info("cleanly interrupted while polling");
      }
    }

    log.info("polling stopped");
  }

  private void handleRecord(ConsumerRecord<String, String> record, ConsumerUseCases useCases)
  {
    try
    {
      byte eventType = record.headers().lastHeader(EventType.HEADER).value()[0];

      switch (eventType)
      {
        case EventType.NEW_TRANSFER:

          NewTransferEvent newTransferEvent =
              mapper.readValue(record.value(), NewTransferEvent.class);
          useCases
              .create(
                  newTransferEvent.getId(),
                  newTransferEvent.getPayer(),
                  newTransferEvent.getPayee(),
                  newTransferEvent.getAmount());
          break;

        case EventType.TRANSFER_STATE_CHANGED:

          TransferStateChangedEvent stateChangedEvent =
              mapper.readValue(record.value(), TransferStateChangedEvent.class);
          useCases.handleStateChange(stateChangedEvent.getId(), stateChangedEvent.getState());
          break;
      }
    }
    catch (JsonProcessingException e)
    {
      log.error(
          "ignoring invalid json in message #{} on {}/{}: {}",
          record.offset(),
          record.topic(),
          record.partition(),
          record.value());
    }
    catch (IllegalArgumentException e)
    {
      log.error(
          "ignoring invalid message #{} on {}/{}: {}, message={}",
          record.offset(),
          record.topic(),
          record.partition(),
          e.getMessage(),
          record.value());
    }
  }


  public Optional<String> uriForKey(String key)
  {
    synchronized (this)
    {
      while (partitionOwnershipUnknown)
      {
        try { wait(); } catch (InterruptedException e) {}
      }

      int partition = TransferPartitioner.computeHashForKey(key, numPartitions);
      return
          Optional
              .ofNullable(instanceIdByPartition[partition])
              .map(id -> instanceIdUriMapping.get(id));
    }
  }

  @EventListener
  public synchronized void onApplicationEvent(ContextRefreshedEvent event)
  {
    // "Needed", because this method is called synchronously during the
    // initialization pahse of Spring. If the subscription happens
    // in the same thread, it would block the completion of the initialization.
    // Hence, the app would not react to any signal (CTRL-C, for example) except
    // a KILL until the restoring is finished.
    future = CompletableFuture.runAsync(() -> start());
  }


  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions)
  {
    partitionOwnershipUnknown = true;
    log.info("partitions revoked: {}", partitions);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions)
  {
    log.info("partitions assigned: {}", partitions);
    fetchAssignmentsAsync();
    if (partitions.size() > 0)
      restore(partitions);
  }

  private void fetchAssignmentsAsync()
  {
    adminClient
        .describeConsumerGroups(List.of(groupId))
        .describedGroups()
        .get(groupId)
        .whenComplete((descriptions, e) ->
        {
          if (e != null)
          {
            log.error("could not fetch group data: {}", e.getMessage());
          }
          else
          {
            synchronized (this)
            {
              for (MemberDescription description : descriptions.members())
              {
                description
                    .assignment()
                    .topicPartitions()
                    .forEach(tp -> instanceIdByPartition[tp.partition()] = description.groupInstanceId().get());
              }
              partitionOwnershipUnknown = false;
              notifyAll();
            }
          }
        });
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions)
  {
    partitionOwnershipUnknown = true;
    log.info("partiotions lost: {}", partitions);
  }


  private void restore(Collection<TopicPartition> partitions)
  {
    log.info("--> starting restore...");

    partitions
        .stream()
        .map(topicPartition -> topicPartition.partition())
        .forEach(partition -> repository.resetStorageForPartition(partition));

    Map<Integer, Long> lastSeen =
        consumer
            .endOffsets(partitions)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey().partition(),
                entry -> entry.getValue() - 1));

    Map<Integer, Long> positions =
        lastSeen
            .keySet()
            .stream()
            .collect(Collectors.toMap(
                partition -> partition,
                partition -> 0l));

    while (
        positions
            .entrySet()
            .stream()
            .map(entry -> entry.getValue() < lastSeen.get(entry.getKey()))
            .reduce(false, (a, b) -> a || b))
    {
      try
      {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        if (records.count() == 0)
          continue;

        log.debug("polled {} records", records.count());
        records.forEach(record ->
        {
          handleRecord(record, restoreUseCases);
          positions.put(record.partition(), record.offset());
        });
      }
      catch(WakeupException e)
      {
        log.info("--> cleanly interrupted while restoring");
      }
    }

    log.info("--> restore completed!");
  }

  @PostMapping("start")
  public synchronized String start()
  {
    if (running)
    {
      log.info("already running!");
      return "Already running!";
    }

    int foundNumPartitions = consumer.partitionsFor(topic).size();
    if (foundNumPartitions != numPartitions)
    {
      log.error(
          "unexpected number of partitions for topic {}: expected={}, found={}",
          topic,
          numPartitions,
          foundNumPartitions
          );
      return "Wrong number of partitions for topic " + topic + ": " + foundNumPartitions;
    }

    consumer.subscribe(List.of(topic), this);

    running = true;
    future = CompletableFuture.runAsync(this);

    log.info("started");
    return "Started";
  }

  @PostMapping("stop")
  public synchronized String stop()
  {
    if (!running)
    {
      log.info("not running!");
      return "Not running";
    }

    running = false;

    if (!future.isDone())
      consumer.wakeup();

    log.info("waiting for the consumer...");
    try
    {
      future.get();
    }
    catch (InterruptedException|ExecutionException e)
    {
      log.error("Exception while joining polling task!", e);
      return e.getMessage();
    }
    finally
    {
      future = null;
      consumer.unsubscribe();
    }

    log.info("stopped");
    return "Stopped";
  }

  public synchronized void shutdown()
  {
    log.info("shutdown initiated!");
    shutdown = true;
    stop();
    log.info("closing consumer");
    consumer.close();
  }



  public interface ConsumerUseCases
      extends
        GetTransferUseCase,
        CreateTransferUseCase,
        HandleStateChangeUseCase {};
}
