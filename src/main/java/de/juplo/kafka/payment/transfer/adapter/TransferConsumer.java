package de.juplo.kafka.payment.transfer.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.ports.CreateTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.GetTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.HandleStateChangeUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@RequestMapping("/consumer")
@ResponseBody
@RequiredArgsConstructor
@Slf4j
public class TransferConsumer implements Runnable
{
  private final String topic;
  private final KafkaConsumer<String, String> consumer;
  private final ExecutorService executorService;
  private final ObjectMapper mapper;
  private final ConsumerUseCases productionUseCases, restoreUseCases;

  private boolean restoring = true;
  private boolean running = false;
  private boolean shutdown = false;
  private Future<?> future = null;


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

  @EventListener
  public synchronized void onApplicationEvent(ContextRefreshedEvent event)
  {
    // Needed, because this method is called synchronously during the
    // initialization pahse of Spring. If the restoring is processed
    // in the same thread, it would block the completion of the initialization.
    // Hence, the app would not react to any signal (CTRL-C, for example) except
    // a KILL until the restoring is finished.
    future = executorService.submit(() -> restore());
  }

  private void restore()
  {
    log.info("--> starting restore...");

    List<TopicPartition> partitions =
        consumer
            .partitionsFor(topic)
            .stream()
            .map(info -> new TopicPartition(topic, info.partition()))
            .collect(Collectors.toList());

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

    log.info("assigning {}}", partitions);
    consumer.assign(partitions);

    while (
        restoring &&
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
        return;
      }
    }

    log.info("--> restore completed!");
    restoring = false;

    // We are intentionally _not_ unsubscribing here, since that would
    // reset the offset to _earliest_, because we disabled offset-commits.

    start();
  }

  @PostMapping("start")
  public synchronized String start()
  {
    if (restoring)
    {
      log.error("cannot start while restoring");
      return "Denied: Restoring!";
    }

    String result = "Started";

    if (running)
    {
      stop();
      result = "Restarted";
    }

    running = true;
    future = executorService.submit(this);

    log.info("started");
    return result;
  }

  @PostMapping("stop")
  public synchronized String stop()
  {
    if (!(running || restoring))
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
