package de.juplo.kafka.payment.transfer.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.HandleTransferUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


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
  private final HandleTransferUseCase handleTransferUseCase;

  private boolean running = false;
  private Future<?> future = null;


  @Override
  public void run()
  {
    while (running)
    {
      try
      {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        if (records.count() > 0)
          log.debug("polled {} records", records.count());

        records.forEach(record ->
        {
          try
          {
            Transfer transfer = mapper.readValue(record.value(), Transfer.class);
            handleTransferUseCase.handle(transfer);
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
        });
      }
      catch (WakeupException e)
      {
        log.info("polling aborted!");
      }
    }

    log.info("polling stopped");
  }


  @PostMapping("start")
  public synchronized String start()
  {
    String result = "Started";

    if (running)
    {
      stop();
      result = "Restarted";
    }

    log.info("subscribing to topic {}", topic);
    consumer.subscribe(Set.of(topic));
    running = true;
    future = executorService.submit(this);

    return result;
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
    log.info("waiting for the polling-loop to finish...");
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
      log.info("unsubscribing");
      consumer.unsubscribe();
    }

    return "Stoped";
  }

  public synchronized void shutdown()
  {
    log.info("shutdown initiated!");
    stop();
    log.info("closing consumer");
    consumer.close();
  }
}
