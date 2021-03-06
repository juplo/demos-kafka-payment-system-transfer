package de.juplo.kafka.payment.transfer.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.MessagingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;


@RequiredArgsConstructor
@Slf4j
public class KafkaMessagingService implements MessagingService
{
  private final KafkaProducer<String, String> producer;
  private final ObjectMapper mapper;
  private final String topic;


  @Override
  public CompletableFuture<?> send(Transfer transfer)
  {
    return send(transfer.getId(), EventType.NEW_TRANSFER, NewTransferEvent.ofTransfer(transfer));
  }

  public CompletableFuture<?> send(Long id, Transfer.State state)
  {
    return send(id, EventType.TRANSFER_STATE_CHANGED, new TransferStateChangedEvent(id, state));
  }

  private CompletableFuture send(Long id, byte eventType, Object payload)
  {
    try
    {
      CompletableFuture<TopicPartition> future = new CompletableFuture<>();

      ProducerRecord<String, String> record =
          new ProducerRecord<>(
              topic,
              Long.toString(id),
              mapper.writeValueAsString(payload));
      record.headers().add(EventType.HEADER, new byte[] { eventType });

      producer.send(record, (metadata, exception) ->
      {
        if (metadata != null)
        {
          log.debug("Sent {} to {}/{}:{}", payload, metadata.topic(), metadata.partition(), metadata.offset());
          future.complete(new TopicPartition(metadata.topic(), metadata.partition()));
        }
        else
        {
          log.error("Could not send {}: {}", payload, exception.getMessage());
          future.completeExceptionally(exception);
        }
      });

      return future;
    }
    catch (JsonProcessingException e)
    {
      throw new RuntimeException("Could not convert " + payload, e);
    }
  }

}
