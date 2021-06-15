package de.juplo.kafka.payment.transfer.adapter;

import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.MessagingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;


@Component
@Slf4j
public class NoOpMessageService implements MessagingService
{
  @Override
  public CompletableFuture<?> send(Transfer transfer)
  {
    log.info("restoring transfer: {}", transfer);
    return CompletableFuture.completedFuture(transfer.toString());
  }

  @Override
  public CompletableFuture<?> send(Long id, Transfer.State state)
  {
    log.info("restoring state-change for transfer {}: {}", id, state);
    return CompletableFuture.completedFuture("transfer: " + id + " - " + state);
  }
}
