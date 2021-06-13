package de.juplo.kafka.payment.transfer.domain;


import de.juplo.kafka.payment.transfer.ports.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static de.juplo.kafka.payment.transfer.domain.Transfer.State.CHECKED;
import static de.juplo.kafka.payment.transfer.domain.Transfer.State.RECEIVED;


@Slf4j
@RequiredArgsConstructor
public class TransferService implements ReceiveTransferUseCase, HandleTransferUseCase, GetTransferUseCase
{
  private final TransferRepository repository;
  private final MessagingService messagingService;

  public CompletableFuture<TopicPartition> receive(Transfer transfer)
  {
    transfer.setState(RECEIVED);
    return messagingService.send(transfer);
  }

  @Override
  public void handle(Transfer transfer)
  {
    Transfer.State state = transfer.getState();
    switch (state)
    {
      case RECEIVED:
        repository.store(transfer);
        check(transfer);
        break;

      case CHECKED:
        repository.store(transfer);
        // TODO: What's next...?
        break;

      default:
        log.warn("TODO: handle {} state {}", state.foreign ? "foreign" : "domain", state);
    }
  }

  private void check(Transfer transfer)
  {
    repository
        .get(transfer.getId())
        .ifPresentOrElse(
            stored ->
            {
              if (!transfer.equals(stored))
                log.error("ignoring already received transfer with differing data: old={}, new={}", stored, transfer);
            },
            () ->
            {
              repository.store(transfer);
              // TODO: Do some time consuming checks...
              transfer.setState(CHECKED);
              messagingService.send(transfer);
            });
  }

  public Optional<Transfer> get(Long id)
  {
    return repository.get(id);
  }
}
