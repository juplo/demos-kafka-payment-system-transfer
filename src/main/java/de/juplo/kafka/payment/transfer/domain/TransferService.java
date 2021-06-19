package de.juplo.kafka.payment.transfer.domain;


import de.juplo.kafka.payment.transfer.ports.GetTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.HandleTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.MessagingService;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import static de.juplo.kafka.payment.transfer.domain.Transfer.State.CHECKED;
import static de.juplo.kafka.payment.transfer.domain.Transfer.State.CREATED;


@Slf4j
@RequiredArgsConstructor
public class TransferService implements HandleTransferUseCase, GetTransferUseCase
{
  private final TransferRepository repository;
  private final MessagingService messagingService;

  private void create(Transfer transfer)
  {
    repository
        .get(transfer.getId())
        .ifPresentOrElse(
            stored -> log.info("transfer already exisits: {}, ignoring: {}", stored, transfer),
            () ->
            {
              repository.store(transfer);
              transfer.setState(CREATED);
              messagingService.send(transfer);
            });
  }

  @Override
  public void handle(Transfer transfer)
  {
    Transfer.State state = transfer.getState();
    switch (state)
    {
      case RECEIVED:
        repository.store(transfer);
        create(transfer);
        break;

      case CREATED:
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
    // TODO: Do some time consuming checks...
    transfer.setState(CHECKED);
    messagingService.send(transfer);
  }

  public Optional<Transfer> get(Long id)
  {
    return repository.get(id);
  }
}
