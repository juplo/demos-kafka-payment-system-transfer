package de.juplo.kafka.payment.transfer.domain;


import de.juplo.kafka.payment.transfer.ports.GetTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.InitiateTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.MessagingService;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import static de.juplo.kafka.payment.transfer.domain.Transfer.State.*;


@Slf4j
@RequiredArgsConstructor
public class TransferService implements InitiateTransferUseCase, GetTransferUseCase
{
  private final TransferRepository repository;
  private final MessagingService messagingService;

  public synchronized void initiate(Transfer transfer)
  {
    repository
        .get(transfer.getId())
        .ifPresentOrElse(
            stored ->
            {
              if (!transfer.equals(stored))
                throw new IllegalArgumentException(
                    "Re-Initiation of transfer with different data: old=" +
                        stored +
                        ", new=" +
                        transfer);

              if (stored.getState() == FAILED)
              {
                repository.update(transfer.getId(), FAILED, SENT);
                log.info("Resending faild transfer: " + stored);
                send(transfer);
              }
            },
            () ->
            {
              send(transfer);
              transfer.setState(SENT);
              repository.store(transfer);
            });
  }

  private void send(Transfer transfer)
  {
    messagingService
        .send(transfer)
        .thenApply(
            $ ->
            {
              repository.update(transfer.getId(), SENT, PENDING);
              return null;
            })
        .exceptionally(
            e ->
            {
              repository.update(transfer.getId(), SENT, FAILED);
              return null;
            });
  }

  public Optional<Transfer> get(Long id)
  {
    return repository.get(id);
  }
}
