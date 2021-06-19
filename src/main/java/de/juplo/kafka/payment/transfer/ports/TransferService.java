package de.juplo.kafka.payment.transfer.ports;


import de.juplo.kafka.payment.transfer.domain.Transfer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import static de.juplo.kafka.payment.transfer.domain.Transfer.State.CHECKED;
import static de.juplo.kafka.payment.transfer.domain.Transfer.State.CREATED;


@Slf4j
@RequiredArgsConstructor
public class TransferService implements CreateTransferUseCase, HandleStateChangeUseCase, GetTransferUseCase
{
  private final TransferRepository repository;
  private final MessagingService messagingService;

  @Override
  public void create(Long id, Long payer, Long payee, Integer amount)
  {
    repository
        .get(id)
        .ifPresentOrElse(
            stored -> log.info(
                "transfer already exisits: {}, ignoring: id={}, payer={}, payee={}, amount={}",
                stored,
                payer,
                payee,
                amount),
            () ->
            {
              Transfer transfer =
                  Transfer
                      .builder()
                      .id(id)
                      .payer(payer)
                      .payee(payee)
                      .amount(amount)
                      .build();

              log.info("creating transfer: {}", transfer);
              repository.store(transfer);
              messagingService.send(transfer.getId(), CREATED);
            });
  }

  @Override
  public void handleStateChange(Long id, Transfer.State state)
  {
    get(id)
        .ifPresentOrElse(
            transfer ->
            {
              switch (state)
              {
                case CREATED:

                  transfer.setState(CREATED);
                  repository.store(transfer);
                  check(transfer);
                  break;

                case CHECKED:

                  transfer.setState(CHECKED);
                  repository.store(transfer);
                  // TODO: What's next...?
                  break;

                default:

                  log.warn("TODO: handle {} state {}", state.foreign ? "foreign" : "domain", state);
              }
            },
            () -> log.error("unknown transfer: {}", id));
  }

  private void check(Transfer transfer)
  {
    // TODO: Do some time consuming checks...
    messagingService.send(transfer.getId(), CHECKED);
  }

  public Optional<Transfer> get(Long id)
  {
    return repository.get(id);
  }
}
