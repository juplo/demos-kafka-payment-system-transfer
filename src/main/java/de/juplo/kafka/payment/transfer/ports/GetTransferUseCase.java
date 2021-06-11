package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;

import java.util.Optional;


public interface GetTransferUseCase
{
  Optional<Transfer> get(Long id);
}
