package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;


public interface HandleStateChangeUseCase
{
  void handle(Transfer transfer);
}
