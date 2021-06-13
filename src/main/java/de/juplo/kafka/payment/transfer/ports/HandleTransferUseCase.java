package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;


public interface HandleTransferUseCase
{
  void handle(Transfer transfer);
}
