package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;


public interface InitiateTransferUseCase
{
  void initiate(Transfer transfer);
}
