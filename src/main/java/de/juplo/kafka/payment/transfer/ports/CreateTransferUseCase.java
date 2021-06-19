package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;


public interface CreateTransferUseCase
{
  void create(Long id, Long payer, Long payee, Integer amount);
}
