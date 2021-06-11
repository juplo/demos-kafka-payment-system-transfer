package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;

import java.util.Optional;


public interface TransferRepository
{
  void store(Transfer transfer);

  Optional<Transfer> get(Long id);

  void update(Long id, Transfer.State oldState, Transfer.State newState) throws IllegalArgumentException;

  void remove(Long id);
}
