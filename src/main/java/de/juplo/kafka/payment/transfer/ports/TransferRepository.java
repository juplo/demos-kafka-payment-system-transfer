package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;

import java.util.Map;
import java.util.Optional;


public interface TransferRepository
{
  void store(Transfer transfer);

  Optional<Transfer> get(Long id);

  void remove(Long id);

  long activatePartition(int partition);

  void deactivatePartition(int partition, long offset);

  long storedPosition(int partition);

  void storeState(Map<Integer, Long> offsets);
}
