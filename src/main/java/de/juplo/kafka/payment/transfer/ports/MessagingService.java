package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;

import java.util.concurrent.CompletableFuture;


public interface MessagingService
{
  CompletableFuture<?> send(Transfer transfer);
  CompletableFuture<?> send(Long id, Transfer.State state);
}
