package de.juplo.kafka.payment.transfer.ports;

import de.juplo.kafka.payment.transfer.domain.Transfer;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;


public interface ReceiveTransferUseCase
{
  CompletableFuture<TopicPartition> receive(Transfer transfer);
}
