package de.juplo.kafka.payment.transfer.adapter;


import de.juplo.kafka.payment.transfer.domain.Transfer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.LinkedList;
import java.util.List;


@Data
@EqualsAndHashCode
@Builder
public class TransferStateChangedEvent
{
  private long id;
  private Transfer.State state;
}
