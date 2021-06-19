package de.juplo.kafka.payment.transfer.adapter;

import de.juplo.kafka.payment.transfer.domain.Transfer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;


@Data
@EqualsAndHashCode
@Builder
public class NewTransferEvent
{
  private Long id;
  private Long payer;
  private Long payee;
  private Integer amount;

  public Transfer toTransfer()
  {
    return
        Transfer
            .builder()
            .id(id)
            .payer(payer)
            .payee(payee)
            .amount(amount)
            .build();
  }

  public static NewTransferEvent ofTransfer(Transfer transfer)
  {
    return
        NewTransferEvent
            .builder()
            .id(transfer.getId())
            .payer(transfer.getPayer())
            .payee(transfer.getPayee())
            .amount(transfer.getAmount())
            .build();
  }
}
