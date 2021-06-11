package de.juplo.kafka.payment.transfer.adapter;

import de.juplo.kafka.payment.transfer.domain.Transfer;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;


/**
 * Simple DTO used by the REST interface
 */
@Data
@Builder
public class TransferDTO
{
  @NotNull(message = "Cannot be null")
  @Min(value = 1, message = "A valid transfer id must be a positive number")
  private Long id;
  @NotNull(message = "Cannot be null")
  @Min(value = 1, message = "A valid bank account id must be a positive number")
  private Long payer;
  @NotNull(message = "Cannot be null")
  @Min(value = 1, message = "A valid bank account id must be a positive number")
  private Long payee;
  @NotNull(message = "Cannot be null")
  @Min(value = 1, message = "The amount of a transfer must be a positv value")
  private Integer amount;

  private Transfer.State state;


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


  public static TransferDTO of(Transfer transfer)
  {
    return
        TransferDTO
            .builder()
            .id(transfer.getId())
            .payer(transfer.getPayer())
            .payee(transfer.getPayee())
            .amount(transfer.getAmount())
            .state(transfer.getState())
            .build();
  }
}
