package de.juplo.kafka.payment.transfer.domain;


import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;


@Data
@Builder
@EqualsAndHashCode(exclude = "state")
public class Transfer
{
  public enum State
  {
    SENT,
    FAILED,
    PENDING,
    APPROVED,
    REJECTED
  }

  private final long id;
  private final long payer;
  private final long payee;
  private final int amount;

  private State state;
}
