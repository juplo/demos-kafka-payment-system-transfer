package de.juplo.kafka.payment.transfer.domain;


import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.LinkedList;
import java.util.List;

import static de.juplo.kafka.payment.transfer.domain.Transfer.State.*;


@Data
@Builder
@EqualsAndHashCode(exclude = { "state", "messages" })
public class Transfer
{
  public enum State
  {
    CREATED(false),
    INVALID(false),
    CHECKED(false),
    APPROVED(true),
    REJECTED(true);

    public final boolean foreign;

    State(boolean foreign)
    {
      this.foreign = foreign;
    }
  }

  private final long id;
  private final long payer;
  private final long payee;
  private final int amount;

  private State state;

  private final List<String> messages = new LinkedList<>();


  public Transfer setState(State state)
  {
    if (state == null)
      throw new IllegalArgumentException("State must not be null!");

    switch (state)
    {
      case CREATED:
        if (this.state != null)
          throw new IllegalArgumentException("Illegal state-change " + this.state + " -> CREATED");
        break;

      case CHECKED:
      case INVALID:
        if (this.state != CREATED)
          throw new IllegalArgumentException("Illegal state-change " + this.state + " -> " + state);
        break;

      default:
          throw new IllegalArgumentException("State-change not yet considered:" + this.state + " -> " + state);

    }

    this.state = state;
    return this;
  }

  public Transfer addMessage(String message)
  {
    messages.add(message);
    return this;
  }
}
