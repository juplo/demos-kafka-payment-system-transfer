package de.juplo.kafka.payment.transfer.domain;

import org.junit.jupiter.api.Test;

import static de.juplo.kafka.payment.transfer.domain.Transfer.State.PENDING;
import static de.juplo.kafka.payment.transfer.domain.Transfer.State.SENT;
import static org.assertj.core.api.Assertions.assertThat;


public class TransferTest
{
  @Test
  public void testEqualsIgnoresState()
  {
    Transfer a = Transfer.builder().id(1).payer(1).payee(1).amount(1).state(SENT).build();
    Transfer b = Transfer.builder().id(1).payer(1).payee(1).amount(1).state(PENDING).build();

    assertThat(a).isEqualTo(b);
  }
}
