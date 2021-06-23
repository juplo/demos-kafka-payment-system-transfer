package de.juplo.kafka.payment.transfer;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties("juplo.transfer")
@Getter
@Setter
public class TransferServiceProperties
{
  private String bootstrapServers = "localhost:9092";
  private String topic = "transfers";
  private String groupId = "transfers";
}
