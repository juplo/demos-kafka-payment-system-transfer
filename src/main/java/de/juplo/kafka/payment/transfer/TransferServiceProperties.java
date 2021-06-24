package de.juplo.kafka.payment.transfer;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;


@ConfigurationProperties("juplo.transfer")
@Getter
@Setter
public class TransferServiceProperties
{
  private String bootstrapServers = "localhost:9092";
  private String topic = "transfers";
  private Integer numPartitions = 5;
  private String groupId = "transfers";
  private String groupInstanceId;
  private Map<String, String> instanceIdUriMapping;
  private String localStateStorePath;
  private int stateStoreInterval = 60;

  public Map<String, String> getInstanceIdUriMapping()
  {
    return instanceIdUriMapping == null ? new HashMap<>() : instanceIdUriMapping;
  }
}
