package de.juplo.kafka.payment.transfer.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.adapter.TransferPartitioner;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class InMemoryTransferRepository implements TransferRepository
{
  private final int numPartitions;
  private final Map<Long, String> map[];
  private final ObjectMapper mapper;


  public InMemoryTransferRepository(int numPartitions, ObjectMapper mapper)
  {
    this.numPartitions = numPartitions;
    this.map = new HashMap[numPartitions];
    for (int i = 0; i < numPartitions; i++)
      this.map[i] = new HashMap<>();
    this.mapper = mapper;
  }


  @Override
  public void store(Transfer transfer)
  {
    try
    {
      int partition = partitionForId(transfer.getId());
      map[partition].put(transfer.getId(), mapper.writeValueAsString(transfer));
    }
    catch (JsonProcessingException e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<Transfer> get(Long id)
  {
    return
        Optional
            .ofNullable(map[partitionForId(id)].get(id))
            .map(json -> {
              try
              {
                return mapper.readValue(json, Transfer.class);
              }
              catch (JsonProcessingException e)
              {
                throw new RuntimeException("Could not convert JSON: " + json, e);
              }
            });
  }

  @Override
  public void remove(Long id)
  {
    map[partitionForId(id)].remove(id);
  }

  @Override
  public void resetStorageForPartition(int partition)
  {
    log.info(
        "resetting storage for partition {}: dropping {} entries",
        partition,
        map[partition].size());
    map[partition].clear();
  }

  private int partitionForId(long id)
  {
    String key = Long.toString(id);
    return TransferPartitioner.computeHashForKey(key, numPartitions);
  }
}
