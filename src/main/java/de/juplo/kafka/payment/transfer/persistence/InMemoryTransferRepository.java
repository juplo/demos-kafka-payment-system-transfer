package de.juplo.kafka.payment.transfer.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.adapter.TransferPartitioner;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;


@Slf4j
public class InMemoryTransferRepository implements TransferRepository
{
  private final int numPartitions;
  private final ObjectMapper mapper;

  private final Data data;
  private final Optional<File> stateStore;


  public InMemoryTransferRepository(Optional<File> stateStore, int numPartitions, ObjectMapper mapper)
  {
    this.stateStore = stateStore;
    this.numPartitions = numPartitions;
    this.mapper = mapper;

    Data data = null;
    try
    {
      if (stateStore.isPresent())
      {
        try (
            FileInputStream fis = new FileInputStream(stateStore.get());
            ObjectInputStream ois = new ObjectInputStream(fis))
        {
          data = (Data) ois.readObject();
          final long offsets[] = data.offsets;
          final Map<Long, String> map[] = data.map;
          IntStream
              .range(0, numPartitions)
              .forEach(i -> log.info(
                  "restored locally stored state for partition {}: position={}, entries={}",
                  i,
                  offsets[i],
                  offsets[i] == 0 ? 0 : map[i].size()));
          return;
        }
        catch (IOException | ClassNotFoundException e)
        {
          log.error("could not read state from local store {}: {}", stateStore.get(), e.getMessage());
        }
      }

      log.info("restoring state from Kafka");
      data = new Data(numPartitions);
    }
    finally
    {
      this.data = data;
      IntStream
          .range(0, numPartitions)
          .forEach(i -> log.info(
              "restored state for partition {}: position={}, entries={}",
              i,
              this.data.offsets[i],
              this.data.offsets[i] == 0 ? 0 : this.data.map[i].size()));
    }
  }


  @Override
  public void store(Transfer transfer)
  {
    try
    {
      int partition = partitionForId(transfer.getId());
      data.map[partition].put(transfer.getId(), mapper.writeValueAsString(transfer));
      // We reset the offset for the state of the modified partition,
      // because the corresponding offset is not known (yet).
      data.offsets[partition] = 0;
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
            .ofNullable(data.map[partitionForId(id)].get(id))
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
    data.map[partitionForId(id)].remove(id);
  }

  @Override
  public long activatePartition(int partition)
  {
    if (data.offsets[partition] == 0)
    {
      // Initialize the state of the partition, if
      // no corresponding offset is known.
      if (data.map[partition] != null)
        log.warn(
            "dropping state for partition {} ({} entries), because the corresponding offset is unknown!",
            partition,
            data.map[partition].size());

      data.map[partition] = new HashMap<>();
    }

    return data.offsets[partition];
  }

  @Override
  public void deactivatePartition(int partition, long offset)
  {
    data.offsets[partition] = offset;
  }

  @Override
  public long storedPosition(int partition)
  {
    return data.offsets[partition];
  }

  @Override
  public void storeState(Map<Integer, Long> offsets)
  {
    offsets.forEach((partition, offset) -> data.offsets[partition] = offset);
    for (int i = 0; i < numPartitions; i++)
    {
      if (data.offsets[i] == 0 && data.map[i] != null)
      {
        log.warn(
            "dropping state for partition {} ({} entries), because the corresponding offset is unknown!",
            i,
            data.map[i].size());

        data.map[i] = null;
      }
    }
    stateStore.ifPresent(file ->
    {
      try (
          FileOutputStream fos = new FileOutputStream(file);
          ObjectOutputStream oos = new ObjectOutputStream(fos))
      {
        oos.writeObject(data);
        IntStream
            .range(0, numPartitions)
            .forEach(i -> log.info(
                "locally stored state for partition {}: position={}, entries={}",
                i,
                data.offsets[i],
                data.offsets[i] == 0 ? 0 : this.data.map[i].size()));
      }
      catch (IOException e)
      {
        log.error("could not write state to store {}: {}", file, e.getMessage());
      }
    });
  }


  private int partitionForId(long id)
  {
    String key = Long.toString(id);
    return TransferPartitioner.computeHashForKey(key, numPartitions);
  }


  static class Data implements Serializable
  {
    final long offsets[];
    final Map<Long, String> map[];

    Data(int numPartitions)
    {
      offsets = new long[numPartitions];
      map = new Map[numPartitions];
      for (int i = 0; i < numPartitions; i++)
        offsets[i] = 0;
    }
  }
}
