package de.juplo.kafka.payment.transfer.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@Component
@RequiredArgsConstructor
@Slf4j
public class InMemoryTransferRepository implements TransferRepository
{
  private final Map<Long, String> map = new HashMap<>();
  private final ObjectMapper mapper;


  @Override
  public void store(Transfer transfer)
  {
    try
    {
      map.put(transfer.getId(), mapper.writeValueAsString(transfer));
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
            .ofNullable(map.get(id))
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
    map.remove(id);
  }
}
