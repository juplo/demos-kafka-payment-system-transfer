package de.juplo.kafka.payment.transfer.adapter;


import de.juplo.kafka.payment.transfer.domain.Transfer;
import de.juplo.kafka.payment.transfer.ports.GetTransferUseCase;
import de.juplo.kafka.payment.transfer.ports.ReceiveTransferUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


@RestController
@RequiredArgsConstructor
@Slf4j
 public class TransferController
{
  public final static String PATH = "/transfers";

  private final ReceiveTransferUseCase receiveTransferUseCase;
  private final GetTransferUseCase getTransferUseCase;


  @PostMapping(
      path = PATH,
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> transfer(
      HttpServletRequest request,
      @Valid @RequestBody TransferDTO transferDTO)
  {
    Transfer transfer =
        Transfer
            .builder()
            .id(transferDTO.getId())
            .payer(transferDTO.getPayer())
            .payee(transferDTO.getPayee())
            .amount(transferDTO.getAmount())
            .build();

    DeferredResult<ResponseEntity<?>> result = new DeferredResult<>();

    receiveTransferUseCase
        .receive(transfer)
        .thenApply(
            $ ->
                ResponseEntity
                    .created(URI.create(PATH + "/" + transferDTO.getId()))
                    .build())
        .thenAccept(
            responseEntity -> result.setResult(responseEntity))
        .exceptionally(
            e ->
            {
              result.setErrorResult(e);
              return null;
            });

    return result;
  }

  @GetMapping(
      path = PATH + "/{id}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<TransferDTO> get(@PathVariable Long id)
  {
    return
        getTransferUseCase
            .get(id)
            .map(transfer -> ResponseEntity.ok(TransferDTO.of(transfer)))
            .orElse(ResponseEntity.notFound().build());
  }

  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ExceptionHandler(MethodArgumentNotValidException.class)
  public Map<String, Object> handleValidationExceptions(
      HttpServletRequest request,
      MethodArgumentNotValidException e)
  {
    Map<String, Object> errorAttributes = new HashMap<>();
    errorAttributes.put("status", HttpStatus.BAD_REQUEST.value());
    errorAttributes.put("error", HttpStatus.BAD_REQUEST.getReasonPhrase());
    errorAttributes.put("path", request.getRequestURI());
    errorAttributes.put("method", request.getMethod());
    errorAttributes.put("timestamp", new Date());
    Map<String, String> errors = new HashMap<>();
    e.getBindingResult().getAllErrors().forEach((error) -> {
      String fieldName = ((FieldError) error).getField();
      String errorMessage = error.getDefaultMessage();
      errors.put(fieldName, errorMessage);
    });
    errorAttributes.put("errors", errors);
    errorAttributes.put("message", "Validation failed: Invalid message format, error count: " + errors.size());
    return errorAttributes;
  }
}
