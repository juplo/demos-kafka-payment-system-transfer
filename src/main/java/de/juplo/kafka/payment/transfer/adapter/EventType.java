package de.juplo.kafka.payment.transfer.adapter;

public abstract class EventType
{
  public final static String HEADER = "$";

  public final static byte NEW_TRANSFER = 1;
  public final static byte TRANSFER_STATE_CHANGED = 2;
}
