package de.juplo.kafka.payment.transfer.adapter;

import de.juplo.kafka.payment.transfer.domain.Transfer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.springframework.util.Assert;

import java.nio.charset.Charset;
import java.util.Map;


/**
 * This partitioner uses the same algorithm to compute the partion
 * for a given record as hash of its key as the
 * {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
 *
 * The main reason, to use this partitioner instead of the default is, to
 * make it more explicitly visible in the code of this example, that the
 * same algorithm is used when calculating the partition for a record, that
 * is about to be send, as when calculating the partition for a given
 * {@link Transfer#getId() transfer-id} to look up the node, that can
 * serve a get-request for that transfer.
 */
public class TransferPartitioner implements Partitioner
{
  public final static Charset CONVERSION_CHARSET = Charset.forName("UTF-8");

  public final static String TOPIC = "topic";
  public final static String NUM_PARTITIONS = "num-partitions";


  /**
   * Computes the partition as a hash of the given key.
   *
   * @param key The key
   * @return An <code>int</code>, that represents the partition.
   */
  public static int computeHashForKey(String key, int numPartitions)
  {
    return TransferPartitioner.computeHashForKey(key.getBytes(CONVERSION_CHARSET), numPartitions);
  }

  /**
   * Computes the partition as a hash of the given key.
   *
   * @param keyBytes The key as an <code>byte</code></code>-array.
   * @return An <code>int</code>, that represents the partition.
   */
  public static int computeHashForKey(byte[] keyBytes, int numPartitions)
  {
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
  }


  private String topic;
  private int numPartitions;

  @Override
  public void configure(Map<String, ?> configs)
  {
    Assert.notNull(configs.get(TOPIC), TOPIC + " must not be null");
    Assert.hasText(configs.get(TOPIC).toString(), TOPIC + " must not be empty");
    Assert.notNull(configs.get(NUM_PARTITIONS), NUM_PARTITIONS + " must not be null");
    Assert.isAssignable(
        configs.get(NUM_PARTITIONS).getClass(),
        Integer.class,
        NUM_PARTITIONS + " must by an int");

    topic = configs.get(TOPIC).toString();
    numPartitions = (Integer)configs.get(NUM_PARTITIONS);
  }

  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name - Only used to check, if it equals the configured fix topic.
   * @param key The key to partition on - Not used: the algorithm uses the argument <code>keyBytes</code>.
   * @param keyBytes serialized key to partition on - Must not be null.
   * @param value The value to partition on or null - Not used.
   * @param valueBytes serialized value to partition on or null - Not used.
   * @param cluster The current cluster metadata - Not used.
   */
  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
  {
    if (keyBytes == null)
      throw new IllegalArgumentException("The argument \"keyBytes\" must not be null");

    if (!topic.equals(this.topic))
      throw new IllegalArgumentException(
          "This partitioner can only be used for a fixe partition. Here: fixed=" +
              this.topic +
              ", requested=" +
              topic);

    // "Stolen" from the DefaultPartitioner: hash the keyBytes to choose a partition
    return computeHashForKey(keyBytes, numPartitions);
  }

  @Override
  public void close() {}
  @Override
  public void onNewBatch(String topic, Cluster cluster, int prevPartition) {}
}
