package io.plainround.seatwave.partitions.routing;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Where one key lives: the ring version that placed it, its logical shard and the machine that
 * serves that shard. Immutable.
 *
 * @param ringVersion the directory's LIVE ring version the router used
 * @param shardName the logical shard, for example {@code shard-17}
 * @param hostName the machine, for example {@code shard-host-1}
 */
public record ShardAddress(int ringVersion, String shardName, String hostName) {

  // The alphabet ring_shards.shard_name allows; it also makes the schema name safe to use.
  private static final Pattern SHARD_NAME = Pattern.compile("[a-z0-9-]{1,32}");

  /** Checks the names, so an address can never name a schema outside the shard alphabet. */
  public ShardAddress {
    Objects.requireNonNull(shardName, "shardName");
    Objects.requireNonNull(hostName, "hostName");
    if (!SHARD_NAME.matcher(shardName).matches()) {
      throw new IllegalArgumentException("shard name must match [a-z0-9-]{1,32}: " + shardName);
    }
  }

  /**
   * Returns the schema that holds this shard's tables: {@code shard-17} lives in {@code shard_17}.
   */
  public String schemaName() {
    return schemaOf(shardName);
  }

  /**
   * Returns the schema of a shard name.
   *
   * @param shardName a shard name from the directory
   * @return the name with every hyphen replaced by an underscore
   */
  public static String schemaOf(String shardName) {
    return shardName.replace('-', '_');
  }
}
