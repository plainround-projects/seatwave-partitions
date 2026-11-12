package io.plainround.seatwave.partitions.routing;

import com.github.sviperll.result4j.Result;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * One transaction's work on one shard. The router hands it a connection whose schema is the shard's
 * and whose autocommit is off; it commits when the work returns a success and rolls back when the
 * work returns a refusal or throws.
 *
 * @param <T> what the work returns
 */
@FunctionalInterface
public interface ShardWork<T> {

  /**
   * Runs the work.
   *
   * @param connection the shard's connection, inside a transaction
   * @return the value, or the refusal that makes the router roll back
   * @throws SQLException if the database fails in a way the work does not expect
   */
  Result<T, WriteRefusal> run(Connection connection) throws SQLException;
}
