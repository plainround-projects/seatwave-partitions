package io.plainround.seatwave.partitions.holds;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.ids.LeasedIdGenerator;
import io.plainround.seatwave.partitions.routing.ShardKeys;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Registers customers on their own shards. The id comes from a {@link LeasedIdGenerator} (no
 * sequence spans 64 shards); the row then goes to the shard of {@code customer-<id>}, where the
 * customer's reservations and "my tickets" rows will live too.
 *
 * <p>Simplified on purpose: Module 13's {@code one_customer_per_ref} is now unique per shard only.
 * Production code would keep a global lookup from {@code customer_ref} to the id, placed by the ref
 * on the same ring, written before the customer row, as {@code id_routes} is for holds.
 *
 * <p>Thread-safe: the generator and the router are.
 */
public final class Customers {

  // Module 7's customer string, as Module 13's customer_ref_format checks.
  private static final Pattern CUSTOMER_REF = Pattern.compile("[A-Za-z0-9_-]{1,64}");
  private static final String REGISTER =
      "INSERT INTO customers (customer_id, customer_ref, created_at) VALUES (?, ?, now())";

  private final ShardRouter router;
  private final LeasedIdGenerator ids;

  /**
   * Creates the registry.
   *
   * @param router the router
   * @param ids the generator of customer ids
   */
  public Customers(ShardRouter router, LeasedIdGenerator ids) {
    this.router = Objects.requireNonNull(router, "router");
    this.ids = Objects.requireNonNull(ids, "ids");
  }

  /**
   * Registers one customer.
   *
   * @param customerRef Module 7's customer string, for example {@code fan_1}
   * @return the new customer id, or a routing refusal
   * @throws SQLException if the database fails
   * @throws InterruptedException if the thread is interrupted while the shard is paused
   * @throws io.plainround.seatwave.partitions.ids.IdGenerationRefusedException if no id can be
   *     issued (clock or lease), which the caller answers with 503
   */
  public Result<Long, WriteRefusal> register(String customerRef)
      throws SQLException, InterruptedException {
    Objects.requireNonNull(customerRef, "customerRef");
    if (!CUSTOMER_REF.matcher(customerRef).matches()) {
      throw new IllegalArgumentException(
          "a customer ref is 1 to 64 letters, digits, _ or -: " + customerRef);
    }
    long customerId = ids.nextId();
    return router.write(
        ShardKeys.customer(customerId),
        connection -> {
          try (PreparedStatement insert = connection.prepareStatement(REGISTER)) {
            insert.setLong(1, customerId);
            insert.setString(2, customerRef);
            insert.executeUpdate();
            return Result.success(customerId);
          }
        });
  }
}
