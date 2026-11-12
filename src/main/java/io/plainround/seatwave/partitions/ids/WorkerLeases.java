package io.plainround.seatwave.partitions.ids;

import com.github.sviperll.result4j.Result;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Grants worker ids from the directory's {@code id_worker_leases}, one holder per id at a time, so
 * no two live generators share a worker id (the third place the id invariant is enforced).
 *
 * <p>The grant is one statement: an insert of the id, or, if the id has a row, an update that
 * happens only when that row's lease has expired ({@code ON CONFLICT ... DO UPDATE ... WHERE}).
 * PostgreSQL runs the conflict check and the update atomically per row, so two instances that pick
 * the same free id at once cannot both get it: one inserts, the other's update finds a live lease
 * and returns no row, and it tries the next id. The directory's clock ({@code now()}) is the only
 * clock that decides expiry, so instances whose clocks disagree still agree on who holds an id. The
 * holder stops using its lease a tenth of the duration before the directory's expiry, by its own
 * clock, measured from before it sent the request.
 *
 * <p>Thread-safe: stateless apart from the pool, which is.
 */
public final class WorkerLeases {

  private static final Pattern HOLDER = Pattern.compile("[A-Za-z0-9._-]{1,64}");
  // Ids tried per round; a round that loses every one to racers reads the free ids again.
  private static final int CANDIDATES_PER_ROUND = 64;
  private static final int MAX_ROUNDS = 16;
  // The holder's own deadline comes this share of the lease earlier than the directory's, which
  // covers the request's own time and a clock that runs slightly fast.
  private static final int SAFETY_DIVISOR = 10;

  private static final String FREE_IDS =
      """
      SELECT candidate FROM generate_series(0, ?) AS candidate
      WHERE NOT EXISTS (
        SELECT 1 FROM directory.id_worker_leases
        WHERE worker_id = candidate AND expires_at > now())
      ORDER BY candidate
      LIMIT ?
      """;

  private static final String GRANT =
      """
      INSERT INTO directory.id_worker_leases AS lease (worker_id, holder, expires_at)
      VALUES (?, ?, now() + make_interval(secs => ?))
      ON CONFLICT (worker_id) DO UPDATE
        SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
        WHERE lease.expires_at <= now()
      RETURNING worker_id
      """;

  private static final String RENEW =
      """
      UPDATE directory.id_worker_leases
      SET expires_at = now() + make_interval(secs => ?)
      WHERE worker_id = ? AND holder = ? AND expires_at > now()
      """;

  private static final String RELEASE =
      "DELETE FROM directory.id_worker_leases WHERE worker_id = ? AND holder = ?";

  private final DataSource directory;
  private final Duration leaseDuration;
  private final int maxWorkerId;
  private final Clock clock;

  /**
   * Creates the lease client.
   *
   * @param directory a pool to the directory as the application role
   * @param leaseDuration how long a grant or renewal lasts in the directory
   * @param layout the layout whose worker ids are granted
   * @param clock the holder's clock, for the lease's local deadline
   */
  public WorkerLeases(DataSource directory, Duration leaseDuration, IdLayout layout, Clock clock) {
    this.directory = Objects.requireNonNull(directory, "directory");
    this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
    this.maxWorkerId = Math.toIntExact(Objects.requireNonNull(layout, "layout").maxWorkerId());
    this.clock = Objects.requireNonNull(clock, "clock");
    if (leaseDuration.toSeconds() < 1) {
      throw new IllegalArgumentException("a lease must last at least 1 s, was " + leaseDuration);
    }
  }

  /**
   * Takes the lowest free worker id.
   *
   * @param holder who takes it: letters, digits, dot, underscore or hyphen, at most 64
   * @return the lease, or {@link LeaseRefusal#NO_FREE_WORKER_ID} when every id has a live lease
   * @throws SQLException if the directory fails
   */
  public Result<WorkerLease, LeaseRefusal> take(String holder) throws SQLException {
    checkHolder(holder);
    Instant requestedAt = clock.instant();
    try (Connection connection = directory.getConnection()) {
      for (int round = 0; round < MAX_ROUNDS; round++) {
        List<Integer> candidates = freeIds(connection);
        if (candidates.isEmpty()) {
          return Result.error(LeaseRefusal.NO_FREE_WORKER_ID);
        }
        for (int candidate : candidates) {
          if (granted(connection, candidate, holder)) {
            return Result.success(new WorkerLease(candidate, holder, deadline(requestedAt)));
          }
        }
      }
    }
    // Every round lost every candidate to other instances: as good as none free right now.
    return Result.error(LeaseRefusal.NO_FREE_WORKER_ID);
  }

  /**
   * Extends a lease that is still live and still this holder's.
   *
   * @param lease the lease to extend
   * @return the extended lease, or {@link LeaseRefusal#LEASE_LOST} if it had expired in the
   *     directory or belongs to another holder; the caller must then stop issuing ids
   * @throws SQLException if the directory fails
   */
  public Result<WorkerLease, LeaseRefusal> renew(WorkerLease lease) throws SQLException {
    Objects.requireNonNull(lease, "lease");
    Instant requestedAt = clock.instant();
    try (Connection connection = directory.getConnection();
        PreparedStatement renew = connection.prepareStatement(RENEW)) {
      renew.setLong(1, leaseDuration.toSeconds());
      renew.setInt(2, lease.workerId());
      renew.setString(3, lease.holder());
      if (renew.executeUpdate() == 0) {
        return Result.error(LeaseRefusal.LEASE_LOST);
      }
    }
    return Result.success(new WorkerLease(lease.workerId(), lease.holder(), deadline(requestedAt)));
  }

  /**
   * Gives a lease back at shutdown, so its id is free at once instead of after expiry.
   *
   * @param lease the lease
   * @return true if the lease was still this holder's and is now removed
   * @throws SQLException if the directory fails
   */
  public boolean release(WorkerLease lease) throws SQLException {
    Objects.requireNonNull(lease, "lease");
    try (Connection connection = directory.getConnection();
        PreparedStatement release = connection.prepareStatement(RELEASE)) {
      release.setInt(1, lease.workerId());
      release.setString(2, lease.holder());
      return release.executeUpdate() == 1;
    }
  }

  private List<Integer> freeIds(Connection connection) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(FREE_IDS)) {
      query.setInt(1, maxWorkerId);
      query.setInt(2, CANDIDATES_PER_ROUND);
      List<Integer> candidates = new ArrayList<>();
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          candidates.add(rows.getInt(1));
        }
      }
      return candidates;
    }
  }

  private boolean granted(Connection connection, int workerId, String holder) throws SQLException {
    try (PreparedStatement grant = connection.prepareStatement(GRANT)) {
      grant.setInt(1, workerId);
      grant.setString(2, holder);
      grant.setLong(3, leaseDuration.toSeconds());
      try (ResultSet row = grant.executeQuery()) {
        // No row: the id has a live lease of someone else's (the WHERE of DO UPDATE was false).
        return row.next();
      }
    }
  }

  private Instant deadline(Instant requestedAt) {
    return requestedAt.plus(leaseDuration).minus(leaseDuration.dividedBy(SAFETY_DIVISOR));
  }

  private static void checkHolder(String holder) {
    Objects.requireNonNull(holder, "holder");
    if (!HOLDER.matcher(holder).matches()) {
      throw new IllegalArgumentException("holder must match " + HOLDER.pattern() + ": " + holder);
    }
  }
}
