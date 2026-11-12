package io.plainround.seatwave.partitions.routing;

import java.sql.SQLException;
import java.util.Optional;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

/** Reads which named constraint (or trigger rule) a database error came from. Stateless. */
public final class Constraints {

  /** The ownership rule of {@code db/host/V1__served_shards.sql}. */
  public static final String WRITE_TO_OWNER_ONLY = "write_to_owner_only";

  private Constraints() {}

  /**
   * Returns the constraint a statement violated.
   *
   * @param failure the error from the driver
   * @return the constraint name the server reported, or empty for any other error
   */
  public static Optional<String> violated(SQLException failure) {
    if (failure instanceof PSQLException serverError) {
      ServerErrorMessage message = serverError.getServerErrorMessage();
      if (message != null) {
        return Optional.ofNullable(message.getConstraint());
      }
    }
    return Optional.empty();
  }
}
