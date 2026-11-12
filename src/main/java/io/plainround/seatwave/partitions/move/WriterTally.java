package io.plainround.seatwave.partitions.move;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

/**
 * What the move's writer did. Immutable.
 *
 * @param sent hold attempts started
 * @param heldIds every hold the flow acknowledged
 * @param bookedIds every booking the flow acknowledged
 * @param refusals refusals by kind (the {@code WriteRefusal} case's name), sorted by name
 * @param failures attempts that ended with a database error
 */
public record WriterTally(
    long sent,
    List<UUID> heldIds,
    List<UUID> bookedIds,
    Map<String, Long> refusals,
    long failures) {

  /** Copies the collections; the refusals keep their order by name. */
  public WriterTally {
    heldIds = List.copyOf(Objects.requireNonNull(heldIds, "heldIds"));
    bookedIds = List.copyOf(Objects.requireNonNull(bookedIds, "bookedIds"));
    refusals = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(refusals)));
  }
}
