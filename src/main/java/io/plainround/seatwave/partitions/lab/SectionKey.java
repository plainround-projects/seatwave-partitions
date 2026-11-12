package io.plainround.seatwave.partitions.lab;

import io.plainround.seatwave.partitions.routing.ShardKeys;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One section of one event: the shard key of its seats, holds and bookings. Immutable.
 *
 * @param eventId the event, positive
 * @param sectionCode the section, one to four capital letters
 */
public record SectionKey(long eventId, String sectionCode) {

  private static final Pattern SECTION_CODE = Pattern.compile("[A-Z]{1,4}");

  /** Checks the event id and the section code, as Module 13's section_code_format does. */
  public SectionKey {
    Objects.requireNonNull(sectionCode, "sectionCode");
    if (eventId < 1 || !SECTION_CODE.matcher(sectionCode).matches()) {
      throw new IllegalArgumentException(
          "a section key needs a positive event id and a code of 1 to 4 capital letters: "
              + eventId
              + ", "
              + sectionCode);
    }
  }

  /** Returns the text placed on the ring: {@code event-<id>/section-<code>}. */
  public String shardKey() {
    return ShardKeys.section(eventId, sectionCode);
  }

  /**
   * Returns a seat label in this section.
   *
   * @param seatNumber the seat's number, from 1
   * @return for example {@code A-7}
   */
  public String seatLabel(int seatNumber) {
    return sectionCode + "-" + seatNumber;
  }
}
