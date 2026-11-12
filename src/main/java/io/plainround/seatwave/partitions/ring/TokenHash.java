package io.plainround.seatwave.partitions.ring;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Turns a key into a token: a position on the ring, the first eight bytes of the key's MD5 digest
 * read as one {@code long}.
 *
 * <p>MD5 is used for spread, not for security: every Java platform must provide it, it gives the
 * same token on every machine and every JDK version, and similar keys ("customer-41",
 * "customer-42") land far apart. {@link String#hashCode()} would be faster but has only 32 bits and
 * places similar strings close together. The function is part of the shard contract: changing it
 * moves almost every key, so it is chosen once. Thread-safe: it keeps no state.
 */
public final class TokenHash {

  private static final String DIGEST_ALGORITHM = "MD5";

  private TokenHash() {}

  /**
   * Returns the token of a key.
   *
   * @param key the shard key as text, for example {@code "event-1/section-A"}
   * @return the key's position on the ring, any {@code long} value
   * @throws NullPointerException if the key is null
   */
  public static long tokenOf(String key) {
    Objects.requireNonNull(key, "key");
    byte[] digest = newDigest().digest(key.getBytes(StandardCharsets.UTF_8));
    // Eight of the sixteen digest bytes are plenty: 2^64 positions for at most a few hundred
    // thousand ring points and billions of keys.
    return ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
  }

  private static MessageDigest newDigest() {
    try {
      // A new instance per call: MessageDigest is not thread-safe, and creating one is cheap.
      return MessageDigest.getInstance(DIGEST_ALGORITHM);
    } catch (NoSuchAlgorithmException missing) {
      throw new IllegalStateException("the Java platform must provide MD5", missing);
    }
  }
}
