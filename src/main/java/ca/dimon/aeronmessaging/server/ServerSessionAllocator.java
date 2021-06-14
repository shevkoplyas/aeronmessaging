package ca.dimon.aeronmessaging.server;

import org.agrona.collections.IntHashSet;

import java.security.SecureRandom;
import java.util.Objects;

/**
 * <p>
 * An allocator for session IDs. The allocator randomly selects values from
 * the given range {@code [min, max]} and will not return a previously-returned value {@code x}
 * until {@code x} has been freed with {@code {@link ServerSessionAllocator#free(int)}.
 * </p>
 *
 * <p>
 * This implementation uses storage proportional to the number of currently-allocated
 * values. Allocation time is bounded by {@code max - min}, will be {@code O(1)}
 * with no allocated values, and will increase to {@code O(n)} as the number
 * of allocated values approached {@code max - min}.
 * </p>
 */

public final class ServerSessionAllocator
{
  private final IntHashSet used;
  private final SecureRandom random;
  private final int min;
  private final int max_count;

  private ServerSessionAllocator(
    final int in_min,
    final int in_max,
    final SecureRandom in_random)
  {
    if (in_max < in_min) {
      throw new IllegalArgumentException(
        String.format(
          "Maximum value %d must be >= minimum value %d",
          Integer.valueOf(in_max),
          Integer.valueOf(in_min)));
    }

    this.used = new IntHashSet();
    this.min = in_min;
    this.max_count = Math.max(in_max - in_min, 1);
    this.random = Objects.requireNonNull(in_random, "random");
  }

  /**
   * Create a new session allocator.
   *
   * @param in_min    The minimum session ID (inclusive)
   * @param in_max    The maximum session ID (exclusive)
   * @param in_random A random number generator
   *
   * @return A new allocator
   */

  public static ServerSessionAllocator create(
    final int in_min,
    final int in_max,
    final SecureRandom in_random)
  {
    return new ServerSessionAllocator(in_min, in_max, in_random);
  }

  /**
   * Allocate a new session.
   *
   * @return A new session ID
   *
   * @throws ServerSessionAllocationException If there are no non-allocated sessions left
   */

  public int allocate()
    throws ServerSessionAllocationException
  {
    if (this.used.size() == this.max_count) {
      throw new ServerSessionAllocationException(
        "No session IDs left to allocate");
    }

    for (int index = 0; index < this.max_count; ++index) {
      final int session = this.random.nextInt(this.max_count) + this.min;
      if (!this.used.contains(session)) {
        this.used.add(session);
        return session;
      }
    }

    throw new ServerSessionAllocationException(
      String.format(
        "Unable to allocate a session ID after %d attempts (%d values in use)",
        Integer.valueOf(this.max_count),
        Integer.valueOf(this.used.size())
      )
    );
  }

  /**
   * Free a session. After this method returns, {@code session} becomes eligible
   * for allocation by future calls to {@link #allocate()}.
   *
   * @param session The session to free
   */

  public void free(final int session)
  {
    this.used.remove(session);
  }
}
