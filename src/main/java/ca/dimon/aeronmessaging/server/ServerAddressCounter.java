package ca.dimon.aeronmessaging.server;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A counter for IP addresses.
 */

public final class ServerAddressCounter
{
  private final Map<InetAddress, Integer> counts;

  private ServerAddressCounter()
  {
    this.counts = new HashMap<>();
  }

  /**
   * Create a new counter.
   *
   * @return A new counter
   */

  public static ServerAddressCounter create()
  {
    return new ServerAddressCounter();
  }

  /**
   * @param address The IP address
   *
   * @return The current count for the given address
   */

  public int countFor(
    final InetAddress address)
  {
    Objects.requireNonNull(address, "address");

    if (this.counts.containsKey(address)) {
      return this.counts.get(address).intValue();
    }

    return 0;
  }

  /**
   * Increment the count for the given address.
   *
   * @param address The IP address
   *
   * @return The current count for the given address
   */

  public int increment(
    final InetAddress address)
  {
    Objects.requireNonNull(address, "address");

    if (this.counts.containsKey(address)) {
      final int next = this.counts.get(address).intValue() + 1;
      this.counts.put(address, Integer.valueOf(next));
      return next;
    }

    this.counts.put(address, Integer.valueOf(1));
    return 1;
  }

  /**
   * Decrement the count for the given address.
   *
   * @param address The IP address
   *
   * @return The current count for the given address
   */

  public int decrement(
    final InetAddress address)
  {
    Objects.requireNonNull(address, "address");

    if (this.counts.containsKey(address)) {
      final int next = this.counts.get(address).intValue() - 1;
      if (next <= 0) {
        this.counts.remove(address);
        return 0;
      }

      this.counts.put(address, Integer.valueOf(next));
      return next;
    }

    return 0;
  }
}
