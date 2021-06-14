package ca.dimon.aeronmessaging.server;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;

import java.util.Collections;

/**
 * <p>An allocator for port numbers.</p>
 *
 * <p>
 * The allocator accepts a base number {@code p} and a maximum count {@code n | n > 0}, and will allocate
 * up to {@code n} numbers, in a random order, in the range {@code [p, p + n - 1}.
 * </p>
 */

public final class ServerPortAllocator
{
  private final int port_lo;
  private final int port_hi;
  private final IntHashSet ports_used;
  private final IntArrayList ports_free;

  /**
   * Create a new port allocator.
   *
   * @param port_base The base port
   * @param max_ports The maximum number of ports that will be allocated
   *
   * @return A new port allocator
   */

  public static ServerPortAllocator create(
    final int port_base,
    final int max_ports)
  {
    return new ServerPortAllocator(port_base, max_ports);
  }

  private ServerPortAllocator(
    final int in_port_lo,
    final int in_max_ports)
  {
    if (in_port_lo <= 0 || in_port_lo >= 65536) {
      throw new IllegalArgumentException(
        String.format(
          "Base port %d must be in the range [1, 65535]",
          Integer.valueOf(in_port_lo)));
    }

    this.port_lo = in_port_lo;
    this.port_hi = in_port_lo + (in_max_ports - 1);

    if (this.port_hi < 0 || this.port_hi >= 65536) {
      throw new IllegalArgumentException(
        String.format(
          "Uppermost port %d must be in the range [1, 65535]",
          Integer.valueOf(this.port_hi)));
    }

    this.ports_used = new IntHashSet(in_max_ports);
    this.ports_free = new IntArrayList();

    for (int port = in_port_lo; port <= this.port_hi; ++port) {
      this.ports_free.addInt(port);
    }
    Collections.shuffle(this.ports_free);
  }

  /**
   * Free a given port. Has no effect if the given port is outside of the range
   * considered by the allocator.
   *
   * @param port The port
   */

  public void free(
    final int port)
  {
    if (port >= this.port_lo && port <= this.port_hi) {
      this.ports_used.remove(port);
      this.ports_free.addInt(port);
    }
  }

  /**
   * Allocate {@code count} ports.
   *
   * @param count The number of ports that will be allocated
   *
   * @return An array of allocated ports
   *
   * @throws ServerPortAllocationException If there are fewer than {@code count} ports available to allocate
   */

  public int[] allocate(
    final int count)
    throws ServerPortAllocationException
  {
    if (this.ports_free.size() < count) {
      throw new ServerPortAllocationException(
        String.format(
          "Too few ports available to allocate %d ports",
          Integer.valueOf(count)));
    }

    final int[] result = new int[count];
    for (int index = 0; index < count; ++index) {
      result[index] = this.ports_free.remove(0).intValue();
      this.ports_used.add(result[index]);
    }

    return result;
  }
}
