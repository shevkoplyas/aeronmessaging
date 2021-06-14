package ca.dimon.aeronmessaging.server;

import ca.dimon.aeronmessaging.server.AeronMessagingServerException;

/**
 * A session could not be allocated.
 */

public final class ServerSessionAllocationException
  extends AeronMessagingServerException
{
  /**
   * Create an exception.
   *
   * @param message The message
   */

  public ServerSessionAllocationException(
    final String message)
  {
    super(message);
  }
}
