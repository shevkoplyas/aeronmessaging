package ca.dimon.aeronmessaging.server;

import ca.dimon.aeronmessaging.server.AeronMessagingServerException;

/**
 * A port could not be allocated.
 */

public final class ServerPortAllocationException extends AeronMessagingServerException
{
  /**
   * Create an exception.
   *
   * @param message The message
   */

  public ServerPortAllocationException(
    final String message)
  {
    super(message);
  }
}
