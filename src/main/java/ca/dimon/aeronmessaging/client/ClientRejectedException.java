package ca.dimon.aeronmessaging.client;

/**
 * The server rejected this client when it tried to connect.
 */

public final class ClientRejectedException extends ClientException
{
  /**
   * Create an exception.
   *
   * @param message The message
   */

  public ClientRejectedException(final String message)
  {
    super(message);
  }
}
