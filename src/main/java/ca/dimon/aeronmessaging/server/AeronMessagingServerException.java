package ca.dimon.aeronmessaging.server;

import java.util.Objects;

/**
 * The type of exceptions raised by the server.
 */

public abstract class AeronMessagingServerException extends Exception
{
  /**
   * Create an exception.
   *
   * @param message The message
   */

  public AeronMessagingServerException(final String message)
  {
    super(Objects.requireNonNull(message, "message"));
  }

  /**
   * Create an exception.
   *
   * @param cause The cause
   */

  public AeronMessagingServerException(final Throwable cause)
  {
    super(Objects.requireNonNull(cause, "cause"));
  }
}
