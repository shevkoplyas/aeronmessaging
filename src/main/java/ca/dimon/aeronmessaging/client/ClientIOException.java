package ca.dimon.aeronmessaging.client;

import java.io.IOException;

public final class ClientIOException extends ClientException
{
  public ClientIOException(final IOException cause)
  {
    super(cause);
  }
}
