package ca.dimon.aeronmessaging.client;

import org.immutables.value.Value;

import java.net.InetAddress;
import java.nio.file.Path;

/**
 * Configuration values for the client.
 */

@Value.Immutable
public interface AeronMessagingClientConfiguration
{
  /**
   * @return The base directory that will be used for the client; should be unique for each client instance
   */

  @Value.Parameter
  Path baseDirectory();

  /**
   * @return The address of the server
   */

  @Value.Parameter
  InetAddress remoteAddress();

  /**
   * @return The server's data port
   */

  @Value.Parameter
  int remoteInitialPort();

  /**
   * @return The server's control port
   */

  @Value.Parameter
  int remoteInitialControlPort();
}
