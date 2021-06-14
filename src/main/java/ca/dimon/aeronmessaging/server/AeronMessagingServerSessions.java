package ca.dimon.aeronmessaging.server;

/**
 * Known session numbers.
 */

public final class AeronMessagingServerSessions
{
  /**
   * The inclusive lower bound of the reserved sessions range.
   */

  public static final int RESERVED_SESSION_ID_LOW = 1;

  /**
   * The inclusive upper bound of the reserved sessions range.
   */

  public static final int RESERVED_SESSION_ID_HIGH = 0x7FFFFFFF; // same as 2147483647, but much more readable ;)

  private AeronMessagingServerSessions()
  {

  }
}
