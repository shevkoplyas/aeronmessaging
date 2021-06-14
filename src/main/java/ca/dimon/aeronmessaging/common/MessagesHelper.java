package ca.dimon.aeronmessaging.common;

import ca.dimon.aeronmessaging.common.send_message_exceptions.*;
import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Convenience functions to send messages.
 */
public final class MessagesHelper {

    // All classes of this project should use the same log4j logger (and never use System.out.print() for loggign):
    protected final Logger logger = LogManager.getLogger(this.getClass());
    // see discussion: https://stackoverflow.com/questions/7624895/how-to-use-log4j-with-multiple-classes
    // in particular this answer: https://stackoverflow.com/a/7625722/7022062

    // Let's make the constructor private, so no one can instantiate this class (all the useful methods are static).
    private MessagesHelper() {
    }

    /**
     * Send the given message to the given publication. If the publication fails
     * to accept the message, the method will retry {@code 5} times, waiting
     * {@code 100} milliseconds each time, before throwing an exception.
     *
     * @param publication The publication
     * @param buffer A buffer that will hold the message for sending
     * @param text The message
     *
     * @return The new publication stream position
     *
     * @throws IOException If the message cannot be sent
     */
    public static long send_message(
            final Publication publication,
            final UnsafeBuffer buffer,
            final String text)
            throws IOException {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(buffer, "buffer");
        Objects.requireNonNull(text, "text");

//    logger.trace("[{}] send: {}", Integer.toString(pub.sessionId()), text);
        final byte[] value = text.getBytes(UTF_8);
        buffer.putBytes(0, value);

        long result_code = 0L;
        // Note: when we shut down server, and the client is trying to "publication.offer()" then
        // 1st 2 results will be result_code=-2 (as if Publication.BACK_PRESSURED)
        // and only attepmt 3-4-5 will have result_code=-1 (as Publication.NOT_CONNECTED). So if
        // you give only 2 attempts here it won't work correctly (you won't see NOT_CONNECTED and your program
        // will keep re-enqueueing the message as if it was BACK_PRESSUR'ed, which is wrong). Tested this on Aeron ver. 1.8.2 and ver. 1.25.1
        // *** Update: *** actually it has nothing to do with number of tries, but rather timing. It takes Aeron to realize that
        // the connection to server is lost somewhere around:
        // 23:00:33.146 - started getting result_code=-2 (as if backbressured)
        // 23:00:36.351 - got 1st (well, and last, since exit on it) result_code=-1, which was 3 seconds later.
        // So it took client 3 seconds to realise -1 (NOT_CONNECTED)
        for (int index = 0; index < 5; ++index) {
            result_code = publication.offer(buffer, 0, value.length);

            // If failed to send the message, let's sleep a few milliseconds and try again.
            if (result_code < 0L) {
                try {
                    Thread.sleep(50L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            return result_code;
        }

//        // When connection to the server is lost the client will throw an exception:
//        // ClientIOException: java.io.IOException: Could not send message: Error code: Not connected
//        throw new IOException(
//                "Could not send message: Error code: " + error_code_to_string(result_code));

        // Let's throw different exceptions depending on the error code we got from publication.offer()
        if (result_code == Publication.NOT_CONNECTED) {
            throw new PublicationNotConnected();
        }
        if (result_code == Publication.ADMIN_ACTION) {
            throw new PublicationAdminAction();
        }
        if (result_code == Publication.BACK_PRESSURED) {
            throw new PublicationBackPressured();
        }
        if (result_code == Publication.CLOSED) {
            throw new PublicationClosed();
        }
        if (result_code == Publication.MAX_POSITION_EXCEEDED) {
            throw new PublicationMaxPositionExceeded();
        }
        throw new IllegalStateException();
    }

// Commented out due to: it is not good idea to throw a new IOException carring the reason as
// of "why sending message didn't work well" in form of human-readable String. Then all the other
// parts of the code where we catch this exception would have to parse String to distinguish between
// different error cases, and parsing string is not good (message can change in future, then we'd have
// to fix it everybloodywhere, so let's create separate exception classes - one per each case.
//
//    private static String error_code_to_string(final long result) {
//        if (result == Publication.NOT_CONNECTED) {
//            return "Not connected";
//        }
//        if (result == Publication.ADMIN_ACTION) {
//            return "Administrative action";
//        }
//        if (result == Publication.BACK_PRESSURED) {
//            return "Back pressured";
//        }
//        if (result == Publication.CLOSED) {
//            return "Publication is closed";
//        }
//        if (result == Publication.MAX_POSITION_EXCEEDED) {
//            return "Maximum term position exceeded";
//        }
//        throw new IllegalStateException();
//    }

    /**
     * Extract a UTF-8 encoded string from the given buffer.
     *
     * @param buffer The buffer
     * @param offset The offset from the start of the buffer
     * @param length The number of bytes to extract
     *
     * @return A string
     */
    public static String parse_message_utf8(
            final DirectBuffer buffer,
            final int offset,
            final int length) {
        Objects.requireNonNull(buffer, "buffer");
        final byte[] data = new byte[length];
        buffer.getBytes(offset, data);
        return new String(data, UTF_8);
    }
}
