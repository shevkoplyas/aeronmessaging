package ca.dimon.aeronmessaging.server;

import ca.dimon.aeronmessaging.common.MessagesHelper;
import ca.dimon.aeronmessaging.common.AeronChannelsHelper;
import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

/**
 * A conversation between the server and a single client.
 */
public final class AeronMessagingServerDuologue implements AutoCloseable {

    // All classes of this project should use the same log4j logger (and never use System.out.print() for loggign):
    protected final Logger logger = LogManager.getLogger(this.getClass());
    // see discussion: https://stackoverflow.com/questions/7624895/how-to-use-log4j-with-multiple-classes
    // in particular this answer: https://stackoverflow.com/a/7625722/7022062

    private final UnsafeBuffer send_buffer;
    private final AeronMessagingServerExecutorService exec;
    private final Instant initial_expire;
    private final InetAddress owner;
    private final int port_data;
    private final int port_control;
    private final int session;
    private final FragmentAssembler private_message_handler;  // process incoming "private" messages from the client.
    private boolean closed;
    private Publication private_publication;     // "private_" means "per individual connected client", not shared with others (liek "all_clients" pub/sub
    private Subscription private_subscription;   // same

    private final ConcurrentLinkedQueue<String> outgoing_messages_to_private_queue = new ConcurrentLinkedQueue();
    private final ConcurrentLinkedQueue<String> incoming_messages_from_private_queue = new ConcurrentLinkedQueue();

    private AeronMessagingServerDuologue(
            final AeronMessagingServerExecutorService in_exec,
            final Instant in_initial_expire,
            final InetAddress in_owner_address,
            final int in_session,
            final int in_port_data,
            final int in_port_control) {
        this.exec
                = Objects.requireNonNull(in_exec, "executor");
        this.initial_expire
                = Objects.requireNonNull(in_initial_expire, "initial_expire");
        this.owner
                = Objects.requireNonNull(in_owner_address, "owner");

        this.send_buffer
                = new UnsafeBuffer(BufferUtil.allocateDirectAligned(1024, 16));

        this.session = in_session;
        this.port_data = in_port_data;
        this.port_control = in_port_control;
        this.closed = false;

        this.private_message_handler = new FragmentAssembler((data, offset, length, header) -> {
            try {
                this.on_private_message_received(data, offset, length, header);
            } catch (final IOException ex) {
                logger.error("FragmentAssembler failed to process incoming (client -> server) message. Exception details:", ex);
                this.close();
            }
        });
    }

    /**
     * Create a new duologue. This will create a new publication and
     * subscription pair using a specific session ID and intended only for a
     * single client at a given address.
     *
     * @param aeron The Aeron instance
     * @param clock A clock used for time-related operations
     * @param exec An executor
     * @param local_address The local address of the server ports
     * @param owner_address The address of the client
     * @param session The session ID
     * @param port_data The data port
     * @param port_control The control port
     *
     * @return A new duologue
     */
    public static AeronMessagingServerDuologue create(
            final Aeron aeron,
            final Clock clock,
            final AeronMessagingServerExecutorService exec,
            final InetAddress local_address,
            final InetAddress owner_address,
            final int session,
            final int port_data,
            final int port_control) {
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(exec, "exec");
        Objects.requireNonNull(local_address, "local_address");
        Objects.requireNonNull(owner_address, "owner_address");

//        logger.debug(
//                "creating new duologue at {} ({},{}) session {} for {}",
//                local_address,
//                Integer.valueOf(port_data),
//                Integer.valueOf(port_control),
//                Integer.toString(session),
//                owner_address);

        final Instant initial_expire
                = clock.instant().plus(10L, ChronoUnit.SECONDS);

        final ConcurrentPublication pub
                = AeronChannelsHelper.createPublicationDynamicMDCWithSession(aeron,
                        local_address,
                        port_control,
                        AeronMessagingServer.MAIN_STREAM_ID,
                        session);

        try {
            final AeronMessagingServerDuologue duologue
                    = new AeronMessagingServerDuologue(
                            exec,
                            initial_expire,
                            owner_address,
                            session,
                            port_data,
                            port_control);

            final Subscription sub
                    = AeronChannelsHelper.createSubscriptionWithHandlersAndSession(aeron,
                            local_address,
                            port_data,
                            AeronMessagingServer.MAIN_STREAM_ID,
                            duologue::onClientConnected,
                            duologue::onClientDisconnected,
                            session);

            duologue.setPublicationSubscription(pub, sub);
            return duologue;
        } catch (final Exception e) {
            try {
                pub.close();
            } catch (final Exception pe) {
                e.addSuppressed(pe);
            }
            throw e;
        }
    }

    /**
     * Poll the duologue for activity.The handler will pass received message to
     * on_private_message_received(), which will add message into the incoming_messages_from_private_queue.
     *
     * @return
     */
    public int poll() {
        this.exec.assertIsExecutorThread();
        return this.private_subscription.poll(this.private_message_handler, 10);
    }

    /**
     * This method actually sends a "private" message to the client (the message
     * will not be seen by other clients, it will go through "per client"
     * publication channel dedicated to only this one particular client).
     *
     * @param message
     * @throws IOException
     */
    public boolean send_private_message_to_client(
            String message)
            throws IOException {

        if (private_publication.isConnected()) {
            MessagesHelper.send_message(
                    this.private_publication,
                    this.send_buffer,
                    message);
            return true;
        }
        return false;
    }

    /**
     * Simply add given message into the outgoing message queue (private
     * channel).
     *
     * @param message
     * @return
     * @throws IOException
     */
    public boolean enqueue_private_message_to_client(
            String message)
            throws IOException {

        if (private_publication.isConnected()) {
            this.outgoing_messages_to_private_queue.add(message);
            return true;
        }
        return false;
    }

    /**
     * Simply dequeue 1 message from given connected client.
     * 
     * @return 
     */
    public String get_private_message() {
        if (incoming_messages_from_private_queue.isEmpty()) {
            return null;
        }
        return incoming_messages_from_private_queue.poll();
    }
    
    /**
     * Poll the duologue for activity.The handler will pass received message to
     * on_private_message_received()
     *
     * @return
     */
    public int send_enqueued_messages()
            throws IOException {

        this.exec.assertIsExecutorThread();

        if (!private_publication.isConnected() || outgoing_messages_to_private_queue.isEmpty()) {
            return 0;
        }

        String outgoing_message = outgoing_messages_to_private_queue.poll();
        if (send_private_message_to_client(outgoing_message)) {
            return 1;
        } else {
            return 0;
        }
    }

    private void on_private_message_received( // from_client
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
            throws IOException {
        this.exec.assertIsExecutorThread();

//        final String session_name
//                = Integer.toString(header.sessionId());
        final String message
                = MessagesHelper.parse_message_utf8(buffer, offset, length);

        // Simply place incoming private message into the duologue.incoming_messages_from_private_queue
        incoming_messages_from_private_queue.add(message);

//        logger.debug("[{}] received private message from client: {}", session_name, message);
    }

    private void setPublicationSubscription(
            final Publication in_publication,
            final Subscription in_subscription) {
        this.private_publication
                = Objects.requireNonNull(in_publication, "Publication");
        this.private_subscription
                = Objects.requireNonNull(in_subscription, "Subscription");
    }

    private void onClientDisconnected(
            final Image image) {
        this.exec.my_call(() -> {
            final int image_session = image.sessionId();
            final String session_name = Integer.toString(image_session);
            final InetAddress address = IPAddressesHelper.extractAddress(image.sourceIdentity());

            if (this.private_subscription.imageCount() == 0) {
                logger.debug("[{}] last client ({}) disconnected", session_name, address);
                this.close();
            } else {
                logger.debug("[{}] client {} disconnected", session_name, address);
            }
            return 0; // this value does not really matter, we'll use Callable<Integer> to return number of received fragments and decide if we need to sleep or not in the main loop.
        });
    }

    private void onClientConnected(
            final Image image) {
        this.exec.my_call(() -> {
            final InetAddress remote_address
                    = IPAddressesHelper.extractAddress(image.sourceIdentity());

            if (Objects.equals(remote_address, this.owner)) {
                logger.debug("[{}] client with correct IP connected",
                        Integer.toString(image.sessionId()));
            } else {
                logger.error("connecting client has wrong address: {}",
                        remote_address);
            }
            return 0; // this value does not really matter, we'll use Callable<Integer> to return number of received fragments and decide if we need to sleep or not in the main loop.
        });
    }

    /**
     * @param now The current time
     *
     * @return {@code true} if this duologue has no subscribers and the current
     * time {@code now} is after the intended expiry date of the duologue
     */
    public boolean isExpired(
            final Instant now) {
        Objects.requireNonNull(now, "now");

        this.exec.assertIsExecutorThread();

        return this.private_subscription.imageCount() == 0
                && now.isAfter(this.initial_expire);
    }

    /**
     * @return {@code true} iff {@link #close()} has been called
     */
    public boolean isClosed() {
        this.exec.assertIsExecutorThread();

        return this.closed;
    }

    @Override
    public void close() {
        this.exec.assertIsExecutorThread();

        if (!this.closed) {
            try {
                try {
                    this.private_publication.close();
                } finally {
                    this.private_subscription.close();
                }
            } finally {
                this.closed = true;
            }
        }
    }

    /**
     * @return The data port
     */
    public int portData() {
        return this.port_data;
    }

    /**
     * @return The control port
     */
    public int portControl() {
        return this.port_control;
    }

    /**
     * @return The IP address that is permitted to participate in this duologue
     */
    public InetAddress ownerAddress() {
        return this.owner;
    }

    /**
     * @return The session ID of the duologue
     */
    public int session() {
        return this.session;
    }
}
