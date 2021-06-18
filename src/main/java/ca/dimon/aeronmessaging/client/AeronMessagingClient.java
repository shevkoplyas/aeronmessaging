package ca.dimon.aeronmessaging.client;

import ca.dimon.aeronmessaging.server.AeronMessagingServerSessions;
import ca.dimon.aeronmessaging.client.ImmutableAeronMessagingClientConfiguration;
import ca.dimon.aeronmessaging.common.MessagesHelper;
import ca.dimon.aeronmessaging.common.AeronChannelsHelper;
import ca.dimon.aeronmessaging.common.send_message_exceptions.PublicationAdminAction;
import ca.dimon.aeronmessaging.common.send_message_exceptions.PublicationBackPressured;
import ca.dimon.aeronmessaging.common.send_message_exceptions.PublicationClosed;
import ca.dimon.aeronmessaging.common.send_message_exceptions.PublicationMaxPositionExceeded;
import ca.dimon.aeronmessaging.common.send_message_exceptions.PublicationNotConnected;
import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.State;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A mindlessly simple Echo client. Found here:
 * http://www.io7m.com/documents/aeron-guide/#client_server_take_2
 */
public final class AeronMessagingClient implements Closeable, Runnable {

    // All classes of this project should use the same log4j logger (and never use System.out.print() for loggign):
    protected final Logger logger = LogManager.getLogger(this.getClass());
    // see discussion: https://stackoverflow.com/questions/7624895/how-to-use-log4j-with-multiple-classes
    // in particular this answer: https://stackoverflow.com/a/7625722/7022062

    // We also specify a stream ID when creating the subscription. Aeron is capable of
    // multiplexing several independent streams of messages into a single connection.
    private static final int MAIN_STREAM_ID = 0x100500ff; // must be the save value on client and server

    private static final Pattern PATTERN_ERROR
            = Pattern.compile("^ERROR (.*)$");
    private static final Pattern PATTERN_CONNECT
            = Pattern.compile("^CONNECT ([0-9]+) ([0-9]+) ([0-9A-F]+)$");

    private final MediaDriver media_driver;
    private final Aeron aeron;
    private final AeronMessagingClientConfiguration configuration;
    private final SecureRandom random;
    private volatile int remote_data_port;
    private volatile int remote_control_port;
    private volatile boolean remote_ports_received;
    private volatile boolean failed;
    private volatile int remote_session;
    private volatile int duologue_key;

    // Since now we're in Runnable and in own thread, let's catch & preserve the exceptino we got and simply return.
    // There are some ways to throw an exception from runnable, but I didn't like them: https://stackoverflow.com/questions/11584159/is-there-a-way-to-make-runnables-run-throw-an-exception
    public Exception got_exception = null;

    // We have 4 queues: 2 outgoing queues (private and broadcast) and 2 corresponding incoming queues.
    // See details on Queue:
    // https://docs.oracle.com/en/java/javase/16/docs/api/java.base/java/util/Queue.html
    // In particular ConcurrentLinkedDeque:
    // https://docs.oracle.com/en/java/javase/16/docs/api/java.base/java/util/concurrent/ConcurrentLinkedDeque.html
    //
    private final ConcurrentLinkedDeque<String> outgoing_messages_to_all_clients_queue = new ConcurrentLinkedDeque();
    private final ConcurrentLinkedDeque<String> outgoing_messages_to_private_queue = new ConcurrentLinkedDeque();
    private final ConcurrentLinkedDeque<String> incoming_messages_from_all_clients_queue = new ConcurrentLinkedDeque();
    private final ConcurrentLinkedDeque<String> incoming_messages_from_private_queue = new ConcurrentLinkedDeque();

    private AeronMessagingClient(
            final MediaDriver in_media_driver,
            final Aeron in_aeron,
            final AeronMessagingClientConfiguration in_configuration) {
        logger.debug("AeronMessagingClient constructor (debug stream)");
        logger.error("AeronMessagingClient constructor (error stream)");
        this.media_driver = Objects.requireNonNull(in_media_driver, "media_driver");
        this.aeron = Objects.requireNonNull(in_aeron, "aeron");
        this.configuration = Objects.requireNonNull(in_configuration, "configuration");
        this.random = new SecureRandom();
    }

    //////////////////////////////// public methods to send a message to the server (enqueue messages to be sent to the server) //////////////////////
    // simply synonym / alias  for "send_private(message)"
    public void send_message(String message) {
        send_private_message(message);
    }

    public void send_private_message(String message) {
        this.outgoing_messages_to_private_queue.add(message);
    }

    // "control" aka "broadcast" or "all-clients" channel
    public void send_all_clients_control_channel_message(String message) {
        this.outgoing_messages_to_all_clients_queue.add(message);
    }

    //////////////////////////////// public methods to get a message to the server (dequeue already received messages) //////////////////////
    /**
     * Simply synonym / alias for "get_private_message()"
     *
     * @return message from corresponding queue or null if no messages in queue
     */
    public String get_message() {
        return get_private_message();
    }

    /**
     * Getter f-n to retrieve an item (String) from the
     * incoming_messages_from_private_queue.
     *
     * @return an item (String) or null if queue is empty.
     */
    public String get_private_message() {
        if (!incoming_messages_from_private_queue.isEmpty()) {
            return incoming_messages_from_private_queue.poll();
        }
        return null;
    }

    /**
     * Getter f-n to retrieve an item (String) from the
     * incoming_messages_from_all_clients_queue.
     *
     * @return an item (String) or null if queue is empty.
     */
    public String get_all_clients_control_channel_message() {
        if (!incoming_messages_from_all_clients_queue.isEmpty()) {
            return incoming_messages_from_all_clients_queue.poll();
        }
        return null;
    }

    /**
     * Create a new client.
     *
     * @param configuration The client configuration data
     *
     * @return A new client
     *
     * @throws ClientCreationException On any initialization error
     */
    public static AeronMessagingClient create(
            final AeronMessagingClientConfiguration configuration)
            throws ClientException {
        Objects.requireNonNull(configuration, "configuration");

        final String directory
                = configuration.baseDirectory()
                        .toAbsolutePath()
                        .toString();

        final MediaDriver.Context media_context
                = new MediaDriver.Context()
                        .dirDeleteOnStart(true)
                        .publicationReservedSessionIdLow(AeronMessagingServerSessions.RESERVED_SESSION_ID_LOW)
                        .publicationReservedSessionIdHigh(AeronMessagingServerSessions.RESERVED_SESSION_ID_HIGH)
                        .aeronDirectoryName(directory);

        final Aeron.Context aeron_context
                = new Aeron.Context().aeronDirectoryName(directory);

        MediaDriver media_driver = null;

        try {
            media_driver = MediaDriver.launch(media_context);

            Aeron aeron = null;
            try {
                aeron = Aeron.connect(aeron_context);
            } catch (final Exception e) {
                closeIfNotNull(aeron);
                throw e;
            }

            return new AeronMessagingClient(media_driver, aeron, configuration);
        } catch (final Exception e) {
            try {
                closeIfNotNull(media_driver);
            } catch (final Exception c_ex) {
                e.addSuppressed(c_ex);
            }
            throw new ClientCreationException(e);
        }
    }

    private static void closeIfNotNull(
            final AutoCloseable closeable)
            throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    /**
     * Let's move aeron_messaging_server_thread into the class members since
     * we'll need the reference to that thread if/when server.close() is called
     * (for example we're shutting down the framework and need to send .close()
     * to all it's parts/components).
     *
     */
    public Thread aeron_messaging_client_thread = null;

    /**
     * Let's say client is_alive == true only when its messaging thread
     * is_alive.
     *
     * @return
     */
    public boolean is_alive() {
        return aeron_messaging_client_thread != null && aeron_messaging_client_thread.isAlive();
    }

    /**
     * The main() f-n is a simple example on how to instantiate and run the
     * AeronMessagingClient class in its own thread. It is not an "entry point"
     * of the class and is here only for the demo purposes. The actual "entry
     * point" is the run() method, which will be engaged when you call
     * thread.start().
     *
     * @param args Command-line arguments
     *
     * @throws Exception On any error
     */
    public static void main(
            final String[] args)
            throws Exception {
        if (args.length < 4) {
            System.err.println("Error: expected 4 arguments, got only " + args.length
                    + ". Usage: directory remote-address remote-data-port remote-control-port");
            System.exit(1);
        }

        final Path directory = Paths.get(args[0]);
        final InetAddress remote_address = InetAddress.getByName(args[1]);
        final int remote_data_port = Integer.parseUnsignedInt(args[2]);
        final int remote_control_port = Integer.parseUnsignedInt(args[3]);

        final ImmutableAeronMessagingClientConfiguration aeron_client_configuration
                = ImmutableAeronMessagingClientConfiguration.builder()
                        .baseDirectory(directory)
                        .remoteAddress(remote_address)
                        .remoteInitialControlPort(remote_control_port)
                        .remoteInitialPort(remote_data_port)
                        .build();

        // Keep trying to connect to the server "forever" with attempts done every ~5 seconds
        while (true) {

//            logger.debug("Creating aeron messaging client...");
            // Start aeron_messaging_client in it's own thread, we'll use it's public methods to enqueue (send) / dequeue (receive) messages
            final AeronMessagingClient aeron_messaging_client = create(aeron_client_configuration);
            aeron_messaging_client.logger.debug("AeronMessagingClient created, starting its main thread...");
            Thread aeron_messaging_thread = new Thread(aeron_messaging_client);
            aeron_messaging_thread.start();

//            logger.debug("Main thread keep chaga away while AeronMessagingClient is working in it's own separate thread...");
            //
            // Main consumer loop (it uses aeron_messaging_client to send/receive messages from/to the server)
            // This loop is running on the "main thread", which shovels/sends messages from/to aeron_messaging_client instance.
            //
            while (true) {
                String incoming_private_message = aeron_messaging_client.get_private_message();
                String incoming_public_message = aeron_messaging_client.get_all_clients_control_channel_message();

                // Stresstest: client keep sending messages to server from it's "main loop" by engaging aeron_messaging_client.send_private_message(msg) method.
                //
                // Define how many messages to inject into the queue in 1 iteration. Note: we have ~1000 iterations / sec due to 1ms sleep.
                int how_many_messages_to_send = 1; // slowest: just ~1K messages / second just for the demo purposes
//            int how_many_messages_to_send = 300; // ~100K messages per second: does not fly.. crushes something in Aeron driver
//            int how_many_messages_to_send = 200; // ~200K messages per second: starts then slip down to ~170-180K messages / second throughput, then network UDP shows spikes with pauses (instead of steady flat UDP bps/pps lines)
//                int how_many_messages_to_send = 160; // ~150K messages per second: works great for hours, stable (20Kpps, 24.5MiBps = 196mbps shown by "bmon" v.4.0)
//            int how_many_messages_to_send = 125; // ~120K messages per second: works great for hours, stable (16Kpps, 19MiBps = 152mbps shown by "bmon" v.4.0)
//                int how_many_messages_to_send = 110; // ~100K messages per second: works great > 7 hours straight, no errors, no losses, bps/pps on "lo" i-face are perfect flat lines

                // Enqueue N messages to be sent via the "private per client" channel to the server.
                for (int i = 0; i < how_many_messages_to_send; i++) {
                    aeron_messaging_client.send_private_message("Some 100-byte-long message goes here...askjdfl;asjkfsl;akjdfsl;ak jfsla;kjfsla;kf asl;d fjk 100 byte");
                }

                // If there were no incoming messages (no via "all-clients" nor via "private per client" channel), then sleep 1ms
                if (incoming_private_message == null && incoming_public_message == null) {
                    Thread.sleep(1000); // sleep 1s between messages for simple demo
//                    Thread.sleep(1);  // almost no sleep just to play with some "stress test" modes

                    // Main thread checks periodically if messaging thread is still runnig.
                    // Check if aeron_messaging_thread is still running
                    // [Q] Shall we use aeron_messaging_thread.getState()?
                    //       State thread_state = aeron_messaging_thread.getState(); //      * This method is designed for use in monitoring of the system state, not for synchronization control.
                    // [A] No, the thread State (from java.lang.Thread.State) can be in one of many "good states", for example:
                    //       - TIMED_WAITING (if thread is in sleep() state or called wait() on something
                    //       - RUNNABLE etc.
                    //
                    // Let's simply check if the thread is alive:
                    if (!aeron_messaging_thread.isAlive()) { //      * Tests if this thread is alive. A thread is alive if it has been started and has not yet died.
                        break;
                    }
                }

                // TODO: we might also want to pring "main consumer loop" stats here as well...
            }
//            logger.debug("AeronMessagingClient.main(): main loop is complete (it should be blocking 'forever'.. have we lost the connection to the server?.");
            // Need to close Aeron and MediaDriver, otherwise the program will not terminate.
            aeron_messaging_client.close();

//            logger.debug("Sleeping ~5sec before trying to connect to the server again...");
            Thread.sleep(5 * 1000);
        }
    }

    /**
     * Run the client, returning when the client is finished.
     *
     */
    @Override
    public void run() {
        /**
         * Generate a one-time pad.
         */

        this.duologue_key = this.random.nextInt();

        final UnsafeBuffer buffer
                = new UnsafeBuffer(BufferUtil.allocateDirectAligned(1024, 16));

        final String session_name;
        Subscription all_client_subscription = null;
        Publication all_client_publication = null;

// Commented out "try-with-resources" approach since this imply the "resource" to be closed after try block finishes,
// but we want to maintain "all_client" sub and pub!
//        try (all_client_subscription = this.setupAllClientsSubscription()) {
//            try (all_client_publication = this.setupAllClientsPublication()) {
        try {
            all_client_subscription = this.setupAllClientsSubscription();
            all_client_publication = this.setupAllClientsPublication();

            /**
             * Send a one-time pad to the server.
             */
            MessagesHelper.send_message(
                    all_client_publication,
                    buffer,
                    "HELLO " + Integer.toUnsignedString(this.duologue_key, 16).toUpperCase());

            session_name = Integer.toString(all_client_publication.sessionId());
            this.waitForConnectResponse(all_client_subscription, session_name);
        } catch (final ClientException | IOException e) {
            // Since now we're in Runnable and in own thread, let's catch & preserve the exceptino we got and simply return.
            // There are some ways to throw an exception from runnable, but I didn't like them: https://stackoverflow.com/questions/11584159/is-there-a-way-to-make-runnables-run-throw-an-exception
            this.got_exception = e;
            logger.error("got_exception: " + e);
            return; // this closes the AeronMessagingClient thread (end of run())
        }

        /**
         * Connect to the publication and subscription that the server has sent
         * back to this client.
         */
        try (final Subscription private_subscription = this.setupConnectSubscription()) {
            try (final Publication private_publication = this.setupConnectPublication()) {
                this.runMainMessageProcessingLoop(
                        buffer,
                        session_name,
                        private_subscription,
                        private_publication,
                        all_client_subscription,
                        all_client_publication
                );

            } catch (final IOException e) {
                // Since now we're in Runnable and in own thread, let's catch & preserve the exceptino we got and simply return.
                // There are some ways to throw an exception from runnable, but I didn't like them: https://stackoverflow.com/questions/11584159/is-there-a-way-to-make-runnables-run-throw-an-exception
                this.got_exception = e;
                logger.error("got_exception: " + e);
            }
        } catch (final Exception e) {
            // Since now we're in Runnable and in own thread, let's catch & preserve the exceptino we got and simply return.
            // There are some ways to throw an exception from runnable, but I didn't like them: https://stackoverflow.com/questions/11584159/is-there-a-way-to-make-runnables-run-throw-an-exception
            this.got_exception = e;
            logger.error("got_exception: " + e);
        }
    }

    private void runMainMessageProcessingLoop(
            final UnsafeBuffer tmp_send_buffer,
            final String session_name,
            final Subscription private_subscription,
            final Publication private_publication,
            final Subscription all_clients_subscription,
            final Publication all_clients_publication
    )
            throws IOException {
        final FragmentHandler private_subscription_message_handler
                = new FragmentAssembler(
                        (data, offset, length, header)
                        -> on_private_message_received(session_name, data, offset, length));

        final FragmentHandler all_client_subscription_message_handler
                = new FragmentAssembler(
                        (data, offset, length, header)
                        -> on_all_clients_message_received(session_name, data, offset, length));

        // 
        // Main loop (client side, inside aeron_messaging_thread):
        //   - polling messages from subscriptions (both "all-client" subscription and own private "per agent" subscription)
        //   - sending outbound messages by shovelling broadcast_messages_to_all_clients_queue
        //   - every 10 sec send PUBLISH message via all_clients_publication with server stats
        //
        Clock clock = Clock.systemUTC();
        long total_messages_sent_count = 0;
        long total_messages_sent_size = 0;
        long polled_fragmetns_total_count = 0;
        long failed_to_send_backpressured_count = 0;
        Instant main_loop_start_instant = clock.instant();
        long main_loop_start_epoch_ms = main_loop_start_instant.toEpochMilli();
        long main_loop_iterations_count = 0;
        long stats_report_count = 0;
        long last_stats_sent_epoch_ms = 0; // keep track of exact ms when report was generated to avoid sending it multiple times (loop is fast)

        while (true) {
            main_loop_iterations_count++;

            // Get current time in 2 forms: Instant and epoch_ms (one will be used for human-readable time, other for "robots":)
            Instant now_instant = clock.instant();
            long now_epoch_ms = now_instant.toEpochMilli();

            // Try to receive (poll) messages from all subscriptions
            // 1-of-2) incoming: We have 1 private "per client" subscription (server sends messages to only this client)
            int fragments_received = private_subscription.poll(private_subscription_message_handler, 100);

            // 2-of-2) incoming: We have "all client" subscription, which sends the same things to all connected clients.
            fragments_received += all_clients_subscription.poll(all_client_subscription_message_handler, 100);

            // Check 2 outbound queues:  1-of-2) outgoing_messages_to_all_clients_queue
            int current_iteration_messages_sent_count = 0;
            if (!outgoing_messages_to_all_clients_queue.isEmpty()) {
                String outgoing_message = outgoing_messages_to_all_clients_queue.poll();   // Queue.poll() - Retrieves and removes the head of this queue, or returns null if this queue is empty.

                // Try to send the message.
                try {
                    // This might throw: java.io.IOException: Could not send message: Error code: Back pressured
                    MessagesHelper.send_message(
                            all_clients_publication,
                            tmp_send_buffer,
                            outgoing_message);

                    // Successfully sent message, increase some stats
                    total_messages_sent_count++;
                    total_messages_sent_size += outgoing_message.length();
                    current_iteration_messages_sent_count++;

                    // We should distinguish different types of exceptions thrown by MessagesHelper.send_message().
                    // If we backpressured we need to backdown and re-enque the message along with failed_to_send_backpressured_count++;
                    // if we got: java.io.IOException: Could not send message: Error code: Not connected!
                    // Tthen the connection to the server is gone (for example the server was shut down).
                    // Simply ignoring and waiting for the server to show up again will not work because after the server restart all the
                    // clients should re-connect again (go through HELLO nnn  => CONNECT mmm, xxx, yyy => ...)
                    // So "Not connected!" is a "game over" and we should quit the main loop to close AeronMessagingClient thread now.
                    // Let's investigate different types of exceptions we get here:
                    //
                } catch (PublicationNotConnected ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationNotConnected. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (PublicationAdminAction ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationAdminAction. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (PublicationBackPressured ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationBackPressured.");
                    // Failed to send the message due to backbressure, put it back into the front of the queue
                    outgoing_messages_to_all_clients_queue.addFirst(outgoing_message);

                    // Increase "failed_to_send_count" stats and continue
                    failed_to_send_backpressured_count++;

                } catch (PublicationClosed ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationClosed. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (PublicationMaxPositionExceeded ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationMaxPositionExceeded. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (IOException ex) {
                    // We're in some IllegalStateException() state.
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): We're in some IllegalStateException() state. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server. Exception was: " + ex);
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.
                }
            }

            // Check 2 outbound queues:  2-of-2) outgoing_messages_to_private_queue
            if (!outgoing_messages_to_private_queue.isEmpty()) {
                String outgoing_message = outgoing_messages_to_private_queue.poll();   // Queue.poll() - Retrieves and removes the head of this queue, or returns null if this queue is empty.

                try {
                    // Try to send the message.
                    // This might throw: java.io.IOException: Could not send message: Error code: Back pressured
                    MessagesHelper.send_message(
                            private_publication,
                            tmp_send_buffer,
                            outgoing_message);

                    // Successfully sent message, increase some stats
                    total_messages_sent_count++;
                    total_messages_sent_size += outgoing_message.length();
                    current_iteration_messages_sent_count++;

                    // We should distinguish different types of exceptions thrown by MessagesHelper.send_message().
                    // If we backpressured we need to backdown and re-enque the message along with failed_to_send_backpressured_count++;
                    // if we got: java.io.IOException: Could not send message: Error code: Not connected!
                    // Tthen the connection to the server is gone (for example the server was shut down).
                    // Simply ignoring and waiting for the server to show up again will not work because after the server restart all the
                    // clients should re-connect again (go through HELLO nnn  => CONNECT mmm, xxx, yyy => ...)
                    // So "Not connected!" is a "game over" and we should quit the main loop to close AeronMessagingClient thread now.
                    // Let's investigate different types of exceptions we get here:
                    //
                } catch (PublicationNotConnected ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationNotConnected. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (PublicationAdminAction ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationAdminAction. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (PublicationBackPressured ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationBackPressured!?");
                    // Failed to send the message due to backbressure, put it back into the front of the queue
                    outgoing_messages_to_all_clients_queue.addFirst(outgoing_message);

                    // Increase "failed_to_send_count" stats and continue
                    failed_to_send_backpressured_count++;

                } catch (PublicationClosed ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationClosed. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (PublicationMaxPositionExceeded ex) {
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationMaxPositionExceeded. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                } catch (IOException ex) {
                    // We're in some IllegalStateException() state.
                    logger.error("Exception cought while trying to MessagesHelper.send_message(): We're in some IllegalStateException() state. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server. Exception was: " + ex);
                    return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.
                }
            }

            // Send stats to the server's "all clients" channel once every 10 seconds
            // Every 10 sec send PUBLISH stats message via all_clients_publication with some client stats.
            if (now_epoch_ms > last_stats_sent_epoch_ms + 10000
                    // && all_clients_publication.isConnected()  - let's pring stats even when no clients connected, just for debug, and we'll .send_message() only if anybody connected
                    && last_stats_sent_epoch_ms != now_epoch_ms) {

                // Calculate some stats
                long main_loop_run_duration_ms = now_epoch_ms - main_loop_start_epoch_ms + 1;  // +1 is a cheesy way to avoid /0 and it won't matter after few seconds run
                long average_tx_message_rate_per_s = total_messages_sent_count * 1000 / main_loop_run_duration_ms;
                long average_tx_bytes_rate_per_ms = total_messages_sent_size / main_loop_run_duration_ms;
                long average_rx_fragments_rate_per_ms = polled_fragmetns_total_count / main_loop_run_duration_ms;
                long main_loop_iterations_rate_per_ms = main_loop_iterations_count / main_loop_run_duration_ms;
                last_stats_sent_epoch_ms = now_epoch_ms;
                stats_report_count++;

                String server_stats = "{\"mime_type\": \"client/stats\", "
                        + "\"stats_report_count\": " + stats_report_count + ", "
                        + "\"sent_messages_count\": " + total_messages_sent_count + ", "
                        + "\"sent_messages_size\": " + total_messages_sent_size + ", "
                        // This might be terribly slow on large gueues since it will have to walk through the whole chain of objects in the queue                                        
                        //                                        + "\"incoming_messages_from_all_clients_queue_size\": " + incoming_messages_from_all_clients_queue.size() + ", "
                        + "\"polled_fragmetns_total_count\": " + polled_fragmetns_total_count + ", "
                        + "\"failed_to_send_count\": " + failed_to_send_backpressured_count + ", "
                        + "\"main_loop_run_duration_ms\": " + main_loop_run_duration_ms + ", "
                        + "\"average_tx_message_rate_per_s\": " + average_tx_message_rate_per_s + ", "
                        + "\"average_tx_bytes_rate_per_ms\": " + average_tx_bytes_rate_per_ms + ", "
                        + "\"average_rx_fragments_rate_per_ms\": " + average_rx_fragments_rate_per_ms + ", "
                        + "\"main_loop_iterations_rate_per_ms\": " + main_loop_iterations_rate_per_ms + ", "
                        + "\"timestamp_epoch_ms\": " + now_epoch_ms + ", "
                        + "\"timestamp\": " + now_instant.toString()
                        + "}";

                // Print stats to the terminal even if nobody connected
                logger.debug("CLIENT STATS: " + server_stats);

                // Send the stats message to the client, only if at least 1 client is connected
                if (all_clients_publication.isConnected()) {
                    // We're bypassing the outgoing queue and directly "submit()" message into the ExecutorService here.
                    // We could do both - .sendMessage() and inject it into the queue, so the receiving side (client) 
                    // will get server stats and can track what is the lag on that particular queue shovelling.
                    // For example if we have 100 messages in the outgoing queue, then it will take 100 "main loop" iterations
                    // to get to that enqueued message.
                    // The epoch_ms can be used as a "key" to match the two messages on the client side.
                    // Try to send the message.
                    try {
                        // This might throw: java.io.IOException: Could not send message: Error code: Back pressured
                        MessagesHelper.send_message(
                                all_clients_publication,
                                tmp_send_buffer,
                                server_stats
                        );
                        // Successfully sent message, increase some stats
                        total_messages_sent_count++;
                        total_messages_sent_size += server_stats.length();
                        current_iteration_messages_sent_count++;

//                    // Let's actually do it!
//                    this.send_all_clients_control_channel_message(server_stats);
//                    // Alternatively we could iterate all private connections and send message to all clients
//                    // using: clients_state_tracker.sent_private_message_to_all_clients(server_stats),
//                    // which would deliver the message to all clients once, but via different channel
                        // Successfully sent message, increase some stats
//                        total_messages_sent_count++;
//                        total_messages_sent_size += server_stats.length();
//                        current_iteration_messages_sent_count++;
//
//
                        // We should distinguish different types of exceptions thrown by MessagesHelper.send_message().
                        // If we backpressured we need to backdown and re-enque the message along with failed_to_send_backpressured_count++;
                        // if we got: java.io.IOException: Could not send message: Error code: Not connected!
                        // Tthen the connection to the server is gone (for example the server was shut down).
                        // Simply ignoring and waiting for the server to show up again will not work because after the server restart all the
                        // clients should re-connect again (go through HELLO nnn  => CONNECT mmm, xxx, yyy => ...)
                        // So "Not connected!" is a "game over" and we should quit the main loop to close AeronMessagingClient thread now.
                        // Let's investigate different types of exceptions we get here:
                        //
                    } catch (PublicationNotConnected ex) {
                        logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationNotConnected. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                        return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                    } catch (PublicationAdminAction ex) {
                        logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationAdminAction. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                        return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                    } catch (PublicationBackPressured ex) {
                        // Failed to send the message due to backbressure, put it back into the front of the queue
                        outgoing_messages_to_all_clients_queue.addFirst(server_stats);

                        // Increase "failed_to_send_count" stats and continue
                        failed_to_send_backpressured_count++;

                    } catch (PublicationClosed ex) {
                        logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationClosed. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                        return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                    } catch (PublicationMaxPositionExceeded ex) {
                        logger.error("Exception cought while trying to MessagesHelper.send_message(): PublicationMaxPositionExceeded. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.");
                        return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.

                    } catch (IOException ex) {
                        // We're in some IllegalStateException() state.
                        logger.error("Exception cought while trying to MessagesHelper.send_message(): We're in some IllegalStateException() state. Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server. Exception was: " + ex);
                        return;  // Closing the main loop (closing AeronMessagingClient). You'll neeed to try to reconnect the server.
                    }

                }
            }

            // Sleep 1ms only if no fragments received and there was nothing to send
            if ((fragments_received + current_iteration_messages_sent_count) == 0) {
                try {
                    Thread.sleep(1L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * <pre>
     * The incoming_messages_from_all_clients_queue  <-- got filled by on_all_clients_message_received()  <-- used by all_client_subscription_message_handler() <-- used by all_client_subscription.poll()
     * </pre>
     *
     * @param session_name
     * @param buffer
     * @param offset
     * @param length
     */
    private void on_all_clients_message_received(
            final String session_name,
            final DirectBuffer buffer,
            final int offset,
            final int length) {
        final String message
                = MessagesHelper.parse_message_utf8(buffer, offset, length);
        this.incoming_messages_from_all_clients_queue.add(message);
        logger.debug("[{}] on_all_clients_message_received: {}", session_name, message);

    }

    /**
     * <pre>
     * The incoming_messages_from_private_queue  <-- got filled by on_private_message_received()  <-- used by private_subscription_message_handler()    <-- used by private_subscription.poll()
     * </pre>
     *
     * @param session_name
     * @param buffer
     * @param offset
     * @param length
     */
    private void on_private_message_received(
            final String session_name,
            final DirectBuffer buffer,
            final int offset,
            final int length) {
        final String message
                = MessagesHelper.parse_message_utf8(buffer, offset, length);
        this.incoming_messages_from_private_queue.add(message);
        logger.debug("[{}] on_private_message_received: {}", session_name, message);

    }

    private Publication setupConnectPublication()
            throws ClientTimedOutException {
        final ConcurrentPublication publication
                = AeronChannelsHelper.createPublicationWithSession(
                        this.aeron,
                        this.configuration.remoteAddress(),
                        this.remote_data_port,
                        this.remote_session,
                        MAIN_STREAM_ID);

        for (int index = 0; index < 1000; ++index) {
            if (publication.isConnected()) {
                logger.debug("CONNECT publication connected   +++++ this.configuration.remoteAddress() = " + this.configuration.remoteAddress() + " this.remote_data_port = " + this.remote_data_port);
                return publication;
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        publication.close();
        throw new ClientTimedOutException("Making CONNECT publication to server");
    }

    private Subscription setupConnectSubscription()
            throws ClientTimedOutException {
        final Subscription subscription
                = AeronChannelsHelper.createSubscriptionDynamicMDCWithSession(
                        this.aeron,
                        this.configuration.remoteAddress(),
                        this.remote_control_port,
                        this.remote_session,
                        MAIN_STREAM_ID);

        for (int index = 0; index < 1000; ++index) {
            if (subscription.isConnected() && subscription.imageCount() > 0) {
                logger.debug("CONNECT subscription connected   ++++ this.configuration.remoteAddress() = " + this.configuration.remoteAddress() + " control port: " + this.remote_control_port);
                return subscription;
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        subscription.close();
        throw new ClientTimedOutException(
                "Making CONNECT subscription to server");
    }

    private void waitForConnectResponse(
            final Subscription all_client_subscription,
            final String session_name)
            throws ClientTimedOutException, ClientRejectedException {
        logger.debug("waiting for response");

        final FragmentHandler handler
                = new FragmentAssembler(
                        (data, offset, length, header)
                        -> this.onInitialResponse(session_name, data, offset, length));

        long start_wait_for_connect_response_epoch_ms = System.currentTimeMillis();
        long connect_timeout_ms = 10000;
        while (true) {
//        for (int index = 0; index < 1000; ++index) {

            long current_epoch_ms = System.currentTimeMillis();

            all_client_subscription.poll(handler, 1000);

            if (this.failed) {
                throw new ClientRejectedException("Server rejected this client");
            }

            if (this.remote_ports_received) {
                return;
            }

            try {
                Thread.sleep(1L);
                // Dimon: we keep trying to pull our "CONNECT" welcome message from the server only 1000 times,
                // so if the control channel blusting messages for all other (already connected) clients, then
                // we have high chances to fail to connect here..
                // We can address it by:
                //   - onInitialResponse() should not consider incoming messages with length > N bytes
                //   - let's make 1st few bytes in the control stream known, say start with "CONTROL ", then no need to even prematurely use regexp
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Break the loop if timeout time exceeded
            if (current_epoch_ms - start_wait_for_connect_response_epoch_ms >= connect_timeout_ms) {
                break;
            }
        }

        throw new ClientTimedOutException(
                "Waiting for CONNECT response from server (connect_timeout_ms value was " + connect_timeout_ms + ")");
    }

    /**
     * Parse the initial response from the server.
     */
    private void onInitialResponse(
            final String session_name,
            final DirectBuffer buffer,
            final int offset,
            final int length) {

//        logger.trace("debug: got response with length: " + length + ", offset: " + offset);
        // The initial "CONNECT" response can't be shorter than 21 byte and can't be longer than ~60 bytes
        if (length < 21 || length > 60) {
            return;
        }

        // The initial "CONNECT" response from the server is now supposed to start with "CONTROL " prefix
        if (buffer.getByte(offset + 0) != 'C'
                || buffer.getByte(offset + 1) != 'O'
                || buffer.getByte(offset + 2) != 'N'
                || buffer.getByte(offset + 3) != 'T'
                || buffer.getByte(offset + 4) != 'R'
                || buffer.getByte(offset + 5) != 'O'
                || buffer.getByte(offset + 6) != 'L'
                || buffer.getByte(offset + 7) != ' ') {
            return;
        }

        final String response = MessagesHelper.parse_message_utf8(buffer, offset, length);

        logger.trace("[{}] response: {}", session_name, response);

        /**
         * Try to extract the session identifier to determine whether the
         * message was intended for this client or not.
         */
        final int space = response.indexOf(" ", 8);  // +8 is for "CONTROL " prefix
        if (space == -1) {
            logger.error(
                    "[{}] server returned unrecognized message (can not find space): {}",
                    session_name,
                    response);
            return;
        }

        final String message_session = response.substring(8, space);  // +8 is for "CONTROL " prefix
        if (!Objects.equals(message_session, session_name)) {
            logger.trace(
                    "[{}] ignored message intended for another client (expected session=" + session_name + ", but received session=" + message_session + ")",
                    session_name);
            return;
        }

        // The message was intended for this client. Try to parse it as one of
        // the available message types.
        final String text = response.substring(space).trim();   // Trim leading session and space, so now response text should start with CONNECT

        final Matcher error_matcher = PATTERN_ERROR.matcher(text);
        if (error_matcher.matches()) {
            final String message = error_matcher.group(1);
            logger.error("[{}] server returned an error: {}", session_name, message);
            this.failed = true;
            return;
        }

        final Matcher connect_matcher = PATTERN_CONNECT.matcher(text);
        if (connect_matcher.matches()) {
            final int port_data
                    = Integer.parseUnsignedInt(connect_matcher.group(1));
            final int port_control
                    = Integer.parseUnsignedInt(connect_matcher.group(2));
            final int session_crypted
                    = Integer.parseUnsignedInt(connect_matcher.group(3), 16);

            logger.debug(
                    "[{}] connect {} {} (encrypted {})",
                    session_name,
                    Integer.valueOf(port_data),
                    Integer.valueOf(port_control),
                    Integer.valueOf(session_crypted));
            this.remote_control_port = port_control;
            this.remote_data_port = port_data;
            this.remote_session = this.duologue_key ^ session_crypted;
            this.remote_ports_received = true;
            return;
        }

        logger.error(
                "[{}] server returned unrecognized message: {}",
                session_name,
                text);
    }

    private Publication setupAllClientsPublication()
            throws ClientTimedOutException {
        final ConcurrentPublication publication
                = AeronChannelsHelper.createPublication(
                        this.aeron,
                        this.configuration.remoteAddress(),
                        this.configuration.remoteInitialPort(),
                        MAIN_STREAM_ID);

        for (int index = 0; index < 1000; ++index) {
            if (publication.isConnected()) {
                logger.debug("initial publication connected");
                return publication;
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        publication.close();
        throw new ClientTimedOutException("Making initial publication to server");
    }

    private Subscription setupAllClientsSubscription()
            throws ClientTimedOutException {
        final Subscription subscription
                = AeronChannelsHelper.createSubscriptionDynamicMDC(
                        this.aeron,
                        this.configuration.remoteAddress(),
                        this.configuration.remoteInitialControlPort(),
                        MAIN_STREAM_ID);

        for (int index = 0; index < 1000; ++index) {
            if (subscription.isConnected() && subscription.imageCount() > 0) {
                logger.debug("initial subscription connected");
                return subscription;
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        subscription.close();
        throw new ClientTimedOutException(
                "Making initial subscription to server");
    }

    @Override
    public void close() {
        try {
            closeIfNotNull(this.aeron);
            closeIfNotNull(this.media_driver);
        } catch (Exception ex) {
            logger.error("AeronMessagignClient.close() somehow failed... exception: " + ex);
        }
    }
}
