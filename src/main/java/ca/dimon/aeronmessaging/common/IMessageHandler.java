package ca.dimon.aeronmessaging.common;

/**
 * <pre>
 * All incoming messages "private" and "publicly broadcasted to all clients" will be passed to the
 * instance which implements IMessageHandler interface.
 * aeron_main_consumer_loop_iteration
 *    Just as a reminder: if you want to use AeronMessaging in your app you'd:
 *       - create and instance of AeronMessagingServer (and AeronMessagingClient on all clients)
 *       - register your incoming messages handler as an instance, which implements IMessageHandler i-face:
 *              aeron_messaging_server.register_external_message_handler(my_handler);
 *       - periodically call aeron_main_consumer_loop_iteration() in your app thread, which will shovei incoming
 *         messages and pass them to your IMessageHandler (if any)
 *
 * Note: this interface + f-n aeron_main_consumer_loop_iteration() were created only for Server side to simplify
 * quite complex process of receiving messages on the server side. It hides the knowlede of "how to iterate all connected
 * clients" from your app, which uses AeromMessagingServer.  As for the client side (see class AeromMessagingClient)
 * the message consumption is much simpler on the client side, thus no need for such interface + function on the cilent.
 * Here's the example of 2-liner on the AeronMessagingClient side to read messages from "public" and "private" channels:
 *
 * client_side_main_loop {
 *     . . .
 *     String incoming_private_message = aeron_messaging_client.get_private_message();
 *     String incoming_public_message = aeron_messaging_client.get_all_clients_control_channel_message();
 *     . . .
 * }
 *
 * </pre>
 */
public interface IMessageHandler {

    // This f-n handles incoming messages that came via "all clients" channel, which means this message was broadcasted to all connected clients.
    void process_all_clients_message(String message);

    // This f-n handles incoming messages that came via "private" channel (server sent it to only 1 of potentially many connected clients).
    void process_private_message(Integer session_id, String message);
}
