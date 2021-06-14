package ca.dimon.aeronmessaging.server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.time.Clock;
import java.time.Instant;

/**
 * Moving all the variables used to service aeron_messaging_server main loop
 * into separate class, which will help to keep track of Aeron's loop state. Doing
 * this would allow us to create a new function
 * "aeron_messaging_server_main_loop_iteration()", which we'll can call from the
 * encapsulating app (class that uses AeronMessaging client/server) main loop
 * as a simple 1-liner call if we choose to run AeronMessagingServer in the same
 * thread. Much simpler alternative would be to allow AeronMessagingServer to have
 * its own thread so all the messaging would automagically happen in parallel.
 * 
 */
public class MainConsumerLoopVars {

    public String mime_type = "aeron_messaging_server/main_consumer_loop_variables";

    public long total_incoming_public_messages_count = 0;
    public long total_incoming_private_messages_count = 0;
    public long iteration_incoming_public_messages_count = 0;
    public long iteration_incoming_private_messages_count = 0;

    public Clock clock;
    public Instant main_loop_start_instant;
    public long main_loop_start_epoch_ms;

    public long last_stats_sent_epoch_ms = 0; // Keep track of exact ms when report was generated to avoid sending it multiple times (loop is fast)
    public long main_loop_iterations_count = 0;

    // Sometimes we can't read just 1 incoming message in 1 main loop iteration since
    // it might fundamentally limit our throughput (main loop does other things,
    // so repeating them for each one incoming message is too cpu-expensive).
    // On the other hand we can't process ALL incoming messages, since the queue might
    // be already full and a new messages stream is constantly coming, so this
    // variable would allow us to limit the max number of messages to process in one main loop iteration.
    // Reasonable values would be a couple hundreds (unless your messages are some huge JSON objects,
    // in which case you probably want to make this value down to a couple dosen). Play with it.
    public int number_of_messages_to_read_in_one_go_limit = 100;

    public MainConsumerLoopVars() {
        clock = Clock.systemUTC();
        main_loop_start_instant = clock.instant();
        main_loop_start_epoch_ms = main_loop_start_instant.toEpochMilli();
    }

    public long main_loop_run_duration_ms;
    public long average_rx_private_messages_per_s;
    public long main_loop_iterations_rate_per_s;
    public String thread_id;
    public long timestamp_epoch_ms;

    public String get_updated_stats() {
        // Get current time in 2 forms: Instant and epoch_ms (one will be used for human-readable time, other for "robots":)
        Instant now_instant = clock.instant();
        long timestamp_epoch_ms = now_instant.toEpochMilli();
        main_loop_run_duration_ms = timestamp_epoch_ms - main_loop_start_epoch_ms + 1;  // +1 is a cheesy way to avoid /0 and it won't matter after few seconds run
        average_rx_private_messages_per_s = total_incoming_private_messages_count * 1000 / main_loop_run_duration_ms;
        main_loop_iterations_rate_per_s = main_loop_iterations_count * 1000 / main_loop_run_duration_ms;
        thread_id = Thread.currentThread().toString();
        return this.to_json();
    }

    /**
     * <pre>
     * to_json() is useful when we need to serialize the object (parsable + human readable).
     *
     * </pre>
     */
    public String to_json() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
//        String pretty_json = gson.toJson(toJsonObject());
        String pretty_json = gson.toJson(this);
        return pretty_json;
    }

//    public JsonObject toJsonObject() {
//        JsonObject json_object = new JsonObject();
//        json_object.addProperty("timestamp_epoch_ms", TimeAdv.get_current_system_epoch_ms());
//        return toJsonObject(json_object);
//    }
    public JsonObject toJsonObject(JsonObject json_object) {
        return json_object;
    }
}
