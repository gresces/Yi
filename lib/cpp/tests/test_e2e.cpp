// End-to-end test for the header-only C++ client.
//
// Requires a running Yi service. The test uses unique consumer names and
// unique event types, so it can run against a database that already contains
// events.

#include "yi.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

namespace {

int failures = 0;

#define CHECK(condition)                                                       \
    do {                                                                       \
        if (!(condition)) {                                                    \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,      \
                         #condition);                                          \
            ++failures;                                                        \
        }                                                                      \
    } while (false)

bool has_type(const yi::Event &event, const std::string &type) {
    return std::find(event.types.begin(), event.types.end(), type) !=
           event.types.end();
}

std::string unique_suffix() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::to_string(
        std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

} // namespace

int main(int argc, char **argv) {
    const std::string base_url =
        argc > 1 ? argv[1] : "http://127.0.0.1:8081";
    const std::string suffix = unique_suffix();
    const std::string type = "cpp.e2e." + suffix;
    const std::string name = "cpp-e2e-" + suffix;

    try {
        yi::Client client(base_url);
        CHECK(client.health() == "ok");

        // ---- producer: explicit event, duplicate detection -----------------

        auto event = yi::Event::create(
            "cpp-lib", {type + ".explicit", "note"},
            yi::json{{"text", "explicit event"}});
        const auto first = client.publish(event);
        CHECK(!first.duplicate);

        const auto duplicate = client.publish(event.to_json());
        CHECK(duplicate.duplicate);

        // ---- consumer: capture, next, ack, nack, stats ---------------------

        yi::CaptureOptions capture;
        capture.source = "cpp-lib";
        capture.types = {type};
        capture.data = yi::json{{"tag", "capture"}};
        const auto captured = client.capture("hello from C++", capture);
        CHECK(!captured.duplicate);

        yi::ConsumerOptions consumer_options;
        consumer_options.poll_timeout = std::chrono::seconds(5);
        yi::Consumer consumer(client, name, {type}, consumer_options);

        auto message = consumer.next();
        CHECK(message.has_value());
        if (message) {
            CHECK(has_type(message->event(), type));
            CHECK(message->event().data.at("text") == "hello from C++");
            CHECK(message->event().data.at("tag") == "capture");
            message->ack();
        }

        auto nacked = yi::Event::create(
            "cpp-lib", {type}, yi::json{{"text", "nack me"}});
        CHECK(!client.publish(nacked).duplicate);

        message = consumer.next();
        CHECK(message.has_value());
        if (message) {
            CHECK(message->event().id == nacked.id);
            message->nack("e2e nack");
        }

        const auto stats = client.stats();
        const auto found = std::find_if(
            stats.begin(), stats.end(),
            [&](const yi::ConsumerStats &item) { return item.name == name; });
        CHECK(found != stats.end());

        const auto own = consumer.stats();
        CHECK(own.name == name);
        CHECK(own.delivered == 1);
        CHECK(own.failed == 1);
        CHECK(own.pending == 0);
        CHECK(own.last_error && *own.last_error == "e2e nack");

        // ---- conflict ------------------------------------------------------

        bool conflicted = false;
        try {
            yi::Consumer other(client, name, {"other.type"}, consumer_options);
            (void)other;
        } catch (const yi::ConflictError &error) {
            conflicted = error.status() == 409;
        }
        CHECK(conflicted);

        // ---- events query and type filtering -------------------------------

        yi::EventsQuery query;
        query.limit = 0;
        query.type = type;
        const auto events = client.events(query);
        CHECK(events.size() == 2);
        for (const auto &item : events) {
            CHECK(has_type(item, type));
        }

        yi::EventsQuery absent;
        absent.limit = 0;
        absent.type = type + ".absent";
        CHECK(client.events(absent).empty());

        // ---- failures ------------------------------------------------------

        const auto failure_list = client.failures();
        const auto failed = std::find_if(
            failure_list.begin(), failure_list.end(), [&](const yi::Failure &item) {
                return item.consumer == name && item.error == "e2e nack";
            });
        CHECK(failed != failure_list.end());

        // ---- range-based iteration ----------------------------------------

        const std::string iter_type = type + ".iter";
        const std::string iter_name = name + "-iter";
        yi::Consumer iterating(client, iter_name, {iter_type}, consumer_options);

        auto iter_event = yi::Event::create(
            "cpp-lib", {iter_type}, yi::json{{"text", "iterate"}});
        CHECK(!client.publish(iter_event).duplicate);

        int seen = 0;
        for (const auto &item : iterating) {
            ++seen;
            CHECK(item.event().id == iter_event.id);
            item.ack();
            iterating.close();
        }
        CHECK(seen == 1);

        bool closed_throws = false;
        try {
            (void)iterating.next();
        } catch (const yi::Error &) {
            closed_throws = true;
        }
        CHECK(closed_throws);

        // ---- long-poll timeout --------------------------------------------

        const std::string idle_name = name + "-idle";
        client.register_consumer(idle_name, {type + ".idle"});
        const auto idle = client.next(idle_name, std::chrono::milliseconds(100));
        CHECK(!idle.has_value());
        client.deregister(idle_name);

        // ---- cleanup -------------------------------------------------------

        consumer.deregister();
        client.deregister(iter_name);
    } catch (const std::exception &error) {
        std::fprintf(stderr, "FAIL exception: %s\n", error.what());
        return 1;
    }

    if (failures != 0) {
        std::fprintf(stderr, "%d checks failed\n", failures);
        return 1;
    }
    std::printf("e2e: OK\n");
    return 0;
}
