// Unit tests for the header-only C++ client. No server required.

#include "yi.hpp"

#include <cstdio>
#include <string>
#include <type_traits>
#include <vector>

namespace {

static_assert(std::is_copy_constructible_v<yi::Client>);
static_assert(std::is_move_constructible_v<yi::Consumer>);
static_assert(!std::is_copy_constructible_v<yi::Consumer>);
static_assert(!std::is_move_assignable_v<yi::Consumer>);
static_assert(std::is_copy_constructible_v<yi::Message>);
static_assert(std::is_move_constructible_v<yi::Message>);

int failures = 0;

#define CHECK(condition)                                                       \
    do {                                                                       \
        if (!(condition)) {                                                    \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,      \
                         #condition);                                          \
            ++failures;                                                        \
        }                                                                      \
    } while (false)

void test_event_create() {
    const auto event = yi::Event::create(
        "phone", {"note", "raw.captured", "note", "  "},
        yi::json{{"text", "hello"}});

    CHECK(event.id.size() == 32);
    CHECK(event.source == "phone");
    CHECK((event.types == std::vector<std::string>{"note", "raw.captured"}));
    CHECK(event.time.size() == 20);
    CHECK(event.time.back() == 'Z');
    CHECK(event.data.at("text") == "hello");
}

void test_event_round_trip() {
    auto event = yi::Event::create("laptop", {"raw.captured"},
                                   yi::json{{"text", "round trip"}});
    const auto parsed = yi::Event::from_json(event.to_json());
    CHECK(parsed.id == event.id);
    CHECK(parsed.source == event.source);
    CHECK(parsed.types == event.types);
    CHECK(parsed.time == event.time);
    CHECK(parsed.data == event.data);
}

void test_event_legacy_and_validation() {
    const auto legacy = yi::Event::from_json(yi::json{
        {"id", "e1"}, {"source", "s"}, {"type", "note"}, {"time", "t"}});
    CHECK((legacy.types == std::vector<std::string>{"note"}));
    CHECK(legacy.data.is_object());

    bool threw = false;
    try {
        (void)yi::Event::from_json(yi::json{{"id", "e1"}, {"data", yi::json::array()}});
    } catch (const yi::Error &) {
        threw = true;
    }
    CHECK(threw);

    threw = false;
    try {
        (void)yi::Event::create("", {"note"});
    } catch (const yi::Error &) {
        threw = true;
    }
    CHECK(threw);

    threw = false;
    try {
        (void)yi::Event::create("s", {});
    } catch (const yi::Error &) {
        threw = true;
    }
    CHECK(threw);

    threw = false;
    try {
        (void)yi::Event::create("s", {"note"}, yi::json::array());
    } catch (const yi::Error &) {
        threw = true;
    }
    CHECK(threw);
}

void test_models() {
    const auto published = yi::PublishResult::from_json(
        yi::json{{"duplicate", true}, {"matched", 3}});
    CHECK(published.duplicate);
    CHECK(published.matched == 3);

    const auto stats = yi::ConsumerStats::from_json(yi::json{
        {"name", "c"},
        {"types", {"raw.captured", "note"}},
        {"cursor", 5},
        {"pending", 2},
        {"delivered", 7},
        {"failed", 1},
        {"last_error", "boom"},
        {"last_event", "e1"},
        {"last_at", "2026-11-15T10:00:00Z"}});
    CHECK(stats.name == "c");
    CHECK(stats.types.size() == 2);
    CHECK(stats.cursor == 5);
    CHECK(stats.pending == 2);
    CHECK(stats.delivered == 7);
    CHECK(stats.failed == 1);
    CHECK(stats.last_error && *stats.last_error == "boom");
    CHECK(stats.last_event && *stats.last_event == "e1");
    CHECK(stats.last_at && *stats.last_at == "2026-11-15T10:00:00Z");

    const auto stats_json = stats.to_json();
    CHECK(stats_json.at("name") == "c");
    CHECK(stats_json.at("cursor") == 5);
    CHECK(stats_json.at("last_error") == "boom");

    const auto failure = yi::Failure::from_json(yi::json{
        {"consumer", "c"},
        {"event_id", "e1"},
        {"source", "phone"},
        {"error", "bad"},
        {"at", "2026-11-15T10:00:00Z"}});
    CHECK(failure.consumer == "c");
    CHECK(failure.event_id == "e1");
    CHECK(failure.source == "phone");
    CHECK(failure.error == "bad");
    CHECK(failure.at == "2026-11-15T10:00:00Z");
}

void test_detail_helpers() {
    CHECK((yi::detail::normalize_types({" b ", "a", "a", ""}) ==
           std::vector<std::string>{"a", "b"}));
    CHECK(yi::detail::url_encode("") == "");
    CHECK(yi::detail::url_encode("a b/c") == "a%20b%2Fc");
    CHECK(yi::detail::next_path("c", std::chrono::milliseconds(2500)) ==
          "/consumers/c/next?timeout=2.500");
    CHECK(yi::detail::next_path("a b", std::chrono::milliseconds(1)) ==
          "/consumers/a%20b/next?timeout=0.001");
}

void test_errors() {
    try {
        throw yi::ConflictError("conflict", 409, "{}");
    } catch (const yi::HttpError &error) {
        CHECK(error.status() == 409);
        CHECK(error.body() == "{}");
    } catch (...) {
        CHECK(false);
    }

    try {
        throw yi::TransportError(CURLE_COULDNT_CONNECT, "no route");
    } catch (const yi::Error &error) {
        CHECK(std::string(error.what()) == "no route");
    }
}

} // namespace

int main() {
    test_event_create();
    test_event_round_trip();
    test_event_legacy_and_validation();
    test_models();
    test_detail_helpers();
    test_errors();

    if (failures != 0) {
        std::fprintf(stderr, "%d checks failed\n", failures);
        return 1;
    }
    std::printf("unit: OK\n");
    return 0;
}
