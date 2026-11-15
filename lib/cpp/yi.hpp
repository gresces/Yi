// yi.hpp — modern, header-only C++17 client for the Yi event service.
//
// Dependencies:
//   - libcurl (link with -lcurl)
//   - nlohmann/json (header-only; https://github.com/nlohmann/json)
//
// The library is entirely header-only: include this file and link libcurl.
// It covers the producer, the pull-based consumer and the admin endpoints.
//
// Threading: yi::Client is immutable after construction and safe to share
// between threads. A yi::Consumer is a local handle around a server-side
// registration and must not be used concurrently.

#ifndef YI_HPP
#define YI_HPP

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <ctime>
#include <initializer_list>
#include <iomanip>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yi {

using json = nlohmann::json;

inline constexpr std::string_view version = "0.1.5";
inline constexpr std::chrono::milliseconds default_request_timeout{10000};
inline constexpr std::chrono::milliseconds default_poll_timeout{25000};

// ---- errors ---------------------------------------------------------------

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class HttpError : public Error {
public:
    HttpError(std::string message, int status, std::string body)
        : Error(std::move(message)), status_(status), body_(std::move(body)) {}

    int status() const noexcept { return status_; }
    const std::string &body() const noexcept { return body_; }

private:
    int status_;
    std::string body_;
};

class ConflictError : public HttpError {
public:
    using HttpError::HttpError;
};

class TransportError : public Error {
public:
    TransportError(CURLcode code, std::string message)
        : Error(std::move(message)), code_(code) {}

    CURLcode code() const noexcept { return code_; }

private:
    CURLcode code_;
};

// ---- details --------------------------------------------------------------

namespace detail {

inline std::string_view trim(std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front())) != 0) {
        value.remove_prefix(1);
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back())) != 0) {
        value.remove_suffix(1);
    }
    return value;
}

inline std::vector<std::string> normalize_types(std::vector<std::string> types) {
    std::vector<std::string> normalized;
    normalized.reserve(types.size());
    for (const auto &type : types) {
        const auto trimmed = trim(type);
        if (!trimmed.empty()) {
            normalized.emplace_back(trimmed);
        }
    }
    std::sort(normalized.begin(), normalized.end());
    normalized.erase(std::unique(normalized.begin(), normalized.end()),
                     normalized.end());
    return normalized;
}

// Subscription patterns share the server's trim/sort/dedupe normalization.
// The pattern syntax itself (Go RE2) is validated by the server.
inline std::vector<std::string>
normalize_patterns(std::vector<std::string> patterns) {
    return normalize_types(std::move(patterns));
}

inline json register_body(std::vector<std::string> types,
                          std::vector<std::string> patterns) {
    types = normalize_types(std::move(types));
    patterns = normalize_patterns(std::move(patterns));
    if (types.empty() && patterns.empty()) {
        throw Error("consumer needs at least one type or pattern");
    }

    json body;
    body["types"] = types;
    if (!patterns.empty()) {
        body["patterns"] = patterns;
    }
    return body;
}

inline std::string random_id() {
    std::random_device device;
    std::seed_seq seed{device(), device(), device(), device()};
    std::mt19937_64 engine(seed);

    const auto high = engine();
    const auto low = engine();
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(16) << high
        << std::setw(16) << low;
    return out.str();
}

inline std::string utc_now() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t seconds = std::chrono::system_clock::to_time_t(now);
    std::tm parts{};
#if defined(_MSC_VER)
    if (gmtime_s(&parts, &seconds) != 0) {
        return "1970-01-01T00:00:00Z";
    }
#else
    if (gmtime_r(&seconds, &parts) == nullptr) {
        return "1970-01-01T00:00:00Z";
    }
#endif
    char buffer[32] = {};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &parts) ==
        0) {
        return "1970-01-01T00:00:00Z";
    }
    return buffer;
}

inline const json *find_field(const json &value, const char *key) {
    if (!value.is_object()) {
        return nullptr;
    }
    const auto it = value.find(key);
    return it == value.end() ? nullptr : &*it;
}

inline std::string string_field(const json &value, const char *key) {
    const auto *field = find_field(value, key);
    if (field == nullptr || !field->is_string()) {
        return {};
    }
    return field->get<std::string>();
}

inline std::optional<std::string> optional_string_field(const json &value,
                                                        const char *key) {
    const auto *field = find_field(value, key);
    if (field == nullptr || !field->is_string()) {
        return std::nullopt;
    }
    return field->get<std::string>();
}

inline long long int_field(const json &value, const char *key) {
    const auto *field = find_field(value, key);
    if (field == nullptr || !field->is_number_integer()) {
        return 0;
    }
    try {
        return field->get<long long>();
    } catch (const json::exception &) {
        return 0;
    }
}

inline bool bool_field(const json &value, const char *key) {
    const auto *field = find_field(value, key);
    return field != nullptr && field->is_boolean() &&
           field->get<bool>();
}

inline json parse_json(std::string_view text, std::string_view what) {
    try {
        return json::parse(text.begin(), text.end());
    } catch (const json::exception &error) {
        throw Error(std::string(what) + ": invalid JSON: " + error.what());
    }
}

inline std::string dump_json(const json &value, std::string_view what) {
    try {
        return value.dump();
    } catch (const json::exception &error) {
        throw Error(std::string(what) + ": cannot encode JSON: " + error.what());
    }
}

inline void ensure_curl() {
    static const bool initialized = [] {
        return curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK;
    }();
    if (!initialized) {
        throw Error("curl_global_init failed");
    }
}

inline std::string url_encode(std::string_view value) {
    if (value.empty()) {
        return {};
    }
    ensure_curl();
    char *escaped = curl_easy_escape(nullptr, value.data(),
                                     static_cast<int>(value.size()));
    if (escaped == nullptr) {
        throw Error("URL escaping failed");
    }
    std::string result(escaped);
    curl_free(escaped);
    return result;
}

inline std::string consumer_path(std::string_view name,
                                std::string_view suffix = {}) {
    return "/consumers/" + url_encode(name) + std::string(suffix);
}

inline std::string next_path(std::string_view name,
                             std::chrono::milliseconds timeout) {
    const auto millis =
        timeout.count() > 0 ? static_cast<long long>(timeout.count()) : 1;
    std::ostringstream out;
    out << consumer_path(name, "/next?timeout=") << (millis / 1000) << '.'
        << std::setw(3) << std::setfill('0') << (millis % 1000);
    return out.str();
}

struct Response {
    long status = 0;
    std::string body;
};

template <typename T>
inline void set_opt(CURL *handle, CURLoption option, T value) {
    if (curl_easy_setopt(handle, option, value) != CURLE_OK) {
        throw Error("curl_easy_setopt failed");
    }
}

inline std::size_t write_callback(char *ptr, std::size_t size,
                                  std::size_t nmemb, void *userdata) {
    auto *body = static_cast<std::string *>(userdata);
    const std::size_t bytes = size * nmemb;
    try {
        body->append(ptr, bytes);
    } catch (...) {
        return 0;
    }
    return bytes;
}

// Returns the Authorization header line for a bearer token, or nullopt when
// the token is empty (authentication disabled).
inline std::optional<std::string> authorization_header(std::string_view token) {
    if (token.empty()) {
        return std::nullopt;
    }
    return std::string("Authorization: Bearer ") + std::string(token);
}

inline Response http_request(std::string_view base_url,
                             std::string_view method, std::string_view path,
                             const std::optional<std::string> &body,
                             std::chrono::milliseconds timeout,
                             std::string_view token = {}) {
    ensure_curl();
    if (timeout.count() <= 0) {
        throw Error("request timeout must be positive");
    }

    CURL *raw_handle = curl_easy_init();
    if (raw_handle == nullptr) {
        throw Error("curl_easy_init failed");
    }
    const std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> handle(
        raw_handle, &curl_easy_cleanup);

    struct HeaderList {
        curl_slist *list = nullptr;
        ~HeaderList() {
            if (list != nullptr) {
                curl_slist_free_all(list);
            }
        }
    } headers;

    const auto add_header = [&headers](const char *header) {
        curl_slist *extended = curl_slist_append(headers.list, header);
        if (extended == nullptr) {
            throw Error("out of memory while building HTTP headers");
        }
        headers.list = extended;
    };

    add_header("Accept: application/json");
    if (const auto authorization = authorization_header(token)) {
        add_header(authorization->c_str());
    }
    if (body) {
        add_header("Content-Type: application/json");
    }

    const std::string url = std::string(base_url) + std::string(path);
    const std::string method_text(method);
    std::string response_body;

    set_opt(handle.get(), CURLOPT_URL, url.c_str());
    set_opt(handle.get(), CURLOPT_CUSTOMREQUEST, method_text.c_str());
    if (body) {
        set_opt(handle.get(), CURLOPT_POSTFIELDS, body->data());
        set_opt(handle.get(), CURLOPT_POSTFIELDSIZE,
                static_cast<long>(body->size()));
    }
    set_opt(handle.get(), CURLOPT_HTTPHEADER, headers.list);
    set_opt(handle.get(), CURLOPT_WRITEFUNCTION, &write_callback);
    set_opt(handle.get(), CURLOPT_WRITEDATA,
            static_cast<void *>(&response_body));
    set_opt(handle.get(), CURLOPT_TIMEOUT_MS,
            static_cast<long>(timeout.count()));
    set_opt(handle.get(), CURLOPT_NOSIGNAL, 1L);
    set_opt(handle.get(), CURLOPT_USERAGENT, "yi-cpp/0.1.5");

    const CURLcode code = curl_easy_perform(handle.get());
    if (code != CURLE_OK) {
        throw TransportError(
            code, method_text + " " + url + " failed: " +
                      std::string(curl_easy_strerror(code)));
    }

    long status = 0;
    if (curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &status) !=
        CURLE_OK) {
        throw Error("cannot read the HTTP status");
    }
    return Response{status, std::move(response_body)};
}

[[noreturn]] inline void throw_http_error(std::string_view method,
                                          std::string_view url, long status,
                                          const std::string &body) {
    std::string message;
    if (!body.empty()) {
        try {
            const auto parsed = json::parse(body);
            const auto *field = find_field(parsed, "error");
            if (field != nullptr && field->is_string()) {
                message = field->get<std::string>();
            }
        } catch (const json::exception &) {
        }
        if (message.empty()) {
            message = body;
        }
    }
    if (message.empty()) {
        message = "HTTP " + std::to_string(status);
    }

    const std::string text = std::string(method) + " " + std::string(url) +
                             " failed with HTTP " + std::to_string(status) +
                             ": " + message;
    if (status == 409) {
        throw ConflictError(text, static_cast<int>(status), body);
    }
    throw HttpError(text, static_cast<int>(status), body);
}

inline void expect_status(const Response &response, std::string_view method,
                          std::string_view url,
                          std::initializer_list<long> accepted) {
    for (const long status : accepted) {
        if (response.status == status) {
            return;
        }
    }
    throw_http_error(method, url, response.status, response.body);
}

} // namespace detail

// ---- models ---------------------------------------------------------------

struct Event {
    std::string id;
    std::string source;
    std::vector<std::string> types;
    std::string time;
    json data = json::object();

    static Event create(std::string source,
                        std::vector<std::string> types = {"raw.captured"},
                        json data = json::object()) {
        if (source.empty()) {
            throw Error("event source is required");
        }
        types = detail::normalize_types(std::move(types));
        if (types.empty()) {
            throw Error("event needs at least one type");
        }
        if (!data.is_object()) {
            throw Error("event data must be a JSON object");
        }
        return Event{detail::random_id(), std::move(source), std::move(types),
                     detail::utc_now(), std::move(data)};
    }

    static Event from_json(const json &value) {
        if (!value.is_object()) {
            throw Error("event must be a JSON object");
        }

        Event event;
        event.id = detail::string_field(value, "id");
        event.source = detail::string_field(value, "source");
        event.time = detail::string_field(value, "time");

        const auto *types = detail::find_field(value, "types");
        if (types != nullptr && !types->is_null()) {
            if (!types->is_array()) {
                throw Error("event types must be a JSON array");
            }
            for (const auto &item : *types) {
                if (item.is_string()) {
                    event.types.push_back(item.get<std::string>());
                }
            }
        } else {
            const auto *legacy = detail::find_field(value, "type");
            if (legacy != nullptr && legacy->is_string()) {
                event.types.push_back(legacy->get<std::string>());
            }
        }
        event.types = detail::normalize_types(std::move(event.types));

        const auto *data = detail::find_field(value, "data");
        if (data != nullptr && !data->is_null()) {
            if (!data->is_object()) {
                throw Error("event data must be a JSON object");
            }
            event.data = *data;
        }
        return event;
    }

    json to_json() const {
        if (!data.is_object()) {
            throw Error("event data must be a JSON object");
        }
        return json{{"id", id},       {"source", source},
                    {"types", types}, {"time", time},
                    {"data", data}};
    }
};

struct PublishResult {
    bool duplicate = false;
    int matched = 0;

    static PublishResult from_json(const json &value) {
        if (!value.is_object()) {
            throw Error("publish result must be a JSON object");
        }
        return PublishResult{detail::bool_field(value, "duplicate"),
                             static_cast<int>(detail::int_field(value, "matched"))};
    }
};

struct ConsumerStats {
    std::string name;
    std::vector<std::string> types;
    // Go RE2 subscription patterns; older servers omit the field.
    std::vector<std::string> patterns;
    long long cursor = 0;
    long long pending = 0;
    long long delivered = 0;
    long long failed = 0;
    std::optional<std::string> last_error;
    std::optional<std::string> last_event;
    std::optional<std::string> last_at;

    static ConsumerStats from_json(const json &value) {
        if (!value.is_object()) {
            throw Error("consumer stats must be a JSON object");
        }

        ConsumerStats stats;
        stats.name = detail::string_field(value, "name");
        stats.cursor = detail::int_field(value, "cursor");
        stats.pending = detail::int_field(value, "pending");
        stats.delivered = detail::int_field(value, "delivered");
        stats.failed = detail::int_field(value, "failed");
        stats.last_error = detail::optional_string_field(value, "last_error");
        stats.last_event = detail::optional_string_field(value, "last_event");
        stats.last_at = detail::optional_string_field(value, "last_at");

        const auto *types = detail::find_field(value, "types");
        if (types != nullptr && !types->is_null()) {
            if (!types->is_array()) {
                throw Error("consumer stats types must be a JSON array");
            }
            for (const auto &item : *types) {
                if (item.is_string()) {
                    stats.types.push_back(item.get<std::string>());
                }
            }
        }

        const auto *patterns = detail::find_field(value, "patterns");
        if (patterns != nullptr && !patterns->is_null()) {
            if (!patterns->is_array()) {
                throw Error("consumer stats patterns must be a JSON array");
            }
            for (const auto &item : *patterns) {
                if (item.is_string()) {
                    stats.patterns.push_back(item.get<std::string>());
                }
            }
        }
        return stats;
    }

    json to_json() const {
        json value{{"name", name},
                   {"types", types},
                   {"cursor", cursor},
                   {"pending", pending},
                   {"delivered", delivered},
                   {"failed", failed}};
        if (!patterns.empty()) {
            value["patterns"] = patterns;
        }
        if (last_error) {
            value["last_error"] = *last_error;
        }
        if (last_event) {
            value["last_event"] = *last_event;
        }
        if (last_at) {
            value["last_at"] = *last_at;
        }
        return value;
    }
};

struct Failure {
    std::string consumer;
    std::string event_id;
    std::string source;
    std::string error;
    std::string at;

    static Failure from_json(const json &value) {
        if (!value.is_object()) {
            throw Error("failure must be a JSON object");
        }
        return Failure{
            detail::string_field(value, "consumer"),
            detail::string_field(value, "event_id"),
            detail::string_field(value, "source"),
            detail::string_field(value, "error"),
            detail::string_field(value, "at"),
        };
    }
};

// ---- options --------------------------------------------------------------

struct EventsQuery {
    int limit = 50; // <= 0 returns every matching event
    int offset = 0;
    std::optional<std::string> type;
};

struct CaptureOptions {
    std::string source = "cpp";
    std::vector<std::string> types{"raw.captured"};
    json data = json::object();
};

struct ClientOptions {
    std::chrono::milliseconds request_timeout = default_request_timeout;
    // Bearer token for the service port. Empty means no Authorization header,
    // which matches a server without an auth-token configured.
    std::string token;
};

struct ConsumerOptions {
    std::chrono::milliseconds poll_timeout = default_poll_timeout;
    // Go RE2 patterns matched against each event type. Combined with the
    // exact types passed to the Consumer constructor (union semantics).
    std::vector<std::string> patterns;
};

// ---- client ---------------------------------------------------------------

class Client {
public:
    explicit Client(
        std::string base_url = "http://127.0.0.1:8081",
        ClientOptions options = {})
        : base_url_(std::move(base_url)), options_(options) {
        while (!base_url_.empty() && base_url_.back() == '/') {
            base_url_.pop_back();
        }
        if (base_url_.empty()) {
            throw Error("base URL is required");
        }
        if (options_.request_timeout.count() <= 0) {
            throw Error("request timeout must be positive");
        }
        if (options_.token.find('\r') != std::string::npos ||
            options_.token.find('\n') != std::string::npos) {
            throw Error("auth token must not contain CR or LF");
        }
        detail::ensure_curl();
    }

    const std::string &base_url() const noexcept { return base_url_; }
    const ClientOptions &options() const noexcept { return options_; }

    std::string health() const {
        const auto response =
            request("GET", "/healthz", std::nullopt, std::nullopt);
        detail::expect_status(response, "GET", base_url_ + "/healthz", {200});

        auto body = response.body;
        while (!body.empty() &&
               std::isspace(static_cast<unsigned char>(body.back())) != 0) {
            body.pop_back();
        }
        return body;
    }

    PublishResult publish(const Event &event) const {
        return publish_json(event.to_json());
    }

    PublishResult publish(const json &event) const {
        return publish_json(event);
    }

    PublishResult capture(std::string_view text,
                          CaptureOptions options = {}) const {
        if (!options.data.is_object()) {
            throw Error("capture data must be a JSON object");
        }
        options.data["text"] = std::string(text);
        return publish(Event::create(std::move(options.source),
                                     std::move(options.types),
                                     std::move(options.data)));
    }

    std::vector<Event> events(EventsQuery query = {}) const {
        if (query.limit < 0) {
            query.limit = 0;
        }
        if (query.offset < 0) {
            query.offset = 0;
        }

        std::ostringstream route;
        route << "/events?limit=" << query.limit << "&offset=" << query.offset;
        if (query.type && !query.type->empty()) {
            route << "&type=" << detail::url_encode(*query.type);
        }
        const std::string path = route.str();

        const auto response = request("GET", path, std::nullopt, std::nullopt);
        detail::expect_status(response, "GET", base_url_ + path, {200});
        return parse_events(response.body);
    }

    std::vector<ConsumerStats> stats() const {
        const auto response =
            request("GET", "/stats", std::nullopt, std::nullopt);
        detail::expect_status(response, "GET", base_url_ + "/stats", {200});

        const auto parsed = detail::parse_json(response.body, "stats");
        if (!parsed.is_array()) {
            throw Error("stats: expected a JSON array");
        }
        std::vector<ConsumerStats> result;
        result.reserve(parsed.size());
        for (const auto &item : parsed) {
            result.push_back(ConsumerStats::from_json(item));
        }
        return result;
    }

    std::vector<Failure> failures() const {
        const auto response =
            request("GET", "/failures", std::nullopt, std::nullopt);
        detail::expect_status(response, "GET", base_url_ + "/failures", {200});

        const auto parsed = detail::parse_json(response.body, "failures");
        if (!parsed.is_array()) {
            throw Error("failures: expected a JSON array");
        }
        std::vector<Failure> result;
        result.reserve(parsed.size());
        for (const auto &item : parsed) {
            result.push_back(Failure::from_json(item));
        }
        return result;
    }

    ConsumerStats register_consumer(
        std::string_view name, std::vector<std::string> types,
        std::vector<std::string> patterns = {}) const {
        if (name.empty()) {
            throw Error("consumer name is required");
        }
        json body = detail::register_body(std::move(types),
                                          std::move(patterns));
        const std::string path = detail::consumer_path(name);
        const auto response = request("PUT", path,
                                      detail::dump_json(body, "register"),
                                      std::nullopt);
        detail::expect_status(response, "PUT", base_url_ + path, {200, 201});
        return ConsumerStats::from_json(
            detail::parse_json(response.body, "register"));
    }

    void deregister(std::string_view name) const {
        if (name.empty()) {
            throw Error("consumer name is required");
        }
        const std::string path = detail::consumer_path(name);
        const auto response =
            request("DELETE", path, std::nullopt, std::nullopt);
        detail::expect_status(response, "DELETE", base_url_ + path, {204});
    }

    std::optional<Event> next(
        std::string_view name,
        std::chrono::milliseconds timeout = default_poll_timeout) const {
        if (name.empty()) {
            throw Error("consumer name is required");
        }
        if (timeout.count() <= 0) {
            throw Error("poll timeout must be positive");
        }

        const std::string path = detail::next_path(name, timeout);
        const auto socket_timeout =
            std::max(options_.request_timeout,
                     timeout + std::chrono::seconds(5));
        const auto response =
            request("GET", path, std::nullopt, socket_timeout);

        if (response.status == 204) {
            return std::nullopt;
        }
        detail::expect_status(response, "GET", base_url_ + path, {200});

        const auto parsed = detail::parse_json(response.body, "next");
        const auto *event = detail::find_field(parsed, "event");
        if (event == nullptr) {
            throw Error("next: response does not contain an event");
        }
        return Event::from_json(*event);
    }

    void ack(std::string_view name, std::string_view event_id = {}) const {
        if (name.empty()) {
            throw Error("consumer name is required");
        }
        json body = json::object();
        if (!event_id.empty()) {
            body["id"] = std::string(event_id);
        }
        const std::string path = detail::consumer_path(name, "/ack");
        const auto response = request("POST", path,
                                      detail::dump_json(body, "ack"),
                                      std::nullopt);
        detail::expect_status(response, "POST", base_url_ + path, {204});
    }

    void nack(std::string_view name, std::string_view error,
              std::string_view event_id = {}) const {
        if (name.empty()) {
            throw Error("consumer name is required");
        }
        json body;
        body["error"] = std::string(error);
        if (!event_id.empty()) {
            body["id"] = std::string(event_id);
        }
        const std::string path = detail::consumer_path(name, "/nack");
        const auto response = request("POST", path,
                                      detail::dump_json(body, "nack"),
                                      std::nullopt);
        detail::expect_status(response, "POST", base_url_ + path, {204});
    }

private:
    detail::Response request(
        std::string_view method, std::string_view path,
        std::optional<std::string> body,
        std::optional<std::chrono::milliseconds> timeout) const {
        return detail::http_request(base_url_, method, path, body,
                                    timeout.value_or(options_.request_timeout),
                                    options_.token);
    }

    PublishResult publish_json(const json &event) const {
        if (!event.is_object()) {
            throw Error("event must be a JSON object");
        }
        const auto response =
            request("POST", "/events", detail::dump_json(event, "publish"),
                    std::nullopt);
        detail::expect_status(response, "POST", base_url_ + "/events",
                              {200, 201});
        return PublishResult::from_json(
            detail::parse_json(response.body, "publish"));
    }

    std::vector<Event> parse_events(const std::string &body) const {
        const auto parsed = detail::parse_json(body, "events");
        if (!parsed.is_array()) {
            throw Error("events: expected a JSON array");
        }
        std::vector<Event> result;
        result.reserve(parsed.size());
        for (const auto &item : parsed) {
            result.push_back(Event::from_json(item));
        }
        return result;
    }

    std::string base_url_;
    ClientOptions options_;
};

// ---- message --------------------------------------------------------------

class Message {
public:
    Message(const Message &) = default;
    Message &operator=(const Message &) = default;
    Message(Message &&) noexcept = default;
    Message &operator=(Message &&) noexcept = default;

    const Event &event() const noexcept { return event_; }
    const std::string &consumer() const noexcept { return consumer_; }

    void ack() const { client_.ack(consumer_, event_.id); }

    void nack(std::string_view error) const {
        client_.nack(consumer_, error, event_.id);
    }

private:
    friend class Consumer;

    Message(Client client, std::string consumer, Event event)
        : client_(std::move(client)), consumer_(std::move(consumer)),
          event_(std::move(event)) {}

    Client client_;
    std::string consumer_;
    Event event_;
};

// ---- consumer -------------------------------------------------------------

class Consumer {
public:
    class Iterator;

    Consumer(const Client &client, std::string name,
             std::vector<std::string> types, ConsumerOptions options = {})
        : client_(client), name_(std::move(name)),
          types_(detail::normalize_types(std::move(types))),
          patterns_(detail::normalize_patterns(std::move(options.patterns))),
          options_(std::move(options)) {
        if (name_.empty()) {
            throw Error("consumer name is required");
        }
        if (types_.empty() && patterns_.empty()) {
            throw Error("consumer needs at least one type or pattern");
        }
        if (options_.poll_timeout.count() <= 0) {
            throw Error("poll timeout must be positive");
        }
        client_.register_consumer(name_, types_, patterns_);
    }

    ~Consumer() = default;

    Consumer(const Consumer &) = delete;
    Consumer &operator=(const Consumer &) = delete;

    Consumer(Consumer &&other) noexcept
        : client_(std::move(other.client_)), name_(std::move(other.name_)),
          types_(std::move(other.types_)),
          patterns_(std::move(other.patterns_)), options_(other.options_),
          closed_(other.closed_) {
        other.closed_ = true;
    }

    // Move assignment is disabled: overwriting a live local handle would
    // silently leave its server-side registration behind.
    Consumer &operator=(Consumer &&) = delete;

    const std::string &name() const noexcept { return name_; }
    const std::vector<std::string> &types() const noexcept { return types_; }
    const std::vector<std::string> &patterns() const noexcept {
        return patterns_;
    }
    bool closed() const noexcept { return closed_; }

    ConsumerStats stats() const {
        const auto all = client_.stats();
        const auto found = std::find_if(
            all.begin(), all.end(), [this](const ConsumerStats &item) {
                return item.name == name_;
            });
        if (found == all.end()) {
            throw Error("consumer '" + name_ + "' not found in stats");
        }
        return *found;
    }

    std::optional<Message> next() const { return next(options_.poll_timeout); }

    std::optional<Message> next(std::chrono::milliseconds timeout) const {
        if (closed_) {
            throw Error("consumer '" + name_ + "' is closed");
        }

        auto event = client_.next(name_, timeout);
        if (!event) {
            return std::nullopt;
        }
        return Message(client_, name_, std::move(*event));
    }

    void close() noexcept { closed_ = true; }

    void deregister() {
        if (name_.empty()) {
            throw Error("consumer is moved-from");
        }
        client_.deregister(name_);
        closed_ = true;
    }

    Iterator begin() const;
    Iterator end() const noexcept;

private:
    Client client_;
    std::string name_;
    std::vector<std::string> types_;
    std::vector<std::string> patterns_;
    ConsumerOptions options_;
    bool closed_ = false;
};

class Consumer::Iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Message;
    using difference_type = std::ptrdiff_t;
    using pointer = const Message *;
    using reference = const Message &;

    Iterator() noexcept = default;

    reference operator*() const { return *current_; }
    pointer operator->() const { return &*current_; }

    Iterator &operator++() {
        advance();
        return *this;
    }

    Iterator operator++(int) {
        Iterator copy = *this;
        advance();
        return copy;
    }

    friend bool operator==(const Iterator &left,
                           const Iterator &right) noexcept {
        return left.consumer_ == right.consumer_;
    }

    friend bool operator!=(const Iterator &left,
                           const Iterator &right) noexcept {
        return !(left == right);
    }

private:
    friend class Consumer;

    explicit Iterator(const Consumer &consumer) : consumer_(&consumer) {
        advance();
    }

    void advance() {
        while (consumer_ != nullptr) {
            if (consumer_->closed()) {
                consumer_ = nullptr;
                current_.reset();
                return;
            }
            try {
                auto message = consumer_->next();
                if (message) {
                    current_ = std::move(message);
                    return;
                }
            } catch (const Error &) {
                if (consumer_->closed()) {
                    consumer_ = nullptr;
                    current_.reset();
                    return;
                }
                throw;
            }
        }
    }

    const Consumer *consumer_ = nullptr;
    std::optional<Message> current_;
};

inline Consumer::Iterator Consumer::begin() const { return Iterator(*this); }

inline Consumer::Iterator Consumer::end() const noexcept { return Iterator(); }

} // namespace yi

#endif // YI_HPP
