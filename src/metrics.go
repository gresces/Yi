package main

import (
	"fmt"
	"strings"

	"yi/src/bus"
)

// renderPrometheus emits the Prometheus text exposition format (version 0.0.4).
func renderPrometheus(stats bus.PrometheusStats, version string) string {
	var out strings.Builder

	writeMetricHeader(&out, "yi_up", "gauge", "Whether the Yi process is running.")
	fmt.Fprintln(&out, "yi_up 1")

	writeMetricHeader(&out, "yi_build_info", "gauge", "Build information for this Yi process.")
	fmt.Fprintf(&out, "yi_build_info{version=\"%s\"} 1\n", escapePrometheusLabel(version))

	writeMetricHeader(&out, "yi_events_active", "gauge", "Active (non-deleted, non-archived) events.")
	fmt.Fprintf(&out, "yi_events_active %d\n", stats.Active)

	writeMetricHeader(&out, "yi_events_published_total", "counter", "Events successfully published by this process.")
	fmt.Fprintf(&out, "yi_events_published_total %d\n", stats.PublishedTotal)

	writeMetricHeader(&out, "yi_events_acked_total", "counter", "Events acknowledged by this process.")
	fmt.Fprintf(&out, "yi_events_acked_total %d\n", stats.AckedTotal)

	writeMetricHeader(&out, "yi_events_nacked_total", "counter", "Events nacked by this process.")
	fmt.Fprintf(&out, "yi_events_nacked_total %d\n", stats.NackedTotal)

	writeMetricHeader(&out, "yi_failures", "gauge", "Recorded failure rows.")
	fmt.Fprintf(&out, "yi_failures %d\n", stats.Failures)

	writeMetricHeader(&out, "yi_consumers", "gauge", "Registered consumers.")
	fmt.Fprintf(&out, "yi_consumers %d\n", len(stats.Consumers))

	writeMetricHeader(&out, "yi_consumer_pending", "gauge", "Events waiting for each consumer.")
	for _, consumer := range stats.Consumers {
		label := escapePrometheusLabel(consumer.Name)
		fmt.Fprintf(&out, "yi_consumer_pending{consumer=\"%s\"} %d\n", label, consumer.Pending)
	}

	writeMetricHeader(&out, "yi_consumer_cursor", "gauge", "Last acknowledged event sequence for each consumer.")
	for _, consumer := range stats.Consumers {
		label := escapePrometheusLabel(consumer.Name)
		fmt.Fprintf(&out, "yi_consumer_cursor{consumer=\"%s\"} %d\n", label, consumer.Cursor)
	}

	writeMetricHeader(&out, "yi_consumer_delivered_total", "counter", "Lifetime deliveries per consumer.")
	for _, consumer := range stats.Consumers {
		label := escapePrometheusLabel(consumer.Name)
		fmt.Fprintf(&out, "yi_consumer_delivered_total{consumer=\"%s\"} %d\n", label, consumer.Delivered)
	}

	writeMetricHeader(&out, "yi_consumer_failed_total", "counter", "Lifetime nacks per consumer.")
	for _, consumer := range stats.Consumers {
		label := escapePrometheusLabel(consumer.Name)
		fmt.Fprintf(&out, "yi_consumer_failed_total{consumer=\"%s\"} %d\n", label, consumer.Failed)
	}

	writeMetricHeader(&out, "yi_producer_events", "gauge", "Active events per producer source.")
	for _, producer := range stats.Producers {
		label := escapePrometheusLabel(producer.Source)
		fmt.Fprintf(&out, "yi_producer_events{source=\"%s\"} %d\n", label, producer.Count)
	}

	writeMetricHeader(&out, "yi_events_by_type", "gauge", "Active events per event type.")
	for _, item := range stats.Types {
		label := escapePrometheusLabel(string(item.Type))
		fmt.Fprintf(&out, "yi_events_by_type{type=\"%s\"} %d\n", label, item.Count)
	}

	return out.String()
}

func writeMetricHeader(out *strings.Builder, name, kind, help string) {
	fmt.Fprintf(out, "# HELP %s %s\n", name, help)
	fmt.Fprintf(out, "# TYPE %s %s\n", name, kind)
}

func escapePrometheusLabel(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
	)
	return replacer.Replace(value)
}
