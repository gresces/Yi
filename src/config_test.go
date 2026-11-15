package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "yi.config")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultConfigPath(t *testing.T) {
	if defaultConfigPath != "/etc/yi/yi.config" {
		t.Fatalf("defaultConfigPath = %q", defaultConfigPath)
	}

	opts, err := parseInstallArgs(nil)
	if err != nil {
		t.Fatalf("parseInstallArgs: %v", err)
	}
	if opts.ConfigPath != defaultConfigPath {
		t.Errorf("install config path = %q", opts.ConfigPath)
	}
}

func TestLoadConfigFileAppliesValues(t *testing.T) {
	path := writeConfigFile(t, `
# a comment
; another comment
HOST = 0.0.0.0
service_port = 9091
frontend-port = 9092
frontend = false
data = "/var/lib/yi data"
archive-batch = 7
`)

	cfg, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}

	s := defaultSettings()
	if err := cfg.apply(&s); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if s.Host != "0.0.0.0" {
		t.Errorf("host = %q", s.Host)
	}
	if s.ServicePort != 9091 {
		t.Errorf("service-port = %d", s.ServicePort)
	}
	if s.FrontendPort != 9092 {
		t.Errorf("frontend-port = %d", s.FrontendPort)
	}
	if s.Frontend {
		t.Error("frontend should be false")
	}
	if s.Data != "/var/lib/yi data" {
		t.Errorf("data = %q", s.Data)
	}
	if s.ArchiveBatch != 7 {
		t.Errorf("archive-batch = %d", s.ArchiveBatch)
	}
}

func TestGeneratedServiceConfigIsValid(t *testing.T) {
	path := writeConfigFile(t, renderServiceConfig("yi"))

	cfg, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	s := defaultSettings()
	if err := cfg.apply(&s); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s.Data != "/var/lib/yi" {
		t.Errorf("data = %q", s.Data)
	}
	if !s.Frontend {
		t.Error("generated config should enable the frontend")
	}
}

func TestLoadConfigFileSingleQuotesAndBOM(t *testing.T) {
	path := writeConfigFile(t, "\ufeffdata = '/var/lib/yi'\n")

	cfg, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	s := defaultSettings()
	if err := cfg.apply(&s); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s.Data != "/var/lib/yi" {
		t.Errorf("data = %q", s.Data)
	}
}

func TestLoadConfigFileErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"unknown key", "bogus = 1\n", `unknown key "bogus"`},
		{"missing equals", "host\n", "expected key = value"},
		{"empty key", "= value\n", "empty key"},
		{"bad integer", "service-port = nope\n", "expected integer"},
		{"port range", "service-port = 70000\n", "out of range"},
		{"bad bool", "frontend = maybe\n", "expected boolean"},
		{"unterminated quote", "data = \"unterminated\n", "unterminated quoted value"},
		{"bad archive", "archive-batch = nope\n", "expected integer"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeConfigFile(t, test.content)
			cfg, err := loadConfigFile(path)
			if err == nil {
				s := defaultSettings()
				err = cfg.apply(&s)
			}
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error %q does not contain %q", err, test.want)
			}
			if !strings.Contains(err.Error(), ":1:") {
				t.Fatalf("error %q does not include a line number", err)
			}
		})
	}
}

func TestLoadConfigFileMissing(t *testing.T) {
	_, err := loadConfigFile(filepath.Join(t.TempDir(), "absent.config"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestParseServerArgsPrecedence(t *testing.T) {
	path := writeConfigFile(t, `
host = 0.0.0.0
service-port = 9091
frontend-port = 9092
frontend = false
data = /var/lib/from-config
archive-batch = 7
`)

	result, err := parseServerArgs([]string{
		"-config", path,
		"-host", "127.0.0.1",
		"-service-port", "9999",
		"-frontend=true",
	}, filepath.Join(t.TempDir(), "absent.config"))
	if err != nil {
		t.Fatalf("parseServerArgs: %v", err)
	}

	if !result.ConfigLoaded {
		t.Error("expected the config to be marked as loaded")
	}
	if result.Settings.Host != "127.0.0.1" {
		t.Errorf("host = %q", result.Settings.Host)
	}
	if result.Settings.ServicePort != 9999 {
		t.Errorf("service-port = %d", result.Settings.ServicePort)
	}
	if result.Settings.FrontendPort != 9092 {
		t.Errorf("frontend-port = %d", result.Settings.FrontendPort)
	}
	if !result.Settings.Frontend {
		t.Error("frontend should be true")
	}
	if result.Settings.Data != "/var/lib/from-config" {
		t.Errorf("data = %q", result.Settings.Data)
	}
	if result.Settings.ArchiveBatch != 7 {
		t.Errorf("archive-batch = %d", result.Settings.ArchiveBatch)
	}
}

func TestParseServerArgsDefaultConfigIsOptional(t *testing.T) {
	defaultPath := filepath.Join(t.TempDir(), "absent.config")

	result, err := parseServerArgs(nil, defaultPath)
	if err != nil {
		t.Fatalf("parseServerArgs: %v", err)
	}
	if result.ConfigLoaded {
		t.Error("missing default config should not be marked as loaded")
	}
	if result.Settings != defaultSettings() {
		t.Errorf("settings = %+v, want defaults", result.Settings)
	}
}

func TestParseServerArgsExplicitMissingConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.config")

	_, err := parseServerArgs([]string{"-config", missing}, missing)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestParseServerArgsVersionSkipsConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.config")

	result, err := parseServerArgs([]string{"-version", "-config", missing}, missing)
	if err != nil {
		t.Fatalf("parseServerArgs: %v", err)
	}
	if !result.ShowVersion {
		t.Error("expected ShowVersion")
	}
}

func TestParseServerArgsRejectsEmptyValues(t *testing.T) {
	if _, err := parseServerArgs([]string{"-config", ""}, defaultConfigPath); err == nil {
		t.Fatal("expected an error for an empty config path")
	}

	path := writeConfigFile(t, "host = \"\"\n")
	if _, err := parseServerArgs([]string{"-config", path}, defaultConfigPath); err == nil {
		t.Fatal("expected an error for an empty host")
	}
}
