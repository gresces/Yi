package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderServiceUnit(t *testing.T) {
	opts := installOptions{
		Name:       "yi",
		BinPath:    "/usr/local/bin/yi",
		ConfigPath: "/etc/yi/yi.config",
	}

	unit := renderServiceUnit(opts)
	for _, want := range []string{
		"ExecStart=/usr/local/bin/yi -config /etc/yi/yi.config",
		"StateDirectory=yi",
		"WorkingDirectory=/var/lib/yi",
		"DynamicUser=yes",
		"ProtectSystem=strict",
		"WantedBy=multi-user.target",
	} {
		if !strings.Contains(unit, want) {
			t.Errorf("unit does not contain %q:\n%s", want, unit)
		}
	}
}

func TestRenderServiceUnitEscapesPaths(t *testing.T) {
	opts := installOptions{
		Name:       "yi-test",
		BinPath:    "/opt/yi bin/yi",
		ConfigPath: "/etc/yi/yi test.config",
	}

	unit := renderServiceUnit(opts)
	if !strings.Contains(unit,
		`ExecStart="/opt/yi bin/yi" -config "/etc/yi/yi test.config"`) {
		t.Errorf("paths were not escaped:\n%s", unit)
	}
}

func TestSystemdEscape(t *testing.T) {
	tests := map[string]string{
		"/usr/local/bin/yi": "/usr/local/bin/yi",
		"/opt/yi bin/yi":    `"/opt/yi bin/yi"`,
		`/opt/a"b/yi`:       `"/opt/a\"b/yi"`,
		`/opt/a\b/yi`:       `"/opt/a\\b/yi"`,
		"":                  `""`,
	}

	for input, want := range tests {
		if got := systemdEscape(input); got != want {
			t.Errorf("systemdEscape(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestInstallServiceWritesFiles(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source-yi")
	if err := os.WriteFile(source, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}

	opts := installOptions{
		ConfigPath: filepath.Join(root, "etc", "yi.config"),
		UnitDir:    filepath.Join(root, "systemd"),
		Name:       "yi-test",
		BinPath:    filepath.Join(root, "bin", "yi"),
		ExecPath:   source,
		NoStart:    true,
	}

	var out bytes.Buffer
	if err := installService(opts, &out); err != nil {
		t.Fatalf("installService: %v", err)
	}

	bin, err := os.ReadFile(opts.BinPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(bin) != "binary" {
		t.Errorf("installed binary = %q", bin)
	}
	info, err := os.Stat(opts.BinPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("installed binary is not executable: %v", info.Mode())
	}

	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"data = /var/lib/yi-test",
		"service-port = 8081",
	} {
		if !strings.Contains(string(config), want) {
			t.Errorf("config does not contain %q:\n%s", want, config)
		}
	}

	unitPath := unitFilePath(mustNormalizeInstall(t, opts))
	unit, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(unit), "ExecStart="+opts.BinPath+" -config "+opts.ConfigPath) {
		t.Errorf("unit has an unexpected ExecStart:\n%s", unit)
	}

	for _, want := range []string{
		"wrote config",
		"wrote unit",
		"skipped systemctl",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output does not contain %q:\n%s", want, out.String())
		}
	}
}

func TestInstallServiceKeepsExistingConfig(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source-yi")
	if err := os.WriteFile(source, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}

	opts := installOptions{
		ConfigPath: filepath.Join(root, "etc", "yi.config"),
		UnitDir:    filepath.Join(root, "systemd"),
		Name:       "yi-test",
		BinPath:    filepath.Join(root, "bin", "yi"),
		ExecPath:   source,
		NoStart:    true,
	}
	if err := os.MkdirAll(filepath.Dir(opts.ConfigPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opts.ConfigPath, []byte("host = 0.0.0.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := installService(opts, &out); err != nil {
		t.Fatalf("installService: %v", err)
	}

	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(config) != "host = 0.0.0.0\n" {
		t.Errorf("existing config was overwritten: %q", config)
	}
	if !strings.Contains(out.String(), "keeping existing config") {
		t.Errorf("unexpected output:\n%s", out.String())
	}
}

func TestInstallServiceDryRun(t *testing.T) {
	root := t.TempDir()
	opts := installOptions{
		ConfigPath: filepath.Join(root, "etc", "yi.config"),
		UnitDir:    filepath.Join(root, "systemd"),
		Name:       "yi-test",
		BinPath:    filepath.Join(root, "bin", "yi"),
		ExecPath:   filepath.Join(root, "source-yi"),
		DryRun:     true,
	}

	var out bytes.Buffer
	if err := installService(opts, &out); err != nil {
		t.Fatalf("installService: %v", err)
	}

	if _, err := os.Stat(opts.ConfigPath); !os.IsNotExist(err) {
		t.Errorf("dry-run wrote config: %v", err)
	}
	if _, err := os.Stat(opts.BinPath); !os.IsNotExist(err) {
		t.Errorf("dry-run wrote binary: %v", err)
	}
	if !strings.Contains(out.String(), "# config:") ||
		!strings.Contains(out.String(), "# unit:") {
		t.Errorf("unexpected dry-run output:\n%s", out.String())
	}
}

func TestInstallServiceRejectsInvalidName(t *testing.T) {
	opts := installOptions{
		ConfigPath: filepath.Join(t.TempDir(), "yi.config"),
		UnitDir:    t.TempDir(),
		Name:       "../evil",
		BinPath:    filepath.Join(t.TempDir(), "yi"),
		ExecPath:   "/bin/true",
	}
	if err := installService(opts, &bytes.Buffer{}); err == nil {
		t.Fatal("expected an error for an invalid service name")
	}
}

func TestUninstallService(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "etc", "yi.config")
	unitDir := filepath.Join(root, "systemd")
	unitPath := filepath.Join(unitDir, "yi-test.service")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unitPath, []byte("[Unit]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("host = 127.0.0.1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := uninstallOptions{
		ConfigPath: configPath,
		UnitDir:    unitDir,
		Name:       "yi-test",
		NoStart:    true,
	}

	var out bytes.Buffer
	if err := uninstallService(opts, &out); err != nil {
		t.Fatalf("uninstallService: %v", err)
	}

	if _, err := os.Stat(unitPath); !os.IsNotExist(err) {
		t.Errorf("unit still exists: %v", err)
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Errorf("config was removed: %v", err)
	}
	if !strings.Contains(out.String(), "config kept at") {
		t.Errorf("unexpected output:\n%s", out.String())
	}
}

func TestParseInstallArgs(t *testing.T) {
	opts, err := parseInstallArgs([]string{
		"-config", "/tmp/yi.config",
		"-unit-dir", "/tmp/systemd",
		"-name", "yi-test",
		"-bin", "/tmp/bin/yi",
		"-exec", "/tmp/source",
		"-no-start",
		"-dry-run",
	})
	if err != nil {
		t.Fatalf("parseInstallArgs: %v", err)
	}
	if opts.ConfigPath != "/tmp/yi.config" ||
		opts.UnitDir != "/tmp/systemd" ||
		opts.Name != "yi-test" ||
		opts.BinPath != "/tmp/bin/yi" ||
		opts.ExecPath != "/tmp/source" ||
		!opts.NoStart || !opts.DryRun {
		t.Errorf("unexpected options: %+v", opts)
	}
}

func TestParseUninstallArgs(t *testing.T) {
	opts, err := parseUninstallArgs([]string{
		"-config", "/tmp/yi.config",
		"-unit-dir", "/tmp/systemd",
		"-name", "yi-test",
		"-no-start",
		"-dry-run",
	})
	if err != nil {
		t.Fatalf("parseUninstallArgs: %v", err)
	}
	if opts.ConfigPath != "/tmp/yi.config" ||
		opts.UnitDir != "/tmp/systemd" ||
		opts.Name != "yi-test" ||
		!opts.NoStart || !opts.DryRun {
		t.Errorf("unexpected options: %+v", opts)
	}
}

func mustNormalizeInstall(t *testing.T, opts installOptions) installOptions {
	t.Helper()
	if err := opts.normalize(); err != nil {
		t.Fatal(err)
	}
	return opts
}
