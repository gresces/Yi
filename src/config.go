package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// defaultConfigPath is where the packaged service and a bare `yi` look for
// configuration. A missing default file is not an error; an explicitly
// requested file must exist.
const defaultConfigPath = "/etc/yi/yi.config"

type settings struct {
	Host         string
	ServicePort  int
	FrontendPort int
	Frontend     bool
	Data         string
	ArchiveBatch int64
	AuthToken    string
}

func defaultSettings() settings {
	return settings{
		Host:         "127.0.0.1",
		ServicePort:  8081,
		FrontendPort: 8080,
		Frontend:     true,
		Data:         "data",
		ArchiveBatch: 256,
	}
}

func (s settings) validate() error {
	if strings.TrimSpace(s.Host) == "" {
		return errors.New("host must not be empty")
	}
	if strings.TrimSpace(s.Data) == "" {
		return errors.New("data must not be empty")
	}
	if s.ServicePort < 0 || s.ServicePort > 65535 {
		return fmt.Errorf("service-port %d is out of range 0-65535", s.ServicePort)
	}
	if s.FrontendPort < 0 || s.FrontendPort > 65535 {
		return fmt.Errorf("frontend-port %d is out of range 0-65535", s.FrontendPort)
	}
	if err := validateAuthToken(s.AuthToken); err != nil {
		return err
	}
	return nil
}

type serverConfig struct {
	Settings     settings
	ConfigPath   string
	ConfigLoaded bool
	ShowVersion  bool
}

// parseServerArgs parses command-line flags, then merges a config file under
// them. Precedence is: defaults < config file < explicitly set flags.
//
// fallbackConfigPath is injected so tests do not depend on /etc.
func parseServerArgs(args []string, fallbackConfigPath string) (serverConfig, error) {
	defaults := defaultSettings()

	fs := flag.NewFlagSet("yi", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: yi [flags]")
		fmt.Fprintln(fs.Output())
		fs.PrintDefaults()
	}

	configPath := fs.String("config", fallbackConfigPath, "配置文件路径")
	showVersion := fs.Bool("version", false, "打印版本并退出")
	host := fs.String("host", defaults.Host, "监听地址")
	servicePort := fs.Int("service-port", defaults.ServicePort, "服务端口")
	frontendPort := fs.Int("frontend-port", defaults.FrontendPort, "前端端口")
	withFrontend := fs.Bool("frontend", defaults.Frontend, "是否启动前端")
	dataDir := fs.String("data", defaults.Data, "数据目录")
	archiveEvery := fs.Int64("archive-batch", defaults.ArchiveBatch, "归档批量;0 表示禁用归档")
	authToken := fs.String("auth-token", defaults.AuthToken, "Bearer 认证令牌;为空表示关闭认证")

	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if fs.NArg() > 0 {
		return serverConfig{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	result := serverConfig{
		ConfigPath:  *configPath,
		ShowVersion: *showVersion,
	}
	if *showVersion {
		return result, nil
	}

	if strings.TrimSpace(*configPath) == "" {
		return serverConfig{}, errors.New("config path must not be empty")
	}

	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		explicit[f.Name] = true
	})

	s := defaults
	cfg, err := loadConfigFile(*configPath)
	switch {
	case err == nil:
		if err := cfg.apply(&s); err != nil {
			return serverConfig{}, err
		}
		result.ConfigLoaded = true
	case errors.Is(err, os.ErrNotExist) && !explicit["config"]:
		// The default config path is optional.
	default:
		return serverConfig{}, err
	}

	// Explicit flags win over the config file.
	if explicit["host"] {
		s.Host = *host
	}
	if explicit["service-port"] {
		s.ServicePort = *servicePort
	}
	if explicit["frontend-port"] {
		s.FrontendPort = *frontendPort
	}
	if explicit["frontend"] {
		s.Frontend = *withFrontend
	}
	if explicit["data"] {
		s.Data = *dataDir
	}
	if explicit["archive-batch"] {
		s.ArchiveBatch = *archiveEvery
	}
	if explicit["auth-token"] {
		s.AuthToken = *authToken
	}

	if err := s.validate(); err != nil {
		return serverConfig{}, err
	}

	result.Settings = s
	return result, nil
}

// ---- config file ----------------------------------------------------------

type configValue struct {
	text string
	line int
}

type fileConfig struct {
	path   string
	values map[string]configValue
}

var knownConfigKeys = map[string]struct{}{
	"host":          {},
	"service-port":  {},
	"frontend-port": {},
	"frontend":      {},
	"data":          {},
	"archive-batch": {},
	"auth-token":    {},
}

// loadConfigFile reads a small key = value file. Blank lines and lines whose
// first non-space character is '#' or ';' are ignored. Keys accept '-' or '_'
// separators. Values may be double- or single-quoted.
func loadConfigFile(path string) (fileConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileConfig{}, err
	}
	defer file.Close()

	cfg := fileConfig{path: path, values: make(map[string]configValue)}
	scanner := bufio.NewScanner(file)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if lineNo == 1 {
			line = strings.TrimSpace(strings.TrimPrefix(line, "\ufeff"))
		}
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		key, raw, found := strings.Cut(line, "=")
		if !found {
			return fileConfig{}, fmt.Errorf("%s:%d: expected key = value", path, lineNo)
		}

		key = normalizeConfigKey(key)
		if key == "" {
			return fileConfig{}, fmt.Errorf("%s:%d: empty key", path, lineNo)
		}
		if _, ok := knownConfigKeys[key]; !ok {
			return fileConfig{}, fmt.Errorf("%s:%d: unknown key %q", path, lineNo, key)
		}

		cfg.values[key] = configValue{
			text: strings.TrimSpace(raw),
			line: lineNo,
		}
	}
	if err := scanner.Err(); err != nil {
		return fileConfig{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func normalizeConfigKey(key string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "_", "-")
}

func (c fileConfig) errorAt(value configValue, format string, args ...any) error {
	return fmt.Errorf("%s:%d: %s", c.path, value.line, fmt.Sprintf(format, args...))
}

func (c fileConfig) apply(s *settings) error {
	if value, ok := c.values["host"]; ok {
		parsed, err := c.parseString(value)
		if err != nil {
			return err
		}
		if strings.TrimSpace(parsed) == "" {
			return c.errorAt(value, "host must not be empty")
		}
		s.Host = parsed
	}

	if value, ok := c.values["service-port"]; ok {
		parsed, err := c.parseInt(value, 0, 65535)
		if err != nil {
			return err
		}
		s.ServicePort = parsed
	}

	if value, ok := c.values["frontend-port"]; ok {
		parsed, err := c.parseInt(value, 0, 65535)
		if err != nil {
			return err
		}
		s.FrontendPort = parsed
	}

	if value, ok := c.values["frontend"]; ok {
		parsed, err := strconv.ParseBool(value.text)
		if err != nil {
			return c.errorAt(value, "frontend: expected boolean, got %q", value.text)
		}
		s.Frontend = parsed
	}

	if value, ok := c.values["data"]; ok {
		parsed, err := c.parseString(value)
		if err != nil {
			return err
		}
		if strings.TrimSpace(parsed) == "" {
			return c.errorAt(value, "data must not be empty")
		}
		s.Data = parsed
	}

	if value, ok := c.values["archive-batch"]; ok {
		parsed, err := strconv.ParseInt(value.text, 10, 64)
		if err != nil {
			return c.errorAt(value, "archive-batch: expected integer, got %q", value.text)
		}
		s.ArchiveBatch = parsed
	}

	if value, ok := c.values["auth-token"]; ok {
		parsed, err := c.parseString(value)
		if err != nil {
			return err
		}
		if err := validateAuthToken(parsed); err != nil {
			return c.errorAt(value, "%v", err)
		}
		s.AuthToken = parsed
	}

	return nil
}

func (c fileConfig) parseString(value configValue) (string, error) {
	text := value.text
	if text == "" {
		return "", nil
	}
	if text[0] != '"' && text[0] != '\'' {
		return text, nil
	}
	if len(text) < 2 || text[len(text)-1] != text[0] {
		return "", c.errorAt(value, "unterminated quoted value")
	}
	if text[0] == '\'' {
		return text[1 : len(text)-1], nil
	}
	parsed, err := strconv.Unquote(text)
	if err != nil {
		return "", c.errorAt(value, "invalid quoted value: %v", err)
	}
	return parsed, nil
}

func (c fileConfig) parseInt(value configValue, min, max int) (int, error) {
	parsed, err := strconv.Atoi(value.text)
	if err != nil {
		return 0, c.errorAt(value, "expected integer, got %q", value.text)
	}
	if parsed < min || parsed > max {
		return 0, c.errorAt(value, "value %d is out of range %d-%d", parsed, min, max)
	}
	return parsed, nil
}
