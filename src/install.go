package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	defaultUnitDir     = "/etc/systemd/system"
	defaultBinPath     = "/usr/local/bin/yi"
	defaultServiceName = "yi"
)

var serviceNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

type installOptions struct {
	User       bool
	ConfigPath string
	UnitDir    string
	Name       string
	BinPath    string
	ExecPath   string
	DataDir    string
	AuthToken  string
	NoStart    bool
	DryRun     bool
}

func defaultInstallOptions() installOptions {
	return installOptions{
		ConfigPath: defaultConfigPath,
		UnitDir:    defaultUnitDir,
		Name:       defaultServiceName,
		BinPath:    defaultBinPath,
	}
}

func parseInstallArgs(args []string) (installOptions, error) {
	opts := defaultInstallOptions()

	fs := flag.NewFlagSet("yi install", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: yi install [flags]")
		fmt.Fprintln(fs.Output(), "       yi install --user [flags]   安装为当前用户的 systemd 服务(不需要 sudo)")
		fmt.Fprintln(fs.Output())
		fs.PrintDefaults()
	}

	fs.BoolVar(&opts.User, "user", false, "安装为当前用户的 systemd 服务(不需要 sudo; 也可写 --user)")
	fs.StringVar(&opts.ConfigPath, "config", opts.ConfigPath, "配置文件路径")
	fs.StringVar(&opts.UnitDir, "unit-dir", opts.UnitDir, "systemd unit 目录")
	fs.StringVar(&opts.Name, "name", opts.Name, "服务名")
	fs.StringVar(&opts.BinPath, "bin", opts.BinPath, "安装后的二进制路径")
	fs.StringVar(&opts.ExecPath, "exec", "", "要安装的二进制(默认当前进程)")
	fs.StringVar(&opts.AuthToken, "auth-token", "", "认证令牌;留空时自动生成(仅新配置)")
	fs.BoolVar(&opts.NoStart, "no-start", false, "只写文件,不调用 systemctl")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "打印生成的文件,不写入")

	if err := fs.Parse(args); err != nil {
		return installOptions{}, err
	}
	if fs.NArg() > 0 {
		return installOptions{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if opts.User {
		home, err := userHomeDir()
		if err != nil {
			return installOptions{}, err
		}
		visited := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })
		if !visited["config"] {
			opts.ConfigPath = filepath.Join(home, ".config", "yi", "yi.config")
		}
		if !visited["unit-dir"] {
			opts.UnitDir = filepath.Join(home, ".config", "systemd", "user")
		}
		if !visited["bin"] {
			opts.BinPath = filepath.Join(home, ".local", "bin", "yi")
		}
	}
	return opts, nil
}

func userHomeDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory: %w", err)
	}
	if strings.TrimSpace(home) == "" {
		return "", errors.New("locate home directory: HOME is empty")
	}
	return home, nil
}

func runInstall(args []string) error {
	opts, err := parseInstallArgs(args)
	if err != nil {
		return err
	}
	return installService(opts, os.Stdout)
}

func (o *installOptions) normalize() error {
	o.Name = strings.TrimSuffix(strings.TrimSpace(o.Name), ".service")
	if err := validateServiceName(o.Name); err != nil {
		return err
	}
	if strings.TrimSpace(o.ConfigPath) == "" {
		return errors.New("config path must not be empty")
	}
	if strings.TrimSpace(o.UnitDir) == "" {
		return errors.New("unit directory must not be empty")
	}
	if strings.TrimSpace(o.BinPath) == "" {
		return errors.New("binary path must not be empty")
	}
	o.AuthToken = strings.TrimSpace(o.AuthToken)
	if err := validateAuthToken(o.AuthToken); err != nil {
		return err
	}

	if o.User {
		if strings.TrimSpace(o.DataDir) == "" {
			dataDir, err := defaultUserDataDir(o.Name)
			if err != nil {
				return err
			}
			o.DataDir = dataDir
		}
	} else {
		o.DataDir = serviceDataDir(o.Name)
	}

	if o.ExecPath == "" {
		execPath, err := os.Executable()
		if err != nil {
			return fmt.Errorf("locate current executable: %w", err)
		}
		o.ExecPath = execPath
	}

	var err error
	if o.ConfigPath, err = filepath.Abs(o.ConfigPath); err != nil {
		return fmt.Errorf("config path: %w", err)
	}
	if o.UnitDir, err = filepath.Abs(o.UnitDir); err != nil {
		return fmt.Errorf("unit directory: %w", err)
	}
	if o.BinPath, err = filepath.Abs(o.BinPath); err != nil {
		return fmt.Errorf("binary path: %w", err)
	}
	if o.DataDir, err = filepath.Abs(o.DataDir); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	if o.ExecPath, err = filepath.Abs(o.ExecPath); err != nil {
		return fmt.Errorf("executable path: %w", err)
	}
	return nil
}

func validateServiceName(name string) error {
	if !serviceNamePattern.MatchString(name) || strings.Contains(name, "..") {
		return fmt.Errorf("invalid service name %q", name)
	}
	return nil
}

func serviceDataDir(name string) string {
	return "/var/lib/" + name
}

func defaultUserDataDir(name string) (string, error) {
	home, err := userHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "share", name), nil
}

func (o installOptions) resolvedDataDir() string {
	if o.DataDir != "" {
		return o.DataDir
	}
	if o.User {
		if dataDir, err := defaultUserDataDir(o.Name); err == nil {
			return dataDir
		}
	}
	return serviceDataDir(o.Name)
}

func unitFilePath(opts installOptions) string {
	return filepath.Join(opts.UnitDir, opts.Name+".service")
}

func renderServiceConfig(name, token string) string {
	return renderServiceConfigFor(name, serviceDataDir(name), token)
}

func renderServiceConfigFor(name, dataDir, token string) string {
	authLine := ""
	if token != "" {
		authLine = "auth-token = " + token + "\n"
	}
	return fmt.Sprintf(`# Yi configuration file.
# Generated by "yi install"; edit and restart %s.service afterwards.

host = 127.0.0.1
service-port = 8081
frontend-port = 8080
frontend = true
data = %s
archive-batch = 256
%s`, name, dataDir, authLine)
}

func renderServiceUnit(opts installOptions) string {
	if opts.User {
		return fmt.Sprintf(`[Unit]
Description=Yi personal event service

[Service]
Type=simple
ExecStart=%s -config %s
WorkingDirectory=%s
Restart=on-failure
RestartSec=2
LimitNOFILE=65536
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes

[Install]
WantedBy=default.target
`, systemdEscape(opts.BinPath), systemdEscape(opts.ConfigPath),
			systemdEscape(opts.resolvedDataDir()))
	}

	dataDir := serviceDataDir(opts.Name)
	return fmt.Sprintf(`[Unit]
Description=Yi personal event service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%s -config %s
WorkingDirectory=%s
StateDirectory=%s
Restart=on-failure
RestartSec=2
LimitNOFILE=65536
UMask=0077
DynamicUser=yes
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=%s

[Install]
WantedBy=multi-user.target
`, systemdEscape(opts.BinPath), systemdEscape(opts.ConfigPath),
		systemdEscape(dataDir), opts.Name, systemdEscape(dataDir))
}

// systemdEscape quotes one ExecStart/WorkingDirectory argument when it
// contains characters systemd would otherwise split on.
func systemdEscape(value string) string {
	if value != "" && !strings.ContainsAny(value, " \t\n\r\"'\\") {
		return value
	}
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return `"` + replacer.Replace(value) + `"`
}

func installService(opts installOptions, out io.Writer) error {
	if err := opts.normalize(); err != nil {
		return err
	}

	configExists, err := fileExists(opts.ConfigPath)
	if err != nil {
		return fmt.Errorf("check config: %w", err)
	}
	if configExists && opts.AuthToken != "" {
		return fmt.Errorf("-auth-token cannot be applied because config already exists: %s", opts.ConfigPath)
	}

	token := opts.AuthToken
	if token == "" && !configExists {
		token, err = newAuthToken()
		if err != nil {
			return fmt.Errorf("generate auth token: %w", err)
		}
	}

	unitPath := unitFilePath(opts)
	configText := renderServiceConfigFor(opts.Name, opts.resolvedDataDir(), token)
	unitText := renderServiceUnit(opts)

	if opts.DryRun {
		fmt.Fprintf(out, "# config: %s\n%s\n# unit: %s\n%s",
			opts.ConfigPath, configText, unitPath, unitText)
		if opts.User && !opts.NoStart {
			fmt.Fprintln(out, "# would run: systemctl --user daemon-reload")
			fmt.Fprintf(out, "# would run: systemctl --user enable %s\n", opts.Name)
			fmt.Fprintf(out, "# would run: systemctl --user restart %s\n", opts.Name)
			fmt.Fprintf(out, "# would run: loginctl enable-linger %s\n", lingerUser())
		}
		return nil
	}

	if err := ensureDir(filepath.Dir(opts.ConfigPath)); err != nil {
		return fmt.Errorf("config directory: %w", err)
	}
	if err := ensureDir(opts.UnitDir); err != nil {
		return fmt.Errorf("unit directory: %w", err)
	}
	if err := ensureDir(filepath.Dir(opts.BinPath)); err != nil {
		return fmt.Errorf("binary directory: %w", err)
	}
	if opts.User {
		if err := ensureDir(opts.resolvedDataDir()); err != nil {
			return fmt.Errorf("data directory: %w", err)
		}
	}

	installed, err := installBinary(opts.ExecPath, opts.BinPath)
	if err != nil {
		return fmt.Errorf("install binary: %w", err)
	}
	if installed {
		fmt.Fprintf(out, "installed binary %s\n", opts.BinPath)
	} else {
		fmt.Fprintf(out, "binary already present at %s\n", opts.BinPath)
	}

	created, err := writeFileIfMissing(opts.ConfigPath, []byte(configText), 0o644)
	if err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if created {
		fmt.Fprintf(out, "wrote config %s\n", opts.ConfigPath)
		if token != "" {
			fmt.Fprintf(out, "auth token: %s\n", token)
		}
	} else {
		fmt.Fprintf(out, "keeping existing config %s\n", opts.ConfigPath)
	}

	if err := writeFileAtomic(unitPath, []byte(unitText), 0o644); err != nil {
		return fmt.Errorf("write unit: %w", err)
	}
	fmt.Fprintf(out, "wrote unit %s\n", unitPath)

	if opts.NoStart {
		fmt.Fprintf(out, "skipped systemctl; run: %s daemon-reload && %s enable --now %s\n",
			systemctlLabel(opts.User), systemctlLabel(opts.User), opts.Name)
		return nil
	}

	if err := runSystemctlFor(opts.User, "daemon-reload"); err != nil {
		return err
	}
	if err := runSystemctlFor(opts.User, "enable", opts.Name); err != nil {
		return err
	}
	if err := runSystemctlFor(opts.User, "restart", opts.Name); err != nil {
		return err
	}
	fmt.Fprintf(out, "enabled and restarted %s.service\n", opts.Name)
	if opts.User {
		enableUserLinger(out)
	}
	return nil
}

type uninstallOptions struct {
	User       bool
	ConfigPath string
	UnitDir    string
	Name       string
	DataDir    string
	NoStart    bool
	DryRun     bool
}

func defaultUninstallOptions() uninstallOptions {
	return uninstallOptions{
		ConfigPath: defaultConfigPath,
		UnitDir:    defaultUnitDir,
		Name:       defaultServiceName,
	}
}

func parseUninstallArgs(args []string) (uninstallOptions, error) {
	opts := defaultUninstallOptions()

	fs := flag.NewFlagSet("yi uninstall", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: yi uninstall [flags]")
		fmt.Fprintln(fs.Output(), "       yi uninstall --user [flags]   卸载当前用户的 systemd 服务")
		fmt.Fprintln(fs.Output())
		fs.PrintDefaults()
	}

	fs.BoolVar(&opts.User, "user", false, "卸载当前用户的 systemd 服务(也可写 --user)")
	fs.StringVar(&opts.ConfigPath, "config", opts.ConfigPath, "保留的配置文件路径")
	fs.StringVar(&opts.UnitDir, "unit-dir", opts.UnitDir, "systemd unit 目录")
	fs.StringVar(&opts.Name, "name", opts.Name, "服务名")
	fs.BoolVar(&opts.NoStart, "no-start", false, "只删文件,不调用 systemctl")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "只打印将执行的动作")

	if err := fs.Parse(args); err != nil {
		return uninstallOptions{}, err
	}
	if fs.NArg() > 0 {
		return uninstallOptions{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if opts.User {
		home, err := userHomeDir()
		if err != nil {
			return uninstallOptions{}, err
		}
		visited := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })
		if !visited["config"] {
			opts.ConfigPath = filepath.Join(home, ".config", "yi", "yi.config")
		}
		if !visited["unit-dir"] {
			opts.UnitDir = filepath.Join(home, ".config", "systemd", "user")
		}
	}
	return opts, nil
}

func runUninstall(args []string) error {
	opts, err := parseUninstallArgs(args)
	if err != nil {
		return err
	}
	return uninstallService(opts, os.Stdout)
}

func (o *uninstallOptions) normalize() error {
	o.Name = strings.TrimSuffix(strings.TrimSpace(o.Name), ".service")
	if err := validateServiceName(o.Name); err != nil {
		return err
	}
	if strings.TrimSpace(o.ConfigPath) == "" {
		return errors.New("config path must not be empty")
	}
	if strings.TrimSpace(o.UnitDir) == "" {
		return errors.New("unit directory must not be empty")
	}

	if o.User {
		if strings.TrimSpace(o.DataDir) == "" {
			dataDir, err := defaultUserDataDir(o.Name)
			if err != nil {
				return err
			}
			o.DataDir = dataDir
		}
	} else {
		o.DataDir = serviceDataDir(o.Name)
	}

	var err error
	if o.ConfigPath, err = filepath.Abs(o.ConfigPath); err != nil {
		return fmt.Errorf("config path: %w", err)
	}
	if o.UnitDir, err = filepath.Abs(o.UnitDir); err != nil {
		return fmt.Errorf("unit directory: %w", err)
	}
	if o.DataDir, err = filepath.Abs(o.DataDir); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	return nil
}

func uninstallService(opts uninstallOptions, out io.Writer) error {
	if err := opts.normalize(); err != nil {
		return err
	}

	unitPath := filepath.Join(opts.UnitDir, opts.Name+".service")
	if opts.DryRun {
		fmt.Fprintf(out, "would remove %s\n", unitPath)
		fmt.Fprintf(out, "would keep config %s and data %s\n",
			opts.ConfigPath, opts.DataDir)
		if opts.User && !opts.NoStart {
			fmt.Fprintf(out, "would run: systemctl --user disable --now %s\n", opts.Name)
			fmt.Fprintln(out, "would run: systemctl --user daemon-reload")
		}
		return nil
	}

	if !opts.NoStart {
		if err := runSystemctlFor(opts.User, "disable", "--now", opts.Name); err != nil {
			fmt.Fprintf(out, "warning: %s disable: %v\n", systemctlLabel(opts.User), err)
		}
		if err := runSystemctlFor(opts.User, "daemon-reload"); err != nil {
			fmt.Fprintf(out, "warning: %s daemon-reload: %v\n", systemctlLabel(opts.User), err)
		}
	} else {
		fmt.Fprintf(out, "skipped systemctl; run %s daemon-reload after removing the unit\n",
			systemctlLabel(opts.User))
	}

	switch err := os.Remove(unitPath); {
	case err == nil:
		fmt.Fprintf(out, "removed %s\n", unitPath)
	case errors.Is(err, os.ErrNotExist):
		fmt.Fprintf(out, "unit not present: %s\n", unitPath)
	default:
		return fmt.Errorf("remove unit: %w", err)
	}

	fmt.Fprintf(out, "config kept at %s; data kept at %s\n",
		opts.ConfigPath, opts.DataDir)
	return nil
}

func systemctlLabel(user bool) string {
	if user {
		return "systemctl --user"
	}
	return "systemctl"
}

func runSystemctl(args ...string) error {
	return runSystemctlFor(false, args...)
}

func runSystemctlFor(user bool, args ...string) error {
	cmdArgs := args
	if user {
		cmdArgs = append([]string{"--user"}, args...)
	}
	cmd := exec.Command("systemctl", cmdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", systemctlLabel(user), strings.Join(args, " "), err)
	}
	return nil
}

func lingerUser() string {
	for _, key := range []string{"USER", "LOGNAME"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return "$USER"
}

// enableUserLinger makes the user service survive logout. It is best-effort:
// installing without linger still works, so failures only produce a warning.
func enableUserLinger(out io.Writer) {
	user := lingerUser()
	cmd := exec.Command("loginctl", "enable-linger", user)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(out, "warning: loginctl enable-linger %s failed: %v\n", user, err)
		fmt.Fprintf(out, "warning: run manually: loginctl enable-linger %s\n", user)
		return
	}
	fmt.Fprintf(out, "enabled linger for %s\n", user)
}

// ---- files ----------------------------------------------------------------

func installBinary(src, dst string) (bool, error) {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false, err
	}
	if !srcInfo.Mode().IsRegular() {
		return false, fmt.Errorf("%s is not a regular file", src)
	}

	if dstInfo, err := os.Stat(dst); err == nil && os.SameFile(srcInfo, dstInfo) {
		return false, nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, err
	}

	source, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer source.Close()

	temp, err := os.CreateTemp(filepath.Dir(dst), ".yi-bin-*")
	if err != nil {
		return false, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)

	if _, err := io.Copy(temp, source); err != nil {
		_ = temp.Close()
		return false, err
	}
	if err := temp.Chmod(srcInfo.Mode().Perm() | 0o111); err != nil {
		_ = temp.Close()
		return false, err
	}
	if err := temp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tempName, dst); err != nil {
		return false, err
	}
	return true, nil
}

func fileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func ensureDir(path string) error {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", path)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
	} else {
		return err
	}
	// MkdirAll applies the process umask; the service's DynamicUser must be
	// able to traverse and read this directory.
	return os.Chmod(path, 0o755)
}

func writeFileIfMissing(path string, data []byte, mode os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			return false, fmt.Errorf("%s is a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, err
	}
	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return false, err
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return false, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return false, err
	}
	return true, nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	temp, err := os.CreateTemp(dir, ".yi-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)

	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return err
	}
	return nil
}
