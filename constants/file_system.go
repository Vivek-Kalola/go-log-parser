/*
 * Copyright (c) 2023 RevDau Industries (P) Limited
 */

package constants

import (
	"os"
	"strings"
)

var (
	CWD, _ = os.Getwd()
)

const (
	PathSeparator   = string(os.PathSeparator)
	ConfigDir       = "config"
	ConfigFile      = "config.json"
	PluginEngineDir = "plugin-engine"
	ContextsDir     = "contexts"
	LogsDir         = "logs"
	MaxUint64       = ^uint64(0)
)

func File(args ...string) string {
	return strings.Join(args, PathSeparator)
}
