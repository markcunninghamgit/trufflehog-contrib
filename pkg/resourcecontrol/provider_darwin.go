//go:build darwin

package resourcecontrol

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type darwinProvider struct {
	runtime RuntimeProvider
}

func newPlatformProvider(config ProviderConfig) Provider {
	return compositeProvider{providers: []Provider{config.Runtime, darwinProvider{runtime: config.Runtime}}}
}

func (p darwinProvider) Snapshot(ctx context.Context) Snapshot {
	commandCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pressure, pressureErr := commandOutput(commandCtx, "/usr/bin/memory_pressure", "-Q")
	vmstat, vmstatErr := commandOutput(commandCtx, "/usr/bin/vm_stat")
	if pressureErr != nil && vmstatErr != nil {
		return Snapshot{Level: PressureUnknown, Reason: "macOS memory probes unavailable"}
	}

	freePercent, freeOK := parseFreePercent(pressure)
	if !freeOK {
		freePercent, freeOK = parseVMStatFreePercent(vmstat)
	}
	if !freeOK {
		return Snapshot{Level: PressureUnknown, Reason: "macOS memory pressure could not be parsed"}
	}

	level := PressureHealthy
	if freePercent <= 5 {
		level = PressureCritical
	} else if freePercent <= 15 {
		level = PressureElevated
	}
	return Snapshot{Level: level, Reason: fmt.Sprintf("macos_free_percent=%.1f", freePercent)}
}

func commandOutput(ctx context.Context, name string, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, name, args...).Output()
	return string(output), err
}

var freePercentPattern = regexp.MustCompile(`(?i)free percentage:\s*([0-9]+(?:\.[0-9]+)?)%`)

func parseFreePercent(output string) (float64, bool) {
	matches := freePercentPattern.FindStringSubmatch(output)
	if len(matches) != 2 {
		return 0, false
	}
	value, err := strconv.ParseFloat(matches[1], 64)
	return value, err == nil && value >= 0 && value <= 100
}

func parseVMStatFreePercent(output string) (float64, bool) {
	pageSize := float64(4096)
	pages := map[string]float64{}
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "page size of") {
			fields := strings.Fields(line)
			for i, field := range fields {
				if field == "of" && i+1 < len(fields) {
					pageSize, _ = strconv.ParseFloat(fields[i+1], 64)
				}
			}
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		value := strings.Trim(strings.TrimSpace(parts[1]), ".")
		count, err := strconv.ParseFloat(value, 64)
		if err == nil {
			pages[strings.TrimSpace(parts[0])] = count * pageSize
		}
	}
	free := pages["Pages free"] + pages["Pages inactive"] + pages["Pages speculative"]
	active := free + pages["Pages active"] + pages["Pages wired down"] + pages["Pages occupied by compressor"]
	if active == 0 {
		return 0, false
	}
	return free / active * 100, true
}
