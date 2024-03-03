package ffmpeg_go

import (
	"bytes"
	"context"
	"os/exec"
	"time"
)

// Probe Run ffprobe on the specified file and return a JSON representation of the output.
func Probe(ffprobePath string, fileName string, kwargs ...KwArgs) (string, error) {
	return ProbeWithTimeout(ffprobePath, fileName, 0, MergeKwArgs(kwargs))
}

func ProbeWithTimeout(ffprobePath string, fileName string, timeOut time.Duration, kwargs KwArgs) (string, error) {
	args := KwArgs{
		"show_format":  "",
		"show_streams": "",
		"of":           "json",
	}

	return ProbeWithTimeoutExec(ffprobePath, fileName, timeOut, MergeKwArgs([]KwArgs{args, kwargs}))
}

func ProbeWithTimeoutExec(ffprobePath string, fileName string, timeOut time.Duration, kwargs KwArgs) (string, error) {
	args := ConvertKwargsToCmdLineArgs(kwargs)
	args = append(args, fileName)
	ctx := context.Background()
	if timeOut > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(context.Background(), timeOut)
		defer cancel()
	}
	var cmd *exec.Cmd

	if ffprobePath == "" {
		cmd = exec.CommandContext(ctx, "ffprobe", args...)

	} else {
		cmd = exec.CommandContext(ctx, ffprobePath, args...)

	}
	buf := bytes.NewBuffer(nil)
	cmd.Stdout = buf
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	return string(buf.Bytes()), nil
}
