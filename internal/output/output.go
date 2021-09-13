package output

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	strftime "github.com/jehiah/go-strftime"
	"github.com/sirupsen/logrus"
)

var (
	bufferSize = 10000 * 1500
)

type FileOutput struct {
	outputPathTemplate string
	outputPath         string
	roateExec          string
	roateExecArgs []string
	f                  *os.File
	w                  *bufio.Writer
	log *logrus.Logger
}

func NewFileOutput(log *logrus.Logger, outputPathTemplate, roateExec string) (*FileOutput, error) {
	commands := strings.Split(roateExec," ")
	f := &FileOutput{
		outputPathTemplate: outputPathTemplate,
		roateExec:          commands[0],
		log: log,
	}
	if len(commands) > 1 {
		for i:=1;i<len(commands); i++ {
			if commands[i] != "" {
				f.roateExecArgs = append(f.roateExecArgs, commands[i])
			}
		}
	}
	if err := f.refresh(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *FileOutput) refresh() error {
	npath := strftime.Format(f.outputPathTemplate,time.Now())
	if npath == f.outputPath {
		return nil
	}
	nfp, err := os.Create(npath)
	if err != nil {
		return fmt.Errorf("failed to create file `%s`: %w", npath, err)
	}
	nfw := bufio.NewWriterSize(nfp, bufferSize)
	oldf := f.f
	oldw := f.w
	oldp := f.outputPath

	f.outputPath = npath
	f.f = nfp
	f.w = nfw
	if oldw != nil {
		oldw.Flush()
	}
	if oldf != nil {
		oldf.Close()
	}
	if f.roateExec != "" && oldp != "" {
		go func() {
			if err := exec.Command(f.roateExec, append(f.roateExecArgs,oldp)...).Run() ; err != nil {
				f.log.Warn("failed to exec after rotate command: %v", err)
			}
		}()
	}
	return nil
}

func (f *FileOutput) Write(bs []byte) (int, error) {
	if err := f.refresh(); err != nil {
		return 0, err
	}
	if _, err := f.w.Write(bs); err != nil {
		return 0, err
	}
	if _, err := f.w.Write([]byte("\n")); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *FileOutput) Close() error {
	f.w.Flush()
	f.f.Close()
	return nil
}
