package output

import "os"

type Stdout struct {
}

func NewStdout() *Stdout {
	return &Stdout{}
}

func (f *Stdout) Write(bs []byte) (int, error) {
	if _, err := os.Stdout.Write(bs); err != nil {
		return 0, err
	}
	if _, err := os.Stdout.Write([]byte("\n")); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *Stdout) Close() error {
	return nil
}
