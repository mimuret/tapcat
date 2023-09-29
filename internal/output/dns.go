package output

import (
	"strings"

	"github.com/miekg/dns"
)

type DNS struct {
	WriteOnly bool
	Servers   []string
}

func NewDNS(servers string) (*DNS) {
	return &DNS{Servers: strings.Split(servers, ",")}
}

func (f *DNS) Write(bs []byte) (int, error) {
	for _, s := range f.Servers {
		f.write(s, bs)
	}
	return 0, nil
}

func (f *DNS) write(server string, bs []byte) (int, error) {
	c := &dns.Client{}
	conn, err := c.Dial(server)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.Write(bs)
}

func (f *DNS) Close() error {
	return nil
}
