package format

import (
	"fmt"

	"github.com/miekg/dns"

	"github.com/mimuret/dtap"
)

type DNSFormater struct {
	RecursionDesired bool
}

func NewDNSFormater(rd bool) *DNSFormater {
	return &DNSFormater{RecursionDesired: rd}
}

func (o *DNSFormater) Format(dt *dtap.DnstapFlatT) ([]byte, error) {
	m := &dns.Msg{}
	m.SetQuestion(dt.Qname, dns.StringToType[dt.Qtype])
	m.RecursionDesired = o.RecursionDesired
	bs, err := m.Pack()
	if err != nil {
		return nil, fmt.Errorf("failed to make DNS Request: %w", err)
	}
	return bs, nil
}
