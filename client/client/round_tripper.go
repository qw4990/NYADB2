/*
   RoundTripper 模拟一次收发包的过程
*/
package client

import "nyadb2/transporter"

type RoundTripper interface {
	RoundTrip(pkg transporter.Package) (transporter.Package, error)
	Close() error
}

type roundTripper struct {
	p transporter.Packager
}

func NewRoundTripper(packager transporter.Packager) *roundTripper {
	return &roundTripper{
		p: packager,
	}
}

func (rt *roundTripper) RoundTrip(pkg transporter.Package) (transporter.Package, error) {
	err := rt.p.Send(pkg)
	if err != nil {
		return nil, err
	}
	return rt.p.Receive()
}

func (rt *roundTripper) Close() error {
	return rt.p.Close()
}
