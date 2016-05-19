/*
	pacakger 包裹了protocoler和transporter, 为用户提供了包的收发接口.
*/
package transporter

type Packager interface {
	Send(pkg Package) error
	Receive() (Package, error)
	Close() error
}

type packager struct {
	transporter Transporter
	protocoler  Protocoler
}

func NewPackager(t Transporter, p Protocoler) *packager {
	return &packager{
		transporter: t,
		protocoler:  p,
	}
}

func (p *packager) Send(pkg Package) error {
	data := p.protocoler.Encode(pkg)
	return p.transporter.Send(data)
}

func (p *packager) Receive() (Package, error) {
	data, err := p.transporter.Receive()
	if err != nil {
		return nil, err
	}
	return p.protocoler.Decode(data)
}

func (p *packager) Close() error {
	return p.transporter.Close()
}

type Package interface {
	Data() []byte
	Err() error
}

type SimplePackage struct {
	data []byte
	err  error
}

func NewPackage(data []byte, err error) *SimplePackage {
	return &SimplePackage{
		data: data,
		err:  err,
	}
}

func (sp *SimplePackage) Data() []byte {
	return sp.data
}

func (sp *SimplePackage) Err() error {
	return sp.err
}
