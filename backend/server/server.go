package server

import (
	"net"
	"nyadb2/backend/server/executor"
	"nyadb2/backend/tbm"
	"nyadb2/backend/utils"
	"nyadb2/transporter"
)

type Server interface {
	Start()
}

type server struct {
	network string
	address string

	tbm tbm.TableManager
}

func NewServer(network, address string, tbm tbm.TableManager) *server {
	return &server{
		network: network,
		address: address,
		tbm:     tbm,
	}
}

func (s *server) Start() {
	listener, err := net.Listen(s.network, s.address)
	if err != nil {
		panic(err)
	}
	utils.Info("Server Start at ", s.network, s.address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			utils.Warn(err)
			continue
		}
		go s.serve(conn)
	}
}

func (s *server) serve(conn net.Conn) {
	var netErr error

	utils.Info("Establish Connection: ", conn.RemoteAddr())
	defer func() {
		utils.Info("Disconnect Connection: ", conn.RemoteAddr(), ", NetErr: ", netErr)
		conn.Close()
	}()

	tr := transporter.NewHexTransporter(conn)
	pr := transporter.NewProtocoler()
	packager := transporter.NewPackager(tr, pr)
	defer packager.Close()
	var pkg transporter.Package

	var exe executor.Executor
	exe = executor.NewExecutor(s.tbm)
	defer exe.Close()

	for {
		pkg, netErr = packager.Receive()
		if netErr != nil {
			break
		}

		sql := pkg.Data()
		result, sqlErr := exe.Execute(sql)
		pkg = transporter.NewPackage(result, sqlErr)

		netErr = packager.Send(pkg)
		if netErr != nil {
			break
		}
	}
}
