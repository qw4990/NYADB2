/*
	begin [X]
	commit xid
	abort xid

	insert xid xxx
	read xid uid
	delete xid uid
*/
package main

import (
	"errors"
	"net"
	"nyadb/transporter"
	"nyadb2/backend/sm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"strings"
)

const (
	_NET     = "tcp"
	_ADDRESS = ":8080"
)

var (
	ErrInvalidCMD = errors.New("Invalid command.")
	SM            sm.SerializabilityManager
)

func main() {
	SM = sm.CreateDB("/tmp/xxxxxx", 1<<20)

	listener, err := net.Listen(_NET, _ADDRESS)
	if err != nil {
		panic(err)
	}

	utils.Info("Start.")

	for {
		conn, err := listener.Accept()
		if err != nil {
			utils.Info(err)
			continue
		}
		go serve(conn)
	}
}

func serve(conn net.Conn) {
	utils.Info(conn.RemoteAddr())

	tr := transporter.NewHexTransporter(conn)
	pr := transporter.NewProtocoler()
	pk := transporter.NewPackager(tr, pr)
	defer pk.Close()

	for {
		pg, err := pk.Receive()
		if err != nil {
			utils.Info(err)
			break
		}

		cmd := pg.Data()
		cmd_str := string(cmd)
		utils.Info(cmd_str)
		cmds := strings.Split(cmd_str, " ")

		var pkg transporter.Package
		switch cmds[0] {
		case "begin":
			var xid tm.XID
			if len(cmds) == 1 {
				xid = SM.Begin(0)
			} else {
				xid = SM.Begin(1)
			}
			pkg = transporter.NewPackage([]byte(utils.Uint64ToStr(uint64(xid))), nil)
		case "commit":
			xid := tm.XID(utils.StrToUint64(cmds[1]))
			err := SM.Commit(xid)
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else {
				pkg = transporter.NewPackage([]byte("Commit"), nil)
			}
		case "abort":
			xid := tm.XID(utils.StrToUint64(cmds[1]))
			SM.Abort(xid)
			pkg = transporter.NewPackage([]byte("Abort"), nil)
		case "insert":
			xid := tm.XID(utils.StrToUint64(cmds[1]))
			uuid, err := SM.Insert(xid, []byte(cmds[2]))
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else {
				pkg = transporter.NewPackage([]byte(utils.Uint64ToStr(uint64(uuid))), nil)
			}
		case "read":
			xid := tm.XID(utils.StrToUint64(cmds[1]))
			uuid := utils.UUID(utils.StrToUint64(cmds[2]))
			result, ok, err := SM.Read(xid, uuid)
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else if ok == false {
				pkg = transporter.NewPackage([]byte("nil"), nil)
			} else {
				pkg = transporter.NewPackage(result, nil)
			}
		case "delete":
			xid := tm.XID(utils.StrToUint64(cmds[1]))
			uuid := utils.UUID(utils.StrToUint64(cmds[2]))
			ok, err := SM.Delete(xid, uuid)
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else if ok == false {
				pkg = transporter.NewPackage([]byte("nil"), nil)
			} else {
				pkg = transporter.NewPackage([]byte("delete"), nil)
			}
		default:
			pkg = transporter.NewPackage(nil, ErrInvalidCMD)
		}

		err = pk.Send(pkg)
		if err != nil {
			utils.Info(err)
			break
		}
	}
}
