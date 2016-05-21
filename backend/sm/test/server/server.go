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
	"flag"
	"net"
	"nyadb2/backend/dm"
	"nyadb2/backend/sm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"nyadb2/transporter"
	"strings"
)

const (
	_NET     = "tcp"
	_ADDRESS = ":8080"

	_DEFAULT_MEM = (1 << 20) * 64 // 64MB
)

var (
	ErrInvalidCMD = errors.New("Invalid command.")
	SM            sm.SerializabilityManager
)

func main() {
	open := flag.String("open", "", "-open DBPath")
	create := flag.String("create", "", "-create DBPath")
	flag.Parse()

	if *open != "" {
		tm := tm.Open(*open)
		dm := dm.Open(*open, _DEFAULT_MEM, tm)
		SM = sm.NewSerializabilityManager(tm, dm)
	} else if *create != "" {
		tm := tm.Create(*create)
		dm := dm.Create(*create, _DEFAULT_MEM, tm)
		SM = sm.NewSerializabilityManager(tm, dm)
	} else {
		return
	}

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
			xid, _ := utils.StrToUint64(cmds[1])
			err := SM.Commit(tm.XID(xid))
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else {
				pkg = transporter.NewPackage([]byte("Commit"), nil)
			}
		case "abort":
			xid, _ := utils.StrToUint64(cmds[1])
			SM.Abort(tm.XID(xid))
			pkg = transporter.NewPackage([]byte("Abort"), nil)
		case "insert":
			xid, _ := utils.StrToUint64(cmds[1])
			uuid, err := SM.Insert(tm.XID(xid), []byte(cmds[2]))
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else {
				pkg = transporter.NewPackage([]byte(utils.Uint64ToStr(uint64(uuid))), nil)
			}
		case "read":
			xid, _ := utils.StrToUint64(cmds[1])
			uuid, _ := utils.StrToUint64(cmds[2])
			result, ok, err := SM.Read(tm.XID(xid), utils.UUID(uuid))
			if err != nil {
				pkg = transporter.NewPackage(nil, err)
			} else if ok == false {
				pkg = transporter.NewPackage([]byte("nil"), nil)
			} else {
				pkg = transporter.NewPackage(result, nil)
			}
		case "delete":
			xid, _ := utils.StrToUint64(cmds[1])
			uuid, _ := utils.StrToUint64(cmds[2])
			ok, err := SM.Delete(tm.XID(xid), utils.UUID(uuid))
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
