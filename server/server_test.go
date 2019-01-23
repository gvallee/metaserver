/*
 * Copyright(c)         Geoffroy Vallee
 *                      All rights reserved
 */

package server

import ("testing"
	"net/url"
	"log")

import err "github.com/gvallee/syserror"

func createVirtualServer () (*Dataserver, err.SysError) {
	myurl, parse_err := url.Parse ("http://127.0.0.1")
	if (parse_err != nil) {
		log.Fatal ("FATAL: Cannot parse URL")
	}
	server := new (Dataserver)
	myerr := server.Init (myurl)
	if (myerr != err.NoErr) {
		log.Fatal ("FATAL: Cannot add virtual server")
	}
	server.SetBlocksize (512)

	return server, err.NoErr
}

func TestGeneric (t *testing.T) {
        basedir := "/tmp/GoFS/metadata_server"

	/* Initialize the client (it will initialize the metadata server) */
	myFS := ClientInit (basedir)
	if (myFS == nil) {
		log.Fatal ("FATAL: Cannot initialize the client")
	}

	/* Create data servers */
	server1, syserr1 := createVirtualServer ()
	if (syserr1 != err.NoErr) {
		log.Fatal ("FATAL: Cannot create a virtual server")
	}
	server_url, parseerr := server1.GetURI()
	if (parseerr != err.NoErr) {
		log.Fatal ("FATAL: Cannot get server's URI")
	}
	server_url_str := server_url.String()
	myFS.AddDataserver ("default", server1, server_url_str)

        /* Prepare the buffer to write */
	buff1 := make ([]byte, 1024)

	/* Actually doing a write operation */
        ws, writeErr := myFS.Write ("default", buff1)
	if (writeErr != err.NoErr) {
		log.Fatal ("FATAL, write failed: ", writeErr.Error())
	}
	if (ws != 1024) {
		log.Fatal ("FATAL: written size is invalid");
	}

	moreData := make ([]byte, 124)

	for i := 0; i < 3; i++ {
		ws, writeErr = myFS.Write ("default", moreData)
		if (writeErr != err.NoErr) {
			log.Fatal ("FATAL, write failed: ", writeErr.Error())
		}
		if (ws != 124) {
			log.Fatal ("FATAL: written size is invalid");
		}
	}
}
