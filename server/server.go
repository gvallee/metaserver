/*
 * Copyright(c)         2019    Geoffroy Vallee
 *                              All rights reserved
 */

package server

import ("os"
	"fmt"
	"net"
	"math/rand"
	"strings"
	)

import err "github.com/gvallee/syserror"
import comm "github.com/gvallee/fscomm"

/**
 * Structure representing the data server for a given namespace.
 * Really note that it is for a given namespace so if the server is used
 * for multiple namespace we will have as many Dataserver structures as
 * namespace
 */
type Dataserver struct {
	surl            string // URL of the server, also used to connect to the server
	block_size      uint64 // block size specific to the server
	curWriteBlock   uint64 // current block used for write operations
	curWriteOffset uint64 // current block offset used for write operations
	curReadBlock    uint64 // current block used for read operations
	curReadOffset   uint64 // current block offset used for read operations
	conn            net.Conn // Connection to reach the server
}

/**
 * Structure that tracks the data saved in our file system based on offsets
 */
type DataInfo struct {
	start           uint64 // Absolute start offset
	end             uint64 // Absolute end offset
	server          *Dataserver // Where the data is stored
	blockid         uint64
	blockoffset     uint64
}

/**
 * Structure representing a meta-data server.
 */
type Server struct {
	basedir         string	// Basedir of the server (where metadata will be stored
}

/**
 * Structure representing a namespace from a client/metadata server point of view.
 */
type Namespace struct {
	// Should store the state of the namespace
	name		string // namespace's name
	file		*os.File // file handle that is used to store metadata in files (based on the meta-data server's basedir

	// Used to track where the data is
	datainfo	[]*DataInfo // Used to track where the data is

	// Cache data to track what data servers are used
	nds             int     // List of data servers
	dataservers     map[string]*Dataserver  // Map used to store information about data servers we are connecting to. quick lookup based on URI
	listDataservers []*Dataserver // Global list of data server, used for lookups that are not based on the server's URL

	// We cache some information so we can easily resume/continue writing operations
	lastWriteDataserver	*Dataserver // Pointer to the server where the last write operation was performed (used to continue write operations in sub-sequent operations)
	lastWriteBlockid	uint64 // Block id used for the last write operation
	writeOffset		uint64 // Block offset used for the last write operation

	// We cache some information so we can easily resume/continue reading operations
	lastReadDataserver	*Dataserver // Pointer to the server where the last read operation was performed (used to continue read operations in sub-sequent operations)
	lastReadBlockid		uint64 // Block id used for the last read operation
	readOffset		uint64 // Block offset used for the last read operation
	globalOffset            uint64 // Global read offset, i.e., based on the overall data, not a specific block. Used for sequential reads over data spaning multiple servers.
}

/**
 * Structure used to store the state of our file system
 */
type MyGoFS struct {
	namespaces		map[string]*Namespace // List of existing namespaces in the current FS; used to lookup a namespace
	listNamespaces		[]*Namespace // Overall list of namespaces so we can iterate over them

	LocalMetadataServer     *Server // Pointer to the meta-data server's structure.
}

/**
 * Function that can be used to add a server while doing manually conenct
 * or testing with virtual data servers.
 * @param[in]	namespace	Namespace's name for which the server needs to be added
 * @param[in]	ds		Pointer to the sttructure representing the server to be added
 * @param[in]	url		URL of the server to be added
 * @param[in]	blocksize	Dataserver's block size
 * @return	System error handle
 */
func (aFS *MyGoFS) AddDataserver (namespace string, ds *Dataserver, url string, blocksize uint64) (err.SysError) {
	if (aFS == nil) { return err.ErrFatal }

	ns, nserr := aFS.LookupNamespace (namespace)
	if (nserr != err.NoErr) { return err.NoErr }

	ns.dataservers[url] = ds
	ns.listDataservers = append (ns.listDataservers, ds)
	ns.nds++
	ds.block_size = blocksize
	fmt.Println ("New dataserver registered with block size of", blocksize)

	return err.NoErr
}

/**
 * Return the URL for a specific data server
 * @return Pointer to a URL structure and system error handle
 */
func (ds *Dataserver) GetURL () (string, err.SysError) {
	if (ds == nil) { return "", err.ErrFatal }

	return ds.surl, err.NoErr
}

/**
 * Function that returns the blockid where we have some free space to store data in
 * the context of a given namespace. Basically, when we know which data server to
 * use to store the data, this function let us in which block we must save the data
 * @param[in] ns	Current namespace for the operation
 * @return	ID of a free block that can be used for the current operation
 * @return	System error handle
 */
func (ds *Dataserver) GetFreeBlock (ns *Namespace) (uint64, err.SysError) {
	if (ns == nil) {
		return uint64(0), err.ErrFatal
	}

	if (len (ns.datainfo) == 0) {
		// No data at all yet, we can use the first block
		return 0, err.NoErr
	}

	// We need to find the last block writen to that server for the current namespace
	var targetDS *Dataserver = nil
	for i := len (ns.datainfo) - 1; i >= 0; i-- {
		if (ns.datainfo[i].server == ds) {
			// We found it!
			targetDS = ns.datainfo[i].server
		}
	}

	if (targetDS != nil) {
		if (targetDS.curWriteOffset < targetDS.block_size) {
			// We still have space in the last block we used
			return targetDS.curWriteBlock, err.NoErr
		}

		// We need to use a brand new block
		return targetDS.curWriteBlock + 1, err.NoErr
	} else {
		// That server is not used yet so we start at the first block
		return 0, err.NoErr
	}
}

/**
 * Initialize the internal representation of a data server. Note that we are still in the context of
 * a meta-data server, so this is only meta-information about existing data server; not the data server
 * itself.
 * @param[in]	url	Data server's URL
 * @return	System error handle
 */
func (ds *Dataserver) Init (url string) (err.SysError) {
	if (ds == nil) { return err.ErrNotAvailable }

	ds.surl = url
	ds.block_size = 0
	ds.curWriteBlock = 0
	ds.curWriteOffset = 0
	ds.curReadBlock = 0
	ds.curReadOffset = 0
	ds.conn = nil

	return err.NoErr
}

/**
 * Initialize a namespace structure.
 * @param[in]	name	Namespace's name
 * @return	System error handle
 */
func (ns *Namespace) Init (name string) (err.SysError) {
	ns.name = name
	ns.file = nil

	// Initialize the variables used to cache information about write operations
	ns.lastWriteDataserver = nil
	ns.lastWriteBlockid = 0
	ns.writeOffset = 0

	// Initialize the variables used to cache information about read operations
	ns.lastReadDataserver = nil
	ns.lastReadBlockid = 0
	ns.readOffset = 0

	// Initialize the info we used for book keeping the data server
	// that are used in the context of the namespace
	ns.nds = 0
	ns.dataservers = make (map[string]*Dataserver)

	return err.NoErr
}

/**
 * Get the block size of a specific data server. That information is set when
 * the meta-data server connects to a data server
 * @return	block size
 * @return	System error handle
 */
func (aServer *Dataserver) GetBlocksize () (uint64, err.SysError) {
	if (aServer == nil) {
		return 0, err.ErrFatal
	}

	return aServer.block_size, err.NoErr
}

/**
 * Explicitly set the block size of a specific data server.
 * @param[in]	size	Data server's block size
 * return	System error handle
 */
func (aServer *Dataserver) SetBlocksize (size uint64) err.SysError {
	if (aServer == nil) {
		return err.ErrNotAvailable
	}

	aServer.block_size = size
	return err.NoErr
}

/**
 * Lookup a namespace for a given file system
 * @param[in]	namespace	Namespace's name
 * @return	Pointer to the corresponding namespace's structure
 * @return	System error handle
 */
func (aGoFS *MyGoFS) LookupNamespace (namespace string) (*Namespace, err.SysError) {
	return aGoFS.namespaces[namespace], err.NoErr
}

/**
 * Lookup where the last write operation landed, i.e., which block on which server. Used to continue writing after
 * the last write operation.
 * @input[in]	namespace	Namespace's name used for the write operation
 * @return dataserver	Pointer to the structure representing the data server where the last write operation ended
 * @return blockid	Last block id used by the last write operation
 * @return blocksize	Last block size used by the last write operation
 * @return offset	Last block offset used by the last write operation
 * @return System error handle
 */
func (aGoFS *MyGoFS) LookupLastWriteBlockUsed (namespace string) (*Dataserver, uint64, uint64, uint64, err.SysError) {
	if (aGoFS == nil) {
		return nil, 0, 0, 0, err.ErrFatal
	}

	ns, myerr := aGoFS.LookupNamespace (namespace)
	if (myerr != err.NoErr || ns == nil) {
		return nil, 0, 0, 0, err.ErrNotAvailable
	}

	if (ns.lastWriteDataserver != nil) {
		blocksize, myerr := ns.lastWriteDataserver.GetBlocksize()
		if (myerr != err.NoErr || blocksize == 0) {
			return nil, 0, 0, 0, err.ErrFatal
		}

		return ns.lastWriteDataserver, ns.lastWriteBlockid, blocksize, ns.writeOffset, err.NoErr
	} else {
		// We could find a last write yet
		return nil, 0, 0, 0, err.NoErr
	}
}

/**
 * Lookup where the last read operation stopped, i.e., which block on which server. Used to continue read operations.
 * @input[in]   namespace       Namespace's name used for the read operation
 * @dataserver   Pointer to the structure representing the data server where the last read operation ended
 * @return blockid      Last block id used by the last read operation
 * @return blocksize    Last block size used by the last read operation
 * @return blockoffset  Last block offset used by the last read operation
 * @return globalOffset Global offset where the last read stopped
 * @return System error handle
 */
func (aGoFS *MyGoFS) LookupLastReadBlockUsed (namespace string) (*Dataserver, uint64, uint64, uint64, uint64, err.SysError) {
	if (aGoFS == nil) { return nil, 0, 0, 0, 0, err.ErrFatal }

	ns, myerr := aGoFS.LookupNamespace (namespace)
	if (myerr != err.NoErr || ns == nil) { return nil, 0, 0, 0, 0, err.ErrNotAvailable }

	if (ns.lastReadDataserver != nil) {
		blocksize, myerr := ns.lastReadDataserver.GetBlocksize()
		if (myerr != err.NoErr || blocksize == 0) { return nil, 0, 0, 0, 0, err.ErrFatal }

		return ns.lastReadDataserver, ns.lastReadBlockid, blocksize, ns.readOffset, ns.globalOffset, err.NoErr
	} else {
		// No read operation yet, we look from the data map where is the begining of the data in the namespace
		if (ns.datainfo == nil || ns.datainfo[0].server == nil) {
			// We try to do a read but we have no data
			return nil, 0, 0, 0, 0, err.ErrNotAvailable
		}
		firstserver := ns.datainfo[0].server
		blocksize, myerr := firstserver.GetBlocksize()
		if (myerr != err.NoErr || blocksize == 0) { return nil, 0, 0, 0, 0, err.ErrFatal }

		return firstserver, 0, blocksize, 0, 0, err.NoErr
	}
}

/**
 * Update the information related to the last block access of the last write operation. Used to resume writting later on.
 * @param[in] ds	Pointer to the data server's structure used last.
 * @param[in] namespace	Namespace's name for the write operation
 * @param[in] blockid	Block id used last
 * @param[in] startOffset Offset in block used last
 * @param[in] writeSize	Amount of data writen last
 * @return	System error handle
 */
func (aGoFS *MyGoFS) UpdateLastWriteInfo (ds *Dataserver, namespace string, blockid uint64, startOffset uint64, writeSize uint64) (err.SysError) {
	if (aGoFS == nil) {
		return err.ErrFatal
	}

	fmt.Println ("Update last write info with block", blockid)
	ns, myerr := aGoFS.LookupNamespace (namespace)
	if (myerr != err.NoErr) {
		return err.ErrFatal
	}

	ns.lastWriteDataserver = ds
	ns.lastWriteBlockid = blockid
	ns.writeOffset = writeSize

	if (blockid == ds.curWriteBlock && ds.curWriteOffset < ds.block_size) {
		// The data was added to a block already inuse
		ds.curWriteOffset += writeSize
		if (ds.curWriteOffset > ds.block_size) { return err.ErrDataOverflow }
	} else {
		// We use a new block
		ds.curWriteOffset = writeSize
		ds.curWriteBlock = blockid
	}

	// Update the block map if necessary
	var entry_found int = 0
	start := blockid * ds.block_size + startOffset
	for i := 0; i < len (ns.datainfo); i++ {
		if (ns.datainfo[i].start <= start && (ns.datainfo[i].start + ns.datainfo[i].server.block_size) > start) {
			fmt.Println ("Consecutive write detected to server", ds.surl, " with blocks ", ns.datainfo[i].blockid, "and", blockid)
			if (ns.datainfo[i].blockid != blockid) {
				// We are now using a new block
				break
			}

			if (ns.datainfo[i].end != startOffset) {
				// This is not a consecutive write, meaning since last write on this server, data was saved on another server so we need to precisely separate what is already there and the new data
				break
			}
			entry_found = 1
			// We added data to the block that we already used
			ns.datainfo[i].end += writeSize
		}
		if (ns.datainfo[i].end > start) {
			break
		}
	}

	// We do not have a structure to track data yet so we need to add a new one (the goal being to known exactly where all the data is)
	if (entry_found == 0) {
		fmt.Println ("Using a new block for", start, "to", start + writeSize, "on", ds.surl)
		di := new (DataInfo)
		di.start = start
		di.end = start + writeSize - 1
		di.server = ds
		di.blockid = blockid
		di.blockoffset = startOffset
		ns.datainfo = append (ns.datainfo, di)
	}

	fmt.Println ("Recording write for namespace", namespace, "on block", blockid, "starting at", startOffset, "with", writeSize, "bytes on", ds.surl)

	// Flush the metadata to make sure it is saved on dick
	aGoFS.FlushMetadataToDisk (ns)

	return err.NoErr
}

/** Update the metadata related to the last read operation.
 * @param[in] ds        Pointer to the data server's structure used last
 * @param[in] namespace Namespace's name for the read operation
 * @param[in] blockid   Block id used last
 * @param[in] startOffset Offset in block used last
 * @param[in] readSize Amount of data read last
 * @return      System error handle
 */
func (aGoFS *MyGoFS) UpdateLastReadInfo (ds *Dataserver, namespace string, blockid uint64, readSize uint64) err.SysError {
	if (aGoFS == nil) {
		return err.ErrFatal
	}

	ns, myerr := aGoFS.LookupNamespace (namespace)
	if (myerr != err.NoErr) {
		return err.ErrFatal
	}

	ns.lastReadDataserver = ds
	ns.lastReadBlockid = blockid
	ns.readOffset = readSize
	ns.globalOffset = ns.globalOffset + readSize

	return err.NoErr
}

/**
 * Flush a specific namespace.
 * @param[in]	basedir	File system basedir
 * @return	System error handle
 */
func (ns *Namespace) flush (basedir string) (err.SysError) {
	if (ns == nil) {
		return err.ErrFatal
	}

	// Figure out the file specific to the namespace
	path := basedir + "/" + ns.name

	var myerr error

	// Make sure the file for saving metadata is correctly created and open
	if (ns.file == nil) {
		ns.file, myerr = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
		if (myerr != nil) {
			return err.ErrFatal
		}
	}

	// Write the metadata to the file
	_, seek_err := ns.file.Seek (0, 0)
	if (seek_err != nil) { return err.ErrFatal }
	_, write_err := ns.file.WriteString (ns.name + "\n")
	if (write_err != nil) { return err.ErrFatal }

	// Capture datainfo
	_, write_err = ns.file.WriteString (fmt.Sprintf ("# data info\n"))
	if (write_err != nil) { return err.ErrFatal }
	_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", len (ns.datainfo)))
	if (write_err != nil) { return err.ErrFatal }
	for i := 0; i < len (ns.datainfo); i++ {
		_, write_err = ns.file.WriteString (ns.datainfo[i].server.surl + " ")
		if (write_err != nil) { return err.ErrFatal }
		_, write_err = ns.file.WriteString (fmt.Sprintf ("%d ", ns.datainfo[i].start))
		if (write_err != nil) { return err.ErrFatal }
		_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.datainfo[i].end))
		if (write_err != nil) { return err.ErrFatal }
	}

	// Capture dataservers' info
	_, write_err = ns.file.WriteString (fmt.Sprintf ("# data servers' info\n"))
	if (write_err != nil) { return err.ErrFatal }
	_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.nds))
	if (write_err != nil) { return err.ErrFatal }

	for i := 0; i < ns.nds; i++ {
		_, write_err = ns.file.WriteString (ns.listDataservers[i].surl + " ")
		if (write_err != nil) { return err.ErrFatal }
		_, write_err = ns.file.WriteString (fmt.Sprintf ("%d ", ns.listDataservers[i].block_size))
		if (write_err != nil) { return err.ErrFatal }
		_, write_err = ns.file.WriteString (fmt.Sprintf ("%d ", ns.listDataservers[i].curWriteBlock))
		if (write_err != nil) { return err.ErrFatal }
		_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.listDataservers[i].curWriteOffset))
		if (write_err != nil) { return err.ErrFatal }
	}

	// Capture last write info
	_, write_err = ns.file.WriteString (fmt.Sprintf ("# last write info\n"))
	if (write_err != nil) { return err.ErrFatal }
	if (ns.lastWriteDataserver != nil) {
		_, write_err = ns.file.WriteString (ns.lastWriteDataserver.surl + "\n")
		if (write_err != nil) { return err.ErrFatal }
	} else {
		_, write_err = ns.file.WriteString ("None\n")
		if (write_err != nil) { return err.ErrFatal }
	}
	_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.lastWriteBlockid))
	if (write_err != nil) { return err.ErrFatal }
	_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.writeOffset))
	if (write_err != nil) { return err.ErrFatal }

	// Capture last read info
	_, write_err = ns.file.WriteString (fmt.Sprintf ("# last read info\n"))
	if (write_err != nil) { return err.ErrFatal }
	if (ns.lastReadDataserver != nil) {
		_, write_err = ns.file.WriteString (ns.lastReadDataserver.surl + "\n")
		if (write_err != nil) { return err.ErrFatal }
	} else {
		_, write_err = ns.file.WriteString ("None\n")
		if (write_err != nil) { return err.ErrFatal }
	}
	_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.lastReadBlockid))
	if (write_err != nil) { return err.ErrFatal }
	_, write_err = ns.file.WriteString (fmt.Sprintf ("%d\n", ns.readOffset))
	if (write_err != nil) { return err.ErrFatal }

	// Mark the end of the valid data
	_, write_err = ns.file.WriteString ("# End of latest data")
	if (write_err != nil) { return err.ErrFatal }

	// Flush the file
	ns.file.Sync()

	return err.NoErr
}

/**
 * Flush one of our file system's metadata to disk.
 * @param[in]	ns	Namespace to flush
 * @return	System error handle
 */
func (aGoFS *MyGoFS) FlushMetadataToDisk (ns *Namespace) (err.SysError) {
	basedir := aGoFS.LocalMetadataServer.basedir

	go ns.flush(basedir) // flush can be expensive so we execute it asychronisely

	return err.NoErr
}

/**
 * Add a new namespace to an existing file system handle
 * @param[in] ns	Namespace to flush
 * @return	System error handle
 */
func (aGoFS *MyGoFS) AddNamespace (ns *Namespace) (err.SysError) {
	if (ns == nil) {
		return err.ErrFatal
	}

	aGoFS.namespaces[ns.name] = ns
	aGoFS.listNamespaces = append (aGoFS.listNamespaces, ns)
	return err.NoErr
}

/**
 * Initialize a meta-data server.
 * @param[in]	basedir	Server's basedir
 * @return	Meta-data server's metadata
 */
func ServerInit (basedir string) *Server {
	// Deal with the server's basedir (we have to make sure it exists)
	_, myerror := os.Stat (basedir)
	if (myerror != nil) {
		return nil
	}

	// Create and return the data structure for the new server
	new_server := new (Server)
	new_server.basedir = basedir
	return new_server
}

/**
 * Initialize a new file system
 * @param[in]	basedir	File system's basedir
 * @return	System error handle
 */
func (aGoFS *MyGoFS) Init (basedir string) (err.SysError) {
	// For now, the metadata server is embeded into the client
	aGoFS.LocalMetadataServer = ServerInit (basedir)
	if (aGoFS.LocalMetadataServer == nil) {
		return err.ErrFatal
	}
	aGoFS.namespaces = make(map[string]*Namespace)

	defaultNS := new (Namespace)
	defaultNS.Init ("default")
	mysyserr := aGoFS.AddNamespace (defaultNS)
	if (mysyserr != err.NoErr) {
		return mysyserr
	}

	return err.NoErr
}

/**
 * Initialize the client side of our file system
 * @param[in]	metadata_basedir	Basedir that the metadata server will use (the metadata server is instantiated in the client)
 * @return	Pointer to the structure representing the new file system associated to the client
 */
func ClientInit (metadata_basedir string, servers string) *MyGoFS {

	newGoFS := new (MyGoFS)
	if (newGoFS == nil) { return nil }

	mysyserr := newGoFS.Init (metadata_basedir)
	if (mysyserr != err.NoErr) { return nil }

	/* Connecting to data servers */
	list_servers := strings.Split (servers, " ")
	fmt.Println ("Connecting to", len (list_servers), "servers: ", servers)

	for i := 0; i < len (list_servers); i++ {
		fmt.Println ("\t-> Connecting to", list_servers[i])
		connerr := newGoFS.ConnectToDataserver (list_servers[i])
		if (connerr != err.NoErr) { return nil }
	}

	return newGoFS
}

/**
 * Finalize a client.
 */
func (myfs *MyGoFS) ClientFini () err.SysError {
	if (myfs == nil) { return err.NoErr } // FS is already finalized

	// We go through all the namespaces and disconnect from the data servers
	for i := 0; i < len (myfs.listNamespaces); i++ {
		for j := 0; j < len (myfs.listNamespaces[i].listDataservers); j++ {
			if (myfs.listNamespaces[i].listDataservers[j] != nil && myfs.listNamespaces[i].listDataservers[j].conn != nil) {
				// We assume that by the time we are done, we always disconnect from the data servers, sending them a termination message
				comm.SendMsg (myfs.listNamespaces[i].listDataservers[j].conn, comm.TERMMSG, nil)
				myfs.listNamespaces[i].listDataservers[j].conn.Close ()
				myfs.listNamespaces[i].listDataservers[j].conn = nil
			}
		}
	}

	myfs = nil
	return err.NoErr
}

/**
 * Connect to an existing data server
 * @param[in]	server_url	URL of the server to conenct to
 * @return	System error handle
 */
func (myfs *MyGoFS) ConnectToDataserver (server_url string) (err.SysError) {
	if (myfs == nil) { return err.ErrNotAvailable }

	fmt.Println ("Connecting to dataserver", server_url)
	newDataServer := new (Dataserver)
	newDataServer.Init (server_url)

	conn, bs, connerr := comm.Connect2Server (server_url)
	newDataServer.conn = conn
	if (connerr != err.NoErr) { return err.ErrFatal }

	// We always assume that the default namespace is ready to go, it is our reference namespace. So we add the dataserver to the default namespace
	syserr := myfs.AddDataserver ("default", newDataServer, server_url, bs)
	if (syserr != err.NoErr) { return err.ErrNotAvailable }

	return err.NoErr
}

/**
 * Send a write request to a data server, so that the data will be stored in a block.
 * Everything is ready so that the server can save the data directly upon reception.
 * We assume the call does not block and is fault tolerant.
 * @parma[in]	dataserver	Target data server
 * @param[in]	namespace	Target namespace
 * @param[in]	blockid		Target block id on the data server
 * @param[in]	block_offset	Target block offset on the data server
 * @param[in]	buff		Data to write into the target block on the data server
 * @param[in]	buff_size	Size of the data in the buffer to save in the target block
 * @param[in]	buff_offset	Offset in the buffer to save in the target block
 * @return	System error handle
 */
func (myFS *MyGoFS) SendWriteReq (dataserver *Dataserver, namespace string, blockid uint64, block_offset uint64, buff []byte, buff_size uint64, buff_offset uint64) (err.SysError) {
	// Get the connection for the server
	conn := dataserver.conn
	if (conn == nil) { return err.ErrNotAvailable }

	// We make sure to restrict the range on the data so we send just enough data for that
	// specific block.
	// Fortunately, Go slices are very helpful with notations such as buff[start:end]
	senderr := comm.SendData (conn, namespace, blockid, block_offset, buff[buff_offset:buff_offset + buff_size])
	if (senderr != err.NoErr) { return senderr }

	return err.NoErr
}

/**
 * Send a read request to a data server. The operation is blocking.
 * We assume the call does not block and is fault tolerant.
 * @parma[in]   dataserver      Target data server
 * @param[in]   namespace       Target namespace
 * @param[in]   blockid         Target block id on the data server
 * @param[in]   block_offset    Target block offset on the data server
 * @param[in]	size		Size of the data to read
 * @return      System error handle
 */
func  (myFS *MyGoFS) SendReadReq (dataserver *Dataserver, namespace string, blockid uint64, offset uint64, size uint64) (err.SysError) {
	// Get the connection for the server
	conn := dataserver.conn
	if (conn == nil) { return err.ErrNotAvailable }

	senderr := comm.SendReadReq (conn, namespace, blockid, offset, size)
	if (senderr != err.NoErr) { return senderr }

	return err.NoErr
}

/**
 * Get the new dataserver when the last block is full, or to start the very first write operation
 * @param[in]	ns	Namespace of the operation
 * @return	Pointer to the data server to use next
 * @return	System error handle.
 */
func (myFS *MyGoFS) GetNextDataserver (ns *Namespace) (*Dataserver, err.SysError) {
	if (myFS == nil) {
		return nil, err.ErrFatal
	}

	serverID := rand.Intn (ns.nds)
	return ns.listDataservers[serverID], err.NoErr
}

/**
 * Write to an initialized file system.
 * Example:
 * s, err := myFS.Write ("namespace1", buff)
 * @param[in]	namespace	Namespace of the write operation
 * @param[in]	buff		Buffer to write
 * @return	Size in bytes written to the file system. Note that the operation is asynchronous, the data may still be in transit.
 * @return	System error handle
 */
func (myFS *MyGoFS) Write (namespace string, buff []byte) (uint64, err.SysError) {
	// First we look up the namespace
	ns, mynserr := myFS.LookupNamespace (namespace)
	if (mynserr != err.NoErr) { return 0, err.ErrFatal }

	// Lookup where we wrote data (server + blockid)
	dataserver, blockid, blocksize, offset, mylookuperr := myFS.LookupLastWriteBlockUsed (namespace)
	if (mylookuperr != err.NoErr) { return 0, mylookuperr }

	// A few variables that we will need, i.e., global info about the operation
	var writeSize uint64
	var totalSize uint64 = uint64 (len (buff))
	var curSize uint64 = 0
	fmt.Println (totalSize, "bytes to write...")

	// Fill up the last block we used and split the rest of the data to different blocks on different dataserver
	if (dataserver != nil && dataserver.curWriteOffset < dataserver.block_size) {
		serverURL, mySrvLookupErr := dataserver.GetURL ()
		if (mySrvLookupErr != err.NoErr) { return 0, err.ErrFatal }

		spaceLeft := blocksize - offset
		fmt.Println ("Space left:", spaceLeft)
		if (totalSize > spaceLeft) {
			writeSize = spaceLeft
		} else {
			writeSize = totalSize
		}

		fmt.Printf ("Writing %d/%d to block %d on server %s\n", writeSize, len (buff), blockid, serverURL)
		sendWriteReqErr := myFS.SendWriteReq (dataserver, namespace, blockid, offset, buff, writeSize, curSize)
		if (sendWriteReqErr != err.NoErr) {
			return 0, err.ErrFatal
		}
		curSize += writeSize

		// Update the block map & last block used info
		update_err := myFS.UpdateLastWriteInfo (dataserver, namespace, blockid, dataserver.curWriteOffset, writeSize)
		if (update_err != err.NoErr) { return 0, update_err }

		fmt.Println ("Wrote", curSize, "bytes so far")

		fmt.Println ("****************")
	}

	// Get the next server where to write data
	for (totalSize > curSize) {
		nextDataserver, myQueryErr := myFS.GetNextDataserver (ns)
		if (myQueryErr != err.NoErr) { return 0, err.ErrNotAvailable }

		blocksize, myQueryErr = nextDataserver.GetBlocksize()
		if (myQueryErr != err.NoErr) { return 0, err.ErrFatal }

		serverURI, mySrvLookupErr := nextDataserver.GetURL ()
		if (mySrvLookupErr != err.NoErr) { return 0, err.ErrFatal }

		freeBlockid, myblockidlookuperr := nextDataserver.GetFreeBlock (ns)
		if (myblockidlookuperr != err.NoErr) { return 0, myblockidlookuperr }

		writeSize = 0
		if (blocksize > (totalSize - curSize)) {
			writeSize = totalSize - curSize
		} else {
			writeSize = blocksize
		}

		fmt.Printf ("Writing %d/%d to block %d on server %s\n", writeSize, len (buff), freeBlockid, serverURI)
		sendWriteReqErr := myFS.SendWriteReq (nextDataserver, namespace, freeBlockid, 0, buff, writeSize, curSize)
	        if (sendWriteReqErr != err.NoErr) { return 0, err.ErrFatal }

		curSize += writeSize

		// Update the block map & last block used info
		update_err := myFS.UpdateLastWriteInfo (nextDataserver, namespace, freeBlockid, 0, writeSize)
		if (update_err != err.NoErr) { return 0, update_err }

		fmt.Println ("Wrote", curSize, "bytes so far")

		fmt.Println ("****************")
	}

	return curSize, err.NoErr
}

/**
 * Read from an initialized file system
 * @param[in]	namespace	Namespace's name from wich we want to read
 * @param[in]	size		Size to read. Read operations are assumed to be serialized, in order, starting at the begining of the namespace
 * @return	Buffer with the received data
 * @return	System error handle
 * Example
 * s, buff, err := myFS.Read ("namespace2", size)
 */
func (myFS *MyGoFS) Read (namespace string, size uint64) ([]byte, err.SysError) {
	// First we look up the namespace
        ns, mynserr := myFS.LookupNamespace (namespace)
        if (mynserr != err.NoErr) { return nil, err.ErrFatal }

	data := make ([]byte, size)

	var totalReadSize uint64 = 0
	for (totalReadSize < size) {
		// Lookup where the data is (server + blockid)
		dataserver, blockid, _, _, global_offset, mylookuperr := myFS.LookupLastReadBlockUsed (namespace)
		if (mylookuperr != err.NoErr) { return nil, mylookuperr }

		// First thing to figure out: can we read more data from the last block we accessed?
		var targetDS *Dataserver = nil
		var di *DataInfo = nil
		for i := 0; i < len (ns.datainfo); i++ {
			if (ns.datainfo[i].start <= global_offset && ns.datainfo[i].end > global_offset) {
				targetDS = ns.datainfo[i].server
				di = ns.datainfo[i]
				fmt.Println ("Found data starting at", global_offset, ", it is in block", blockid, "(", di.start, "-", di.end, ")")
				break
			}

			if (global_offset == ns.datainfo[i].end) {
				// We reached the end of the last block we read, so we look for the next chunk
				targetDS = ns.datainfo[i+1].server
				di = ns.datainfo[i+1]
			}
		}

		// if not the first read, global_offset points right now to the last piece
		// we already read so we will strat reading right after that
		nextReadGlobalOffset := global_offset

		if (targetDS != nil) {
			// We found a server, we start with some sanity checks
			if (dataserver != targetDS) {
				fmt.Println ("Inconsistent server bookkeeping")
				return nil, err.ErrFatal
			}

			// How much more data can we get from there?
			readSize := di.end - nextReadGlobalOffset + 1
			if (readSize + totalReadSize > size) { readSize = size - totalReadSize }

			// Get that data
			fmt.Println ("Sending read req to server", dataserver.surl, ", blockid: ", di.blockid, "offset: ", di.blockoffset, ", for ", readSize, "bytes")
			senderr := myFS.SendReadReq (dataserver, namespace, di.blockid, di.blockoffset, readSize)
			if (senderr != err.NoErr) { return nil, senderr }
			msgtype, datasize, blockdata, recverr := comm.RecvMsg (dataserver.conn)
			if (msgtype != comm.RDREPLY || datasize != readSize || blockdata == nil || recverr != err.NoErr) { return nil, err.ErrFatal }
			fmt.Println ("Received data for range", nextReadGlobalOffset, "to", nextReadGlobalOffset + readSize, "(", readSize, "bytes) from", dataserver.surl, ", blockid:", di.blockid, ", offset:", di.blockoffset)

			// Copy the received data to the target buffer
			fmt.Println ("Copying data to target buffer")
			copy (data[totalReadSize:totalReadSize + readSize], blockdata)

			fmt.Println ("Updating read info after reading", readSize, "byte")
			myFS.UpdateLastReadInfo (dataserver, namespace, di.blockid, readSize)

			totalReadSize += readSize
			fmt.Println ("We read", totalReadSize, "bytes so far")
		} else {
			// Nothing to read
			return nil, err.NoErr
		}
	}
	fmt.Println ("****************")

	return data, err.NoErr
}

