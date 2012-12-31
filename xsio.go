
package xsio

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lxs
#include <xs/xs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"unsafe"
)


type Context interface {
	SetMaximumSockets(int) error
	SetNumberIOThreads(int) error
	GetMaximumSockets() int
	GetNumberIOThreads() int
	NewSocket(ty SocketType) (Socket,error)
	Term() error
}

type Socket interface {
	
	Bind(string) (Endpoint,error)
	Connect(string) (Endpoint,error)
	Close() error
	Shutdown(Endpoint) error

	Send([]byte, SocketOption) error
	SendMsg(Message,SocketOption) error
	SendMultipart([][]byte, SocketOption) error
	Recv(SocketOption) ([]byte,error,uint64)
	RecvMultipart(SocketOption) ([][]byte,error)
}

type Endpoint interface {
	
	Address() string
	ID() C.int
	Reset()
}

type Message interface {

	Use() *C.xs_msg_t
	Size() int
	Close() error
	ZeroCopy() (*C.xs_msg_t,error)
}



type SocketType int

const (
	REQ = SocketType(C.XS_REQ)
	REP = SocketType(C.XS_REP)
	XREQ = SocketType(C.XS_XREQ)
	XREP = SocketType(C.XS_XREP)
	PUB = SocketType(C.XS_PUB)
	SUB = SocketType(C.XS_SUB)
	XPUB = SocketType(C.XS_XPUB)
	XSUB = SocketType(C.XS_XSUB)
	PUSH = SocketType(C.XS_PUSH)
	PULL = SocketType(C.XS_PULL)
	SURVEYOR = SocketType(C.XS_SURVEYOR)
	RESPONDENT = SocketType(C.XS_RESPONDENT)
	XSURVEYOR = SocketType(C.XS_XSURVEYOR)
	XRESPONDENT = SocketType(C.XS_XRESPONDENT)
	PAIR = SocketType(C.XS_PAIR)
)

type SocketOption int

const (
	DONTWAIT = SocketOption(C.XS_DONTWAIT)
	SNDMORE = SocketOption(C.XS_SNDMORE)
	NONE = SocketOption(0) /* added for convience */
)


func errno() error { 
	eno := C.xs_errno()
	if eno >= C.XS_HAUSNUMERO {
		return xsError{eno}
	}
	return xsError{eno} /* FIXME */
}


type xsError struct {
	eno C.int
}

func (e xsError) Error() string {
	return C.GoString(C.xs_strerror(C.int(e.eno)))
}


/* xs_version : */
func Version() (int,int,int) {
	var major, minor, patch C.int
	C.xs_version(&major,&minor,&patch)
	return int(major),int(minor),int(patch)
}


/* Context */
type xsContext struct {

	ctx unsafe.Pointer
	maxSockets int
	iothreads int
}

/* int xs_term (void *context) : */
func (c *xsContext) Term() (error) {

	r := C.xs_term(c.ctx)
	if r == 0 {
		return nil
	}

	/* else we have an error: */
	return errno()
}

/* int xs_setctxopt (void *ctx, int option_name, const void *option_value,size_t option_len) : */
func (c *xsContext) SetMaximumSockets(m int) (error) {

	r := C.xs_setctxopt(c.ctx,C.XS_MAX_SOCKETS,unsafe.Pointer(&m),C.size_t(unsafe.Sizeof(&m)))
	if r == 0 {
		c.maxSockets = m
		return nil
	}

	return errno()
}

func (c *xsContext) GetMaximumSockets() (int) {

	return c.maxSockets
}

/* int xs_setctxopt (void *ctx, int option_name, const void *option_value,size_t option_len) : */
func (c *xsContext) SetNumberIOThreads(m int) (error) {

	r := C.xs_setctxopt(c.ctx,C.XS_IO_THREADS,unsafe.Pointer(&m),C.size_t(unsafe.Sizeof(&m)))
	if r == 0 {
		c.iothreads = m
		return nil
	}

	return errno()
}

func (c *xsContext) GetNumberIOThreads() (int) {
	
	return c.iothreads
}


/* void * xs_socket (void *ctx,int type) : */
func (c *xsContext) NewSocket(ty SocketType) (Socket,error){

	if sck := C.xs_socket(c.ctx,C.int(ty)); sck != nil {
			
		return &xsSocket{sck},nil
	}
	return nil,errno()
}
		

/* xs_init : */
func NewContext() (Context,error) {

	if ctx := C.xs_init(); ctx != nil {
			
		return &xsContext{ctx,512,1}, nil
	}
	return nil,errno()
}

/* socket : */
type xsSocket struct {

	sck unsafe.Pointer
}

/* int xs_close (void *sck) : */
func (sck *xsSocket) Close() error {
	
	r := C.xs_close(sck.sck)
	if r == 0 {
		return nil
	}
	return errno()
}

/* int xs_bind (void *sck, const char *endpoint) */
func (sck *xsSocket) Bind(endpoint string) (Endpoint,error) {

	c_endpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(c_endpoint))
	id := C.xs_bind(sck.sck,c_endpoint)
	if id == -1 {
		
		return nil,errno()
	}

	return &xsEndpoint{endpoint,id},nil
}

/* int xs_connect (void *sck,const char *endpoint) */
func (sck *xsSocket) Connect(endpoint string) (Endpoint,error) {

	c_endpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(c_endpoint))
	id := C.xs_connect(sck.sck,c_endpoint)
	if id == -1 {

		return nil, errno()
	}

	return &xsEndpoint{endpoint,id},nil
}
	
/* int xs_shutdown (void *sck,int how) : */
func (sck *xsSocket) Shutdown(e Endpoint) error {

	if r := C.xs_shutdown(sck.sck,e.ID()); r != 0 {
		return errno() 
	}

	e.Reset()

	return nil
}	

/* int xs_send (void *sck,void *buf,size_t len,int flags) */
func (sck *xsSocket) Send(data []byte, flags SocketOption) error {

	r := C.xs_send (sck.sck,unsafe.Pointer(&data[0]),C.size_t(len(data)),C.int(flags))
	if r == -1  {
		return errno()
	}

	return nil
}
	
func (sck *xsSocket) SendMsg(msg Message,flags SocketOption) error {

	/* do a zero-copy */
	cmsg,err := msg.ZeroCopy()
	if err != nil {
		return err
	}

	r := C.xs_sendmsg (sck.sck,cmsg,C.int(flags))
	if r == -1 {
		C.xs_msg_close(cmsg)
		return errno()
	}

	return nil
}

func (sck *xsSocket) SendMultipart(parts [][]byte,flags SocketOption) error {

	endpart := len(parts) - 1
	for i := 0; i < endpart; i++ {
		if err := sck.Send(parts[i],SNDMORE|flags); err != nil {
			return err
		}
	}
	return sck.Send(parts[endpart],flags)
}


/* int xs_recv (void *sck,void *buf,size_t len,int flags) */
func (sck *xsSocket) Recv(flags SocketOption) ([]byte,error,uint64) {

	var msg C.xs_msg_t
	r := C.xs_msg_init (&msg)
	if (r != 0) {
		return nil,errno(),0
	}

	defer C.xs_msg_close (&msg)

	r = C.xs_recvmsg (sck.sck,&msg,C.int(flags))
	if (r == -1) {
		return nil,errno(),0
	}
	size := C.xs_msg_size(&msg)
	var more uint64
	more_size := C.size_t(unsafe.Sizeof(more))
	C.xs_getmsgopt(&msg,C.XS_MORE,unsafe.Pointer(&more),&more_size)

	if size > 0 {
		data := make([]byte,int(size))
		C.memcpy(unsafe.Pointer(&data[0]),C.xs_msg_data(&msg),size)
		return data,nil,more
	} 
	
	return nil,nil,more /* nothing to recv */
}

func (sck *xsSocket) RecvMultipart(flags SocketOption) ([][]byte,error) {

	parts := make([][]byte,0)
	for {
		data, err,more := sck.Recv(flags)
		if err != nil {
			return parts,err
		}
		parts = append(parts,data)
		/* check for more frames */
		
		if more == 0 {
			break
		}
	}

	return parts,nil
}
			



/* Endpoint : */
type xsEndpoint struct {

	address string
	id C.int
}

func (e *xsEndpoint) Address() string {
	return e.address
}

func (e *xsEndpoint) ID() C.int {
	return e.id
}

func (e *xsEndpoint) Reset() {
	e.id = C.int(0)
}


/* Message : */
type xsMessage struct {

	msg C.xs_msg_t
}

func (msg *xsMessage) Use() *C.xs_msg_t {

	return &msg.msg
}

func (msg *xsMessage) Size() int {

	return int(C.xs_msg_size(&msg.msg))
}

func (msg *xsMessage) Close() error {

	r := C.xs_msg_close (&msg.msg)
	if r == 0 {
		return nil
	}

	return errno()
}	

func (msg *xsMessage) ZeroCopy() (*C.xs_msg_t,error) {

	var nmc C.xs_msg_t
	C.xs_msg_init(&nmc)
	r := C.xs_msg_copy(&nmc,&msg.msg)
	if r != 0 {
		return nil, errno()
	}
	
	return &nmc,nil
}

func NewMessage (data []byte) (Message,error) {

	var msg xsMessage
	//r := C.xs_msg_init_data (&msg.msg,unsafe.Pointer(&data[0]),C.size_t(len(data)),nil,nil)
	size := C.size_t(len(data))
	r := C.xs_msg_init_size(&msg.msg,size)
	if r == -1 {
		return nil,errno()
	}
	C.memcpy(unsafe.Pointer(C.xs_msg_data(&msg.msg)), unsafe.Pointer(&data[0]),size)

	return &msg,nil
}
