
package main

import (
	"../../xsiogo"
	"log"
)

func main() {

	ctx,err := xsio.NewContext()
	if err != nil {
		log.Fatal(err)
	}
	/*
	err = ctx.SetMaximumSockets(4)
	if err != nil {
		log.Fatal(err)
	}

	err = ctx.SetNumberIOThreads(2)
	if err != nil {
		log.Fatal(err)
	}
	 */
	log.Printf("number of I/O Threads is %d, max sockets is %d\n",ctx.GetNumberIOThreads(),
		ctx.GetMaximumSockets())
	
	/* server : */
	sck,err := ctx.NewSocket(xsio.REP)
	if err != nil {
		log.Fatal(err)
	}
	
	end,err := sck.Bind("tcp://127.0.0.1:8080")
	if err != nil {
		log.Fatal(err)
	}
	
	/* client : */
	cln,err := ctx.NewSocket(xsio.REQ) 
	if err != nil {
		log.Fatal(err)
	}

	_,err = cln.Connect("tcp://127.0.0.1:8080")
	if err != nil {
		log.Fatal(err)
	}

	frame0,err := xsio.NewMessage([]byte("hello there"))
	if err != nil {
		log.Fatal(err)
	}
	
	log.Printf("size of message is %dbytes (%d)\n",frame0.Size(),len([]byte("hello there")))

	/* trying send a message */
	//err = cln.Send([]byte("hello there"),0)

	for i := 0; i < 10; i++ {
	
		err = cln.SendMsg(frame0,xsio.NONE)
		if err != nil {
			log.Fatal(err)
		}
		
		/* try recv'ing a message */
		msg,err,_ := sck.Recv(xsio.NONE)
		if err != nil {
			log.Fatal(err)
		}
		
		log.Printf("%s\n",string(msg))

		/* echo back */
		sck.SendMsg(frame0,xsio.NONE)
		if err != nil {
			log.Fatal(err)
		}

		_,err,_ = cln.Recv(xsio.NONE)
		if err != nil {
			log.Fatal(err)
		}

	}
	
	
	




	/* shutdown and close */
	frame0.Close()
	cln.Close()
	
	err = sck.Shutdown(end)
	if err != nil {
		log.Fatal(err)
	}
	sck.Close()

	err = ctx.Term()
	if err != nil {
		log.Fatal(err)
	}
}

