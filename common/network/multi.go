package network

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

const DefaultFallbackDelay = 300 * time.Millisecond

func DialSerial(ctx context.Context, dialer Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.Conn, error) {
	conn, _, err := DialSerialWithAddr(ctx, dialer, network, destination, destinationAddresses)
	return conn, err
}

func DialSerialWithAddr(ctx context.Context, dialer Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.Conn, netip.Addr, error) {
	if parallelDialer, isParallel := dialer.(ParallelDialer); isParallel {
		return parallelDialer.DialParallelWithAddr(ctx, network, destination, destinationAddresses)
	}
	length := len(destinationAddresses)
	group := newTCPConnGroup(length)
	for _, address := range destinationAddresses {
		go func(address netip.Addr) {
			conn, err := dialer.DialContext(ctx, network, M.SocksaddrFrom(address, destination.Port))
			group.add(conn, address, err)
		}(address)
	}
	return group.wait()
}

func ListenSerial(ctx context.Context, dialer Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.PacketConn, netip.Addr, error) {
	length := len(destinationAddresses)
	group := newUDPConnGroup(length)
	for _, address := range destinationAddresses {
		go func(address netip.Addr) {
			conn, err := dialer.ListenPacket(ctx, M.SocksaddrFrom(address, destination.Port))
			group.add(conn, address, err)
		}(address)
	}
	return group.wait()
}

func DialParallel(ctx context.Context, dialer Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, fallbackDelay time.Duration) (net.Conn, error) {
	conn, _, err := DialParallelWithAddr(ctx, dialer, network, destination, destinationAddresses, preferIPv6, fallbackDelay)
	return conn, err
}

func DialParallelWithAddr(ctx context.Context, dialer Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, fallbackDelay time.Duration) (net.Conn, netip.Addr, error) {
	// kanged form net.Dial

	if fallbackDelay == 0 {
		fallbackDelay = DefaultFallbackDelay
	}

	returned := make(chan struct{})
	defer close(returned)

	addresses4 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is4() || address.Is4In6()
	})
	addresses6 := common.Filter(destinationAddresses, func(address netip.Addr) bool {
		return address.Is6() && !address.Is4In6()
	})
	if len(addresses4) == 0 || len(addresses6) == 0 {
		return DialSerialWithAddr(ctx, dialer, network, destination, destinationAddresses)
	}
	var primaries, fallbacks []netip.Addr
	if preferIPv6 {
		primaries = addresses6
		fallbacks = addresses4
	} else {
		primaries = addresses4
		fallbacks = addresses6
	}
	type dialResult struct {
		net.Conn
		netip.Addr
		error
		primary bool
		done    bool
	}
	results := make(chan dialResult) // unbuffered
	startRacer := func(ctx context.Context, primary bool) {
		ras := primaries
		if !primary {
			ras = fallbacks
		}
		c, addr, err := DialSerialWithAddr(ctx, dialer, network, destination, ras)
		select {
		case results <- dialResult{Conn: c, Addr: addr, error: err, primary: primary, done: true}:
		case <-returned:
			if c != nil {
				c.Close()
			}
		}
	}
	var primary, fallback dialResult
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go startRacer(primaryCtx, true)
	fallbackTimer := time.NewTimer(fallbackDelay)
	defer fallbackTimer.Stop()
	for {
		select {
		case <-fallbackTimer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go startRacer(fallbackCtx, false)

		case res := <-results:
			if res.error == nil {
				return res.Conn, res.Addr, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, netip.Addr{}, primary.error
			}
			if res.primary && fallbackTimer.Stop() {
				fallbackTimer.Reset(0)
			}
		}
	}
}

type tcpConn struct {
	conn net.Conn
	addr netip.Addr
	err  error
}

type tcpConnGroup struct {
	count   int
	channel chan tcpConn
}

func newTCPConnGroup(count int) tcpConnGroup {
	return tcpConnGroup{count, make(chan tcpConn, count)}
}

func (g *tcpConnGroup) add(conn net.Conn, addr netip.Addr, err error) {
	g.channel <- tcpConn{conn, addr, err}
}

func (g *tcpConnGroup) wait() (net.Conn, netip.Addr, error) {
	var i int
	var errors []error
	defer func() {
		go g.close(i)
	}()
	for i = 0; i < g.count; i++ {
		tc := <-g.channel
		if tc.err == nil {
			return tc.conn, tc.addr, nil
		}
		errors = append(errors, tc.err)
	}
	return nil, netip.Addr{}, E.Errors(errors...)
}

func (g *tcpConnGroup) close(index int) {
	for i := index + 1; i < g.count; i++ {
		<-g.channel
	}
	close(g.channel)
}

type udpConn struct {
	conn net.PacketConn
	addr netip.Addr
	err  error
}

type udpConnGroup struct {
	count   int
	channel chan udpConn
}

func newUDPConnGroup(count int) udpConnGroup {
	return udpConnGroup{count, make(chan udpConn)}
}

func (g *udpConnGroup) add(conn net.PacketConn, addr netip.Addr, err error) {
	g.channel <- udpConn{conn, addr, err}
}

func (g *udpConnGroup) wait() (net.PacketConn, netip.Addr, error) {
	var i int
	var errors []error
	defer func() {
		go g.close(i)
	}()
	for i = 0; i < g.count; i++ {
		uc := <-g.channel
		if uc.err == nil {
			return uc.conn, uc.addr, nil
		}
		errors = append(errors, uc.err)
	}
	return nil, netip.Addr{}, E.Errors(errors...)
}

func (g *udpConnGroup) close(index int) {
	for i := index + 1; i < g.count; i++ {
		<-g.channel
	}
	close(g.channel)
}
