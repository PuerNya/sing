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

type TCPGroup struct {
	conn net.Conn
	err  error
}

type UDPGroup struct {
	conn net.PacketConn
	addr netip.Addr
	err  error
}

func DialSerial(ctx context.Context, dialer Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.Conn, error) {
	if parallelDialer, isParallel := dialer.(ParallelDialer); isParallel {
		return parallelDialer.DialParallel(ctx, network, destination, destinationAddresses)
	}
	var connErrors []error
	length := len(destinationAddresses)
	TCPGroupChan := make(chan TCPGroup, length)
	for _, address := range destinationAddresses {
		go func(address netip.Addr) {
			conn, err := dialer.DialContext(ctx, network, M.SocksaddrFrom(address, destination.Port))
			TCPGroupChan <- TCPGroup{
				conn: conn,
				err:  err,
			}
		}(address)
	}
	for i := 0; i < length; i++ {
		group := <-TCPGroupChan
		if group.err != nil {
			connErrors = append(connErrors, group.err)
			continue
		}
		go func(index int) {
			for i := index; i < length; i++ {
				group := <-TCPGroupChan
				if group.err != nil {
					continue
				}
				group.conn.Close()
			}
		}(i + 1)
		return group.conn, nil
	}
	return nil, E.Errors(connErrors...)
}

func ListenSerial(ctx context.Context, dialer Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr) (net.PacketConn, netip.Addr, error) {
	var connErrors []error
	length := len(destinationAddresses)
	UDPGroupChan := make(chan UDPGroup, length)
	for _, address := range destinationAddresses {
		go func(address netip.Addr) {
			conn, err := dialer.ListenPacket(ctx, M.SocksaddrFrom(address, destination.Port))
			UDPGroupChan <- UDPGroup{
				conn: conn,
				addr: address,
				err:  err,
			}
		}(address)
	}
	for i := 0; i < length; i++ {
		group := <-UDPGroupChan
		if group.err != nil {
			connErrors = append(connErrors, group.err)
			continue
		}
		go func(index int) {
			for i := index; i < length; i++ {
				group := <-UDPGroupChan
				if group.err != nil {
					continue
				}
				group.conn.Close()
			}
		}(i + 1)
		return group.conn, group.addr, nil
	}
	return nil, netip.Addr{}, E.Errors(connErrors...)
}

func DialParallel(ctx context.Context, dialer Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, fallbackDelay time.Duration) (net.Conn, error) {
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
		return DialSerial(ctx, dialer, network, destination, destinationAddresses)
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
		c, err := DialSerial(ctx, dialer, network, destination, ras)
		select {
		case results <- dialResult{Conn: c, error: err, primary: primary, done: true}:
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
				return res.Conn, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, primary.error
			}
			if res.primary && fallbackTimer.Stop() {
				fallbackTimer.Reset(0)
			}
		}
	}
}
