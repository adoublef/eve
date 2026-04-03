package order_test

import (
	"context"
	"io"
	"iter"
	"net/url"
	"runtime/trace"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/adoublef/eve/internal/order"
	"golang.org/x/sync/errgroup"
)

func TestHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numRegions = 1 << 10
			numPages   = 1 << 10
			numOrders  = 1 << 10

			defaultDelay = 1 * time.Second
		)

		g, ctx := errgroup.WithContext(t.Context())

		regions := make(chan uint64)
		g.Go(func() error {
			defer close(regions)

			t := time.NewTicker(defaultDelay)
			defer t.Stop()

			for range numRegions {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
				}
				select {
				case <-ctx.Done():
					return nil
				case regions <- 1:
				}
			}
			return nil
		})
		pages := make(chan uint64)
		g.Go(func() error {
			defer close(pages)

			t := time.NewTicker(defaultDelay)
			defer t.Stop()

			for range numPages {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
				}
				select {
				case <-ctx.Done():
					return nil
				case pages <- 1:
				}
			}
			return nil
		})
		orders := make(chan Order)
		g.Go(func() error {
			defer close(orders)

			t := time.NewTicker(defaultDelay)
			defer t.Stop()

			o := Order{
				OrderID:      1,
				IsBuyOrder:   false, // or true
				Issued:       "issued",
				LocationID:   1,
				MinVolume:    1,
				Price:        1,
				Range:        "range",
				SystemID:     1,
				TypeID:       1,
				VolumeRemain: 1,
				VolumeTotal:  1,
			}
			for range numOrders {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
				}
				select {
				case <-ctx.Done():
					return nil
				case orders <- o:
				}
			}
			return nil
		})

		g.Go(func() error {
			ctx, task := trace.NewTask(ctx, "orderStream")
			defer task.End()
			// create a reader
			h := &Handler{
				Client: &testClient{regions, pages, orders},
			}
			s := h.OrderStream(ctx, nil, false)
			defer s.Close()

			_, err := io.Copy(writerOnly{io.Discard}, s) // need to discard but this needs a delay
			return err
		})

		ok(t, g.Wait())
	})
}

type writerOnly struct {
	io.Writer
}

func ok(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", t.Name(), err)
	}
}

type testClient struct {
	regions <-chan uint64
	pages   <-chan uint64
	orders  <-chan Order
}

// func run(ctx context.Context)

func (c *testClient) Regions(ctx context.Context, u *url.URL) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		for v := range c.regions {
			if !yield(v, ctx.Err()) {
				return
			}
		}
	}
}

func (c *testClient) Max(ctx context.Context, u *url.URL, region uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case v := <-c.pages:
		return v, nil
	}
}

func (c *testClient) Orders(ctx context.Context, u *url.URL, region, page uint64) iter.Seq2[Order, error] {
	return func(yield func(Order, error) bool) {
		for v := range c.orders {
			if !yield(v, ctx.Err()) {
				return
			}
		}
	}
}
