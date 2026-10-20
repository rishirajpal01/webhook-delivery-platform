package postgres

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/jackc/pgx/v5/stdlib"
)

// ListenDue holds one connection that LISTENs on DueChannel and calls wake
// for every notification, until ctx is cancelled or the connection fails.
// It returns once LISTEN has failed or the wait ended; callers retry.
func (s *Store) ListenDue(ctx context.Context, wake func()) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(dc any) error {
		c, ok := dc.(*stdlib.Conn)
		if !ok {
			return errors.New("listen: not a pgx connection")
		}
		pc := c.Conn()
		if _, err := pc.Exec(ctx, "LISTEN "+DueChannel); err != nil {
			return err
		}
		for {
			if _, err := pc.WaitForNotification(ctx); err != nil {
				// The session still has LISTEN on; never hand it back to
				// the pool.
				return errors.Join(err, driver.ErrBadConn)
			}
			wake()
		}
	})
}
