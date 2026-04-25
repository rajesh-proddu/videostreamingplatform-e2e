package client

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PgVectorClient struct {
	pool *pgxpool.Pool
}

func NewPgVectorClient(ctx context.Context, dsn string) (*PgVectorClient, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &PgVectorClient{pool: pool}, nil
}

func (c *PgVectorClient) Close() {
	c.pool.Close()
}

func (c *PgVectorClient) EnsureWatchHistoryTable(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS watch_history (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT 'WATCH_COMPLETED',
            watched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `)
	return err
}

func (c *PgVectorClient) InsertWatch(ctx context.Context, userID, videoID, eventType string, watchedAt time.Time) error {
	_, err := c.pool.Exec(ctx,
		`INSERT INTO watch_history (user_id, video_id, event_type, watched_at) VALUES ($1, $2, $3, $4)`,
		userID, videoID, eventType, watchedAt,
	)
	return err
}

func (c *PgVectorClient) SeedHistory(ctx context.Context, userID string, videoIDs []string) error {
	for _, vid := range videoIDs {
		if err := c.InsertWatch(ctx, userID, vid, "WATCH_COMPLETED", time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (c *PgVectorClient) SeedTrending(ctx context.Context, videoID string, count int) error {
	for i := 0; i < count; i++ {
		uid := fmt.Sprintf("trending-user-%d-%d", time.Now().UnixNano(), i)
		if err := c.InsertWatch(ctx, uid, videoID, "WATCH_COMPLETED", time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (c *PgVectorClient) DeleteUserHistory(ctx context.Context, userID string) error {
	_, err := c.pool.Exec(ctx, `DELETE FROM watch_history WHERE user_id = $1`, userID)
	return err
}

func (c *PgVectorClient) DeleteByVideoID(ctx context.Context, videoID string) error {
	_, err := c.pool.Exec(ctx, `DELETE FROM watch_history WHERE video_id = $1`, videoID)
	return err
}

func (c *PgVectorClient) CountForUser(ctx context.Context, userID string) (int, error) {
	var n int
	err := c.pool.QueryRow(ctx, `SELECT COUNT(*) FROM watch_history WHERE user_id = $1`, userID).Scan(&n)
	return n, err
}
