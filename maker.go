package dbmongo

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrConfigNotFound = errors.New("mongo config not found")

type Maker interface {
	MakeMongoDB(ctx context.Context, name string) (MongoDB, error)
}

type MongoMaker struct {
	sync.RWMutex

	channels Channels
	db       map[string]MongoDB
}

func NewMaker(channels Channels) *MongoMaker {
	return &MongoMaker{
		channels: channels,
		db:       map[string]MongoDB{},
	}
}

func (g *MongoMaker) MakeMongoDB(ctx context.Context, name string) (MongoDB, error) {
	if db := g.getDB(name); db != nil {
		return db, nil
	}

	cfg, err := g.getConfig(name)
	if err != nil {
		return nil, err
	}

	database, err := NewDatabase(ctx, cfg)
	if err != nil {
		return nil, err
	}

	g.Lock()
	defer g.Unlock()

	g.db[name] = database

	return database, nil
}

func (g *MongoMaker) Close(ctx context.Context) (err error) {
	g.RLock()
	defer g.RUnlock()

	for _, db := range g.db {
		if err1 := db.Close(ctx); err1 != nil {
			if err == nil {
				err = err1
			} else {
				err = fmt.Errorf("%w; %w", err, err1)
			}
		}
	}
	return
}

func (g *MongoMaker) getDB(name string) MongoDB {
	g.RLock()
	defer g.RUnlock()

	if db, ok := g.db[name]; ok {
		return db
	}
	return nil
}

func (g *MongoMaker) getConfig(name string) (Config, error) {
	g.RLock()
	defer g.RUnlock()

	if cfg, ok := g.channels[name]; ok {
		return cfg, nil
	}

	return Config{}, fmt.Errorf("%w: `%s`", ErrConfigNotFound, name)
}
