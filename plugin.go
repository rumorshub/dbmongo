package dbmongo

import (
	"context"

	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
)

const PluginName = "db.mongo"

type Plugin struct {
	maker *MongoMaker
}

func (p *Plugin) Init(cfg Configurer) error {
	const op = errors.Op("db.mongo_plugin_init")

	if !cfg.Has(PluginName) {
		return errors.E(op, errors.Disabled)
	}

	var channels Channels
	if err := cfg.UnmarshalKey(PluginName, &channels); err != nil {
		return errors.E(op, err)
	}

	p.maker = NewMaker(channels)

	return nil
}

func (p *Plugin) Serve() chan error {
	return make(chan error, 1)
}

func (p *Plugin) Stop(ctx context.Context) error {
	return p.maker.Close(ctx)
}

func (p *Plugin) Provides() []*dep.Out {
	return []*dep.Out{
		dep.Bind((*Maker)(nil), p.MongoMaker),
	}
}

func (p *Plugin) MongoMaker() *MongoMaker {
	return p.maker
}

func (p *Plugin) Name() string {
	return PluginName
}
