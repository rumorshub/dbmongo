package dbmongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

var _ MongoDB = (*Database)(nil)

var ErrNoDB = errors.New("database name not found in URI")

const (
	ErrMsgClient   = "failed to create mongodb client due to error: %w"
	ErrMsgDatabase = "failed to create mongodb database due to error: %w"
)

type DB interface {
	// Collection gets a handle for a collection with the given name configured with the given CollectionOptions.
	Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
}

type MongoDB interface {
	DB

	// Name returns the name of the database.
	Name() string

	// Drop drops the database on the server. This method ignores "namespace not found" errors so it is safe to drop
	// a database that does not exist on the server.
	Drop(ctx context.Context) error

	// Client returns the Client the Database was created from.
	Client() *mongo.Client

	// Watch returns a change stream for all changes to the corresponding database. See
	// https://www.mongodb.com/docs/manual/changeStreams/ for more information about change streams.
	//
	// The Database must be configured with read concern majority or no read concern for a change stream to be created
	// successfully.
	//
	// The pipeline parameter must be a slice of documents, each representing a pipeline stage. The pipeline cannot be
	// nil but can be empty. The stage documents must all be non-nil. See https://www.mongodb.com/docs/manual/changeStreams/ for
	// a list of pipeline stages that can be used with change streams. For a pipeline of bson.D documents, the
	// mongo.Pipeline{} type can be used.
	//
	// The opts parameter can be used to specify options for change stream creation (see the options.ChangeStreamOptions
	// documentation).
	Watch(ctx context.Context, pipeline any, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)

	// Aggregate executes an aggregate command the database. This requires MongoDB version >= 3.6 and driver version >=
	// 1.1.0.
	//
	// The pipeline parameter must be a slice of documents, each representing an aggregation stage. The pipeline
	// cannot be nil but can be empty. The stage documents must all be non-nil. For a pipeline of bson.D documents, the
	// mongo.Pipeline type can be used. See
	// https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/#db-aggregate-stages for a list of valid
	// stages in database-level aggregations.
	//
	// The opts parameter can be used to specify options for this operation (see the options.AggregateOptions documentation).
	//
	// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/aggregate/.
	Aggregate(ctx context.Context, pipeline any, opts ...*options.AggregateOptions) (*mongo.Cursor, error)

	// RunCommand executes the given command against the database. This function does not obey the Database's read
	// preference. To specify a read preference, the RunCmdOptions.ReadPreference option must be used.
	//
	// The runCommand parameter must be a document for the command to be executed. It cannot be nil.
	// This must be an order-preserving type such as bson.D. Map types such as bson.M are not valid.
	//
	// The opts parameter can be used to specify options for this operation (see the options.RunCmdOptions documentation).
	//
	// The behavior of RunCommand is undefined if the command document contains any of the following:
	// - A session ID or any transaction-specific fields
	// - API versioning options when an API version is already declared on the Client
	// - maxTimeMS when Timeout is set on the Client
	RunCommand(ctx context.Context, runCommand any, opts ...*options.RunCmdOptions) *mongo.SingleResult

	// RunCommandCursor executes the given command against the database and parses the response as a cursor. If the command
	// being executed does not return a cursor (e.g. insert), the command will be executed on the server and an error will
	// be returned because the server response cannot be parsed as a cursor. This function does not obey the Database's read
	// preference. To specify a read preference, the RunCmdOptions.ReadPreference option must be used.
	//
	// The runCommand parameter must be a document for the command to be executed. It cannot be nil.
	// This must be an order-preserving type such as bson.D. Map types such as bson.M are not valid.
	//
	// The opts parameter can be used to specify options for this operation (see the options.RunCmdOptions documentation).
	//
	// The behavior of RunCommandCursor is undefined if the command document contains any of the following:
	// - A session ID or any transaction-specific fields
	// - API versioning options when an API version is already declared on the Client
	// - maxTimeMS when Timeout is set on the Client
	RunCommandCursor(ctx context.Context, runCommand any, opts ...*options.RunCmdOptions) (*mongo.Cursor, error)

	// ListCollectionSpecifications executes a listCollections command and returns a slice of CollectionSpecification
	// instances representing the collections in the database.
	//
	// The filter parameter must be a document containing query operators and can be used to select which collections
	// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
	// collections.
	//
	// The opts parameter can be used to specify options for the operation (see the options.ListCollectionsOptions
	// documentation).
	//
	// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listCollections/.
	//
	// BUG(benjirewis): ListCollectionSpecifications prevents listing more than 100 collections per database when running
	// against MongoDB version 2.6.
	ListCollectionSpecifications(ctx context.Context, filter any, opts ...*options.ListCollectionsOptions) ([]*mongo.CollectionSpecification, error)

	// ListCollections executes a listCollections command and returns a cursor over the collections in the database.
	//
	// The filter parameter must be a document containing query operators and can be used to select which collections
	// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
	// collections.
	//
	// The opts parameter can be used to specify options for the operation (see the options.ListCollectionsOptions
	// documentation).
	//
	// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listCollections/.
	//
	// BUG(benjirewis): ListCollections prevents listing more than 100 collections per database when running against
	// MongoDB version 2.6.
	ListCollections(ctx context.Context, filter any, opts ...*options.ListCollectionsOptions) (*mongo.Cursor, error)

	// ListCollectionNames executes a listCollections command and returns a slice containing the names of the collections
	// in the database. This method requires driver version >= 1.1.0.
	//
	// The filter parameter must be a document containing query operators and can be used to select which collections
	// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
	// collections.
	//
	// The opts parameter can be used to specify options for the operation (see the options.ListCollectionsOptions
	// documentation).
	//
	// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listCollections/.
	//
	// BUG(benjirewis): ListCollectionNames prevents listing more than 100 collections per database when running against
	// MongoDB version 2.6.
	ListCollectionNames(ctx context.Context, filter any, opts ...*options.ListCollectionsOptions) ([]string, error)

	// CreateCollection executes a create command to explicitly create a new collection with the specified name on the
	// server. If the collection being created already exists, this method will return a mongo.CommandError. This method
	// requires driver version 1.4.0 or higher.
	//
	// The opts parameter can be used to specify options for the operation (see the options.CreateCollectionOptions
	// documentation).
	//
	// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/create/.
	CreateCollection(ctx context.Context, name string, opts ...*options.CreateCollectionOptions) error

	// CreateView executes a create command to explicitly create a view on the server. See
	// https://www.mongodb.com/docs/manual/core/views/ for more information about views. This method requires driver version >=
	// 1.4.0 and MongoDB version >= 3.4.
	//
	// The viewName parameter specifies the name of the view to create.
	//
	// # The viewOn parameter specifies the name of the collection or view on which this view will be created
	//
	// The pipeline parameter specifies an aggregation pipeline that will be exececuted against the source collection or
	// view to create this view.
	//
	// The opts parameter can be used to specify options for the operation (see the options.CreateViewOptions
	// documentation).
	CreateView(ctx context.Context, viewName, viewOn string, pipeline any, opts ...*options.CreateViewOptions) error

	// ReadConcern returns the read concern used to configure the Database object.
	ReadConcern() *readconcern.ReadConcern

	// ReadPreference returns the read preference used to configure the Database object.
	ReadPreference() *readpref.ReadPref

	// WriteConcern returns the write concern used to configure the Database object.
	WriteConcern() *writeconcern.WriteConcern

	Ping(ctx context.Context) error

	Close(ctx context.Context) error
}

type Database struct {
	*mongo.Database
}

func NewDatabase(ctx context.Context, cfg Config) (*Database, error) {
	dbName, err := ExtractDatabaseName(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgDatabase, err)
	}

	client, err := NewClient(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}

	db := &Database{Database: client.Database(dbName)}

	if cfg.Ping {
		if err = db.Ping(ctx); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *Database) Ping(ctx context.Context) error {
	if err := db.Client().Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("could not connect to MongoDB: %w", err)
	}
	return nil
}

func (db *Database) Close(ctx context.Context) error {
	return db.Client().Disconnect(ctx)
}

func NewClient(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf(ErrMsgClient, err)
	}
	return client, nil
}

func ExtractDatabaseName(uri string) (string, error) {
	cs, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return "", err
	}
	if len(cs.Database) == 0 {
		return "", ErrNoDB
	}
	return cs.Database, nil
}
