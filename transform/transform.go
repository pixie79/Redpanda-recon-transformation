package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/patrickmn/go-cache"
	"golang.org/x/exp/slog"

	avro "github.com/linkedin/goavro/v2"
	redpanda "github.com/redpanda-data/redpanda/src/go/transform-sdk"
	sr "github.com/redpanda-data/redpanda/src/go/transform-sdk/sr"
)

var (
	logger   *slog.Logger
	logLevel string
	// Create a cache with a default expiration time of 5 minutes, and which
	// purges expired items every 10 minutes
	schemaCache         = cache.New(2*time.Hour, 10*time.Minute)
	inputTopicStripped  string
	destinationSchemaID string
)

type rawRecordValue struct {
	Metadata            metadata        `avro:"metadata" json:"metadata"`
	BusinessDataPayload json.RawMessage `avro:"business_data_payload" json:"business_data_payload"`
}

type metadata struct {
	MessageKey           string `avro:"message_key" json:"message_key"`
	CreatedDate          int32  `avro:"created_date" json:"created_date"`
	UpdatedDate          int32  `avro:"updated_date" json:"updated_date"`
	OutboxPublishedDate  int32  `avro:"outbox_published_date" json:"outbox_published_date"`
	EventType            string `avro:"event_type" json:"event_type"`
	EventTypeKeyName     string `avro:"event_type_key_name" json:"event_type_key_name"`
	EventMetadataVersion string `avro:"event_metadata_version" json:"event_metadata_version"`
}

func init() {
	var (
		inputTopic  string
		set         bool
		topicPrefix string
	)

	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		slog.Debug("No .env file found")
	}
	logLevel, set = os.LookupEnv("LOG_LEVEL")
	if !set {
		logLevel = "INFO"
	}

	logger = initLog()

	inputTopic, set = os.LookupEnv("INPUT_TOPIC")
	if !set {
		die("", "INPUT_TOPIC environment variable not set")
	}

	destinationSchemaID, set = os.LookupEnv("DESTINATION_SCHEMA_ID")
	if !set {
		die("", "DESTINATION_SCHEMA_ID environment variable not set")
	}

	topicPrefix, set = os.LookupEnv("TOPIC_PREFIX")
	if !set {
		logger.Debug("TOPIC_PREFIX environment variable not set - not stripping prefix")
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
	}
}

func initLog() *slog.Logger {
	switch logLevel {
	case "DEBUG":
		opts := &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
		logger.Info(fmt.Sprintf("Loglevel set to: %s", logLevel))
		slog.SetDefault(logger)
		return logger
	default:
		opts := &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
		logger.Info(fmt.Sprintf("Loglevel set to: %s", logLevel))
		slog.SetDefault(logger)
		return logger
	}
}

func die(msg string, args ...any) {
	logger.Error(fmt.Sprintf("%+v", args...))
	os.Exit(1)
}

func maybeDie(err error, msg string, args ...any) {
	if err != nil {
		die(msg, args...)
	}
}

func main() {
	// Register schema:
	//	jq '. | {schema: tojson}' schema.avsc | \
	//		curl -X POST "http://localhost:58646/subjects/10x_event_history_avro-value/versions" \
	//		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
	//		-d @-
	// Register your transform function.
	// This is a good place to perform other setup too.
	// Create Avro codec to use in transform function

	redpanda.OnRecordWritten(toAvro)
}

func getSchema(id string) *avro.Codec {
	schema, found := schemaCache.Get(id)
	if found {
		codec, err := avro.NewCodec(schema.(string))
		maybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))
		return codec
	}

	registry := sr.NewClient()
	schemaIdInt, err := strconv.Atoi(id)
	maybeDie(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", id))
	remoteSchema, err := registry.LookupSchemaById(schemaIdInt)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for ID: %s", id))
	logger.Debug(fmt.Sprintf("Schema: %s", remoteSchema.Schema))

	schemaCache.Set(id, remoteSchema.Schema, cache.DefaultExpiration)
	codec, err := avro.NewCodec(remoteSchema.Schema)
	maybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))

	return codec
}

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e redpanda.WriteEvent) map[string]any {
	rawEvent, err := json.Marshal(e.Record().Value)
	maybeDie(err, "unable to parse raw event: %+v")
	logger.Debug(fmt.Sprintf("Raw Event %s", rawEvent))

	sourceSchemaID, err := sr.ExtractID(rawEvent)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for id: %d", sourceSchemaID))

	sourceCodec := *getSchema(fmt.Sprintf("%d", sourceSchemaID))

	native, _, err := sourceCodec.NativeFromBinary(rawEvent)
	maybeDie(err, fmt.Sprintf("Error creating native from binary: %+v", err))

	textualOut, err := sourceCodec.TextualFromNative(nil, native)
	maybeDie(err, fmt.Sprintf("Error creating text from binary: %+v", err))

	var out = rawRecordValue{}
	err = json.Unmarshal(textualOut, &out)
	maybeDie(err, fmt.Sprintf("Error creating json: %+v", err))

	m := map[string]any{
		"topic":                 inputTopicStripped,
		"message_key":           out.Metadata.MessageKey,
		"outbox_published_date": out.Metadata.OutboxPublishedDate,
	}

	return m
}

// toAvro transforms a redpanda.WriteEvent into a slice of redpanda.Record objects.
//
// It takes a redpanda.WriteEvent as a parameter and returns a slice of redpanda.Record objects or error.
func toAvro(event redpanda.WriteEvent) ([]redpanda.Record, error) {
	destinationCodec := getSchema(destinationSchemaID)
	transformedData := doTransform(event)

	encoded, err := destinationCodec.BinaryFromNative(nil, transformedData)
	maybeDie(err, fmt.Sprintf("Unable to encode map: %+v", err))

	record := redpanda.Record{
		Key:   event.Record().Key,
		Value: encoded,
	}

	return []redpanda.Record{record}, nil
}
