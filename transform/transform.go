package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	// "github.com/patrickmn/go-cache"

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
	// schemaCache         = cache.New(2*time.Hour, 10*time.Minute)
	inputTopicStripped  string
	destinationSchemaID string
	sd                  sr.Serde[*destinationRecordValue]
)

type destinationRecordValue struct {
	Topic               string `avro:"topic" json:"topic"`
	MessageKey          string `avro:"message_key" json:"message_key"`
	OutboxPublishedDate int64  `avro:"outbox_published_date" json:"outbox_published_date"`
}

// init initializes the application.
//
// This function sets up the necessary environment variables and configuration
// for the application to run. It initializes the logger, retrieves the
// required input topic and destination schema ID from the environment
// variables, and performs necessary validations. It also registers the
// destination schema and encodes the destination record values.
func init() {
	var (
		inputTopic  string
		set         bool
		topicPrefix string
	)

	logLevel, set = os.LookupEnv("LOG_LEVEL")
	if !set {
		logLevel = "INFO"
	}

	logger = initLog()

	inputTopic, set = os.LookupEnv("INPUT_TOPIC")
	if !set {
		die("INPUT_TOPIC environment variable not set")
	}

	destinationSchemaID, set = os.LookupEnv("DESTINATION_SCHEMA_ID")
	if !set {
		die("DESTINATION_SCHEMA_ID environment variable not set")
	}
	destinationSchemaIdInt, err := strconv.Atoi(destinationSchemaID)
	maybeDie(err, fmt.Sprintf("DESTINATION_SCHEMA_ID not an integer: %s", destinationSchemaID))

	topicPrefix, set = os.LookupEnv("TOPIC_PREFIX")
	if !set {
		logger.Debug("TOPIC_PREFIX environment variable not set - not stripping prefix")
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
	}

	destinationSchema := getSchema(destinationSchemaID)
	destinationCodec, err := avro.NewCodec(destinationSchema.Schema)
	maybeDie(err, "Error creating Avro codec")
	logger.Info(fmt.Sprintf("Destination Schema: %s %d ", destinationSchemaID, destinationSchemaIdInt))

	sd.Register(
		destinationSchemaIdInt,
		sr.EncodeFn[*destinationRecordValue](func(e *destinationRecordValue) ([]byte, error) {
			return destinationCodec.BinaryFromNative(nil, map[string]interface{}{
				"topic":                 inputTopicStripped,
				"message_key":           e.MessageKey,
				"outbox_published_date": e.OutboxPublishedDate,
			})
		}),
	)
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

func die(msg string) {
	logger.Error(msg)
	os.Exit(1)
}

func maybeDie(err error, msg string) {
	if err != nil {
		die(fmt.Sprintf("%s: %+v", msg, err))
	}
}

// main is the entry point of the program.
//
// It registers a schema, performs setup, and runs a transformer function.
// It does not have any parameters.
// It does not return any values.
func main() {
	// Register schema:
	// jq '. | {schema: tojson}' schema.avsc | \
	// 	curl -X POST "http://localhost:58646/subjects/10x_event_history_avro-value/versions" \
	// 	-H "Content-Type: application/vnd.schemaregistry.v1+json" \
	// 	-d @-
	// Register your transform function.
	// This is a good place to perform other setup too.
	// Create Avro codec to use in transform function
	logger.Info("Running transformer")
	redpanda.OnRecordWritten(toAvro)
}

// getSchema retrieves the schema with the given ID.
//
// Parameters:
// - id: the ID of the schema to retrieve.
//
// Returns:
// - *sr.Schema: the retrieved schema.
func getSchema(id string) *sr.Schema {
	// schemaCache := cache.New(2*time.Hour, 10*time.Minute)
	// schema, found := schemaCache.Get(id)
	// if found {
	// 	codec, err := avro.NewCodec(schema.(string))
	// 	maybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))
	// 	return codec
	// }

	registry := sr.NewClient()
	schemaIdInt, err := strconv.Atoi(id)
	logger.Debug(fmt.Sprintf("Schema ID: %s", id))
	maybeDie(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", id))
	schema, err := registry.LookupSchemaById(schemaIdInt)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for ID: %s", id))
	// schemaCache.Set(id, schema.Schema, cache.DefaultExpiration)
	return schema
}

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e redpanda.WriteEvent) []byte {
	rawEvent, err := json.Marshal(e.Record().Value)
	maybeDie(err, "unable to parse raw event")
	logger.Debug(fmt.Sprintf("Raw Event %s", rawEvent))

	sourceSchemaID, err := sr.ExtractID(e.Record().Value)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for id: %d", sourceSchemaID))

	sourceSchema := *getSchema(fmt.Sprintf("%d", sourceSchemaID))
	nestedMap := decodeAvro(sourceSchema.Schema, rawEvent)

	messageKey, outboxPublishedDate, ok := getValues(nestedMap)
	if !ok {
		logger.Error("Unable to get message key")
		panic("Unable to get message key")
	}

	record := &destinationRecordValue{
		Topic:               inputTopicStripped,
		MessageKey:          messageKey,
		OutboxPublishedDate: outboxPublishedDate,
	}

	encoded, err := sd.Encode(record)
	maybeDie(err, "Unable to encode map")

	return encoded
}

// toAvro transforms a redpanda.WriteEvent into a slice of redpanda.Record objects.
//
// It takes a redpanda.WriteEvent as a parameter and returns a slice of redpanda.Record objects or error.
func toAvro(event redpanda.WriteEvent) ([]redpanda.Record, error) {
	encodedValue := doTransform(event)
	record := redpanda.Record{
		Key:   event.Record().Key,
		Value: encodedValue,
	}
	return []redpanda.Record{record}, nil
}

// getValues returns the message key and outbox published date from a nested map.
//
// The function takes a nested map, `nestedMap`, as a parameter and iterates over its key-value pairs.
// If the value is a nested map and the key is "metadata", the function extracts the message key
// and outbox published date from the nested map and returns them along with a boolean indicating success.
//
// Parameters:
//   - nestedMap: a map[string]interface{} representing a nested map
//
// Returns:
//   - messageKey: a string representing the extracted message key
//   - outboxPublishedDate: an int64 representing the extracted outbox published date
//   - ok: a boolean indicating whether the extraction was successful
func getValues(nestedMap map[string]interface{}) (messageKey string, outboxPublishedDate int64, ok bool) {
	var outboxPublishedDateTime time.Time

	for key, value := range nestedMap {
		m, ok := value.(map[string]interface{})

		if ok {
			if key == "metadata" {
				messageKey = m["message_key"].(string)
				logger.Info(fmt.Sprintf("Key Type: %T", m["outbox_published_date"]))
				outboxPublishedDateTime = m["outbox_published_date"].(time.Time)
				return messageKey, outboxPublishedDateTime.UnixMicro(), true
			}
		}
	}
	return "", -1, false
}

// B64DecodeMsg decodes a base64 encoded key and returns a subset of the key starting from the specified offset.
//
// Parameters:
//   - b64Key: The base64 encoded key to be decoded.
//   - offsetF: An optional integer representing the offset from which to start the subset of the key. If not provided, it defaults to 7.
//
// Returns:
//   - []byte: The subset of the key starting from the specified offset.
//   - error: An error if the decoding or subset operation fails.
func B64DecodeMsg(b64Key string, offsetF ...int) ([]byte, error) {
	offset := 7
	if len(offsetF) > 0 {
		offset = offsetF[0]
	}

	key, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, err
	}

	result := key[offset:]
	return result, nil
}

// decodeAvro decodes an Avro event using the provided schema and returns a nested map[string]interface{}.
//
// Parameters:
// - schema: The Avro schema used for decoding the event (string).
// - event: The Avro event to be decoded ([]byte).
//
// Returns:
// - nestedMap: The decoded event as a nested map[string]interface{}.
func decodeAvro(schema string, event []byte) map[string]interface{} {
	sourceCodec, err := avro.NewCodec(schema)
	maybeDie(err, "Error creating Avro codec")

	strEvent := strings.Replace(string(event), "\"", "", -1)
	newEvent, err := B64DecodeMsg(strEvent, 5)
	maybeDie(err, "Error decoding base64")
	native, _, err := sourceCodec.NativeFromBinary(newEvent)
	maybeDie(err, "Error creating native from binary")
	// logger.Debug(prettyPrint(native))
	nestedMap, ok := native.(map[string]interface{})
	if !ok {
		die("Unable to convert native to map[string]interface{}")
	}

	return nestedMap

}
