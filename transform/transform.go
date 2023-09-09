package main

// Register schema:
// jq '. | {schema: tojson}' schema.avsc | \
// 	curl -X POST "http://localhost:58646/subjects/10x_event_history_avro-value/versions" \
// 	-H "Content-Type: application/vnd.schemaregistry.v1+json" \
// 	-d @-
	
import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/slog"

	avro "github.com/linkedin/goavro/v2"
	redpanda "github.com/redpanda-data/redpanda/src/go/transform-sdk"
	sr "github.com/redpanda-data/redpanda/src/go/transform-sdk/sr"
	kafka "github.com/pixie79/data-utils/kafka"
	utils "github.com/pixie79/data-utils/utils"
)

var (
	// Create a cache with a default expiration time of 5 minutes, and which
	// purges expired items every 10 minutes
	// schemaCache         = cache.New(2*time.Hour, 10*time.Minute)
	inputTopicStripped     string
	destinationCodec       *avro.Codec
	destinationSchemaID    string
	destinationSchemaIDInt int
	sd                     sr.Serde[*destinationRecordValue]
)

type destinationRecordValue struct {
	Topic               string `avro:"topic" json:"topic"`
	MessageKey          string `avro:"message_key" json:"message_key"`
	OutboxPublishedDate int64  `avro:"outbox_published_date" json:"outbox_published_date"`
}

// init initializes the application.
//
// This function sets up the necessary environment variables and configuration
// for the application to run. It initializes the utils.Logger, retrieves the
// required input topic and destination schema ID from the environment
// variables, and performs necessary validations. It also registers the
// destination schema and encodes the destination record values.
func init() {
	var (
		inputTopic  string
		set         bool
		topicPrefix string
		err         error
	)

	inputTopic, set = os.LookupEnv("INPUT_TOPIC")
	if !set {
		utils.Die("INPUT_TOPIC environment variable not set")
	}

	destinationSchemaID, set = os.LookupEnv("DESTINATION_SCHEMA_ID")
	if !set {
		utils.Die("DESTINATION_SCHEMA_ID environment variable not set")
	}
	destinationSchemaIDInt, err = strconv.Atoi(destinationSchemaID)
	utils.MaybeDie(err, fmt.Sprintf("DESTINATION_SCHEMA_ID not an integer: %s", destinationSchemaID))

	topicPrefix, set = os.LookupEnv("TOPIC_PREFIX")
	if !set {
		utils.Logger.Debug("TOPIC_PREFIX environment variable not set - not stripping prefix")
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
	}

	utils.Logger.Info(fmt.Sprintf("Input Topic Stripped: %s", inputTopicStripped))
	destinationSchema := kafka.GetSchema(destinationSchemaID)
	destinationCodec, err = avro.NewCodec(destinationSchema)
	utils.MaybeDie(err, "Error creating Avro codec")
	utils.Logger.Info(fmt.Sprintf("Destination Schema: %s %d ", destinationSchemaID, destinationSchemaIDInt))
}


// main is the entry point of the program.
//
// It registers a schema, performs setup, and runs a transformer function.
// It does not have any parameters.
// It does not return any values.
func main() {
	utils.Logger.Info("Running transformer")
	redpanda.OnRecordWritten(toAvro)
}

func toAvro(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	rawEvent, err := json.Marshal(e.Record().Value)
	utils.MaybeDie(err, "unable to parse raw event")
	utils.Logger.Debug(fmt.Sprintf("Raw Event %s", rawEvent))

	sourceSchemaID, err := sr.ExtractID(e.Record().Value)
	utils.MaybeDie(err, fmt.Sprintf("Unable to retrieve schema for id: %d", sourceSchemaID))

	sourceSchema := *kafka.GetSchema(fmt.Sprintf("%d", sourceSchemaID))
	nestedMap := kafka.DecodeAvro(sourceSchema, rawEvent)

	messageKey, outboxPublishedDate, ok := getValues(nestedMap)
	if !ok {
		utils.Die("Unable to get message key")
	}

	encodedBuffer := Utils.EncodedBuffer(destinationSchemaIDInt)

	utils.Logger.Info(fmt.Sprintf("ID: %d", destinationSchemaIDInt))
	utils.Logger.Info("**************************************************")
	utils.Logger.Info(fmt.Sprintf("Encoded Buffer: %v", encodedBuffer))
	encoded, err := destinationCodec.BinaryFromNative(nil, map[string]interface{}{
		"topic":                 inputTopicStripped,
		"message_key":           messageKey,
		"outbox_published_date": outboxPublishedDate,
	})
	utils.MaybeDie(err, "Unable to encode map")
	data := append(encodedBuffer, encoded...)
	utils.Logger.Info(fmt.Sprintf("Data: %v", data))

	record := redpanda.Record{
		Key:   event.Record().Key,
		Value: append(data),
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
				outboxPublishedDateTime = m["outbox_published_date"].(time.Time)
				return messageKey, outboxPublishedDateTime.UnixMicro(), true
			}
		}
	}
	return "", -1, false
}
