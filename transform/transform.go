package main

// Register schema:
// jq '. | {schema: tojson}' schema.avsc | \
// 	curl -X POST "http://localhost:58646/subjects/10x_event_history_avro-value/versions" \
// 	-H "Content-Type: application/vnd.schemaregistry.v1+json" \
// 	-d @-

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	avro "github.com/linkedin/goavro/v2"

	tuAvro "github.com/pixie79/tiny-utils/avro"
	tuSr "github.com/pixie79/tiny-utils/sr"

	tuUtils "github.com/pixie79/tiny-utils/utils"
	redpanda "github.com/redpanda-data/redpanda/src/go/transform-sdk"
)

type destinationEvent struct {
	Topic               string `avro:"topic" json:"topic"`
	MessageKey          string `avro:"message_key" json:"message_key"`
	OutboxPublishedDate int64  `avro:"outbox_published_date" json:"outbox_published_date"`
}

var (
	inputTopicStripped     string
	destinationCodec       *avro.Codec
	destinationSchemaID    string
	destinationSchemaIDInt int
)

// init initializes the application.
//
// This function sets up the necessary environment variables and configuration
// for the application to run. It initializes the Logger, retrieves the
// required input topic and destination schema ID from the environment
// variables, and performs necessary validations. It also registers the
// destination schema and encodes the destination record values.
func init() {
	var (
		inputTopic  string
		topicPrefix string
		err         error
		set         bool
	)

	inputTopic = tuUtils.GetEnvOrDie("INPUT_TOPIC")

	destinationSchemaID = tuUtils.GetEnvOrDie("DESTINATION_SCHEMA_ID")

	destinationSchemaIDInt, err = strconv.Atoi(destinationSchemaID)
	tuUtils.MaybeDie(err, fmt.Sprintf("DESTINATION_SCHEMA_ID not an integer: %s", destinationSchemaID))

	topicPrefix, set = os.LookupEnv("TOPIC_PREFIX")
	if !set {
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
	}

	destinationSchema := tuSr.GetSchema(destinationSchemaID)
	destinationCodec, err = avro.NewCodec(destinationSchema)
	tuUtils.MaybeDie(err, "Error creating Avro codec")
}

// main is the entry point of the program.
//
// It registers a schema, performs setup, and runs a transformer function.
// It does not have any parameters.
// It does not return any values.
func main() {
	tuUtils.Print("INFO", "Running transformer")
	redpanda.OnRecordWritten(toAvro)
}

func toAvro(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	rawEvent, err := json.Marshal(e.Record().Value)
	tuUtils.MaybeDie(err, "unable to parse raw event")
	tuUtils.Print("DEBUG", fmt.Sprintf("Raw Event %s", rawEvent))

	sourceSchemaID, err := tuSr.ExtractID(e.Record().Value)
	if err != nil {
		return nil, err
	}

	sourceSchema := tuSr.GetSchema(fmt.Sprintf("%d", sourceSchemaID))
	nestedMap := tuAvro.DecodeAvro(sourceSchema, rawEvent)

	messageKey, outboxPublishedDate, ok := getValues(nestedMap)
	if !ok {
		tuUtils.Die("Unable to get message key")
	}

	encodedBuffer := tuUtils.EncodedBuffer(destinationSchemaIDInt)

	tuUtils.Print("DEBUG", fmt.Sprintf("ID: %d", destinationSchemaIDInt))
	tuUtils.Print("DEBUG", "**************************************************")
	destinationData := &destinationEvent{
		Topic:               inputTopicStripped,
		MessageKey:          messageKey,
		OutboxPublishedDate: outboxPublishedDate,
	}
	tuUtils.Print("DEBUG", fmt.Sprintf("destinationData: %+v", destinationData))

	textualIn, err := json.Marshal(destinationData)
	tuUtils.MaybeDie(err, "Unable to encode map JSON")
	tuUtils.Print("DEBUG", fmt.Sprintf("JSON: %s", textualIn))

	nativeIn, _, err := destinationCodec.NativeFromTextual(textualIn)
	tuUtils.MaybeDie(err, "Unable to encode map Text")
	tuUtils.Print("DEBUG", fmt.Sprintf("nativeIn: %s", nativeIn))

	encoded, err := destinationCodec.BinaryFromNative(nil, nativeIn)

	// encoded, err := destinationCodec.BinaryFromNative(nil, map[string]interface{}{
	// 	"topic":                 inputTopicStripped,
	// 	"message_key":           messageKey,
	// 	"outbox_published_date": outboxPublishedDate,
	// })

	tuUtils.MaybeDie(err, "Unable to encode map")
	tuUtils.Print("DEBUG", fmt.Sprintf("Encoded: %s", encoded))
	encodedBuffer = append(encodedBuffer, encoded...)
	tuUtils.Print("DEBUG", fmt.Sprintf("Data: %v", encodedBuffer))
	tuUtils.Print("DEBUG", fmt.Sprintf("Data: %s", string(encodedBuffer)))

	record := redpanda.Record{
		Key:   e.Record().Key,
		Value: encodedBuffer,
	}

	tuUtils.Print("DEBUG", fmt.Sprintf("Record: %+v", record))
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
