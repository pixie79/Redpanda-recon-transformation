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

	avro "github.com/linkedin/goavro/v2"
	redpanda "github.com/redpanda-data/redpanda/src/go/transform-sdk"
	sr "github.com/redpanda-data/redpanda/src/go/transform-sdk/sr"
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
	LogLevel               string
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

	inputTopic = getEnvOrDie("INPUT_TOPIC")

	destinationSchemaID = getEnvOrDie("DESTINATION_SCHEMA_ID")

	destinationSchemaIDInt, err = strconv.Atoi(destinationSchemaID)
	handleError(err, fmt.Sprintf("DESTINATION_SCHEMA_ID not an integer: %s", destinationSchemaID))

	topicPrefix, set = os.LookupEnv("TOPIC_PREFIX")
	if !set {
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
	}

	destinationSchema := getSchema(destinationSchemaID)
	destinationCodec, err = avro.NewCodec(destinationSchema)
	handleError(err, "Error creating Avro codec")
}

// main is the entry point of the program.
//
// It registers a schema, performs setup, and runs a transformer function.
// It does not have any parameters.
// It does not return any values.
func main() {
	fmt.Printf("Running transformer\n")
	redpanda.OnRecordWritten(toAvro)
}

func toAvro(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	rawEvent, err := json.Marshal(e.Record().Value)
	handleError(err, "unable to parse raw event")
	fmt.Printf("Raw Event %s\n", rawEvent)

	sourceSchemaID, err := sr.ExtractID(e.Record().Value)
	if err != nil {
		return nil, err
	}

	sourceSchema := getSchema(fmt.Sprintf("%d", sourceSchemaID))
	nestedMap := decodeAvro(sourceSchema, rawEvent)

	messageKey, outboxPublishedDate, ok := getValues(nestedMap)
	if !ok {
		panic("Unable to get message key")
	}

	hdr := encodeBuffer(destinationSchemaIDInt)

	encoded, err := destinationCodec.BinaryFromNative(hdr, map[string]interface{}{
		"topic":                 inputTopicStripped,
		"message_key":           messageKey,
		"outbox_published_date": outboxPublishedDate,
	})

	fmt.Printf("Data: %v\n", encoded)
	fmt.Printf("Data: %s\n", string(encoded))

	record := redpanda.Record{
		Key:   e.Record().Key,
		Value: encoded,
	}

	fmt.Printf("Record: %+v\n", record)
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
	handleError(err, "Error creating Avro codec")

	eventStr := strings.Replace(string(event), "\"", "", -1)
	decodedEvent, err := b64DecodeMsg(eventStr, 5)
	handleError(err, "Error decoding base64")

	native, _, err := sourceCodec.NativeFromBinary(decodedEvent)
	handleError(err, "Error creating native from binary")

	nestedMap, ok := native.(map[string]interface{})
	if !ok {
		panic("Unable to convert native to map[string]interface{}")
	}

	return nestedMap
}

// getEnvDefault returns the value of the environment variable associated with the provided key.
// If the environment variable does not exist, it returns the provided default value.
//
// Parameters:
// - key: the key of the environment variable to retrieve.
// - defaultVal: the default value to return if the environment variable does not exist.
//
// Return type:
// - string: the value of the environment variable or the default value.
func getEnvDefault(key, defaultVal string) string {
	value, exists := os.LookupEnv(key)
	if exists {
		return value
	}
	return defaultVal
}

// getEnvOrDie retrieves the value of an environment variable given its key and returns it.
// If the environment variable does not exist, the function logs a fatal error and exits.
//
// Parameters:
// - key: the key of the environment variable to retrieve.
//
// Return:
// - string: the value of the environment variable.
func getEnvOrDie(key string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		panic(fmt.Sprintf("%s: environment variable not set", key))
	}
	return value
}

// encodeBuffer encodes the given schemaID into a byte array and returns the resulting header.
//
// The schemaID parameter is an integer representing the ID of the schema to be encoded.
// The function returns a byte array that represents the header, including the schemaID.
func encodeBuffer(schemaID int) []byte {
	byteArray := make([]byte, 4)
	binary.BigEndian.PutUint32(byteArray, uint32(schemaID))
	header := append([]byte{0}, byteArray...)
	return header
}

// b64DecodeMsg decodes a base64 encoded key and returns a subset of the key starting from the specified offset.
//
// Parameters:
//   - b64Key: The base64 encoded key to be decoded.
//   - offsetF: An optional integer representing the offset from which to start the subset of the key. If not provided, it defaults to 7.
//
// Returns:
//   - []byte: The subset of the key starting from the specified offset.
//   - error: An error if the decoding or subset operation fails.
func b64DecodeMsg(b64Key string, offsetF ...int) ([]byte, error) {
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

// handleError handles the given error and logs a fatal message.
//
// It takes two parameters:
// - err: the error to be handled
// - message: the message to be logged along with the error
//
// This function does not return any value.
func handleError(err error, message string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %+v", message, err))
	}
}

// getSchema retrieves the schema with the given ID from the specified URL.
//
// Parameters:
// - id: The ID of the schema (as a string).
//
// Returns:
// - The retrieved schema (as a string).
func getSchema(id string) string {
	client := sr.NewClient()
	schemaID, err := strconv.Atoi(id)
	handleError(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", id))
	schema, err := client.LookupSchemaById(schemaID)
	handleError(err, fmt.Sprintf("Unable to retrieve schema for ID: %s", id))
	return schema.Schema
}
