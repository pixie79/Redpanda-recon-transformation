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

	LogLevel = GetEnvDefault("LOG_LEVEL", "INFO")

	inputTopic = GetEnvOrDie("INPUT_TOPIC")

	destinationSchemaID = GetEnvOrDie("DESTINATION_SCHEMA_ID")

	destinationSchemaIDInt, err = strconv.Atoi(destinationSchemaID)
	MaybeDie(err, fmt.Sprintf("DESTINATION_SCHEMA_ID not an integer: %s", destinationSchemaID))

	topicPrefix, set = os.LookupEnv("TOPIC_PREFIX")
	if !set {
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
	}

	destinationSchema := GetSchema(destinationSchemaID)
	destinationCodec, err = avro.NewCodec(destinationSchema)
	MaybeDie(err, "Error creating Avro codec")
}

// main is the entry point of the program.
//
// It registers a schema, performs setup, and runs a transformer function.
// It does not have any parameters.
// It does not return any values.
func main() {
	Print("INFO", "Running transformer")
	redpanda.OnRecordWritten(toAvro)
}

func toAvro(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	rawEvent, err := json.Marshal(e.Record().Value)
	MaybeDie(err, "unable to parse raw event")
	Print("DEBUG", fmt.Sprintf("Raw Event %s", rawEvent))

	sourceSchemaID, err := sr.ExtractID(e.Record().Value)
	if err != nil {
		return nil, err
	}

	sourceSchema := GetSchema(fmt.Sprintf("%d", sourceSchemaID))
	nestedMap := DecodeAvro(sourceSchema, rawEvent)

	messageKey, outboxPublishedDate, ok := getValues(nestedMap)
	if !ok {
		Die("Unable to get message key")
	}

	hdr := EncodedBuffer(destinationSchemaIDInt)

	encoded, err := destinationCodec.BinaryFromNative(hdr, map[string]interface{}{
		"topic":                 inputTopicStripped,
		"message_key":           messageKey,
		"outbox_published_date": outboxPublishedDate,
	})

	Print("DEBUG", fmt.Sprintf("Data: %v", encoded))
	Print("DEBUG", fmt.Sprintf("Data: %s", string(encoded)))

	record := redpanda.Record{
		Key:   e.Record().Key,
		Value: encoded,
	}

	Print("DEBUG", fmt.Sprintf("Record: %+v", record))
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

// GetSchemaIdFromPayload returns the schema id from the payload
func GetSchemaIdFromPayload(msg []byte) int {
	schemaID := binary.BigEndian.Uint32(msg[1:5])
	return int(schemaID)
}

// decodeAvro decodes an Avro event using the provided schema and returns a nested map[string]interface{}.
//
// Parameters:
// - schema: The Avro schema used for decoding the event (string).
// - event: The Avro event to be decoded ([]byte).
//
// Returns:
// - nestedMap: The decoded event as a nested map[string]interface{}.
func DecodeAvro(schema string, event []byte) map[string]interface{} {
	sourceCodec, err := avro.NewCodec(schema)
	MaybeDie(err, "Error creating Avro codec")

	strEvent := strings.Replace(string(event), "\"", "", -1)
	newEvent, err := B64DecodeMsg(strEvent, 5)
	MaybeDie(err, "Error decoding base64")
	native, _, err := sourceCodec.NativeFromBinary(newEvent)
	MaybeDie(err, "Error creating native from binary")
	nestedMap, ok := native.(map[string]interface{})
	if !ok {
		Die("Unable to convert native to map[string]interface{}")
	}
	return nestedMap
}

// GetEnvDefault retrieves the value of the environment variable specified by the key.
// If the environment variable does not exist, it returns the default value.
//
// Parameters:
// - key: the name of the environment variable to retrieve.
// - defaultVal: the value to return if the environment variable does not exist.
//
// Return:
// - string: the value of the environment variable or the default value.
func GetEnvDefault(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// EncodedBuffer returns the encoded buffer of an integer.
//
// It takes an integer as input and returns a byte slice that represents the encoded buffer.
func EncodedBuffer(schemaId int) []byte {
	byteArray := make([]byte, 4)
	binary.BigEndian.PutUint32(byteArray, uint32(schemaId))
	hdr := append([]byte{0}, byteArray...)
	return hdr
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

// Die exits the program after logging an error message.
//
// It takes a message as a parameter and does not return anything.
func Die(msg string) {
	panic(msg)
}

// MaybeDie is a function that checks if an error exists and calls the Die function with a formatted error message if it does.
//
// Parameters:
// - err: the error to check.
// - msg: the message to include in the error message.
func MaybeDie(err error, msg string) {
	if err != nil {
		Die(fmt.Sprintf("%s: %+v", msg, err))
	}
}

// GetEnvOrDie returns the value of the specified environment variable or exits the program.
//
// It takes a key string as a parameter and returns a string value.
func GetEnvOrDie(key string) string {
	value, set := os.LookupEnv(key)
	if !set {
		Die(fmt.Sprintf("%s: environment variable not set", key))
	}
	return value
}

// GetSchema retrieves the schema with the given ID from the specified URL.
//
// Parameters:
// - id: The ID of the schema (as a string).
// - url: The URL of the Schema Registry.
//
// Returns:
// - The retrieved schema (as a string).
func GetSchema(id string) string {
	registry := sr.NewClient()
	schemaIdInt, err := strconv.Atoi(id)
	Print("DEBUG", fmt.Sprintf("Schema ID: %s", id))
	MaybeDie(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", id))
	schema, err := registry.LookupSchemaById(schemaIdInt)
	MaybeDie(err, fmt.Sprintf("Unable to retrieve schema for ID: %s", id))
	return schema.Schema
}

// Print prints the given message with the specified log level.
//
// Parameters:
//   - level: the log level to use (e.g. "INFO", "ERROR").
//   - msg: the message to be printed.
func Print(level, msg string) {
	switch strings.ToUpper(LogLevel) {
	case "WARNING":
		fmt.Printf("%s: %s\n", strings.ToUpper(level), msg)
	case "ERROR":
		fmt.Printf("%s: %s\n", strings.ToUpper(level), msg)
	case "DEBUG":
		fmt.Printf("%s: %s\n", strings.ToUpper(level), msg)
	case "INFO":
		fmt.Printf("%s: %s\n", strings.ToUpper(level), msg)
	}

}
