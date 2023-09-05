package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	avro "github.com/linkedin/goavro/v2"
	"github.com/redpanda-data/redpanda/src/go/transform-sdk"
	sr "github.com/redpanda-data/redpanda/src/go/transform-sdk/sr"
)

var (
	destinationCodec 	avro.Codec
	sourceCodec 		avro.Codec
	inputTopic			string
	inputTopicStripped 	string
	destinationSchemaID int
)

type rawRecordValue struct {
	Metadata 				metadata 			`avro:"metadata" json:"metadata"`
	BusinessDataPayload 	json.RawMessage  	`avro:"business_data_payload" json:"business_data_payload"` 
}

type metadata struct {
	MessageKey 				string				`avro:"message_key" json:"message_key"`
	CreatedDate 			int32				`avro:"created_date" json:"created_date"`
	UpdatedDate 			int32				`avro:"updated_date" json:"updated_date"`
	OutboxPublishedDate 	int32				`avro:"outbox_published_date" json:"outbox_published_date"`
	EventType 				string				`avro:"event_type" json:"event_type"`
	EventTypeKeyName 		string				`avro:"event_type_key_name" json:"event_type_key_name"`
	EventMetadataVersion 	string				`avro:"event_metadata_version" json:"event_metadata_version"`
}

func init() {
	var(
		topicSet		bool
		topicPrefix		string
		schemaIDSet		bool
		prefixSet		bool
		err				error
	)
	inputTopic, topicSet = os.LookupEnv("INPUT_TOPIC")
	if !topicSet {
		panic("INPUT_TOPIC environment variable not set")
	}

	destinationSchemaIDStr, schemaIDSet := os.LookupEnv("DESTINATION_SCHEMA_ID")
	if !schemaIDSet {
		panic("DESTINATION_SCHEMA_ID environment variable not set")
	}

	destinationSchemaID, err = strconv.Atoi(destinationSchemaIDStr)
	maybeDie(err,fmt.Sprintf("DESTINATION_SCHEMA_ID not an integer: %s", destinationSchemaIDStr))

	topicPrefix, prefixSet = os.LookupEnv("TOPIC_PREFIX")
	if !prefixSet {
		logger.Warn(fmt.Sprintf("TOPIC_PREFIX environment variable not set - not stripping prefix"))
		inputTopicStripped = inputTopic
	} else {
		inputTopicStripped = strings.TrimPrefix(inputTopic, topicPrefix)
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
	registry := sr.NewClient()
	schema, err := registry.LookupSchemaById(destinationSchemaID)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for ID: %d", destinationSchemaID))
	logger.Debug(fmt.Sprintf("Schema: %s", schema.Schema))
	
	avroCodec, err := avro.NewCodec(schema.Schema)
	maybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))
	destinationCodec = *avroCodec
	redpanda.OnRecordWritten(toAvro)
}

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e redpanda.WriteEvent) map[string]any {
	rawEvent, err := json.Marshal(e.Record().Value)
	maybeDie(err, "unable to parse raw event: %+v")
	logger.Debug(fmt.Sprintf("Raw Event %s", rawEvent))

	sourceSchemaID, err := sr.ExtractID(rawEvent)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for id: %d", sourceSchemaID))

	registry := sr.NewClient()
	sourceSchema, err := registry.LookupSchemaById(sourceSchemaID)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for id: %d", sourceSchemaID))
	logger.Debug(fmt.Sprintf("Schema: %s", sourceSchema.Schema))

	c, err := avro.NewCodec(sourceSchema.Schema)
	maybeDie(err,fmt.Sprintf("Error creating Avro codec: %+v", err))
	sourceCodec = *c

	native, _, err := sourceCodec.NativeFromBinary(rawEvent)
    maybeDie(err, fmt.Sprintf("Error creating native from binary: %+v", err))

	textualOut, err := sourceCodec.TextualFromNative(nil, native)
    maybeDie(err, fmt.Sprintf("Error creating text from binary: %+v", err))

    var out = rawRecordValue{}
    err = json.Unmarshal(textualOut, &out)
    maybeDie(err, fmt.Sprintf("Error creating json: %+v", err))

	m := map[string]any{
		"topic": inputTopicStripped,
		"message_key": out.Metadata.MessageKey,
		"outbox_published_date": out.Metadata.OutboxPublishedDate,
	}

	return m
}

// toAvro transforms a redpanda.WriteEvent into a slice of redpanda.Record objects.
//
// It takes a redpanda.WriteEvent as a parameter and returns a slice of redpanda.Record objects or error.
func toAvro(event redpanda.WriteEvent) ([]redpanda.Record, error) {
	m := doTransform(event)

	encoded, err := destinationCodec.BinaryFromNative(nil, m)
	maybeDie(err, fmt.Sprintf("Unable to encode map: %+v", err))

	record := redpanda.Record{
		Key:   event.Record().Key,
		Value: encoded,
	}

	return []redpanda.Record{record}, nil
}