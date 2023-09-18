package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"

	avro "github.com/linkedin/goavro/v2"
	duKafka "github.com/pixie79/data-utils/kafka"
	duSr "github.com/pixie79/data-utils/sr"
	tuUtils "github.com/pixie79/tiny-utils/utils"
)

var (
	fileName               = flag.String("filename", "", "filename")
	destinationSchemaID    string
	destinationSchemaIDInt int
	destinationTopic       string
	schemaURL              string
)

func init() {
	var (
		err error
	)

	flag.Parse()

	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		tuUtils.Print("DEBUG", "No .env file found")
	}

	destinationTopic = tuUtils.GetEnvOrDie("TOPIC")

	destinationSchemaID = tuUtils.GetEnvOrDie("SCHEMA_ID")

	destinationSchemaIDInt, err = strconv.Atoi(destinationSchemaID)
	tuUtils.MaybeDie(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", destinationSchemaID))

	schemaURL = tuUtils.GetEnvOrDie("SCHEMA_REGISTRY")
}

func main() {
	remoteSchema := duSr.GetSchema(destinationSchemaID, schemaURL)
	codec, err := avro.NewCodec(remoteSchema)
	tuUtils.MaybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))

	// Read data file
	dataFile, err := os.ReadFile(*fileName)
	tuUtils.MaybeDie(err, fmt.Sprintf("Error reading file: %+v", err))

	// Create OCF reader
	ocfReader, err := avro.NewOCFReader(bytes.NewReader(dataFile))
	tuUtils.MaybeDie(err, fmt.Sprintf("Error creating OCF reader: %+v", err))

	// Read and process records
	var avroRecords []*kgo.Record
	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		if err != nil {
			tuUtils.Print("ERROR", (fmt.Sprintf("Error reading OCF file: %+v", err)))
		}

		nestedMap, ok := record.(map[string]interface{})
		if !ok {
			tuUtils.Die("Unable to get nested map")
		}

		messageKey, ok := getMessageKey(nestedMap)
		if !ok {
			tuUtils.Die("Unable to get message key")
		}
		tuUtils.Print("INFO", fmt.Sprintf("Message Key: %s", messageKey))
		encoded, err := codec.BinaryFromNative(nil, record)
		tuUtils.MaybeDie(err, fmt.Sprintf("Unable to encode map: %+v", err))

		encodedBuffer := tuUtils.EncodedBuffer(destinationSchemaIDInt)
		tuUtils.Print("DEBUG", "**************************************************")
		tuUtils.Print("DEBUG", fmt.Sprintf("Encoded Buffer: %v", encodedBuffer))
		tuUtils.Print("DEBUG", fmt.Sprintf("Destination Schema ID: %d", destinationSchemaIDInt))
		data := append(encodedBuffer, encoded...)

		// Create avro record
		avroRecord := &kgo.Record{
			Key:   []byte(messageKey),
			Value: data,
			Topic: destinationTopic,
		}
		tuUtils.Print("DEBUG", fmt.Sprintf("Avro Record: %+v", avroRecord))
		avroRecords = append(avroRecords, avroRecord)
	}

	// Submit records
	client := duKafka.CreateConnectionAndSubmitRecords(context.Background())
	err = duKafka.SubmitRecords(context.Background(), client, avroRecords)
	tuUtils.MaybeDie(err, "Error submitting records")

}

func getMessageKey(nestedMap map[string]interface{}) (messageKey string, ok bool) {
	for key, value := range nestedMap {
		m, ok := value.(map[string]interface{})
		if ok {
			if key == "metadata" {
				return m["message_key"].(string), true
			}
		}
	}
	return "", false
}
