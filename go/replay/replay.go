package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"

	sr "github.com/landoop/schema-registry"
	avro "github.com/linkedin/goavro/v2"
)

var (
	fileName               = flag.String("filename", "", "filename")
	destinationSchemaID    string
	destinationSchemaIDInt int
	destinationTopic       string
	schemaURL              string
)

func init() {
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		slog.Info("No .env file found")
	}

	destinationTopic = getEnvOrDie("TOPIC")
	destinationSchemaID = getEnvOrDie("SCHEMA_ID")

	var err error
	destinationSchemaIDInt, err = strconv.Atoi(destinationSchemaID)
	handleError(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", destinationSchemaID))

	schemaURL = getEnvOrDie("SCHEMA_REGISTRY")
}

// main is the entry point of the program.
//
// It retrieves a remote schema using the provided destination schema ID and schema URL.
// Then, it creates an Avro codec using the retrieved schema.
// It reads a file and creates an OCF reader for it.
// It iterates over the records in the OCF file, converts them to Avro format,
// and appends them to a slice of Avro records.
// Finally, it submits the Avro records to a Kafka topic using a Kafka client.
func main() {
	remoteSchema, err := getSchema(destinationSchemaID, schemaURL)
	handleError(err, "Error getting schema")
	codec, err := avro.NewCodec(remoteSchema)
	handleError(err, "Error creating Avro codec")

	dataFile, err := os.ReadFile(*fileName)
	handleError(err, "Error reading file")

	ocfReader, err := avro.NewOCFReader(bytes.NewReader(dataFile))
	handleError(err, "Error creating OCF reader")

	var avroRecords []*kgo.Record
	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		if err != nil {
			fmt.Printf("Error reading OCF file: %+v\n", err)
			continue
		}

		nestedMap, ok := record.(map[string]interface{})
		if !ok {
			panic("Unable to get nested map")
		}

		messageKey, ok := getMessageKey(nestedMap)
		if !ok {
			panic("Unable to get message key")
		}

		fmt.Printf("Message Key: %s\n", messageKey)

		hdr := encodeBuffer(destinationSchemaIDInt)
		value, err := codec.BinaryFromNative(hdr, record)
		handleError(err, "Unable to encode map")

		avroRecord := &kgo.Record{
			Key:   []byte(messageKey),
			Value: value,
			Topic: destinationTopic,
		}
		avroRecords = append(avroRecords, avroRecord)
	}

	client, err := createKafkaConnection(context.Background())
	handleError(err, "Error creating Kafka client")

	err = submitRecords(context.Background(), client, avroRecords)
	handleError(err, "Error submitting records")
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

// getMessageKey returns the message key from a nested map.
//
// It takes a nestedMap parameter, which is a map[string]interface{} that contains nested maps.
// The function iterates over the nestedMap and checks if a key called "metadata" exists.
// If the "metadata" key exists and its value is a map[string]interface{}, it retrieves the value associated with the key "message_key".
// The function returns the message key as a string and a boolean indicating whether the operation was successful or not.
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

// getSchema retrieves the schema for a given schema ID from the Schema Registry.
//
// Parameters:
// - schemaID: The ID of the schema to retrieve.
// - registryURL: The URL of the Schema Registry.
//
// Returns:
// - string: The retrieved schema.
// - error: An error if the schema retrieval fails.
func getSchema(schemaID string, registryURL string) (string, error) {
	registry, err := sr.NewClient(registryURL)
	if err != nil {
		return "", fmt.Errorf("cannot connect to Schema Registry: %w", err)
	}

	schemaIDInt, err := strconv.Atoi(schemaID)
	if err != nil {
		return "", fmt.Errorf("schema ID not an integer: %w", err)
	}

	schema, err := registry.GetSchemaByID(schemaIDInt)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve schema for ID %s: %w", schemaID, err)
	}

	return schema, nil
}

// createKafkaConnection creates a Kafka connection.
//
// ctx: the context.Context to use for the connection.
//
// Returns:
// - *kgo.Client: the Kafka client.
// - error: an error if the connection could not be established.
func createKafkaConnection(ctx context.Context) (*kgo.Client, error) {
	transactionID := randomString(20)

	seedEnv := getEnvOrDie("KAFKA_SEEDS")
	seeds := []string{seedEnv}

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.TransactionalID(transactionID),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.RecordRetries(4),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
	// Initialize public CAs for TLS
	// opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))

	//// Initializes SASL/SCRAM 256
	// Get Credentials from context
	// opts = append(opts, kgo.SASL(scram.Auth{
	// 	User: credentials.Username,
	// 	Pass: credentials.Password,
	// }.AsSha256Mechanism()))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("could not connect to Kafka: %w", err)
	}

	return client, nil
}

// submitRecords submits Kafka records to a Kafka broker and commits them in a transaction.
//
// ctx - The context.Context object for cancellation signals and deadlines.
// client - The *kgo.Client object for Kafka operations.
// kafkaRecords - A slice of *kgo.Record objects representing the Kafka records to be submitted.
// Returns an error if any step in the process fails.
func submitRecords(ctx context.Context, client *kgo.Client, kafkaRecords []*kgo.Record) error {
	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	if err := produceMessages(ctx, client, kafkaRecords); err != nil {
		if rollbackErr := rollbackTransaction(client); rollbackErr != nil {
			return rollbackErr
		}
		return fmt.Errorf("failed to produce messages: %v", err)
	}

	if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	fmt.Printf("produced %d Kafka records\n", len(kafkaRecords))
	return nil
}

// produceMessages is a function that produces messages using a Kafka client.
//
// It takes the following parameters:
// - ctx: the context.Context object used for cancellation and timeouts.
// - client: a pointer to a kgo.Client object representing the Kafka client.
// - records: a slice of pointers to kgo.Record objects representing the records to be produced.
//
// It returns an error if any error occurred while producing the messages.
func produceMessages(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
	var errPromise kgo.FirstErrPromise

	for _, record := range records {
		client.Produce(ctx, record, errPromise.Promise())
	}

	return errPromise.Err()
}

// rollbackTransaction is a function that rolls back a transaction.
//
// It takes a *kgo.Client as a parameter.
// It returns an error.
func rollbackTransaction(client *kgo.Client) error {
	ctx := context.Background()

	if err := client.AbortBufferedRecords(ctx); err != nil {
		return err
	}

	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// randomString generates a random string of length n.
//
// It takes an integer parameter n, which specifies the length of the generated string.
// The function returns a string.
func randomString(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)[:n]
}
