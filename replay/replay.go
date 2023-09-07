package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/patrickmn/go-cache"
	"golang.org/x/exp/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	sr "github.com/landoop/schema-registry"
	avro "github.com/linkedin/goavro/v2"
)

var (
	logger      *slog.Logger
	logLevel    string
	fileName    = flag.String("filename", "", "filename")
	hostname, _ = os.Hostname()
	// Create a cache with a default expiration time of 5 minutes, and which
	// purges expired items every 10 minutes
	schemaCache = cache.New(2*time.Hour, 10*time.Minute)
	schemaID    string
	schemaIdInt int
	seedEnv     string
	topic       string
)

func init() {
	var (
		set bool
		err error
	)

	flag.Parse()

	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		slog.Debug("No .env file found")
	}
	logLevel, set = os.LookupEnv("LOG_LEVEL")
	if !set {
		logLevel = "INFO"
	}

	logger = initLog()

	topic, set = os.LookupEnv("TOPIC")
	if !set {
		die("", "TOPIC environment variable not set")
	}

	schemaID, set = os.LookupEnv("SCHEMA_ID")
	if !set {
		die("", "SCHEMA_ID environment variable not set")
	}
	schemaIdInt, err = strconv.Atoi(schemaID)
	maybeDie(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", schemaID))

	seedEnv, set = os.LookupEnv("KAFKA_SEEDS")
	if !set {
		die("", "KAFKA_SEEDS environment variable not set")
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

func getSchema(id string, idInt int) *avro.Codec {
	const schemaURL = "http://localhost:18081"
	schema, found := schemaCache.Get(id)
	if found {
		codec, err := avro.NewCodec(schema.(string))
		maybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))
		return codec
	}

	registry, err := sr.NewClient(schemaURL)
	maybeDie(err, fmt.Sprintf("Cannot connect to Schema Registry: %+v", err))
	maybeDie(err, fmt.Sprintf("SCHEMA_ID not an integer: %s", id))
	remoteSchema, err := registry.GetSchemaByID(idInt)
	maybeDie(err, fmt.Sprintf("Unable to retrieve schema for ID: %s", id))
	logger.Debug(fmt.Sprintf("Schema: %s", remoteSchema))

	schemaCache.Set(id, remoteSchema, cache.DefaultExpiration)
	codec, err := avro.NewCodec(remoteSchema)
	maybeDie(err, fmt.Sprintf("Error creating Avro codec: %+v", err))

	return codec
}

func main() {
	codec := getSchema(schemaID, schemaIdInt)

	schemaIdBytes := seqToBuffer(schemaIdInt)
	logger.Info(fmt.Sprintf("Schema ID: %s", schemaIdBytes))
	// Read data file
	dataFile, err := os.ReadFile(*fileName)
	maybeDie(err, fmt.Sprintf("Error reading file: %+v", err))

	// Create OCF reader
	ocfReader, err := avro.NewOCFReader(bytes.NewReader(dataFile))
	maybeDie(err, fmt.Sprintf("Error creating OCF reader: %+v", err))

	// Read and process records
	var avroRecords []*kgo.Record
	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		if err != nil {
			logger.Error(fmt.Sprintf("Error reading OCF file: %+v", err))
		}

		nestedMap, ok := record.(map[string]interface{})
		if !ok {
			panic("Unable to get nested map")
		}

		messageKey, ok := getMessageKey(nestedMap)
		if !ok {
			logger.Error("Unable to get message key")
			panic("Unable to get message key")
		}
		logger.Info(fmt.Sprintf("Message Key: %s", messageKey))
		encoded, err := codec.BinaryFromNative(nil, record)
		maybeDie(err, fmt.Sprintf("Unable to encode map: %+v", err))

		encodedValue := [][]byte{schemaIdBytes, encoded}

		// Create avro record
		avroRecord := &kgo.Record{
			Key:   []byte(messageKey),
			Value: bytes.Join(encodedValue, nil),
			Topic: topic,
		}
		logger.Info(fmt.Sprintf("Avro Record: %+v", avroRecord))
		avroRecords = append(avroRecords, avroRecord)
	}

	// Submit records
	client := CreateConnectionAndSubmitRecords(context.Background())
	err = SubmitRecords(context.Background(), client, avroRecords)
	maybeDie(err, "Error submitting records")

}

func seqToBuffer(seq int) []byte {
	buf := make([]byte, 5)
	for i := len(buf) - 1; seq != 0; i-- {
		buf[i] = byte(seq & 0xff)
		seq >>= 5
	}
	logger.Info(fmt.Sprintf("Buffer: %s", buf))
	return buf
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

// CreateConnectionAndSubmitRecords creates a connection to Kafka
func CreateConnectionAndSubmitRecords(ctx context.Context) *kgo.Client {
	var (
		opts          []kgo.Opt
		transactionId = fmt.Sprintf("replay-%s", hostname)
	)
	// Set up the kgo Client, which handles all the broker communication
	// and underlies any producer/consumer actions.
	seeds := []string{seedEnv}
	opts = append(opts,
		kgo.SeedBrokers(seeds...),
		kgo.TransactionalID(transactionId),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.RecordRetries(4),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)

	client, err := kgo.NewClient(opts...)
	maybeDie(err, "could not connect to Kafka")
	logger.Debug(fmt.Sprintf("Connected to Kafka: %+v", client))
	// defer client.Close()

	return client
}

// SubmitRecords submits records to Kafka
func SubmitRecords(ctx context.Context, client *kgo.Client, kafkaRecords []*kgo.Record) error {
	// Start the transaction so that we can start buffering records.
	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("error beginning transaction: %v", err)
	}

	// Write a message for each log file to Kafka.
	if err := ProduceMessages(ctx, client, kafkaRecords); err != nil {
		if rollbackErr := RollbackTransaction(client); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	// we're running in autocommit mode by default, which will flush all the
	// buffered messages before attempting to commit the transaction.
	if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	logger.Info(fmt.Sprintf("kafka Records produced %d", len(kafkaRecords)))
	return nil
}

// ProduceMessages produces messages to Kafka
func ProduceMessages(ctx context.Context, client *kgo.Client, record []*kgo.Record) error {
	var (
		errPromise kgo.FirstErrPromise
	)

	for _, s := range record {
		client.Produce(ctx, s, errPromise.Promise())
	}
	// Wait for all the records to be flushed or for an error to be returned.
	return errPromise.Err()
}

// RollbackTransaction rolls back a transaction
func RollbackTransaction(client *kgo.Client) error {
	// Background context is used because cancelling either of these operations can result
	// in buffered messages being added to the next transaction.
	ctx := context.Background()
	// Remove any records that have not yet been flushed.
	err := client.AbortBufferedRecords(ctx)
	if err != nil {
		return err
	}
	// End the transaction itself so that flushed records will be committed.
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}
	return nil
}
