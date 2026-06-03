package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"aws-in-a-box/arn"
	"aws-in-a-box/http"
	"aws-in-a-box/server"
	"aws-in-a-box/services/dynamodb"
	"aws-in-a-box/services/kinesis"
	"aws-in-a-box/services/kms"
	"aws-in-a-box/services/s3"
	"aws-in-a-box/services/sqs"
)

var BazelSuffix = ""

func versionString() string {
	version := Version + BazelSuffix

	// We only have ReadBuildInfo in non-bazel builds.
	if version != Version {
		buildinfo, ok := debug.ReadBuildInfo()
		if ok {
			for _, s := range buildinfo.Settings {
				if s.Key == "vcs.modified" {
					version += " (dirty)"
					break
				}
			}
		}
	}

	return version
}

func main() {
	addr := flag.String("addr", "localhost:4569", "Address to run on")
	reusePort := flag.Bool("reuse-port", false, "If true, sets the SO_REUSEPORT socket option")
	persistDir := flag.String("persistDir", "", "Directory to persist data to. If empty, data is not persisted.")
	logLevel := flag.String("logLevel", "debug", "debug/info/warn/error")

	enableKinesis := flag.Bool("enableKinesis", true, "Enable Kinesis service")
	kinesisInitialStreams := flag.String("kinesisInitialStreams", "",
		"Streams to create at startup. Example: stream1,stream2,stream3")
	kinesisInitialShardsPerStream := flag.Int64("kinesisInitialShardsPerStream", 2,
		"How many shards to create for each stream listed in -kinesisInitialStreams")
	kinesisDefaultDuration := flag.Duration("kinesisDefaultDuration", 24*time.Hour,
		"How long to retain messages. Can be used to control memory usage. After creation, retention can be adjusted with [Increase/Decrease]StreamRetentionPeriod")
	kinesisStreamCreateDuration := flag.Duration("kinesisStreamCreateDuration", 5*time.Second,
		"How long a new Kinesis stream stays in CREATING status")
	kinesisStreamDeleteDuration := flag.Duration("kinesisStreamDeleteDuration", 5*time.Second,
		"How long a deleted Kinesis stream stays in DELETING status")

	enableKMS := flag.Bool("enableKMS", true, "Enable Kinesis service")

	enableDynamoDB := flag.Bool("experimental_enableDynamoDB", true, "Enable DynamoDB service")

	enableS3 := flag.Bool("enableS3", true, "Enable S3 service")
	s3InitialBuckets := flag.String("s3InitialBuckets", "", "Buckets to create at startup. Example: bucket1,bucket2,bucket3")

	enableSQS := flag.Bool("enableSQS", true, "Enable SQS service")
	sqsInitialQueues := flag.String("sqsInitialQueues", "", "Queues to create at startup. Example: queue1,queue2,queue3")

	flag.Parse()

	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		panic("Invalid log level")
	}

	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(textHandler).With("version", versionString())

	methodRegistry := make(http.Registry)

	arnGenerator := arn.Generator{
		// TODO: make these configurable?
		AwsAccountId: "123456789012",
		Region:       "us-east-1",
	}

	if *enableKinesis {
		logger := logger.With("service", "kinesis")
		k := kinesis.New(kinesis.Options{
			Logger:               logger,
			ArnGenerator:         arnGenerator,
			DefaultRetention:     *kinesisDefaultDuration,
			StreamCreateDuration: *kinesisStreamCreateDuration,
			StreamDeleteDuration: *kinesisStreamDeleteDuration,
		})
		for _, name := range strings.Split(*kinesisInitialStreams, ",") {
			k.CreateStream(kinesis.CreateStreamInput{
				StreamName: name,
				ShardCount: *kinesisInitialShardsPerStream,
			})
		}
		k.RegisterHTTPHandlers(logger, methodRegistry)
		logger.Info("Enabled Kinesis")
	}

	if *enableKMS {
		logger := logger.With("service", "kms")
		k, err := kms.New(kms.Options{
			Logger:       logger,
			ArnGenerator: arnGenerator,
			PersistDir:   *persistDir,
		})
		if err != nil {
			log.Fatal(err)
		}
		k.RegisterHTTPHandlers(logger, methodRegistry)
		logger.Info("Enabled KMS")
	}

	if *enableDynamoDB {
		logger := logger.With("service", "dynamodb")
		d := dynamodb.New(dynamodb.Options{
			Logger:       logger,
			ArnGenerator: arnGenerator,
		})
		d.RegisterHTTPHandlers(logger, methodRegistry)
		logger.Info("Enabled DynamoDB (EXPERIMENTAL!!!)")
	}

	handlerChain := []server.HandlerFunc{server.HandlerFuncFromRegistry(logger, methodRegistry)}

	if *enableSQS {
		logger := logger.With("service", "sqs")
		s := sqs.New(sqs.Options{
			Logger:       logger,
			ArnGenerator: arnGenerator,
		})
		for _, name := range strings.Split(*sqsInitialQueues, ",") {
			if name == "" {
				continue
			}
			s.CreateQueue(sqs.CreateQueueInput{
				QueueName: name,
			})
		}
		// Register JSON handler
		s.RegisterHTTPHandlers(logger, methodRegistry)
		// Register form data handler
		handlerChain = append(handlerChain, sqs.NewHandler(logger, s))
		logger.Info("Enabled SQS")
	}

	if *enableS3 {
		logger := logger.With("service", "s3")
		s, err := s3.New(s3.Options{
			Logger:     logger,
			Addr:       *addr,
			PersistDir: *persistDir,
		})
		if err != nil {
			log.Fatal(err)
		}
		for _, name := range strings.Split(*s3InitialBuckets, ",") {
			s.CreateBucket(s3.CreateBucketInput{
				Bucket: name,
			})
		}
		handlerChain = append(handlerChain, s3.NewHandler(logger, s))
	}

	srv := server.NewWithHandlerChain(handlerChain...)

	lc := net.ListenConfig{
		Control: func(network, address string, conn syscall.RawConn) error {
			if !*reusePort {
				return nil
			}

			var setSockoptErr error
			err := conn.Control(func(fd uintptr) {
				setSockoptErr = setSockopt(fd)
			})
			if err != nil {
				return err
			}
			return setSockoptErr
		},
	}

	l, err := lc.Listen(context.Background(), "tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	err = srv.Serve(l)
	if err != nil {
		panic(err)
	}
}
