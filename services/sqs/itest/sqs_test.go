package itest

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-in-a-box/arn"
	"aws-in-a-box/server"
	sqsImpl "aws-in-a-box/services/sqs"
)

func makeClientServerPair() (*sqs.Client, *http.Server) {
	return makeClientServerPairWithInitialQueues(nil)
}

func makeClientServerPairWithInitialQueues(initialQueues []string) (*sqs.Client, *http.Server) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	impl := sqsImpl.New(sqsImpl.Options{
		ArnGenerator: arn.Generator{
			AwsAccountId: "123456789012",
			Region:       "us-east-1",
		},
	})
	if err != nil {
		panic(err)
	}
	for _, name := range initialQueues {
		impl.CreateQueue(sqsImpl.CreateQueueInput{
			QueueName: name,
		})
	}
	methodRegistry := make(map[string]http.HandlerFunc)
	impl.RegisterHTTPHandlers(slog.Default(), methodRegistry)

	srv := server.NewWithHandlerChain(
		server.HandlerFuncFromRegistry(slog.Default(), methodRegistry),
		sqsImpl.NewHandler(slog.Default(), impl),
	)
	go srv.Serve(listener)

	client := sqs.New(sqs.Options{
		BaseEndpoint: aws.String("http://" + listener.Addr().String()),
		Retryer:      aws.NopRetryer{},
	})

	return client, srv
}

func TestSendReceiveMessage_RoundtripAttributes(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	resp, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("queue"),
	})
	if err != nil {
		t.Fatal(err)
	}

	messageAttributes := map[string]types.MessageAttributeValue{
		"string": {
			DataType:    aws.String("String"),
			StringValue: aws.String("s"),
		},
		"stringList": {
			DataType:         aws.String("String"),
			StringListValues: []string{"s1", "s2"},
		},
		"binary": {
			DataType:    aws.String("Binary"),
			BinaryValue: []byte("b"),
		},
		"binaryList": {
			DataType:         aws.String("Binary"),
			BinaryListValues: [][]byte{[]byte("b1"), []byte("b2")},
		},
	}

	body := "just a body, nothing to see here"
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:          resp.QueueUrl,
		MessageBody:       aws.String(body),
		MessageAttributes: messageAttributes,
	})
	if err != nil {
		t.Fatal(err)
	}

	receiveResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              resp.QueueUrl,
		MessageAttributeNames: []string{".*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 1 {
		t.Fatalf("Did not receive right number of messages: %d", len(receiveResp.Messages))
	}
	msg := receiveResp.Messages[0]
	if *msg.Body != body {
		t.Fatal("Didn't get back the right message")
	}
	if *messageAttributes["string"].StringValue != *msg.MessageAttributes["string"].StringValue {
		t.Fatal("string attribute did not roundtrip")
	}
	if !slices.Equal(messageAttributes["binary"].BinaryValue, msg.MessageAttributes["binary"].BinaryValue) {
		t.Fatal("binary attribute did not roundtrip")
	}
	if !slices.Equal(messageAttributes["stringList"].StringListValues, msg.MessageAttributes["stringList"].StringListValues) {
		t.Fatalf("stringList attribute did not roundtrip, got %v, want %v",
			msg.MessageAttributes["stringList"].StringListValues,
			messageAttributes["stringList"].StringListValues,
		)
	}
	if !slices.EqualFunc(messageAttributes["binaryList"].BinaryListValues, msg.MessageAttributes["binaryList"].BinaryListValues, bytes.Equal) {
		t.Fatalf("binaryList attribute did not roundtrip, got %v, want %v",
			msg.MessageAttributes["binaryList"].BinaryListValues,
			messageAttributes["binaryList"].BinaryListValues,
		)
	}
}

func TestReceiveMessageOutput_MultipleMessages(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	resp, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("queue"),
	})
	if err != nil {
		t.Fatal(err)
	}

	bodies := []string{"message-1", "message-2", "message-3"}
	for _, body := range bodies {
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    resp.QueueUrl,
			MessageBody: aws.String(body),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	receiveResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            resp.QueueUrl,
		MaxNumberOfMessages: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(receiveResp.Messages))
	}

	var receivedBodies []string
	for _, msg := range receiveResp.Messages {
		if msg.Body == nil {
			t.Fatal("Message body should not be nil")
		}
		if msg.MessageId == nil || *msg.MessageId == "" {
			t.Fatal("Message should have a MessageId")
		}
		if msg.ReceiptHandle == nil || *msg.ReceiptHandle == "" {
			t.Fatal("Message should have a ReceiptHandle")
		}
		receivedBodies = append(receivedBodies, *msg.Body)
	}

	slices.Sort(receivedBodies)
	slices.Sort(bodies)
	if !slices.Equal(receivedBodies, bodies) {
		t.Fatalf("Expected bodies %v, got %v", bodies, receivedBodies)
	}
}

func TestMessageVisibility(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	resp, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("queue"),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    resp.QueueUrl,
		MessageBody: aws.String("body"),
	})
	if err != nil {
		t.Fatal(err)
	}

	receiveResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: resp.QueueUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 1 {
		t.Fatal("Message should be visible")
	}
	receiptHandle := receiveResp.Messages[0].ReceiptHandle

	receiveResp, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: resp.QueueUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 0 {
		t.Fatal("Message should be invisible")
	}

	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:      resp.QueueUrl,
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		t.Fatal(err)
	}

	receiveResp, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: resp.QueueUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 1 {
		t.Fatal("Message should be visible again")
	}
	receiptHandle = receiveResp.Messages[0].ReceiptHandle

	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:      resp.QueueUrl,
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      resp.QueueUrl,
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		t.Fatal(err)
	}

	receiveResp, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: resp.QueueUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 0 {
		t.Fatal("Deleted message should not be returned")
	}
}

func TestInitialQueues(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPairWithInitialQueues([]string{"queue-a", "queue-b"})
	defer srv.Shutdown(ctx)

	listResp, err := client.ListQueues(ctx, &sqs.ListQueuesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.QueueUrls) != 2 {
		t.Fatalf("Expected 2 queues, got %d", len(listResp.QueueUrls))
	}

	urlResp, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("queue-a"),
	})
	if err != nil {
		t.Fatal(err)
	}

	body := "hello from pre-created queue"
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    urlResp.QueueUrl,
		MessageBody: aws.String(body),
	})
	if err != nil {
		t.Fatal(err)
	}

	receiveResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: urlResp.QueueUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receiveResp.Messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(receiveResp.Messages))
	}
	if *receiveResp.Messages[0].Body != body {
		t.Fatalf("Expected body %q, got %q", body, *receiveResp.Messages[0].Body)
	}
}
