package itest

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
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

func createFifoQueue(t *testing.T, ctx context.Context, client *sqs.Client, name string) string {
	t.Helper()
	resp, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return *resp.QueueUrl
}

// createFifoQueueNoDedup creates a FIFO queue without content-based dedup, so
// the caller must supply an explicit MessageDeduplicationId.
func createFifoQueueNoDedup(t *testing.T, ctx context.Context, client *sqs.Client, name string) string {
	t.Helper()
	resp, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return *resp.QueueUrl
}

func TestFifoQueue_CreateWithSuffix(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	queueUrl := createFifoQueue(t, ctx, client, "test-queue.fifo")

	listResp, err := client.ListQueues(ctx, &sqs.ListQueuesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.QueueUrls) != 1 {
		t.Fatalf("Expected 1 queue, got %d", len(listResp.QueueUrls))
	}
	if listResp.QueueUrls[0] != queueUrl {
		t.Fatalf("Queue URL mismatch: %s != %s", listResp.QueueUrls[0], queueUrl)
	}
}

// isErrorCode reports whether err is an AWS API error with the given code and,
// if msgSubstr is non-empty, a message containing it (to disambiguate
// same-code errors).
func isErrorCode(err error, code, msgSubstr string) bool {
	var apiErr interface {
		ErrorCode() string
		ErrorMessage() string
	}
	return errors.As(err, &apiErr) &&
		apiErr.ErrorCode() == code &&
		strings.Contains(apiErr.ErrorMessage(), msgSubstr)
}

// TestFifoQueue_Validation covers FIFO input-validation error paths.
func TestFifoQueue_Validation(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	t.Run("RequiresMessageGroupId", func(t *testing.T) {
		queueUrl := createFifoQueue(t, ctx, client, "needs-group-id.fifo")
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueUrl),
			MessageBody: aws.String("body"),
		})
		if !isErrorCode(err, "MissingParameter", "MessageGroupId") {
			t.Fatalf("Expected MissingParameter error naming MessageGroupId, got %v", err)
		}
	})

	t.Run("RequiresDeduplicationId", func(t *testing.T) {
		queueUrl := createFifoQueueNoDedup(t, ctx, client, "needs-dedup-id.fifo")
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:       aws.String(queueUrl),
			MessageBody:    aws.String("body"),
			MessageGroupId: aws.String("group1"),
		})
		if !isErrorCode(err, "MissingParameter", "MessageDeduplicationId") {
			t.Fatalf("Expected MissingParameter error naming MessageDeduplicationId, got %v", err)
		}
	})

	t.Run("RejectsFifoAttributeWithoutSuffix", func(t *testing.T) {
		_, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String("not-fifo-named"),
			Attributes: map[string]string{
				"FifoQueue": "true",
			},
		})
		if !isErrorCode(err, "ValidationException", "") {
			t.Fatalf("Expected ValidationException for FifoQueue=true without .fifo suffix, got %v", err)
		}
	})
}

// TestFifoQueue_OrderingAndBlocking checks in-order delivery within a group and
// that the group is blocked while one of its messages is in flight.
func TestFifoQueue_OrderingAndBlocking(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	queueUrl := createFifoQueue(t, ctx, client, "ordered.fifo")

	bodies := []string{"first", "second", "third"}
	for _, body := range bodies {
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:       aws.String(queueUrl),
			MessageBody:    aws.String(body),
			MessageGroupId: aws.String("group1"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, expectedBody := range bodies {
		// The group yields only its next in-order message per receive.
		receiveResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueUrl),
			MaxNumberOfMessages: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(receiveResp.Messages) != 1 {
			t.Fatalf("Expected 1 in-order message, got %d", len(receiveResp.Messages))
		}
		if *receiveResp.Messages[0].Body != expectedBody {
			t.Fatalf("Expected %q, got %q", expectedBody, *receiveResp.Messages[0].Body)
		}

		// Blocked until deleted: a second receive yields nothing.
		blockedResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueUrl),
			MaxNumberOfMessages: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(blockedResp.Messages) != 0 {
			t.Fatalf("Expected 0 messages while group is in flight, got %d", len(blockedResp.Messages))
		}

		_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueUrl),
			ReceiptHandle: receiveResp.Messages[0].ReceiptHandle,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestFifoQueue_Deduplication covers content-based and explicit dedup modes.
func TestFifoQueue_Deduplication(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	t.Run("ContentBased", func(t *testing.T) {
		queueUrl := createFifoQueue(t, ctx, client, "content-dedup.fifo")
		for i := 0; i < 3; i++ {
			_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:       aws.String(queueUrl),
				MessageBody:    aws.String("duplicate body"),
				MessageGroupId: aws.String("group1"),
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		receiveResp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueUrl),
			MaxNumberOfMessages: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(receiveResp.Messages) != 1 {
			t.Fatalf("Expected 1 message after deduplication, got %d", len(receiveResp.Messages))
		}
	})

	t.Run("ExplicitId", func(t *testing.T) {
		queueUrl := createFifoQueueNoDedup(t, ctx, client, "explicit-dedup.fifo")

		// Same dedup ID, different body: deduplicated, returns the first ID.
		first, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(queueUrl),
			MessageBody:            aws.String("body-a"),
			MessageGroupId:         aws.String("group1"),
			MessageDeduplicationId: aws.String("dedup-1"),
		})
		if err != nil {
			t.Fatal(err)
		}
		second, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(queueUrl),
			MessageBody:            aws.String("body-b"),
			MessageGroupId:         aws.String("group1"),
			MessageDeduplicationId: aws.String("dedup-1"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(first.MessageId) == "" {
			t.Fatal("Expected a MessageId on the first send")
		}
		if aws.ToString(second.MessageId) != aws.ToString(first.MessageId) {
			t.Fatalf("Expected dedup hit to return original MessageId %q, got %q",
				aws.ToString(first.MessageId), aws.ToString(second.MessageId))
		}

		// A different dedup ID is a distinct message.
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(queueUrl),
			MessageBody:            aws.String("body-c"),
			MessageGroupId:         aws.String("group1"),
			MessageDeduplicationId: aws.String("dedup-2"),
		})
		if err != nil {
			t.Fatal(err)
		}

		// Drain the queue, deleting as we go (one message per group per receive).
		var bodies []string
		for {
			resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueUrl),
				MaxNumberOfMessages: 10,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Messages) == 0 {
				break
			}
			for _, m := range resp.Messages {
				bodies = append(bodies, *m.Body)
				_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(queueUrl),
					ReceiptHandle: m.ReceiptHandle,
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		if len(bodies) != 2 || bodies[0] != "body-a" || bodies[1] != "body-c" {
			t.Fatalf("Expected [body-a body-c], got %v", bodies)
		}
	})
}

func TestFifoQueue_MessageGroupIsolation(t *testing.T) {
	ctx := context.Background()
	client, srv := makeClientServerPair()
	defer srv.Shutdown(ctx)

	queueUrl := createFifoQueue(t, ctx, client, "groups.fifo")

	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueUrl),
		MessageBody:    aws.String("group-a msg1"),
		MessageGroupId: aws.String("group-a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueUrl),
		MessageBody:    aws.String("group-b msg1"),
		MessageGroupId: aws.String("group-b"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Receive first message (from group-a, since it was sent first).
	resp1, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp1.Messages) != 1 {
		t.Fatal("Expected 1 message")
	}

	// Without deleting group-a's message, we should still be able to receive
	// group-b's message since groups are independent.
	resp2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.Messages) != 1 {
		t.Fatal("Expected 1 message from another group")
	}
	if *resp1.Messages[0].Body == *resp2.Messages[0].Body {
		t.Fatal("Both receives returned the same message; groups are not isolated")
	}
}
