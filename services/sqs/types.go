package sqs

import "encoding/xml"

type CreateQueueInput struct {
	Attributes map[string]string
	QueueName  string
	Tags       map[string]string
}

type CreateQueueOutput struct {
	XMLName  xml.Name `xml:"CreateQueueResult"`
	QueueUrl string
}

type DeleteQueueInput struct {
	QueueUrl string
}

type DeleteQueueOutput struct{}

const AWSTraceHeaderAttributeName = "AWSTraceHeader"

type SendMessageInput struct {
	DelaySeconds            int
	MessageAttributes       APIMessageAttributes
	MessageBody             string
	MessageDeduplicationId  string
	MessageGroupId          string
	MessageSystemAttributes APIMessageAttributes
	QueueUrl                string
}

type SendMessageOutput struct {
	XMLName xml.Name `xml:"SendMessageResult"`

	MD5OfMessageAttributes       string
	MD5OfMessageBody             string
	MD5OfMessageSystemAttributes string
	MessageId                    string
	SequenceNumber               string
}

type APIAttribute struct {
	BinaryListValues [][]byte `xml:"BinaryListValue"`
	BinaryValue      []byte
	DataType         string
	StringListValues []string `xml:"StringListValue"`
	StringValue      string
}

type TagQueueInput struct {
	QueueUrl string
	Tags     map[string]string
}

type TagQueueOutput struct{}

type UntagQueueInput struct {
	QueueUrl string
	TagKeys  []string
}

type UntagQueueOutput struct{}

type GetQueueUrlInput struct {
	QueueName string
}

type GetQueueUrlOutput struct {
	QueueUrl string
}

type ListQueuesInput struct {
	MaxResults      int
	QueueNamePrefix string
}

type ListQueuesOutput struct {
	QueueUrls []string `xml:"queueUrls"`
}

type GetQueueAttributesInput struct {
	attributeNames []string
	//`query:"attributeNames"`
	QueueUrl string
}

type GetQueueAttributesOutput struct {
	Attributes map[string]string `xml:"attributes"`
}

type ListQueueTagsInput struct {
	QueueUrl string
}

type ListQueueTagsOutput struct {
	Tags map[string]string
}

// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#SQS-ReceiveMessage-request-AttributeNames
type SystemAttributeName string

const (
	All                              = SystemAttributeName("All")
	ApproximateFirstReceiveTimestamp = SystemAttributeName("ApproximateFirstReceiveTimestamp")
	ApproximateReceiveCount          = SystemAttributeName("ApproximateReceiveCount")
	AWSTraceHeader                   = SystemAttributeName("AWSTraceHeader")
	SenderId                         = SystemAttributeName("SenderId")
	SentTimestamp                    = SystemAttributeName("SentTimestamp")
	// TODO: this one not listed in https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#SQS-ReceiveMessage-request-MessageSystemAttributeNames
	SqsManagedSseEnabled     = SystemAttributeName("SqsManagedSseEnabled")
	MessageDeduplicationId   = SystemAttributeName("MessageDeduplicationId")
	MessageGroupId           = SystemAttributeName("MessageGroupId")
	SequenceNumber           = SystemAttributeName("SequenceNumber")
	DeadLetterQueueSourceArn = SystemAttributeName("DeadLetterQueueSourceArn")
	// TODO: there are more
)

type ReceiveMessageInput struct {
	// Deprecated
	AttributeNames              []SystemAttributeName
	MaxNumberOfMessages         int
	MessageAttributeNames       []string
	MessageSystemAttributeNames []SystemAttributeName
	QueueUrl                    string
	// ReceiveRequestAttemptId
	VisibilityTimeout int
	WaitTimeSeconds   int
}

type ReceiveMessageOutput struct {
	XMLName xml.Name     `xml:"ReceiveMessageResult"`
	Message []APIMessage `json:"Messages"`
}

type APIAttributes map[string]string
type APIMessageAttributes map[string]APIAttribute

func (a APIMessageAttributes) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type XMLAttribute struct {
		Name  string
		Value APIAttribute
	}
	attrs := make([]XMLAttribute, 0, len(a))
	for k, v := range a {
		attrs = append(attrs, XMLAttribute{
			Name:  k,
			Value: v,
		})
	}

	return e.EncodeElement(attrs, start)
}

type APIMessage struct {
	//Attributes             APIAttributes
	Body                   string
	MD5OfBody              string
	MD5OfMessageAttributes string
	MessageAttributes      APIMessageAttributes `xml:"MessageAttribute"`
	MessageId              string
	ReceiptHandle          string
}

type DeleteMessageInput struct {
	QueueUrl      string
	ReceiptHandle string
}

type DeleteMessageOutput struct{}

type DeleteMessageBatchInput struct {
	QueueUrl string
	Entries  []struct {
		Id            string
		ReceiptHandle string
	}
}

type DeleteMessageBatchOutput struct {
	Failed     []BatchResultErrorEntry
	Successful []BatchResultSuccessEntry
}

type BatchResultErrorEntry struct {
	Code        string
	Id          string
	Message     string
	SenderFault bool
}

type BatchResultSuccessEntry struct {
	Id string
}

type SetQueueAttributesInput struct {
	QueueUrl   string
	Attributes map[string]string
}

type SetQueueAttributesOutput struct{}

type ChangeMessageVisibilityInput struct {
	QueueUrl          string
	ReceiptHandle     string
	VisibilityTimeout int
}

type ChangeMessageVisibilityOutput struct{}

type ChangeMessageVisibilityBatchInput struct {
	QueueUrl string
	Entries  []struct {
		Id                string
		ReceiptHandle     string
		VisibilityTimeout int
	}
}

type ChangeMessageVisibilityBatchOutput struct {
	Failed     []BatchResultErrorEntry
	Successful []BatchResultSuccessEntry
}
