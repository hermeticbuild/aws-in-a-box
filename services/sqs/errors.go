package sqs

import "aws-in-a-box/awserrors"

func XXXTodoException(message string) *awserrors.Error {
	return &awserrors.Error{
		Code: 400,
		Body: awserrors.ErrorBody{
			Type:    "XXXTodoException",
			Message: message,
		},
	}
}

func QueueNameExists(message string) *awserrors.Error {
	return awserrors.Generate400Exception("QueueNameExists", message)
}

func QueueDoesNotExist(message string) *awserrors.Error {
	return awserrors.Generate400Exception("QueueDoesNotExist", message)
}

func ValidationException(message string) *awserrors.Error {
	return awserrors.Generate400Exception("ValidationException", message)
}

func EmptyBatchRequest(message string) *awserrors.Error {
	return awserrors.Generate400Exception("EmptyBatchRequest", message)
}

func TooManyEntriesInBatchRequest(message string) *awserrors.Error {
	return awserrors.Generate400Exception("TooManyEntriesInBatchRequest", message)
}

func InvalidIdFormat(message string) *awserrors.Error {
	return awserrors.Generate400Exception("InvalidIdFormat", message)
}

func ReceiptHandleIsInvalid(message string) *awserrors.Error {
	return awserrors.Generate400Exception("ReceiptHandleIsInvalid", message)
}

func MissingParameter(message string) *awserrors.Error {
	return awserrors.Generate400Exception("MissingParameter", message)
}
