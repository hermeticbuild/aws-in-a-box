package sqs

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"

	"github.com/gofrs/uuid/v5"

	"aws-in-a-box/awserrors"
)

func register[Input any, Output any](
	logger *slog.Logger,
	registry map[string]http.HandlerFunc,
	method string,
	handler func(input Input) (*Output, *awserrors.Error),
) {
	logger = logger.With("method", method)
	registry[method] = func(w http.ResponseWriter, r *http.Request) {
		logger.Info("Handling request")

		var input Input
		err := unmarshal(r, &input)
		if err != nil {
			logger.Error("Unmarshaling input", "err", err)
			panic(fmt.Errorf("%s: %v", method, err))
		}
		logger.Debug("Parsed input", "input", input)

		output, awserr := handler(input)
		logger.Debug("Got output", "output", output, "error", awserr)

		requestId := uuid.Must(uuid.NewV4()).String()
		marshal(w, xmlResp[Output]{
			output,
			ResponseMetadata{RequestId: requestId},
		}, awserr)
	}
}

func NewHandler(logger *slog.Logger, s *SQS) func(w http.ResponseWriter, r *http.Request) bool {
	registry := make(map[string]http.HandlerFunc)
	registerHTTPHandlers(logger, registry, s)

	return func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			return false
		}

		r.ParseForm()
		action := r.Form.Get("Action")
		handler, ok := registry[action]
		if !ok {
			return false
		}
		handler(w, r)
		return true
	}
}

func unmarshal(r *http.Request, target any) error {
	v := reflect.ValueOf(target).Elem()
	ty := v.Type()

	for i := 0; i < ty.NumField(); i++ {
		field := ty.Field(i)
		fieldSingular := field.Name[:len(field.Name)-1]

		f := v.Field(i)

		switch k := field.Type.Kind(); k {
		case reflect.Int:
			v := r.FormValue(field.Name)
			if v == "" {
				continue
			}
			i, err := strconv.Atoi(r.FormValue(field.Name))
			if err != nil {
				return err
			}
			f.Set(reflect.ValueOf(i))
		case reflect.String:
			f.Set(reflect.ValueOf(r.FormValue(field.Name)))
		case reflect.Slice:
			for i := 1; ; i++ {
				v := r.FormValue(fmt.Sprintf("%s.%d", fieldSingular, i))
				if v == "" {
					break
				}
				f.Set(reflect.Append(f, reflect.ValueOf(v)))
			}
		case reflect.Map:
			// Initialize the map and then read as many elements as we can.
			f.Set(reflect.MakeMap(f.Type()))

		EntriesLoop:
			for i := 1; ; i++ {
				// TODO(zbarsky): this is pretty HAX way to control the deserialization
				switch field.Name {
				// Legacy query-protocol field names ("Attribute", "Tag") and their
				// current struct names ("Attributes", "Tags"). Modern SDKs use JSON
				// via RegisterHTTPHandlers; this path can be removed once no
				// form-urlencoded clients remain.
				case "Attribute", "Attributes", "Tag", "Tags":
					mapKey := r.FormValue(fmt.Sprintf("%s.%d.Key", fieldSingular, i))
					mapValue := r.FormValue(fmt.Sprintf("%s.%d.Value", fieldSingular, i))
					if mapKey == "" && mapValue == "" {
						break EntriesLoop
					}
					if mapKey != "" && mapValue != "" {
						f.SetMapIndex(reflect.ValueOf(mapKey), reflect.ValueOf(mapValue))
						continue EntriesLoop
					}
					return errors.New("mismatched key/value?")
				case "MessageAttributes", "MessageSystemAttributes":
					mapKey := r.FormValue(fmt.Sprintf("%s.%d.Name", fieldSingular, i))
					mapValue := extractAPIAttribute(
						fmt.Sprintf("%s.%d.Value", fieldSingular, i),
						r.FormValue)
					if mapKey == "" && mapValue.DataType == "" {
						break EntriesLoop
					}
					if mapKey != "" && mapValue.DataType != "" {
						f.SetMapIndex(reflect.ValueOf(mapKey), reflect.ValueOf(mapValue))
						continue EntriesLoop
					}
					return errors.New("mismatched key/value?")
				default:
					panic("Unknown field: " + field.Name)
				}
			}
		default:
			panic(field)
		}

	}

	return nil
}

func extractAPIAttribute(prefix string, get func(string) string) APIAttribute {
	attr := APIAttribute{
		BinaryValue: []byte(get(prefix + ".BinaryValue")),
		StringValue: get(prefix + ".StringValue"),
		DataType:    get(prefix + ".DataType"),
	}

	for i := 1; ; i++ {
		v := get(fmt.Sprintf("%s.BinaryListValue.%d", prefix, i))
		if v == "" {
			break
		}
		attr.BinaryListValues = append(attr.BinaryListValues, []byte(v))
	}

	for i := 1; ; i++ {
		v := get(fmt.Sprintf("%s.StringListValue.%d", prefix, i))
		if v == "" {
			break
		}
		attr.StringListValues = append(attr.StringListValues, v)
	}

	return attr
}

type xmlResp[T any] struct {
	T                *T
	ResponseMetadata ResponseMetadata
}

type ResponseMetadata struct {
	RequestId string
}

func marshal(w http.ResponseWriter, output any, awserr *awserrors.Error) {
	if awserr != nil {
		w.WriteHeader(awserr.Code)
		w.Write([]byte(awserr.Body.Message))
	} else {
		err := xml.NewEncoder(w).Encode(output)
		if err != nil {
			panic(err)
		}
	}
}
