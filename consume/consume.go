package consume

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func ConsumeConcurrent(sess *session.Session) {
	fmt.Println("Connecting and reading from queue...")
	svc := sqs.New(sess)
	fmt.Println("Connected.")

	queueURL := "http://localhost:4566/000000000000/first-proj"
	var timeout int64 = 5
	concurrency := 200
	sync := createFullBufferedChannel(concurrency)

	for { // create an infinite processing loop
		msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   aws.Int64(timeout),
		})

		if err != nil {
			// TODO: add error notification
			log.Println("Receive message error: ", err)
			continue
		}
		if len(msgResult.Messages) == 0 {
			time.Sleep(100 * time.Microsecond)
			continue
		}
		messages := msgResult.Messages
		// go func(messages []*sqs.Message) {
		for i := range messages {
			// request the exact amount of "workers" from pool.
			// Again, empty buffer will block this operation
			<-sync
			// fmt.Println(len(sync))
			go func(i int, messages []*sqs.Message) {
				// email := msgResult.Messages[i].
				// 	MessageAttributes["email"].StringValue

				// body := msgResult.Messages[i].Body

				// book := getMessage(*mess.Body)

				dynamoClient := dynamodb.New(sess)
				AddTableItem(dynamoClient, *messages[i].Body, aws.String("Books"))

				_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &queueURL,
					ReceiptHandle: messages[i].ReceiptHandle,
				})

				if err != nil {
					log.Println("Deleting message error", err)
				}

				// return "worker" to the "pool"
				sync <- true
			}(i, messages)
		}
		// }(msgResult.Messages)
	}
}

func createFullBufferedChannel(capacity int) chan bool {
	sync := make(chan bool, capacity)

	for i := 0; i < capacity; i++ {
		sync <- true
	}
	return sync
}

func getMessage(jsonReq string) *Book {
	var msg *SqsResponse
	var book *Book

	err := json.Unmarshal([]byte(jsonReq), &msg)
	if err != nil {
		fmt.Println(err.Error())
	}

	// fmt.Println(msg.Message)
	err = json.Unmarshal([]byte(msg.Message), &book)
	if err != nil {
		fmt.Println(err.Error())
	}
	book.Timestamp = time.Now()

	return book
}

// AddTableItem adds an item to an Amazon DynamoDB table
// Inputs:
//
//	sess is the current session, which provides configuration for the SDK's service clients
//	year is the year when the movie was released
//	table is the name of the table
//	title is the movie title
//	plot is a summary of the plot of the movie
//	rating is the movie rating, from 0.0 to 10.0
//
// Output:
//
//	If success, nil
//	Otherwise, an error from the call to PutItem
func AddTableItem(svc dynamodbiface.DynamoDBAPI, bookJson string, table *string) error {
	book := getMessage(bookJson)
	fmt.Println(*book)

	av, err := dynamodbattribute.MarshalMap(book)

	// snippet-end:[dynamodb.go.create_new_item.assign_struct]
	if err != nil {
		return err
	}

	// snippet-start:[dynamodb.go.create_new_item.call]
	_, err = svc.PutItem(&dynamodb.PutItemInput{
		Item:      av,
		TableName: table,
	})
	// snippet-end:[dynamodb.go.create_new_item.call]
	if err != nil {
		fmt.Println(err.Error())
	}

	return nil
}

func ConsumeNaive(sess *session.Session) {
	fmt.Println("Connecting and reading from queue...")
	svc := sqs.New(sess)
	fmt.Println("Connected.")

	queueURL := "http://localhost:4566/000000000000/first-proj"
	var timeout int64 = 5

	for {
		msgResult, _ := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   &timeout,
		})

		wg := sync.WaitGroup{}
		for i, mess := range msgResult.Messages {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// email := msgResult.Messages[i].MessageAttributes["email"].StringValue
				// body := msgResult.Messages[i].Body
				// book := getMessage(*mess.Body)

				dynamoClient := dynamodb.New(sess)
				AddTableItem(dynamoClient, *mess.Body, aws.String("Books"))

				_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &queueURL,
					ReceiptHandle: msgResult.Messages[i].ReceiptHandle,
				})

				if err != nil {
					log.Println("Deleting message error", err)
				}

			}()
		}
		if len(msgResult.Messages) == 0 { // add aditional sleep if queue is empty
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

type SqsResponse struct {
	Type             string    `json:"Type"`
	MessageID        string    `json:"MessageId"`
	TopicArn         string    `json:"TopicArn"`
	Message          string    `json:"Message"`
	Timestamp        time.Time `json:"Timestamp"`
	SignatureVersion string    `json:"SignatureVersion"`
	Signature        string    `json:"Signature"`
	SigningCertURL   string    `json:"SigningCertURL"`
	UnsubscribeURL   string    `json:"UnsubscribeURL"`
}

type Book struct {
	Uuid        string    `json:"uuid"`
	Name        string    `json:"name"`
	Author      string    `json:"author"`
	Publication string    `json:"publication"`
	Timestamp   time.Time `json:"timestamp"`
}
