# test-consumer-sqs-go

This consumer reads the messages from SQS and dumps it in DynamoDB

### Install and run localstack on docker
docker run -it -d -p 4566:4566 -p 4510-4559:4510-4559 --name aws_localstack localstack/localstack

### Commands that would come in handy for SNS/SQS

#### Create SQS queue locally
aws --endpoint-url=http://localhost:4566 sqs create-queue --region=us-west-2 --queue-name first-proj

#### List SQS queue
aws --endpoint-url=http://localhost:4566 sqs list-queues --region=us-west-2

#### Read queue messages
aws --endpoint-url=http://localhost:4566 sqs receive-message --region=us-west-2 --queue-url http://localhost:4566/000000000000/first-proj

#### Create a SNS topic
aws --endpoint-url=http://localhost:4566 sns create-topic --region=us-west-2 --name first-proj-sns

#### List subscriptions
aws --endpoint-url=http://localhost:4566 sns list-subscriptions --region=us-west-2

#### Subscribe to SNS topic
aws --endpoint-url=http://localhost:4566 sns subscribe --region=us-west-2 --topic-arn arn:aws:sns:us-west-2:000000000000:first-proj-sns --protocol sqs --notification-endpoint http://localhost:4566/000000000000/first-proj

#### Read the message
aws --endpoint-url=http://localhost:4566 sqs receive-message --region=us-west-2 --queue-url http://localhost:4566/000000000000/first-proj
