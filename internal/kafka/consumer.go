package kafka

import (
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	reader *kafka.Reader
	logger *logrus.Logger
}

type MessageHandler func(ctx context.Context, msg kafka.Message) error

func NewConsumer(brokers []string, topic string, groupID string, logger *logrus.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MaxWait:     10 * time.Millisecond,
		MinBytes:    1e3,  // 1KB
		MaxBytes:    10e6, // 10MB
		ErrorLogger: logger,
	})

	return &Consumer{
		reader: reader,
		logger: logger,
	}
}

func (c *Consumer) ReadMessage() (ctx context.Context, msg kafka.Message) error {
	// return c.reader.ReadMessage(context.Background())
	c.logger.WithFields(logrus.Fields{
		"topic": c.reader.Config().Topic,
		"groupID": c.reader.Config().GroupID,
	}).Info("Starting kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.WithError(ctx.Err()).Error("context cancelled while reading message")
			return ctx.Err()
		default:
			message, err := c.reader.FetchMessage(ctx)
			if err != nil {
				c.logger.WithError(err).Error("failed to fetch message")
				continue
			}

			err = MessageHandler(ctx, message)
			if err != nil {
				c.logger.WithError(err).WithFields(logrus.Fields{
					"offset":    message.Offset,
					"partition": message.Partition,
					"key":       string(message.Key),
				}).Error("Failed to process message")
				// In a production system, you might want to send to a dead letter queue
				continue
			}

			err = c.reader.CommitMessages(ctx, message)
			if err != nil {
				c.logger.WithError(err).WithFields(logrus.Fields{
					"offset":    message.Offset,
					"partition": message.Partition,
					"key":       string(message.Key),
				}).Error("Failed to commit message")
			}

			c.logger.WithFields(logrus.Fields{
				"offset":    message.Offset,
				"partition": message.Partition,
				"key":       string(message.Key),
			}).Debug("Message processed successfully")
		}
	}
}

func UnmarshalMessage(message kafka.Message, v interface{}) error {
	return json.Unmarshal(message.Value, v)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}


