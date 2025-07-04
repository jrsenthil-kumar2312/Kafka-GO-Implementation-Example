package kafka

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
	logrus "github.com/sirupsen/logrus"
)

type Producer struct {
	writer *kafka.Writer
	logger *logrus.Logger
}

func NewProducer(brokers []string, topic string, logger *logrus.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		ErrorLogger:  logger,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

func (p *Producer) PublishMessage(ctx context.Context, key string, value interface{}) error {
	valueBytes, err := json.Marshal(value)
	if err != nil {
		p.logger.WithError(err).Error("failed to marshal message value")
		return err
	}
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: valueBytes,
		time:  time.Now(),
	})

	if err != nil {
		p.logger.WithError(err).WithFields(
			logrus.Fields{"key": key, "topic": p.writer.Topic}).Error("failed to write message to Kafka")
		return err
	}

	p.logger.WithFields(
		logrus.Fields{"key": key, "topic": p.writer.Topic}).Info("message published to Kafka")

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func CreateTopic(brokers []string, topic string, partitions int, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokers[0])

	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", controller.Host+":"+strconv.Itoa(controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		},
	}

	return controllerConn.CreateTopics(topicConfigs...)
}
