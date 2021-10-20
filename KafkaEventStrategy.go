// KafkaEventStrategy.go

// Package kafkastrategy implements the Data event strategy making use of kafka.
package kafkaeventstrategy

import (
	"fmt"
	
	"vie.git.bwinparty.com/golang/dcexchange/event"
	"vie.git.bwinparty.com/golang/messaging/kafka/client"
	"vie.git.bwinparty.com/golang/util/instance"
	"vie.git.bwinparty.com/golang/util/json/json"
	"vie.git.bwinparty.com/golang/util/logging/log"
)

// Options represents the Kafka event strategy options
type Options struct {
	Kafka                *kfc.Options `json:"kafka"`
	EventProcessorCount  int          `json:"eventProcessorCount"`
	EventTopic           string       `json:"eventTopic"`
	EventBufferSize      int          `json:"eventBufferSize"`
	PartitionConsistency bool         `json:"partitionConsistency"`
}

// kafkaEventStrategy represents the kafka event strategy instance.
type kafkaEventStrategy struct {
	options   *Options
	kc        *kfc.Client
	publisher kfc.Publisher
	events    chan *event.Event
}

// NewKafkaEventStrategy creates and instance of the kafkaEventStrategy
func NewKafkaEventStrategy(options *Options) *kafkaEventStrategy {
	hostname := instance.Current.Hostname()

	kc, _ := kfc.NewClient(hostname+"-dce-producer", kfc.WithOptions(options.Kafka))

	return &kafkaEventStrategy{
		options: options,
		kc:      kc,
		events:  make(chan *event.Event, 1024),
	}
}

// Init initiates the kafka publisher and the event processor that processes the
// Data collector events.
func (kes *kafkaEventStrategy) Init() {
	log.WithFields(&log.Fields{
		"topic":      kes.options.EventTopic,
		"bufferSize": kes.options.EventBufferSize,
	}).Info("Create DCE Producer")

	kes.publisher, _ = kes.kc.NewPublisher(kes.options.EventTopic, kfc.WithBufferSize(kes.options.EventBufferSize))

	go kes.eventProcessor()
}

// Events returns the DataEvent channel into which the events are pushed by
// the collector frontend
func (kes *kafkaEventStrategy) Events() chan *event.Event {
	return kes.events
}

// eventProcessor processes the events pushed from the collector frontend.
func (kes *kafkaEventStrategy) eventProcessor() {
	for event := range kes.events {

		//jsonBytes, jsonErr := json.Marshal(event.Data)
		fmt.Println(event.Data)
asdasdadasdasd
		var key []byte = nil
		if kes.options.PartitionConsistency {
		fmt.Println("IN KES>OPTIONS IF")
			key = []byte(event.Key)
			jsonBytes, jsonErr = json.Marshal(event)
		}

		if jsonErr != nil {
			log.WithFields(&log.Fields{
				"error":   jsonErr,
				"useCase": "EVENT_PUBLISH",
			}).Error("")
		} else {

			if kes.options.PartitionConsistency {
				kes.publisher.Messages() <- kfc.NewMsgWithKey([]byte(key), []byte(event.Data))
				log.WithFields(&log.Fields{
					"key":     key,
					"event":   string(jsonBytes),
					"useCase": "EVENT_PUBLISHED",
				}).Info("")
			} else {
				kes.publisher.Messages() <- kfc.NewMsg(jsonBytes)

				log.WithFields(&log.Fields{
					"event":   string(jsonBytes),
					"useCase": "EVENT_PUBLISHED",
				}).Info("")
			}

		}
	}
}
