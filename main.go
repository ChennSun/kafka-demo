package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("set up")
	return nil
}
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("clean up")
	return nil
}
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d value：%q\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

type myPartition struct{}

func (p *myPartition) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	return 0, nil
}

func (p *myPartition) RequiresConsistency() bool {
	return false
}

func main() {
	done := make(chan int)
	go func() {
		consume()
		done <- 1
	}()
	res, err := product("中国人不骗中国人")
	_, _ = product("222222")
	_, _ = product("wowo")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
	<-done
}

func product(msg string) (bool, error) {
	cfg := sarama.NewConfig()

	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		//return new (myPartition)
		return &myPartition{}
	}
	producer, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, cfg)
	if err != nil {
		return false, err
	}
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		for p := range producer.Successes() {
			fmt.Println(p)
		}
		w.Done()
	}()
	w.Add(1)
	go func() {
		for p := range producer.Errors() {
			fmt.Println(p)
		}
		w.Done()
	}()
	producerMessage := &sarama.ProducerMessage{}
	producerMessage.Topic = "kafka_test_2"
	producerMessage.Value = sarama.StringEncoder(msg)
	producer.Input() <- producerMessage
	producer.AsyncClose()
	w.Wait()
	return true, nil
}

func consume() {
	// 独立消费者
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, sarama.NewConfig())
	if err != nil {
		fmt.Println(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		tmp := int32(i)
		wg.Add(1)
		go func() {
			partition, err := consumer.ConsumePartition("kafka_test_2", tmp, sarama.OffsetNewest)
			if err != nil {
				return
			}
			defer func() { _ = consumer.Close() }()
			for msg := range partition.Messages() {
				fmt.Printf("Message topic:%q partition:%d offset:%d value：%q\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// 消费者组
	//g , _ := sarama.NewConsumerGroup([]string{"127.0.0.1:9092"}, "group_1", sarama.NewConfig())
	//defer func() { _ = g.Close() }()
	//go func() {
	//	for err := range g.Errors() {
	//		fmt.Println("ERROR", err)
	//	}
	//}()
	//ctx := context.Background()
	//for {
	//	topics := []string{"kafka_test_2"}
	//	handler := exampleConsumerGroupHandler{}
	//	err := g.Consume(ctx, topics, handler)
	//	if err != nil {
	//		panic(err)
	//	}
	//}

}
