// Example function-based high-level Apache Kafka consumer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
	"app/lib"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	//"strconv"
	"sync"
	"unsafe"
)

func init() {
	lib.InitConfig("1") //初始化配置

}
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	topic := os.Args[1]
	SetProcessName(topic)
	subTopic := "Gula-" + topic
	group := lib.GetConfig("base")["kafka_borker.groupPrefix"].String() + subTopic
	broker := lib.GetConfig("base")["kafka_borker.address"].String()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"group.id":                group,
		"session.timeout.ms":      60000,
		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 100,
		"log.connection.close":    "false",
		"api.version.request":     true,
		"debug":                   lib.GetConfig("base")["kafka_borker.debug"].String(),
		"default.topic.config":    kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)
	topics := []string{subTopic}
	err = c.SubscribeTopics(topics, nil)
	run := true
	if lib.GetConfig("base")["kafka_consumer.doGoruntine"].String() == "1" {
		wg := &sync.WaitGroup{} //并发处理
		var goruntineNum int64
		goruntineNum = 0
		for run == true {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false
			default:
				maxGoruntineNum := lib.GetConfig("base")["kafka_consumer.goruntineNum"].Int()
				if goruntineNum >= maxGoruntineNum {
					fmt.Printf("%% over-goruntine %v\n", maxGoruntineNum)
					continue
				}
				ev := c.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					//c.Commit()
					phpExe := lib.GetConfig("phpcli")["phpExe.name"].String()
					cliFile := lib.GetConfig("phpcli")["cli.file"].String()
					wg.Add(1)
					goruntineNum++
					go lib.GoExecPhp(phpExe, []string{cliFile, topic, string(e.Value)}, wg, &goruntineNum)
					fmt.Printf("%% Goruntine-num %v\n", goruntineNum)
					fmt.Printf("%% Reached-Goruntine %v\n", e.TopicPartition)
					//lib.LogWrite("result:"+res,"kafka-consumer-"+topic)
					//fmt.Printf(e.TopicPartition)
				case kafka.PartitionEOF:
					fmt.Printf("%% Reached-Goruntine %v\n", e)
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					run = false
				default:
					fmt.Printf("Ignored-Goruntine %v\n", e)
				}
			}
		}
		fmt.Printf("wait-Goruntine ...\n")
		wg.Wait()
	} else {
		for run == true {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false
			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					//c.Commit()
					phpExe := lib.GetConfig("phpcli")["phpExe.name"].String()
					cliFile := lib.GetConfig("phpcli")["cli.file"].String()
					lib.ExecPhp(phpExe, []string{cliFile, topic, string(e.Value)})
					fmt.Printf("%% Reached %v\n", e.TopicPartition)
				//lib.LogWrite("result:"+res,"kafka-consumer-"+topic)
				//fmt.Printf(e.TopicPartition)
				case kafka.PartitionEOF:
					fmt.Printf("%% Reached %v\n", e)
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					run = false
				default:
					fmt.Printf("Ignored %v\n", e)
				}
			}
		}
	}
	fmt.Printf("Closing-Goruntine consumer\n")
	c.Close()
	os.Exit(0)
}
func SetProcessName(name string) error {
	bytes := append([]byte(name), 0)
	ptr := unsafe.Pointer(&bytes[0])
	if _, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, syscall.PR_SET_NAME, uintptr(ptr), 0, 0, 0, 0); errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}
