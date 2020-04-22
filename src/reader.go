package reader

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	db "applytics.in/yang/src/database"
	util "applytics.in/yang/src/helpers"

	kafka "github.com/Albinzr/kafkaGo"
)

var env = util.LoadEnvConfig()

var kafkaConfig = &kafka.Config{
	Topic:     env.KafkaTopic,
	Partition: env.Partition,
	URL:       env.KafkaURL,
	GroupID:   env.GroupID,
	MinBytes:  env.MinBytes,
	MaxBytes:  env.MaxBytes,
}

var dbConfig = &db.Config{
	URL:          env.MongoURL,
	DatabaseName: env.DatabaseName,
}

//Start :- start reader
func Start() {
	fmt.Printf("%+v\n", kafkaConfig)
	fmt.Printf("%+v\n", dbConfig)

	readFromKafkaUpdateDB()
}

func readFromKafkaUpdateDB() {
	util.LogInfo("trying to connect to mongo DB")
	err := dbConfig.Init()
	util.LogInfo("connection call made")
	if err != nil {
		util.LogError("Database connection issue", err)
		time.AfterFunc(30*time.Second, readFromKafkaUpdateDB)
		return
	}

	util.LogInfo("mongo DB connected")
	readFromKafka()
}

func readFromKafka() {
	//Start kafka
	err := startKafka()
	if err != nil {
		util.LogError("Kafka connection issue", err)
		//if error try after (T) sec
		time.AfterFunc(30*time.Second, readFromKafka)
	}
	util.LogInfo("Starting reading message from kafka")

	kafkaConfig.Reader(kafkaReaderCallback)
}

func kafkaReaderCallback(reader kafka.Reader, message kafka.Message) {
	msgBytes := message.Value
	var jsonInterface map[string]interface{}
	json.Unmarshal([]byte(msgBytes), &jsonInterface)
	delete(jsonInterface, "_id")
	go func() {
		var err error
		if jsonInterface["type"] == "session" {
			err = dbConfig.Insert("sessionp", jsonInterface)
		} else if jsonInterface["type"] == "event" {
			err = dbConfig.Insert("subSessionx", jsonInterface)
		}
		if err == nil {
			kafka.Commit(reader, message)
			util.LogInfo("*C")
		} else {
			fmt.Println("err-------->", err)
		}
	}()
}

func startKafka() error {
	if kafkaConfig.IsKafkaReady() {
		util.LogInfo("Connected to kafka")
		return nil
	}
	err := errors.New("Cannot connect to kafka")
	return err
}