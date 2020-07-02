package reader

import (
	"errors"
	"fmt"
	"time"

	db "applytics.in/yang/src/database"
	util "applytics.in/yang/src/helpers"
	kafka "github.com/Albinzr/kafkaGo"
	lz "github.com/Albinzr/lzGo"
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
		time.AfterFunc(5*time.Second, readFromKafka)
	}
	util.LogInfo("Starting reading message from kafka")

	kafkaConfig.Reader(kafkaReaderCallback)
}


func kafkaReaderCallback(reader kafka.Reader, message kafka.Message) {

		enMsg := string(message.Value)
		var msg string
		if enMsg[0:2] == "en"{
			message, err := lz.DecompressFromBase64("N4IgzglgJiBcIBYAOAxAsgWgG4CECmAnBgKoAMIANCAMbRwhIBOeAtgIYDmE1lIbd8Jq07cMARl5IBIAPS8IAOyh4AHnDFiqAFwCeSPPTxY8Crby0QWeMFrYsk6gKwEATAA4xHxwGZSpAGxUUGy2cADaoBZW6h5u3i7+AHQIAOyOVCwA9gCuYHgAGuoIcRk5eQCacP6+2noG8Fm5eGiZxrwKbFgQHCEQmQoAKnVwpAC+FJGW9Z6eKSkuiWLepU2FsGLFyyCNFVUuKbX69DvNrQZUHV09Fv1DR7BjEyBR07FupASJ3gfbZQVFJV+TUqsEcbgQh3qQLyLTaF063V6t2GD3Gk2i6ze/lIiRcmmh/3WqQhBJBCH8BEhxz+sPOIEuiJugxRYwAuqMgA==")
			msg = message
			if err != nil || enMsg == "" {
				fmt.Println("decomperssion failed*------------------------------------>")
			}
		}else{
			msg = enMsg[3:]
		}
		util.LogInfo(len(msg))
		//
		//var jsonInterface map[string]interface{}
		//err := json.Unmarshal([]byte(msg), &jsonInterface)
		//if err != nil {
		//	fmt.Println("JSON CONVERSION Failled....................................")
		//}
		//
		//util.LogInfo("TYPE:",jsonInterface["type"])
		//
		//if jsonInterface["type"] == "session" {
		//	err = dbConfig.Insert("record", jsonInterface)
		//} else if jsonInterface["type"] == "event" {
		//	err = dbConfig.Insert("subRecord", jsonInterface)
		//} else if jsonInterface["type"] == "close" {
		//	err = dbConfig.UpdateSession("record", jsonInterface)
		//} else {
		//	util.LogInfo("wrong data detected _______________********_______________",len(msg))
		//}
		//commitKafkaMessage(err, reader, message)

}

func commitKafkaMessage(err error, reader kafka.Reader, message kafka.Message) {
	if err == nil {
		kafka.Commit(reader, message)
	} else {
		//TODO: - if duplicare remove else set up a retry system (3 times) then delete
		fmt.Println("err-------->", err)
		kafka.Commit(reader, message)
	}
}

func startKafka() error {
	if kafkaConfig.IsKafkaReady() {
		util.LogInfo("Connected to kafka")
		return nil
	}
	err := errors.New("Cannot connect to kafka")
	return err
}
