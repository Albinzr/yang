package database

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Config for database connection
type Config struct {
	URL          string
	DatabaseName string

	client   *mongo.Client
	database *mongo.Database
	ctx      context.Context
}

// Tests :- afsdfdfdf
func (c *Config) Tests() error {

	m := make(map[string]interface{})
	m["kid"] = 1
	c.database.Collection("test").InsertOne(c.ctx, m)

	// updateSet := bson.M{
	// 	"$push": bson.M{
	// 		"url": "url://",
	// 	},
	// 	"$set": bson.M{
	// 		"name": "2342344",
	// 	},
	// 	"$setOnInsert": bson.M{
	// 		"rol": "2342344",
	// 	},
	// }

	updateSet := bson.M{
		"$push": bson.M{
			"urls": "https://www.premagic.com/1",
		},
		"$set": bson.M{
			"entryUrl": "https://www.premagic.com/3",
			"exitUrl":  "https://www.premagic.com/2",
		},
	}
	r, err := c.database.Collection("test").UpdateOne(c.ctx, bson.M{"kid": 1}, updateSet)

	fmt.Println(r, err, "*****tests..........", updateSet)

	return err
}

//Init :- initalize function
func (c *Config) Init() error {
	var err error
	c.client, err = mongo.NewClient(options.Client().ApplyURI(c.URL))
	LogError("databaseClientError", err)
	c.database = c.client.Database(c.DatabaseName)

	c.ctx, _ = context.WithCancel(context.Background())
	err = c.client.Connect(c.ctx)
	LogError("databaseConnectionError", err)

	return err
}

//Insert :-  database insert
func (c *Config) Insert(collectionName string, jsonInterface map[string]interface{}) error {
	_, err := c.database.Collection(collectionName).InsertOne(c.ctx, jsonInterface)
	return err
}

//LogError simple error log
func LogError(message string, errorData error) {
	if errorData != nil {
		fmt.Println(message+"-Error", "............", errorData)
		return
	}
}

//UpdateSession :-  database insert
func (c *Config) UpdateSession(collectionName string, jsonInterface map[string]interface{}) error {
	//TODO: - add sid and aid in search query connect using $and
	sid := jsonInterface["sid"]
	ip := jsonInterface["ip"]
	startTime := int64(jsonInterface["startTime"].(float64))
	endTime := int64(jsonInterface["endTime"].(float64))
	initial := jsonInterface["initial"].(bool)

	// errorCount := int(jsonInterface["errorCount"].(float64))
	// clickCount := int(jsonInterface["clickCount"].(float64))
	// pageCount := int(jsonInterface["pageCount"].(float64))

	searchQuery := bson.D{
		primitive.E{Key: "sid", Value: sid},
	}

	updateSet := bson.D{
		primitive.E{Key: "ip", Value: ip},
		primitive.E{Key: "endTime", Value: endTime},
	}

	// primitive.E{Key: "errorCount", Value: errorCount},
	// primitive.E{Key: "clickCount", Value: clickCount},
	// primitive.E{Key: "pageCount", Value: pageCount},

	if initial {
		updateSet = append(updateSet, primitive.E{Key: "startTime", Value: startTime})
	}

	updateData := bson.D{
		primitive.E{Key: "$set",
			Value: updateSet,
		},
	}
	fmt.Println(searchQuery, updateData, "******************************")
	_, err := c.database.Collection(collectionName).UpdateOne(c.ctx, searchQuery, updateData)

	if err != nil {
		fmt.Println("cannot insert to db with search query:", searchQuery, "options", updateData)
	}
	return err
}
