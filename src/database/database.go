package database

import (
	"context"
	"errors"
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

	errorCount := int(jsonInterface["errorCount"].(float64))
	clickCount := int(jsonInterface["clickCount"].(float64))
	pageCount := int(jsonInterface["pageCount"].(float64))

	searchQuery := bson.D{
		primitive.E{Key: "sid", Value: sid},
	}

	updateSet := bson.D{
		primitive.E{Key: "ip", Value: ip},
		primitive.E{Key: "endTime", Value: endTime},
		primitive.E{Key: "errorCount", Value: errorCount},
		primitive.E{Key: "clickCount", Value: clickCount},
		primitive.E{Key: "pageCount", Value: pageCount},
	}

	if initial {
		updateSet = append(updateSet, primitive.E{Key: "startTime", Value: startTime})
	}

	updateData := bson.D{
		primitive.E{Key: "$set",
			Value: updateSet,
		},
	}

	fmt.Println(searchQuery)
	fmt.Println(updateData)

	_, err := c.database.Collection(collectionName).UpdateOne(c.ctx, searchQuery, updateData)
	fmt.Println(err, "*****")
	return err
}

//UpdateSessionUserInfo :-  database insert
func (c *Config) UpdateSessionUserInfo(collectionName string, jsonInterface map[string]interface{}) error {
	//TODO: - add sid and aid in search query connectinusing $and
	if sid := jsonInterface["sid"]; sid != nil {

		searchQuery := bson.D{primitive.E{Key: "sid", Value: sid}}
		updateSet := bson.D{}

		if username := jsonInterface["username"]; username != nil {
			updateSet = append(updateSet, primitive.E{Key: "username", Value: username})
		}
		if id := jsonInterface["uuid"]; id != nil {
			updateSet = append(updateSet, primitive.E{Key: "id", Value: id})
		}
		if sex := jsonInterface["sex"]; sex != nil {
			updateSet = append(updateSet, primitive.E{Key: "sex", Value: sex})
		}
		if age := jsonInterface["age"]; age != nil {
			updateSet = append(updateSet, primitive.E{Key: "age", Value: age})
		}
		if email := jsonInterface["email"]; email != nil {
			updateSet = append(updateSet, primitive.E{Key: "email", Value: email})
		}
		if extra := jsonInterface["extra"]; extra != nil {
			updateSet = append(updateSet, primitive.E{Key: "extra", Value: extra})
		}
		// updateSet = append(updateSet, primitive.E{Key: "$addToSet", Value: primitive.E{Key: "tags", Value: "tag"}})
		// if tag, err := jsonInterface["tag"].(string); err {
		// updateSet = append(updateSet,bson.M{
		// 	"$addToSet": bson.M{
		// 		"tags": tag,
		// 	},
		// })
		// }

		// if url, err := jsonInterface["url"].(string); err {
		// 	updateSet = append(updateSet, bson.M{
		// 		"$addToSet": bson.M{
		// 			"urls": bson.M{"$each": []string{url}},
		// 		},
		// 	})
		// }

		fmt.Println("query", updateSet, jsonInterface)
		//updateData := bson.A{"$set", updateSet}

		updateData := bson.M{
			"$addToSet": bson.M{
				"tags": bson.M{"$each": []string{"camera", "electronics", "accessories"}},
			},
		}
		// bson.D{
		// 	primitive.E{Key: "$set",
		// 		Value: updateSet,
		// 	},
		// }
		r, err := c.database.Collection(collectionName).UpdateOne(c.ctx, searchQuery, updateData)
		fmt.Println(r, "------------------")
		fmt.Println(err, "*****")
		return err
	}

	return errors.New("sid missing")
}
