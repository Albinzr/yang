package database

import (
	"context"
	"errors"
	"fmt"
	"log"

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

	_, err := c.database.Collection(collectionName).UpdateOne(c.ctx, searchQuery, updateData)

	if err != nil {
		fmt.Println("cannot insert to db with search query:", searchQuery, "options", updateData)
	}
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

		updateData := bson.D{
			primitive.E{Key: "$set",
				Value: updateSet,
			},
		}
		_, err := c.database.Collection(collectionName).UpdateOne(c.ctx, searchQuery, updateData)

		return err
	}

	return errors.New("sid missing")
}

//UpdateSessionArrays :-  database insert
func (c *Config) UpdateSessionArrays(collectionName string, jsonInterface map[string]interface{}) error {
	//TODO: - add sid and aid in search query connectinusing $and
	if sid := jsonInterface["sid"]; sid != nil {

		searchQuery := bson.D{primitive.E{Key: "sid", Value: sid}}
		updateData := bson.M{}

		if tag, err := jsonInterface["tag"].(string); err {
			// updateData["$push"] = bson.M{
			// 	"tags": tag,
			// }

			updateData["$push"] = bson.M{
				"urls": tag,
			}

			if _, err := jsonInterface["initial"].(bool); err {
				fmt.Println("inside")
				updateData["$set"] = bson.M{
					"entryUrl": tag,
					"exitUrl":  tag,
				}
			} else {
				updateData["$set"] = bson.M{
					"exitUrl": tag,
				}
			}
		}
		cursor, err := c.database.Collection(collectionName).Find(c.ctx, bson.M{"sid": sid})
		if err != nil {
			log.Fatal(err)
		}
		var episodes []bson.M
		if err = cursor.All(c.ctx, &episodes); err != nil {
			log.Fatal(err)
		}
		fmt.Println(episodes)

		if url, err := jsonInterface["url"].(string); err {

			// updateData["$push"] = bson.M{
			// 	"urls": "https://www.premagic.com/x",
			// }

			// updateData["$set"] = bson.M{
			// 	"entryUrl": "https://www.premagic.com/y",
			// 	"exitUrl":  "https://www.premagic.com/z",
			// }

			// updateData["$push"] = bson.M{
			// 	"urls": url,
			// }

			// if _, err := jsonInterface["initial"].(bool); err {
			// 	fmt.Println("inside")
			// 	updateData["$set"] = bson.M{
			// 		"entryUrl": url,
			// 		"exitUrl":  url,
			// 	}
			// } else {
			// 	updateData["$set"] = bson.M{
			// 		"exitUrl": url,
			// 	}
			// }
		}

		r, err := c.database.Collection(collectionName).UpdateMany(c.ctx, searchQuery, updateData)

		fmt.Println(r, err, "*****update_$push", searchQuery, updateData)

		return err
	}
	return errors.New("sid missing")
}
