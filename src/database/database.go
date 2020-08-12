package database

import (
	"context"
	"errors"
	"fmt"
	"reflect"

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
	fmt.Println(jsonInterface, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", jsonInterface["pageCount"], reflect.TypeOf(jsonInterface["pageCount"]))
	searchQuery := bson.D{}
	updateData := bson.D{}
	//
	setQuery := bson.D{}
	pushQuery := bson.D{}
	incQuery := bson.D{}

	// TODO: - add sid and aid in search query connect using $and
	if sid, isPresent := jsonInterface["sid"]; isPresent {
		searchQuery = append(searchQuery, primitive.E{Key: "sid", Value: sid})
	} else {
		return errors.New("no sid found")
	}
	if aid, isPresent := jsonInterface["aid"]; isPresent {
		searchQuery = append(searchQuery, primitive.E{Key: "aid", Value: aid})
	} else {
		return errors.New("no sid found")
	}

	if ip, isPresent := jsonInterface["ip"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "ip", Value: ip})
	}

	if initial, isPresent := getBoolFromMap(jsonInterface, "initial"); isPresent && initial {
		if startTime, isPresent := getFloat64FromMap(jsonInterface, "startTime"); isPresent {
			setQuery = append(setQuery, primitive.E{Key: "startTime", Value: startTime})
			if endTime, isPresent := getFloat64FromMap(jsonInterface, "endTime"); isPresent {
				setQuery = append(setQuery, primitive.E{Key: "endTime", Value: endTime})
				setQuery = append(setQuery, primitive.E{Key: "duration", Value: endTime - startTime})
			}
		}
	} else {
		if startTime, isPresent := getFloat64FromMap(jsonInterface, "startTime"); isPresent {
			if endTime, isPresent := getFloat64FromMap(jsonInterface, "endTime"); isPresent {
				setQuery = append(setQuery, primitive.E{Key: "endTime", Value: endTime})
				incQuery = append(incQuery, primitive.E{Key: "duration", Value: endTime - startTime})
			}
		}
	}

	if errorCount, isPresent := getFloat64FromMap(jsonInterface, "errorCount"); isPresent {
		setQuery = append(setQuery, primitive.E{Key: "errorCount", Value: errorCount})
	}

	if pageCount, isPresent := getFloat64FromMap(jsonInterface, "pageCount"); isPresent {
		setQuery = append(setQuery, primitive.E{Key: "pageCount", Value: pageCount})
	}

	if clickCount, isPresent := getFloat64FromMap(jsonInterface, "clickCount"); isPresent {
		setQuery = append(setQuery, primitive.E{Key: "clickCount", Value: clickCount})
	}

	if tags, isPresent := jsonInterface["tags"].([]string); isPresent {
		fmt.Println(reflect.TypeOf(tags), "----------------------------------------------------", tags)
		pushQuery = append(pushQuery, primitive.E{Key: "tags", Value: primitive.E{Key: "$each", Value: tags}})
	}

	if urls, isPresent := jsonInterface["urls"].([]string); isPresent {
		fmt.Println(reflect.TypeOf(urls), "----------------------------------------------------", urls)
		pushQuery = append(pushQuery, primitive.E{Key: "urls", Value: primitive.E{Key: "$each", Value: urls}})
	}

	if username, isPresent := jsonInterface["username"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "username", Value: username})
	}

	if id, isPresent := jsonInterface["id"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "id", Value: id})
	}

	if sex, isPresent := jsonInterface["sex"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "sex", Value: sex})
	}

	if age, isPresent := getIntFromMap(jsonInterface, "age"); isPresent {
		setQuery = append(setQuery, primitive.E{Key: "age", Value: age})
	}

	if email, isPresent := jsonInterface["email"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "email", Value: email})
	}

	if entryURL, isPresent := jsonInterface["entryUrl"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "entryUrl", Value: entryURL})
	}

	if exitURL, isPresent := jsonInterface["exitUrl"]; isPresent {
		setQuery = append(setQuery, primitive.E{Key: "exitUrl", Value: exitURL})
	}

	if len(setQuery) > 0 {
		updateData = append(updateData, primitive.E{Key: "$set",
			Value: setQuery,
		})
	}
	if len(pushQuery) > 0 {
		updateData = append(updateData, primitive.E{Key: "$push",
			Value: pushQuery,
		})
	}

	if len(incQuery) > 0 {
		updateData = append(updateData, primitive.E{Key: "$inc",
			Value: incQuery,
		})
	}

	fmt.Println(searchQuery, updateData, "******************************")
	r, err := c.database.Collection(collectionName).UpdateOne(c.ctx, searchQuery, updateData)

	if err != nil {
		fmt.Println("cannot insert to db with search query:", searchQuery, "options", jsonInterface)
	}
	fmt.Println("result____________________________", r)
	return err
}

func getFloat64FromMap(items map[string]interface{}, key string) (float64, bool) {
	if initialInterface, isPresent := items[key]; isPresent {
		if initial, isPresent := initialInterface.(float64); isPresent {
			return initial, true
		}
		return 0, false
	}
	return 0, false
}

func getIntFromMap(items map[string]interface{}, key string) (int, bool) {
	if initialInterface, isPresent := items[key]; isPresent {
		if initial, isPresent := initialInterface.(int); isPresent {
			return initial, true
		}
		return 0, false
	}
	return 0, false
}

func getBoolFromMap(items map[string]interface{}, key string) (bool, bool) {
	if initialInterface, isPresent := items[key]; isPresent {
		if initial, isPresent := initialInterface.(bool); isPresent {
			return initial, true
		}
		return false, false
	}
	return false, false
}
