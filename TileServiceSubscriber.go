package main

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/micro/go-micro/util/log"
	SchedularService "github.com/nayanmakasare/SchedularService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

type TileServiceSubscriber struct {
	MongoCollection *mongo.Collection
	RedisConnection *redis.Client
}

func MakeRedisKey(badKey string) string {
	return strings.ToLower(strings.Replace(badKey, " ", "", -1))
}




//cvte:shinko:schedule
//cvte:shinko:pagename
//cvte:shinko:pagename:carousel
//cvte:shinko:pagename:rows


func (h *TileServiceSubscriber) SubscriberSchedule(ctx context.Context, schedule *SchedularService.Schedule) error {
	log.Info("got message making Schedule for ",schedule.Vendor, schedule.Brand)
	scheduleRedisKey := MakeRedisKey(schedule.Vendor + ":" + schedule.Brand + ":" + "schedule")
	log.Info("sub 1 ")
	for _, i := range schedule.Page {
		log.Info("************************************************************")
		pageRedisKey := MakeRedisKey(schedule.Vendor + ":" + schedule.Brand + ":" + i.PageName)
		h.RedisConnection.SAdd(scheduleRedisKey, pageRedisKey)
		for _, k := range i.Carousel {
			resultByte, err := proto.Marshal(k)
			if err != nil {
				return err
			}
			pageCarouselKey := MakeRedisKey(pageRedisKey + ":" + "carousel")
			h.RedisConnection.SAdd(pageCarouselKey, resultByte)
		}
		pageRowKey := MakeRedisKey(pageRedisKey + ":" + "rows")
		for _, j := range i.Rows {
			rowRedisKey := MakeRedisKey(pageRedisKey + ":" + j.RowName )
			log.Info("rowRedisKey ", rowRedisKey)
			h.RedisConnection.SAdd(pageRowKey, rowRedisKey)
			var myPipes []bson.D



			// TODO best learning
			/*
				I was implementing pipe in aggregation where find these values from array to that property
			myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.categories", bson.D{{"$in", bson.A{j.CategoryList}}}}}}},)
			It gave me wrong result of some and correct of others

			Then naseeb se I removed bson.A{} and passed the slice directly and it gave me all right results
			*/


			if j.CategoryList != nil && len(j.CategoryList) > 0 {
				log.Info(rowRedisKey," " ,"have categories")
				myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.categories", bson.D{{"$in", j.CategoryList}}}}}},)
			}
			if j.LanguageList != nil && len(j.LanguageList) > 0 {
				log.Info(rowRedisKey," " ,"have Language")
				myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.languages", bson.D{{"$in", j.LanguageList}}}}}},)
			}
			if j.GenerList != nil && len(j.GenerList) > 0 {
				log.Info(rowRedisKey," " ,"have Genres")
				myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.genre", bson.D{{"$in", j.GenerList}}}}}},)
			}

			myPipes = append(myPipes,bson.D{{"$sort", bson.D{{"created_at", -1}}}}, )
			//myPipes = append(myPipes, bson.D{{"$limit", 400}},)
			myPipes = append(myPipes, bson.D{{"$project", bson.D{{"_id", 0}, {"ref_id", 1}}}},)

			cur, err := h.MongoCollection.Aggregate(context.Background(), myPipes, options.Aggregate().SetMaxTime(2000*time.Millisecond))
			if err != nil {
				log.Fatal(err)
			}
			counter := 0
			for cur.Next(context.TODO()) {
				counter++
				h.RedisConnection.SAdd(rowRedisKey, cur.Current.Lookup("ref_id").StringValue())
			}
			log.Info("result pipe ", rowRedisKey," " ,counter)
			err = cur.Close(context.Background())
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	return nil
}