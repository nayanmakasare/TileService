package main

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/util/log"
	NotificationService "github.com/nayanmakasare/NotificationService/proto"
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
	NotificationEventPublisher  micro.Publisher
}


//TODO schema of redis keys
//cvte:shinko:timezone eg: cvte:shinko:6-9
//cvte:shinko:timezone:pageIndes:pagename
//cvte:shinko:timezone:pageIndes:pagename:carousel
//cvte:shinko:timezone:pageIndes:pagename:rows
//cvte:shinko:timezone:pageIndes:pagename:rows:rowIndex:rowName


func (h *TileServiceSubscriber) SubscriberChangeSchedule(ctx context.Context, schedule *SchedularService.CloudwalkerScheduler) error{
	for _,i := range schedule.Shedule {
		if i.TimeZone == getTimeZone() {
			log.Info("Refreshing schedule for ", schedule.Vendor, schedule.Brand, i.TimeZone)
			redisScheduleKey := MakeRedisKey(schedule.Vendor+":"+schedule.Brand+":"+i.TimeZone)
			h.RedisConnection.Del(redisScheduleKey)
			for _,j := range i.Pages {
				redisPageKey := MakeRedisKey(redisScheduleKey+":"+String(j.PageIndex)+":"+j.PageName)
				h.RedisConnection.Del(redisPageKey)
				h.RedisConnection.SAdd(redisScheduleKey,redisPageKey)
				// making carousel
				redisCarouselKey := MakeRedisKey(redisPageKey+":carousel")
				h.RedisConnection.Del(redisCarouselKey)
				for _, k := range j.Carousel{

					resultByte, err := proto.Marshal(k)
					if err != nil {
						return err
					}
					h.RedisConnection.SAdd(redisCarouselKey, resultByte)
				}
				h.RedisConnection.Expire(redisCarouselKey, 3*time.Hour)

				redisRowsKey := MakeRedisKey(redisPageKey+":rows")
				h.RedisConnection.Del(redisRowsKey)
				// making rows
				for _,l := range j.Rows {
					rowKey := MakeRedisKey(redisRowsKey+":"+String(l.RowIndex)+":"+l.Rowname)
					h.RedisConnection.Del(rowKey)
					h.RedisConnection.SAdd(redisRowsKey, rowKey)
					var myPipes []bson.D
					// pipe1
					if l.Categorylist != nil && len(l.Categorylist) > 0 {
						myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.categories", bson.D{{"$in", l.Categorylist}}}}}},)
					}
					//pipe2
					if l.Languagelist != nil && len(l.Languagelist) > 0 {
						myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.languages", bson.D{{"$in", l.Languagelist}}}}}},)
					}
					//pipe3
					if l.GenreList != nil && len(l.GenreList) > 0 {
						myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.genre", bson.D{{"$in", l.GenreList}}}}}},)
					}
					//pipe4
					if l.SourceList != nil && len(l.SourceList) > 0 {
						myPipes = append(myPipes, bson.D{{"$match", bson.D{{"content.source", bson.D{{"$in", l.SourceList}}}}}},)
					}
					//pipe5
					myPipes = append(myPipes,bson.D{{"$sort", bson.D{{"metadata.year", -1}}}}, )
					//pipe6
					myPipes = append(myPipes, bson.D{{"$project", bson.D{{"_id", 0}, {"ref_id", 1}}}},)
					cur, err := h.MongoCollection.Aggregate(context.Background(), myPipes, options.Aggregate().SetMaxTime(2000*time.Millisecond))
					if err != nil {
						return err
					}
					for cur.Next(context.TODO()) {
						h.RedisConnection.SAdd(rowKey, cur.Current.Lookup("ref_id").StringValue())
					}
					cur.Close(context.TODO())
					h.RedisConnection.Expire(rowKey, 3*time.Hour)
				}
				h.RedisConnection.Expire(redisPageKey, 3*time.Hour)
			}
			h.RedisConnection.Expire(redisScheduleKey, 3*time.Hour)
			break
		}
	}

	messageToPublish :=  NotificationService.MessageToPublish{
		ExchangeName:         "amq.topic",
		RoutingKeyName:       schedule.Vendor+"."+schedule.Brand,
		MessageTosend:        []byte("refreshSchedule"),
	}
	return h.NotificationEventPublisher.Publish(ctx,messageToPublish)
}


func String(n int32) string {
	buf := [11]byte{}
	pos := len(buf)
	i := int64(n)
	signed := i < 0
	if signed {
		i = -i
	}
	for {
		pos--
		buf[pos], i = '0'+byte(i%10), i/10
		if i == 0 {
			if signed {
				pos--
				buf[pos] = '-'
			}
			return string(buf[pos:])
		}
	}
}

func MakeRedisKey(badKey string) string {
	return strings.ToLower(strings.Replace(badKey, " ", "_", -1))
}

func getTimeZone() string {
	currentHour := time.Now().Hour()
	var timeZone string
	if(currentHour >= 6 && currentHour < 9){
		timeZone = "6-9"
	}else if(currentHour >= 9 && currentHour < 12){
		timeZone = "9-12"
	}else if(currentHour >= 12 && currentHour < 15){
		timeZone = "12-15"
	}else if(currentHour >= 15 && currentHour < 18){
		timeZone = "15-18"
	}else if(currentHour >= 18 && currentHour < 21){
		timeZone = "18-21"
	}else if(currentHour >= 21 && currentHour < 24){
		timeZone = "21-24"
	}else if(currentHour >= 24){
		timeZone = "1-6"
	}
	return timeZone
}




// TODO best learning
/*
		I was implementing pipe in aggregation where find these values from array to that property
	myPipes = append(myPipes, bson.D{{"$match", bson.D{{"metadata.categories", bson.D{{"$in", bson.A{j.CategoryList}}}}}}},)
	It gave me wrong result of some and correct of others

	Then naseeb se I removed bson.A{} and passed the slice directly and it gave me all right results
*/




















