package main

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/micro/go-micro"
	grpcClient "github.com/micro/go-micro/client/grpc"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-micro/service/grpc"
	"github.com/micro/go-micro/util/log"
	TileService "github.com/nayanmakasare/TileService/proto"
	TvAuthenticationService "github.com/nayanmakasare/TvAuthenticationService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/credentials"
	"gopkg.in/robfig/cron.v3"
	"time"
)

const (
	defaultHost = "mongodb://nayan:tlwn722n@cluster0-shard-00-00-8aov2.mongodb.net:27017,cluster0-shard-00-01-8aov2.mongodb.net:27017,cluster0-shard-00-02-8aov2.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority"
	//defaultHost = "mongodb://192.168.1.9:27017"
	//defaultHost = "mongodb://192.168.1.143:27017"
)


func logWrapper(fn server.HandlerFunc) server.HandlerFunc {
	return func(ctx context.Context, req server.Request, rsp interface{}) error {
		log.Infof("[wrapper] server request: %v", req.Endpoint())
		err := fn(ctx, req, rsp)
		return err
	}
}

func tvAuthWrapper(fn server.HandlerFunc) server.HandlerFunc {
	return func(ctx context.Context, req server.Request, rsp interface{}) error {
		meta, ok := metadata.FromContext(ctx)
		log.Info("Triggered Auth ", meta["tvEmac"])
		if !ok {
			return errors.New("no auth meta-data found in request")
		}
		emac := meta["tvEmac"]
		authClient := TvAuthenticationService.NewTvAuthenticationService("TvAuthenticationService", grpcClient.NewClient())
		isAuthorised ,err := authClient.CloudwalkerAuthWall(context.Background(), &TvAuthenticationService.TargetTv{Emac:emac})
		if err != nil {
			return err
		}
		if !isAuthorised.Result {
			return errors.New("Not an authorised Tv")
		}
		err = fn(ctx, req, rsp)
		return err
	}
}

var (
	crt = "server.crt"
	key = "server.key"
)

func main(){

	creds, err := credentials.NewServerTLSFromFile(crt, key)
	if err != nil {
		log.Fatal(err)
	}



	service := grpc.NewService(
		micro.Name("TileService"),
		micro.Address(":50051"),
		micro.Version("1.0"),
		micro.WrapHandler(logWrapper),
		//TODO need to learn about how to pass metadata from context in android.
		//micro.WrapHandler(tvAuthWrapper),
	)
	service.Init()
	uri := defaultHost
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Debug(err)
	}
	client := GetRedisClient()

	// Register notification Broker
	notificationPublisher := micro.NewPublisher("notify", service.Client())
	tileMongoCollection := mongoClient.Database("test").Collection("cwmovies")

	// cron job which run on every one hour
	c := cron.New()
	c.AddFunc("@hourly", func() {
		syncTilesToRedis(tileMongoCollection, client)
	})
	c.Start()

	// when the service get live
	go syncTilesToRedis(tileMongoCollection, client)

	handler := TileServiceHandler{RedisConnection:client}
	err = TileService.RegisterTileServiceHandler(service.Server(), &handler)
	if err != nil {
		log.Fatal(err)
	}

	subscriber := TileServiceSubscriber{
		MongoCollection:tileMongoCollection,
		RedisConnection:client,
		NotificationEventPublisher:notificationPublisher,
	}

	//Register Subscriber
	//Subscribe
	err = micro.RegisterSubscriber("applySchedule", service.Server(), &subscriber)
	if err != nil {
		log.Fatal(err)
	}

	// Run service
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
	log.Info("Stopping cron")
	defer c.Stop()
}

func GetRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatalf("Could not connect to redis %v", err)
	}
	return client
}


func syncTilesToRedis(tileCollection *mongo.Collection, tileRedisClient *redis.Client) {
	myStages := mongo.Pipeline{
		bson.D{{"$project", bson.D{{"_id", 0}, {"ref_id", 1},
			{"metadata.title", 1},
			{"posters.landscape", 1},
			{"posters.portrait", 1},
			{"content.package", 1},
			{"content.detailPage", 1},
			{"content.target", 1},
			{"content.type", 1},
			{"content.playstoreUrl", 1},
			{"content.useAlternate", 1},
			{"content.alternateUrl", 1}}}},
	}

	cur, err := tileCollection.Aggregate(context.Background(), myStages)
	if err != nil {
		log.Fatal(err)
	}
	/*TODO an important learning when i did "cur.Current.Lookup("ref_id").String()" it gave me values with "/" at front and end,
		due to which i was unable to parse the string and get the value from redis so after a long reading I used ,
	"cur.Current.Lookup("ref_id").StringValue()" which gave the proper string without "/" as needed.
	*/
	for cur.Next(context.TODO()){
		var movieTile TileService.MovieTile
		err = cur.Decode(&movieTile)
		ref_id := cur.Current.Lookup("ref_id").StringValue()
		movieTile.RefId = ref_id
		resultByteArray, err := proto.Marshal(&movieTile)
		if err != nil {
			log.Fatal(err)
		}
		tileRedisClient.HSet("cloudwalkerTiles", ref_id, resultByteArray)
	}
	err = cur.Close(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	return
}
