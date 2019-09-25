package main

import (
	"context"
	"errors"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/util/log"
	TileService "github.com/nayanmakasare/TileService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type TileServiceHandler struct {
	MongoCollection *mongo.Collection
	RedisConnection  *redis.Client
	EventPublisher micro.Publisher
}


var defaultRows = []string{
	"cloudwalkerTrailers",
	"cloudwalkerApps",
	"cloudwalkerNews",
	"cloudwalkerLatestMovies",
	"cloudwalkerLatestSeries",
	"cloudwalkerPopularVideos",
	"cloudwalkerHindiMusic",
	"cloudwalkerLatestDocumentary",
	"cloudwalkerShortFilms",
	"cloudwalkerEnglishMusic",
	"cloudwalkerCookery",
}

func (h *TileServiceHandler) CheckInRedis(redisKey string) (bool, error) {
	intCmdResult := h.RedisConnection.Exists(redisKey)
	if intCmdResult.Val() == 1 {
		return  true , nil
	}else {
		return false , nil
	}
}

func (h *TileServiceHandler) GetMovieTiles(ctx context.Context, req *TileService.RowId, stream TileService.TileService_GetMovieTilesStream) error {
	log.Info("Triggered 1")
	exists, err := h.CheckInRedis(req.RowId)
	if err != nil {
		return err
	}
	if exists {
		stringResultCmd := h.RedisConnection.SMembers(req.RowId)
		resultStringArray := stringResultCmd.Val()
		for i := 0 ; i < len(resultStringArray) ; i++ {
			var resultMovie TileService.MovieTile
			err := proto.Unmarshal([]byte(resultStringArray[i]), &resultMovie)
			if err != nil {
				return err
			}
			err = stream.Send(&resultMovie)
			if err != nil {
				return err
			}
		}
		return stream.Close()
	} else {
		return errors.New("Data not in redis")
	}
}

func(h *TileServiceHandler) GetRows( ctx context.Context, request *TileService.GetRowsRequest, stream TileService.TileService_GetRowsStream) error {
	if len(request.UserId) > 0 {
		return errors.New("Yet To implement")
	}else {
		for i, v := range defaultRows {
			tempRowSpecs := &TileService.RowSpec{
				RowName:              v,
				RowId:                v,
				RowShape:             "landscape",
				RowPosition:		  int32(i),
			}
			err := stream.Send(tempRowSpecs)
			if err != nil {
				return err
			}
		}
		log.Info(request.Brand, request.UserId, request.Vendor)
		err := h.EventPublisher.Publish(ctx, request)
		if err != nil {
			return err
		}
		return stream.Close()
	}
}

func (h *TileServiceHandler) InitializingEngine(ctx context.Context, req *TileService.InitializingEngineRequest, res *TileService.InitializingEngineResponse)  error{
	log.Info("Triggered init")
	initProcessResult := make(chan bool, 8)
	go h.StoringAllTilesToRedis(initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerTrailers", "metadata.categories", "Movie Trailers", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerLatestMovies", "metadata.categories", "Movies", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerLatestSeries", "metadata.categories", "Series", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerLatestDocumentary", "metadata.categories", "Documentary", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerPopularVideos", "metadata.categories", "Popular Videos", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerCookery", "metadata.categories", "Cookery", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerHindiMusic", "metadata.categories", "Music Videos Hindi", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerEnglishMusic", "metadata.categories", "Music Videos English", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerNews", "metadata.categories", "News", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerApps", "metadata.categories", "App", initProcessResult)
	go h.StoringDataFromDbToRedis("cloudwalkerShortFilms", "metadata.categories", "Short Film", initProcessResult)
	res.IsDone = <- initProcessResult
	return nil
}

func (h *TileServiceHandler) StoringAllTilesToRedis(result chan bool) {
	myStages := mongo.Pipeline{
		bson.D{{"$project", bson.D{{"_id", 0},{"ref_id", 1},{"metadata.title", 1},{"posters.landscape", 1},{"posters.portrait", 1},{"content.package", 1}, {"content.detailPage", 1}}}},
	}
	cur, err := h.MongoCollection.Aggregate(context.Background(), myStages)
	if err != nil {
		result <- false
		log.Fatal(err)
	}
	for cur.Next(context.Background()){
		var movieTile TileService.MovieTile
		err = cur.Decode(&movieTile)
		movieTile.RefId = cur.Current.Lookup("ref_id").StringValue()
		resultByteArray, err := proto.Marshal(&movieTile)
		if err != nil {
			log.Info("Tigger 4")
			result <- false
			log.Fatal(err)
		}

		/*TODO an important learning when i did "cur.Current.Lookup("ref_id").String()" it gave me values with "/" at front and end,
			due to which i was unable to parse the string and get the value from redis so after a long reading I used ,
		"cur.Current.Lookup("ref_id").StringValue()" which gave the proper string without "/" as needed.
		*/
		h.RedisConnection.HSet("cloudwalkerTiles",cur.Current.Lookup("ref_id").StringValue(), resultByteArray)
	}
	err =  cur.Close(context.TODO())
	if err != nil {
		result <- false
		log.Fatal(err)
	}
	result <- true
}

func (h *TileServiceHandler) StoringDataFromDbToRedis(redisKey, contentKey, contentValue string, resultStatus chan bool) {
	log.Info("StoringDataFromDbToRedis 1")
	myStages := mongo.Pipeline{
		//stage 1
		bson.D{{"$match", bson.D{{contentKey, bson.D{{"$in", bson.A{contentValue}}}}}}},
		//stage 2
		bson.D{{"$sort", bson.D{{"created_at", -1}}}},
		//stage 3
		bson.D{{"$limit", 200}},
		//stage 4
		bson.D{{"$project", bson.D{{"_id", 0},{"ref_id", 1},{"metadata.title", 1},{"posters.landscape", 1},{"posters.portrait", 1},{"content.package", 1}, {"content.detailPage", 1}}}},
	}

	cur, err := h.MongoCollection.Aggregate(context.Background(), myStages, options.Aggregate().SetMaxTime(2000*time.Millisecond))
	if err != nil {
		log.Info("StoringDataFromDbToRedis 2")
		resultStatus <- false
		log.Fatal(err)
	}
	count := 0
	for cur.Next(context.Background()) {
		var movieTile TileService.MovieTile
		err := cur.Decode(&movieTile)
		movieTile.RefId = cur.Current.Lookup("ref_id").StringValue()
		count++
		resultByteArray, err := proto.Marshal(&movieTile)
		if err != nil {
			log.Info("StoringDataFromDbToRedis 2")
			resultStatus <- false
			log.Fatal(err)
		}
		h.RedisConnection.SAdd(redisKey,resultByteArray)
	}

	log.Info(redisKey, count)
	err = cur.Close(context.Background())
	if err != nil {
		log.Info("StoringDataFromDbToRedis 3")
		resultStatus <- false
		log.Fatal(err)
	}
	resultStatus <- true
}

type SimpleQuery struct {
	query string
}


func (h *TileServiceHandler) streamMovieTiles(stream TileService.TileService_GetMovieTilesStream){
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	movieTilesChan, errorsChan := h.AsyncListMovieTile(ctx)
	var wg sync.WaitGroup

	//spawn worker goRoutines
	for i := 0 ; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for{
				select {
				case err, ok := <- errorsChan:
					if !ok {
						return
					}
					if err == context.Canceled {
						return
					}
					log.Fatal(err)

				case movieTile, ok := <- movieTilesChan:
					if !ok {
						return
					}
					stream.Send(&movieTile)
				}
			}
		}()
	}
	stream.Close()
	wg.Wait()
}

func(h *TileServiceHandler) AsyncListMovieTile(ctx context.Context) (<- chan TileService.MovieTile, <- chan error ){
	movieTileChan := make(chan TileService.MovieTile, 20)
	errorsChan := make(chan error, 1)

	go asyncListMovieTile(ctx, movieTileChan, errorsChan, h.MongoCollection)
	return  movieTileChan, errorsChan
}

func asyncListMovieTile(ctx context.Context, movieTileChan chan <- TileService.MovieTile, errorsChan chan <- error, mongoCollection *mongo.Collection){
	defer close(movieTileChan)
	defer close(errorsChan)

	for {
		select {
		case <- ctx.Done():
			errorsChan <- ctx.Err()
			return
		default:
		}

		movieTiles, err := 	fetchMovieTilesFromMongo(mongoCollection)
		if err != nil {
			errorsChan <- err
			return
		}
		if len(movieTiles) == 0 {
			return
		}

		for _, movieTile := range movieTiles {
			movieTileChan <- movieTile
		}
	}
}

func fetchMovieTilesFromMongo(collection *mongo.Collection) ([]TileService.MovieTile, error) {
	cur, err := collection.Find(context.Background(), bson.D{{}})
	if err != nil {
		return nil, err
	}
	var movieTiles []TileService.MovieTile
	for cur.Next(context.Background()){
		var movieTile TileService.MovieTile
		err = cur.Decode(&movieTile)
		if err != nil {
			return nil, err
		}
		movieTiles = append(movieTiles, movieTile)
	}
	return movieTiles, nil
}


